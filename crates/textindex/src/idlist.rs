//! A compact store for "dense key to ascending list of ids".
//!
//! The index finds the documents (pieces of text) that match a search. The
//! caller then needs, for each piece, the ascending list of rows that use it.
//! This store keeps those lists: keys are `0..n` with no gaps, each key has a
//! list (possibly empty) of ascending `u32`.
//!
//! The lists use the same gap, bit-packing and block code as the index
//! postings. Keys are grouped (up to 128 keys or about 32 KiB of lists, whichever
//! comes first); a group is one read and carries its own checksum, which is
//! verified whenever one of its keys is read. The README gives the layout.

use std::io::Write;

use nwdb_core::{Error, Result};

use crate::codec::{BLOCK, decode_block, encode_blocks, get_varint, get_varint_u32, put_varint};
use crate::ids::is_ascending;
use crate::source::{ReadAt, Sink, read_checked};

const MAGIC: &[u8; 4] = b"NWIL";
const VERSION: u32 = 1;
const FOOTER: usize = 40;
const INDEX_ENTRY: usize = 12;
const MAX_GROUP_KEYS: usize = 128;
const GROUP_BYTES: usize = 32 * 1024;
const INLINE_MAX: usize = 8;

/// Counts describing an id-list file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IdListMeta {
    /// Number of keys.
    pub keys: u64,
    /// Total ids over all lists.
    pub ids: u64,
    /// Size of the file in bytes.
    pub bytes: u64,
}

/// Writes an id-list file. Lists are pushed in key order: the first list
/// pushed belongs to key 0, the next to key 1, and so on.
pub struct IdListWriter<W: Write> {
    out: Sink<W>,
    lists: Vec<u8>,
    lens: Vec<usize>,
    first_key: u64,
    keys: u64,
    ids: u64,
    index: Vec<(u32, u64)>,
    failed: bool,
}

impl<W: Write> IdListWriter<W> {
    /// Starts a file written to `out`.
    pub fn new(out: W) -> Self {
        Self {
            out: Sink::new(out),
            lists: Vec::new(),
            lens: Vec::new(),
            first_key: 0,
            keys: 0,
            ids: 0,
            index: Vec::new(),
            failed: false,
        }
    }

    /// Appends the list of the next key. `ids` must be strictly ascending.
    /// Memory is bounded by one group: about 32 KiB plus one encoded list.
    pub fn push(&mut self, ids: &[u32]) -> Result<()> {
        if self.failed {
            return Err(Error::fatal("id-list writer failed earlier"));
        }
        if !is_ascending(ids) {
            return Err(Error::invalid_argument(
                "an id list must be strictly ascending",
            ));
        }
        if self.keys > u64::from(u32::MAX) {
            return Err(Error::invalid_argument("too many keys"));
        }
        let before = self.lists.len();
        put_varint(&mut self.lists, ids.len() as u64);
        if ids.len() <= INLINE_MAX {
            let mut next_min = 0u64;
            for &id in ids {
                put_varint(&mut self.lists, u64::from(id) - next_min);
                next_min = u64::from(id) + 1;
            }
        } else {
            encode_blocks(ids, &mut self.lists, |_, _| {});
        }
        self.lens.push(self.lists.len() - before);
        self.keys += 1;
        self.ids += ids.len() as u64;
        if self.lens.len() >= MAX_GROUP_KEYS || self.lists.len() >= GROUP_BYTES {
            self.close_group().inspect_err(|_| self.failed = true)?;
        }
        Ok(())
    }

    fn close_group(&mut self) -> Result<()> {
        if self.lens.is_empty() {
            return Ok(());
        }
        let mut header = Vec::with_capacity(self.lens.len() * 2);
        for &l in &self.lens {
            put_varint(&mut header, l as u64);
        }
        let crc = crc32c::crc32c_append(crc32c::crc32c(&header), &self.lists);
        self.index.push((self.first_key as u32, self.out.pos()));
        self.out.write(&header)?;
        self.out.write(&self.lists)?;
        self.out.write(&crc.to_le_bytes())?;
        self.first_key += self.lens.len() as u64;
        self.lens.clear();
        self.lists.clear();
        Ok(())
    }

    /// Writes the group index and footer and flushes the writer.
    pub fn finish(mut self) -> Result<IdListMeta> {
        if self.failed {
            return Err(Error::fatal("id-list writer failed earlier"));
        }
        self.close_group()?;
        let index_offset = self.out.pos();
        let mut index = Vec::with_capacity(self.index.len() * INDEX_ENTRY);
        for &(key, off) in &self.index {
            index.extend_from_slice(&key.to_le_bytes());
            index.extend_from_slice(&off.to_le_bytes());
        }
        let mut footer = [0u8; FOOTER];
        footer[0..4].copy_from_slice(&VERSION.to_le_bytes());
        footer[4..8].copy_from_slice(&(self.index.len() as u32).to_le_bytes());
        footer[8..16].copy_from_slice(&self.keys.to_le_bytes());
        footer[16..24].copy_from_slice(&self.ids.to_le_bytes());
        footer[24..32].copy_from_slice(&index_offset.to_le_bytes());
        let crc = crc32c::crc32c_append(crc32c::crc32c(&index), &footer[..32]);
        footer[32..36].copy_from_slice(&crc.to_le_bytes());
        footer[36..40].copy_from_slice(MAGIC);
        self.out.write(&index)?;
        self.out.write(&footer)?;
        self.out.flush()?;
        Ok(IdListMeta {
            keys: self.keys,
            ids: self.ids,
            bytes: self.out.pos(),
        })
    }
}

/// Reads an id-list file with positioned reads; `Send + Sync`. Memory held:
/// 12 bytes per group (about 12 bytes per 128 keys at most).
pub struct IdListReader<S: ReadAt> {
    src: S,
    len: u64,
    meta: IdListMeta,
    first_keys: Vec<u32>,
    offsets: Vec<u64>,
    index_offset: u64,
}

fn corrupt<T>(what: &str) -> Result<T> {
    Err(Error::corrupt(what))
}

impl<S: ReadAt> IdListReader<S> {
    /// Opens the file, checking the footer and the group index.
    pub fn open(src: S) -> Result<Self> {
        let len = src.size()?;
        if len < FOOTER as u64 {
            return corrupt("file is too short to be an id-list file");
        }
        let mut f = [0u8; FOOTER];
        src.read_exact_at(&mut f, len - FOOTER as u64)?;
        if &f[36..40] != MAGIC {
            return corrupt("not an id-list file: bad magic");
        }
        let u32_at = |i: usize| u32::from_le_bytes([f[i], f[i + 1], f[i + 2], f[i + 3]]);
        let u64_at = |i: usize| {
            let mut x = [0u8; 8];
            x.copy_from_slice(&f[i..i + 8]);
            u64::from_le_bytes(x)
        };
        if u32_at(0) != VERSION {
            return corrupt("unknown id-list format version");
        }
        let (groups, keys, ids, index_offset) = (u32_at(4), u64_at(8), u64_at(16), u64_at(24));
        let index_len = u64::from(groups) * INDEX_ENTRY as u64;
        let body = len - FOOTER as u64;
        if index_len > body || index_offset != body - index_len {
            return corrupt("group index does not fit the file");
        }
        let index = read_checked(&src, len, index_offset, index_len, "group index")?;
        if crc32c::crc32c_append(crc32c::crc32c(&index), &f[..32]) != u32_at(32) {
            return corrupt("footer checksum mismatch");
        }
        if (keys == 0) != (groups == 0)
            || u64::from(groups) > keys
            || keys > u64::from(u32::MAX) + 1
        {
            return corrupt("key and group counts disagree");
        }
        let mut first_keys: Vec<u32> = Vec::with_capacity(groups as usize);
        let mut offsets: Vec<u64> = Vec::with_capacity(groups as usize);
        for (i, e) in index.as_chunks::<INDEX_ENTRY>().0.iter().enumerate() {
            let key = u32::from_le_bytes([e[0], e[1], e[2], e[3]]);
            let mut o = [0u8; 8];
            o.copy_from_slice(&e[4..12]);
            let off = u64::from_le_bytes(o);
            let ok = if i == 0 {
                key == 0 && off == 0
            } else {
                key > first_keys[i - 1]
                    && key - first_keys[i - 1] <= MAX_GROUP_KEYS as u32
                    && off > offsets[i - 1]
            };
            if !ok || u64::from(key) >= keys || off >= index_offset {
                return corrupt("group index is not ascending");
            }
            first_keys.push(key);
            offsets.push(off);
        }
        if groups > 0 && keys - u64::from(first_keys[groups as usize - 1]) > MAX_GROUP_KEYS as u64 {
            return corrupt("last group has too many keys");
        }
        Ok(Self {
            src,
            len,
            meta: IdListMeta {
                keys,
                ids,
                bytes: len,
            },
            first_keys,
            offsets,
            index_offset,
        })
    }

    /// Counts describing the file.
    pub fn meta(&self) -> &IdListMeta {
        &self.meta
    }

    /// Number of keys.
    pub fn keys(&self) -> u64 {
        self.meta.keys
    }

    /// The ascending ids of `key`. A key at or above [`IdListReader::keys`]
    /// is `NotFound`. The group holding the key is read whole and its
    /// checksum verified.
    pub fn get(&self, key: u32) -> Result<Vec<u32>> {
        if u64::from(key) >= self.meta.keys {
            return Err(Error::not_found(format!("key {key} is not in the file")));
        }
        let g = self.first_keys.partition_point(|&k| k <= key) - 1;
        let start = self.offsets[g];
        let end = self
            .offsets
            .get(g + 1)
            .copied()
            .unwrap_or(self.index_offset);
        let in_group = match self.first_keys.get(g + 1) {
            Some(&next) => u64::from(next),
            None => self.meta.keys,
        } - u64::from(self.first_keys[g]);
        let group = read_checked(&self.src, self.len, start, end - start, "group")?;
        let Some((body, crc)) = group.split_last_chunk::<4>() else {
            return corrupt("group is too short for its checksum");
        };
        if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
            return corrupt("group checksum mismatch");
        }
        // The header is one length per key; the lists follow it.
        let mut p = 0usize;
        let (mut list_at, mut want) = (0u64, None);
        let mut lens = Vec::with_capacity(in_group as usize);
        for _ in 0..in_group {
            lens.push(get_varint(body, &mut p)?);
        }
        let data = &body[p..];
        for (i, &l) in lens.iter().enumerate() {
            if i as u64 == u64::from(key - self.first_keys[g]) {
                want = Some((list_at, l));
            }
            list_at = list_at.saturating_add(l);
        }
        if list_at != data.len() as u64 {
            return corrupt("list lengths do not add up to the group size");
        }
        let Some((at, l)) = want else {
            return corrupt("key is missing from its group");
        };
        let list = &data[at as usize..(at + l) as usize];
        self.decode_list(list)
    }

    fn decode_list(&self, list: &[u8]) -> Result<Vec<u32>> {
        let mut p = 0usize;
        let n = get_varint(list, &mut p)?;
        if n > self.meta.ids
            || (n > INLINE_MAX as u64 && n.div_ceil(BLOCK as u64) > list.len() as u64)
        {
            return corrupt("list claims more ids than the file or its bytes can hold");
        }
        let n = n as usize;
        let mut out = Vec::with_capacity(n.min(BLOCK * 32));
        let mut next_min = 0u64;
        if n <= INLINE_MAX {
            for _ in 0..n {
                let id = next_min + u64::from(get_varint_u32(list, &mut p)?);
                if id > u64::from(u32::MAX) {
                    return corrupt("list passes the largest id");
                }
                out.push(id as u32);
                next_min = id + 1;
            }
        } else {
            let mut left = n;
            while left > 0 {
                let b = left.min(BLOCK);
                p += decode_block(&list[p..], b, next_min, &mut out)?;
                next_min = u64::from(out[out.len() - 1]) + 1;
                left -= b;
            }
        }
        if p != list.len() {
            return corrupt("list has bytes after its last id");
        }
        Ok(out)
    }
}
