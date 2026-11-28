//! A compact store for "dense key to ascending list of ids".
//!
//! The index finds the documents (pieces of text) that match a search. The
//! caller then needs, for each piece, the ascending list of rows that use it.
//! This store keeps those lists: keys are `0..n` with no gaps, each key has a
//! list (possibly empty) of ascending `u32`.
//!
//! The lists use the same gap, bit-packing and block code as the index
//! postings. Keys are grouped (up to 128 keys or 32 KiB of lists, whichever
//! comes first; a list that does not fit the group being filled starts the
//! next one, and a list over 32 KiB is a group of its own); a group is one
//! read and carries its own checksum, which is also kept in the group index
//! and checked against both whenever one of its keys is read. The footer's
//! checksum covers the group index, so it covers every group's checksum. The
//! README gives the layout.

use std::io::Write;

use nwdb_core::{Error, Result};

use crate::codec::{
    BLOCK, decode_block, encode_blocks, get_varint, get_varint_u32, put_inline, put_varint,
};
use crate::format::{corrupt, le32, le64};
use crate::ids::is_ascending;
use crate::source::{ReadAt, Sink, read_checked};

const MAGIC: &[u8; 4] = b"NWIL";
const VERSION: u32 = 2;
const FOOTER: usize = 40;
const INDEX_ENTRY: usize = 16;
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
    /// The checksum in the footer, which covers the group index and so every
    /// group's checksum.
    pub footer_crc: u32,
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
    index: Vec<(u32, u64, u32)>,
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
    /// Memory is bounded by one group (up to about 32 KiB, or one encoded
    /// list if that is larger) plus one encoded list, and 16 bytes per group
    /// already written.
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
            put_inline(&mut self.lists, ids);
        } else {
            encode_blocks(ids, &mut self.lists, |_, _| {});
        }
        let len = self.lists.len() - before;
        if before > 0 && self.lists.len() > GROUP_BYTES {
            // This list would take the group past the limit: close the group
            // without it, so that a reader of a small neighbour never pays
            // for a large list. The list then starts the next group, and if
            // it alone is over the limit it is a group of its own.
            let list = self.lists.split_off(before);
            self.close_group().inspect_err(|_| self.failed = true)?;
            self.lists = list;
        }
        self.lens.push(len);
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
        debug_assert!(self.lists.len() <= GROUP_BYTES || self.lens.len() == 1);
        for &l in &self.lens {
            put_varint(&mut header, l as u64);
        }
        let crc = crc32c::crc32c_append(crc32c::crc32c(&header), &self.lists);
        self.index
            .push((self.first_key as u32, self.out.pos(), crc));
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
        for &(key, off, crc) in &self.index {
            index.extend_from_slice(&key.to_le_bytes());
            index.extend_from_slice(&off.to_le_bytes());
            index.extend_from_slice(&crc.to_le_bytes());
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
            footer_crc: crc,
        })
    }
}

/// Reads an id-list file with positioned reads; `Send + Sync`. Memory held:
/// 16 bytes per group (about 16 bytes per 128 keys at most).
pub struct IdListReader<S: ReadAt> {
    src: S,
    len: u64,
    meta: IdListMeta,
    first_keys: Vec<u32>,
    offsets: Vec<u64>,
    crcs: Vec<u32>,
    index_offset: u64,
}

/// One group read and checked: its bytes, where its lists start in them, and
/// where each list ends, counted from there.
struct Loaded {
    group: usize,
    bytes: Vec<u8>,
    data_at: usize,
    ends: Vec<usize>,
}

/// The encoded list of one key, from a group that has been checked.
pub struct IdList<'a> {
    bytes: &'a [u8],
    max_ids: u64,
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
        let u32_at = |i: usize| le32(&f, i);
        let u64_at = |i: usize| le64(&f, i);
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
        let mut crcs: Vec<u32> = Vec::with_capacity(groups as usize);
        for (i, e) in index.as_chunks::<INDEX_ENTRY>().0.iter().enumerate() {
            let (key, off, crc) = (le32(e, 0), le64(e, 4), le32(e, 12));
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
            crcs.push(crc);
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
                footer_crc: u32_at(32),
            },
            first_keys,
            offsets,
            crcs,
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
        let mut ids = Vec::new();
        self.for_each(std::iter::once(key), |_, list| {
            ids = list.decode()?;
            Ok(())
        })?;
        Ok(ids)
    }

    /// Calls `f` with the list of each of `keys`, in the order given. The
    /// group read last is kept, so keys in ascending order read each group
    /// once however many of its keys there are. A key at or above
    /// [`IdListReader::keys`] is `NotFound`. A group is checked against its
    /// checksum, in the file and in the group index, when it is read.
    pub fn for_each(
        &self,
        keys: impl IntoIterator<Item = u32>,
        mut f: impl FnMut(u32, IdList<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut loaded: Option<Loaded> = None;
        for key in keys {
            if u64::from(key) >= self.meta.keys {
                return Err(Error::not_found(format!("key {key} is not in the file")));
            }
            let g = self.first_keys.partition_point(|&k| k <= key) - 1;
            if loaded.as_ref().is_none_or(|l| l.group != g) {
                loaded = Some(self.load_group(g)?);
            }
            let l = loaded.as_ref().expect("just loaded");
            let i = (key - self.first_keys[g]) as usize;
            let from = if i == 0 { 0 } else { l.ends[i - 1] };
            let bytes = &l.bytes[l.data_at + from..l.data_at + l.ends[i]];
            let max_ids = self.meta.ids;
            f(key, IdList { bytes, max_ids })?;
        }
        Ok(())
    }

    fn load_group(&self, g: usize) -> Result<Loaded> {
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
        let bytes = read_checked(&self.src, self.len, start, end - start, "group")?;
        let Some((body, crc)) = bytes.split_last_chunk::<4>() else {
            return corrupt("group is too short for its checksum");
        };
        let now = crc32c::crc32c(body);
        if now != u32::from_le_bytes(*crc) || now != self.crcs[g] {
            return corrupt("group checksum mismatch");
        }
        // The header is one length per key; the lists follow it.
        let mut p = 0usize;
        let mut list_at = 0u64;
        let mut ends = Vec::with_capacity(in_group as usize);
        for _ in 0..in_group {
            list_at = list_at.saturating_add(get_varint(body, &mut p)?);
            ends.push(list_at);
        }
        if list_at != (body.len() - p) as u64 {
            return corrupt("list lengths do not add up to the group size");
        }
        let ends = ends.into_iter().map(|e| e as usize).collect();
        Ok(Loaded {
            group: g,
            bytes,
            data_at: p,
            ends,
        })
    }
}

impl IdList<'_> {
    /// How many ids the list holds, from its leading count and without
    /// decoding the ids. The count is checked against what the file and the
    /// list's own bytes can hold.
    pub fn id_count(&self) -> Result<u64> {
        let mut p = 0usize;
        let n = get_varint(self.bytes, &mut p)?;
        if n > self.max_ids
            || (n > INLINE_MAX as u64 && n.div_ceil(BLOCK as u64) > self.bytes.len() as u64)
        {
            return corrupt("list claims more ids than the file or its bytes can hold");
        }
        Ok(n)
    }

    /// The ascending ids.
    pub fn decode(&self) -> Result<Vec<u32>> {
        let list = self.bytes;
        let n = self.id_count()? as usize;
        let mut p = 0usize;
        get_varint(list, &mut p)?;
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
