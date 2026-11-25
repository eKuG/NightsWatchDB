//! Reading and searching an index file.
//!
//! The reader keeps only the directory and the sparse index of each segment
//! in memory. Every search is positioned reads: one block of the dictionary,
//! then the posting list blocks it needs. Nothing is shared between calls, so
//! one reader serves any number of threads.
//!
//! Every number read from the file is checked before it is used: lengths
//! against the file's length, offsets against the area they point into, counts
//! against the bytes that would have to back them. A file that fails a check
//! is [`Error::Corrupt`]. On top of the structural checks each segment has a
//! checksum, verified the first time the segment is used.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::atomic::{AtomicBool, Ordering};

use nwdb_core::{Error, Result};

use crate::codec::{BLOCK, SKIP_ENTRY, block_max, decode_block, get_varint, get_varint_u32};
use crate::format::{
    DIR_ENTRY, DirEntry, FOOTER, INLINE_MAX, MAX_DICT_BLOCK, TERMS_PER_BLOCK, decode_footer,
    footer_crc,
};
use crate::ids::DocIds;
use crate::meta::IndexMeta;
use crate::source::{ReadAt, read_checked};
use crate::tokenizer::MAX_TOKEN_LEN;

/// Bytes a cursor reads ahead when it walks a posting list block by block.
const READ_AHEAD: u64 = 8 * 1024;

/// First terms of the dictionary blocks of one segment, and where each block
/// starts. This is what stays in memory.
#[derive(Default)]
struct Sparse {
    terms: Vec<u8>,
    ends: Vec<usize>,
    offs: Vec<u64>,
}

impl Sparse {
    fn len(&self) -> usize {
        self.offs.len()
    }

    fn term(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.terms[start..self.ends[i]]
    }

    /// Number of blocks whose first term satisfies `pred`, where `pred` is
    /// true for a prefix of the blocks.
    fn count(&self, pred: impl Fn(&[u8]) -> bool) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(self.term(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// A dictionary entry: the document count and where the ids are.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    df: u32,
    inline: [u32; INLINE_MAX],
    /// Offset of the posting list inside the postings area (when `df` is
    /// above `INLINE_MAX`).
    off: u64,
}

struct Segment {
    e: DirEntry,
    postings_at: u64,
    dict_at: u64,
    sparse: Sparse,
    verified: AtomicBool,
}

fn corrupt<T>(what: &str) -> Result<T> {
    Err(Error::corrupt(what))
}

impl Segment {
    fn postings_end(&self) -> u64 {
        self.postings_at + self.e.postings_len
    }

    /// Terms in dictionary block `i`.
    fn terms_in_block(&self, i: usize) -> usize {
        if i + 1 < self.sparse.len() {
            TERMS_PER_BLOCK as usize
        } else {
            (self.e.terms - u64::from(TERMS_PER_BLOCK) * (self.sparse.len() as u64 - 1)) as usize
        }
    }

    /// Checks the checksum of postings and dictionary, once per reader.
    fn verify<S: ReadAt>(&self, src: &S) -> Result<()> {
        if self.verified.load(Ordering::Relaxed) {
            return Ok(());
        }
        let total = self.e.postings_len + self.e.dict_len;
        let mut buf = vec![0u8; total.min(64 * 1024) as usize];
        let (mut crc, mut done) = (0u32, 0u64);
        while done < total {
            let n = (total - done).min(buf.len() as u64) as usize;
            src.read_exact_at(&mut buf[..n], self.postings_at + done)?;
            crc = crc32c::crc32c_append(crc, &buf[..n]);
            done += n as u64;
        }
        if crc != self.e.body_crc {
            return corrupt("segment checksum mismatch");
        }
        self.verified.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn read_block<S: ReadAt>(&self, src: &S, i: usize) -> Result<Vec<u8>> {
        let a = self.sparse.offs[i];
        let b = self
            .sparse
            .offs
            .get(i + 1)
            .copied()
            .unwrap_or(self.e.dict_len);
        let len = b - a;
        if len > MAX_DICT_BLOCK {
            return corrupt("dictionary block is larger than any block can be");
        }
        let mut buf = vec![0u8; len as usize];
        src.read_exact_at(&mut buf, self.dict_at + a)?;
        Ok(buf)
    }

    /// Decodes dictionary block `i`, calling `f(term, entry)` for each term
    /// until it returns false. Returns false if `f` stopped the walk.
    fn walk_block<S: ReadAt>(
        &self,
        src: &S,
        i: usize,
        f: &mut dyn FnMut(&[u8], Entry) -> bool,
    ) -> Result<bool> {
        let bytes = self.read_block(src, i)?;
        let e = &self.e;
        let mut cur: Vec<u8> = Vec::with_capacity(MAX_TOKEN_LEN);
        let mut p = 0usize;
        for k in 0..self.terms_in_block(i) {
            let shared = get_varint(&bytes, &mut p)?;
            let suffix = get_varint(&bytes, &mut p)?;
            if shared > cur.len() as u64 {
                return corrupt("front-coded term shares more than the previous term has");
            }
            if suffix == 0 || shared + suffix > MAX_TOKEN_LEN as u64 {
                return corrupt("term has an impossible length");
            }
            let (shared, suffix) = (shared as usize, suffix as usize);
            let Some(sb) = bytes.get(p..p + suffix) else {
                return corrupt("term runs past the end of its dictionary block");
            };
            p += suffix;
            if shared < cur.len() && sb[0] <= cur[shared] {
                return corrupt("terms are not in ascending order");
            }
            cur.truncate(shared);
            cur.extend_from_slice(sb);
            if k == 0 && cur != self.sparse.term(i) {
                return corrupt("first term of a block differs from the sparse index");
            }
            let df = get_varint_u32(&bytes, &mut p)?;
            if df == 0 || df > e.docs {
                return corrupt("term has an impossible document count");
            }
            let mut entry = Entry {
                df,
                inline: [0; INLINE_MAX],
                off: 0,
            };
            if df as usize <= INLINE_MAX {
                let mut next_min = 0u64;
                for slot in entry.inline.iter_mut().take(df as usize) {
                    let id = next_min + u64::from(get_varint_u32(&bytes, &mut p)?);
                    if id < u64::from(e.doc_lo) || id > u64::from(e.doc_hi) {
                        return corrupt("inline posting is outside the segment's document range");
                    }
                    *slot = id as u32;
                    next_min = id + 1;
                }
            } else {
                entry.off = get_varint(&bytes, &mut p)?;
                let need = (df as u64).div_ceil(BLOCK as u64) * (SKIP_ENTRY as u64 + 1);
                if entry
                    .off
                    .checked_add(need)
                    .is_none_or(|x| x > e.postings_len)
                {
                    return corrupt("posting list lies outside the postings area");
                }
            }
            if !f(&cur, entry) {
                return Ok(false);
            }
        }
        if p != bytes.len() {
            return corrupt("dictionary block has bytes after its last term");
        }
        Ok(true)
    }

    fn lookup<S: ReadAt>(&self, src: &S, term: &[u8]) -> Result<Option<Entry>> {
        let at = self.sparse.count(|t| t <= term);
        if at == 0 {
            return Ok(None);
        }
        let mut found = None;
        self.walk_block(src, at - 1, &mut |t, e| {
            if t == term {
                found = Some(e);
            }
            t < term
        })?;
        Ok(found)
    }

    /// Calls `f` for every term that starts with `prefix`, in order, until it
    /// returns false.
    fn walk_prefix<S: ReadAt>(
        &self,
        src: &S,
        prefix: &[u8],
        f: &mut dyn FnMut(&[u8], Entry) -> bool,
    ) -> Result<()> {
        let first = self.sparse.count(|t| t < prefix).saturating_sub(1);
        for i in first..self.sparse.len() {
            let head = self.sparse.term(i);
            if i > first && head > prefix && !head.starts_with(prefix) {
                break;
            }
            let more = self.walk_block(src, i, &mut |t, e| {
                if t.starts_with(prefix) {
                    f(t, e)
                } else {
                    t < prefix
                }
            })?;
            if !more {
                break;
            }
        }
        Ok(())
    }
}

/// Forward reader of one posting list that can jump with the skip table.
pub(crate) struct Cursor<'a, S: ReadAt> {
    src: &'a S,
    seg: &'a Segment,
    df: u32,
    kind: Kind,
    /// Posting blocks decoded so far.
    pub blocks_read: u64,
}

enum Kind {
    Inline { ids: [u32; INLINE_MAX], pos: usize },
    Disk(Box<Disk>),
}

struct Disk {
    /// (last id, start relative to the end of the skip table) per block.
    skip: Vec<(u32, u32)>,
    data_at: u64,
    block: Option<usize>,
    buf: Vec<u32>,
    pos: usize,
    raw: Vec<u8>,
    raw_at: u64,
}

impl<'a, S: ReadAt> Cursor<'a, S> {
    fn open(src: &'a S, seg: &'a Segment, e: &Entry) -> Result<Self> {
        let df = e.df;
        if df as usize <= INLINE_MAX {
            return Ok(Self {
                src,
                seg,
                df,
                kind: Kind::Inline {
                    ids: e.inline,
                    pos: 0,
                },
                blocks_read: 0,
            });
        }
        let nb = (df as usize).div_ceil(BLOCK);
        let table_len = (nb * SKIP_ENTRY) as u64;
        // The entry parser already checked that the table and one byte per
        // block fit in the postings area.
        let table_at = seg.postings_at + e.off;
        let mut raw = vec![0u8; nb * SKIP_ENTRY];
        src.read_exact_at(&mut raw, table_at)?;
        let data_len = seg.e.postings_len - e.off - table_len;
        let mut skip = Vec::with_capacity(nb);
        for c in raw.as_chunks::<SKIP_ENTRY>().0 {
            skip.push((
                u32::from_le_bytes([c[0], c[1], c[2], c[3]]),
                u32::from_le_bytes([c[4], c[5], c[6], c[7]]),
            ));
        }
        for (i, &(last, start)) in skip.iter().enumerate() {
            let ok_last =
                last >= seg.e.doc_lo && last <= seg.e.doc_hi && (i == 0 || skip[i - 1].0 < last);
            let ok_start = u64::from(start) < data_len
                && if i == 0 {
                    start == 0
                } else {
                    let gap = u64::from(start).wrapping_sub(u64::from(skip[i - 1].1));
                    start > skip[i - 1].1 && gap <= block_max(BLOCK) as u64
                };
            if !ok_last || !ok_start {
                return corrupt("skip table is not ascending or points outside the list");
            }
        }
        Ok(Self {
            src,
            seg,
            df,
            kind: Kind::Disk(Box::new(Disk {
                skip,
                data_at: table_at + table_len,
                block: None,
                buf: Vec::new(),
                pos: 0,
                raw: Vec::new(),
                raw_at: 0,
            })),
            blocks_read: 0,
        })
    }

    fn load(&mut self, i: usize) -> Result<()> {
        let Kind::Disk(d) = &mut self.kind else {
            return Ok(());
        };
        let nb = d.skip.len();
        let n = if i + 1 == nb {
            self.df as usize - BLOCK * (nb - 1)
        } else {
            BLOCK
        };
        let at = d.data_at + u64::from(d.skip[i].1);
        let avail = self.seg.postings_end() - at;
        let want = if i + 1 < nb {
            u64::from(d.skip[i + 1].1 - d.skip[i].1)
        } else {
            (block_max(n) as u64).min(avail)
        };
        let covered = d.raw_at <= at && at + want <= d.raw_at + d.raw.len() as u64;
        if !covered {
            let sequential = d.block.is_some_and(|b| b + 1 == i);
            let window = if sequential {
                want.max(READ_AHEAD)
            } else {
                want
            }
            .min(avail);
            d.raw.resize(window as usize, 0);
            self.src.read_exact_at(&mut d.raw, at)?;
            d.raw_at = at;
        }
        let from = (at - d.raw_at) as usize;
        let Some(bytes) = d.raw.get(from..from + want as usize) else {
            return corrupt("posting block lies outside the postings area");
        };
        let next_min = if i == 0 {
            0
        } else {
            u64::from(d.skip[i - 1].0) + 1
        };
        d.buf.clear();
        let used = decode_block(bytes, n, next_min, &mut d.buf)?;
        if i + 1 < nb && used as u64 != want {
            return corrupt("posting block does not end where the next one starts");
        }
        let (first, last) = (d.buf[0], d.buf[d.buf.len() - 1]);
        if last != d.skip[i].0 || first < self.seg.e.doc_lo {
            return corrupt("posting block disagrees with its skip entry");
        }
        d.block = Some(i);
        d.pos = 0;
        self.blocks_read += 1;
        Ok(())
    }

    /// The first id that is at least `target`, or `None` at the end. Targets
    /// must not decrease between calls.
    pub fn seek(&mut self, target: u64) -> Result<Option<u32>> {
        loop {
            let next = match &mut self.kind {
                Kind::Inline { ids, pos } => {
                    let n = self.df as usize;
                    *pos += ids[*pos..n].partition_point(|&x| u64::from(x) < target);
                    return Ok((*pos < n).then(|| ids[*pos]));
                }
                Kind::Disk(d) => {
                    if d.block.is_some() && d.pos < d.buf.len() {
                        d.pos += d.buf[d.pos..].partition_point(|&x| u64::from(x) < target);
                        if d.pos < d.buf.len() {
                            return Ok(Some(d.buf[d.pos]));
                        }
                    }
                    let from = d.block.map_or(0, |b| b + 1);
                    let next = from + d.skip[from..].partition_point(|s| u64::from(s.0) < target);
                    if next >= d.skip.len() {
                        return Ok(None);
                    }
                    next
                }
            };
            self.load(next)?;
        }
    }

    /// Every id of the list, reading it in large steps.
    fn drain(mut self) -> Result<Vec<u32>> {
        let mut out = Vec::with_capacity((self.df as usize).min(1 << 16));
        let blocks = match &self.kind {
            Kind::Inline { ids, .. } => {
                out.extend_from_slice(&ids[..self.df as usize]);
                0
            }
            Kind::Disk(d) => d.skip.len(),
        };
        for i in 0..blocks {
            self.load(i)?;
            if let Kind::Disk(d) = &self.kind {
                out.extend_from_slice(&d.buf);
            }
        }
        Ok(out)
    }
}

/// Merges the lists of the cursors into one ascending list without
/// duplicates. Memory is the output plus one block per cursor.
fn merge_union<S: ReadAt>(mut cursors: Vec<Cursor<'_, S>>) -> Result<Vec<u32>> {
    if cursors.len() == 1 {
        return cursors.remove(0).drain();
    }
    let mut heap = BinaryHeap::new();
    for (k, c) in cursors.iter_mut().enumerate() {
        if let Some(id) = c.seek(0)? {
            heap.push(Reverse((id, k)));
        }
    }
    let mut out: Vec<u32> = Vec::new();
    while let Some(Reverse((id, k))) = heap.pop() {
        if out.last() != Some(&id) {
            out.push(id);
        }
        if let Some(next) = cursors[k].seek(u64::from(id) + 1)? {
            heap.push(Reverse((next, k)));
        }
    }
    Ok(out)
}

/// Reads an index file. See the crate documentation.
pub struct IndexReader<S: ReadAt> {
    src: S,
    meta: IndexMeta,
    segments: Vec<Segment>,
}

impl<S: ReadAt> IndexReader<S> {
    /// Opens an index: reads the footer, the directory and the sparse index
    /// of every segment, and checks them. The posting and dictionary bytes of
    /// a segment are checked the first time the segment is used.
    pub fn open(src: S) -> Result<Self> {
        let len = src.size()?;
        if len < FOOTER as u64 {
            return corrupt("file is too short to be an index");
        }
        let mut fb = [0u8; FOOTER];
        src.read_exact_at(&mut fb, len - FOOTER as u64)?;
        let (f, stored_crc) = decode_footer(&fb)?;
        let after_segments = len - FOOTER as u64;
        let dir_len = u64::from(f.segments) * DIR_ENTRY as u64;
        if dir_len > after_segments || f.directory_offset != after_segments - dir_len {
            return corrupt("directory does not fit the file");
        }
        let dir = read_checked(&src, len, f.directory_offset, dir_len, "directory")?;
        if footer_crc(&dir, &fb) != stored_crc {
            return corrupt("footer checksum mismatch");
        }

        let mut segments = Vec::with_capacity(f.segments as usize);
        let mut meta = IndexMeta {
            segments: f.segments,
            docs: f.docs,
            text_bytes: f.text_bytes,
            terms: f.terms,
            postings: f.postings,
            index_bytes: len,
            footer_bytes: dir_len + FOOTER as u64,
            ..IndexMeta::default()
        };
        let (mut at, mut prev_hi) = (0u64, None::<u32>);
        let (mut docs, mut terms, mut postings) = (0u64, 0u64, 0u64);
        for raw in dir.as_chunks::<DIR_ENTRY>().0 {
            let e = DirEntry::decode(raw)?;
            if e.offset != at {
                return corrupt("segments are not contiguous");
            }
            let Some(end) = e.end().filter(|&x| x <= f.directory_offset) else {
                return corrupt("segment extends past the directory");
            };
            Self::check_entry(&e, prev_hi)?;
            let sparse_at = e.offset + e.postings_len + e.dict_len;
            let raw_sparse = read_checked(&src, len, sparse_at, e.sparse_len, "sparse index")?;
            if crc32c::crc32c(&raw_sparse) != e.sparse_crc {
                return corrupt("sparse index checksum mismatch");
            }
            segments.push(Segment {
                e,
                postings_at: e.offset,
                dict_at: e.offset + e.postings_len,
                sparse: Self::parse_sparse(&raw_sparse, &e)?,
                verified: AtomicBool::new(false),
            });
            meta.postings_bytes += e.postings_len;
            meta.dictionary_bytes += e.dict_len;
            meta.sparse_bytes += e.sparse_len;
            docs += u64::from(e.docs);
            terms += e.terms;
            postings += e.postings;
            prev_hi = Some(e.doc_hi);
            at = end;
        }
        if at != f.directory_offset {
            return corrupt("segments do not fill the space before the directory");
        }
        if docs != f.docs || terms != f.terms || postings != f.postings {
            return corrupt("footer totals disagree with the directory");
        }
        meta.docs = docs;
        Ok(Self {
            src,
            meta,
            segments,
        })
    }

    fn check_entry(e: &DirEntry, prev_hi: Option<u32>) -> Result<()> {
        if e.docs == 0 || e.doc_lo > e.doc_hi {
            return corrupt("segment has no documents or an inverted id range");
        }
        if prev_hi.is_some_and(|h| e.doc_lo <= h) {
            return corrupt("segment document ranges overlap or are out of order");
        }
        if u64::from(e.docs) > u64::from(e.doc_hi) - u64::from(e.doc_lo) + 1 {
            return corrupt("segment has more documents than its id range");
        }
        if u64::from(e.dict_blocks) != e.terms.div_ceil(u64::from(TERMS_PER_BLOCK)) {
            return corrupt("dictionary block count does not match the term count");
        }
        // Every term costs at least four dictionary bytes and every block at
        // least ten sparse bytes, so counts that the sizes cannot back are
        // rejected before anything is allocated from them.
        let dict_min = e.terms.checked_mul(4);
        let sparse_min = u64::from(e.dict_blocks) * 10;
        if dict_min.is_none_or(|m| m > e.dict_len) || sparse_min > e.sparse_len {
            return corrupt("segment sizes cannot hold its terms");
        }
        if e.terms == 0 && (e.postings != 0 || e.postings_len != 0 || e.dict_len != 0) {
            return corrupt("segment without terms has content");
        }
        if e.postings < e.terms {
            return corrupt("segment has fewer postings than terms");
        }
        Ok(())
    }

    fn parse_sparse(b: &[u8], e: &DirEntry) -> Result<Sparse> {
        let n = e.dict_blocks as usize;
        let mut s = Sparse {
            terms: Vec::new(),
            ends: Vec::with_capacity(n),
            offs: Vec::with_capacity(n),
        };
        let mut p = 0usize;
        for i in 0..n {
            let l = get_varint(b, &mut p)?;
            if l == 0 || l > MAX_TOKEN_LEN as u64 {
                return corrupt("sparse index term has an impossible length");
            }
            let l = l as usize;
            let (Some(term), Some(off)) = (b.get(p..p + l), b.get(p + l..p + l + 8)) else {
                return corrupt("sparse index entry runs past the end");
            };
            let mut off8 = [0u8; 8];
            off8.copy_from_slice(off);
            let off = u64::from_le_bytes(off8);
            let bad_off = off >= e.dict_len
                || if i == 0 {
                    off != 0
                } else {
                    off <= s.offs[i - 1]
                };
            if bad_off {
                return corrupt("sparse index offsets are not ascending inside the dictionary");
            }
            if i > 0 && s.term(i - 1) >= term {
                return corrupt("sparse index terms are not ascending");
            }
            s.terms.extend_from_slice(term);
            s.ends.push(s.terms.len());
            s.offs.push(off);
            p += l + 8;
        }
        if p != b.len() {
            return corrupt("sparse index has bytes after its last entry");
        }
        Ok(s)
    }

    /// Number of documents added to the index.
    pub fn doc_count(&self) -> u64 {
        self.meta.docs
    }

    /// Counts describing the index.
    pub fn meta(&self) -> &IndexMeta {
        &self.meta
    }

    /// Checks the checksums of every segment now instead of on first use.
    pub fn verify(&self) -> Result<()> {
        self.segments.iter().try_for_each(|s| s.verify(&self.src))
    }

    /// Documents that contain `token`, ascending. The token must be in the
    /// form the tokeniser produces (lowercase, at most 64 bytes); anything
    /// else matches nothing.
    pub fn term(&self, token: impl AsRef<[u8]>) -> Result<DocIds> {
        let token = token.as_ref();
        let mut out = Vec::new();
        if token.is_empty() || token.len() > MAX_TOKEN_LEN {
            return Ok(DocIds::default());
        }
        for seg in &self.segments {
            seg.verify(&self.src)?;
            if let Some(e) = seg.lookup(&self.src, token)? {
                out.extend(Cursor::open(&self.src, seg, &e)?.drain()?);
            }
        }
        Ok(DocIds::from_ascending(out))
    }

    /// Documents that contain any term starting with `prefix`, ascending.
    ///
    /// If more than `limit_terms` distinct terms match, nothing is read and
    /// the call fails with `InvalidArgument`: a prefix that broad is the
    /// caller's mistake, and the limit is what keeps the memory of this call
    /// bounded. Memory is the output, the matching dictionary entries and one
    /// posting block and skip table per matching term in one segment.
    pub fn prefix(&self, prefix: impl AsRef<[u8]>, limit_terms: usize) -> Result<DocIds> {
        let prefix = prefix.as_ref();
        if prefix.len() > MAX_TOKEN_LEN {
            return Ok(DocIds::default());
        }
        let mut seen: BTreeSet<Box<[u8]>> = BTreeSet::new();
        let mut per_segment: Vec<Vec<Entry>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            seg.verify(&self.src)?;
            let mut found = Vec::new();
            let mut over = false;
            seg.walk_prefix(&self.src, prefix, &mut |t, e| {
                if !seen.contains(t) {
                    if seen.len() >= limit_terms {
                        over = true;
                        return false;
                    }
                    seen.insert(t.into());
                }
                found.push(e);
                true
            })?;
            if over {
                return Err(Error::invalid_argument(format!(
                    "prefix matches more than {limit_terms} terms"
                )));
            }
            per_segment.push(found);
        }
        let mut out = Vec::new();
        for (seg, entries) in self.segments.iter().zip(&per_segment) {
            if entries.is_empty() {
                continue;
            }
            let cursors = entries
                .iter()
                .map(|e| Cursor::open(&self.src, seg, e))
                .collect::<Result<Vec<_>>>()?;
            out.extend(merge_union(cursors)?);
        }
        Ok(DocIds::from_ascending(out))
    }

    /// Documents that contain every one of `tokens`, ascending. The lists
    /// are intersected with the skip data of their posting lists, so a rare
    /// token next to a common one reads only the blocks it needs.
    pub fn intersect<T: AsRef<[u8]>>(&self, tokens: &[T]) -> Result<DocIds> {
        self.intersect_counting(tokens).map(|(ids, _)| ids)
    }

    /// As [`IndexReader::intersect`], also returning the posting blocks read.
    pub(crate) fn intersect_counting<T: AsRef<[u8]>>(&self, tokens: &[T]) -> Result<(DocIds, u64)> {
        if tokens.is_empty() {
            return Err(Error::invalid_argument("no tokens to intersect"));
        }
        let mut sorted: Vec<&[u8]> = tokens.iter().map(AsRef::as_ref).collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut out = Vec::new();
        let mut blocks = 0;
        if sorted
            .iter()
            .any(|t| t.is_empty() || t.len() > MAX_TOKEN_LEN)
        {
            return Ok((DocIds::default(), 0));
        }
        'segments: for seg in &self.segments {
            seg.verify(&self.src)?;
            let mut cursors = Vec::with_capacity(sorted.len());
            for t in &sorted {
                match seg.lookup(&self.src, t)? {
                    Some(e) => cursors.push(Cursor::open(&self.src, seg, &e)?),
                    None => continue 'segments,
                }
            }
            out.extend(leapfrog(&mut cursors)?);
            blocks += cursors.iter().map(|c| c.blocks_read).sum::<u64>();
        }
        Ok((DocIds::from_ascending(out), blocks))
    }
}

/// Ids present in every cursor. It walks the shortest list and asks the
/// others to skip to each candidate.
fn leapfrog<S: ReadAt>(cs: &mut [Cursor<'_, S>]) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    cs.sort_by_key(|c| c.df);
    let Some(mut cand) = cs[0].seek(0)? else {
        return Ok(out);
    };
    'outer: loop {
        for i in 1..cs.len() {
            match cs[i].seek(u64::from(cand))? {
                None => break 'outer,
                Some(v) if v != cand => {
                    match cs[0].seek(u64::from(v))? {
                        None => break 'outer,
                        Some(x) => cand = x,
                    }
                    continue 'outer;
                }
                Some(_) => {}
            }
        }
        out.push(cand);
        match cs[0].seek(u64::from(cand) + 1)? {
            Some(x) => cand = x,
            None => break,
        }
    }
    Ok(out)
}
