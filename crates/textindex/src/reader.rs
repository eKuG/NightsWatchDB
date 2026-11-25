//! Reading and searching an index file.
//!
//! The reader keeps only the directory and the sparse index of each segment
//! in memory. Every search is positioned reads: one block of the dictionary,
//! then the posting list blocks it needs. Apart from the verdict of each
//! segment's checksum, nothing is shared between calls, so one reader serves
//! any number of threads.
//!
//! Every number read from the file is checked before it is used: lengths
//! against the file's length, offsets against the area they point into, counts
//! against the bytes that would have to back them. A file that fails a check
//! is [`Error::Corrupt`]. On top of the structural checks each segment has a
//! checksum, verified the first time the segment is used.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::Mutex;
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
use crate::tokenizer::{LONG_MARK, MAX_KEY_LEN, MAX_TOKEN_LEN, long_aware_key};

/// Bytes a cursor reads ahead when it walks a posting list block by block,
/// but only when it is the only list the search has open. A search with
/// several lists open reads exactly the block it needs, so what a search
/// holds does not multiply by this.
const READ_AHEAD: u64 = 8 * 1024;

/// Skip entries a cursor holds at a time (one page). A cursor reads the
/// skip table of a long list one page at a time, so its memory does not
/// depend on the length of the list.
const PAGE: usize = 64;

/// What a search for a token or a set of tokens found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Matches {
    /// The documents, ascending.
    pub ids: DocIds,
    /// True if `ids` are exactly the documents that hold the token (or every
    /// token). False if a token was longer than 64 bytes, which is listed
    /// under its first 64 bytes only: `ids` are then candidates, and the
    /// caller confirms each against its text with
    /// [`contains_token`](crate::contains_token).
    pub exact: bool,
}

/// Whether `key` can be a key of the dictionary: an ordinary token of 1 to 64
/// bytes, or a zero byte and 64 more.
fn valid_key(key: &[u8]) -> bool {
    match key.first() {
        Some(&LONG_MARK) => key.len() == MAX_KEY_LEN,
        Some(_) => key.len() <= MAX_TOKEN_LEN,
        None => false,
    }
}

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
    /// Set once the checksum matched, so later searches skip the lock.
    verified: AtomicBool,
    /// The outcome of the first verification. A mismatch is kept, so a
    /// damaged segment is read whole once, not by every search; a thread that
    /// arrives during the first verification waits on the lock for it.
    outcome: Mutex<Outcome>,
}

enum Outcome {
    Unknown,
    Good,
    /// The message of the checksum failure.
    Damaged(String),
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

    /// Checks the checksum of postings and dictionary, once per reader. The
    /// verdict (good or damaged) is remembered. A failed read is not a
    /// verdict about the file, so it is reported and the next call tries
    /// again.
    fn verify<S: ReadAt>(&self, src: &S) -> Result<()> {
        if self.verified.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut outcome = self.outcome.lock().unwrap_or_else(|e| e.into_inner());
        match &*outcome {
            Outcome::Good => return Ok(()),
            Outcome::Damaged(m) => return Err(Error::corrupt(m)),
            Outcome::Unknown => {}
        }
        match self.checksum(src) {
            Ok(()) => {
                *outcome = Outcome::Good;
                self.verified.store(true, Ordering::Release);
                Ok(())
            }
            Err(Error::Corrupt(m)) => {
                *outcome = Outcome::Damaged(m.clone());
                Err(Error::Corrupt(m))
            }
            Err(e) => Err(e),
        }
    }

    fn checksum<S: ReadAt>(&self, src: &S) -> Result<()> {
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
    ///
    /// `prev_end` is the least offset the next posting list may have: the
    /// builder writes lists one after another in term order, so a list that
    /// starts before the end of the previous one (counting only the bytes it
    /// cannot do without) is damage. This is what stops many entries from
    /// pointing at the same list. Pass the same variable for consecutive
    /// blocks of one walk.
    fn walk_block<S: ReadAt>(
        &self,
        src: &S,
        i: usize,
        prev_end: &mut u64,
        f: &mut dyn FnMut(&[u8], Entry) -> bool,
    ) -> Result<bool> {
        let bytes = self.read_block(src, i)?;
        let e = &self.e;
        let mut cur: Vec<u8> = Vec::with_capacity(MAX_KEY_LEN);
        let mut p = 0usize;
        for k in 0..self.terms_in_block(i) {
            let shared = get_varint(&bytes, &mut p)?;
            let suffix = get_varint(&bytes, &mut p)?;
            if shared > cur.len() as u64 {
                return corrupt("front-coded term shares more than the previous term has");
            }
            if suffix == 0 || shared + suffix > MAX_KEY_LEN as u64 {
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
            if !valid_key(&cur) {
                return corrupt("term has an impossible length");
            }
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
                if entry.off < *prev_end {
                    return corrupt("posting lists overlap or are out of order");
                }
                *prev_end = entry.off + need;
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
        self.walk_block(src, at - 1, &mut 0, &mut |t, e| {
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
        let mut prev_end = 0;
        for i in first..self.sparse.len() {
            let head = self.sparse.term(i);
            if i > first && head > prefix && !head.starts_with(prefix) {
                break;
            }
            let more = self.walk_block(src, i, &mut prev_end, &mut |t, e| {
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
///
/// What it holds does not depend on the length of the list: one page of the
/// skip table (at most `PAGE + 2` entries, 528 bytes), the decoded block
/// (512 bytes) and the bytes of the block (at most 513), plus, when it is
/// the only open list of a search, a read-ahead buffer.
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

/// The skip table is read in pages of `PAGE` entries, each with the entry
/// before it and the one after it, because decoding a block needs the last id
/// of the block before and the start of the block after.
struct Disk {
    /// Blocks in the list.
    nb: usize,
    /// Where the skip table starts in the file.
    table_at: u64,
    /// Bytes of the blocks that follow the table, up to the end of the area.
    data_len: u64,
    data_at: u64,
    /// Bytes to read at once when the next block is the one after the last.
    ahead: u64,
    /// The page held, and the index of the first entry of `win`.
    page: Option<usize>,
    base: usize,
    /// (last id, start relative to the end of the skip table) per block.
    win: [(u32, u32); PAGE + 2],
    block: Option<usize>,
    buf: Vec<u32>,
    pos: usize,
    raw: Vec<u8>,
    raw_at: u64,
}

impl Disk {
    /// Reads and checks the entries of `page`: the same checks as for a whole
    /// table (ids ascend inside the segment's range, starts ascend by no more
    /// than a full block and stay inside the list), applied to what is read.
    fn load_page<S: ReadAt>(&mut self, src: &S, seg: &Segment, page: usize) -> Result<()> {
        if self.page == Some(page) {
            return Ok(());
        }
        let start = (page * PAGE).saturating_sub(1);
        let end = ((page + 1) * PAGE + 1).min(self.nb);
        let n = end - start;
        let mut raw = [0u8; (PAGE + 2) * SKIP_ENTRY];
        let raw = &mut raw[..n * SKIP_ENTRY];
        src.read_exact_at(raw, self.table_at + (start * SKIP_ENTRY) as u64)?;
        for (j, c) in raw.as_chunks::<SKIP_ENTRY>().0.iter().enumerate() {
            let last = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            let st = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
            let ok_last = last >= seg.e.doc_lo
                && last <= seg.e.doc_hi
                && (j == 0 || self.win[j - 1].0 < last);
            let ok_start = u64::from(st) < self.data_len
                && if start + j == 0 {
                    st == 0
                } else if j == 0 {
                    true
                } else {
                    let prev = self.win[j - 1].1;
                    st > prev && u64::from(st - prev) <= block_max(BLOCK) as u64
                };
            if !ok_last || !ok_start {
                self.page = None;
                return corrupt("skip table is not ascending or points outside the list");
            }
            self.win[j] = (last, st);
        }
        self.page = Some(page);
        self.base = start;
        Ok(())
    }

    /// The last id of the final block of `page`, without loading the page
    /// unless it is the one held.
    fn page_last<S: ReadAt>(&self, src: &S, page: usize) -> Result<u32> {
        let idx = ((page + 1) * PAGE).min(self.nb) - 1;
        if self.page == Some(page) {
            return Ok(self.win[idx - self.base].0);
        }
        let mut b = [0u8; 4];
        src.read_exact_at(&mut b, self.table_at + (idx * SKIP_ENTRY) as u64)?;
        Ok(u32::from_le_bytes(b))
    }

    /// The first block at or after `from` whose last id is at least
    /// `target`, found by binary search over pages and then inside one page.
    fn find_block<S: ReadAt>(
        &mut self,
        src: &S,
        seg: &Segment,
        from: usize,
        target: u64,
    ) -> Result<Option<usize>> {
        if from >= self.nb {
            return Ok(None);
        }
        let pages = self.nb.div_ceil(PAGE);
        let first = from / PAGE;
        let page = if u64::from(self.page_last(src, first)?) >= target {
            first
        } else {
            let (mut lo, mut hi) = (first + 1, pages);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if u64::from(self.page_last(src, mid)?) < target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo >= pages {
                return Ok(None);
            }
            lo
        };
        self.load_page(src, seg, page)?;
        let from = from.max(page * PAGE);
        let to = ((page + 1) * PAGE).min(self.nb);
        let entries = &self.win[from - self.base..to - self.base];
        let k = entries.partition_point(|s| u64::from(s.0) < target);
        if k == entries.len() {
            return corrupt("skip table changed while it was searched");
        }
        Ok(Some(from + k))
    }
}

impl<'a, S: ReadAt> Cursor<'a, S> {
    /// Opens the list of `e`. `read_ahead` is the bytes to read at once when
    /// the cursor walks blocks in order; pass 0 when other cursors are open.
    fn open(src: &'a S, seg: &'a Segment, e: &Entry, read_ahead: u64) -> Result<Self> {
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
        // block fit in the postings area. Nothing of the table is read yet.
        let table_at = seg.postings_at + e.off;
        Ok(Self {
            src,
            seg,
            df,
            kind: Kind::Disk(Box::new(Disk {
                nb,
                table_at,
                data_len: seg.e.postings_len - e.off - table_len,
                data_at: table_at + table_len,
                ahead: read_ahead,
                page: None,
                base: 0,
                win: [(0, 0); PAGE + 2],
                block: None,
                buf: Vec::with_capacity(BLOCK),
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
        d.load_page(self.src, self.seg, i / PAGE)?;
        let nb = d.nb;
        let (this, next, before) = (
            d.win[i - d.base],
            d.win.get(i + 1 - d.base).copied().unwrap_or_default(),
            d.win[i.saturating_sub(1) - d.base],
        );
        let n = if i + 1 == nb {
            self.df as usize - BLOCK * (nb - 1)
        } else {
            BLOCK
        };
        let at = d.data_at + u64::from(this.1);
        let avail = self.seg.postings_end() - at;
        let want = if i + 1 < nb {
            u64::from(next.1 - this.1)
        } else {
            (block_max(n) as u64).min(avail)
        };
        let next_min = if i == 0 { 0 } else { u64::from(before.0) + 1 };
        let want_last = this.0;
        let covered = d.raw_at <= at && at + want <= d.raw_at + d.raw.len() as u64;
        if !covered {
            let sequential = d.block.is_some_and(|b| b + 1 == i);
            let window = if sequential { want.max(d.ahead) } else { want }.min(avail);
            d.raw.resize(window as usize, 0);
            self.src.read_exact_at(&mut d.raw, at)?;
            d.raw_at = at;
        }
        let from = (at - d.raw_at) as usize;
        let Some(bytes) = d.raw.get(from..from + want as usize) else {
            return corrupt("posting block lies outside the postings area");
        };
        d.buf.clear();
        let used = decode_block(bytes, n, next_min, &mut d.buf)?;
        if i + 1 < nb && used as u64 != want {
            return corrupt("posting block does not end where the next one starts");
        }
        let (first, last) = (d.buf[0], d.buf[d.buf.len() - 1]);
        if last != want_last || first < self.seg.e.doc_lo {
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
                    match d.find_block(self.src, self.seg, from, target)? {
                        Some(next) => next,
                        None => return Ok(None),
                    }
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
            Kind::Disk(d) => d.nb,
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
                outcome: Mutex::new(Outcome::Unknown),
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
            if l == 0 || l > MAX_KEY_LEN as u64 {
                return corrupt("sparse index term has an impossible length");
            }
            let l = l as usize;
            let (Some(term), Some(off)) = (b.get(p..p + l), b.get(p + l..p + l + 8)) else {
                return corrupt("sparse index entry runs past the end");
            };
            if !valid_key(term) {
                return corrupt("sparse index term has an impossible length");
            }
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

    /// Documents that contain `token`, ascending.
    ///
    /// A token of 1 to 64 bytes must be in the form the tokeniser produces
    /// (lowercase); the answer is then exact. A token of more than 64 bytes is
    /// looked up under its first 64 bytes (case folded), and the answer is a
    /// list of candidates, flagged `exact: false`: another token that shares
    /// those 64 bytes is listed too, so the caller confirms each candidate
    /// with [`contains_token`](crate::contains_token). An empty token, or one
    /// that starts with a byte that no token can start with, matches nothing.
    pub fn term(&self, token: impl AsRef<[u8]>) -> Result<Matches> {
        let token = token.as_ref();
        let exact = token.len() <= MAX_TOKEN_LEN;
        let mut out = Vec::new();
        if token.is_empty() || (exact && token[0] == LONG_MARK) {
            return Ok(Matches {
                ids: DocIds::default(),
                exact,
            });
        }
        let mut buf = [0u8; MAX_KEY_LEN];
        let key: &[u8] = if exact {
            token
        } else {
            long_aware_key(token, &mut buf)
        };
        for seg in &self.segments {
            seg.verify(&self.src)?;
            if let Some(e) = seg.lookup(&self.src, key)? {
                // one list is open, so it may read ahead
                out.extend(Cursor::open(&self.src, seg, &e, READ_AHEAD)?.drain()?);
            }
        }
        Ok(Matches {
            ids: DocIds::from_ascending(out),
            exact,
        })
    }

    /// Documents that contain any token starting with `prefix`, ascending,
    /// tokens longer than 64 bytes included. The answer is exact. A prefix of
    /// more than 64 bytes is `InvalidArgument`; one that starts with a byte
    /// that no token can start with matches nothing.
    ///
    /// If more than `limit_terms` distinct terms match, nothing is read and
    /// the call fails with `InvalidArgument`: a prefix that broad is the
    /// caller's mistake, and the limit is what keeps the memory of this call
    /// bounded. (A token longer than 64 bytes counts as a term here, once per
    /// distinct first 64 bytes.) Memory is the output, the matching dictionary
    /// entries and, per matching term in one segment, what an open list holds
    /// (see the README): the length of the lists does not matter.
    pub fn prefix(&self, prefix: impl AsRef<[u8]>, limit_terms: usize) -> Result<DocIds> {
        let prefix = prefix.as_ref();
        if prefix.len() > MAX_TOKEN_LEN {
            return Err(Error::invalid_argument("a prefix is at most 64 bytes long"));
        }
        if prefix.first() == Some(&LONG_MARK) {
            return Ok(DocIds::default());
        }
        // The keys of tokens longer than 64 bytes live behind a zero byte, so
        // they are found by walking that namespace with the same prefix.
        let mut long_prefix = Vec::with_capacity(prefix.len() + 1);
        long_prefix.push(LONG_MARK);
        long_prefix.extend_from_slice(prefix);
        let mut seen: BTreeSet<Box<[u8]>> = BTreeSet::new();
        let mut per_segment: Vec<Vec<Entry>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            seg.verify(&self.src)?;
            let mut found = Vec::new();
            let mut over = false;
            for (walk, long) in [(&long_prefix[..], true), (prefix, false)] {
                seg.walk_prefix(&self.src, walk, &mut |t, e| {
                    // an empty prefix also matches the zero-byte keys; they
                    // were taken by the first walk
                    if !long && t[0] == LONG_MARK {
                        return true;
                    }
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
            }
            per_segment.push(found);
        }
        let mut out = Vec::new();
        for (seg, entries) in self.segments.iter().zip(&per_segment) {
            if entries.is_empty() {
                continue;
            }
            // Several lists are open at once: none may read ahead.
            let ahead = if entries.len() == 1 { READ_AHEAD } else { 0 };
            let cursors = entries
                .iter()
                .map(|e| Cursor::open(&self.src, seg, e, ahead))
                .collect::<Result<Vec<_>>>()?;
            out.extend(merge_union(cursors)?);
        }
        Ok(DocIds::from_ascending(out))
    }

    /// Documents that contain every one of `tokens`, ascending. The lists
    /// are intersected with the skip data of their posting lists, so a rare
    /// token next to a common one reads only the blocks it needs. The answer
    /// is exact unless a token is longer than 64 bytes (see
    /// [`IndexReader::term`]); then it is the candidates, flagged
    /// `exact: false`. An empty token matches nothing.
    pub fn intersect<T: AsRef<[u8]>>(&self, tokens: &[T]) -> Result<Matches> {
        self.intersect_counting(tokens).map(|(m, _)| m)
    }

    /// As [`IndexReader::intersect`], also returning the posting blocks read.
    pub(crate) fn intersect_counting<T: AsRef<[u8]>>(
        &self,
        tokens: &[T],
    ) -> Result<(Matches, u64)> {
        if tokens.is_empty() {
            return Err(Error::invalid_argument("no tokens to intersect"));
        }
        let exact = tokens.iter().all(|t| t.as_ref().len() <= MAX_TOKEN_LEN);
        let none = || {
            (
                Matches {
                    ids: DocIds::default(),
                    exact,
                },
                0,
            )
        };
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for t in tokens.iter().map(AsRef::as_ref) {
            if t.is_empty() || (t.len() <= MAX_TOKEN_LEN && t[0] == LONG_MARK) {
                return Ok(none());
            }
            let mut buf = [0u8; MAX_KEY_LEN];
            keys.push(long_aware_key(t, &mut buf).to_vec());
        }
        keys.sort_unstable();
        keys.dedup();
        // One list is open only when there is one key; else none reads ahead.
        let ahead = if keys.len() == 1 { READ_AHEAD } else { 0 };
        let mut out = Vec::new();
        let mut blocks = 0;
        'segments: for seg in &self.segments {
            seg.verify(&self.src)?;
            let mut cursors = Vec::with_capacity(keys.len());
            for k in &keys {
                match seg.lookup(&self.src, k)? {
                    Some(e) => cursors.push(Cursor::open(&self.src, seg, &e, ahead)?),
                    None => continue 'segments,
                }
            }
            out.extend(leapfrog(&mut cursors)?);
            blocks += cursors.iter().map(|c| c.blocks_read).sum::<u64>();
        }
        Ok((
            Matches {
                ids: DocIds::from_ascending(out),
                exact,
            },
            blocks,
        ))
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
