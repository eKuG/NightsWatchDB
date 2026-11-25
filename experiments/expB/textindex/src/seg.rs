//! One index segment file: writer, reader, posting cursor. Layout in README.md.
use crate::codec::*;
use crate::util::*;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const MAGIC: &[u8; 4] = b"TXIX";
pub const VERSION: u8 = 1;
pub const BLOCK_TERMS: usize = 32;
pub const INLINE_MAX: usize = 8;
const FOOTER: usize = 60;

#[derive(Default, Clone, Debug)]
pub struct SegStats {
    pub terms: u64,
    pub postings: u64,
    pub postings_bytes: u64,
    pub dict_bytes: u64,
    pub sparse_bytes: u64,
    pub footer_bytes: u64,
}
impl SegStats {
    pub fn total(&self) -> u64 {
        self.postings_bytes + self.dict_bytes + self.sparse_bytes + self.footer_bytes
    }
    pub fn add(&mut self, o: &SegStats) {
        self.terms += o.terms;
        self.postings += o.postings;
        self.postings_bytes += o.postings_bytes;
        self.dict_bytes += o.dict_bytes;
        self.sparse_bytes += o.sparse_bytes;
        self.footer_bytes += o.footer_bytes;
    }
}

/// Appends a front-coded term: shared prefix length with `prev`, suffix length, suffix.
pub fn put_term(b: &mut Vec<u8>, prev: &[u8], term: &[u8]) {
    let s = prev.iter().zip(term).take_while(|(a, b)| a == b).count();
    put_varint(b, s as u64);
    put_varint(b, (term.len() - s) as u64);
    b.extend_from_slice(&term[s..]);
}

/// Reads a front-coded term into `cur`, which holds the previous term on entry.
pub fn get_term(b: &[u8], p: &mut usize, cur: &mut Vec<u8>) {
    let s = get_varint(b, p) as usize;
    let l = get_varint(b, p) as usize;
    cur.truncate(s);
    cur.extend_from_slice(&b[*p..*p + l]);
    *p += l;
}

/// Writes a segment file from terms in ascending byte order. `lists` must be ascending and non-empty.
pub fn write_segment<'a>(path: &Path, doc_lo: u32, doc_hi: u32, terms: &mut dyn Iterator<Item = (&'a [u8], &'a [u32])>) -> R<SegStats> {
    let mut w = std::io::BufWriter::with_capacity(1 << 20, File::create(path)?);
    let (mut st, mut dict, mut sparse) = (SegStats::default(), vec![], vec![]);
    let (mut prev, mut tmp, mut nblocks): (Vec<u8>, Vec<u8>, u64) = (vec![], vec![], 0);
    for (term, ids) in terms {
        if st.terms % BLOCK_TERMS as u64 == 0 {
            prev.clear();
            put_varint(&mut sparse, term.len() as u64);
            sparse.extend_from_slice(term);
            sparse.extend_from_slice(&(dict.len() as u64).to_le_bytes());
            nblocks += 1;
        }
        put_term(&mut dict, &prev, term);
        prev.clear();
        prev.extend_from_slice(term);
        put_varint(&mut dict, ids.len() as u64);
        if ids.len() <= INLINE_MAX {
            let mut last = u32::MAX;
            for &id in ids {
                put_varint(&mut dict, gap(id, last) as u64);
                last = id;
            }
        } else {
            put_varint(&mut dict, st.postings_bytes);
            tmp.clear();
            encode_list(ids, &mut tmp);
            w.write_all(&tmp)?;
            st.postings_bytes += tmp.len() as u64;
        }
        st.terms += 1;
        st.postings += ids.len() as u64;
    }
    st.dict_bytes = dict.len() as u64;
    st.sparse_bytes = sparse.len() as u64;
    st.footer_bytes = FOOTER as u64;
    let dict_off = st.postings_bytes;
    let sparse_off = dict_off + st.dict_bytes;
    w.write_all(&dict)?;
    w.write_all(&sparse)?;
    let mut f = Vec::with_capacity(FOOTER);
    for v in [dict_off, sparse_off, st.terms, nblocks, st.postings] {
        f.extend_from_slice(&v.to_le_bytes());
    }
    for v in [doc_lo, doc_hi, BLOCK_TERMS as u32] {
        f.extend_from_slice(&v.to_le_bytes());
    }
    f.extend_from_slice(&[VERSION, 0, 0, 0]);
    f.extend_from_slice(MAGIC);
    debug_assert_eq!(f.len(), FOOTER);
    w.write_all(&f)?;
    w.flush()?;
    Ok(st)
}

/// A dictionary entry. Lists of up to 8 ids are inline; longer ones are at `off` in the postings area.
#[derive(Clone, Debug)]
pub struct Entry {
    pub df: u32,
    inline: [u32; INLINE_MAX],
    off: u64,
}

pub struct Segment {
    file: File,
    pub doc_lo: u32,
    pub doc_hi: u32,
    dict_off: u64,
    sparse_off: u64,
    sparse: Vec<(Vec<u8>, u64)>,
}

impl Segment {
    pub fn open(path: &Path) -> R<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len < FOOTER {
            return Err(format!("{}: too short", path.display()).into());
        }
        let mut f = [0u8; FOOTER];
        file.read_exact_at(&mut f, (len - FOOTER) as u64)?;
        if &f[56..] != MAGIC || f[52] != VERSION {
            return Err(format!("{}: bad magic or version", path.display()).into());
        }
        let (dict_off, sparse_off, nblocks) = (le64(&f, 0), le64(&f, 8), le64(&f, 24));
        let mut sb = vec![0u8; len - FOOTER - sparse_off as usize];
        file.read_exact_at(&mut sb, sparse_off)?;
        let (mut sparse, mut p) = (vec![], 0);
        for _ in 0..nblocks {
            let l = get_varint(&sb, &mut p) as usize;
            sparse.push((sb[p..p + l].to_vec(), le64(&sb, p + l)));
            p += l + 8;
        }
        Ok(Self { file, doc_lo: le32(&f, 40), doc_hi: le32(&f, 44), dict_off, sparse_off, sparse })
    }

    fn block_bytes(&self, i: usize) -> R<Vec<u8>> {
        let a = self.sparse[i].1;
        let b = self.sparse.get(i + 1).map_or(self.sparse_off - self.dict_off, |x| x.1);
        let mut buf = vec![0u8; (b - a) as usize];
        self.file.read_exact_at(&mut buf, self.dict_off + a)?;
        Ok(buf)
    }

    /// Decodes the entries of block `i`; `f` returns false to stop.
    fn walk_block(&self, i: usize, f: &mut dyn FnMut(&[u8], Entry) -> bool) -> R<bool> {
        let b = self.block_bytes(i)?;
        let (mut p, mut cur) = (0, vec![]);
        while p < b.len() {
            get_term(&b, &mut p, &mut cur);
            let df = get_varint(&b, &mut p) as u32;
            let mut e = Entry { df, inline: [0; INLINE_MAX], off: 0 };
            if df as usize <= INLINE_MAX {
                let mut last = u32::MAX;
                for k in 0..df as usize {
                    last = ungap(get_varint(&b, &mut p) as u32, last);
                    e.inline[k] = last;
                }
            } else {
                e.off = get_varint(&b, &mut p);
            }
            if !f(&cur, e) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn lookup(&self, term: &[u8]) -> R<Option<Entry>> {
        let bi = self.sparse.partition_point(|(t, _)| t.as_slice() <= term);
        if bi == 0 {
            return Ok(None);
        }
        let mut found = None;
        self.walk_block(bi - 1, &mut |t, e| {
            if t == term {
                found = Some(e);
            }
            t < term
        })?;
        Ok(found)
    }

    /// Entries of every term starting with `prefix`, in term order.
    pub fn prefix(&self, prefix: &[u8]) -> R<Vec<Entry>> {
        let mut out = vec![];
        let first = self.sparse.partition_point(|(t, _)| t.as_slice() < prefix).saturating_sub(1);
        for i in first..self.sparse.len() {
            let cont = self.walk_block(i, &mut |t, e| {
                if t.starts_with(prefix) {
                    out.push(e);
                    true
                } else {
                    t < prefix
                }
            })?;
            if !cont {
                break;
            }
        }
        Ok(out)
    }

    /// Every id of the list, in one read.
    pub fn list(&self, e: &Entry) -> R<Vec<u32>> {
        let df = e.df as usize;
        if df <= INLINE_MAX {
            return Ok(e.inline[..df].to_vec());
        }
        let nb = df.div_ceil(BLOCK);
        let mut skip = vec![0u8; SKIP * nb];
        self.file.read_exact_at(&mut skip, e.off)?;
        let (last_start, last_n) = (le32(&skip, SKIP * (nb - 1) + 4) as usize, df - BLOCK * (nb - 1));
        let mut data = vec![0u8; last_start + block_max(last_n)];
        let at = e.off + (SKIP * nb) as u64;
        let n = data.len().min((self.dict_off - at) as usize);
        self.file.read_exact_at(&mut data[..n], at)?;
        let (mut out, mut prev) = (Vec::with_capacity(df), u32::MAX);
        for b in 0..nb {
            let s = le32(&skip, SKIP * b + 4) as usize;
            decode_block(&data[s..], if b + 1 == nb { last_n } else { BLOCK }, prev, &mut out);
            prev = le32(&skip, SKIP * b);
        }
        Ok(out)
    }

    pub fn cursor(&self, e: &Entry) -> R<Cursor<'_>> {
        let df = e.df as usize;
        if df <= INLINE_MAX {
            return Ok(Cursor { seg: self, df: e.df, kind: CK::Mem(e.inline[..df].to_vec(), 0), blocks_read: 0 });
        }
        let nb = df.div_ceil(BLOCK);
        let mut sk = vec![0u8; SKIP * nb];
        self.file.read_exact_at(&mut sk, e.off)?;
        let skip = (0..nb).map(|b| (le32(&sk, SKIP * b), le32(&sk, SKIP * b + 4))).collect();
        Ok(Cursor { seg: self, df: e.df, kind: CK::Disk { skip, data_at: e.off + (SKIP * nb) as u64, blk: usize::MAX, buf: vec![], pos: 0 }, blocks_read: 0 })
    }
}

enum CK {
    Mem(Vec<u32>, usize),
    Disk { skip: Vec<(u32, u32)>, data_at: u64, blk: usize, buf: Vec<u32>, pos: usize },
}

/// Forward-only reader of one posting list that can jump with the skip table.
pub struct Cursor<'a> {
    seg: &'a Segment,
    pub df: u32,
    kind: CK,
    /// Posting blocks fetched and decoded so far.
    pub blocks_read: u64,
}

impl Cursor<'_> {
    /// First id >= `target` (targets must not decrease between calls), or None at the end.
    pub fn seek(&mut self, target: u32) -> R<Option<u32>> {
        let df = self.df as usize;
        match &mut self.kind {
            CK::Mem(v, pos) => {
                *pos += v[*pos..].partition_point(|&x| x < target);
                Ok(v.get(*pos).copied())
            }
            CK::Disk { skip, data_at, blk, buf, pos } => loop {
                if *blk != usize::MAX && *pos < buf.len() {
                    *pos += buf[*pos..].partition_point(|&x| x < target);
                    if *pos < buf.len() {
                        return Ok(Some(buf[*pos]));
                    }
                }
                let from = if *blk == usize::MAX { 0 } else { *blk + 1 };
                let i = from + skip[from.min(skip.len())..].partition_point(|s| s.0 < target);
                if i >= skip.len() {
                    return Ok(None);
                }
                let n = if i + 1 == skip.len() { df - BLOCK * i } else { BLOCK };
                let at = *data_at + skip[i].1 as u64;
                let want = block_max(n).min((self.seg.dict_off - at) as usize);
                let mut raw = vec![0u8; want];
                self.seg.file.read_exact_at(&mut raw, at)?;
                buf.clear();
                decode_block(&raw, n, if i == 0 { u32::MAX } else { skip[i - 1].0 }, buf);
                (*blk, *pos) = (i, 0);
                self.blocks_read += 1;
            },
        }
    }
}

/// Ascending ids present in every cursor; drives from the shortest list and skips the others.
pub fn intersect(cs: &mut [Cursor]) -> R<Vec<u32>> {
    let mut out = vec![];
    if cs.is_empty() {
        return Ok(out);
    }
    cs.sort_by_key(|c| c.df);
    let mut cand = match cs[0].seek(0)? {
        Some(x) => x,
        None => return Ok(out),
    };
    'outer: loop {
        for i in 1..cs.len() {
            match cs[i].seek(cand)? {
                None => break 'outer,
                Some(v) if v != cand => {
                    match cs[0].seek(v)? {
                        None => break 'outer,
                        Some(x) => cand = x,
                    }
                    continue 'outer;
                }
                _ => {}
            }
        }
        out.push(cand);
        match cs[0].seek(cand.wrapping_add(1))? {
            Some(x) if cand != u32::MAX => cand = x,
            _ => break,
        }
    }
    Ok(out)
}

/// Terms of one segment in order, decoded a dictionary block at a time.
pub struct TermIter<'a> {
    seg: &'a Segment,
    next_block: usize,
    cur: Vec<(Vec<u8>, u32)>,
    pos: usize,
}
impl<'a> TermIter<'a> {
    pub fn new(seg: &'a Segment) -> Self {
        Self { seg, next_block: 0, cur: vec![], pos: 0 }
    }
    pub fn peek(&mut self) -> R<Option<&(Vec<u8>, u32)>> {
        while self.pos >= self.cur.len() && self.next_block < self.seg.sparse.len() {
            self.cur.clear();
            let cur = &mut self.cur;
            self.seg.walk_block(self.next_block, &mut |t, e| {
                cur.push((t.to_vec(), e.df));
                true
            })?;
            self.next_block += 1;
            self.pos = 0;
        }
        Ok(self.cur.get(self.pos))
    }
    pub fn advance(&mut self) {
        self.pos += 1;
    }
}

/// Merges the term streams of several segments: calls `f(term, summed df, segments containing it)`
/// once per distinct term in ascending order.
pub fn merge_terms(segs: &[Segment], f: &mut dyn FnMut(&[u8], u64, u32)) -> R<()> {
    let mut its: Vec<TermIter> = segs.iter().map(TermIter::new).collect();
    loop {
        let mut min: Option<Vec<u8>> = None;
        for it in its.iter_mut() {
            if let Some((t, _)) = it.peek()? {
                if min.as_ref().is_none_or(|m| t < m) {
                    min = Some(t.clone());
                }
            }
        }
        let Some(m) = min else { return Ok(()) };
        let (mut df, mut n) = (0u64, 0u32);
        for it in its.iter_mut() {
            if let Some((t, d)) = it.peek()? {
                if *t == m {
                    df += *d as u64;
                    n += 1;
                    it.advance();
                }
            }
        }
        f(&m, df, n);
    }
}
