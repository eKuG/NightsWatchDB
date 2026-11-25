//! Reader for the `dedup` payload store (dd.idx, dd.refs, dd.blocks; formats in expA/payload/README.md).
//! Copied and cut down from expA/payload/src/dd.rs; the decoded-block cache is bounded by bytes (LRU).
use crate::util::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::rc::Rc;

const ABSENT: u32 = 0xFFFF_FFFF;
const WHOLE: u32 = 0x4000_0000;

pub struct Store {
    refs: File,
    blocks: File,
    pub traces: Vec<([u8; 16], u64, u32)>,
    row_offs: Vec<u64>,
    piece_tab: Vec<[u32; 3]>,
    block_tab: Vec<(u64, u32, u32)>,
    dec: zstd::bulk::Decompressor<'static>,
    cache: HashMap<u32, (Rc<Vec<u8>>, u64)>,
    cache_bytes: usize,
    pub cache_cap: usize,
    tick: u64,
    pub blocks_decoded: u64,
    pub bytes_decoded: u64,
}

/// A piece of text inside a decoded block.
pub struct Piece {
    blk: Rc<Vec<u8>>,
    off: usize,
    len: usize,
}
impl Piece {
    pub fn bytes(&self) -> &[u8] {
        &self.blk[self.off..self.off + self.len]
    }
}

impl Store {
    pub fn open(dir: &Path, cache_cap: usize) -> R<Self> {
        let mut b = vec![];
        File::open(dir.join("dd.idx"))?.read_to_end(&mut b)?;
        if b.len() < 36 || &b[..4] != b"DDX1" {
            return Err("bad dd.idx".into());
        }
        let (nt, nr, np, nb) = (le64(&b, 4) as usize, le64(&b, 12) as usize, le64(&b, 20) as usize, le64(&b, 28) as usize);
        if b.len() != 36 + nt * 28 + (nr + 1) * 8 + np * 12 + nb * 16 {
            return Err("dd.idx has the wrong size".into());
        }
        let mut at = 36;
        let traces = (0..nt).map(|i| (b[at + i * 28..at + i * 28 + 16].try_into().expect("16 bytes"), le64(&b, at + i * 28 + 16), le32(&b, at + i * 28 + 24))).collect();
        at += nt * 28;
        let row_offs = (0..=nr).map(|i| le64(&b, at + i * 8)).collect();
        at += (nr + 1) * 8;
        let piece_tab = (0..np).map(|i| [le32(&b, at + i * 12), le32(&b, at + i * 12 + 4), le32(&b, at + i * 12 + 8)]).collect();
        at += np * 12;
        let block_tab = (0..nb).map(|i| (le64(&b, at + i * 16), le32(&b, at + i * 16 + 8), le32(&b, at + i * 16 + 12))).collect();
        Ok(Self {
            refs: File::open(dir.join("dd.refs"))?,
            blocks: File::open(dir.join("dd.blocks"))?,
            traces,
            row_offs,
            piece_tab,
            block_tab,
            dec: zstd::bulk::Decompressor::new()?,
            cache: HashMap::new(),
            cache_bytes: 0,
            cache_cap,
            tick: 0,
            blocks_decoded: 0,
            bytes_decoded: 0,
        })
    }

    pub fn rows(&self) -> u32 {
        self.row_offs.len() as u32 - 1
    }
    pub fn pieces(&self) -> u32 {
        self.piece_tab.len() as u32
    }
    pub fn piece_len(&self, id: u32) -> u64 {
        self.piece_tab[id as usize][2] as u64
    }
    /// Total bytes of distinct piece text.
    pub fn piece_bytes_total(&self) -> u64 {
        self.piece_tab.iter().map(|p| p[2] as u64).sum()
    }
    /// Trace number of every row.
    pub fn row_trace(&self) -> Vec<u32> {
        let mut v = vec![0u32; self.rows() as usize];
        for (t, &(_, first, n)) in self.traces.iter().enumerate() {
            v[first as usize..first as usize + n as usize].fill(t as u32);
        }
        v
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();
        self.cache_bytes = 0;
    }

    fn load(&mut self, b: u32) -> R<Rc<Vec<u8>>> {
        self.tick += 1;
        if let Some(e) = self.cache.get_mut(&b) {
            e.1 = self.tick;
            return Ok(e.0.clone());
        }
        let (off, clen, rlen) = *self.block_tab.get(b as usize).ok_or("block id out of range")?;
        let mut c = vec![0u8; clen as usize];
        self.blocks.read_exact_at(&mut c, off)?;
        let raw = self.dec.decompress(&c, rlen as usize)?;
        if raw.len() != rlen as usize {
            return Err("block has the wrong size".into());
        }
        self.blocks_decoded += 1;
        self.bytes_decoded += raw.len() as u64;
        let rc = Rc::new(raw);
        self.cache_bytes += rc.len();
        self.cache.insert(b, (rc.clone(), self.tick));
        // evict the least recently used blocks while over the byte cap, never the one just loaded
        while self.cache_bytes > self.cache_cap && self.cache.len() > 1 {
            let old = *self.cache.iter().filter(|(k, _)| **k != b).min_by_key(|(_, v)| v.1).expect("another block").0;
            self.cache_bytes -= self.cache.remove(&old).map_or(0, |x| x.0.len());
        }
        Ok(rc)
    }

    pub fn piece(&mut self, id: u32) -> R<Piece> {
        let [blk, off, len] = *self.piece_tab.get(id as usize).ok_or("piece id out of range")?;
        Ok(Piece { blk: self.load(blk)?, off: off as usize, len: len as usize })
    }

    /// Piece ids of one row, in attribute order; returns the bytes the rebuilt text has besides the
    /// pieces (`[`, `,`, `]` of arrays and the five 0x00 separators between attributes).
    pub fn row_pieces(&mut self, row: u32, out: &mut Vec<u32>) -> R<u64> {
        let (a, b) = (self.row_offs[row as usize], self.row_offs[row as usize + 1]);
        let mut w = vec![0u8; (b - a) as usize];
        self.refs.read_exact_at(&mut w, a)?;
        out.clear();
        Ok(parse_row(&w, &mut 0, out))
    }

    /// Visits every row in order, reading dd.refs in chunks of about `chunk` bytes.
    pub fn for_rows(&mut self, chunk: u64, f: &mut dyn FnMut(&mut Store, u32, &[u32], u64) -> R<()>) -> R<()> {
        let (mut row, n) = (0u32, self.rows());
        let (mut words, mut pieces) = (vec![], vec![]);
        while row < n {
            let mut end = row + 1;
            while end < n && self.row_offs[end as usize + 1] - self.row_offs[row as usize] <= chunk {
                end += 1;
            }
            let base = self.row_offs[row as usize];
            words.resize((self.row_offs[end as usize] - base) as usize, 0);
            self.refs.read_exact_at(&mut words, base)?;
            let mut p = 0;
            for r in row..end {
                pieces.clear();
                let extra = parse_row(&words, &mut p, &mut pieces);
                f(self, r, &pieces, extra)?;
            }
            row = end;
        }
        Ok(())
    }
}

/// Parses the six attributes of one row from `w` at `*p`; pushes piece ids, returns the non-piece bytes.
pub fn parse_row(w: &[u8], p: &mut usize, out: &mut Vec<u32>) -> u64 {
    let mut extra = 5u64;
    for _ in 0..6 {
        let h = le32(w, *p);
        *p += 4;
        if h == ABSENT {
            continue;
        }
        let n = if h & WHOLE != 0 { 1 } else { h as usize };
        if h & WHOLE == 0 {
            extra += 2 + n.saturating_sub(1) as u64;
        }
        for _ in 0..n {
            out.push(le32(w, *p));
            *p += 4;
        }
    }
    extra
}
