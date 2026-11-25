//! `dedup`: every distinct piece of text is stored once per scope (a time window).
//!
//! Pieces: the whole value for system_instructions, tool_definitions, tool_call_arguments and
//! tool_call_result; each top-level element of the JSON array for input_messages and
//! output_messages (the value is `[` + pieces joined by `,` + `]`, checked byte for byte at
//! build time; anything that does not rebuild is stored whole and counted as a fallback).
//!
//! Files (all integers little-endian)
//!   dd.blocks  zstd (level 3) frames back to back. Within a scope, pieces are packed in
//!              first-seen order into blocks of at most 1 MiB uncompressed; a piece of 1 MiB
//!              or more gets a block of its own.
//!   dd.refs    per row (global row order), six attributes in table order, each
//!                0xFFFFFFFF                       value absent
//!                0x40000000, u32 piece            whole value is that piece
//!                u32 n (< 0x40000000), n x u32    array value: `[` + pieces joined by `,` + `]`
//!   dd.idx     "DDX1", u64 traces, rows, pieces, blocks; then
//!                traces:  { trace_id[16], first_row u64, rows u32 }          28 bytes each
//!                rows+1 u64 offsets into dd.refs
//!                pieces:  { block u32, offset in block u32, length u32 }     12 bytes each
//!                blocks:  { offset in dd.blocks u64, compressed u32, raw u32 } 16 bytes each
//!              Piece ids are global; scopes never share pieces.
use crate::split::split_array;
use crate::util::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use twox_hash::XxHash3_128;

const BLOCK: usize = 1 << 20;
const LEVEL: i32 = 3;
const ABSENT: u32 = 0xFFFF_FFFF;
const WHOLE: u32 = 0x4000_0000;
const MAGIC: &[u8; 4] = b"DDX1";

pub struct DdBuilder {
    dir: PathBuf,
    window_ns: u64,
    refs: Counting<BufWriter<File>>,
    blocks: Counting<BufWriter<File>>,
    comp: zstd::bulk::Compressor<'static>,
    cur_win: Option<u64>,
    // state of the open scope; dropped at its end
    pieces: Vec<Vec<u8>>,
    map: HashMap<u128, Vec<u32>>,
    base: u32,
    scope_bytes: u64,
    // tables for dd.idx
    piece_tab: Vec<[u32; 3]>,
    block_tab: Vec<(u64, u32, u32)>,
    row_offs: Vec<u64>,
    traces: Vec<([u8; 16], u64, u32)>,
    trace_first: u64,
    trace_id: [u8; 16],
    // statistics
    whole_fallbacks: u64,
    collisions: u64,
    scopes: u64,
    occ_bytes: u64,
    seg_unique: u64,
    tr_seen: HashSet<u128>,
    tr_unique: u64,
    glob_seen: HashSet<u128>,
    glob_unique: u64,
    max_scope_bytes: u64,
    max_scope_pieces: u64,
    max_scope_state: u64,
    buf: Vec<u8>,
}

impl DdBuilder {
    pub fn new(dir: &Path, window_s: u64) -> R<Self> {
        let w = |n: &str| -> R<Counting<BufWriter<File>>> { Ok(Counting { inner: BufWriter::with_capacity(1 << 20, File::create(dir.join(n))?), n: 0 }) };
        Ok(Self {
            dir: dir.into(),
            window_ns: window_s.max(1) * 1_000_000_000,
            refs: w("dd.refs")?,
            blocks: w("dd.blocks")?,
            comp: zstd::bulk::Compressor::new(LEVEL)?,
            cur_win: None,
            pieces: vec![],
            map: HashMap::new(),
            base: 0,
            scope_bytes: 0,
            piece_tab: vec![],
            block_tab: vec![],
            row_offs: vec![],
            traces: vec![],
            trace_first: 0,
            trace_id: [0; 16],
            whole_fallbacks: 0,
            collisions: 0,
            scopes: 0,
            occ_bytes: 0,
            seg_unique: 0,
            tr_seen: HashSet::new(),
            tr_unique: 0,
            glob_seen: HashSet::new(),
            glob_unique: 0,
            max_scope_bytes: 0,
            max_scope_pieces: 0,
            max_scope_state: 0,
            buf: vec![],
        })
    }

    /// Global id of the piece, adding it to the scope if new.
    fn intern(&mut self, b: &[u8]) -> u32 {
        let h = XxHash3_128::oneshot(b);
        let n = b.len() as u64;
        self.occ_bytes += n;
        if self.tr_seen.insert(h) {
            self.tr_unique += n;
        }
        if self.glob_seen.insert(h) {
            self.glob_unique += n;
        }
        let ids = self.map.entry(h).or_default();
        for &i in ids.iter() {
            if self.pieces[i as usize] == b {
                return self.base + i;
            }
            self.collisions += 1;
        }
        let i = self.pieces.len() as u32;
        ids.push(i);
        self.pieces.push(b.to_vec());
        self.scope_bytes += n;
        self.base + i
    }

    fn emit_block(&mut self, raw: &[u8]) -> R<()> {
        let c = self.comp.compress(raw)?;
        self.block_tab.push((self.blocks.n, c.len() as u32, raw.len() as u32));
        self.blocks.write_all(&c)?;
        Ok(())
    }

    /// Packs the scope's pieces into blocks, writes them, and releases the scope's memory.
    fn end_scope(&mut self) -> R<()> {
        self.max_scope_bytes = self.max_scope_bytes.max(self.scope_bytes);
        self.max_scope_pieces = self.max_scope_pieces.max(self.pieces.len() as u64);
        // piece bytes plus about 100 bytes per piece for the vector header, map entry and id list
        self.max_scope_state = self.max_scope_state.max(self.scope_bytes + 100 * self.pieces.len() as u64);
        self.seg_unique += self.scope_bytes;
        let mut cur: Vec<u8> = vec![];
        for p in std::mem::take(&mut self.pieces) {
            if p.len() >= BLOCK {
                if !cur.is_empty() {
                    self.emit_block(&cur)?;
                    cur.clear();
                }
                self.piece_tab.push([self.block_tab.len() as u32, 0, p.len() as u32]);
                self.emit_block(&p)?;
                continue;
            }
            if !cur.is_empty() && cur.len() + p.len() > BLOCK {
                self.emit_block(&cur)?;
                cur.clear();
            }
            self.piece_tab.push([self.block_tab.len() as u32, cur.len() as u32, p.len() as u32]);
            cur.extend_from_slice(&p);
        }
        if !cur.is_empty() {
            self.emit_block(&cur)?;
        }
        self.map = HashMap::new();
        self.scope_bytes = 0;
        self.base = self.piece_tab.len() as u32;
        Ok(())
    }
}

impl Builder for DdBuilder {
    fn begin_trace(&mut self, id: &[u8; 16], start_ns: u64) -> R<()> {
        let w = start_ns / self.window_ns;
        if self.cur_win != Some(w) {
            if self.cur_win.is_some() {
                self.end_scope()?;
            }
            self.cur_win = Some(w);
            self.scopes += 1;
        }
        self.tr_seen.clear();
        (self.trace_id, self.trace_first) = (*id, self.row_offs.len() as u64);
        Ok(())
    }
    fn span(&mut self, s: &SpanRec) -> R<()> {
        self.row_offs.push(self.refs.n);
        let mut out = std::mem::take(&mut self.buf);
        out.clear();
        for (a, v) in s.vals.iter().enumerate() {
            let Some(v) = v else {
                out.extend_from_slice(&ABSENT.to_le_bytes());
                continue;
            };
            let parts = if a < 2 { split_array(v) } else { None };
            match parts {
                Some(p) => {
                    out.extend_from_slice(&(p.len() as u32).to_le_bytes());
                    for (s, e) in p {
                        out.extend_from_slice(&self.intern(&v[s..e]).to_le_bytes());
                    }
                }
                None => {
                    if a < 2 {
                        self.whole_fallbacks += 1;
                    }
                    out.extend_from_slice(&WHOLE.to_le_bytes());
                    out.extend_from_slice(&self.intern(v).to_le_bytes());
                }
            }
        }
        self.refs.write_all(&out)?;
        self.buf = out;
        Ok(())
    }
    fn end_trace(&mut self) -> R<()> {
        self.traces.push((self.trace_id, self.trace_first, (self.row_offs.len() as u64 - self.trace_first) as u32));
        Ok(())
    }
    fn finish(&mut self) -> R<(Vec<(String, u64)>, Fields)> {
        if self.cur_win.is_some() {
            self.end_scope()?;
        }
        self.refs.flush()?;
        self.blocks.flush()?;
        self.row_offs.push(self.refs.n);
        let mut w = BufWriter::new(File::create(self.dir.join("dd.idx"))?);
        w.write_all(MAGIC)?;
        for n in [self.traces.len(), self.row_offs.len() - 1, self.piece_tab.len(), self.block_tab.len()] {
            w.write_all(&(n as u64).to_le_bytes())?;
        }
        for (id, first, n) in &self.traces {
            w.write_all(id)?;
            w.write_all(&first.to_le_bytes())?;
            w.write_all(&n.to_le_bytes())?;
        }
        for o in &self.row_offs {
            w.write_all(&o.to_le_bytes())?;
        }
        for p in &self.piece_tab {
            for x in p {
                w.write_all(&x.to_le_bytes())?;
            }
        }
        for (o, c, r) in &self.block_tab {
            w.write_all(&o.to_le_bytes())?;
            w.write_all(&c.to_le_bytes())?;
            w.write_all(&r.to_le_bytes())?;
        }
        w.flush()?;
        let sz = |n: &str| std::fs::metadata(self.dir.join(n)).map(|m| m.len());
        let ratio = |u: u64| format!("{:.4}", if self.occ_bytes == 0 { 0.0 } else { u as f64 / self.occ_bytes as f64 });
        let f = vec![
            ("whole_fallbacks", self.whole_fallbacks.to_string()),
            ("hash_collisions", self.collisions.to_string()),
            ("scope_ratio", obj(&[("trace", ratio(self.tr_unique)), ("segment", ratio(self.seg_unique)), ("dataset", ratio(self.glob_unique))])),
            ("pieces", self.piece_tab.len().to_string()),
            ("unique_piece_bytes", self.seg_unique.to_string()),
            ("piece_occurrence_bytes", self.occ_bytes.to_string()),
            ("scopes", self.scopes.to_string()),
            ("blocks", self.block_tab.len().to_string()),
            ("max_scope_piece_bytes", self.max_scope_bytes.to_string()),
            ("max_scope_pieces", self.max_scope_pieces.to_string()),
            ("max_scope_state_bytes_est", self.max_scope_state.to_string()),
        ];
        Ok((vec![("dd.blocks".into(), sz("dd.blocks")?), ("dd.refs".into(), sz("dd.refs")?), ("dd.idx".into(), sz("dd.idx")?)], f))
    }
}

pub struct DdStore {
    refs: File,
    blocks: File,
    traces: Vec<([u8; 16], u64, u32)>,
    by_id: HashMap<[u8; 16], usize>,
    row_offs: Vec<u64>,
    piece_tab: Vec<[u32; 3]>,
    block_tab: Vec<(u64, u32, u32)>,
    dec: zstd::bulk::Decompressor<'static>,
    cache: HashMap<u32, Vec<u8>>,
    order: VecDeque<u32>,
    cache_bytes: usize,
    cbuf: Vec<u8>,
    scratch: Vec<u8>,
}

/// Decoded blocks kept for the scan; point lookups start from an empty cache.
const CACHE_CAP: usize = 256 << 20;

impl DdStore {
    pub fn open(dir: &Path) -> R<Self> {
        let mut b = vec![];
        File::open(dir.join("dd.idx"))?.read_to_end(&mut b)?;
        if b.len() < 36 || &b[..4] != MAGIC {
            return Err("bad dd.idx".into());
        }
        let (nt, nr, np, nb) = (le64(&b, 4) as usize, le64(&b, 12) as usize, le64(&b, 20) as usize, le64(&b, 28) as usize);
        if b.len() != 36 + nt * 28 + (nr + 1) * 8 + np * 12 + nb * 16 {
            return Err("dd.idx has the wrong size".into());
        }
        let mut at = 36;
        let traces: Vec<_> = (0..nt).map(|i| (b[at + i * 28..at + i * 28 + 16].try_into().expect("16 bytes"), le64(&b, at + i * 28 + 16), le32(&b, at + i * 28 + 24))).collect();
        at += nt * 28;
        let row_offs = (0..=nr).map(|i| le64(&b, at + i * 8)).collect();
        at += (nr + 1) * 8;
        let piece_tab = (0..np).map(|i| [le32(&b, at + i * 12), le32(&b, at + i * 12 + 4), le32(&b, at + i * 12 + 8)]).collect();
        at += np * 12;
        let block_tab = (0..nb).map(|i| (le64(&b, at + i * 16), le32(&b, at + i * 16 + 8), le32(&b, at + i * 16 + 12))).collect();
        let by_id = traces.iter().enumerate().map(|(i, t)| (t.0, i)).collect();
        Ok(Self {
            refs: File::open(dir.join("dd.refs"))?,
            blocks: File::open(dir.join("dd.blocks"))?,
            traces,
            by_id,
            row_offs,
            piece_tab,
            block_tab,
            dec: zstd::bulk::Decompressor::new()?,
            cache: HashMap::new(),
            order: VecDeque::new(),
            cache_bytes: 0,
            cbuf: vec![],
            scratch: vec![],
        })
    }

    fn clear_cache(&mut self) {
        self.cache.clear();
        self.order.clear();
        self.cache_bytes = 0;
    }

    fn load(&mut self, b: u32) -> R<()> {
        if self.cache.contains_key(&b) {
            return Ok(());
        }
        let (off, clen, rlen) = *self.block_tab.get(b as usize).ok_or("block id out of range")?;
        let mut c = vec![0u8; clen as usize];
        self.blocks.read_exact_at(&mut c, off)?;
        let raw = self.dec.decompress(&c, rlen as usize)?;
        if raw.len() != rlen as usize {
            return Err("block has the wrong size".into());
        }
        self.cache_bytes += raw.len();
        self.cache.insert(b, raw);
        self.order.push_back(b);
        while self.cache_bytes > CACHE_CAP && self.order.len() > 1 {
            let old = self.order.pop_front().expect("non-empty");
            self.cache_bytes -= self.cache.remove(&old).map_or(0, |x| x.len());
        }
        Ok(())
    }

    /// Reads the references of rows [first, first+n) and visits their values.
    fn rows_range(&mut self, first: u64, n: u64, v: &mut dyn Visit) -> R<()> {
        let (a, b) = (self.row_offs[first as usize], self.row_offs[(first + n) as usize]);
        let mut words = std::mem::take(&mut self.cbuf);
        words.resize((b - a) as usize, 0);
        self.refs.read_exact_at(&mut words, a)?;
        let mut p = 0usize;
        for _ in 0..n * 6 {
            let h = le32(&words, p);
            p += 4;
            if h == ABSENT {
                continue;
            }
            if h & WHOLE != 0 {
                let [blk, off, len] = self.piece(le32(&words, p))?;
                p += 4;
                self.load(blk)?;
                v.value(&self.cache[&blk][off as usize..(off + len) as usize]);
            } else {
                self.scratch.clear();
                self.scratch.push(b'[');
                for k in 0..h {
                    if k > 0 {
                        self.scratch.push(b',');
                    }
                    let [blk, off, len] = self.piece(le32(&words, p))?;
                    p += 4;
                    self.load(blk)?;
                    self.scratch.extend_from_slice(&self.cache[&blk][off as usize..(off + len) as usize]);
                }
                self.scratch.push(b']');
                v.value(&self.scratch);
            }
        }
        self.cbuf = words;
        Ok(())
    }

    fn piece(&self, id: u32) -> R<[u32; 3]> {
        Ok(*self.piece_tab.get(id as usize).ok_or("piece id out of range")?)
    }
}

impl Store for DdStore {
    fn rows(&self) -> u64 {
        self.row_offs.len() as u64 - 1
    }
    fn read_trace(&mut self, id: &[u8; 16], v: &mut dyn Visit) -> R<bool> {
        let Some(&ti) = self.by_id.get(id) else { return Ok(false) };
        self.clear_cache();
        let (_, first, n) = self.traces[ti];
        self.rows_range(first, n as u64, v).map(|_| true)
    }
    fn read_span(&mut self, row: u64, v: &mut dyn Visit) -> R<()> {
        self.clear_cache();
        self.rows_range(row, 1, v)
    }
    fn scan(&mut self, v: &mut dyn Visit) -> R<()> {
        self.clear_cache();
        for ti in 0..self.traces.len() {
            let (id, first, n) = self.traces[ti];
            v.trace(&id);
            self.rows_range(first, n as u64, v)?;
        }
        Ok(())
    }
}
