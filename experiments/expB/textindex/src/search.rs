//! `search`: evaluates the query classes over the index segments, for both variants.
//!
//! Boolean classes are defined over a row's token set. In the piece variant each term becomes a row
//! set (union of the rows of the pieces containing it, through piece_rows) before AND/OR/NOT; the
//! row sets are bitmaps over the row ids. In the span variant documents are rows, so lists combine
//! directly (sorted merges, and skip-based intersection for AND).
use crate::build::seg_path;
use crate::codec::get_varint;
use crate::qry::*;
use crate::seg::*;
use crate::store::Store;
use crate::tok::has_phrase;
use crate::util::*;
use std::path::Path;
use std::time::Instant;

pub struct PieceRows {
    offs: Vec<u64>,
    data: Vec<u8>,
    pub rows: u32,
}

impl PieceRows {
    pub fn open(path: &Path) -> R<Self> {
        let b = std::fs::read(path)?;
        if b.len() < 20 || &b[..4] != b"PRW1" {
            return Err("bad piece_rows.bin".into());
        }
        let (np, rows) = (le64(&b, 4) as usize, le64(&b, 12) as u32);
        let offs = (0..=np).map(|i| le64(&b, 20 + 8 * i)).collect();
        Ok(Self { offs, data: b[20 + 8 * (np + 1)..].to_vec(), rows })
    }
    fn set_bits(&self, piece: u32, bm: &mut [u64]) {
        let (mut p, end, mut row) = (self.offs[piece as usize] as usize, self.offs[piece as usize + 1] as usize, 0u64);
        while p < end {
            row += get_varint(&self.data, &mut p);
            bm[(row >> 6) as usize] |= 1 << (row & 63);
        }
    }
}

pub struct Searcher {
    pub segs: Vec<Segment>,
    pub store: Store,
    pub piece: bool,
    prow: Option<PieceRows>,
    row_trace: Vec<u32>,
}

#[derive(Default)]
pub struct Out {
    pub rows: Vec<u32>,
    /// seconds until the matching document ids were known (before piece-to-row mapping)
    pub match_s: f64,
    pub candidates: Option<u64>,
    pub verified_bytes: Option<u64>,
}

pub fn open_segments(dir: &Path) -> R<Vec<Segment>> {
    let mut v = vec![];
    while seg_path(dir, v.len() as u64).exists() {
        v.push(Segment::open(&seg_path(dir, v.len() as u64))?);
    }
    for w in v.windows(2) {
        if w[0].doc_hi != w[1].doc_lo {
            return Err("segments are not contiguous".into());
        }
    }
    Ok(v)
}

impl Searcher {
    pub fn open(index: &Path, store: Store, piece: bool) -> R<Self> {
        let segs = open_segments(index)?;
        let prow = if piece { Some(PieceRows::open(&index.join("piece_rows.bin"))?) } else { None };
        let docs = if piece { store.pieces() } else { store.rows() };
        if segs.last().map_or(0, |s| s.doc_hi) != docs {
            return Err("the index does not cover the store (wrong variant or store?)".into());
        }
        let row_trace = store.row_trace();
        Ok(Self { segs, store, piece, prow, row_trace })
    }

    pub fn traces(&self, rows: &[u32]) -> u64 {
        let mut n = 0;
        let mut last = u32::MAX;
        for &r in rows {
            let t = self.row_trace[r as usize];
            if t != last {
                n += 1;
                last = t;
            }
        }
        n
    }

    pub fn run(&mut self, q: &Query) -> R<Out> {
        let t0 = Instant::now();
        self.store.clear_cache();
        let op = q.op()?;
        let mut out = Out::default();
        if op == Op::Phrase {
            return if self.piece { self.phrase_piece(q, t0) } else { self.phrase_span(q, t0) };
        }
        if self.piece {
            // stage 1: piece lists per term; stage 2: row bitmaps, combined
            let lists: Vec<Vec<u32>> = if op == Op::Prefix { vec![self.prefix_docs(&q.args[0])?] } else { q.args.iter().map(|t| self.term_docs(t)).collect::<R<_>>()? };
            out.match_s = t0.elapsed().as_secs_f64();
            let nrows = self.prow.as_ref().expect("piece_rows").rows as usize;
            let mut bms: Vec<Vec<u64>> = lists
                .iter()
                .map(|l| {
                    let mut bm = vec![0u64; nrows.div_ceil(64)];
                    l.iter().for_each(|&p| self.prow.as_ref().expect("piece_rows").set_bits(p, &mut bm));
                    bm
                })
                .collect();
            let mut acc = bms.remove(0);
            for b in &bms {
                for (a, x) in acc.iter_mut().zip(b) {
                    match op {
                        Op::And => *a &= x,
                        Op::Or => *a |= x,
                        _ => *a &= !x,
                    }
                }
            }
            out.rows = bits_to_rows(&acc);
        } else {
            for seg in &self.segs {
                let part = match op {
                    Op::Term => seg_list(seg, &q.args[0])?,
                    Op::Prefix => union_all(seg.prefix(q.args[0].as_bytes())?.iter().map(|e| seg.list(e)).collect::<R<_>>()?),
                    Op::And => {
                        let mut cs = vec![];
                        for t in &q.args {
                            match seg.lookup(t.as_bytes())? {
                                Some(e) => cs.push(seg.cursor(&e)?),
                                None => {
                                    cs.clear();
                                    break;
                                }
                            }
                        }
                        intersect(&mut cs)?
                    }
                    Op::Or => union_all(vec![seg_list(seg, &q.args[0])?, seg_list(seg, &q.args[1])?]),
                    _ => diff(&seg_list(seg, &q.args[0])?, &seg_list(seg, &q.args[1])?),
                };
                out.rows.extend(part);
            }
            out.match_s = t0.elapsed().as_secs_f64();
        }
        Ok(out)
    }

    /// Documents containing the term, ascending, over all segments.
    fn term_docs(&self, t: &str) -> R<Vec<u32>> {
        let mut v = vec![];
        for s in &self.segs {
            v.extend(seg_list(s, t)?);
        }
        Ok(v)
    }

    fn prefix_docs(&self, p: &str) -> R<Vec<u32>> {
        let mut v = vec![];
        for s in &self.segs {
            v.extend(union_all(s.prefix(p.as_bytes())?.iter().map(|e| s.list(e)).collect::<R<_>>()?));
        }
        Ok(v)
    }

    /// Documents (per segment) that contain both tokens.
    fn candidates(&self, q: &Query) -> R<Vec<u32>> {
        let mut v = vec![];
        for s in &self.segs {
            let (a, b) = (s.lookup(q.args[0].as_bytes())?, s.lookup(q.args[1].as_bytes())?);
            if let (Some(a), Some(b)) = (a, b) {
                // a phrase of one repeated token needs only one cursor
                let mut cs = if q.args[0] == q.args[1] { vec![s.cursor(&a)?] } else { vec![s.cursor(&a)?, s.cursor(&b)?] };
                v.extend(intersect(&mut cs)?);
            }
        }
        Ok(v)
    }

    fn phrase_piece(&mut self, q: &Query, t0: Instant) -> R<Out> {
        let cand = self.candidates(q)?;
        let (mut hit, mut bytes) = (vec![], 0u64);
        for &p in &cand {
            let pc = self.store.piece(p)?;
            bytes += pc.bytes().len() as u64;
            if has_phrase(pc.bytes(), q.args[0].as_bytes(), q.args[1].as_bytes()) {
                hit.push(p);
            }
        }
        let match_s = t0.elapsed().as_secs_f64();
        let prow = self.prow.as_ref().expect("piece_rows");
        let mut bm = vec![0u64; (prow.rows as usize).div_ceil(64)];
        hit.iter().for_each(|&p| prow.set_bits(p, &mut bm));
        Ok(Out { rows: bits_to_rows(&bm), match_s, candidates: Some(cand.len() as u64), verified_bytes: Some(bytes) })
    }

    fn phrase_span(&mut self, q: &Query, t0: Instant) -> R<Out> {
        let cand = self.candidates(q)?;
        let (mut rows, mut bytes, mut pieces) = (vec![], 0u64, vec![]);
        for &r in &cand {
            self.store.row_pieces(r, &mut pieces)?;
            for &p in &pieces {
                let pc = self.store.piece(p)?;
                bytes += pc.bytes().len() as u64;
                if has_phrase(pc.bytes(), q.args[0].as_bytes(), q.args[1].as_bytes()) {
                    rows.push(r);
                    break;
                }
            }
        }
        Ok(Out { rows, match_s: t0.elapsed().as_secs_f64(), candidates: Some(cand.len() as u64), verified_bytes: Some(bytes) })
    }
}

fn seg_list(s: &Segment, t: &str) -> R<Vec<u32>> {
    Ok(match s.lookup(t.as_bytes())? {
        Some(e) => s.list(&e)?,
        None => vec![],
    })
}

pub fn bits_to_rows(bm: &[u64]) -> Vec<u32> {
    let mut v = vec![];
    for (i, &w) in bm.iter().enumerate() {
        let mut w = w;
        while w != 0 {
            v.push(i as u32 * 64 + w.trailing_zeros());
            w &= w - 1;
        }
    }
    v
}

/// Union of ascending lists as an ascending list without duplicates (pairwise sorted merges).
pub fn union_all(mut ls: Vec<Vec<u32>>) -> Vec<u32> {
    while ls.len() > 1 {
        // merge the two shortest, so the cost stays near n log k
        ls.sort_by_key(|l| std::cmp::Reverse(l.len()));
        let (b, a) = (ls.pop().expect("two"), ls.pop().expect("two"));
        ls.push(union(&a, &b));
    }
    ls.pop().unwrap_or_default()
}

pub fn union(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut o, mut i, mut j) = (Vec::with_capacity(a.len() + b.len()), 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => { o.push(a[i]); i += 1 }
            std::cmp::Ordering::Greater => { o.push(b[j]); j += 1 }
            std::cmp::Ordering::Equal => { o.push(a[i]); i += 1; j += 1 }
        }
    }
    o.extend_from_slice(&a[i..]);
    o.extend_from_slice(&b[j..]);
    o
}

/// Elements of `a` not in `b`.
pub fn diff(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut o, mut j) = (vec![], 0);
    for &x in a {
        while j < b.len() && b[j] < x {
            j += 1;
        }
        if j >= b.len() || b[j] != x {
            o.push(x);
        }
    }
    o
}

/// Latency report for `search`.
pub fn search_cmd(index: &Path, store_dir: &Path, piece: bool, queries: &Path, runs: usize, cache: usize) -> R<()> {
    let t0 = Instant::now();
    let qs = read_queries(queries)?;
    let store = Store::open(store_dir, cache)?;
    let mut s = Searcher::open(index, store, piece)?;
    let open_s = t0.elapsed().as_secs_f64();
    let mut by_class: std::collections::BTreeMap<String, Vec<f64>> = Default::default();
    for qy in &qs {
        let (mut ms, mut matches, mut last) = (vec![], vec![], Out::default());
        for _ in 0..runs.max(1) {
            let t = Instant::now();
            last = s.run(qy)?;
            ms.push(t.elapsed().as_secs_f64() * 1e3);
            matches.push(last.match_s * 1e3);
        }
        by_class.entry(qy.class.clone()).or_default().extend(&ms);
        let mut f: Fields = vec![
            ("id", q(&qy.id)),
            ("class", q(&qy.class)),
            ("rows", last.rows.len().to_string()),
            ("traces", s.traces(&last.rows).to_string()),
            ("rows_sha256", q(&rows_sha(&last.rows))),
            ("runs_ms", arr(&ms, |x| format!("{x:.3}"))),
            ("match_ms", format!("{:.3}", pct(&matches, 50.0))),
        ];
        if let Some(c) = last.candidates {
            f.push(("candidates", c.to_string()));
            f.push(("verified_bytes", last.verified_bytes.unwrap_or(0).to_string()));
        }
        println!("{}", obj(&f));
    }
    let p50 = obj(&by_class.iter().map(|(c, v)| (c.as_str(), format!("{:.3}", pct(v, 50.0)))).collect::<Vec<_>>());
    finish("search", if piece { "piece" } else { "span" }, t0, vec![("queries", qs.len().to_string()), ("runs", runs.to_string()), ("open_s", format!("{open_s:.3}")), ("segments", s.segs.len().to_string()), ("p50_ms", p50)]);
    Ok(())
}

