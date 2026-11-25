//! `build`: tokenises documents into an in-memory accumulator, closes an index segment on a document
//! count or a byte budget, writes it, and starts the next. Also writes `piece_rows` for the piece variant.
use crate::codec::*;
use crate::seg::*;
use crate::store::Store;
use crate::tok::tokenize;
use crate::util::*;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct Opts {
    pub store: PathBuf,
    pub out: PathBuf,
    pub piece: bool,
    pub docs_per_segment: u32,
    pub budget: u64,
    pub cache: usize,
}

pub struct Report {
    pub docs: u64,
    pub segments: u64,
    pub early_closed: u64,
    pub text_bytes: u64,
    pub stats: SegStats,
    pub piece_rows_bytes: u64,
    pub build_s: f64,
    pub distinct_global: u64,
}

/// Postings of the open segment.
struct Acc {
    map: FxMap<Vec<u8>, u32>,
    lists: Vec<Vec<u32>>,
    mem: u64,
    docs: u32,
}

impl Acc {
    fn new() -> Self {
        Self { map: FxMap::default(), lists: vec![], mem: 0, docs: 0 }
    }

    fn add_text(&mut self, doc: u32, text: &[u8]) {
        let Acc { map, lists, mem, .. } = self;
        tokenize(text, |t, long| {
            if long {
                return;
            }
            match map.get(t) {
                Some(&i) => {
                    let l = &mut lists[i as usize];
                    if *l.last().expect("non-empty") != doc {
                        l.push(doc);
                        *mem += 6;
                    }
                }
                None => {
                    map.insert(t.to_vec(), lists.len() as u32);
                    lists.push(vec![doc]);
                    // key bytes, map slot, two vector headers, first posting with growth slack
                    *mem += t.len() as u64 + 100;
                }
            }
        });
    }

    fn flush(&mut self, path: &Path, lo: u32, hi: u32) -> R<SegStats> {
        let mut e: Vec<(&Vec<u8>, &Vec<u32>)> = self.map.iter().map(|(t, &i)| (t, &self.lists[i as usize])).collect();
        e.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let st = write_segment(path, lo, hi, &mut e.iter().map(|(t, l)| (t.as_slice(), l.as_slice())))?;
        drop(e);
        *self = Acc::new();
        Ok(st)
    }
}

pub fn seg_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("seg-{n:05}.tix"))
}

pub fn run(o: &Opts) -> R<Report> {
    std::fs::create_dir_all(&o.out)?;
    let t0 = Instant::now();
    let mut store = Store::open(&o.store, o.cache)?;
    let (mut acc, mut r) = (Acc::new(), Report { docs: 0, segments: 0, early_closed: 0, text_bytes: 0, stats: SegStats::default(), piece_rows_bytes: 0, build_s: 0.0, distinct_global: 0 });
    let mut lo = 0u32;
    let dps = o.docs_per_segment.max(1);
    let close = |acc: &mut Acc, r: &mut Report, hi: u32, lo: &mut u32| -> R<()> {
        let st = acc.flush(&seg_path(&o.out, r.segments), *lo, hi)?;
        r.stats.add(&st);
        r.segments += 1;
        *lo = hi;
        Ok(())
    };
    if o.piece {
        for id in 0..store.pieces() {
            let p = store.piece(id)?;
            r.text_bytes += p.bytes().len() as u64;
            acc.add_text(id, p.bytes());
            acc.docs += 1;
            r.docs += 1;
            if acc.docs >= dps || acc.mem > o.budget {
                r.early_closed += (acc.docs < dps) as u64;
                close(&mut acc, &mut r, id + 1, &mut lo)?;
            }
        }
    } else {
        // Each row's pieces are tokenised in place; the union of their tokens is the token set of the
        // rebuilt text because the joining bytes are separators.
        store.for_rows(4 << 20, &mut |s, row, pieces, extra| {
            r.text_bytes += extra;
            for &pid in pieces {
                let p = s.piece(pid)?;
                r.text_bytes += p.bytes().len() as u64;
                acc.add_text(row, p.bytes());
            }
            acc.docs += 1;
            r.docs += 1;
            if acc.docs >= dps || acc.mem > o.budget {
                r.early_closed += (acc.docs < dps) as u64;
                close(&mut acc, &mut r, row + 1, &mut lo)?;
            }
            Ok(())
        })?;
    }
    if acc.docs > 0 {
        let hi = if o.piece { store.pieces() } else { store.rows() };
        close(&mut acc, &mut r, hi, &mut lo)?;
    }
    if o.piece {
        r.piece_rows_bytes = write_piece_rows(&mut store, &o.out.join("piece_rows.bin"))?;
    }
    r.build_s = t0.elapsed().as_secs_f64();
    // global distinct tokens: merge the segment dictionaries (not part of the build time)
    let segs: Vec<Segment> = (0..r.segments).map(|i| Segment::open(&seg_path(&o.out, i))).collect::<R<_>>()?;
    merge_terms(&segs, &mut |_, _, _| r.distinct_global += 1)?;
    Ok(r)
}

/// piece_rows.bin: "PRW1", u64 pieces, u64 rows, (pieces+1) u64 offsets into the data, then per piece
/// the ascending rows referencing it as varints (first row as is, then row - previous row).
/// Memory: one u32 per distinct (piece, row) pair, which is bounded by dd.refs.
fn write_piece_rows(store: &mut Store, path: &Path) -> R<u64> {
    let np = store.pieces() as usize;
    let (mut lists, mut last) = (vec![Vec::<u32>::new(); np], vec![u32::MAX; np]);
    store.for_rows(4 << 20, &mut |_, row, pieces, _| {
        for &p in pieces {
            if last[p as usize] != row {
                last[p as usize] = row;
                lists[p as usize].push(row);
            }
        }
        Ok(())
    })?;
    let (mut data, mut offs) = (vec![], Vec::with_capacity(np + 1));
    for l in &lists {
        offs.push(data.len() as u64);
        let mut prev = 0;
        for &r in l {
            put_varint(&mut data, (r - prev) as u64);
            prev = r;
        }
    }
    offs.push(data.len() as u64);
    let mut w = std::io::BufWriter::new(File::create(path)?);
    w.write_all(b"PRW1")?;
    w.write_all(&(np as u64).to_le_bytes())?;
    w.write_all(&(store.rows() as u64).to_le_bytes())?;
    for o in &offs {
        w.write_all(&o.to_le_bytes())?;
    }
    w.write_all(&data)?;
    w.flush()?;
    Ok(20 + 8 * offs.len() as u64 + data.len() as u64)
}

pub fn report_fields(r: &Report, piece_text: u64, span_text_hint: u64) -> Fields {
    let seg_total = r.stats.total();
    let total = seg_total + r.piece_rows_bytes;
    let pct = |d: u64| if d == 0 { "null".to_string() } else { format!("{:.2}", 100.0 * total as f64 / d as f64) };
    vec![
        ("docs", r.docs.to_string()),
        ("segments", r.segments.to_string()),
        ("segments_closed_on_budget", r.early_closed.to_string()),
        ("text_bytes", r.text_bytes.to_string()),
        ("distinct_tokens", obj(&[("sum_over_segments", r.stats.terms.to_string()), ("global", r.distinct_global.to_string())])),
        ("postings", r.stats.postings.to_string()),
        (
            "index_bytes",
            obj(&[
                ("segments", seg_total.to_string()),
                ("piece_rows", r.piece_rows_bytes.to_string()),
                ("total", total.to_string()),
                ("segment_parts", obj(&[("postings", r.stats.postings_bytes.to_string()), ("dictionary", r.stats.dict_bytes.to_string()), ("sparse_index", r.stats.sparse_bytes.to_string()), ("footer", r.stats.footer_bytes.to_string())])),
            ]),
        ),
        ("index_pct_of_distinct_piece_text", pct(piece_text)),
        ("index_pct_of_full_span_text", pct(span_text_hint)),
        ("build_s", format!("{:.3}", r.build_s)),
        ("text_mb_per_s", format!("{:.2}", r.text_bytes as f64 / 1e6 / r.build_s.max(1e-9))),
    ]
}

/// Bytes of the text of every span, rebuilt (computed from the references and piece lengths only).
pub fn span_text_bytes(store: &mut Store) -> R<u64> {
    let mut t = 0u64;
    store.for_rows(4 << 20, &mut |s, _, pieces, extra| {
        t += extra + pieces.iter().map(|&p| s.piece_len(p)).sum::<u64>();
        Ok(())
    })?;
    Ok(t)
}
