//! Read side: agg, sample-ids, trace lookup.
use crate::convert::PAY_COLS;
use crate::R;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::PageIndexPolicy;
use std::collections::HashSet;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};

pub struct Table {
    path: PathBuf,
    meta: ArrowReaderMetadata,
}

type Builder = ParquetRecordBatchReaderBuilder<File>;

impl Table {
    /// Reads the footer and page index once.
    pub fn open(path: &Path) -> R<Self> {
        let meta = ArrowReaderMetadata::load(&File::open(path)?, ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional))?;
        Ok(Self { path: path.into(), meta })
    }
    fn builder(&self) -> R<Builder> {
        Ok(Builder::new_with_metadata(File::open(&self.path)?, self.meta.clone()))
    }
    fn col(&self, name: &str) -> usize {
        self.meta.schema().index_of(name).expect("column present")
    }
    fn mask(&self, names: &[&str]) -> ProjectionMask {
        ProjectionMask::leaves(self.meta.parquet_schema(), names.iter().map(|n| self.col(n)))
    }
    fn groups(&self) -> usize {
        self.meta.metadata().num_row_groups()
    }
    pub fn group_count(&self) -> usize {
        self.groups()
    }
    pub fn group_rows(&self, rg: usize) -> usize {
        self.meta.metadata().row_group(rg).num_rows() as usize
    }
    pub fn rows(&self) -> usize {
        self.meta.metadata().file_metadata().num_rows() as usize
    }
    /// (min, max) statistics bytes of a column chunk, if both are known.
    fn minmax(&self, rg: usize, col: usize) -> Option<(&[u8], &[u8])> {
        let s = self.meta.metadata().row_group(rg).column(col).statistics()?;
        Some((s.min_bytes_opt()?, s.max_bytes_opt()?))
    }
    /// Streams a column projection over the whole file, one row group at a time.
    pub fn scan<'a>(&'a self, names: &'a [&str], batch: usize) -> impl Iterator<Item = R<RecordBatch>> + 'a {
        (0..self.groups()).flat_map(move |rg| -> Box<dyn Iterator<Item = R<RecordBatch>>> {
            match self.group_iter(rg, names, batch) {
                Ok(i) => Box::new(i),
                Err(e) => Box::new(std::iter::once(Err(e))),
            }
        })
    }
    pub fn group_iter(&self, rg: usize, names: &[&str], batch: usize) -> R<impl Iterator<Item = R<RecordBatch>>> {
        Ok(self.builder()?.with_row_groups(vec![rg]).with_projection(self.mask(names)).with_batch_size(batch).build()?.map(|b| b.map_err(Into::into)))
    }
    /// Reads only the given global row ranges (all columns unless `names` is set); the reader skips pages.
    fn read_rows(&self, ranges: &[Range<usize>], names: Option<&[&str]>) -> R<Vec<RecordBatch>> {
        let mut out = vec![];
        let mut start = 0usize;
        for rg in 0..self.groups() {
            let len = self.meta.metadata().row_group(rg).num_rows() as usize;
            let local: Vec<_> = ranges.iter().filter(|r| r.start < start + len && r.end > start).map(|r| r.start.max(start) - start..r.end.min(start + len) - start).collect();
            start += len;
            if local.is_empty() {
                continue;
            }
            let mut b = self.builder()?.with_row_groups(vec![rg]).with_batch_size(8192).with_row_selection(RowSelection::from_consecutive_ranges(local.into_iter(), len));
            if let Some(n) = names {
                b = b.with_projection(self.mask(n));
            }
            for batch in b.build()? {
                out.push(batch?);
            }
        }
        Ok(out)
    }
}

pub type AggRow = (Option<String>, u64, i64, i64);

fn i64_of(b: &[u8]) -> i64 {
    i64::from_le_bytes(b.try_into().expect("8-byte statistic"))
}

/// Minimum and maximum `start_ns` from column chunk statistics.
pub fn start_range(t: &Table) -> R<(i64, i64)> {
    let c = t.col("start_ns");
    let mut r: Option<(i64, i64)> = None;
    for rg in 0..t.groups() {
        let (lo, hi) = t.minmax(rg, c).map(|(a, b)| (i64_of(a), i64_of(b))).ok_or("start_ns statistics missing")?;
        r = Some(r.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
    }
    r.ok_or_else(|| "empty file".into())
}

pub fn window(min: i64, max: i64) -> (i64, i64) {
    let d = (max - min) as i128;
    (min + (d / 4) as i64, min + (3 * d / 4) as i64)
}

/// chat spans with start_ns in [from, to), grouped by request_model. Row groups are pruned on start_ns and operation statistics.
pub fn agg(t: &Table, from: i64, to: i64, threads: usize) -> R<Vec<AggRow>> {
    let (cs, co) = (t.col("start_ns"), t.col("operation"));
    let keep: Vec<usize> = (0..t.groups())
        .filter(|&rg| {
            let ok_t = t.minmax(rg, cs).is_none_or(|(a, b)| i64_of(b) >= from && i64_of(a) < to);
            let ok_o = t.minmax(rg, co).is_none_or(|(a, b)| a <= &b"chat"[..] && &b"chat"[..] <= b);
            ok_t && ok_o
        })
        .collect();
    let names = ["operation", "start_ns", "request_model", "input_tokens", "output_tokens"];
    let parts: Vec<R<Vec<AggRow>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.max(1))
            .map(|w| {
                let mine: Vec<usize> = keep.iter().copied().skip(w).step_by(threads.max(1)).collect();
                s.spawn(move || -> R<Vec<AggRow>> {
                    let mut acc: Vec<AggRow> = vec![];
                    if mine.is_empty() {
                        return Ok(acc);
                    }
                    let r = t.builder()?.with_row_groups(mine).with_projection(t.mask(&names)).with_batch_size(8192).build()?;
                    for b in r {
                        let b = b?;
                        let col = |n: &str| b.column_by_name(n).expect("column").as_ref();
                        let (op, st, md) = (col("operation").as_string::<i32>(), col("start_ns").as_primitive::<arrow::datatypes::Int64Type>(), col("request_model").as_string::<i32>());
                        let (it, ot) = (col("input_tokens").as_primitive::<arrow::datatypes::Int64Type>(), col("output_tokens").as_primitive::<arrow::datatypes::Int64Type>());
                        for i in 0..b.num_rows() {
                            if op.is_null(i) || op.value(i) != "chat" || st.value(i) < from || st.value(i) >= to {
                                continue;
                            }
                            let m = md.is_valid(i).then(|| md.value(i));
                            let slot = match acc.iter().position(|a| a.0.as_deref() == m) {
                                Some(p) => p,
                                None => {
                                    acc.push((m.map(String::from), 0, 0, 0));
                                    acc.len() - 1
                                }
                            };
                            let a = &mut acc[slot];
                            a.1 += 1;
                            a.2 += if it.is_valid(i) { it.value(i) } else { 0 };
                            a.3 += if ot.is_valid(i) { ot.value(i) } else { 0 };
                        }
                    }
                    Ok(acc)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("agg worker")).collect()
    });
    let mut all: Vec<AggRow> = vec![];
    for p in parts {
        for r in p? {
            match all.iter_mut().find(|a| a.0 == r.0) {
                Some(a) => (a.1, a.2, a.3) = (a.1 + r.1, a.2 + r.2, a.3 + r.3),
                None => all.push(r),
            }
        }
    }
    all.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(all)
}

/// Distinct trace ids in order of first appearance, one scan of `trace_id`.
pub fn distinct_traces(t: &Table) -> R<Vec<[u8; 16]>> {
    let (mut seen, mut ids) = (HashSet::new(), vec![]);
    for b in t.scan(&["trace_id"], 8192) {
        let a = b?;
        let a = a.column(0).as_fixed_size_binary();
        for i in 0..a.len() {
            let id: [u8; 16] = a.value(i).try_into()?;
            if seen.insert(id) {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

/// Positions 0, k, 2k, ... with k = max(1, distinct / n), at most n of them.
pub fn sample<T>(ids: &[T], n: usize) -> impl Iterator<Item = &T> {
    ids.iter().step_by((ids.len() / n.max(1)).max(1)).take(n)
}

pub fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex(s: &str) -> R<[u8; 16]> {
    let mut o = [0u8; 16];
    if s.len() != 32 {
        return Err(format!("bad trace id {s:?}").into());
    }
    for i in 0..16 {
        o[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)?;
    }
    Ok(o)
}

/// Both tables opened once, with the trace_id bloom filters of every spans row group preloaded.
pub struct Traces {
    spans: Table,
    pay: Table,
    blooms: Vec<Option<Sbbf>>,
}

pub const PRUNING: &str = "spans: row-group trace_id min/max statistics, then the trace_id bloom filter (preloaded at open); matching row groups read trace_id only to find row ranges, then all columns for those rows via page-index row selection. payloads: the same row ranges (tables are row-aligned), no per-file filter needed";

impl Traces {
    pub fn open(dir: &Path) -> R<Self> {
        let spans = Table::open(&dir.join("spans.parquet"))?;
        let pay = Table::open(&dir.join("payloads.parquet"))?;
        if spans.rows() != pay.rows() {
            return Err("spans and payloads row counts differ".into());
        }
        let c = spans.col("trace_id");
        let b = spans.builder()?;
        let blooms = (0..spans.groups()).map(|rg| b.get_row_group_column_bloom_filter(rg, c)).collect::<Result<_, _>>()?;
        Ok(Self { spans, pay, blooms })
    }

    /// Returns (rows of the trace in spans, raw payload value bytes of those rows).
    pub fn lookup(&self, id: &[u8; 16]) -> R<(usize, u64)> {
        let c = self.spans.col("trace_id");
        let (mut ranges, mut start): (Vec<Range<usize>>, usize) = (vec![], 0);
        for rg in 0..self.spans.groups() {
            let len = self.spans.meta.metadata().row_group(rg).num_rows() as usize;
            let inside = self.spans.minmax(rg, c).is_none_or(|(a, b)| a <= &id[..] && &id[..] <= b);
            if inside && self.blooms[rg].as_ref().is_none_or(|f| f.check(&id[..])) {
                let mut off = start;
                for b in self.spans.group_iter(rg, &["trace_id"], 8192)? {
                    let b = b?;
                    let a = b.column(0).as_fixed_size_binary();
                    for i in 0..a.len() {
                        if a.value(i) == id {
                            match ranges.last_mut() {
                                Some(r) if r.end == off + i => r.end += 1,
                                _ => ranges.push(off + i..off + i + 1),
                            }
                        }
                    }
                    off += a.len();
                }
            }
            start += len;
        }
        let rows: usize = self.spans.read_rows(&ranges, None)?.iter().map(|b| b.num_rows()).sum();
        let mut bytes = 0u64;
        for b in self.pay.read_rows(&ranges, Some(&PAY_COLS))? {
            for c in b.columns() {
                let o = c.as_binary::<i64>().value_offsets();
                bytes += (o[o.len() - 1] - o[0]) as u64;
            }
        }
        Ok((rows, bytes))
    }
}

pub fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((p / 100.0 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_rule() {
        let ids: Vec<u32> = (0..1000).collect();
        let s: Vec<_> = sample(&ids, 200).copied().collect();
        assert_eq!((s.len(), s[1], s[199]), (200, 5, 995)); // k = 5
        let s: Vec<_> = sample(&ids, 300).copied().collect(); // k = 3, 300 ids written
        assert_eq!((s.len(), s[299]), (300, 897));
        let few: Vec<u32> = (0..50).collect(); // k = max(1, 0) = 1, fewer ids than n
        assert_eq!(sample(&few, 200).count(), 50);
        let odd: Vec<u32> = (0..399).collect(); // k = 1, stops after n
        assert_eq!(sample(&odd, 200).count(), 200);
    }

    #[test]
    fn hex_roundtrip() {
        let id = [0xab, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255];
        assert_eq!(unhex(&hex(&id)).unwrap(), id);
    }

    #[test]
    fn window_middle_half() {
        assert_eq!(window(100, 500), (200, 400));
    }
}
