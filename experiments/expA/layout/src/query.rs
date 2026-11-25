//! Read side: params, sample-ids, open, list1, list2, agg over any of the three layouts.
use crate::idx::Index;
use crate::{Variant, R};
use arrow::array::*;
use arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};

type Builder = ParquetRecordBatchReaderBuilder<File>;

/// One Parquet file with its footer and page index loaded once and its file handle kept open.
pub struct Table {
    path: PathBuf,
    file: File,
    meta: ArrowReaderMetadata,
}

impl Table {
    pub fn open(path: &Path) -> R<Self> {
        let file = File::open(path)?;
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional))?;
        Ok(Self { path: path.into(), file, meta })
    }
    /// A reader on the shared handle. Duplicated handles share one file offset, so only one thread may use these at a time.
    fn builder(&self) -> R<Builder> {
        Ok(Builder::new_with_metadata(self.file.try_clone()?, self.meta.clone()))
    }
    fn builder_on(&self, f: File) -> Builder {
        Builder::new_with_metadata(f, self.meta.clone())
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
    fn rg_rows(&self, rg: usize) -> usize {
        self.meta.metadata().row_group(rg).num_rows() as usize
    }
    fn minmax(&self, rg: usize, col: usize) -> Option<(&[u8], &[u8])> {
        let s = self.meta.metadata().row_group(rg).column(col).statistics()?;
        Some((s.min_bytes_opt()?, s.max_bytes_opt()?))
    }
    fn null_count(&self, rg: usize, col: usize) -> Option<u64> {
        self.meta.metadata().row_group(rg).column(col).statistics()?.null_count_opt()
    }
    /// Reads the given row ranges (local to one row group); the reader skips pages the ranges do not touch.
    fn read_group(&self, rg: usize, local: &[Range<usize>], names: Option<&[&str]>) -> R<Vec<RecordBatch>> {
        let mut b = self.builder()?.with_row_groups(vec![rg]).with_batch_size(8192).with_row_selection(RowSelection::from_consecutive_ranges(local.iter().cloned(), self.rg_rows(rg)));
        if let Some(n) = names {
            b = b.with_projection(self.mask(n));
        }
        b.build()?.map(|x| x.map_err(Into::into)).collect()
    }
    /// Reads global row ranges (sorted, non-overlapping) with all columns.
    fn read_rows(&self, ranges: &[Range<usize>]) -> R<Vec<RecordBatch>> {
        let (mut out, mut start) = (vec![], 0usize);
        for rg in 0..self.groups() {
            let len = self.rg_rows(rg);
            let local: Vec<_> = ranges.iter().filter(|r| r.start < start + len && r.end > start).map(|r| r.start.max(start) - start..r.end.min(start + len) - start).collect();
            start += len;
            if !local.is_empty() {
                out.extend(self.read_group(rg, &local, None)?);
            }
        }
        Ok(out)
    }
    /// Row ranges (local to the row group) of the pages whose trace_id min/max can contain `id`.
    /// The whole row group when no page index is present.
    fn pages_for(&self, rg: usize, col: usize, id: &[u8; 16]) -> Vec<Range<usize>> {
        let n = self.rg_rows(rg);
        let md = self.meta.metadata();
        let Some(pi) = md.page_index() else { return vec![0..n] };
        let (Some(ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(ci)), Some(oi)) = (pi.column_index(rg, col), pi.offset_index(rg, col)) else { return vec![0..n] };
        let locs = oi.page_locations();
        let mut out: Vec<Range<usize>> = vec![];
        for p in 0..locs.len() {
            let hit = match (ci.min_value(p), ci.max_value(p)) {
                (Some(a), Some(b)) => a <= &id[..] && &id[..] <= b,
                _ => false,
            };
            if hit {
                let r = locs[p].first_row_index as usize..locs.get(p + 1).map_or(n, |l| l.first_row_index as usize);
                match out.last_mut() {
                    Some(l) if l.end == r.start => l.end = r.end,
                    _ => out.push(r),
                }
            }
        }
        out
    }
}

/// A set of files opened once: the segments (or the single span file), plus the extras of `cotrace`.
pub struct Layout {
    spans: Vec<Table>,
    summary: Option<Table>,
    index: Option<Index>,
    blooms: Vec<Vec<Option<Sbbf>>>,
}

impl Layout {
    pub fn open(dir: &Path, v: Variant) -> R<Self> {
        let mut l = Layout { spans: vec![], summary: None, index: None, blooms: vec![] };
        if v == Variant::Cotrace {
            let mut files: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("seg-") && n.ends_with(".parquet"))).collect();
            files.sort();
            l.spans = files.iter().map(|p| Table::open(p)).collect::<R<_>>()?;
            l.summary = Some(Table::open(&dir.join("summary.parquet"))?);
            l.index = Some(Index::load(&dir.join("traces.idx"))?);
        } else {
            l.spans = vec![Table::open(&dir.join("spans.parquet"))?];
            if v == Variant::Svc {
                let t = &l.spans[0];
                let c = t.col("trace_id");
                let b = t.builder()?;
                l.blooms = vec![(0..t.groups()).map(|rg| b.get_row_group_column_bloom_filter(rg, c)).collect::<Result<_, _>>()?];
            }
        }
        if l.spans.is_empty() {
            return Err("no span files found".into());
        }
        Ok(l)
    }

    fn all_groups(&self) -> Vec<(usize, usize)> {
        self.spans.iter().enumerate().flat_map(|(t, s)| (0..s.groups()).map(move |g| (t, g))).collect()
    }

    /// (min, max) start_ns over all spans, from column statistics.
    pub fn time_range(&self) -> R<(i64, i64)> {
        let mut r: Option<(i64, i64)> = None;
        for t in &self.spans {
            let c = t.col("start_ns");
            for rg in 0..t.groups() {
                let (lo, hi) = t.minmax(rg, c).map(|(a, b)| (i64_of(a), i64_of(b))).ok_or("start_ns statistics missing")?;
                r = Some(r.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
            }
        }
        r.ok_or_else(|| "empty dataset".into())
    }
}

fn i64_of(b: &[u8]) -> i64 {
    i64::from_le_bytes(b.try_into().expect("8-byte statistic"))
}

/// The middle half of the start-time range: [min + d/4, min + 3d/4).
pub fn window(min: i64, max: i64) -> (i64, i64) {
    let d = (max - min) as i128;
    (min + (d / 4) as i64, min + (3 * d / 4) as i64)
}

/// Streams the projected columns of the listed row groups over `threads` workers; each worker folds into its own state.
fn scan<T: Send>(tabs: &[Table], groups: &[(usize, usize)], names: &[&str], threads: usize, init: impl Fn() -> T + Sync, f: impl Fn(&mut T, &RecordBatch) + Sync) -> R<Vec<T>> {
    let threads = threads.max(1);
    let parts: Vec<R<T>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|w| {
                let (init, f) = (&init, &f);
                s.spawn(move || -> R<T> {
                    let mut st = init();
                    // each worker opens its own handle per file: reads seek, and a shared handle would race
                    let mut files: Vec<Option<File>> = (0..tabs.len()).map(|_| None).collect();
                    for &(t, rg) in groups.iter().skip(w).step_by(threads) {
                        let tab = &tabs[t];
                        if files[t].is_none() {
                            files[t] = Some(File::open(&tab.path)?);
                        }
                        for b in tab.builder_on(files[t].as_ref().unwrap().try_clone()?).with_row_groups(vec![rg]).with_projection(tab.mask(names)).with_batch_size(8192).build()? {
                            // the reader returns columns in file order; hand them over in the order asked for
                            let b = b?;
                            let order: Vec<usize> = names.iter().map(|n| b.schema().index_of(n).expect("projected column")).collect();
                            f(&mut st, &b.project(&order)?);
                        }
                    }
                    Ok(st)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("scan worker")).collect()
    });
    parts.into_iter().collect()
}

// ---------------------------------------------------------------- params

pub struct Params {
    pub service: String,
    pub model: String,
    pub from: i64,
    pub to: i64,
}

fn top(m: HashMap<String, u64>) -> Option<String> {
    // most frequent, ties to the smaller string
    m.into_iter().max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0))).map(|x| x.0)
}

/// S: most frequent service_name among root spans. M: most frequent request_model among all spans.
pub fn params(l: &Layout, threads: usize) -> R<Params> {
    let r = scan(&l.spans, &l.all_groups(), &["parent_span_id", "service_name", "request_model"], threads, || (HashMap::<String, u64>::new(), HashMap::<String, u64>::new()), |(svc, mdl), b| {
        let (p, s, m) = (b.column(0), b.column(1).as_string::<i32>(), b.column(2).as_string::<i32>());
        for i in 0..b.num_rows() {
            if p.is_null(i) && s.is_valid(i) {
                *svc.entry(s.value(i).to_string()).or_default() += 1;
            }
            if m.is_valid(i) {
                *mdl.entry(m.value(i).to_string()).or_default() += 1;
            }
        }
    })?;
    let (mut svc, mut mdl) = (HashMap::new(), HashMap::new());
    for (a, b) in r {
        a.into_iter().for_each(|(k, v)| *svc.entry(k).or_default() += v);
        b.into_iter().for_each(|(k, v)| *mdl.entry(k).or_default() += v);
    }
    let (lo, hi) = l.time_range()?;
    let (from, to) = window(lo, hi);
    Ok(Params { service: top(svc).ok_or("no root service_name in the data")?, model: top(mdl).ok_or("no request_model in the data")?, from, to })
}

// ---------------------------------------------------------------- sample-ids

/// Trace ids in start order (earliest span start, ties by id), one entry per trace.
fn traces_by_start(l: &Layout, threads: usize) -> R<Vec<(i64, [u8; 16])>> {
    let mut all: Vec<(i64, [u8; 16])> = if let Some(s) = &l.summary {
        let g: Vec<_> = (0..s.groups()).map(|g| (0, g)).collect();
        let parts = scan(std::slice::from_ref(s), &g, &["trace_id", "start_ns"], threads, Vec::new, |v: &mut Vec<(i64, [u8; 16])>, b| {
            let (t, s) = (b.column(0).as_fixed_size_binary(), b.column(1).as_primitive::<Int64Type>());
            v.extend((0..b.num_rows()).map(|i| (s.value(i), t.value(i).try_into().unwrap())));
        })?;
        parts.into_iter().flatten().collect()
    } else {
        let parts = scan(&l.spans, &l.all_groups(), &["trace_id", "start_ns"], threads, HashMap::<[u8; 16], i64>::new, |m, b| {
            let (t, s) = (b.column(0).as_fixed_size_binary(), b.column(1).as_primitive::<Int64Type>());
            for i in 0..b.num_rows() {
                let e = m.entry(t.value(i).try_into().unwrap()).or_insert(i64::MAX);
                *e = (*e).min(s.value(i));
            }
        })?;
        let mut m: HashMap<[u8; 16], i64> = HashMap::new();
        for p in parts {
            for (k, v) in p {
                let e = m.entry(k).or_insert(i64::MAX);
                *e = (*e).min(v);
            }
        }
        m.into_iter().map(|(k, v)| (v, k)).collect()
    };
    all.sort();
    Ok(all)
}

/// Positions 0, k, 2k, ... with k = max(1, distinct / n), at most n of them.
pub fn sample<T>(ids: &[T], n: usize) -> impl Iterator<Item = &T> {
    ids.iter().step_by((ids.len() / n.max(1)).max(1)).take(n)
}

/// Returns (distinct traces, sampled ids).
pub fn sample_ids(l: &Layout, n: usize, threads: usize) -> R<(usize, Vec<[u8; 16]>)> {
    let t = traces_by_start(l, threads)?;
    Ok((t.len(), sample(&t, n).map(|x| x.1).collect()))
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

// ---------------------------------------------------------------- open

impl Layout {
    /// All rows of one trace ordered by start_ns. Footers, page indexes, bloom filters and the trace index are already in memory.
    pub fn open_trace(&self, id: &[u8; 16]) -> R<Option<RecordBatch>> {
        let batches = if let Some(ix) = &self.index {
            let Some(r) = ix.find(id) else { return Ok(None) };
            self.spans[r.segment as usize].read_rows(&[r.first_row as usize..(r.first_row + r.row_count) as usize])?
        } else {
            let t = &self.spans[0];
            let c = t.col("trace_id");
            let (mut ranges, mut start) = (Vec::<Range<usize>>::new(), 0usize);
            for rg in 0..t.groups() {
                let len = t.rg_rows(rg);
                let inside = t.minmax(rg, c).is_none_or(|(a, b)| a <= &id[..] && &id[..] <= b);
                if inside && self.blooms.first().and_then(|b| b[rg].as_ref()).is_none_or(|f| f.check(&id[..])) {
                    // candidate pages, then the exact rows inside them
                    let pages = t.pages_for(rg, c, id);
                    if !pages.is_empty() {
                        let mut rows = pages.iter().flat_map(|r| r.clone());
                        for b in t.read_group(rg, &pages, Some(&["trace_id"]))? {
                            let a = b.column(0).as_fixed_size_binary();
                            for i in 0..a.len() {
                                let row = rows.next().expect("row per selected position");
                                if a.value(i) == id {
                                    match ranges.last_mut() {
                                        Some(r) if r.end == start + row => r.end += 1,
                                        _ => ranges.push(start + row..start + row + 1),
                                    }
                                }
                            }
                        }
                    }
                }
                start += len;
            }
            if ranges.is_empty() {
                return Ok(None);
            }
            t.read_rows(&ranges)?
        };
        let Some(first) = batches.first() else { return Ok(None) };
        let all = concat_batches(&first.schema(), &batches)?;
        let order = sort_to_indices(all.column_by_name("start_ns").expect("column"), None, None)?;
        Ok(Some(take_record_batch(&all, &order)?))
    }
}

/// (rows, order-independent digest of (span_id, start_ns, end_ns, name) over the rows).
pub fn digest(b: &RecordBatch) -> (usize, u64) {
    let (s, st, en, n) = (b.column_by_name("span_id").unwrap().as_fixed_size_binary(), b.column_by_name("start_ns").unwrap().as_primitive::<Int64Type>(), b.column_by_name("end_ns").unwrap().as_primitive::<Int64Type>(), b.column_by_name("name").unwrap().as_string::<i32>());
    let mut d = 0u64;
    for i in 0..b.num_rows() {
        let mut h = Sha256::new();
        h.update(s.value(i));
        h.update(st.value(i).to_le_bytes());
        h.update(en.value(i).to_le_bytes());
        h.update(n.value(i).as_bytes());
        d = d.wrapping_add(u64::from_le_bytes(h.finalize()[..8].try_into().unwrap()));
    }
    (b.num_rows(), d)
}

// ---------------------------------------------------------------- list1, list2

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub id: [u8; 16],
    pub start: i64,
    pub dur: i64,
    pub name: String,
}

/// Newest first, ties by trace id; at most 50 distinct traces.
fn newest(mut v: Vec<Item>) -> Vec<Item> {
    v.sort_by(|a, b| b.start.cmp(&a.start).then_with(|| a.id.cmp(&b.id)));
    v.dedup_by(|b, a| a.id == b.id);
    v.truncate(50);
    v
}

/// SHA-256 of the ids written as lowercase hex and concatenated in order (50 x 32 = 1600 ASCII characters), as lowercase hex.
pub fn result_sha256(v: &[Item]) -> String {
    let mut h = Sha256::new();
    v.iter().for_each(|i| h.update(hex(&i.id)));
    hex(&h.finalize())
}

/// The 50 newest traces whose root span has service_name = S and whose start is in the window.
pub fn list1(l: &Layout, p: &Params, threads: usize) -> R<Vec<Item>> {
    let (from, to, svc) = (p.from, p.to, p.service.as_str());
    if let Some(s) = &l.summary {
        let cs = s.col("start_ns");
        let g: Vec<_> = (0..s.groups()).filter(|&g| s.minmax(g, cs).is_none_or(|(a, b)| i64_of(b) >= from && i64_of(a) < to)).map(|g| (0, g)).collect();
        let parts = scan(std::slice::from_ref(s), &g, &["trace_id", "start_ns", "end_ns", "service_name", "name"], threads, Vec::new, |v: &mut Vec<Item>, b| {
            let (t, st, en, sv, nm) = (b.column(0).as_fixed_size_binary(), b.column(1).as_primitive::<Int64Type>(), b.column(2).as_primitive::<Int64Type>(), b.column(3).as_string::<i32>(), b.column(4).as_string::<i32>());
            for i in 0..b.num_rows() {
                if sv.is_valid(i) && sv.value(i) == svc && st.value(i) >= from && st.value(i) < to {
                    v.push(Item { id: t.value(i).try_into().unwrap(), start: st.value(i), dur: en.value(i) - st.value(i), name: if nm.is_valid(i) { nm.value(i).to_string() } else { String::new() } });
                }
            }
        })?;
        return Ok(newest(parts.into_iter().flatten().collect()));
    }
    let t = &l.spans[0];
    let (cs, cv, cp) = (t.col("start_ns"), t.col("service_name"), t.col("parent_span_id"));
    let g: Vec<_> = (0..t.groups())
        .filter(|&g| {
            t.minmax(g, cs).is_none_or(|(a, b)| i64_of(b) >= from && i64_of(a) < to) && t.minmax(g, cv).is_none_or(|(a, b)| a <= svc.as_bytes() && svc.as_bytes() <= b) && t.null_count(g, cp).is_none_or(|n| n > 0)
        })
        .map(|g| (0, g))
        .collect();
    let parts = scan(&l.spans, &g, &["trace_id", "parent_span_id", "start_ns", "end_ns", "service_name", "name"], threads, Vec::new, |v: &mut Vec<Item>, b| {
        let (t, pa, st, en, sv, nm) = (b.column(0).as_fixed_size_binary(), b.column(1), b.column(2).as_primitive::<Int64Type>(), b.column(3).as_primitive::<Int64Type>(), b.column(4).as_string::<i32>(), b.column(5).as_string::<i32>());
        for i in 0..b.num_rows() {
            if pa.is_null(i) && sv.is_valid(i) && sv.value(i) == svc && st.value(i) >= from && st.value(i) < to {
                v.push(Item { id: t.value(i).try_into().unwrap(), start: st.value(i), dur: en.value(i) - st.value(i), name: nm.value(i).to_string() });
            }
        }
    })?;
    Ok(newest(parts.into_iter().flatten().collect()))
}

/// The 50 newest traces in the window with at least one error span and at least one span of model M.
pub fn list2(l: &Layout, p: &Params, threads: usize) -> R<Vec<Item>> {
    let (from, to, model) = (p.from, p.to, p.model.as_str());
    if let Some(s) = &l.summary {
        let cs = s.col("start_ns");
        let g: Vec<_> = (0..s.groups()).filter(|&g| s.minmax(g, cs).is_none_or(|(a, b)| i64_of(b) >= from && i64_of(a) < to)).map(|g| (0, g)).collect();
        let parts = scan(std::slice::from_ref(s), &g, &["trace_id", "start_ns", "end_ns", "error", "models"], threads, Vec::new, |v: &mut Vec<Item>, b| {
            let (t, st, en, er, ml) = (b.column(0).as_fixed_size_binary(), b.column(1).as_primitive::<Int64Type>(), b.column(2).as_primitive::<Int64Type>(), b.column(3).as_boolean(), b.column(4).as_list::<i32>());
            for i in 0..b.num_rows() {
                if er.value(i) && st.value(i) >= from && st.value(i) < to {
                    let m = ml.value(i);
                    if m.as_string::<i32>().iter().any(|x| x == Some(model)) {
                        v.push(Item { id: t.value(i).try_into().unwrap(), start: st.value(i), dur: en.value(i) - st.value(i), name: String::new() });
                    }
                }
            }
        })?;
        return Ok(newest(parts.into_iter().flatten().collect()));
    }
    // every span of a trace is needed to know its start and whether it has both kinds of span, so the scan covers all row groups
    let parts = scan(&l.spans, &l.all_groups(), &["trace_id", "start_ns", "error_type", "request_model"], threads, HashMap::<[u8; 16], (i64, bool, bool)>::new, |m, b| {
        let (t, st, er, md) = (b.column(0).as_fixed_size_binary(), b.column(1).as_primitive::<Int64Type>(), b.column(2), b.column(3).as_string::<i32>());
        for i in 0..b.num_rows() {
            let e = m.entry(t.value(i).try_into().unwrap()).or_insert((i64::MAX, false, false));
            e.0 = e.0.min(st.value(i));
            e.1 |= er.is_valid(i);
            e.2 |= md.is_valid(i) && md.value(i) == model;
        }
    })?;
    let mut m: HashMap<[u8; 16], (i64, bool, bool)> = HashMap::new();
    for part in parts {
        for (k, v) in part {
            let e = m.entry(k).or_insert((i64::MAX, false, false));
            *e = (e.0.min(v.0), e.1 | v.1, e.2 | v.2);
        }
    }
    Ok(newest(m.into_iter().filter(|(_, v)| v.1 && v.2 && v.0 >= from && v.0 < to).map(|(id, v)| Item { id, start: v.0, dur: 0, name: String::new() }).collect()))
}

// ---------------------------------------------------------------- agg

pub type AggRow = (Option<String>, u64, i64, i64);

/// chat spans with start_ns in [from, to), grouped by request_model. Row groups are pruned on start_ns and operation statistics.
pub fn agg(l: &Layout, p: &Params, threads: usize) -> R<Vec<AggRow>> {
    let (from, to) = (p.from, p.to);
    let g: Vec<_> = l
        .all_groups()
        .into_iter()
        .filter(|&(t, rg)| {
            let tab = &l.spans[t];
            let ok_t = tab.minmax(rg, tab.col("start_ns")).is_none_or(|(a, b)| i64_of(b) >= from && i64_of(a) < to);
            let ok_o = tab.minmax(rg, tab.col("operation")).is_none_or(|(a, b)| a <= &b"chat"[..] && &b"chat"[..] <= b);
            ok_t && ok_o
        })
        .collect();
    let parts = scan(&l.spans, &g, &["operation", "start_ns", "request_model", "input_tokens", "output_tokens"], threads, Vec::<AggRow>::new, |acc, b| {
        let (op, st, md, it, ot) = (b.column(0).as_string::<i32>(), b.column(1).as_primitive::<Int64Type>(), b.column(2).as_string::<i32>(), b.column(3).as_primitive::<Int64Type>(), b.column(4).as_primitive::<Int64Type>());
        for i in 0..b.num_rows() {
            if op.is_null(i) || op.value(i) != "chat" || st.value(i) < from || st.value(i) >= to {
                continue;
            }
            let m = md.is_valid(i).then(|| md.value(i));
            let slot = match acc.iter().position(|a| a.0.as_deref() == m) {
                Some(s) => s,
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
    })?;
    let mut all: Vec<AggRow> = vec![];
    for r in parts.into_iter().flatten() {
        match all.iter_mut().find(|a| a.0 == r.0) {
            Some(a) => (a.1, a.2, a.3) = (a.1 + r.1, a.2 + r.2, a.3 + r.3),
            None => all.push(r),
        }
    }
    all.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(all)
}

pub fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((p / 100.0 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1]
}
