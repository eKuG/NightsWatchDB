//! `build`: the three physical layouts.
use crate::decode::{spans_schema, stream};
use crate::idx::{self, Rec};
use crate::{Variant, R};
use arrow::array::*;
use arrow::compute::{concat_batches, interleave_record_batch, take_record_batch};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use std::cmp::Ordering;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const SEG_RG_ROWS: usize = 16_384;
pub const BIG_RG_ROWS: usize = 65_536;
pub const SUMMARY_RG_ROWS: usize = 65_536;
const RUN_RG_ROWS: usize = 8_192;

pub struct Report {
    pub rows: u64,
    pub traces: u64,
    pub segments: u64,
    pub bytes: Vec<(&'static str, u64)>,
    pub input_bytes: u64,
    pub notes: Vec<(&'static str, String)>,
}

fn props(rg_rows: usize, bloom: bool) -> R<WriterProperties> {
    let mut b = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)).set_statistics_enabled(EnabledStatistics::Page).set_max_row_group_row_count(Some(rg_rows));
    if bloom {
        // sized for the most distinct ids one row group can hold; false-positive rate stays at the library default
        b = b.set_column_bloom_filter_max_ndv(ColumnPath::from("trace_id"), rg_rows as u64);
        b = b.set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true);
    }
    Ok(b.build())
}

fn writer(path: &Path, schema: SchemaRef, p: WriterProperties) -> R<ArrowWriter<BufWriter<File>>> {
    Ok(ArrowWriter::try_new(BufWriter::with_capacity(1 << 20, File::create(path)?), schema, Some(p))?)
}

fn close(w: ArrowWriter<BufWriter<File>>, path: &Path) -> R<u64> {
    w.close()?;
    Ok(std::fs::metadata(path)?.len())
}

pub fn run(input: &Path, out: &Path, v: Variant, window_s: u64, threads: usize, sort_mb: u64) -> R<Report> {
    std::fs::create_dir_all(out)?;
    match v {
        Variant::Cotrace => cotrace(input, out, window_s, threads),
        _ => sorted(input, out, v, threads, sort_mb << 20),
    }
}

// ---------------------------------------------------------------- cotrace

/// Everything the summary row and the trace index need to know about one trace.
#[derive(Default)]
struct Acc {
    id: [u8; 16],
    n: u32,
    start: i64,
    end: i64,
    roots: u32,
    root_start: i64,
    root_service: Option<String>,
    root_name: Option<String>,
    error: bool,
    in_tok: i64,
    out_tok: i64,
    models: Vec<String>,
    session: Option<String>,
    user: Option<String>,
}

struct Seg {
    w: ArrowWriter<BufWriter<File>>,
    path: PathBuf,
    window: i64,
    rows: u32,
}

struct Summary {
    w: ArrowWriter<BufWriter<File>>,
    schema: SchemaRef,
    rows: Vec<Acc>,
}

pub fn summary_schema() -> SchemaRef {
    let f = Field::new;
    Arc::new(Schema::new(vec![
        f("trace_id", DataType::FixedSizeBinary(16), false),
        f("start_ns", DataType::Int64, false),
        f("end_ns", DataType::Int64, false),
        f("service_name", DataType::Utf8, true),
        f("name", DataType::Utf8, true),
        f("span_count", DataType::Int32, false),
        f("error", DataType::Boolean, false),
        f("input_tokens", DataType::Int64, false),
        f("output_tokens", DataType::Int64, false),
        f("models", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        f("session_id", DataType::Utf8, true),
        f("user_id", DataType::Utf8, true),
    ]))
}

impl Summary {
    fn flush(&mut self) -> R<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let r = &self.rows;
        let mut id = FixedSizeBinaryBuilder::with_capacity(r.len(), 16);
        let mut models = ListBuilder::new(StringBuilder::new()).with_field(Field::new("item", DataType::Utf8, true));
        for a in r {
            id.append_value(a.id)?;
            models.append_value(a.models.iter().map(Some));
        }
        let s = |f: fn(&Acc) -> &Option<String>| Arc::new(r.iter().map(|a| f(a).as_deref()).collect::<StringArray>()) as ArrayRef;
        let cols: Vec<ArrayRef> = vec![
            Arc::new(id.finish()),
            Arc::new(r.iter().map(|a| a.start).collect::<Int64Array>()),
            Arc::new(r.iter().map(|a| a.end).collect::<Int64Array>()),
            s(|a| &a.root_service),
            s(|a| &a.root_name),
            Arc::new(r.iter().map(|a| a.n as i32).collect::<Int32Array>()),
            Arc::new(r.iter().map(|a| a.error).collect::<BooleanArray>()),
            Arc::new(r.iter().map(|a| a.in_tok).collect::<Int64Array>()),
            Arc::new(r.iter().map(|a| a.out_tok).collect::<Int64Array>()),
            Arc::new(models.finish()),
            s(|a| &a.session),
            s(|a| &a.user),
        ];
        self.w.write(&RecordBatch::try_new(self.schema.clone(), cols)?)?;
        self.rows.clear();
        Ok(())
    }
}

struct Cotrace<'a> {
    out: &'a Path,
    window_ns: i64,
    schema: SchemaRef,
    seg: Option<Seg>,
    segments: u64,
    seg_bytes: u64,
    index: Vec<Rec>,
    summary: Summary,
    acc: Option<Acc>,
    pending: Vec<RecordBatch>,
    rows: u64,
    multi_root: u64,
    rootless: u64,
    root_not_earliest: u64,
}

impl Cotrace<'_> {
    fn close_seg(&mut self) -> R<()> {
        if let Some(s) = self.seg.take() {
            self.seg_bytes += close(s.w, &s.path)?;
        }
        Ok(())
    }

    /// The trace in `acc` is complete and its rows are in `pending`: place it in a segment.
    fn finish_trace(&mut self) -> R<()> {
        let a = self.acc.take().expect("trace in progress");
        let window = a.start.div_euclid(self.window_ns);
        // a trace is never split: the window of its earliest span decides the segment
        if self.seg.as_ref().is_none_or(|s| window > s.window) {
            self.close_seg()?;
            let path = self.out.join(format!("seg-{:05}.parquet", self.segments));
            self.seg = Some(Seg { w: writer(&path, self.schema.clone(), props(SEG_RG_ROWS, false)?)?, path, window, rows: 0 });
            self.segments += 1;
        }
        let seg = self.seg.as_mut().unwrap();
        for b in self.pending.drain(..) {
            seg.w.write(&b)?;
        }
        self.index.push(Rec { trace_id: a.id, segment: (self.segments - 1) as u32, first_row: seg.rows, row_count: a.n, start_ns: a.start, end_ns: a.end });
        seg.rows += a.n;
        match a.roots {
            0 => self.rootless += 1,
            1 => {}
            _ => self.multi_root += 1,
        }
        if a.roots > 0 && a.root_start != a.start {
            self.root_not_earliest += 1;
        }
        self.summary.rows.push(a);
        if self.summary.rows.len() >= SUMMARY_RG_ROWS {
            self.summary.flush()?;
        }
        Ok(())
    }

    fn push(&mut self, b: RecordBatch) -> R<()> {
        let c = |n: &str| b.column_by_name(n).expect("column").as_ref();
        let (trace, parent, name) = (c("trace_id").as_fixed_size_binary(), c("parent_span_id").as_fixed_size_binary(), c("name").as_string::<i32>());
        let (start, end) = (c("start_ns").as_primitive::<Int64Type>(), c("end_ns").as_primitive::<Int64Type>());
        let (svc, model, err) = (c("service_name").as_string::<i32>(), c("request_model").as_string::<i32>(), c("error_type").as_string::<i32>());
        let (itok, otok) = (c("input_tokens").as_primitive::<Int64Type>(), c("output_tokens").as_primitive::<Int64Type>());
        let (sess, user) = (c("session_id").as_string::<i32>(), c("user_id").as_string::<i32>());
        self.rows += b.num_rows() as u64;
        let mut from = 0;
        for i in 0..b.num_rows() {
            let id: [u8; 16] = trace.value(i).try_into()?;
            if self.acc.as_ref().is_some_and(|a| a.id != id) {
                if i > from {
                    self.pending.push(b.slice(from, i - from));
                }
                from = i;
                self.finish_trace()?;
            }
            let a = self.acc.get_or_insert_with(|| Acc { id, start: i64::MAX, end: i64::MIN, ..Default::default() });
            a.n += 1;
            a.start = a.start.min(start.value(i));
            a.end = a.end.max(end.value(i));
            if parent.is_null(i) {
                a.roots += 1;
                if a.roots == 1 {
                    a.root_start = start.value(i);
                    a.root_service = svc.is_valid(i).then(|| svc.value(i).to_string());
                    a.root_name = Some(name.value(i).to_string());
                }
            }
            a.error |= err.is_valid(i);
            a.in_tok += if itok.is_valid(i) { itok.value(i) } else { 0 };
            a.out_tok += if otok.is_valid(i) { otok.value(i) } else { 0 };
            if model.is_valid(i) && !a.models.iter().any(|m| m == model.value(i)) {
                a.models.push(model.value(i).to_string());
            }
            if a.session.is_none() && sess.is_valid(i) {
                a.session = Some(sess.value(i).to_string());
            }
            if a.user.is_none() && user.is_valid(i) {
                a.user = Some(user.value(i).to_string());
            }
        }
        if from < b.num_rows() {
            self.pending.push(b.slice(from, b.num_rows() - from));
        }
        Ok(())
    }
}

fn cotrace(input: &Path, out: &Path, window_s: u64, threads: usize) -> R<Report> {
    let schema = spans_schema();
    let sp = out.join("summary.parquet");
    let mut c = Cotrace {
        out,
        window_ns: window_s as i64 * 1_000_000_000,
        schema,
        seg: None,
        segments: 0,
        seg_bytes: 0,
        index: vec![],
        summary: Summary { w: writer(&sp, summary_schema(), props(SUMMARY_RG_ROWS, false)?)?, schema: summary_schema(), rows: vec![] },
        acc: None,
        pending: vec![],
        rows: 0,
        multi_root: 0,
        rootless: 0,
        root_not_earliest: 0,
    };
    let input_bytes = stream(input, threads, |b| c.push(b))?;
    if c.acc.is_some() {
        c.finish_trace()?;
    }
    c.close_seg()?;
    c.summary.flush()?;
    let traces = c.index.len() as u64;
    let summary_bytes = close(c.summary.w, &sp)?;
    let ip = out.join("traces.idx");
    std::fs::write(&ip, idx::encode(c.index))?;
    Ok(Report {
        rows: c.rows,
        traces,
        segments: c.segments,
        bytes: vec![("segments", c.seg_bytes), ("traces.idx", std::fs::metadata(&ip)?.len()), ("summary.parquet", summary_bytes)],
        input_bytes,
        notes: vec![("multi_root_traces", c.multi_root.to_string()), ("rootless_traces", c.rootless.to_string()), ("root_not_earliest_traces", c.root_not_earliest.to_string())],
    })
}

// ---------------------------------------------------------------- svc and traceid: external sort

/// Sort keys of a batch. Ties are broken by `seq` (arrival order), so the order is total
/// and the same for every build.
pub struct Keys {
    v: Variant,
    svc: StringArray,
    name: StringArray,
    trace: FixedSizeBinaryArray,
    start: Int64Array,
    seq: UInt64Array,
}

impl Keys {
    fn of(b: &RecordBatch, v: Variant) -> Self {
        let c = |n: &str| b.column_by_name(n).expect("column").clone();
        let (svc, name, trace) = (c("service_name"), c("name"), c("trace_id"));
        Keys { v, svc: svc.as_string::<i32>().clone(), name: name.as_string::<i32>().clone(), trace: trace.as_fixed_size_binary().clone(), start: c("start_ns").as_primitive::<Int64Type>().clone(), seq: c("seq").as_primitive::<UInt64Type>().clone() }
    }
    fn cmp(&self, i: usize, o: &Keys, j: usize) -> Ordering {
        let tail = || self.start.value(i).cmp(&o.start.value(j)).then_with(|| self.seq.value(i).cmp(&o.seq.value(j)));
        match self.v {
            Variant::Svc => {
                opt(&self.svc, i).cmp(&opt(&o.svc, j)).then_with(|| self.name.value(i).cmp(o.name.value(j))).then_with(tail)
            }
            _ => self.trace.value(i).cmp(o.trace.value(j)).then_with(tail),
        }
    }
}

fn opt(k: &StringArray, i: usize) -> Option<&str> {
    k.is_valid(i).then(|| k.value(i))
}

fn run_schema() -> SchemaRef {
    let mut f: Vec<Arc<Field>> = spans_schema().fields().iter().cloned().collect();
    f.push(Arc::new(Field::new("seq", DataType::UInt64, false)));
    Arc::new(Schema::new(f))
}

fn with_seq(b: &RecordBatch, first: u64, schema: &SchemaRef) -> R<RecordBatch> {
    let mut cols = b.columns().to_vec();
    cols.push(Arc::new(UInt64Array::from_iter_values(first..first + b.num_rows() as u64)));
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// Sorts the buffered batches (one concat, one take: about twice the buffered bytes at peak) and writes a run.
fn write_run(pend: &mut Vec<RecordBatch>, v: Variant, schema: &SchemaRef, path: &Path) -> R<()> {
    let big = concat_batches(schema, pend.iter())?;
    pend.clear();
    let sorted = {
        let k = Keys::of(&big, v);
        let mut order: Vec<u32> = (0..big.num_rows() as u32).collect();
        order.sort_unstable_by(|&a, &b| k.cmp(a as usize, &k, b as usize));
        take_record_batch(&big, &UInt32Array::from(order))?
    };
    drop(big);
    let p = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?)).set_statistics_enabled(EnabledStatistics::None).set_max_row_group_row_count(Some(RUN_RG_ROWS)).build();
    let mut w = writer(path, schema.clone(), p)?;
    w.write(&sorted)?;
    close(w, path)?;
    Ok(())
}

struct Cur {
    rdr: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    b: RecordBatch,
    i: usize,
    k: Keys,
    done: bool,
}

impl Cur {
    fn open(path: &Path, v: Variant, schema: &SchemaRef) -> R<Self> {
        let rdr = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.with_batch_size(RUN_RG_ROWS).build()?;
        let b = RecordBatch::new_empty(schema.clone());
        let mut c = Cur { rdr, k: Keys::of(&b, v), b, i: 0, done: false };
        c.advance(v)?;
        Ok(c)
    }
    fn advance(&mut self, v: Variant) -> R<()> {
        match self.rdr.next().transpose()? {
            Some(b) => {
                self.k = Keys::of(&b, v);
                self.b = b;
                self.i = 0;
            }
            None => {
                self.done = true;
                self.b = RecordBatch::new_empty(self.b.schema());
            }
        }
        Ok(())
    }
}

fn sorted(input: &Path, out: &Path, v: Variant, threads: usize, budget: u64) -> R<Report> {
    let rs = run_schema();
    let runs_dir = out.join(".runs");
    std::fs::create_dir_all(&runs_dir)?;
    let (mut pend, mut pend_bytes, mut runs, mut rows, mut traces) = (Vec::<RecordBatch>::new(), 0u64, Vec::<PathBuf>::new(), 0u64, 0u64);
    let mut last: Option<[u8; 16]> = None;
    let input_bytes = stream(input, threads, |b| {
        // spans of a trace are adjacent, so id changes count traces
        let t = b.column_by_name("trace_id").expect("column").as_fixed_size_binary();
        for i in 0..t.len() {
            let id: [u8; 16] = t.value(i).try_into()?;
            traces += (last != Some(id)) as u64;
            last = Some(id);
        }
        let b = with_seq(&b, rows, &rs)?;
        rows += b.num_rows() as u64;
        pend_bytes += b.get_array_memory_size() as u64;
        pend.push(b);
        if pend_bytes >= budget {
            let p = runs_dir.join(format!("run-{:04}.parquet", runs.len()));
            write_run(&mut pend, v, &rs, &p)?;
            runs.push(p);
            pend_bytes = 0;
        }
        Ok(())
    })?;
    if !pend.is_empty() {
        let p = runs_dir.join(format!("run-{:04}.parquet", runs.len()));
        write_run(&mut pend, v, &rs, &p)?;
        runs.push(p);
    }
    eprintln!("sorted {rows} rows into {} runs, merging", runs.len());

    let outp = out.join("spans.parquet");
    let mut w = writer(&outp, spans_schema(), props(BIG_RG_ROWS, v == Variant::Svc)?)?;
    let mut cur = runs.iter().map(|p| Cur::open(p, v, &rs)).collect::<R<Vec<_>>>()?;
    let keep: Vec<usize> = (0..spans_schema().fields().len()).collect();
    let mut pick: Vec<(usize, usize)> = Vec::with_capacity(RUN_RG_ROWS);
    let flush = |w: &mut ArrowWriter<BufWriter<File>>, cur: &[Cur], pick: &mut Vec<(usize, usize)>| -> R<()> {
        if !pick.is_empty() {
            let bs: Vec<&RecordBatch> = cur.iter().map(|c| &c.b).collect();
            w.write(&interleave_record_batch(&bs, pick)?.project(&keep)?)?;
            pick.clear();
        }
        Ok(())
    };
    loop {
        // k is small (rows / run size), a linear scan beats a heap here
        let mut best: Option<usize> = None;
        for r in 0..cur.len() {
            if !cur[r].done && best.is_none_or(|b| cur[r].k.cmp(cur[r].i, &cur[b].k, cur[b].i) == Ordering::Less) {
                best = Some(r);
            }
        }
        let Some(r) = best else { break };
        pick.push((r, cur[r].i));
        cur[r].i += 1;
        let exhausted = cur[r].i == cur[r].b.num_rows();
        if exhausted || pick.len() >= RUN_RG_ROWS {
            flush(&mut w, &cur, &mut pick)?;
        }
        if exhausted {
            cur[r].advance(v)?;
        }
    }
    flush(&mut w, &cur, &mut pick)?;
    drop(cur);
    let bytes = close(w, &outp)?;
    std::fs::remove_dir_all(&runs_dir)?;
    Ok(Report { rows, traces, segments: 1, bytes: vec![("spans.parquet", bytes)], input_bytes, notes: vec![("sort_runs", runs.len().to_string()), ("sort_buffer_mb", (budget >> 20).to_string())] })
}
