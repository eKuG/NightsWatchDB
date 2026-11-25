//! OTLP records -> two Arrow tables -> two Parquet files.
use crate::R;
use arrow::array::*;
use arrow::compute::concat_batches;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, unbounded, Receiver};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, KeyValue};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use prost::Message;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

pub const BATCH_ROWS: usize = 8192;
pub const SPAN_RG_ROWS: usize = 65_536;
pub const PAY_RG_ROWS: usize = 2_048;
pub const PAY_COLS: [&str; 6] = ["input_messages", "output_messages", "system_instructions", "tool_definitions", "tool_call_arguments", "tool_call_result"];
// spans text columns, in the order of TXT_COLS below
pub const TXT_COLS: [&str; 9] = ["operation", "provider", "request_model", "conversation_id", "session_id", "user_id", "agent_name", "tool_name", "error_type"];

enum Slot {
    Txt(usize),
    Int(usize),
    Pay(usize),
}

fn classify(k: &str) -> Option<Slot> {
    Some(match k {
        "gen_ai.operation.name" => Slot::Txt(0),
        "gen_ai.provider.name" => Slot::Txt(1),
        "gen_ai.request.model" => Slot::Txt(2),
        "gen_ai.conversation.id" => Slot::Txt(3),
        "session.id" => Slot::Txt(4),
        "user.id" => Slot::Txt(5),
        "gen_ai.agent.name" => Slot::Txt(6),
        "gen_ai.tool.name" => Slot::Txt(7),
        "error.type" => Slot::Txt(8),
        "gen_ai.usage.input_tokens" => Slot::Int(0),
        "gen_ai.usage.output_tokens" => Slot::Int(1),
        "gen_ai.input.messages" => Slot::Pay(0),
        "gen_ai.output.messages" => Slot::Pay(1),
        "gen_ai.system_instructions" => Slot::Pay(2),
        "gen_ai.tool.definitions" => Slot::Pay(3),
        "gen_ai.tool.call.arguments" => Slot::Pay(4),
        "gen_ai.tool.call.result" => Slot::Pay(5),
        _ => return None,
    })
}

pub fn spans_schema() -> SchemaRef {
    let f = Field::new;
    let utf = |n| Field::new(n, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        f("trace_id", DataType::FixedSizeBinary(16), false),
        f("span_id", DataType::FixedSizeBinary(8), false),
        f("parent_span_id", DataType::FixedSizeBinary(8), true),
        f("name", DataType::Utf8, false),
        f("kind", DataType::Int32, false),
        f("start_ns", DataType::Int64, false),
        f("end_ns", DataType::Int64, false),
        f("status_code", DataType::Int32, false),
        utf("service_name"),
        utf(TXT_COLS[0]),
        utf(TXT_COLS[1]),
        utf(TXT_COLS[2]),
        f("input_tokens", DataType::Int64, true),
        f("output_tokens", DataType::Int64, true),
        utf(TXT_COLS[3]),
        utf(TXT_COLS[4]),
        utf(TXT_COLS[5]),
        utf(TXT_COLS[6]),
        utf(TXT_COLS[7]),
        utf(TXT_COLS[8]),
        f("other_attr_count", DataType::Int32, false),
    ]))
}

pub fn payloads_schema() -> SchemaRef {
    let mut v = vec![Field::new("trace_id", DataType::FixedSizeBinary(16), false), Field::new("span_id", DataType::FixedSizeBinary(8), false)];
    v.extend(PAY_COLS.iter().map(|n| Field::new(*n, DataType::LargeBinary, true)));
    Arc::new(Schema::new(v))
}

#[derive(Default, Clone)]
pub struct Stats {
    pub rows: u64,
    pub in_tok: i64,
    pub out_tok: i64,
    pub nonnull: [u64; 6],
    pub bytes: [u64; 6],
}

pub struct Decoded {
    spans: RecordBatch,
    pay: RecordBatch,
    st: Stats,
}

fn strval(kv: &KeyValue) -> Option<&str> {
    match kv.value.as_ref()?.value.as_ref()? {
        Value::StringValue(s) => Some(s),
        _ => None,
    }
}

fn intval(kv: &KeyValue) -> Option<i64> {
    match kv.value.as_ref()?.value.as_ref()? {
        Value::IntValue(i) => Some(*i),
        _ => None,
    }
}

/// Decode one framed request into one batch per table.
pub fn decode(buf: &[u8], ss: &SchemaRef, ps: &SchemaRef) -> R<Decoded> {
    let req = ExportTraceServiceRequest::decode(buf)?;
    let spans = || req.resource_spans.iter().flat_map(|r| r.scope_spans.iter().flat_map(move |s| s.spans.iter().map(move |sp| (r, sp))));
    let n = spans().count();
    let mut paycap = [0usize; 6];
    for (_, sp) in spans() {
        for kv in &sp.attributes {
            if let Some(Slot::Pay(i)) = classify(&kv.key) {
                paycap[i] += strval(kv).map_or(0, str::len);
            }
        }
    }
    let mut trace = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut parent = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut ptrace = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut pspan = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut name = StringBuilder::with_capacity(n, n * 32);
    let (mut kind, mut status, mut other) = (Int32Builder::with_capacity(n), Int32Builder::with_capacity(n), Int32Builder::with_capacity(n));
    let (mut start, mut end) = (Int64Builder::with_capacity(n), Int64Builder::with_capacity(n));
    let mut service = StringBuilder::with_capacity(n, n * 16);
    let mut txt: [StringBuilder; 9] = std::array::from_fn(|_| StringBuilder::with_capacity(n, n * 16));
    let mut ints: [Int64Builder; 2] = std::array::from_fn(|_| Int64Builder::with_capacity(n));
    let mut pay: [LargeBinaryBuilder; 6] = std::array::from_fn(|i| LargeBinaryBuilder::with_capacity(n, paycap[i]));
    let mut st = Stats { rows: n as u64, ..Default::default() };

    for (rs, sp) in spans() {
        let svc = rs.resource.as_ref().and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name")).and_then(strval);
        let (mut t, mut iv, mut pv, mut oth) = ([None::<&str>; 9], [None::<i64>; 2], [None::<&str>; 6], 0i32);
        for kv in &sp.attributes {
            match classify(&kv.key) {
                Some(Slot::Txt(i)) => t[i] = strval(kv),
                Some(Slot::Int(i)) => iv[i] = intval(kv),
                Some(Slot::Pay(i)) => pv[i] = strval(kv),
                None => oth += 1,
            }
        }
        trace.append_value(&sp.trace_id)?;
        span.append_value(&sp.span_id)?;
        ptrace.append_value(&sp.trace_id)?;
        pspan.append_value(&sp.span_id)?;
        if sp.parent_span_id.is_empty() {
            parent.append_null();
        } else {
            parent.append_value(&sp.parent_span_id)?;
        }
        name.append_value(&sp.name);
        kind.append_value(sp.kind);
        start.append_value(sp.start_time_unix_nano as i64);
        end.append_value(sp.end_time_unix_nano as i64);
        status.append_value(sp.status.as_ref().map_or(0, |s| s.code));
        service.append_option(svc);
        for i in 0..9 {
            txt[i].append_option(t[i]);
        }
        for i in 0..2 {
            ints[i].append_option(iv[i]);
        }
        st.in_tok += iv[0].unwrap_or(0);
        st.out_tok += iv[1].unwrap_or(0);
        other.append_value(oth);
        for i in 0..6 {
            pay[i].append_option(pv[i].map(str::as_bytes));
            if let Some(s) = pv[i] {
                st.nonnull[i] += 1;
                st.bytes[i] += s.len() as u64;
            }
        }
    }
    let [op, prov, model, conv, sess, user, agent, tool, err] = txt.map(|mut b| Arc::new(b.finish()) as ArrayRef);
    let [it, ot] = ints.map(|mut b| Arc::new(b.finish()) as ArrayRef);
    let a = |x: ArrayRef| x;
    let cols = vec![
        a(Arc::new(trace.finish())),
        a(Arc::new(span.finish())),
        a(Arc::new(parent.finish())),
        a(Arc::new(name.finish())),
        a(Arc::new(kind.finish())),
        a(Arc::new(start.finish())),
        a(Arc::new(end.finish())),
        a(Arc::new(status.finish())),
        a(Arc::new(service.finish())),
        op,
        prov,
        model,
        it,
        ot,
        conv,
        sess,
        user,
        agent,
        tool,
        err,
        a(Arc::new(other.finish())),
    ];
    let mut pcols: Vec<ArrayRef> = vec![Arc::new(ptrace.finish()), Arc::new(pspan.finish())];
    pcols.extend(pay.map(|mut b| Arc::new(b.finish()) as ArrayRef));
    Ok(Decoded { spans: RecordBatch::try_new(ss.clone(), cols)?, pay: RecordBatch::try_new(ps.clone(), pcols)?, st })
}

/// Re-chunks a stream of batches into batches of exactly `limit` rows (last one may be short).
pub struct Coalescer {
    schema: SchemaRef,
    limit: usize,
    pend: Vec<RecordBatch>,
    rows: usize,
}

impl Coalescer {
    pub fn new(schema: SchemaRef, limit: usize) -> Self {
        Self { schema, limit, pend: vec![], rows: 0 }
    }
    pub fn push(&mut self, b: RecordBatch) -> R<Vec<RecordBatch>> {
        let mut out = vec![];
        let mut off = 0;
        while off < b.num_rows() {
            let take = (self.limit - self.rows).min(b.num_rows() - off);
            self.pend.push(b.slice(off, take));
            self.rows += take;
            off += take;
            if self.rows == self.limit {
                out.extend(self.flush()?);
            }
        }
        Ok(out)
    }
    pub fn flush(&mut self) -> R<Option<RecordBatch>> {
        let r = match self.pend.len() {
            0 => None,
            1 => self.pend.pop(),
            _ => Some(concat_batches(&self.schema, &self.pend)?),
        };
        self.pend.clear();
        self.rows = 0;
        Ok(r)
    }
}

pub struct Report {
    pub st: Stats,
    pub input_bytes: u64,
    pub spans_bytes: u64,
    pub pay_bytes: u64,
    pub bloom: bool,
}

fn props(rg_rows: usize, bloom: bool) -> R<WriterProperties> {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(rg_rows));
    if bloom {
        // sized for the distinct values one row group can hold; false-positive rate stays at the library default
        b = b.set_column_bloom_filter_max_ndv(ColumnPath::from("trace_id"), rg_rows as u64);
        b = b.set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true);
    }
    Ok(b.build())
}

fn write_file(path: PathBuf, schema: SchemaRef, p: WriterProperties, rx: Receiver<RecordBatch>) -> R<(u64, bool)> {
    let mut w = ArrowWriter::try_new(BufWriter::with_capacity(1 << 20, File::create(&path)?), schema, Some(p))?;
    for b in rx {
        w.write(&b)?;
    }
    let md = w.close()?;
    let bloom = md.row_groups().first().is_some_and(|g| g.column(0).bloom_filter_offset().is_some());
    Ok((std::fs::metadata(&path)?.len(), bloom))
}

/// Runs the whole pipeline: reader -> `threads` decode workers -> ordered coalescer -> one writer per file.
/// `out = None` drops batches after building them. `stop` ends the read early; `progress` counts input bytes read.
pub fn run(input: &Path, out: Option<&Path>, threads: usize, stop: &AtomicBool, progress: &AtomicU64) -> R<Report> {
    let (ss, ps) = (spans_schema(), payloads_schema());
    let (in_tx, in_rx) = bounded::<(u64, Vec<u8>)>(threads * 2);
    let (out_tx, out_rx) = bounded::<(u64, Decoded)>(threads * 2);
    let (pool_tx, pool_rx) = unbounded::<Vec<u8>>();
    let (sp_tx, sp_rx) = bounded::<RecordBatch>(2);
    let (py_tx, py_rx) = bounded::<RecordBatch>(2);
    let writers = out.is_some();
    std::thread::scope(|s| -> R<Report> {
        let reader = s.spawn(move || -> R<u64> {
            let mut r = BufReader::with_capacity(1 << 20, File::open(input)?);
            let (mut seq, mut total) = (0u64, 0u64);
            while !stop.load(Relaxed) && !r.fill_buf()?.is_empty() {
                let mut h = [0u8; 4];
                r.read_exact(&mut h)?;
                let n = u32::from_be_bytes(h) as usize;
                let mut buf = pool_rx.try_recv().unwrap_or_default();
                buf.resize(n, 0);
                r.read_exact(&mut buf)?;
                total += 4 + n as u64;
                progress.fetch_add(4 + n as u64, Relaxed);
                if in_tx.send((seq, buf)).is_err() {
                    break;
                }
                seq += 1;
            }
            Ok(total)
        });
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let (rx, tx, pool, ss, ps) = (in_rx.clone(), out_tx.clone(), pool_tx.clone(), ss.clone(), ps.clone());
                s.spawn(move || -> R<()> {
                    for (seq, buf) in rx {
                        let d = decode(&buf, &ss, &ps)?;
                        let _ = pool.send(buf);
                        if tx.send((seq, d)).is_err() {
                            break;
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        drop((in_rx, out_tx, pool_tx));
        let wr = out.map(|dir| {
            let a = s.spawn(move || write_file(dir.join("spans.parquet"), ss.clone(), props(SPAN_RG_ROWS, true)?, sp_rx));
            let b = s.spawn(move || write_file(dir.join("payloads.parquet"), ps.clone(), props(PAY_RG_ROWS, false)?, py_rx));
            (a, b)
        });
        // ordered stage
        let (mut cs, mut cp) = (Coalescer::new(spans_schema(), BATCH_ROWS), Coalescer::new(payloads_schema(), BATCH_ROWS));
        let (mut held, mut next, mut st) = (BTreeMap::new(), 0u64, Stats::default());
        let send = |tx: &crossbeam_channel::Sender<RecordBatch>, bs: Vec<RecordBatch>| {
            if writers {
                bs.into_iter().for_each(|b| drop(tx.send(b)));
            }
        };
        let mut res = Ok(());
        for (seq, d) in out_rx {
            held.insert(seq, d);
            while let Some(d) = held.remove(&next) {
                next += 1;
                st.rows += d.st.rows;
                st.in_tok += d.st.in_tok;
                st.out_tok += d.st.out_tok;
                for i in 0..6 {
                    st.nonnull[i] += d.st.nonnull[i];
                    st.bytes[i] += d.st.bytes[i];
                }
                match (cs.push(d.spans), cp.push(d.pay)) {
                    (Ok(a), Ok(b)) => {
                        send(&sp_tx, a);
                        send(&py_tx, b);
                    }
                    (Err(e), _) | (_, Err(e)) => res = Err(e),
                }
            }
        }
        if res.is_ok() {
            send(&sp_tx, cs.flush()?.into_iter().collect());
            send(&py_tx, cp.flush()?.into_iter().collect());
        }
        drop((sp_tx, py_tx));
        for w in workers {
            w.join().map_err(|_| "decode worker panicked")??;
        }
        let input_bytes = reader.join().map_err(|_| "reader panicked")??;
        res?;
        let (mut sbytes, mut pbytes, mut bloom) = (0, 0, false);
        if let Some((a, b)) = wr {
            (sbytes, bloom) = a.join().map_err(|_| "writer panicked")??;
            pbytes = b.join().map_err(|_| "writer panicked")??.0;
        }
        Ok(Report { st, input_bytes, spans_bytes: sbytes, pay_bytes: pbytes, bloom })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescer_rechunks() {
        let sch = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let b = |n: i32| RecordBatch::try_new(sch.clone(), vec![Arc::new(Int32Array::from_iter_values(0..n))]).unwrap();
        let mut c = Coalescer::new(sch.clone(), 4);
        let mut sizes = vec![];
        for n in [3, 3, 7] {
            sizes.extend(c.push(b(n)).unwrap().iter().map(|x| x.num_rows()));
        }
        sizes.extend(c.flush().unwrap().iter().map(|x| x.num_rows()));
        assert_eq!(sizes, [4, 4, 4, 1]);
    }
}
