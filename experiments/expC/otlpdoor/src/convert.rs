//! Arrow builders for the `spans`/`payloads` tables (and flat logs/metrics tables).
//! Batches close at MAX_ROWS rows or MAX_BYTES of value bytes, whichever comes first.
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point};
use prost::Message;
use std::sync::Arc;

pub const MAX_ROWS: usize = 8192;
pub const MAX_BYTES: usize = 32 << 20;
pub const PAY_COLS: [&str; 6] = ["input_messages", "output_messages", "system_instructions", "tool_definitions", "tool_call_arguments", "tool_call_result"];
pub const TXT_COLS: [&str; 9] = ["operation", "provider", "request_model", "conversation_id", "session_id", "user_id", "agent_name", "tool_name", "error_type"];

pub enum Slot {
    Txt(usize),
    Int(usize),
    Pay(usize),
}

pub fn classify(k: &str) -> Option<Slot> {
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

/// One span as both decoders present it. Slices borrow from the request (lazy) or from
/// the decoded message (full); the builders copy out of them.
#[derive(Default)]
pub struct SpanRow<'a> {
    pub trace_id: &'a [u8],
    pub span_id: &'a [u8],
    pub parent: &'a [u8],
    pub name: &'a str,
    pub kind: i32,
    pub start: i64,
    pub end: i64,
    pub status: i32,
    pub service: Option<&'a str>,
    pub txt: [Option<&'a str>; 9],
    pub ints: [Option<i64>; 2],
    pub pay: [Option<&'a [u8]>; 6],
    pub other: i32,
}

impl SpanRow<'_> {
    fn value_bytes(&self) -> usize {
        self.name.len() + self.service.map_or(0, str::len) + self.txt.iter().flatten().map(|s| s.len()).sum::<usize>() + self.pay.iter().flatten().map(|s| s.len()).sum::<usize>()
    }
}

/// Check block: computed from the finished Arrow batches.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct Check {
    pub rows: u64,
    pub in_tok: i64,
    pub out_tok: i64,
    pub nonnull: [u64; 6],
    pub bytes: [u64; 6],
}

struct Cols {
    trace: FixedSizeBinaryBuilder,
    span: FixedSizeBinaryBuilder,
    parent: FixedSizeBinaryBuilder,
    ptrace: FixedSizeBinaryBuilder,
    pspan: FixedSizeBinaryBuilder,
    name: StringBuilder,
    kind: Int32Builder,
    status: Int32Builder,
    other: Int32Builder,
    start: Int64Builder,
    end: Int64Builder,
    service: StringBuilder,
    txt: [StringBuilder; 9],
    ints: [Int64Builder; 2],
    pay: [LargeBinaryBuilder; 6],
}

impl Cols {
    /// Payload builders reserve their byte bound up front so large values are never moved
    /// by a growing buffer; untouched capacity costs address space, not resident memory.
    fn new(rows: usize, bytes: usize) -> Self {
        Cols {
            trace: FixedSizeBinaryBuilder::with_capacity(rows, 16),
            span: FixedSizeBinaryBuilder::with_capacity(rows, 8),
            parent: FixedSizeBinaryBuilder::with_capacity(rows, 8),
            ptrace: FixedSizeBinaryBuilder::with_capacity(rows, 16),
            pspan: FixedSizeBinaryBuilder::with_capacity(rows, 8),
            name: StringBuilder::with_capacity(rows, rows * 32),
            kind: Int32Builder::with_capacity(rows),
            status: Int32Builder::with_capacity(rows),
            other: Int32Builder::with_capacity(rows),
            start: Int64Builder::with_capacity(rows),
            end: Int64Builder::with_capacity(rows),
            service: StringBuilder::with_capacity(rows, rows * 16),
            txt: std::array::from_fn(|_| StringBuilder::with_capacity(rows, rows * 16)),
            ints: std::array::from_fn(|_| Int64Builder::with_capacity(rows)),
            pay: std::array::from_fn(|_| LargeBinaryBuilder::with_capacity(rows, bytes)),
        }
    }

    fn finish(mut self) -> Result<(RecordBatch, RecordBatch), String> {
        let e = |x: arrow::error::ArrowError| x.to_string();
        let a = |x: ArrayRef| x;
        let [op, prov, model, conv, sess, user, agent, tool, err] = self.txt.map(|mut b| Arc::new(b.finish()) as ArrayRef);
        let [it, ot] = self.ints.map(|mut b| Arc::new(b.finish()) as ArrayRef);
        let cols = vec![
            a(Arc::new(self.trace.finish())),
            a(Arc::new(self.span.finish())),
            a(Arc::new(self.parent.finish())),
            a(Arc::new(self.name.finish())),
            a(Arc::new(self.kind.finish())),
            a(Arc::new(self.start.finish())),
            a(Arc::new(self.end.finish())),
            a(Arc::new(self.status.finish())),
            a(Arc::new(self.service.finish())),
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
            a(Arc::new(self.other.finish())),
        ];
        let mut pcols: Vec<ArrayRef> = vec![Arc::new(self.ptrace.finish()), Arc::new(self.pspan.finish())];
        pcols.extend(self.pay.map(|mut b| Arc::new(b.finish()) as ArrayRef));
        Ok((RecordBatch::try_new(spans_schema(), cols).map_err(e)?, RecordBatch::try_new(payloads_schema(), pcols).map_err(e)?))
    }
}

pub struct TraceAcc {
    max_rows: usize,
    max_bytes: usize,
    cols: Option<Cols>,
    rows: usize,
    bytes: usize,
    pub batches: u64,
    pub check: Check,
    /// Tests only: keep closed batches for inspection instead of dropping them.
    pub kept: Option<Vec<(RecordBatch, RecordBatch)>>,
}

impl TraceAcc {
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        TraceAcc { max_rows, max_bytes, cols: None, rows: 0, bytes: 0, batches: 0, check: Check::default(), kept: None }
    }

    pub fn push(&mut self, r: &SpanRow) -> Result<(), String> {
        if r.trace_id.len() != 16 || r.span_id.len() != 8 || !(r.parent.is_empty() || r.parent.len() == 8) {
            return Err("trace_id/span_id/parent_span_id have the wrong length".into());
        }
        let size = r.value_bytes();
        if self.rows > 0 && self.bytes + size > self.max_bytes {
            self.close()?;
        }
        let (mr, mb) = (self.max_rows, self.max_bytes);
        let c = self.cols.get_or_insert_with(|| Cols::new(mr, mb));
        // lengths were checked above, so the fixed-size appends cannot fail
        let ok = |x: Result<(), arrow::error::ArrowError>| x.map_err(|e| e.to_string());
        ok(c.trace.append_value(r.trace_id))?;
        ok(c.span.append_value(r.span_id))?;
        ok(c.ptrace.append_value(r.trace_id))?;
        ok(c.pspan.append_value(r.span_id))?;
        if r.parent.is_empty() {
            c.parent.append_null();
        } else {
            ok(c.parent.append_value(r.parent))?;
        }
        c.name.append_value(r.name);
        c.kind.append_value(r.kind);
        c.start.append_value(r.start);
        c.end.append_value(r.end);
        c.status.append_value(r.status);
        c.service.append_option(r.service);
        for i in 0..9 {
            c.txt[i].append_option(r.txt[i]);
        }
        for i in 0..2 {
            c.ints[i].append_option(r.ints[i]);
        }
        c.other.append_value(r.other);
        for i in 0..6 {
            c.pay[i].append_option(r.pay[i]);
        }
        self.rows += 1;
        self.bytes += size;
        if self.rows >= self.max_rows || self.bytes >= self.max_bytes {
            self.close()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), String> {
        let Some(cols) = self.cols.take() else { return Ok(()) };
        let (s, p) = cols.finish()?;
        self.rows = 0;
        self.bytes = 0;
        self.batches += 1;
        let ck = &mut self.check;
        ck.rows += s.num_rows() as u64;
        ck.in_tok += s.column_by_name("input_tokens").unwrap().as_primitive::<Int64Type>().iter().flatten().sum::<i64>();
        ck.out_tok += s.column_by_name("output_tokens").unwrap().as_primitive::<Int64Type>().iter().flatten().sum::<i64>();
        for i in 0..6 {
            let a = p.column(2 + i).as_binary::<i64>();
            ck.nonnull[i] += (a.len() - a.null_count()) as u64;
            ck.bytes[i] += (a.value_offsets()[a.len()] - a.value_offsets()[0]) as u64;
        }
        if let Some(k) = &mut self.kept {
            k.push((s, p));
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn open_rows(&self) -> usize {
        self.rows
    }
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

fn service_of(attrs: &[KeyValue]) -> Option<&str> {
    attrs.iter().find(|kv| kv.key == "service.name").and_then(strval)
}

/// Full path: decode every message with prost, then append.
pub fn full_traces(buf: &[u8], acc: &mut TraceAcc) -> Result<(), String> {
    let req = ExportTraceServiceRequest::decode(buf).map_err(|e| e.to_string())?;
    for rs in &req.resource_spans {
        let svc = rs.resource.as_ref().and_then(|r| service_of(&r.attributes));
        for sp in rs.scope_spans.iter().flat_map(|s| &s.spans) {
            let mut r = SpanRow {
                trace_id: &sp.trace_id,
                span_id: &sp.span_id,
                parent: &sp.parent_span_id,
                name: &sp.name,
                kind: sp.kind,
                start: sp.start_time_unix_nano as i64,
                end: sp.end_time_unix_nano as i64,
                status: sp.status.as_ref().map_or(0, |s| s.code),
                service: svc,
                ..Default::default()
            };
            for kv in &sp.attributes {
                match classify(&kv.key) {
                    Some(Slot::Txt(i)) => r.txt[i] = strval(kv),
                    Some(Slot::Int(i)) => r.ints[i] = intval(kv),
                    Some(Slot::Pay(i)) => r.pay[i] = strval(kv).map(str::as_bytes),
                    None => r.other += 1,
                }
            }
            acc.push(&r)?;
        }
    }
    Ok(())
}

/// Lazy path: walk the wire format, no message objects.
pub fn lazy_traces(buf: &[u8], acc: &mut TraceAcc) -> Result<(), String> {
    crate::lazy::walk_traces(buf, |r| acc.push(r))
}

// ---- logs and metrics (flat schemas, full decode only) ----

fn render(v: &AnyValue) -> String {
    match v.value.as_ref() {
        None => String::new(),
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BytesValue(b)) => b.iter().map(|x| format!("{x:02x}")).collect(),
        Some(other) => format!("{other:?}"),
    }
}

pub struct LogAcc {
    time: Int64Builder,
    sev: Int32Builder,
    sevtext: StringBuilder,
    body: StringBuilder,
    svc: StringBuilder,
    attrs: Int32Builder,
    rows: usize,
    bytes: usize,
    pub batches: u64,
    pub records: u64,
}

impl LogAcc {
    pub fn new() -> Self {
        LogAcc { time: Int64Builder::new(), sev: Int32Builder::new(), sevtext: StringBuilder::new(), body: StringBuilder::new(), svc: StringBuilder::new(), attrs: Int32Builder::new(), rows: 0, bytes: 0, batches: 0, records: 0 }
    }

    pub fn push_request(&mut self, buf: &[u8]) -> Result<(), String> {
        let req = ExportLogsServiceRequest::decode(buf).map_err(|e| e.to_string())?;
        for rl in &req.resource_logs {
            let svc = rl.resource.as_ref().and_then(|r| service_of(&r.attributes));
            for lr in rl.scope_logs.iter().flat_map(|s| &s.log_records) {
                let body = lr.body.as_ref().map(render).unwrap_or_default();
                if self.rows > 0 && self.bytes + body.len() > MAX_BYTES {
                    self.close()?;
                }
                self.time.append_value(lr.time_unix_nano as i64);
                self.sev.append_value(lr.severity_number);
                self.sevtext.append_value(&lr.severity_text);
                self.body.append_value(&body);
                self.svc.append_option(svc);
                self.attrs.append_value(lr.attributes.len() as i32);
                self.rows += 1;
                self.records += 1;
                self.bytes += body.len() + lr.severity_text.len();
                if self.rows >= MAX_ROWS || self.bytes >= MAX_BYTES {
                    self.close()?;
                }
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), String> {
        if self.rows == 0 {
            return Ok(());
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_ns", DataType::Int64, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("attr_count", DataType::Int32, false),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.time.finish()),
            Arc::new(self.sev.finish()),
            Arc::new(self.sevtext.finish()),
            Arc::new(self.body.finish()),
            Arc::new(self.svc.finish()),
            Arc::new(self.attrs.finish()),
        ];
        drop(RecordBatch::try_new(schema, cols).map_err(|e| e.to_string())?);
        self.rows = 0;
        self.bytes = 0;
        self.batches += 1;
        Ok(())
    }
}

pub struct MetricAcc {
    name: StringBuilder,
    time: Int64Builder,
    kind: StringBuilder,
    value: Float64Builder,
    svc: StringBuilder,
    attrs: Int32Builder,
    rows: usize,
    bytes: usize,
    pub batches: u64,
    pub points: u64,
}

impl MetricAcc {
    pub fn new() -> Self {
        MetricAcc { name: StringBuilder::new(), time: Int64Builder::new(), kind: StringBuilder::new(), value: Float64Builder::new(), svc: StringBuilder::new(), attrs: Int32Builder::new(), rows: 0, bytes: 0, batches: 0, points: 0 }
    }

    fn row(&mut self, name: &str, svc: Option<&str>, kind: &str, time: u64, value: Option<f64>, nattrs: usize) -> Result<(), String> {
        self.name.append_value(name);
        self.time.append_value(time as i64);
        self.kind.append_value(kind);
        self.value.append_option(value);
        self.svc.append_option(svc);
        self.attrs.append_value(nattrs as i32);
        self.rows += 1;
        self.points += 1;
        self.bytes += name.len() + kind.len();
        if self.rows >= MAX_ROWS || self.bytes >= MAX_BYTES {
            self.close()?;
        }
        Ok(())
    }

    pub fn push_request(&mut self, buf: &[u8]) -> Result<(), String> {
        let req = ExportMetricsServiceRequest::decode(buf).map_err(|e| e.to_string())?;
        for rm in &req.resource_metrics {
            let svc = rm.resource.as_ref().and_then(|r| service_of(&r.attributes));
            for m in rm.scope_metrics.iter().flat_map(|s| &s.metrics) {
                let n = m.name.as_str();
                match m.data.as_ref() {
                    Some(d @ (Data::Gauge(_) | Data::Sum(_))) => {
                        let (kind, pts) = match d {
                            Data::Gauge(g) => ("gauge", &g.data_points),
                            Data::Sum(s) => ("sum", &s.data_points),
                            _ => unreachable!(),
                        };
                        for p in pts {
                            let v = p.value.as_ref().map(|v| match v {
                                number_data_point::Value::AsDouble(d) => *d,
                                number_data_point::Value::AsInt(i) => *i as f64,
                            });
                            self.row(n, svc, kind, p.time_unix_nano, v, p.attributes.len())?;
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for p in &h.data_points {
                            self.row(n, svc, "histogram", p.time_unix_nano, p.sum, p.attributes.len())?;
                        }
                    }
                    Some(Data::ExponentialHistogram(h)) => {
                        for p in &h.data_points {
                            self.row(n, svc, "exponential_histogram", p.time_unix_nano, p.sum, p.attributes.len())?;
                        }
                    }
                    Some(Data::Summary(s)) => {
                        for p in &s.data_points {
                            self.row(n, svc, "summary", p.time_unix_nano, Some(p.sum), p.attributes.len())?;
                        }
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), String> {
        if self.rows == 0 {
            return Ok(());
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("time_ns", DataType::Int64, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("attr_count", DataType::Int32, false),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.name.finish()),
            Arc::new(self.time.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.svc.finish()),
            Arc::new(self.attrs.finish()),
        ];
        drop(RecordBatch::try_new(schema, cols).map_err(|e| e.to_string())?);
        self.rows = 0;
        self.bytes = 0;
        self.batches += 1;
        Ok(())
    }
}
