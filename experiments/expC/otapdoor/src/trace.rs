//! OTAP trace tables -> the `spans` and `payloads` tables of the target schema, with the same column rules as the
//! OTLP converter of the bake-off (which attributes are promoted, which are payloads, how tokens are read, what
//! counts toward other_attr_count).
use crate::attrs::{Attrs, T_STR};
use crate::cols::{child, col, i64s, prefix_sum, u32s, View};
use crate::R;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub const BATCH_ROWS: usize = 8192;
pub const BATCH_BYTES: usize = 32 << 20;
pub const PAY_COLS: [&str; 6] = ["input_messages", "output_messages", "system_instructions", "tool_definitions", "tool_call_arguments", "tool_call_result"];
pub const TXT_COLS: [&str; 9] = ["operation", "provider", "request_model", "conversation_id", "session_id", "user_id", "agent_name", "tool_name", "error_type"];

// slot numbers: 0..9 text columns, 9..11 integer columns, 11..17 payload columns
const OTHER: u8 = 254;
const UNSEEN: u8 = 255;

pub fn classify(k: &str) -> u8 {
    match k {
        "gen_ai.operation.name" => 0,
        "gen_ai.provider.name" => 1,
        "gen_ai.request.model" => 2,
        "gen_ai.conversation.id" => 3,
        "session.id" => 4,
        "user.id" => 5,
        "gen_ai.agent.name" => 6,
        "gen_ai.tool.name" => 7,
        "error.type" => 8,
        "gen_ai.usage.input_tokens" => 9,
        "gen_ai.usage.output_tokens" => 10,
        "gen_ai.input.messages" => 11,
        "gen_ai.output.messages" => 12,
        "gen_ai.system_instructions" => 13,
        "gen_ai.tool.definitions" => 14,
        "gen_ai.tool.call.arguments" => 15,
        "gen_ai.tool.call.result" => 16,
        _ => OTHER,
    }
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

/// The numbers the generator reports, computed from the Arrow batches this receiver produced.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct Check {
    pub rows: u64,
    pub in_tok: i64,
    pub out_tok: i64,
    pub nonnull: [u64; 6],
    pub bytes: [u64; 6],
}

impl Check {
    pub fn add(&mut self, o: &Check) {
        self.rows += o.rows;
        self.in_tok += o.in_tok;
        self.out_tok += o.out_tok;
        for i in 0..6 {
            self.nonnull[i] += o.nonnull[i];
            self.bytes[i] += o.bytes[i];
        }
    }
    pub fn of(spans: &RecordBatch, pay: &RecordBatch) -> Check {
        let sum = |i: usize| arrow::compute::sum(spans.column(i).as_primitive::<Int64Type>()).unwrap_or(0);
        let mut c = Check { rows: spans.num_rows() as u64, in_tok: sum(12), out_tok: sum(13), ..Default::default() };
        for i in 0..6 {
            let a = pay.column(2 + i).as_binary::<i64>();
            c.nonnull[i] = (a.len() - a.null_count()) as u64;
            c.bytes[i] = (a.offsets()[a.len()] - a.offsets()[0]) as u64;
        }
        c
    }
}

/// One output row, borrowing from the decoded OTAP batches.
pub struct Row<'a> {
    pub trace: &'a [u8],
    pub span: &'a [u8],
    pub parent: Option<&'a [u8]>,
    pub name: &'a str,
    pub kind: i32,
    pub start: i64,
    pub end: i64,
    pub status: i32,
    pub svc: Option<&'a str>,
    pub txt: [Option<&'a str>; 9],
    pub ints: [Option<i64>; 2],
    pub other: i32,
    pub pay: [Option<&'a str>; 6],
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// One row as text for comparing two decodings of the same data: ids in hex, strings quoted, payloads as length and
/// SHA-256 prefix.
pub fn row_line(r: &Row) -> String {
    use sha2::{Digest, Sha256};
    let o = |v: Option<&str>| v.map_or("-".to_string(), |s| format!("{s:?}"));
    let mut l = format!(
        "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}\t{}",
        hex(r.trace),
        hex(r.span),
        r.parent.map_or("-".into(), hex),
        r.name,
        r.kind,
        r.start,
        r.end,
        r.status,
        o(r.svc)
    );
    for t in r.txt {
        l += &format!("\t{}", o(t));
    }
    for i in r.ints {
        l += &format!("\t{}", i.map_or("-".into(), |v| v.to_string()));
    }
    l += &format!("\t{}", r.other);
    for p in r.pay {
        l += &format!("\t{}", p.map_or("-".into(), |s| format!("{}:{}", s.len(), hex(&Sha256::digest(s.as_bytes())[..8]))));
    }
    l.push('\n');
    l
}

/// Builds the two output tables and closes a batch at BATCH_ROWS rows or BATCH_BYTES of value bytes. Closed
/// batches are measured into `check` and dropped (this experiment stops at conversion).
pub struct Sink {
    rows: usize,
    bytes: usize,
    trace: FixedSizeBinaryBuilder,
    span: FixedSizeBinaryBuilder,
    parent: FixedSizeBinaryBuilder,
    name: StringBuilder,
    kind: Int32Builder,
    start: Int64Builder,
    end: Int64Builder,
    status: Int32Builder,
    svc: StringBuilder,
    txt: [StringBuilder; 9],
    ints: [Int64Builder; 2],
    other: Int32Builder,
    pay: [LargeBinaryBuilder; 6],
    ss: SchemaRef,
    ps: SchemaRef,
    pub check: Check,
    pub batches: u64,
    /// when set, one tab-separated line per row is appended here (see `row_line`)
    pub dump: Option<Vec<u8>>,
    row_limit: usize,
    byte_limit: usize,
}

impl Sink {
    pub fn new() -> Sink {
        Sink::with_limits(BATCH_ROWS, BATCH_BYTES)
    }
    pub fn with_limits(row_limit: usize, byte_limit: usize) -> Sink {
        Sink {
            rows: 0,
            bytes: 0,
            trace: FixedSizeBinaryBuilder::new(16),
            span: FixedSizeBinaryBuilder::new(8),
            parent: FixedSizeBinaryBuilder::new(8),
            name: StringBuilder::new(),
            kind: Int32Builder::new(),
            start: Int64Builder::new(),
            end: Int64Builder::new(),
            status: Int32Builder::new(),
            svc: StringBuilder::new(),
            txt: std::array::from_fn(|_| StringBuilder::new()),
            ints: std::array::from_fn(|_| Int64Builder::new()),
            other: Int32Builder::new(),
            pay: std::array::from_fn(|_| LargeBinaryBuilder::new()),
            ss: spans_schema(),
            ps: payloads_schema(),
            check: Check::default(),
            batches: 0,
            dump: None,
            row_limit,
            byte_limit,
        }
    }

    pub fn push(&mut self, r: &Row) -> R<()> {
        if let Some(d) = &mut self.dump {
            d.extend_from_slice(row_line(r).as_bytes());
        }
        self.trace.append_value(r.trace)?;
        self.span.append_value(r.span)?;
        match r.parent {
            Some(p) => self.parent.append_value(p)?,
            None => self.parent.append_null(),
        }
        self.name.append_value(r.name);
        self.kind.append_value(r.kind);
        self.start.append_value(r.start);
        self.end.append_value(r.end);
        self.status.append_value(r.status);
        self.svc.append_option(r.svc);
        self.bytes += 64 + r.name.len() + r.svc.map_or(0, str::len);
        for i in 0..9 {
            self.txt[i].append_option(r.txt[i]);
            self.bytes += r.txt[i].map_or(0, str::len);
        }
        for i in 0..2 {
            self.ints[i].append_option(r.ints[i]);
        }
        self.other.append_value(r.other);
        for i in 0..6 {
            self.pay[i].append_option(r.pay[i].map(str::as_bytes));
            self.bytes += r.pay[i].map_or(0, str::len);
        }
        self.rows += 1;
        if self.rows >= self.row_limit || self.bytes >= self.byte_limit {
            self.close()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> R<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let a = |x: ArrayRef| x;
        let [op, prov, model, conv, sess, user, agent, tool, err] = std::array::from_fn::<ArrayRef, 9, _>(|i| Arc::new(self.txt[i].finish()));
        let [it, ot] = std::array::from_fn::<ArrayRef, 2, _>(|i| Arc::new(self.ints[i].finish()));
        let (trace, span) = (Arc::new(self.trace.finish()), Arc::new(self.span.finish()));
        let cols = vec![
            a(trace.clone()),
            a(span.clone()),
            a(Arc::new(self.parent.finish())),
            a(Arc::new(self.name.finish())),
            a(Arc::new(self.kind.finish())),
            a(Arc::new(self.start.finish())),
            a(Arc::new(self.end.finish())),
            a(Arc::new(self.status.finish())),
            a(Arc::new(self.svc.finish())),
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
        let mut pcols: Vec<ArrayRef> = vec![trace, span];
        pcols.extend(std::array::from_fn::<ArrayRef, 6, _>(|i| Arc::new(self.pay[i].finish())));
        let (sb, pb) = (RecordBatch::try_new(self.ss.clone(), cols)?, RecordBatch::try_new(self.ps.clone(), pcols)?);
        self.check.add(&Check::of(&sb, &pb));
        self.batches += 1;
        self.rows = 0;
        self.bytes = 0;
        Ok(())
    }
}

/// Converts the tables of one message. `spans` is SPANS; `attrs` SPAN_ATTRS; `rattrs` RESOURCE_ATTRS. Events, links and
/// scope data are not part of the target schema and are not read.
pub fn convert(spans: &RecordBatch, attrs: Option<&RecordBatch>, rattrs: Option<&RecordBatch>, sink: &mut Sink) -> R<()> {
    let n = spans.num_rows();
    let c = |name: &str| col(spans, name);
    let need = |name: &str| c(name).ok_or_else(|| format!("SPANS without {name}"));
    let trace = View::new(need("trace_id")?);
    let span = View::new(need("span_id")?);
    let parent = c("parent_span_id").map(View::new);
    let name = c("name").map(View::new);
    let start = i64s(need("start_time_unix_nano")?)?;
    let dur = c("duration_time_unix_nano").map(i64s).transpose()?;
    let kind = c("kind").map(i64s).transpose()?;
    let status = c("status").and_then(|s| child(s, "code")).map(|a| i64s(&a)).transpose()?;
    let ids = c("id").map(|a| u32s(a).map(|a| prefix_sum(&a))).transpose()?;
    let rids = c("resource").and_then(|r| child(r, "id")).map(|a| u32s(&a).map(|a| prefix_sum(&a))).transpose()?;

    // span id -> row
    let mut by_id: Vec<u32> = vec![];
    if let Some(ids) = &ids {
        for (row, id) in ids.iter().enumerate() {
            if let Some(id) = *id {
                if id > (1 << 24) {
                    return Err(format!("span id {id} out of range").into());
                }
                if by_id.len() <= id as usize {
                    by_id.resize(id as usize + 1, u32::MAX);
                }
                by_id[id as usize] = row as u32;
            }
        }
    }

    let at = attrs.map(Attrs::from_batch).transpose()?;
    let mut txt: Vec<[Option<&str>; 9]> = vec![[None; 9]; n];
    let mut ints: Vec<[Option<i64>; 2]> = vec![[None; 2]; n];
    let mut pay: Vec<[Option<&str>; 6]> = vec![[None; 6]; n];
    let mut other: Vec<i32> = vec![0; n];
    if let Some(at) = &at {
        // classify each distinct key once (a dictionary has few entries)
        let mut cls = vec![UNSEEN; at.key.nvals()];
        for r in 0..at.rows() {
            let sp = match by_id.get(at.parent[r] as usize) {
                Some(&row) if row != u32::MAX => row as usize,
                _ => return Err(format!("SPAN_ATTRS row {r} refers to span id {} which no SPANS row carries", at.parent[r]).into()),
            };
            let k = match at.key.pos(r) {
                Some(p) => {
                    if cls[p] == UNSEEN {
                        cls[p] = classify(at.key(r));
                    }
                    cls[p]
                }
                None => OTHER,
            };
            match k {
                0..=8 => txt[sp][k as usize] = at.str(r),
                9..=10 => ints[sp][k as usize - 9] = at.int(r),
                11..=16 => pay[sp][k as usize - 11] = at.str(r),
                _ => other[sp] += 1,
            }
        }
    }

    // resource id -> service.name (the first one in the resource, as the OTLP converter does)
    let mut svc: std::collections::HashMap<u32, &str> = Default::default();
    let ra = rattrs.map(Attrs::from_batch).transpose()?;
    if let Some(ra) = &ra {
        for r in 0..ra.rows() {
            if ra.ty[r] == T_STR && ra.key(r) == "service.name" {
                if let Some(v) = ra.str(r) {
                    svc.entry(ra.parent[r]).or_insert(v);
                }
            }
        }
    }

    for i in 0..n {
        let pick = |a: &Option<Int64Array>| a.as_ref().and_then(|a| a.is_valid(i).then(|| a.value(i)));
        let (s, d) = (start.value(i), dur.as_ref().and_then(|a| a.is_valid(i).then(|| a.value(i))).unwrap_or(0));
        let t = trace.bytes(i).ok_or("null trace_id")?;
        let sid = span.bytes(i).ok_or("null span_id")?;
        sink.push(&Row {
            trace: t,
            span: sid,
            parent: parent.as_ref().and_then(|p| p.bytes(i)),
            name: name.as_ref().and_then(|v| v.str(i)).unwrap_or(""),
            kind: pick(&kind).unwrap_or(0) as i32,
            start: s,
            end: s.wrapping_add(d),
            status: pick(&status).unwrap_or(0) as i32,
            svc: rids.as_ref().and_then(|r| r[i]).and_then(|id| svc.get(&id).copied()),
            txt: txt[i],
            ints: ints[i],
            other: other[i],
            pay: pay[i],
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::{T_INT, T_STR};

    #[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
    enum V {
        S(&'static str),
        I(i64),
    }

    /// Builds an attribute batch the way the Go producer does: rows sorted by (type, key, value, parent id), and the
    /// parent id stored as a delta whenever (type, key, value) equals the previous row's.
    fn attr_batch(rows: &[(u16, &'static str, V)]) -> RecordBatch {
        let ty = |v: &V| if matches!(v, V::S(_)) { T_STR } else { T_INT };
        let mut r = rows.to_vec();
        r.sort_by(|a, b| (ty(&a.2), a.1, a.2, a.0).partial_cmp(&(ty(&b.2), b.1, b.2, b.0)).unwrap());
        let mut parent = vec![];
        for i in 0..r.len() {
            let same = i > 0 && ty(&r[i].2) == ty(&r[i - 1].2) && r[i].1 == r[i - 1].1 && r[i].2 == r[i - 1].2;
            parent.push(if same { r[i].0 - r[i - 1].0 } else { r[i].0 });
        }
        let mut key = StringDictionaryBuilder::<UInt8Type>::new();
        let mut sv = StringDictionaryBuilder::<UInt16Type>::new();
        let mut iv = PrimitiveDictionaryBuilder::<UInt16Type, Int64Type>::new();
        for (_, k, v) in &r {
            key.append_value(k);
            match v {
                V::S(s) => {
                    sv.append_value(s);
                    iv.append_null();
                }
                V::I(i) => {
                    sv.append_null();
                    iv.append_value(*i);
                }
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(parent)),
            Arc::new(key.finish()),
            Arc::new(UInt8Array::from_iter_values(r.iter().map(|x| ty(&x.2)))),
            Arc::new(sv.finish()),
            Arc::new(iv.finish()),
        ];
        RecordBatch::try_from_iter(["parent_id", "key", "type", "str", "int"].into_iter().zip(cols)).unwrap()
    }

    fn fsb(n: i32, rows: &[Option<[u8; 8]>], wide: bool) -> ArrayRef {
        let mut b = FixedSizeBinaryBuilder::new(n);
        for r in rows {
            match r {
                Some(x) if wide => b.append_value([&x[..], &x[..]].concat()).unwrap(),
                Some(x) => b.append_value(x).unwrap(),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    fn dict_i32(v: &[i32]) -> ArrayRef {
        let mut b = PrimitiveDictionaryBuilder::<UInt8Type, Int32Type>::new();
        v.iter().for_each(|x| {
            b.append_value(*x);
        });
        Arc::new(b.finish())
    }

    /// Four spans in producer order. Span ids (delta-coded): 0, 1, null (no related data), 2.
    /// Resource ids (delta-coded): 0, 0, 1, 1.
    fn spans_batch() -> RecordBatch {
        let id = UInt16Array::from(vec![Some(0), Some(1), None, Some(1)]);
        let res = StructArray::from(vec![(Arc::new(Field::new("id", DataType::UInt16, true)), Arc::new(UInt16Array::from(vec![0, 0, 1, 0])) as ArrayRef)]);
        let codes = dict_i32(&[0, 2, 0, 0]);
        let status = StructArray::from(vec![(Arc::new(Field::new("code", codes.data_type().clone(), true)), codes)]);
        let mut name = StringDictionaryBuilder::<UInt8Type>::new();
        ["chat m", "chat m", "execute_tool t1", "invoke_agent a"].iter().for_each(|n| {
            name.append_value(n);
        });
        let tid = |x: u8| Some([x; 8]);
        let cols: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(id)),
            ("resource", Arc::new(res)),
            ("start_time_unix_nano", Arc::new(TimestampNanosecondArray::from(vec![1000, 2000, 3000, 4000]).with_timezone("UTC"))),
            ("duration_time_unix_nano", Arc::new(DurationNanosecondArray::from(vec![10, 20, 30, 40]))),
            ("trace_id", fsb(16, &[tid(1), tid(1), tid(2), tid(2)], true)),
            ("span_id", fsb(8, &[tid(11), tid(12), tid(13), tid(14)], false)),
            ("parent_span_id", fsb(8, &[tid(14), tid(14), tid(14), None], false)),
            ("name", Arc::new(name.finish())),
            ("kind", dict_i32(&[3, 3, 1, 1])),
            ("status", Arc::new(status)),
        ];
        RecordBatch::try_from_iter(cols).unwrap()
    }

    fn run(spans: &RecordBatch, attrs: Option<&RecordBatch>, res: Option<&RecordBatch>) -> (Sink, Vec<Vec<String>>) {
        let mut sink = Sink::new();
        sink.dump = Some(vec![]);
        convert(spans, attrs, res, &mut sink).unwrap();
        sink.close().unwrap();
        let rows = String::from_utf8(sink.dump.clone().unwrap()).unwrap().lines().map(|l| l.split('\t').map(str::to_string).collect()).collect();
        (sink, rows)
    }

    #[test]
    fn joins_attribute_tables_back_to_spans() {
        let attrs = attr_batch(&[
            (0, "gen_ai.operation.name", V::S("chat")),
            (1, "gen_ai.operation.name", V::S("chat")),
            (2, "gen_ai.operation.name", V::S("execute_tool")),
            (0, "gen_ai.usage.input_tokens", V::I(100)),
            (1, "gen_ai.usage.input_tokens", V::I(50)),
            // a token count that arrives as text is neither read as a number nor counted as an unknown attribute
            (0, "gen_ai.usage.output_tokens", V::S("7")),
            (1, "gen_ai.usage.output_tokens", V::I(9)),
            (0, "gen_ai.input.messages", V::S("IN0")),
            (1, "gen_ai.input.messages", V::S("IN0")),
            (2, "gen_ai.tool.call.result", V::S("result-text")),
            (0, "custom.a", V::S("x")),
            (1, "custom.a", V::S("x")),
            (1, "custom.b", V::I(3)),
            (2, "gen_ai.tool.name", V::S("t1")),
        ]);
        let res = attr_batch(&[(0, "service.name", V::S("svcA")), (1, "service.name", V::S("svcB")), (1, "host", V::S("h"))]);
        let (sink, rows) = run(&spans_batch(), Some(&attrs), Some(&res));
        assert_eq!(rows.len(), 4);
        // columns of a dump line: 0 trace, 1 span, 2 parent, 3 name, 4 kind, 5 start, 6 end, 7 status, 8 service,
        // 9..18 text columns, 18..20 token columns, 20 other_attr_count, 21..27 payloads
        let by_span = |s: &str| rows.iter().find(|r| r[1] == s.repeat(8)).unwrap().clone();
        let (a, b, c, d) = (by_span("0b"), by_span("0c"), by_span("0d"), by_span("0e"));
        assert_eq!((a[3].as_str(), a[4].as_str(), a[5].as_str(), a[6].as_str(), a[7].as_str(), a[8].as_str()), ("\"chat m\"", "3", "1000", "1010", "0", "\"svcA\""));
        assert_eq!((b[7].as_str(), b[8].as_str()), ("2", "\"svcA\""));
        // span "0d" has no id (no related data): no attributes, but it still belongs to resource 1
        assert_eq!((c[8].as_str(), c[9].as_str(), c[20].as_str()), ("\"svcB\"", "-", "0"));
        assert_eq!((d[8].as_str(), d[9].as_str(), d[16].as_str(), d[2].as_str()), ("\"svcB\"", "\"execute_tool\"", "\"t1\"", "-"));
        assert_eq!((a[2].as_str(), a[0].as_str()), (b[2].as_str(), "01".repeat(16).as_str()));
        // tokens: span 0 input 100, output absent (text); span 1 input 50, output 9
        assert_eq!((a[18].as_str(), a[19].as_str(), b[18].as_str(), b[19].as_str()), ("100", "-", "50", "9"));
        // other attributes: span 0 has custom.a; span 1 has custom.a and custom.b; span 2 none
        assert_eq!((a[20].as_str(), b[20].as_str(), d[20].as_str()), ("1", "2", "0"));
        assert_eq!(sink.check.rows, 4);
        assert_eq!((sink.check.in_tok, sink.check.out_tok), (150, 9));
        // payload columns: input_messages on 2 spans, tool_call_result on 1
        assert_eq!(sink.check.nonnull, [2, 0, 0, 0, 0, 1]);
        assert_eq!(sink.check.bytes, [6, 0, 0, 0, 0, 11]);
        assert!(a[21].starts_with("3:") && d[26].starts_with("11:") && c[21] == "-");
    }

    #[test]
    fn works_without_attribute_tables_and_with_plain_columns() {
        let (sink, rows) = run(&spans_batch(), None, None);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][8], "-");
        assert_eq!(sink.check, Check { rows: 4, ..Default::default() });
    }

    #[test]
    fn rejects_attribute_rows_with_no_parent_span() {
        // span ids are 0, 1, 2; parent 9 matches nothing
        let attrs = attr_batch(&[(9, "custom.a", V::S("x"))]);
        let mut sink = Sink::new();
        assert!(convert(&spans_batch(), Some(&attrs), None, &mut sink).is_err());
    }

    #[test]
    fn batches_close_on_rows_or_value_bytes() {
        let attrs = attr_batch(&[(0, "gen_ai.input.messages", V::S("0123456789")), (1, "gen_ai.input.messages", V::S("0123456789")), (2, "gen_ai.input.messages", V::S("0123456789"))]);
        let spans = spans_batch();
        let count = |rows: usize, bytes: usize| {
            let mut s = Sink::with_limits(rows, bytes);
            convert(&spans, Some(&attrs), None, &mut s).unwrap();
            s.close().unwrap();
            (s.batches, s.check.rows)
        };
        assert_eq!(count(8192, 32 << 20), (1, 4));
        assert_eq!(count(3, 32 << 20), (2, 4), "row bound");
        // a row costs 64 bytes plus its name and values (80, 80, 79 and 88 here)
        assert_eq!(count(8192, 100), (2, 4), "byte bound");
        assert_eq!(count(8192, 70), (4, 4), "byte bound, every row");
    }

    #[test]
    fn span_ids_decode_as_running_sum_skipping_nulls() {
        assert_eq!(prefix_sum(&UInt32Array::from(vec![Some(0), Some(1), None, Some(1)])), [Some(0), Some(1), None, Some(2)]);
        assert_eq!(prefix_sum(&UInt32Array::from(vec![Some(7), Some(0), Some(0)])), [Some(7), Some(7), Some(7)]);
    }
}
