use crate::wire::{At, Cur, Fault, R, put_varint, utf8};
use arrow_array::RecordBatch;
use nwdb_core::{
    BatchLimits, RecordDecoder, Result, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
};

/// The tag byte stored in front of an OTLP trace request in the write-ahead
/// log. The engine uses it to pick [`TraceDecoder`] on replay.
pub const RECORD_TAG_OTLP_TRACES: u8 = 1;

/// What one decode did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodeStats {
    /// Spans converted.
    pub spans: u64,
    /// Bytes of variable-length values (strings and raw columns) written into
    /// the batches. Fixed-width columns are not counted.
    pub bytes_copied: u64,
    /// `ResourceSpans` messages seen.
    pub resources: u64,
    /// `ScopeSpans` messages seen.
    pub scopes: u64,
}

/// Converts serialized OTLP trace requests into batches of the span table.
///
/// The decoder holds only its limits, so one value can serve any number of
/// threads at once.
#[derive(Debug, Clone, Copy)]
pub struct TraceDecoder {
    limits: BatchLimits,
}

impl TraceDecoder {
    /// Creates a decoder that closes a batch when `limits` is reached.
    pub fn new(limits: BatchLimits) -> Self {
        Self { limits }
    }

    /// Decodes a serialized `ExportTraceServiceRequest`.
    ///
    /// Rows come out in the order the spans appear in the request. A batch is
    /// closed as soon as it reaches either bound of the limits, so a single
    /// span larger than the byte bound is alone in its batch. An empty request
    /// gives no batches. Damaged input is [`nwdb_core::Error::Corrupt`] and
    /// nothing is returned for it.
    pub fn decode(&self, request: &[u8]) -> Result<Vec<RecordBatch>> {
        self.decode_with_stats(request).map(|(batches, _)| batches)
    }

    /// Like [`decode`](Self::decode), and also reports what was converted.
    pub fn decode_with_stats(&self, request: &[u8]) -> Result<(Vec<RecordBatch>, DecodeStats)> {
        let mut sink = Sink {
            builder: SpanBatchBuilder::new(self.limits),
            batches: Vec::new(),
            stats: DecodeStats::default(),
            scratch: Scratch::default(),
        };
        export_request(request, &mut sink)?;
        sink.flush();
        Ok((sink.batches, sink.stats))
    }
}

impl RecordDecoder for TraceDecoder {
    fn decode(&self, record: &[u8]) -> Result<Vec<RecordBatch>> {
        TraceDecoder::decode(self, record)
    }
}

// The promoted attributes. Each row names a column and the attribute keys it
// is copied from, the current semantic-convention name first and older names
// after it. The first name whose value has a type the column can use wins, so
// a newer name with an unusable value does not hide a usable older one. To
// promote another attribute, add a variant and a row.
#[derive(Clone, Copy)]
enum Col {
    Operation,
    Provider,
    RequestModel,
    ResponseModel,
    InputTokens,
    OutputTokens,
    ConversationId,
    SessionId,
    UserId,
    AgentName,
    ToolName,
    ErrorType,
}

const PROMOTED: [(Col, &[&str]); 12] = [
    (Col::Operation, &["gen_ai.operation.name"]),
    (Col::Provider, &["gen_ai.provider.name", "gen_ai.system"]),
    (Col::RequestModel, &["gen_ai.request.model"]),
    (Col::ResponseModel, &["gen_ai.response.model"]),
    (
        Col::InputTokens,
        &["gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"],
    ),
    (
        Col::OutputTokens,
        &[
            "gen_ai.usage.output_tokens",
            "gen_ai.usage.completion_tokens",
        ],
    ),
    (Col::ConversationId, &["gen_ai.conversation.id"]),
    (Col::SessionId, &["session.id"]),
    (Col::UserId, &["user.id"]),
    (Col::AgentName, &["gen_ai.agent.name"]),
    (Col::ToolName, &["gen_ai.tool.name"]),
    (Col::ErrorType, &["error.type"]),
];

/// Most names any one column may have in [`PROMOTED`].
const MAX_NAMES: usize = 3;

/// An attribute value, reduced to what promotion needs.
#[derive(Clone, Copy)]
enum Val<'a> {
    Str(&'a str),
    Int(i64),
    Other,
}

/// The value seen for each promoted name on the current span, indexed by
/// column then by position in its name list. A later occurrence of the same
/// key replaces an earlier one.
struct Seen<'a>([[Option<Val<'a>>; MAX_NAMES]; PROMOTED.len()]);

impl<'a> Seen<'a> {
    fn new() -> Self {
        Seen([[None; MAX_NAMES]; PROMOTED.len()])
    }

    fn record(&mut self, key: &str, val: Val<'a>) {
        for (i, (_, names)) in PROMOTED.iter().enumerate() {
            if let Some(rank) = names.iter().position(|n| *n == key) {
                if let Some(slot) = self.0.get_mut(i).and_then(|row| row.get_mut(rank)) {
                    *slot = Some(val);
                }
                return;
            }
        }
    }

    /// The first name, newest first, for which `usable` gives a value.
    fn first<T>(&self, col: Col, usable: impl Fn(Val<'a>) -> Option<T>) -> Option<T> {
        let row = self.0.get(col as usize)?;
        row.iter().flatten().find_map(|v| usable(*v))
    }

    fn text(&self, col: Col) -> Option<&'a str> {
        self.first(col, |v| match v {
            Val::Str(s) => Some(s),
            _ => None,
        })
    }

    fn count(&self, col: Col) -> Option<i64> {
        self.first(col, |v| match v {
            Val::Int(n) => Some(n),
            Val::Str(s) => s.parse().ok(),
            Val::Other => None,
        })
    }
}

/// Reusable buffers that assemble the length-prefixed raw columns. The wire
/// form of a repeated field has a tag byte before each element, so the column
/// bytes are not contiguous in the request and cannot be borrowed from it.
#[derive(Default)]
struct Scratch {
    attrs: Vec<u8>,
    events: Vec<u8>,
    links: Vec<u8>,
}

impl Scratch {
    fn clear(&mut self) {
        self.attrs.clear();
        self.events.clear();
        self.links.clear();
    }
}

fn non_empty(v: &[u8]) -> Option<&[u8]> {
    (!v.is_empty()).then_some(v)
}

fn push_prefixed(out: &mut Vec<u8>, msg: &[u8]) {
    put_varint(out, msg.len() as u64);
    out.extend_from_slice(msg);
}

struct Sink {
    builder: SpanBatchBuilder,
    batches: Vec<RecordBatch>,
    stats: DecodeStats,
    scratch: Scratch,
}

impl Sink {
    fn flush(&mut self) {
        if !self.builder.is_empty() {
            self.stats.bytes_copied += self.builder.value_bytes() as u64;
            self.batches.push(self.builder.finish());
        }
    }
}

/// What a span inherits from the `ResourceSpans` that holds it.
#[derive(Clone, Copy)]
struct Origin<'a> {
    resource: Option<&'a [u8]>,
    schema_url: Option<&'a str>,
    service_name: Option<&'a str>,
}

/// What a span inherits from its `ScopeSpans`.
#[derive(Clone, Copy, Default)]
struct Scope<'a> {
    /// The `InstrumentationScope` message as received.
    raw: Option<&'a [u8]>,
    name: Option<&'a str>,
    version: Option<&'a str>,
    schema_url: Option<&'a str>,
}

fn export_request(req: &[u8], sink: &mut Sink) -> R<()> {
    let mut c = Cur::new(req);
    while let Some((f, w)) = c.next_field()? {
        if f == 1 {
            let rs = c.expect_len(w).at("resource_spans")?;
            resource_spans(rs, sink).at("resource_spans")?;
        } else {
            c.skip(w)?;
        }
    }
    Ok(())
}

fn resource_spans(body: &[u8], sink: &mut Sink) -> R<()> {
    // The resource may come after the scope spans on the wire, and every span
    // needs it, so find it before walking the spans.
    let mut resource = None;
    let mut schema_url = None;
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => resource = Some(c.expect_len(w).at("resource")?),
            3 => schema_url = optional_str(c.expect_len(w).at("schema_url")?).at("schema_url")?,
            _ => c.skip(w)?,
        }
    }
    let service_name = match resource {
        Some(r) => service_name(r).at("resource")?,
        None => None,
    };
    let origin = Origin {
        resource,
        schema_url,
        service_name,
    };
    sink.stats.resources += 1;

    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        if f == 2 {
            let ss = c.expect_len(w).at("scope_spans")?;
            scope_spans(ss, origin, sink).at("scope_spans")?;
        } else {
            c.skip(w)?;
        }
    }
    Ok(())
}

/// The string value of the last `service.name` attribute of a resource.
fn service_name(resource: &[u8]) -> R<Option<&str>> {
    let mut found = None;
    let mut c = Cur::new(resource);
    while let Some((f, w)) = c.next_field()? {
        if f == 1 {
            let (key, value) = key_value(c.expect_len(w).at("attributes")?).at("attributes")?;
            if key == "service.name" {
                found = match value {
                    Some(v) => match any_value(v).at("attributes.value")? {
                        Val::Str(s) => Some(s),
                        _ => None,
                    },
                    None => None,
                };
            }
        } else {
            c.skip(w)?;
        }
    }
    Ok(found)
}

fn scope_spans(body: &[u8], origin: Origin<'_>, sink: &mut Sink) -> R<()> {
    // As with the resource, the scope may follow its spans.
    let mut scope = Scope::default();
    let mut schema_url = None;
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => {
                let raw = c.expect_len(w).at("scope")?;
                // A repeated scope replaces the earlier one, schema URL aside.
                scope = instrumentation_scope(raw).at("scope")?;
            }
            3 => schema_url = optional_str(c.expect_len(w).at("schema_url")?).at("schema_url")?,
            _ => c.skip(w)?,
        }
    }
    scope.schema_url = schema_url;
    sink.stats.scopes += 1;

    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        if f == 2 {
            let sp = c.expect_len(w).at("spans")?;
            span(sp, origin, scope, sink).at("spans")?;
        } else {
            c.skip(w)?;
        }
    }
    Ok(())
}

/// Reads a string field where an empty value means absent, as proto3 does.
fn optional_str(bytes: &[u8]) -> R<Option<&str>> {
    let s = utf8(bytes)?;
    Ok((!s.is_empty()).then_some(s))
}

fn instrumentation_scope(body: &[u8]) -> R<Scope<'_>> {
    let mut scope = Scope {
        raw: Some(body),
        ..Scope::default()
    };
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => scope.name = optional_str(c.expect_len(w).at("name")?).at("name")?,
            2 => scope.version = optional_str(c.expect_len(w).at("version")?).at("version")?,
            _ => c.skip(w)?,
        }
    }
    Ok(scope)
}

/// Splits a `KeyValue` into its key and the bytes of its value, without
/// looking inside the value.
fn key_value(kv: &[u8]) -> R<(&str, Option<&[u8]>)> {
    let mut key = "";
    let mut value = None;
    let mut c = Cur::new(kv);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => key = utf8(c.expect_len(w).at("key")?).at("key")?,
            2 => value = Some(c.expect_len(w).at("value")?),
            _ => c.skip(w)?,
        }
    }
    Ok((key, value))
}

/// Reads an `AnyValue`. Of several members on the wire the last one wins, as
/// for any oneof. Only strings and integers are looked into.
fn any_value(body: &[u8]) -> R<Val<'_>> {
    let mut val = Val::Other;
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => val = Val::Str(utf8(c.expect_len(w).at("string_value")?).at("string_value")?),
            3 => val = Val::Int(c.expect_varint(w).at("int_value")? as i64),
            2 => {
                c.expect_varint(w).at("bool_value")?;
                val = Val::Other;
            }
            4 => {
                c.expect_fixed64(w).at("double_value")?;
                val = Val::Other;
            }
            8 => {
                c.expect_varint(w).at("string_value_strindex")?;
                val = Val::Other;
            }
            5..=7 => {
                c.expect_len(w).at("value")?;
                val = Val::Other;
            }
            _ => c.skip(w)?,
        }
    }
    Ok(val)
}

/// Status message and code. A message that is repeated is not merged: the last
/// occurrence replaces the earlier ones.
fn status(body: &[u8]) -> R<(Option<&str>, i32)> {
    let mut message = None;
    let mut code = 0;
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            2 => message = optional_str(c.expect_len(w).at("message")?).at("message")?,
            3 => code = c.expect_varint(w).at("code")? as i32,
            _ => c.skip(w)?,
        }
    }
    Ok((message, code))
}

fn span(body: &[u8], origin: Origin<'_>, scope: Scope<'_>, sink: &mut Sink) -> R<()> {
    let Sink {
        builder,
        batches,
        stats,
        scratch,
    } = sink;
    scratch.clear();

    let mut trace_id: &[u8] = &[];
    let mut span_id: &[u8] = &[];
    let mut parent: &[u8] = &[];
    let mut name = "";
    let mut trace_state = None;
    let mut kind = 0i32;
    let (mut start_ns, mut end_ns) = (0i64, 0i64);
    let (mut status_message, mut status_code) = (None, 0i32);
    let mut flags = 0u32;
    let (mut dropped_attrs, mut dropped_events, mut dropped_links) = (0u32, 0u32, 0u32);
    let mut seen = Seen::new();

    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => trace_id = c.expect_len(w).at("trace_id")?,
            2 => span_id = c.expect_len(w).at("span_id")?,
            3 => {
                trace_state = optional_str(c.expect_len(w).at("trace_state")?).at("trace_state")?
            }
            4 => parent = c.expect_len(w).at("parent_span_id")?,
            5 => name = utf8(c.expect_len(w).at("name")?).at("name")?,
            6 => kind = c.expect_varint(w).at("kind")? as i32,
            7 => start_ns = c.expect_fixed64(w).at("start_time_unix_nano")? as i64,
            8 => end_ns = c.expect_fixed64(w).at("end_time_unix_nano")? as i64,
            9 => {
                let kv = c.expect_len(w).at("attributes")?;
                push_prefixed(&mut scratch.attrs, kv);
                let (key, value) = key_value(kv).at("attributes")?;
                // Only the attributes that are promoted have their value read.
                if PROMOTED.iter().any(|(_, names)| names.contains(&key)) {
                    let val = match value {
                        Some(v) => any_value(v).at("attributes.value")?,
                        None => Val::Other,
                    };
                    seen.record(key, val);
                }
            }
            10 => dropped_attrs = c.expect_varint(w).at("dropped_attributes_count")? as u32,
            11 => push_prefixed(&mut scratch.events, c.expect_len(w).at("events")?),
            12 => dropped_events = c.expect_varint(w).at("dropped_events_count")? as u32,
            13 => push_prefixed(&mut scratch.links, c.expect_len(w).at("links")?),
            14 => dropped_links = c.expect_varint(w).at("dropped_links_count")? as u32,
            15 => {
                (status_message, status_code) =
                    status(c.expect_len(w).at("status")?).at("status")?;
            }
            16 => flags = c.expect_fixed32(w).at("flags")?,
            _ => c.skip(w)?,
        }
    }

    let trace_id = TraceId::from_slice(trace_id)
        .map_err(|_| Fault::new("trace_id must be 16 bytes"))
        .at("trace_id")?;
    let span_id = SpanId::from_slice(span_id)
        .map_err(|_| Fault::new("span_id must be 8 bytes"))
        .at("span_id")?;
    // An empty or all-zero parent means the span is a root; the span table
    // marks roots with null.
    let parent_span_id = if parent.is_empty() {
        None
    } else {
        let p = SpanId::from_slice(parent)
            .map_err(|_| Fault::new("parent_span_id must be 8 bytes or empty"))
            .at("parent_span_id")?;
        (!p.is_zero()).then_some(p)
    };

    builder.append(&SpanRecord {
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        start_ns,
        end_ns,
        status_code,
        status_message,
        trace_state,
        flags,
        service_name: origin.service_name,
        scope_name: scope.name,
        scope_version: scope.version,
        resource_schema_url: origin.schema_url,
        scope_schema_url: scope.schema_url,
        operation: seen.text(Col::Operation),
        provider: seen.text(Col::Provider),
        request_model: seen.text(Col::RequestModel),
        response_model: seen.text(Col::ResponseModel),
        input_tokens: seen.count(Col::InputTokens),
        output_tokens: seen.count(Col::OutputTokens),
        conversation_id: seen.text(Col::ConversationId),
        session_id: seen.text(Col::SessionId),
        user_id: seen.text(Col::UserId),
        agent_name: seen.text(Col::AgentName),
        tool_name: seen.text(Col::ToolName),
        error_type: seen.text(Col::ErrorType),
        resource: origin.resource,
        scope: scope.raw,
        attrs: &scratch.attrs,
        events: non_empty(&scratch.events),
        links: non_empty(&scratch.links),
        dropped_attributes_count: dropped_attrs,
        dropped_events_count: dropped_events,
        dropped_links_count: dropped_links,
    });
    stats.spans += 1;
    if builder.is_full() {
        stats.bytes_copied += builder.value_bytes() as u64;
        batches.push(builder.finish());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Seen` indexes its rows by the column's position in `PROMOTED`.
    #[test]
    fn promoted_table_is_indexed_by_column() {
        for (i, (col, names)) in PROMOTED.iter().enumerate() {
            assert_eq!(*col as usize, i);
            assert!(!names.is_empty() && names.len() <= MAX_NAMES);
        }
    }
}
