use crate::wire::{At, Cur, Fault, R, put_varint, utf8};
use arrow_array::RecordBatch;
use nwdb_core::{
    BatchLimits, Origin, OriginKeys, RecordDecoder, Result, SpanBatchBuilder, SpanId, SpanRecord,
    TraceId,
};

/// The tag byte stored in front of an OTLP trace request in the write-ahead
/// log. The engine uses it to pick [`TraceDecoder`] on replay.
pub const RECORD_TAG_OTLP_TRACES: u8 = 1;

/// The default for [`TraceDecoder::with_max_origin_bytes`]: 64 KiB.
pub const DEFAULT_MAX_ORIGIN_BYTES: usize = 64 * 1024;

/// What one decode did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodeStats {
    /// Spans converted and stored.
    pub spans: u64,
    /// Bytes of variable-length values (strings and raw columns) written into
    /// the batches, as [`SpanBatchBuilder::value_bytes`] counts them: a
    /// resource or scope value is counted once per batch however many spans
    /// share it, and every row adds a 4-byte key for each of the seven origin
    /// columns. Other fixed-width columns are not counted.
    pub bytes_copied: u64,
    /// `ResourceSpans` messages seen.
    pub resources: u64,
    /// `ScopeSpans` messages seen.
    pub scopes: u64,
    /// Spans that were read and not stored: an id of the wrong length or all
    /// zero, or an origin value over the limit. They are not in `spans`.
    pub rejected_spans: u64,
    /// A short sentence about the first rejected span, for the reply to the
    /// sender; `None` if no span was rejected.
    pub rejected_reason: Option<String>,
}

/// Converts serialized OTLP trace requests into batches of the span table.
///
/// The decoder holds only its limits, so one value can serve any number of
/// threads at once.
#[derive(Debug, Clone, Copy)]
pub struct TraceDecoder {
    limits: BatchLimits,
    max_origin_bytes: usize,
}

impl TraceDecoder {
    /// Creates a decoder that closes a batch when `limits` is reached and
    /// refuses origin values over [`DEFAULT_MAX_ORIGIN_BYTES`].
    pub fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            max_origin_bytes: DEFAULT_MAX_ORIGIN_BYTES,
        }
    }

    /// Sets the largest size of one origin value: the serialized resource of
    /// a `ResourceSpans`, its schema URL, the serialized scope of a
    /// `ScopeSpans` and its schema URL. All spans of a message with a larger
    /// value are rejected and counted in [`DecodeStats::rejected_spans`]; the
    /// rest of the request is decoded.
    ///
    /// The span table stores each distinct origin value once per batch, so
    /// this limit together with the size of the request bounds what a
    /// request can make the batches hold.
    pub fn with_max_origin_bytes(mut self, max: usize) -> Self {
        self.max_origin_bytes = max;
        self
    }

    /// Decodes a serialized `ExportTraceServiceRequest`.
    ///
    /// Rows come out in the order the spans appear in the request. A batch is
    /// closed as soon as it reaches either bound of the limits, so a single
    /// span larger than the byte bound is alone in its batch. An empty request
    /// gives no batches. Damaged input is [`nwdb_core::Error::Corrupt`] and
    /// nothing is returned for it.
    ///
    /// A span whose trace or span id is not valid (the wrong length, or all
    /// zero), or whose resource or scope is over the origin limit, is skipped
    /// and the rest is decoded. Replay of the write-ahead log uses this call,
    /// and it skips exactly the spans that
    /// [`decode_with_stats`](Self::decode_with_stats) counts as rejected.
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
            max_origin_bytes: self.max_origin_bytes,
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
    max_origin_bytes: usize,
}

impl Sink {
    fn flush(&mut self) {
        if !self.builder.is_empty() {
            self.stats.bytes_copied += self.builder.value_bytes() as u64;
            self.batches.push(self.builder.finish());
        }
    }
}

impl DecodeStats {
    /// Counts a span that was read and not stored. Only the first reason is
    /// kept, with the position of the span among all spans of the request.
    fn reject(&mut self, why: &str) {
        self.rejected_spans += 1;
        if self.rejected_reason.is_none() {
            let n = self.spans + self.rejected_spans;
            self.rejected_reason = Some(format!("span {n} of the request: {why}"));
        }
    }
}

const TOO_BIG_RESOURCE: &str = "its resource or resource schema URL is over the size limit";
const TOO_BIG_SCOPE: &str = "its scope or scope schema URL is over the size limit";

/// What a span inherits from the `ResourceSpans` that holds it.
#[derive(Clone, Copy)]
struct ResourceCtx<'a> {
    /// The resource-level origin fields; the scope-level ones are unset.
    origin: Origin<'a>,
    /// Set when a value is over the size limit: every span is rejected.
    too_big: Option<&'static str>,
    /// The origin interned in the batch being built. Made when the first
    /// span needs it, so a message without spans stores nothing, and again
    /// after a batch has been finished.
    keys: Option<OriginKeys>,
}

/// What a span inherits from its `ScopeSpans`.
#[derive(Clone, Copy, Default)]
struct ScopeCtx<'a> {
    /// The scope-level origin fields; the resource-level ones are unset.
    origin: Origin<'a>,
    too_big: Option<&'static str>,
    keys: Option<OriginKeys>,
}

/// The keys of `origin` in the builder's current batch: `cached` if they are
/// still valid, otherwise interned now.
fn current_keys(
    builder: &mut SpanBatchBuilder,
    cached: &mut Option<OriginKeys>,
    origin: &Origin<'_>,
) -> OriginKeys {
    match *cached {
        Some(keys) if builder.is_current(&keys) => keys,
        _ => *cached.insert(builder.intern_origin(origin)),
    }
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
    // A value over the limit is not stored, and neither is any span of the
    // message. Its contents are not read either.
    let max = sink.max_origin_bytes;
    let too_big =
        resource.is_some_and(|r| r.len() > max) || schema_url.is_some_and(|u| u.len() > max);
    let service_name = match resource {
        Some(r) if !too_big => service_name(r).at("resource")?,
        _ => None,
    };
    let mut ctx = ResourceCtx {
        origin: Origin {
            resource,
            service_name,
            resource_schema_url: schema_url,
            ..Origin::default()
        },
        too_big: too_big.then_some(TOO_BIG_RESOURCE),
        keys: None,
    };
    sink.stats.resources += 1;

    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        if f == 2 {
            let ss = c.expect_len(w).at("scope_spans")?;
            scope_spans(ss, &mut ctx, sink).at("scope_spans")?;
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

fn scope_spans(body: &[u8], resource: &mut ResourceCtx<'_>, sink: &mut Sink) -> R<()> {
    // As with the resource, the scope may follow its spans.
    let mut scope = Origin::default();
    let mut too_big = false;
    let mut schema_url = None;
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => {
                let raw = c.expect_len(w).at("scope")?;
                // A repeated scope replaces the earlier one, schema URL aside.
                too_big = raw.len() > sink.max_origin_bytes;
                scope = if too_big {
                    Origin {
                        scope: Some(raw),
                        ..Origin::default()
                    }
                } else {
                    instrumentation_scope(raw).at("scope")?
                };
            }
            3 => schema_url = optional_str(c.expect_len(w).at("schema_url")?).at("schema_url")?,
            _ => c.skip(w)?,
        }
    }
    scope.scope_schema_url = schema_url;
    too_big |= schema_url.is_some_and(|u| u.len() > sink.max_origin_bytes);
    let mut ctx = ScopeCtx {
        origin: scope,
        too_big: too_big.then_some(TOO_BIG_SCOPE),
        keys: None,
    };
    sink.stats.scopes += 1;

    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        if f == 2 {
            let sp = c.expect_len(w).at("spans")?;
            span(sp, resource, &mut ctx, sink).at("spans")?;
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

fn instrumentation_scope(body: &[u8]) -> R<Origin<'_>> {
    let mut scope = Origin {
        scope: Some(body),
        ..Origin::default()
    };
    let mut c = Cur::new(body);
    while let Some((f, w)) = c.next_field()? {
        match f {
            1 => scope.scope_name = optional_str(c.expect_len(w).at("name")?).at("name")?,
            2 => {
                scope.scope_version = optional_str(c.expect_len(w).at("version")?).at("version")?
            }
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

fn span(
    body: &[u8],
    resource: &mut ResourceCtx<'_>,
    scope: &mut ScopeCtx<'_>,
    sink: &mut Sink,
) -> R<()> {
    let Sink {
        builder,
        batches,
        stats,
        scratch,
        max_origin_bytes: _,
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

    // An empty or all-zero parent means the span is a root; the span table
    // marks roots with null. A parent of any other length is damage.
    let parent_span_id = if parent.is_empty() {
        None
    } else {
        let p = SpanId::from_slice(parent)
            .map_err(|_| Fault::new("parent_span_id must be 8 bytes or empty"))
            .at("parent_span_id")?;
        (!p.is_zero()).then_some(p)
    };

    // Past this point the span was read whole. One that cannot be stored is
    // counted and skipped, so the rest of the request still is.
    if let Some(why) = resource.too_big.or(scope.too_big) {
        stats.reject(why);
        return Ok(());
    }
    // OTLP defines an all-zero id as invalid, like an empty one. Two spans
    // with the same zero id would also be taken for duplicates of each other.
    let ids = match (TraceId::from_slice(trace_id), SpanId::from_slice(span_id)) {
        (Ok(t), _) if t.is_zero() => Err("trace_id is all zero"),
        (_, Ok(s)) if s.is_zero() => Err("span_id is all zero"),
        (Ok(t), Ok(s)) => Ok((t, s)),
        (Err(_), _) => Err("trace_id is not 16 bytes"),
        (_, Err(_)) => Err("span_id is not 8 bytes"),
    };
    let Ok((trace_id, span_id)) = ids else {
        if let Err(why) = ids {
            stats.reject(why);
        }
        return Ok(());
    };

    let keys = OriginKeys::combine(
        &current_keys(builder, &mut resource.keys, &resource.origin),
        &current_keys(builder, &mut scope.keys, &scope.origin),
    );
    builder.append_span(
        &SpanRecord {
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
            attrs: &scratch.attrs,
            events: non_empty(&scratch.events),
            links: non_empty(&scratch.links),
            dropped_attributes_count: dropped_attrs,
            dropped_events_count: dropped_events,
            dropped_links_count: dropped_links,
            // The origin comes from `keys`.
            ..SpanRecord::default()
        },
        &keys,
    );
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
