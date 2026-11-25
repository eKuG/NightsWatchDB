use crate::{SpanId, TraceId};

/// One span, borrowing its variable-length values.
///
/// Field names and meanings match the columns of the span table; see the
/// `schema` module for the encoding of `resource`, `scope`, `attrs`,
/// `events` and `links`. The default is an all-zero, empty span, which is only
/// useful as a base for struct update syntax.
///
/// The table stores the seven origin fields (`resource`, `service_name`,
/// `resource_schema_url`, `scope`, `scope_name`, `scope_version`,
/// `scope_schema_url`) once per distinct value, not once per row; a record
/// read from a batch has them resolved to plain slices.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SpanRecord<'a> {
    /// `trace_id` column.
    pub trace_id: TraceId,
    /// `span_id` column.
    pub span_id: SpanId,
    /// `parent_span_id` column; `None` for a root span.
    pub parent_span_id: Option<SpanId>,
    /// `name` column.
    pub name: &'a str,
    /// `kind` column: the OTLP `SpanKind` number.
    pub kind: i32,
    /// `start_ns` column: Unix nanoseconds.
    pub start_ns: i64,
    /// `end_ns` column: Unix nanoseconds.
    pub end_ns: i64,
    /// `status_code` column: the OTLP `StatusCode` number (2 is error).
    pub status_code: i32,
    /// `status_message` column.
    pub status_message: Option<&'a str>,
    /// `trace_state` column.
    pub trace_state: Option<&'a str>,
    /// `flags` column: the OTLP span flags.
    pub flags: u32,
    /// `service_name` column.
    pub service_name: Option<&'a str>,
    /// `scope_name` column.
    pub scope_name: Option<&'a str>,
    /// `scope_version` column.
    pub scope_version: Option<&'a str>,
    /// `resource_schema_url` column.
    pub resource_schema_url: Option<&'a str>,
    /// `scope_schema_url` column.
    pub scope_schema_url: Option<&'a str>,
    /// `operation` column (promoted attribute).
    pub operation: Option<&'a str>,
    /// `provider` column (promoted attribute).
    pub provider: Option<&'a str>,
    /// `request_model` column (promoted attribute).
    pub request_model: Option<&'a str>,
    /// `response_model` column (promoted attribute).
    pub response_model: Option<&'a str>,
    /// `input_tokens` column (promoted attribute).
    pub input_tokens: Option<i64>,
    /// `output_tokens` column (promoted attribute).
    pub output_tokens: Option<i64>,
    /// `conversation_id` column (promoted attribute).
    pub conversation_id: Option<&'a str>,
    /// `session_id` column (promoted attribute).
    pub session_id: Option<&'a str>,
    /// `user_id` column (promoted attribute).
    pub user_id: Option<&'a str>,
    /// `agent_name` column (promoted attribute).
    pub agent_name: Option<&'a str>,
    /// `tool_name` column (promoted attribute).
    pub tool_name: Option<&'a str>,
    /// `error_type` column (promoted attribute).
    pub error_type: Option<&'a str>,
    /// `resource` column: serialized OTLP `Resource`, as received.
    pub resource: Option<&'a [u8]>,
    /// `scope` column: serialized OTLP `InstrumentationScope`, as received.
    pub scope: Option<&'a [u8]>,
    /// `attrs` column: length-prefixed OTLP `KeyValue` messages.
    pub attrs: &'a [u8],
    /// `events` column: length-prefixed OTLP `Span.Event` messages.
    pub events: Option<&'a [u8]>,
    /// `links` column: length-prefixed OTLP `Span.Link` messages.
    pub links: Option<&'a [u8]>,
    /// `dropped_attributes_count` column.
    pub dropped_attributes_count: u32,
    /// `dropped_events_count` column.
    pub dropped_events_count: u32,
    /// `dropped_links_count` column.
    pub dropped_links_count: u32,
}
