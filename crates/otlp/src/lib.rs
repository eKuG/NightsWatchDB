//! Converts OTLP trace requests into batches of the span table, and back.
//!
//! [`TraceDecoder`] reads the protobuf bytes of an
//! `ExportTraceServiceRequest` exactly as an OTLP sender produces them. It
//! walks the wire format itself and has no protobuf dependency: strings are
//! checked and copied once from the request into the Arrow builders, and the
//! raw columns keep the original message bytes. [`encode_spans`] goes the
//! other way for the query API.
//!
//! # Column mapping
//!
//! | Column | Source |
//! |---|---|
//! | `trace_id`, `span_id` | `Span.trace_id`, `Span.span_id`; must be 16 and 8 bytes |
//! | `parent_span_id` | `Span.parent_span_id`; null when empty or all zero (a root) |
//! | `name`, `kind`, `start_ns`, `end_ns`, `flags` | the span fields of the same name |
//! | `status_code`, `status_message` | `Span.status.code`, `Span.status.message` (empty is null) |
//! | `trace_state` | `Span.trace_state` (empty is null) |
//! | `dropped_*_count` | `Span.dropped_attributes_count`, `dropped_events_count`, `dropped_links_count` |
//! | `service_name` | string value of resource attribute `service.name` |
//! | `scope_name`, `scope_version` | `InstrumentationScope.name`, `.version` (empty is null) |
//! | `resource_schema_url`, `scope_schema_url` | `ResourceSpans.schema_url`, `ScopeSpans.schema_url` (empty is null) |
//! | `operation` | `gen_ai.operation.name` |
//! | `provider` | `gen_ai.provider.name`, else `gen_ai.system` |
//! | `request_model` | `gen_ai.request.model` |
//! | `response_model` | `gen_ai.response.model` |
//! | `input_tokens` | `gen_ai.usage.input_tokens`, else `gen_ai.usage.prompt_tokens` |
//! | `output_tokens` | `gen_ai.usage.output_tokens`, else `gen_ai.usage.completion_tokens` |
//! | `conversation_id` | `gen_ai.conversation.id` |
//! | `session_id` | `session.id` |
//! | `user_id` | `user.id` |
//! | `agent_name` | `gen_ai.agent.name` |
//! | `tool_name` | `gen_ai.tool.name` |
//! | `error_type` | `error.type` |
//! | `resource`, `scope` | the `Resource` and `InstrumentationScope` message bytes as received; null when absent |
//! | `attrs` | each span `KeyValue`, bytes as received, each preceded by its length as a varint |
//! | `events`, `links` | likewise for `Span.Event` and `Span.Link`; null when there are none |
//!
//! Promoted columns are copies; `attrs` keeps the originals. A text column
//! takes a string value. A token column takes an int value or a string that
//! parses as a base-10 `i64`. Any other value type is unusable. The first name
//! in the row whose value is usable is the one used, so the newer name wins
//! over the older one whatever their order, and a newer name with an unusable
//! value does not hide a usable older one. When a key occurs more than once
//! the last occurrence is its value. The table lives in the source as
//! `PROMOTED`.
//!
//! # Wire rules
//!
//! Fields may come in any order and are skipped by wire type when unknown. For
//! a field that should occur once, the last occurrence wins; a repeated
//! message-typed field (`resource`, `scope`, `status`) is replaced, not merged.
//! Groups, truncated input, lengths past the end of the enclosing message,
//! varints over 10 bytes, a wrong wire type on a known field, ids of the wrong
//! length and invalid UTF-8 in a field that is read are all
//! [`nwdb_core::Error::Corrupt`] with the field path in the message. Nothing
//! on this path panics or allocates from an unchecked length.

mod decode;
mod encode;
mod json;
mod wire;

pub use decode::{DecodeStats, RECORD_TAG_OTLP_TRACES, TraceDecoder};
pub use encode::encode_spans;
pub use json::{attrs_to_json, events_to_json, links_to_json, resource_to_json};
