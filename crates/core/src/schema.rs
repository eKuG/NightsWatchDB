//! The span table.
//!
//! One row per span. The columns are, in order:
//!
//! 1. The span itself: `trace_id`, `span_id`, `parent_span_id`, `name`,
//!    `kind`, `start_ns`, `end_ns`, `status_code`, `status_message`,
//!    `trace_state`, `flags`.
//! 2. Where it came from: `service_name`, `scope_name`, `scope_version`,
//!    `resource_schema_url`, `scope_schema_url`, and the raw `resource` and
//!    `scope` of group 4. These seven are dictionary columns, see below.
//! 3. Promoted fields (all nullable): `operation`, `provider`,
//!    `request_model`, `response_model`, `input_tokens`, `output_tokens`,
//!    `conversation_id`, `session_id`, `user_id`, `agent_name`, `tool_name`,
//!    `error_type`.
//! 4. The raw, lossless parts (`LargeBinary`): `resource`, `scope`, `attrs`,
//!    `events`, `links`. `resource` and `scope` are dictionary columns.
//! 5. `dropped_attributes_count`, `dropped_events_count`,
//!    `dropped_links_count`.
//! 6. `arrival` (`UInt64`, never null): the number of the write-ahead-log
//!    record the row arrived in. The engine sets it when a batch enters its
//!    buffer and it travels with the row through flush and merge. When a span
//!    is stored more than once, the copy with the smallest `arrival` is the
//!    one that is shown. A batch built by a decoder holds 0 here. It is not
//!    part of any API output and not in the summary.
//!
//! # Origin columns
//!
//! `resource`, `service_name`, `resource_schema_url`, `scope`, `scope_name`,
//! `scope_version` and `scope_schema_url` describe where a span came from and
//! are the same for every span of a `ResourceSpans` or `ScopeSpans`. They are
//! `Dictionary(Int32, <value type>)` columns: each distinct value is stored
//! once per batch and each row holds a 4-byte key. Without this, one large
//! resource shared by many small spans would be copied into every row and a
//! small request could decode to gigabytes. Nulls are null keys. Two batches
//! (or two row groups of a part) may number the same value differently, so a
//! key means something only together with the dictionary of its own array;
//! [`SpanColumns`](crate::SpanColumns) resolves it and hands out plain slices.
//!
//! # Promoted columns
//!
//! The promoted columns are copies of well-known attributes (model name,
//! token counts, session id and so on) made at ingest so that common queries
//! do not have to decode `attrs`. `attrs` always keeps the originals, under
//! the names the sender used; a promoted column is never the only place a
//! value lives.
//!
//! # Encoding of the raw columns
//!
//! `attrs`, `events` and `links` are each a concatenation of the original OTLP
//! messages, in the order received:
//!
//! | Column   | Message repeated                         |
//! |----------|------------------------------------------|
//! | `attrs`  | `opentelemetry.proto.common.v1.KeyValue` |
//! | `events` | `opentelemetry.proto.trace.v1.Span.Event`|
//! | `links`  | `opentelemetry.proto.trace.v1.Span.Link` |
//!
//! Each message is preceded by its length in bytes as a protobuf varint
//! (the same framing as length-delimited protobuf streams). An empty list is
//! zero bytes. `events` and `links` are null when the span had none and may
//! also be zero bytes; both mean "empty". `attrs` is never null.
//!
//! `resource` is the serialized OTLP `Resource` message exactly as received
//! (no length prefix), or null if the sender sent none. `scope` is the same
//! for the `InstrumentationScope` message: its name, version, attributes and
//! dropped-attributes count. `scope_name` and `scope_version` are copies of
//! two of its fields for filtering, as `service_name` is for the resource.
//!
//! `resource_schema_url` and `scope_schema_url` are the `schema_url` of the
//! `ResourceSpans` and `ScopeSpans` the span arrived in; they name the
//! semantic-convention version the sender used. Both are null when the
//! sender's value was empty or absent.
//!
//! Core does not parse these bytes; the ingest crates write them and the
//! query layer reads them.

use crate::{Error, Result};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, OnceLock};

macro_rules! columns {
    ($($id:ident = $name:literal),* $(,)?) => {
        $(
            #[doc = concat!("The `", $name, "` column.")]
            pub const $id: &str = $name;
        )*
    };
}

/// Column names of the span table.
pub mod col {
    columns! {
        TRACE_ID = "trace_id",
        SPAN_ID = "span_id",
        PARENT_SPAN_ID = "parent_span_id",
        NAME = "name",
        KIND = "kind",
        START_NS = "start_ns",
        END_NS = "end_ns",
        STATUS_CODE = "status_code",
        STATUS_MESSAGE = "status_message",
        TRACE_STATE = "trace_state",
        FLAGS = "flags",
        SERVICE_NAME = "service_name",
        SCOPE_NAME = "scope_name",
        SCOPE_VERSION = "scope_version",
        RESOURCE_SCHEMA_URL = "resource_schema_url",
        SCOPE_SCHEMA_URL = "scope_schema_url",
        OPERATION = "operation",
        PROVIDER = "provider",
        REQUEST_MODEL = "request_model",
        RESPONSE_MODEL = "response_model",
        INPUT_TOKENS = "input_tokens",
        OUTPUT_TOKENS = "output_tokens",
        CONVERSATION_ID = "conversation_id",
        SESSION_ID = "session_id",
        USER_ID = "user_id",
        AGENT_NAME = "agent_name",
        TOOL_NAME = "tool_name",
        ERROR_TYPE = "error_type",
        RESOURCE = "resource",
        SCOPE = "scope",
        ATTRS = "attrs",
        EVENTS = "events",
        LINKS = "links",
        DROPPED_ATTRIBUTES_COUNT = "dropped_attributes_count",
        DROPPED_EVENTS_COUNT = "dropped_events_count",
        DROPPED_LINKS_COUNT = "dropped_links_count",
        ARRIVAL = "arrival",
    }
}

/// Checks that `batch` has `schema`'s columns in order, with its names and
/// data types, and no nulls in a column `schema` declares non-nullable.
/// `what` ("span" or "summary") starts every message.
pub(crate) fn check_batch(what: &str, schema: &SchemaRef, batch: &RecordBatch) -> Result<()> {
    if batch.num_columns() != schema.fields().len() {
        return Err(Error::invalid_argument(format!(
            "{what} batch has {} columns, expected {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    for (i, want) in schema.fields().iter().enumerate() {
        let have = batch.schema_ref().field(i);
        if have.name() != want.name() {
            return Err(Error::invalid_argument(format!(
                "{what} batch column {i} is `{}`, expected `{}`",
                have.name(),
                want.name()
            )));
        }
        let array = batch.column(i);
        if array.data_type() != want.data_type() {
            return Err(Error::invalid_argument(format!(
                "{what} batch column `{}` has type {}, expected {}",
                want.name(),
                array.data_type(),
                want.data_type()
            )));
        }
        if !want.is_nullable() && array.null_count() > 0 {
            return Err(Error::invalid_argument(format!(
                "{what} batch column `{}` must not contain nulls",
                want.name()
            )));
        }
    }
    Ok(())
}

/// The Arrow schema of the span table. Built once and shared.
pub fn span_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(build).clone()
}

fn build() -> SchemaRef {
    use DataType::*;
    let f = |name: &str, ty: DataType, nullable: bool| Field::new(name, ty, nullable);
    let dict = |value: DataType| Dictionary(Box::new(Int32), Box::new(value));
    Arc::new(Schema::new(vec![
        f(col::TRACE_ID, FixedSizeBinary(16), false),
        f(col::SPAN_ID, FixedSizeBinary(8), false),
        f(col::PARENT_SPAN_ID, FixedSizeBinary(8), true),
        f(col::NAME, Utf8, false),
        f(col::KIND, Int32, false),
        f(col::START_NS, Int64, false),
        f(col::END_NS, Int64, false),
        f(col::STATUS_CODE, Int32, false),
        f(col::STATUS_MESSAGE, Utf8, true),
        f(col::TRACE_STATE, Utf8, true),
        f(col::FLAGS, UInt32, false),
        f(col::SERVICE_NAME, dict(Utf8), true),
        f(col::SCOPE_NAME, dict(Utf8), true),
        f(col::SCOPE_VERSION, dict(Utf8), true),
        f(col::RESOURCE_SCHEMA_URL, dict(Utf8), true),
        f(col::SCOPE_SCHEMA_URL, dict(Utf8), true),
        f(col::OPERATION, Utf8, true),
        f(col::PROVIDER, Utf8, true),
        f(col::REQUEST_MODEL, Utf8, true),
        f(col::RESPONSE_MODEL, Utf8, true),
        f(col::INPUT_TOKENS, Int64, true),
        f(col::OUTPUT_TOKENS, Int64, true),
        f(col::CONVERSATION_ID, Utf8, true),
        f(col::SESSION_ID, Utf8, true),
        f(col::USER_ID, Utf8, true),
        f(col::AGENT_NAME, Utf8, true),
        f(col::TOOL_NAME, Utf8, true),
        f(col::ERROR_TYPE, Utf8, true),
        f(col::RESOURCE, dict(LargeBinary), true),
        f(col::SCOPE, dict(LargeBinary), true),
        f(col::ATTRS, LargeBinary, false),
        f(col::EVENTS, LargeBinary, true),
        f(col::LINKS, LargeBinary, true),
        f(col::DROPPED_ATTRIBUTES_COUNT, UInt32, false),
        f(col::DROPPED_EVENTS_COUNT, UInt32, false),
        f(col::DROPPED_LINKS_COUNT, UInt32, false),
        f(col::ARRIVAL, UInt64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins name, type and nullability of every column, in order. If this
    /// fails the on-disk format changed.
    #[test]
    fn golden_span_schema() {
        let got: Vec<String> = span_schema()
            .fields()
            .iter()
            .map(|f| {
                format!(
                    "{}:{}:{}",
                    f.name(),
                    f.data_type(),
                    if f.is_nullable() { "null" } else { "required" }
                )
            })
            .collect();
        let want = [
            "trace_id:FixedSizeBinary(16):required",
            "span_id:FixedSizeBinary(8):required",
            "parent_span_id:FixedSizeBinary(8):null",
            "name:Utf8:required",
            "kind:Int32:required",
            "start_ns:Int64:required",
            "end_ns:Int64:required",
            "status_code:Int32:required",
            "status_message:Utf8:null",
            "trace_state:Utf8:null",
            "flags:UInt32:required",
            "service_name:Dictionary(Int32, Utf8):null",
            "scope_name:Dictionary(Int32, Utf8):null",
            "scope_version:Dictionary(Int32, Utf8):null",
            "resource_schema_url:Dictionary(Int32, Utf8):null",
            "scope_schema_url:Dictionary(Int32, Utf8):null",
            "operation:Utf8:null",
            "provider:Utf8:null",
            "request_model:Utf8:null",
            "response_model:Utf8:null",
            "input_tokens:Int64:null",
            "output_tokens:Int64:null",
            "conversation_id:Utf8:null",
            "session_id:Utf8:null",
            "user_id:Utf8:null",
            "agent_name:Utf8:null",
            "tool_name:Utf8:null",
            "error_type:Utf8:null",
            "resource:Dictionary(Int32, LargeBinary):null",
            "scope:Dictionary(Int32, LargeBinary):null",
            "attrs:LargeBinary:required",
            "events:LargeBinary:null",
            "links:LargeBinary:null",
            "dropped_attributes_count:UInt32:required",
            "dropped_events_count:UInt32:required",
            "dropped_links_count:UInt32:required",
            "arrival:UInt64:required",
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn schema_is_shared() {
        assert!(Arc::ptr_eq(&span_schema(), &span_schema()));
    }
}
