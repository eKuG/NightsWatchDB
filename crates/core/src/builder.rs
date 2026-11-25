use crate::{BatchLimits, SpanRecord, span_schema};
use arrow_array::builder::{
    FixedSizeBinaryBuilder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use std::sync::Arc;

// Pre-sizing. Row-count capacity is capped so a huge `max_rows` does not
// reserve memory for rows that may never come. Short string columns reserve
// 32 bytes a row. The four raw columns reserve 1 MiB of data up front: large
// enough that typical rows do not grow the buffer at all, small enough that
// an idle builder costs only a few MiB. Beyond that, Arrow's buffers grow
// geometrically, so a 1 MiB value costs a handful of reallocations, not one
// per value.
const MAX_PRESIZED_ROWS: usize = 8_192;
const SHORT_STRING_BYTES: usize = 32;
const RAW_COLUMN_BYTES: usize = 1 << 20;

/// Accumulates [`SpanRecord`]s into an Arrow batch with the span schema.
///
/// A builder is full when it holds `max_rows` rows or `max_bytes` value bytes
/// (see [`BatchLimits`]). It never refuses a row: the caller checks
/// [`is_full`](Self::is_full) after appending and calls
/// [`finish`](Self::finish). A single row larger than `max_bytes` therefore
/// ends up alone in a batch rather than being rejected.
pub struct SpanBatchBuilder {
    limits: BatchLimits,
    rows: usize,
    value_bytes: usize,

    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    parent_span_id: FixedSizeBinaryBuilder,
    name: StringBuilder,
    kind: Int32Builder,
    start_ns: Int64Builder,
    end_ns: Int64Builder,
    status_code: Int32Builder,
    status_message: StringBuilder,
    trace_state: StringBuilder,
    flags: UInt32Builder,
    service_name: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    operation: StringBuilder,
    provider: StringBuilder,
    request_model: StringBuilder,
    response_model: StringBuilder,
    input_tokens: Int64Builder,
    output_tokens: Int64Builder,
    conversation_id: StringBuilder,
    session_id: StringBuilder,
    user_id: StringBuilder,
    agent_name: StringBuilder,
    tool_name: StringBuilder,
    error_type: StringBuilder,
    resource: LargeBinaryBuilder,
    attrs: LargeBinaryBuilder,
    events: LargeBinaryBuilder,
    links: LargeBinaryBuilder,
    dropped_attributes_count: UInt32Builder,
    dropped_events_count: UInt32Builder,
    dropped_links_count: UInt32Builder,
}

fn put_str(b: &mut StringBuilder, v: Option<&str>) -> usize {
    b.append_option(v);
    v.map_or(0, str::len)
}

fn put_bytes(b: &mut LargeBinaryBuilder, v: Option<&[u8]>) -> usize {
    b.append_option(v);
    v.map_or(0, <[u8]>::len)
}

impl SpanBatchBuilder {
    /// Creates an empty builder with the given limits.
    pub fn new(limits: BatchLimits) -> Self {
        let n = limits.max_rows.min(MAX_PRESIZED_ROWS);
        let short = || StringBuilder::with_capacity(n, n * SHORT_STRING_BYTES);
        let raw = || LargeBinaryBuilder::with_capacity(n, RAW_COLUMN_BYTES);
        Self {
            limits,
            rows: 0,
            value_bytes: 0,
            trace_id: FixedSizeBinaryBuilder::with_capacity(n, 16),
            span_id: FixedSizeBinaryBuilder::with_capacity(n, 8),
            parent_span_id: FixedSizeBinaryBuilder::with_capacity(n, 8),
            name: short(),
            kind: Int32Builder::with_capacity(n),
            start_ns: Int64Builder::with_capacity(n),
            end_ns: Int64Builder::with_capacity(n),
            status_code: Int32Builder::with_capacity(n),
            status_message: short(),
            trace_state: short(),
            flags: UInt32Builder::with_capacity(n),
            service_name: short(),
            scope_name: short(),
            scope_version: short(),
            operation: short(),
            provider: short(),
            request_model: short(),
            response_model: short(),
            input_tokens: Int64Builder::with_capacity(n),
            output_tokens: Int64Builder::with_capacity(n),
            conversation_id: short(),
            session_id: short(),
            user_id: short(),
            agent_name: short(),
            tool_name: short(),
            error_type: short(),
            resource: raw(),
            attrs: raw(),
            events: raw(),
            links: raw(),
            dropped_attributes_count: UInt32Builder::with_capacity(n),
            dropped_events_count: UInt32Builder::with_capacity(n),
            dropped_links_count: UInt32Builder::with_capacity(n),
        }
    }

    /// Appends one span.
    pub fn append(&mut self, r: &SpanRecord<'_>) {
        // The id types fix the lengths, so these appends cannot fail.
        const LEN: &str = "id length is fixed by its type";
        self.trace_id.append_value(r.trace_id.0).expect(LEN);
        self.span_id.append_value(r.span_id.0).expect(LEN);
        match r.parent_span_id {
            Some(p) => self.parent_span_id.append_value(p.0).expect(LEN),
            None => self.parent_span_id.append_null(),
        }
        self.name.append_value(r.name);
        self.kind.append_value(r.kind);
        self.start_ns.append_value(r.start_ns);
        self.end_ns.append_value(r.end_ns);
        self.status_code.append_value(r.status_code);
        self.flags.append_value(r.flags);
        self.input_tokens.append_option(r.input_tokens);
        self.output_tokens.append_option(r.output_tokens);
        self.attrs.append_value(r.attrs);
        self.dropped_attributes_count
            .append_value(r.dropped_attributes_count);
        self.dropped_events_count
            .append_value(r.dropped_events_count);
        self.dropped_links_count.append_value(r.dropped_links_count);

        let mut bytes = r.name.len() + r.attrs.len();
        bytes += put_str(&mut self.status_message, r.status_message);
        bytes += put_str(&mut self.trace_state, r.trace_state);
        bytes += put_str(&mut self.service_name, r.service_name);
        bytes += put_str(&mut self.scope_name, r.scope_name);
        bytes += put_str(&mut self.scope_version, r.scope_version);
        bytes += put_str(&mut self.operation, r.operation);
        bytes += put_str(&mut self.provider, r.provider);
        bytes += put_str(&mut self.request_model, r.request_model);
        bytes += put_str(&mut self.response_model, r.response_model);
        bytes += put_str(&mut self.conversation_id, r.conversation_id);
        bytes += put_str(&mut self.session_id, r.session_id);
        bytes += put_str(&mut self.user_id, r.user_id);
        bytes += put_str(&mut self.agent_name, r.agent_name);
        bytes += put_str(&mut self.tool_name, r.tool_name);
        bytes += put_str(&mut self.error_type, r.error_type);
        bytes += put_bytes(&mut self.resource, r.resource);
        bytes += put_bytes(&mut self.events, r.events);
        bytes += put_bytes(&mut self.links, r.links);

        self.rows += 1;
        self.value_bytes += bytes;
    }

    /// Number of rows appended since the last [`finish`](Self::finish).
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Sum of the lengths of all variable-length values appended since the
    /// last [`finish`](Self::finish): every string and every raw column.
    /// Fixed-width columns are not counted.
    pub fn value_bytes(&self) -> usize {
        self.value_bytes
    }

    /// True if no row has been appended.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// True once the row bound or the byte bound has been reached.
    pub fn is_full(&self) -> bool {
        self.rows >= self.limits.max_rows || self.value_bytes >= self.limits.max_bytes
    }

    /// Returns the rows appended so far as a batch with the span schema and
    /// resets the builder to empty. Buffers are handed to the batch, so the
    /// next rows start from fresh allocations.
    pub fn finish(&mut self) -> RecordBatch {
        self.rows = 0;
        self.value_bytes = 0;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.parent_span_id.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.start_ns.finish()),
            Arc::new(self.end_ns.finish()),
            Arc::new(self.status_code.finish()),
            Arc::new(self.status_message.finish()),
            Arc::new(self.trace_state.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.operation.finish()),
            Arc::new(self.provider.finish()),
            Arc::new(self.request_model.finish()),
            Arc::new(self.response_model.finish()),
            Arc::new(self.input_tokens.finish()),
            Arc::new(self.output_tokens.finish()),
            Arc::new(self.conversation_id.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.user_id.finish()),
            Arc::new(self.agent_name.finish()),
            Arc::new(self.tool_name.finish()),
            Arc::new(self.error_type.finish()),
            Arc::new(self.resource.finish()),
            Arc::new(self.attrs.finish()),
            Arc::new(self.events.finish()),
            Arc::new(self.links.finish()),
            Arc::new(self.dropped_attributes_count.finish()),
            Arc::new(self.dropped_events_count.finish()),
            Arc::new(self.dropped_links_count.finish()),
        ];
        // The column order above is the schema order, which the schema's
        // golden test and the round-trip tests pin.
        RecordBatch::try_new(span_schema(), columns).expect("builders produce the span schema")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SpanColumns;

    #[test]
    fn full_on_row_bound() {
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 3,
            max_bytes: usize::MAX,
        });
        let r = SpanRecord::default();
        assert!(b.is_empty() && !b.is_full());
        b.append(&r);
        b.append(&r);
        assert!(!b.is_full());
        b.append(&r);
        assert!(b.is_full());
        b.finish();
        assert!(!b.is_full());
    }

    #[test]
    fn full_on_byte_bound() {
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 1_000,
            max_bytes: 100,
        });
        let r = SpanRecord {
            name: "0123456789",        // 10
            session_id: Some("abcde"), // 5
            attrs: &[0; 35],           // 35
            resource: Some(&[0; 10]),  // 10
            ..SpanRecord::default()
        };
        b.append(&r);
        assert_eq!(b.value_bytes(), 60);
        assert!(!b.is_full());
        b.append(&r);
        assert_eq!(b.value_bytes(), 120);
        assert!(b.is_full());
        assert_eq!(b.rows(), 2);
    }

    #[test]
    fn oversized_row_is_accepted() {
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 10,
            max_bytes: 4,
        });
        let big = vec![7u8; 1000];
        b.append(&SpanRecord {
            attrs: &big,
            ..SpanRecord::default()
        });
        assert!(b.is_full());
        let batch = b.finish();
        assert_eq!(
            SpanColumns::try_new(&batch).unwrap().record(0).attrs,
            &big[..]
        );
    }
}
