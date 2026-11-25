use crate::origin::{ORIGIN_COLUMNS, slot};
use crate::{BatchLimits, Origin, OriginKeys, SpanColumns, SpanRecord, span_schema};
use arrow_array::builder::{
    FixedSizeBinaryBuilder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    UInt32Builder,
};
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, RecordBatch};
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Pre-sizing. An idle builder reserves a few tens of KiB in total (it is not
// counted in `value_bytes`): room for `PRESIZED_ROWS` rows of fixed-width
// values and offsets, a few bytes per short string and 64 bytes of `attrs` per
// row. Arrow's buffers then grow geometrically, so a large value costs a
// handful of reallocations, not one per value.
const PRESIZED_ROWS: usize = 64;
const SHORT_STRING_BYTES: usize = 8;
const ATTRS_BYTES_PER_ROW: usize = 64;

/// Largest number of bytes one `Utf8` column may hold before the builder
/// reports itself full. Arrow's `Utf8` offsets are 32 bit, so a column cannot
/// hold 2 GiB; stopping at 1 GiB leaves room for one more value of up to 1 GiB.
const STRING_COLUMN_LIMIT: usize = 1 << 30;

/// Bytes of the key every row stores for each origin column.
const KEY_BYTES: usize = std::mem::size_of::<i32>();

/// Numbers the batches of all builders. A batch number is never reused, so
/// keys made for one batch cannot be mistaken for keys of another batch or
/// another builder.
static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);

fn next_batch() -> u64 {
    NEXT_BATCH.fetch_add(1, Ordering::Relaxed)
}

/// The distinct values of one origin column in the batch being built, each
/// stored once. Values are found by hashing their content, which is done once
/// per [`SpanBatchBuilder::intern_origin`] call and not per row.
#[derive(Default)]
struct Interner {
    /// All values, back to back.
    data: Vec<u8>,
    /// End of each value in `data`; the key of a value is its position.
    ends: Vec<usize>,
    /// Content hash to the newest key with that hash.
    newest: HashMap<u64, u32>,
    /// For each key, the previous key with the same hash.
    older: Vec<Option<u32>>,
}

impl Interner {
    fn value(&self, key: u32) -> &[u8] {
        let key = key as usize;
        let start = if key == 0 { 0 } else { self.ends[key - 1] };
        &self.data[start..self.ends[key]]
    }

    fn intern(&mut self, hasher: &RandomState, value: &[u8]) -> u32 {
        let hash = hasher.hash_one(value);
        let mut candidate = self.newest.get(&hash).copied();
        while let Some(key) = candidate {
            if self.value(key) == value {
                return key;
            }
            candidate = self.older[key as usize];
        }
        // Keys are 32-bit in the table's `Int32` key columns, and a key past
        // `i32::MAX` would need more than 2^31 distinct values in one batch.
        let key = i32::try_from(self.ends.len()).expect("a batch has under 2^31 distinct values");
        let key = key as u32;
        self.older.push(self.newest.insert(hash, key));
        self.data.extend_from_slice(value);
        self.ends.push(self.data.len());
        key
    }

    /// Hands out the values in key order and starts over.
    fn take(&mut self) -> (Vec<u8>, Vec<usize>) {
        self.newest.clear();
        self.older.clear();
        (
            std::mem::take(&mut self.data),
            std::mem::take(&mut self.ends),
        )
    }
}

/// One origin column: the key of every row so far, and its dictionary.
struct OriginColumn {
    keys: Int32Builder,
    dict: Interner,
}

impl OriginColumn {
    fn new(rows: usize) -> Self {
        Self {
            keys: Int32Builder::with_capacity(rows),
            dict: Interner::default(),
        }
    }

    fn finish_with(&mut self, values: ArrayRef) -> ArrayRef {
        let keys = self.keys.finish();
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, values)
                .expect("every key was handed out by this builder's dictionary"),
        )
    }

    fn finish_strings(&mut self) -> ArrayRef {
        let (data, ends) = self.dict.take();
        let mut values = StringBuilder::with_capacity(ends.len(), data.len());
        let mut start = 0;
        for end in ends {
            // Each value was a `&str` when it was interned.
            values.append_value(std::str::from_utf8(&data[start..end]).expect("interned as str"));
            start = end;
        }
        self.finish_with(Arc::new(values.finish()))
    }

    fn finish_bytes(&mut self) -> ArrayRef {
        let (data, ends) = self.dict.take();
        let mut values = LargeBinaryBuilder::with_capacity(ends.len(), data.len());
        let mut start = 0;
        for end in ends {
            values.append_value(&data[start..end]);
            start = end;
        }
        self.finish_with(Arc::new(values.finish()))
    }
}

/// Accumulates [`SpanRecord`]s into an Arrow batch with the span schema.
///
/// A builder is full when it holds `max_rows` rows or `max_bytes` value bytes
/// (see [`BatchLimits`]). It never refuses a row: the caller checks
/// [`is_full`](Self::is_full) after appending and calls
/// [`finish`](Self::finish). A single row larger than `max_bytes` therefore
/// ends up alone in a batch rather than being rejected.
///
/// The builder is also full once any single string column holds 1 GiB, because
/// Arrow's `Utf8` offsets are 32 bit. Callers must check `is_full` after every
/// append: a string column of 2 GiB or more cannot be represented and
/// `append` would panic on it. The check prevents that as long as no single
/// string value is longer than 1 GiB, so callers must reject longer ones.
///
/// # Origin values
///
/// The seven origin columns (see [`Origin`]) are dictionary columns: the
/// builder stores each distinct value once and every row holds a key, so a
/// large resource shared by many spans costs its size once, not once per
/// span. Interning is done by this builder, by content hash, once per
/// [`intern_origin`](Self::intern_origin) call:
///
/// - [`append_span`](Self::append_span) appends a row with keys the caller
///   already has. Its cost does not depend on the size of the origin values.
///   Use it when many spans share an origin.
/// - [`append_from`](Self::append_from) copies a row from another batch and
///   hashes each distinct origin of that batch once per output batch.
/// - [`append`](Self::append) interns the record's own origin values on every
///   call. It is the convenient path and hashes the origin values per row.
pub struct SpanBatchBuilder {
    limits: BatchLimits,
    rows: usize,
    /// Value bytes of the rows, with the keys but without the dictionaries.
    value_bytes: usize,
    /// Bytes held by the fullest string column other than the origin ones.
    max_string_column: usize,
    string_column_limit: usize,
    /// Number of the batch being built; see [`OriginKeys`].
    batch: u64,
    hasher: RandomState,
    /// For `append_from`: the keys already interned for a row of a source
    /// batch, by the source's number and its dictionary keys of the row.
    copied: HashMap<(usize, [Option<u32>; ORIGIN_COLUMNS]), OriginKeys>,

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
    /// The origin columns, in [`Origin`] field order.
    origin: [OriginColumn; ORIGIN_COLUMNS],
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
    attrs: LargeBinaryBuilder,
    events: LargeBinaryBuilder,
    links: LargeBinaryBuilder,
    dropped_attributes_count: UInt32Builder,
    dropped_events_count: UInt32Builder,
    dropped_links_count: UInt32Builder,
}

fn put_str(b: &mut StringBuilder, v: Option<&str>, fullest: &mut usize) -> usize {
    b.append_option(v);
    *fullest = (*fullest).max(b.values_slice().len());
    v.map_or(0, str::len)
}

fn put_bytes(b: &mut LargeBinaryBuilder, v: Option<&[u8]>) -> usize {
    b.append_option(v);
    v.map_or(0, <[u8]>::len)
}

/// Origin columns whose dictionary holds `Utf8` values, which have 32-bit
/// offsets.
const STRING_ORIGINS: [usize; 5] = [
    slot::SERVICE_NAME,
    slot::RESOURCE_SCHEMA_URL,
    slot::SCOPE_NAME,
    slot::SCOPE_VERSION,
    slot::SCOPE_SCHEMA_URL,
];

impl SpanBatchBuilder {
    /// Creates an empty builder with the given limits.
    pub fn new(limits: BatchLimits) -> Self {
        let n = limits.max_rows.min(PRESIZED_ROWS);
        let short = || StringBuilder::with_capacity(n, n * SHORT_STRING_BYTES);
        let raw = |bytes_per_row: usize| LargeBinaryBuilder::with_capacity(n, n * bytes_per_row);
        Self {
            limits,
            rows: 0,
            value_bytes: 0,
            max_string_column: 0,
            string_column_limit: STRING_COLUMN_LIMIT,
            batch: next_batch(),
            hasher: RandomState::new(),
            copied: HashMap::new(),
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
            origin: std::array::from_fn(|_| OriginColumn::new(n)),
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
            attrs: raw(ATTRS_BYTES_PER_ROW),
            events: raw(0),
            links: raw(0),
            dropped_attributes_count: UInt32Builder::with_capacity(n),
            dropped_events_count: UInt32Builder::with_capacity(n),
            dropped_links_count: UInt32Builder::with_capacity(n),
        }
    }

    /// Stores the values of `origin` in the dictionaries of the batch being
    /// built, each distinct value once, and returns their keys.
    ///
    /// Values are found by hashing their content, so the cost is linear in
    /// their size, whatever the number of rows that will use them. A value
    /// that is absent gets no key (a null). The keys are valid until the next
    /// [`finish`](Self::finish); after it, [`append_span`](Self::append_span)
    /// panics when given them.
    ///
    /// Interning does not add a row, but a dictionary value counts in
    /// [`value_bytes`](Self::value_bytes) as soon as it is stored, whether or
    /// not a row uses it.
    pub fn intern_origin(&mut self, origin: &Origin<'_>) -> OriginKeys {
        let values: [Option<&[u8]>; ORIGIN_COLUMNS] = [
            origin.resource,
            origin.service_name.map(str::as_bytes),
            origin.resource_schema_url.map(str::as_bytes),
            origin.scope,
            origin.scope_name.map(str::as_bytes),
            origin.scope_version.map(str::as_bytes),
            origin.scope_schema_url.map(str::as_bytes),
        ];
        let mut keys = [None; ORIGIN_COLUMNS];
        for ((out, column), value) in keys.iter_mut().zip(&mut self.origin).zip(values) {
            *out = value.map(|v| column.dict.intern(&self.hasher, v));
        }
        OriginKeys {
            batch: self.batch,
            keys,
        }
    }

    /// True if `keys` were made for the batch being built, that is, no
    /// [`finish`](Self::finish) has happened since.
    pub fn is_current(&self, keys: &OriginKeys) -> bool {
        keys.batch == self.batch
    }

    /// Appends one span with the origin given by `origin`, which must be from
    /// [`intern_origin`](Self::intern_origin) on this builder since its last
    /// [`finish`](Self::finish). The origin fields of `r` are ignored. The
    /// cost does not depend on the size of the origin values.
    ///
    /// # Panics
    ///
    /// Panics if `origin` is not current, instead of storing keys that point
    /// at the wrong values.
    pub fn append_span(&mut self, r: &SpanRecord<'_>, origin: &OriginKeys) {
        assert!(
            self.is_current(origin),
            "origin keys are from a batch that was already finished, or from another builder"
        );
        // The id types fix the lengths, so these appends cannot fail.
        const LEN: &str = "id length is fixed by its type";
        self.trace_id.append_value(r.trace_id.0).expect(LEN);
        self.span_id.append_value(r.span_id.0).expect(LEN);
        match r.parent_span_id {
            Some(p) => self.parent_span_id.append_value(p.0).expect(LEN),
            None => self.parent_span_id.append_null(),
        }
        self.name.append_value(r.name);
        let mut fullest = self.max_string_column.max(self.name.values_slice().len());
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
        for (column, key) in self.origin.iter_mut().zip(origin.keys) {
            // The key was handed out by this batch's dictionary, so it fits.
            column.keys.append_option(key.map(|k| k as i32));
        }

        let mut bytes = r.name.len() + r.attrs.len() + ORIGIN_COLUMNS * KEY_BYTES;
        bytes += put_str(&mut self.status_message, r.status_message, &mut fullest);
        bytes += put_str(&mut self.trace_state, r.trace_state, &mut fullest);
        bytes += put_str(&mut self.operation, r.operation, &mut fullest);
        bytes += put_str(&mut self.provider, r.provider, &mut fullest);
        bytes += put_str(&mut self.request_model, r.request_model, &mut fullest);
        bytes += put_str(&mut self.response_model, r.response_model, &mut fullest);
        bytes += put_str(&mut self.conversation_id, r.conversation_id, &mut fullest);
        bytes += put_str(&mut self.session_id, r.session_id, &mut fullest);
        bytes += put_str(&mut self.user_id, r.user_id, &mut fullest);
        bytes += put_str(&mut self.agent_name, r.agent_name, &mut fullest);
        bytes += put_str(&mut self.tool_name, r.tool_name, &mut fullest);
        bytes += put_str(&mut self.error_type, r.error_type, &mut fullest);
        bytes += put_bytes(&mut self.events, r.events);
        bytes += put_bytes(&mut self.links, r.links);

        self.rows += 1;
        self.value_bytes += bytes;
        self.max_string_column = fullest;
    }

    /// Appends one span, interning its origin fields first.
    ///
    /// This hashes the record's origin values on every call. When many spans
    /// share an origin, intern it once with
    /// [`intern_origin`](Self::intern_origin) and use
    /// [`append_span`](Self::append_span); when copying rows between batches,
    /// use [`append_from`](Self::append_from).
    pub fn append(&mut self, r: &SpanRecord<'_>) {
        let keys = self.intern_origin(&Origin::of(r));
        self.append_span(r, &keys);
    }

    /// Appends row `row` of `columns`, a batch with the span schema.
    ///
    /// `source` is the caller's number for that batch and must stay the same
    /// batch for as long as this builder is used (a builder that has finished
    /// batches keeps what it learned about a source). The origin values of a
    /// source row are interned the first time the builder sees that
    /// combination of source dictionary keys, and its keys are reused for the
    /// later rows of the same source with the same combination: a distinct
    /// origin of a source batch is hashed once per output batch, not once per
    /// row.
    ///
    /// # Panics
    ///
    /// Panics if `row` is out of range for `columns`.
    pub fn append_from(&mut self, columns: &SpanColumns<'_>, source: usize, row: usize) {
        let record = columns.record(row);
        let cache_key = (source, columns.origin_keys(row));
        let keys = match self.copied.get(&cache_key) {
            Some(keys) => *keys,
            None => {
                let keys = self.intern_origin(&Origin::of(&record));
                self.copied.insert(cache_key, keys);
                keys
            }
        };
        self.append_span(&record, &keys);
    }

    /// Number of rows appended since the last [`finish`](Self::finish).
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Sum of the lengths of the variable-length values of the batch being
    /// built: every string and every raw column of every row, except that
    /// each origin value is counted once however many rows use it, plus the
    /// 4-byte key every row holds for each of the seven origin columns.
    /// Other fixed-width columns are not counted.
    pub fn value_bytes(&self) -> usize {
        self.value_bytes + self.origin.iter().map(|c| c.dict.data.len()).sum::<usize>()
    }

    /// True if no row has been appended.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// True once the row bound or the byte bound has been reached, or any
    /// single string column holds 1 GiB. Callers must check this after every
    /// append and [`finish`](Self::finish) when it is true.
    pub fn is_full(&self) -> bool {
        let origin_strings = STRING_ORIGINS
            .iter()
            .map(|&i| self.origin[i].dict.data.len())
            .max()
            .unwrap_or(0);
        self.rows >= self.limits.max_rows
            || self.value_bytes() >= self.limits.max_bytes
            || self.max_string_column.max(origin_strings) >= self.string_column_limit
    }

    /// Returns the rows appended so far as a batch with the span schema and
    /// resets the builder to empty. Buffers are handed to the batch, so the
    /// next rows start from fresh allocations. Every [`OriginKeys`] made
    /// before this call is invalid after it.
    pub fn finish(&mut self) -> RecordBatch {
        self.rows = 0;
        self.value_bytes = 0;
        self.max_string_column = 0;
        self.batch = next_batch();
        self.copied.clear();
        let [
            resource,
            service_name,
            resource_schema_url,
            scope,
            scope_name,
            scope_version,
            scope_schema_url,
        ] = &mut self.origin;
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
            service_name.finish_strings(),
            scope_name.finish_strings(),
            scope_version.finish_strings(),
            resource_schema_url.finish_strings(),
            scope_schema_url.finish_strings(),
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
            resource.finish_bytes(),
            scope.finish_bytes(),
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
    use crate::{SpanColumns, SpanId, TraceId};
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

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
            name: "0123456789",              // 10
            session_id: Some("abcde"),       // 5
            attrs: &[0; 35],                 // 35
            resource: Some(&[0; 10]),        // 10, once
            scope: Some(&[0; 3]),            // 3, once
            scope_schema_url: Some("u"),     // 1, once
            resource_schema_url: Some("vv"), // 2, once
            ..SpanRecord::default()
        };
        // Per row: 10 + 5 + 35 and seven 4-byte keys. Once: 16 of origin.
        b.append(&r);
        assert_eq!(b.value_bytes(), 50 + 28 + 16);
        assert!(!b.is_full());
        b.append(&r);
        assert_eq!(b.value_bytes(), 2 * (50 + 28) + 16);
        assert!(b.is_full());
        assert_eq!(b.rows(), 2);
    }

    #[test]
    fn full_when_one_string_column_reaches_its_limit() {
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 1_000,
            max_bytes: usize::MAX,
        });
        b.string_column_limit = 100;
        let r = SpanRecord {
            name: "0123456789",
            // Spread over several columns: the total passes 100 but no
            // single column does, so the builder is not full.
            session_id: Some("abcdefghij"),
            user_id: Some("abcdefghij"),
            ..SpanRecord::default()
        };
        for _ in 0..9 {
            b.append(&r);
        }
        assert!(b.value_bytes() > 100 && !b.is_full());
        b.append(&r);
        assert!(b.is_full());
        b.finish();
        assert!(!b.is_full());
    }

    #[test]
    fn full_when_an_origin_dictionary_reaches_the_string_limit() {
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 1_000,
            max_bytes: usize::MAX,
        });
        b.string_column_limit = 100;
        let url = "u".repeat(100);
        b.intern_origin(&Origin {
            resource_schema_url: Some(&url),
            ..Origin::default()
        });
        assert!(b.is_full());
    }

    #[test]
    fn idle_builder_reserves_little() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let reserved = b.finish().get_array_memory_size();
        assert!(reserved < 64 * 1024, "{reserved} bytes reserved");
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

    fn span(n: u8) -> SpanRecord<'static> {
        SpanRecord {
            trace_id: TraceId([n; 16]),
            span_id: SpanId([n; 8]),
            ..SpanRecord::default()
        }
    }

    fn dictionary_len(batch: &RecordBatch, column: &str) -> usize {
        let c = batch.column_by_name(column).unwrap();
        let d = c.as_any_dictionary();
        d.values().len()
    }

    #[test]
    fn a_shared_origin_is_stored_once() {
        let resource = vec![9u8; 10_000];
        let origin = Origin {
            resource: Some(&resource),
            service_name: Some("svc"),
            scope_name: Some("lib"),
            ..Origin::default()
        };
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let keys = b.intern_origin(&origin);
        // Interning the same value again gives the same keys.
        assert_eq!(b.intern_origin(&origin), keys);
        for n in 0..1000 {
            b.append_span(&span(n as u8), &keys);
        }
        assert!(b.value_bytes() < 10_100 + 1000 * 28 + 1000);
        let batch = b.finish();
        assert_eq!(dictionary_len(&batch, "resource"), 1);
        assert_eq!(dictionary_len(&batch, "service_name"), 1);
        assert_eq!(dictionary_len(&batch, "scope"), 0);
        // Copied into every row this would be 10 MB.
        assert!(batch.get_array_memory_size() < 1_000_000);
        let cols = SpanColumns::try_new(&batch).unwrap();
        for row in [0, 500, 999] {
            assert_eq!(Origin::of(&cols.record(row)), origin);
            assert_eq!(cols.origin(row), origin);
        }
    }

    #[test]
    fn distinct_values_get_distinct_keys_and_equal_ones_share() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let o = |name: &'static str| Origin {
            scope_name: Some(name),
            scope_version: Some(name),
            ..Origin::default()
        };
        let (a, c) = (b.intern_origin(&o("a")), b.intern_origin(&o("c")));
        assert_ne!(a, c);
        let a_again = b.intern_origin(&o("a"));
        assert_eq!(a, a_again);
        // The empty value and the absent value are different.
        let empty = b.intern_origin(&Origin {
            scope_name: Some(""),
            ..Origin::default()
        });
        let absent = b.intern_origin(&Origin::default());
        assert_ne!(empty, absent);
        for (n, k) in [a, c, empty, absent].iter().enumerate() {
            b.append_span(&span(n as u8), k);
        }
        let batch = b.finish();
        let cols = SpanColumns::try_new(&batch).unwrap();
        assert_eq!(cols.record(0).scope_name, Some("a"));
        assert_eq!(cols.record(1).scope_version, Some("c"));
        assert_eq!(cols.record(2).scope_name, Some(""));
        assert_eq!(cols.record(2).scope_version, None);
        assert_eq!(cols.record(3).scope_name, None);
        assert_eq!(dictionary_len(&batch, "scope_name"), 3);
        assert_eq!(batch.column_by_name("scope_name").unwrap().null_count(), 1);
    }

    #[test]
    fn many_values_with_colliding_hashes_are_told_apart() {
        // Enough values that some share a hash bucket whatever the hash
        // function; lookups must compare content, not just hashes.
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let names: Vec<String> = (0..2000).map(|i| format!("name-{i}")).collect();
        let keys: Vec<OriginKeys> = names
            .iter()
            .map(|n| {
                b.intern_origin(&Origin {
                    scope_name: Some(n),
                    ..Origin::default()
                })
            })
            .collect();
        for (i, k) in keys.iter().enumerate() {
            let again = b.intern_origin(&Origin {
                scope_name: Some(&names[i]),
                ..Origin::default()
            });
            assert_eq!(*k, again);
        }
        assert_eq!(b.origin[slot::SCOPE_NAME].dict.ends.len(), 2000);
    }

    #[test]
    fn resource_level_and_scope_level_keys_combine() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let res = b.intern_origin(&Origin {
            resource: Some(b"r"),
            service_name: Some("svc"),
            resource_schema_url: Some("ru"),
            ..Origin::default()
        });
        let scope = b.intern_origin(&Origin {
            scope: Some(b"s"),
            scope_name: Some("n"),
            scope_version: Some("v"),
            scope_schema_url: Some("su"),
            ..Origin::default()
        });
        b.append_span(&span(1), &OriginKeys::combine(&res, &scope));
        let batch = b.finish();
        let cols = SpanColumns::try_new(&batch).unwrap();
        let want = Origin {
            resource: Some(b"r"),
            service_name: Some("svc"),
            resource_schema_url: Some("ru"),
            scope: Some(b"s"),
            scope_name: Some("n"),
            scope_version: Some("v"),
            scope_schema_url: Some("su"),
        };
        assert_eq!(cols.origin(0), want);
    }

    #[test]
    #[should_panic(expected = "already finished")]
    fn keys_from_before_finish_are_refused() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let keys = b.intern_origin(&Origin {
            service_name: Some("x"),
            ..Origin::default()
        });
        b.append_span(&span(1), &keys);
        b.finish();
        assert!(!b.is_current(&keys));
        b.append_span(&span(2), &keys);
    }

    #[test]
    #[should_panic(expected = "already finished")]
    fn keys_from_another_builder_are_refused() {
        let mut one = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let mut two = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let keys = one.intern_origin(&Origin::default());
        two.append_span(&span(1), &keys);
    }

    #[test]
    fn append_ignores_origin_fields_only_in_append_span() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let keys = b.intern_origin(&Origin {
            service_name: Some("from keys"),
            ..Origin::default()
        });
        let r = SpanRecord {
            service_name: Some("from record"),
            ..span(1)
        };
        b.append_span(&r, &keys);
        b.append(&r);
        let batch = b.finish();
        let cols = SpanColumns::try_new(&batch).unwrap();
        assert_eq!(cols.record(0).service_name, Some("from keys"));
        assert_eq!(cols.record(1).service_name, Some("from record"));
    }

    /// A batch whose origin dictionaries are numbered differently from what
    /// a plain copy would give, and which has values nobody uses.
    fn scrambled_source() -> RecordBatch {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let origins = [("a", b"ra"), ("b", b"rb"), ("c", b"rc")];
        let keys: Vec<OriginKeys> = origins
            .iter()
            .map(|(name, res)| {
                b.intern_origin(&Origin {
                    service_name: Some(name),
                    resource: Some(&res[..]),
                    ..Origin::default()
                })
            })
            .collect();
        let none = b.intern_origin(&Origin::default());
        // Rows use the origins in the order c, a, c, none, b, a.
        for (n, k) in [keys[2], keys[0], keys[2], none, keys[1], keys[0]]
            .iter()
            .enumerate()
        {
            b.append_span(&span(n as u8), k);
        }
        b.finish()
    }

    #[test]
    fn append_from_copies_rows_across_different_dictionaries() {
        let one = scrambled_source();
        // A second source in which the same values have other keys.
        let mut sb = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        for name in ["c", "z", "a"] {
            sb.append(&SpanRecord {
                service_name: Some(name),
                resource: Some(match name {
                    "a" => b"ra",
                    "c" => b"rc",
                    _ => b"rz",
                }),
                ..span(7)
            });
        }
        let two = sb.finish();
        let (c1, c2) = (
            SpanColumns::try_new(&one).unwrap(),
            SpanColumns::try_new(&two).unwrap(),
        );

        let mut out = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let mut want = Vec::new();
        for (source, cols) in [(0usize, &c1), (1, &c2)] {
            for row in (0..cols.len()).rev() {
                out.append_from(cols, source, row);
                want.push(cols.record(row));
            }
        }
        let batch = out.finish();
        let got = SpanColumns::try_new(&batch).unwrap();
        for (i, w) in want.iter().enumerate() {
            assert_eq!(&got.record(i), w, "row {i}");
        }
        // a, b, c, none from the first source and z from the second.
        assert_eq!(dictionary_len(&batch, "service_name"), 4);
        assert_eq!(dictionary_len(&batch, "resource"), 4);
    }

    #[test]
    fn append_from_hashes_each_distinct_origin_once_per_output_batch() {
        let source = scrambled_source();
        let cols = SpanColumns::try_new(&source).unwrap();
        let mut out = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        for row in 0..cols.len() {
            out.append_from(&cols, 3, row);
        }
        // Rows use four distinct origin combinations (c, a, none, b).
        assert_eq!(out.copied.len(), 4);
        let first = out.finish();
        assert!(out.copied.is_empty());
        // After finish the same source is interned again for the new batch.
        for row in 0..cols.len() {
            out.append_from(&cols, 3, row);
        }
        let second = out.finish();
        assert_eq!(first, second);
    }
}
