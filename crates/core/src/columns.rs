use crate::origin::{ORIGIN_COLUMNS, slot};
use crate::{Error, Origin, Result, SpanId, SpanRecord, TraceId, span_schema};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, DictionaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, LargeBinaryArray,
    RecordBatch, StringArray, UInt32Array, UInt64Array,
};

/// A dictionary column: the keys of the rows and the array they point into.
/// [`SpanColumns::try_new`] has checked that every key is inside `values`.
struct Dict<'a, V> {
    keys: &'a Int32Array,
    values: &'a V,
}

// Derives would require `V: Clone`; the fields are references.
impl<V> Clone for Dict<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Dict<'_, V> {}

impl<'a> Dict<'a, StringArray> {
    fn get(&self, row: usize) -> Option<&'a str> {
        let k = self.key(row)?;
        self.values.is_valid(k).then(|| self.values.value(k))
    }
}

impl<'a> Dict<'a, LargeBinaryArray> {
    fn get(&self, row: usize) -> Option<&'a [u8]> {
        let k = self.key(row)?;
        self.values.is_valid(k).then(|| self.values.value(k))
    }
}

impl<V> Dict<'_, V> {
    fn key(&self, row: usize) -> Option<usize> {
        self.keys
            .is_valid(row)
            .then(|| self.keys.value(row) as usize)
    }
}

/// Typed, checked access to the columns of a batch with the span schema.
///
/// [`try_new`](Self::try_new) verifies the batch once, so reading a row
/// afterwards cannot fail. The origin columns are dictionary columns; a row
/// read from them is resolved through the batch's own dictionary.
#[derive(Clone, Copy)]
pub struct SpanColumns<'a> {
    batch: &'a RecordBatch,
    trace_id: &'a FixedSizeBinaryArray,
    span_id: &'a FixedSizeBinaryArray,
    parent_span_id: &'a FixedSizeBinaryArray,
    name: &'a StringArray,
    kind: &'a Int32Array,
    start_ns: &'a Int64Array,
    end_ns: &'a Int64Array,
    status_code: &'a Int32Array,
    status_message: &'a StringArray,
    trace_state: &'a StringArray,
    flags: &'a UInt32Array,
    service_name: Dict<'a, StringArray>,
    scope_name: Dict<'a, StringArray>,
    scope_version: Dict<'a, StringArray>,
    resource_schema_url: Dict<'a, StringArray>,
    scope_schema_url: Dict<'a, StringArray>,
    operation: &'a StringArray,
    provider: &'a StringArray,
    request_model: &'a StringArray,
    response_model: &'a StringArray,
    input_tokens: &'a Int64Array,
    output_tokens: &'a Int64Array,
    conversation_id: &'a StringArray,
    session_id: &'a StringArray,
    user_id: &'a StringArray,
    agent_name: &'a StringArray,
    tool_name: &'a StringArray,
    error_type: &'a StringArray,
    resource: Dict<'a, LargeBinaryArray>,
    scope: Dict<'a, LargeBinaryArray>,
    attrs: &'a LargeBinaryArray,
    events: &'a LargeBinaryArray,
    links: &'a LargeBinaryArray,
    dropped_attributes_count: &'a UInt32Array,
    dropped_events_count: &'a UInt32Array,
    dropped_links_count: &'a UInt32Array,
    arrival: &'a UInt64Array,
}

fn string(a: &StringArray, row: usize) -> Option<&str> {
    a.is_valid(row).then(|| a.value(row))
}

fn bytes(a: &LargeBinaryArray, row: usize) -> Option<&[u8]> {
    a.is_valid(row).then(|| a.value(row))
}

fn int(a: &Int64Array, row: usize) -> Option<i64> {
    a.is_valid(row).then(|| a.value(row))
}

impl<'a> SpanColumns<'a> {
    /// Checks that `batch` has the span schema and wraps it.
    ///
    /// The columns must appear in schema order with the schema's names and
    /// data types, and a column the schema declares non-nullable must hold no
    /// nulls. Whether the batch's own field metadata says nullable is not
    /// checked, because files read back from storage often mark every column
    /// nullable.
    ///
    /// Returns [`Error::InvalidArgument`] naming the first offending column.
    pub fn try_new(batch: &'a RecordBatch) -> Result<Self> {
        let schema = span_schema();
        if batch.num_columns() != schema.fields().len() {
            return Err(Error::invalid_argument(format!(
                "span batch has {} columns, expected {}",
                batch.num_columns(),
                schema.fields().len()
            )));
        }
        for (i, want) in schema.fields().iter().enumerate() {
            let have = batch.schema_ref().field(i);
            if have.name() != want.name() {
                return Err(Error::invalid_argument(format!(
                    "span batch column {i} is `{}`, expected `{}`",
                    have.name(),
                    want.name()
                )));
            }
            let array = batch.column(i);
            if array.data_type() != want.data_type() {
                return Err(Error::invalid_argument(format!(
                    "span batch column `{}` has type {}, expected {}",
                    want.name(),
                    array.data_type(),
                    want.data_type()
                )));
            }
            if !want.is_nullable() && array.null_count() > 0 {
                return Err(Error::invalid_argument(format!(
                    "span batch column `{}` must not contain nulls",
                    want.name()
                )));
            }
        }

        // Types were verified above, so the downcasts cannot fail.
        fn get<'b, T: 'static>(batch: &'b RecordBatch, name: &str) -> Result<&'b T> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<T>())
                .ok_or_else(|| Error::invalid_argument(format!("span batch column `{name}`")))
        }
        // The array types were verified above. A key outside its dictionary
        // would panic when a row is read, so it is refused here.
        fn dict<'b, V: 'static + Array>(batch: &'b RecordBatch, name: &str) -> Result<Dict<'b, V>> {
            let array: &DictionaryArray<Int32Type> = get(batch, name)?;
            let values =
                array.values().as_any().downcast_ref::<V>().ok_or_else(|| {
                    Error::invalid_argument(format!("span batch column `{name}`"))
                })?;
            let keys = array.keys();
            let inside = keys
                .iter()
                .flatten()
                .all(|k| usize::try_from(k).is_ok_and(|k| k < values.len()));
            if !inside {
                return Err(Error::invalid_argument(format!(
                    "span batch column `{name}` has a dictionary key outside its dictionary"
                )));
            }
            Ok(Dict { keys, values })
        }
        use crate::col;
        Ok(Self {
            batch,
            trace_id: get(batch, col::TRACE_ID)?,
            span_id: get(batch, col::SPAN_ID)?,
            parent_span_id: get(batch, col::PARENT_SPAN_ID)?,
            name: get(batch, col::NAME)?,
            kind: get(batch, col::KIND)?,
            start_ns: get(batch, col::START_NS)?,
            end_ns: get(batch, col::END_NS)?,
            status_code: get(batch, col::STATUS_CODE)?,
            status_message: get(batch, col::STATUS_MESSAGE)?,
            trace_state: get(batch, col::TRACE_STATE)?,
            flags: get(batch, col::FLAGS)?,
            service_name: dict(batch, col::SERVICE_NAME)?,
            scope_name: dict(batch, col::SCOPE_NAME)?,
            scope_version: dict(batch, col::SCOPE_VERSION)?,
            resource_schema_url: dict(batch, col::RESOURCE_SCHEMA_URL)?,
            scope_schema_url: dict(batch, col::SCOPE_SCHEMA_URL)?,
            operation: get(batch, col::OPERATION)?,
            provider: get(batch, col::PROVIDER)?,
            request_model: get(batch, col::REQUEST_MODEL)?,
            response_model: get(batch, col::RESPONSE_MODEL)?,
            input_tokens: get(batch, col::INPUT_TOKENS)?,
            output_tokens: get(batch, col::OUTPUT_TOKENS)?,
            conversation_id: get(batch, col::CONVERSATION_ID)?,
            session_id: get(batch, col::SESSION_ID)?,
            user_id: get(batch, col::USER_ID)?,
            agent_name: get(batch, col::AGENT_NAME)?,
            tool_name: get(batch, col::TOOL_NAME)?,
            error_type: get(batch, col::ERROR_TYPE)?,
            resource: dict(batch, col::RESOURCE)?,
            scope: dict(batch, col::SCOPE)?,
            attrs: get(batch, col::ATTRS)?,
            events: get(batch, col::EVENTS)?,
            links: get(batch, col::LINKS)?,
            dropped_attributes_count: get(batch, col::DROPPED_ATTRIBUTES_COUNT)?,
            dropped_events_count: get(batch, col::DROPPED_EVENTS_COUNT)?,
            dropped_links_count: get(batch, col::DROPPED_LINKS_COUNT)?,
            arrival: get(batch, col::ARRIVAL)?,
        })
    }

    /// The batch these columns read from.
    pub fn batch(&self) -> &'a RecordBatch {
        self.batch
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    /// True if the batch has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The dictionary key of each origin column at `row`, in [`Origin`]
    /// field order, `None` for a null. Keys of different batches are not
    /// comparable, and a batch that did not come from
    /// [`SpanBatchBuilder`](crate::SpanBatchBuilder) may hold one value under
    /// several keys.
    ///
    /// # Panics
    ///
    /// Panics if `row >= self.len()`.
    pub fn origin_keys(&self, row: usize) -> [Option<u32>; ORIGIN_COLUMNS] {
        let mut out = [None; ORIGIN_COLUMNS];
        let mut put = |i: usize, key: Option<usize>| out[i] = key.map(|k| k as u32);
        put(slot::RESOURCE, self.resource.key(row));
        put(slot::SERVICE_NAME, self.service_name.key(row));
        put(slot::RESOURCE_SCHEMA_URL, self.resource_schema_url.key(row));
        put(slot::SCOPE, self.scope.key(row));
        put(slot::SCOPE_NAME, self.scope_name.key(row));
        put(slot::SCOPE_VERSION, self.scope_version.key(row));
        put(slot::SCOPE_SCHEMA_URL, self.scope_schema_url.key(row));
        out
    }

    /// The origin values of one row, borrowed from the batch.
    ///
    /// # Panics
    ///
    /// Panics if `row >= self.len()`.
    pub fn origin(&self, row: usize) -> Origin<'a> {
        Origin {
            resource: self.resource.get(row),
            service_name: self.service_name.get(row),
            resource_schema_url: self.resource_schema_url.get(row),
            scope: self.scope.get(row),
            scope_name: self.scope_name.get(row),
            scope_version: self.scope_version.get(row),
            scope_schema_url: self.scope_schema_url.get(row),
        }
    }

    /// Reads one row, borrowing its values from the batch.
    ///
    /// # Panics
    ///
    /// Panics if `row >= self.len()`.
    pub fn record(&self, row: usize) -> SpanRecord<'a> {
        let id_len = "id column width is checked by its data type";
        SpanRecord {
            trace_id: TraceId::from_slice(self.trace_id.value(row)).expect(id_len),
            span_id: SpanId::from_slice(self.span_id.value(row)).expect(id_len),
            parent_span_id: self
                .parent_span_id
                .is_valid(row)
                .then(|| SpanId::from_slice(self.parent_span_id.value(row)).expect(id_len)),
            name: self.name.value(row),
            kind: self.kind.value(row),
            start_ns: self.start_ns.value(row),
            end_ns: self.end_ns.value(row),
            status_code: self.status_code.value(row),
            status_message: string(self.status_message, row),
            trace_state: string(self.trace_state, row),
            flags: self.flags.value(row),
            service_name: self.service_name.get(row),
            scope_name: self.scope_name.get(row),
            scope_version: self.scope_version.get(row),
            resource_schema_url: self.resource_schema_url.get(row),
            scope_schema_url: self.scope_schema_url.get(row),
            operation: string(self.operation, row),
            provider: string(self.provider, row),
            request_model: string(self.request_model, row),
            response_model: string(self.response_model, row),
            input_tokens: int(self.input_tokens, row),
            output_tokens: int(self.output_tokens, row),
            conversation_id: string(self.conversation_id, row),
            session_id: string(self.session_id, row),
            user_id: string(self.user_id, row),
            agent_name: string(self.agent_name, row),
            tool_name: string(self.tool_name, row),
            error_type: string(self.error_type, row),
            resource: self.resource.get(row),
            scope: self.scope.get(row),
            attrs: self.attrs.value(row),
            events: bytes(self.events, row),
            links: bytes(self.links, row),
            dropped_attributes_count: self.dropped_attributes_count.value(row),
            dropped_events_count: self.dropped_events_count.value(row),
            dropped_links_count: self.dropped_links_count.value(row),
            arrival: self.arrival.value(row),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchLimits, SpanBatchBuilder};
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn id(n: u8) -> (TraceId, SpanId) {
        (TraceId([n; 16]), SpanId([n; 8]))
    }

    fn full_record<'a>(attrs: &'a [u8]) -> SpanRecord<'a> {
        let (t, s) = id(1);
        SpanRecord {
            trace_id: t,
            span_id: s,
            parent_span_id: Some(SpanId([9; 8])),
            name: "chat gpt-5",
            kind: 3,
            start_ns: -5,
            end_ns: 1_000,
            status_code: 2,
            status_message: Some("bad gateway"),
            trace_state: Some("a=b"),
            flags: 0x100,
            service_name: Some("agent"),
            scope_name: Some("scope"),
            scope_version: Some("1.2.3"),
            resource_schema_url: Some("https://example.test/r/1"),
            scope_schema_url: Some("https://example.test/s/1"),
            operation: Some("chat"),
            provider: Some("openai"),
            request_model: Some("gpt-5"),
            response_model: Some("gpt-5-2026"),
            input_tokens: Some(12),
            output_tokens: Some(34),
            conversation_id: Some("c1"),
            session_id: Some("s1"),
            user_id: Some("u1"),
            agent_name: Some("planner"),
            tool_name: Some("search"),
            error_type: Some("timeout"),
            resource: Some(&[1, 2, 3]),
            scope: Some(&[7, 8]),
            attrs,
            events: Some(&[4, 5]),
            links: Some(&[]),
            dropped_attributes_count: 1,
            dropped_events_count: 2,
            dropped_links_count: 3,
            arrival: 77,
        }
    }

    #[test]
    fn round_trip_with_nulls_empty_and_large_values() {
        let big = vec![0xabu8; 1 << 20];
        let (t, s) = id(2);
        let nulls = SpanRecord {
            trace_id: t,
            span_id: s,
            name: "ünïcode \u{1F600} 名前",
            service_name: Some("héllo"),
            ..SpanRecord::default()
        };
        let records = [full_record(&big), nulls, full_record(&[])];

        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        for r in &records {
            b.append(r);
        }
        assert_eq!(b.rows(), 3);
        let batch = b.finish();
        assert!(b.is_empty());
        assert_eq!(b.value_bytes(), 0);
        assert_eq!(batch.schema(), span_schema());

        let cols = SpanColumns::try_new(&batch).unwrap();
        assert_eq!(cols.len(), 3);
        for (i, want) in records.iter().enumerate() {
            assert_eq!(&cols.record(i), want, "row {i}");
        }
        assert_eq!(cols.record(0).attrs.len(), 1 << 20);
        assert!(cols.record(1).parent_span_id.is_none());
        assert!(cols.record(1).resource.is_none());
        assert!(cols.record(1).scope.is_none());
        assert!(cols.record(1).scope_schema_url.is_none());
        assert_eq!(cols.record(1).attrs, b"");
        assert_eq!(cols.record(1).name, "ünïcode \u{1F600} 名前");

        // The builder is reusable after finish.
        b.append(&records[1]);
        assert_eq!(SpanColumns::try_new(&b.finish()).unwrap().len(), 1);
    }

    #[test]
    fn empty_batch_is_valid() {
        let batch = SpanBatchBuilder::new(BatchLimits::DEFAULT).finish();
        let cols = SpanColumns::try_new(&batch).unwrap();
        assert!(cols.is_empty());
    }

    fn valid_batch() -> RecordBatch {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        b.append(&full_record(&[]));
        b.finish()
    }

    fn err_text(batch: &RecordBatch) -> String {
        match SpanColumns::try_new(batch) {
            Err(Error::InvalidArgument(m)) => m,
            Err(e) => panic!("wrong error kind: {e:?}"),
            Ok(_) => panic!("batch was accepted"),
        }
    }

    #[test]
    fn rejects_wrong_column_count() {
        let b = valid_batch();
        let narrow = b.project(&[0, 1]).unwrap();
        assert!(err_text(&narrow).contains("columns"));
    }

    #[test]
    fn rejects_wrong_type_naming_the_column() {
        let b = valid_batch();
        let mut fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut cols: Vec<ArrayRef> = b.columns().to_vec();
        let i = 4; // kind
        fields[i] = Field::new("kind", DataType::Int64, false);
        cols[i] = Arc::new(Int64Array::from(vec![3i64]));
        let bad = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();
        let m = err_text(&bad);
        assert!(m.contains("`kind`"), "{m}");
    }

    #[test]
    fn rejects_wrong_name_naming_the_column() {
        let b = valid_batch();
        let mut fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[3] = Field::new("title", DataType::Utf8, false);
        let bad =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), b.columns().to_vec()).unwrap();
        let m = err_text(&bad);
        assert!(m.contains("`title`") && m.contains("`name`"), "{m}");
    }

    #[test]
    fn rejects_nulls_in_required_column() {
        let b = valid_batch();
        let fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        let mut cols: Vec<ArrayRef> = b.columns().to_vec();
        cols[4] = Arc::new(Int32Array::from(vec![None::<i32>]));
        let bad = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();
        assert!(err_text(&bad).contains("`kind`"));
    }

    #[test]
    fn accepts_all_nullable_fields_from_storage() {
        let b = valid_batch();
        let fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        let loose =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), b.columns().to_vec()).unwrap();
        assert!(SpanColumns::try_new(&loose).is_ok());
    }
}
