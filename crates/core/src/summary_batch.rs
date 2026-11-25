//! Arrow batches of [`TraceSummary`] values: a builder to write them and a
//! typed reader to read them back, mirroring the span table's
//! [`SpanBatchBuilder`](crate::SpanBatchBuilder) and
//! [`SpanColumns`](crate::SpanColumns).

use crate::{Error, Result, TraceId, TraceSummary, summary_schema};
use arrow_array::builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, ListArray, RecordBatch,
    StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

/// Accumulates [`TraceSummary`] values into a batch with
/// [`summary_schema`].
pub struct SummaryBatchBuilder {
    rows: usize,
    trace_id: FixedSizeBinaryBuilder,
    start_ns: Int64Builder,
    end_ns: Int64Builder,
    root_service: StringBuilder,
    root_name: StringBuilder,
    span_count: UInt32Builder,
    error: BooleanBuilder,
    input_tokens: Int64Builder,
    output_tokens: Int64Builder,
    models: ListBuilder<StringBuilder>,
    session_id: StringBuilder,
    user_id: StringBuilder,
}

impl Default for SummaryBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SummaryBatchBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            rows: 0,
            trace_id: FixedSizeBinaryBuilder::new(TraceId::LEN as i32),
            start_ns: Int64Builder::new(),
            end_ns: Int64Builder::new(),
            root_service: StringBuilder::new(),
            root_name: StringBuilder::new(),
            span_count: UInt32Builder::new(),
            error: BooleanBuilder::new(),
            input_tokens: Int64Builder::new(),
            output_tokens: Int64Builder::new(),
            // The list's item field must equal the schema's, or the batch
            // would be rejected when it is assembled.
            models: ListBuilder::new(StringBuilder::new())
                .with_field(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            session_id: StringBuilder::new(),
            user_id: StringBuilder::new(),
        }
    }

    /// Appends one trace summary.
    pub fn append(&mut self, s: &TraceSummary) {
        self.trace_id
            .append_value(s.trace_id.0)
            .expect("id length is fixed by its type");
        self.start_ns.append_value(s.start_ns);
        self.end_ns.append_value(s.end_ns);
        self.root_service.append_option(s.root_service.as_deref());
        self.root_name.append_option(s.root_name.as_deref());
        self.span_count.append_value(s.span_count);
        self.error.append_value(s.error);
        self.input_tokens.append_value(s.input_tokens);
        self.output_tokens.append_value(s.output_tokens);
        for m in &s.models {
            self.models.values().append_value(m);
        }
        self.models.append(true);
        self.session_id.append_option(s.session_id.as_deref());
        self.user_id.append_option(s.user_id.as_deref());
        self.rows += 1;
    }

    /// Number of summaries appended since the last [`finish`](Self::finish).
    pub fn len(&self) -> usize {
        self.rows
    }

    /// True if nothing has been appended.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Returns the summaries appended so far as a batch with
    /// [`summary_schema`] and resets the builder to empty.
    pub fn finish(&mut self) -> RecordBatch {
        self.rows = 0;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.trace_id.finish()),
            Arc::new(self.start_ns.finish()),
            Arc::new(self.end_ns.finish()),
            Arc::new(self.root_service.finish()),
            Arc::new(self.root_name.finish()),
            Arc::new(self.span_count.finish()),
            Arc::new(self.error.finish()),
            Arc::new(self.input_tokens.finish()),
            Arc::new(self.output_tokens.finish()),
            Arc::new(self.models.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.user_id.finish()),
        ];
        // The column order above is the schema order.
        RecordBatch::try_new(summary_schema(), columns)
            .expect("builders produce the summary schema")
    }
}

/// Typed, checked access to the columns of a batch with [`summary_schema`].
///
/// [`try_new`](Self::try_new) verifies the batch once, so reading a row
/// afterwards cannot fail.
#[derive(Debug, Clone, Copy)]
pub struct SummaryColumns<'a> {
    batch: &'a RecordBatch,
    trace_id: &'a FixedSizeBinaryArray,
    start_ns: &'a Int64Array,
    end_ns: &'a Int64Array,
    root_service: &'a StringArray,
    root_name: &'a StringArray,
    span_count: &'a UInt32Array,
    error: &'a BooleanArray,
    input_tokens: &'a Int64Array,
    output_tokens: &'a Int64Array,
    models: &'a ListArray,
    session_id: &'a StringArray,
    user_id: &'a StringArray,
}

fn string(a: &StringArray, row: usize) -> Option<String> {
    a.is_valid(row).then(|| a.value(row).to_owned())
}

impl<'a> SummaryColumns<'a> {
    /// Checks that `batch` has the summary schema and wraps it.
    ///
    /// The columns must appear in schema order with the schema's names and
    /// data types, and a column the schema declares non-nullable must hold no
    /// nulls. As with the span table, the batch's own nullability flags are
    /// not checked, because files read back from storage often mark every
    /// column nullable.
    ///
    /// Returns [`Error::InvalidArgument`] naming the first offending column.
    pub fn try_new(batch: &'a RecordBatch) -> Result<Self> {
        let schema = summary_schema();
        if batch.num_columns() != schema.fields().len() {
            return Err(Error::invalid_argument(format!(
                "summary batch has {} columns, expected {}",
                batch.num_columns(),
                schema.fields().len()
            )));
        }
        for (i, want) in schema.fields().iter().enumerate() {
            let have = batch.schema_ref().field(i);
            if have.name() != want.name() {
                return Err(Error::invalid_argument(format!(
                    "summary batch column {i} is `{}`, expected `{}`",
                    have.name(),
                    want.name()
                )));
            }
            let array = batch.column(i);
            if array.data_type() != want.data_type() {
                return Err(Error::invalid_argument(format!(
                    "summary batch column `{}` has type {}, expected {}",
                    want.name(),
                    array.data_type(),
                    want.data_type()
                )));
            }
            if !want.is_nullable() && array.null_count() > 0 {
                return Err(Error::invalid_argument(format!(
                    "summary batch column `{}` must not contain nulls",
                    want.name()
                )));
            }
        }

        // Types were verified above, so the downcasts cannot fail.
        fn get<T: 'static>(batch: &RecordBatch, i: usize) -> Result<&T> {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| Error::invalid_argument(format!("summary batch column {i}")))
        }
        Ok(Self {
            batch,
            trace_id: get(batch, 0)?,
            start_ns: get(batch, 1)?,
            end_ns: get(batch, 2)?,
            root_service: get(batch, 3)?,
            root_name: get(batch, 4)?,
            span_count: get(batch, 5)?,
            error: get(batch, 6)?,
            input_tokens: get(batch, 7)?,
            output_tokens: get(batch, 8)?,
            models: get(batch, 9)?,
            session_id: get(batch, 10)?,
            user_id: get(batch, 11)?,
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

    /// Reads one row as an owned [`TraceSummary`].
    ///
    /// # Panics
    ///
    /// Panics if `row >= self.len()`.
    pub fn summary(&self, row: usize) -> TraceSummary {
        let models = self.models.value(row);
        let models = models
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("list items are checked to be strings");
        TraceSummary {
            trace_id: TraceId::from_slice(self.trace_id.value(row))
                .expect("id column width is checked by its data type"),
            start_ns: self.start_ns.value(row),
            end_ns: self.end_ns.value(row),
            root_service: string(self.root_service, row),
            root_name: string(self.root_name, row),
            span_count: self.span_count.value(row),
            error: self.error.value(row),
            input_tokens: self.input_tokens.value(row),
            output_tokens: self.output_tokens.value(row),
            models: models.iter().flatten().map(str::to_owned).collect(),
            session_id: string(self.session_id, row),
            user_id: string(self.user_id, row),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::Schema;

    fn sample(n: u8) -> TraceSummary {
        TraceSummary {
            trace_id: TraceId([n; 16]),
            start_ns: -(n as i64),
            end_ns: i64::MAX - n as i64,
            root_service: n.is_multiple_of(2).then(|| format!("svc-é-{n}")),
            root_name: (!n.is_multiple_of(3)).then(|| "root".to_string()),
            span_count: n as u32 + 1,
            error: n % 2 == 1,
            input_tokens: 10 * n as i64,
            output_tokens: 3,
            models: (0..n % 4).map(|i| format!("model-{i}")).collect(),
            session_id: n.is_multiple_of(5).then(|| "s".to_string()),
            user_id: None,
        }
    }

    #[test]
    fn round_trip_every_field() {
        let all: Vec<_> = (0..12).map(sample).collect();
        let mut b = SummaryBatchBuilder::new();
        assert!(b.is_empty());
        for s in &all {
            b.append(s);
        }
        assert_eq!(b.len(), 12);
        let batch = b.finish();
        assert!(b.is_empty());
        assert_eq!(batch.schema(), summary_schema());

        let cols = SummaryColumns::try_new(&batch).unwrap();
        assert_eq!(cols.len(), 12);
        let back: Vec<_> = (0..cols.len()).map(|i| cols.summary(i)).collect();
        assert_eq!(back, all);
    }

    #[test]
    fn builder_is_reusable_and_empty_batch_is_valid() {
        let mut b = SummaryBatchBuilder::default();
        let empty = b.finish();
        assert!(SummaryColumns::try_new(&empty).unwrap().is_empty());
        b.append(&sample(1));
        assert_eq!(b.finish().num_rows(), 1);
        b.append(&sample(2));
        let batch = b.finish();
        let cols = SummaryColumns::try_new(&batch).unwrap();
        assert_eq!(cols.summary(0), sample(2));
        assert_eq!(cols.batch().num_rows(), 1);
    }

    #[test]
    fn rejects_wrong_shapes() {
        let mut b = SummaryBatchBuilder::new();
        b.append(&sample(1));
        let batch = b.finish();

        let fewer = batch.project(&[0, 1]).unwrap();
        assert!(matches!(
            SummaryColumns::try_new(&fewer),
            Err(Error::InvalidArgument(_))
        ));

        let swapped = batch
            .project(&[1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
            .unwrap();
        let e = SummaryColumns::try_new(&swapped).unwrap_err();
        assert!(e.to_string().contains("start_ns"), "{e}");

        let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
        cols[5] = Arc::new(Int32Array::from(vec![1]));
        let fields: Vec<Field> = summary_schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == 5 {
                    Field::new("span_count", DataType::Int32, false)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let wrong = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();
        let e = SummaryColumns::try_new(&wrong).unwrap_err();
        assert!(e.to_string().contains("span_count"), "{e}");
    }

    #[test]
    fn rejects_nulls_in_required_columns() {
        let mut b = SummaryBatchBuilder::new();
        b.append(&sample(1));
        let batch = b.finish();
        let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
        cols[1] = Arc::new(Int64Array::from(vec![None]));
        let schema = Arc::new(Schema::new(
            summary_schema()
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let nulls = RecordBatch::try_new(schema, cols).unwrap();
        let e = SummaryColumns::try_new(&nulls).unwrap_err();
        assert!(e.to_string().contains("start_ns"), "{e}");
    }
}
