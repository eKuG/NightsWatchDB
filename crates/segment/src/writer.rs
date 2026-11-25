//! Writing one part.

use crate::index::{self, INDEX_FILE, TraceEntry};
use crate::{SPANS_FILE, SUMMARY_FILE};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use nwdb_core::{
    Error, PartMeta, Result, SpanColumns, SummaryBatchBuilder, SummaryBuilder, TraceId,
    TraceSummary, part_dir_name, span_schema, summary_schema,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Version of the part's file formats, recorded in [`PartMeta`].
pub const FORMAT_VERSION: u32 = 1;

const ZSTD_LEVEL: i32 = 3;
/// Size at which a data page is closed. Parquet's default, stated here
/// because the read path relies on pages being small enough to skip.
const DATA_PAGE_BYTES: usize = 1 << 20;
/// Data pages also close at this many rows, so a lookup of one trace does not
/// have to decode a page of tens of thousands of unrelated small rows.
const DATA_PAGE_ROWS: usize = 4_096;
/// The Parquet writer checks its page limits between groups of this many
/// values. It is small so that, together with the writer's own byte-aware
/// splitting of large values, one 1 MiB value never drags hundreds of others
/// into the same page.
const WRITE_BATCH_VALUES: usize = 256;
/// Rows per batch handed to the summary writer.
const SUMMARY_BATCH_ROWS: usize = 8_192;
const IO_BUFFER: usize = 1 << 20;

/// Tuning of how a part is written. The defaults are the production values;
/// smaller ones exist so tests can produce many row groups cheaply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterOptions {
    /// A span row group is closed at this many rows. Default 16,384.
    pub row_group_rows: usize,
    /// A span row group is closed once this many value bytes (the lengths of
    /// all strings and raw columns, as in
    /// [`BatchLimits`](nwdb_core::BatchLimits)) have been written to it. The
    /// row that crosses the bound stays in the group, so a group can exceed
    /// it by one row. Default 64 MiB.
    pub row_group_bytes: usize,
    /// A summary row group is closed at this many rows. Default 16,384.
    pub summary_row_group_rows: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            row_group_rows: 16_384,
            row_group_bytes: 64 * 1024 * 1024,
            summary_row_group_rows: 16_384,
        }
    }
}

impl WriterOptions {
    fn validate(&self) -> Result<()> {
        if self.row_group_rows == 0 || self.row_group_bytes == 0 || self.summary_row_group_rows == 0
        {
            return Err(Error::invalid_argument(
                "writer options must all be greater than zero",
            ));
        }
        Ok(())
    }
}

pub(crate) fn parquet_props(row_group_rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(ZSTD_LEVEL).expect("level is within zstd's range"),
        ))
        // Page-level statistics also produce the column index.
        .set_statistics_enabled(EnabledStatistics::Page)
        // The offset index is on by default; stated so a change of default
        // cannot silently disable page skipping.
        .set_offset_index_disabled(false)
        .set_bloom_filter_enabled(false)
        .set_max_row_group_row_count(Some(row_group_rows))
        .set_data_page_size_limit(DATA_PAGE_BYTES)
        .set_dictionary_page_size_limit(DATA_PAGE_BYTES)
        .set_data_page_row_count_limit(DATA_PAGE_ROWS)
        .set_write_batch_size(WRITE_BATCH_VALUES)
        .build()
}

fn pq(e: parquet::errors::ParquetError) -> Error {
    Error::other(e)
}

/// A file shared between the Parquet writer, which owns it for writing, and
/// the part writer, which syncs it after the Parquet writer has finished.
struct SharedFile(Arc<File>);

impl Write for SharedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (&*self.0).write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        (&*self.0).flush()
    }
}

type FileWriter = ArrowWriter<BufWriter<SharedFile>>;

fn new_parquet_writer(
    path: &Path,
    schema: SchemaRef,
    rows: usize,
) -> Result<(FileWriter, Arc<File>)> {
    let file = Arc::new(File::create_new(path)?);
    let sink = BufWriter::with_capacity(IO_BUFFER, SharedFile(file.clone()));
    let writer = ArrowWriter::try_new(sink, schema, Some(parquet_props(rows))).map_err(pq)?;
    Ok((writer, file))
}

fn group_is_full(options: &WriterOptions, rows: usize, bytes: u64) -> bool {
    rows >= options.row_group_rows || bytes >= options.row_group_bytes as u64
}

/// The trace whose spans are currently arriving.
struct Current {
    trace_id: TraceId,
    first_row: u64,
    rows: u64,
}

/// Writes one immutable part: `spans.parquet`, `traces.idx` and
/// `summary.parquet` in a directory that appears under its final name only
/// when [`finish`](Self::finish) has made all three durable.
///
/// Memory use while writing is the Parquet writer's row group (at most
/// [`WriterOptions::row_group_bytes`] of values, plus one row) plus one
/// [`TraceEntry`] and one [`TraceSummary`] per trace so far. It does not grow
/// with the number of spans.
///
/// Dropping a writer that was not finished removes the temporary directory.
pub struct PartWriter {
    tmp_dir: PathBuf,
    final_dir: PathBuf,
    part_id: u64,
    window_start_ns: i64,
    window_ns: i64,
    options: WriterOptions,

    spans: Option<FileWriter>,
    spans_file: Arc<File>,
    /// Rows and value bytes in the span row group being filled.
    group_rows: usize,
    group_bytes: u64,
    /// Rows accepted so far.
    rows: u64,

    entries: Vec<TraceEntry>,
    /// Ids of traces that have ended, to reject a trace that comes back.
    finished: HashSet<TraceId>,
    current: Option<Current>,
    summarizer: SummaryBuilder,
    summaries: Vec<TraceSummary>,

    /// Set when a failure left the files in an unknown state.
    broken: bool,
    committed: bool,
}

impl PartWriter {
    /// Starts a part in `<parts_dir>/<part_dir_name(part_id)>.tmp`.
    ///
    /// `window_start_ns` and `window_ns` describe the time window the part
    /// covers and are recorded in the returned [`PartMeta`]. Creates
    /// `parts_dir` if it does not exist.
    ///
    /// Fails with [`Error::InvalidArgument`] if the final or the temporary
    /// directory already exists, or if `options` has a zero bound.
    pub fn create(
        parts_dir: impl AsRef<Path>,
        part_id: u64,
        window_start_ns: i64,
        window_ns: i64,
        options: WriterOptions,
    ) -> Result<Self> {
        options.validate()?;
        let parts_dir = parts_dir.as_ref();
        let name = part_dir_name(part_id);
        let final_dir = parts_dir.join(&name);
        let tmp_dir = parts_dir.join(format!("{name}.tmp"));
        for dir in [&final_dir, &tmp_dir] {
            if dir.exists() {
                return Err(Error::invalid_argument(format!(
                    "part directory {} already exists",
                    dir.display()
                )));
            }
        }
        fs::create_dir_all(parts_dir)?;
        fs::create_dir(&tmp_dir)?;

        let (spans, spans_file) = match new_parquet_writer(
            &tmp_dir.join(SPANS_FILE),
            span_schema(),
            options.row_group_rows,
        ) {
            Ok(w) => w,
            Err(e) => {
                let _ = fs::remove_dir_all(&tmp_dir);
                return Err(e);
            }
        };
        Ok(Self {
            tmp_dir,
            final_dir,
            part_id,
            window_start_ns,
            window_ns,
            options,
            spans: Some(spans),
            spans_file,
            group_rows: 0,
            group_bytes: 0,
            rows: 0,
            entries: Vec::new(),
            finished: HashSet::new(),
            current: None,
            summarizer: SummaryBuilder::new(),
            summaries: Vec::new(),
            broken: false,
            committed: false,
        })
    }

    /// Appends a batch with the span schema.
    ///
    /// All spans of one trace must arrive contiguously, though a trace may
    /// continue into the next batch, and a trace that has ended must not
    /// appear again. Spans are stored in the order given. A batch that breaks
    /// these rules, or has the wrong schema, is rejected with
    /// [`Error::InvalidArgument`] before any of it is written, and the writer
    /// stays usable.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.check_usable()?;
        let columns = SpanColumns::try_new(batch)?;
        if columns.is_empty() {
            return Ok(());
        }
        let runs = self.plan_runs(batch)?;

        // Nothing below can fail except the Parquet write, after which the
        // writer is marked broken.
        for (trace_id, range) in &runs {
            if self
                .current
                .as_ref()
                .is_some_and(|c| c.trace_id != *trace_id)
            {
                self.end_current_trace();
            }
            let first_row = self.rows + range.start as u64;
            let current = self.current.get_or_insert(Current {
                trace_id: *trace_id,
                first_row,
                rows: 0,
            });
            current.rows += range.len() as u64;
            for row in range.clone() {
                self.summarizer
                    .push(&columns.record(row))
                    .expect("the run holds one trace");
            }
        }
        self.rows += batch.num_rows() as u64;

        let written = self.write_rows(batch);
        if written.is_err() {
            self.broken = true;
        }
        written
    }

    /// Splits a batch into runs of equal trace id and checks the contiguity
    /// rules without changing any state.
    fn plan_runs(&self, batch: &RecordBatch) -> Result<Vec<(TraceId, Range<usize>)>> {
        let ids = batch.column(0).as_fixed_size_binary();
        let mut runs: Vec<(TraceId, Range<usize>)> = Vec::new();
        for row in 0..ids.len() {
            let id = TraceId::from_slice(ids.value(row)).expect("column width is checked");
            match runs.last_mut() {
                Some((last, range)) if *last == id => range.end = row + 1,
                _ => runs.push((id, row..row + 1)),
            }
        }

        let mut ended_in_batch: HashSet<TraceId> = HashSet::new();
        let mut open = self.current.as_ref().map(|c| (c.trace_id, c.rows));
        for (id, range) in &runs {
            let before = match open {
                Some((open_id, rows)) if open_id == *id => rows,
                Some((open_id, _)) => {
                    ended_in_batch.insert(open_id);
                    0
                }
                None => 0,
            };
            if before == 0 && (self.finished.contains(id) || ended_in_batch.contains(id)) {
                return Err(Error::invalid_argument(format!(
                    "trace {id} appears again after other traces: the spans of a trace must be \
                     written contiguously"
                )));
            }
            let total = before + range.len() as u64;
            if total > u64::from(u32::MAX) {
                return Err(Error::invalid_argument(format!(
                    "trace {id} has more than {} spans",
                    u32::MAX
                )));
            }
            open = Some((*id, total));
        }
        Ok(runs)
    }

    /// Hands rows to the Parquet writer in pieces that close each row group
    /// at the row or byte bound.
    fn write_rows(&mut self, batch: &RecordBatch) -> Result<()> {
        let row_bytes = row_value_bytes(batch);
        let options = self.options;
        let spans = self.spans.as_mut().expect("writer is open");
        let mut start = 0;
        while start < row_bytes.len() {
            let mut end = start;
            // At least one row per piece, so a row larger than the byte
            // bound still makes progress and forms a group by itself.
            loop {
                self.group_bytes += row_bytes[end];
                self.group_rows += 1;
                end += 1;
                if end == row_bytes.len()
                    || group_is_full(&options, self.group_rows, self.group_bytes)
                {
                    break;
                }
            }
            spans.write(&batch.slice(start, end - start)).map_err(pq)?;
            if group_is_full(&options, self.group_rows, self.group_bytes) {
                spans.flush().map_err(pq)?;
                self.group_rows = 0;
                self.group_bytes = 0;
            }
            start = end;
        }
        Ok(())
    }

    fn end_current_trace(&mut self) {
        let Some(current) = self.current.take() else {
            return;
        };
        let summary = self
            .summarizer
            .finish_trace()
            .expect("a trace in progress has spans");
        self.entries.push(TraceEntry {
            trace_id: current.trace_id,
            first_row: current.first_row,
            row_count: u32::try_from(current.rows).expect("checked when the rows arrived"),
            start_ns: summary.start_ns,
            end_ns: summary.end_ns,
        });
        self.finished.insert(current.trace_id);
        self.summaries.push(summary);
    }

    fn check_usable(&self) -> Result<()> {
        if self.broken {
            return Err(Error::invalid_argument(
                "writer is unusable after an earlier write failed",
            ));
        }
        Ok(())
    }

    /// Writes the index and the summary, makes the part durable and moves it
    /// to its final directory.
    ///
    /// Fails with [`Error::InvalidArgument`] if no rows were written; nothing
    /// is left behind in that case or on any other failure.
    pub fn finish(mut self) -> Result<PartMeta> {
        self.check_usable()?;
        self.end_current_trace();
        if self.rows == 0 {
            return Err(Error::invalid_argument(
                "a part must contain at least one row",
            ));
        }

        let spans = self.spans.take().expect("writer is open");
        spans.close().map_err(pq)?;
        self.spans_file.sync_all()?;

        self.entries.sort_by_key(|e| e.trace_id);
        write_synced(
            &self.tmp_dir.join(INDEX_FILE),
            &index::encode(&self.entries),
        )?;
        self.write_summary()?;

        let mut files = BTreeMap::new();
        for name in [SPANS_FILE, INDEX_FILE, SUMMARY_FILE] {
            files.insert(
                name.to_string(),
                fs::metadata(self.tmp_dir.join(name))?.len(),
            );
        }
        sync_dir(&self.tmp_dir)?;
        fs::rename(&self.tmp_dir, &self.final_dir)?;
        self.committed = true;
        sync_dir(self.final_dir.parent().expect("part dir has a parent"))?;

        Ok(PartMeta {
            part_id: self.part_id,
            level: 0,
            window_start_ns: self.window_start_ns,
            window_ns: self.window_ns,
            rows: self.rows,
            traces: self.entries.len() as u64,
            min_start_ns: self
                .entries
                .iter()
                .map(|e| e.start_ns)
                .min()
                .expect("rows > 0"),
            max_end_ns: self
                .entries
                .iter()
                .map(|e| e.end_ns)
                .max()
                .expect("rows > 0"),
            files,
            created_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            format_version: FORMAT_VERSION,
        })
    }

    fn write_summary(&mut self) -> Result<()> {
        self.summaries.sort_by_key(|s| (s.start_ns, s.trace_id));
        let (mut writer, file) = new_parquet_writer(
            &self.tmp_dir.join(SUMMARY_FILE),
            summary_schema(),
            self.options.summary_row_group_rows,
        )?;
        let mut builder = SummaryBatchBuilder::new();
        for summary in &self.summaries {
            builder.append(summary);
            if builder.len() >= SUMMARY_BATCH_ROWS {
                writer.write(&builder.finish()).map_err(pq)?;
            }
        }
        if !builder.is_empty() {
            writer.write(&builder.finish()).map_err(pq)?;
        }
        writer.close().map_err(pq)?;
        file.sync_all()?;
        Ok(())
    }
}

impl Drop for PartWriter {
    fn drop(&mut self) {
        if !self.committed {
            // Best effort: the directory may already be gone.
            let _ = fs::remove_dir_all(&self.tmp_dir);
        }
    }
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create_new(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Per-row sum of the lengths of every string and binary value, the same
/// measure as [`BatchLimits`](nwdb_core::BatchLimits) uses.
fn row_value_bytes(batch: &RecordBatch) -> Vec<u64> {
    fn add<O: Copy + Into<i64>>(out: &mut [u64], offsets: &[O]) {
        for (sum, w) in out.iter_mut().zip(offsets.windows(2)) {
            *sum += (w[1].into() - w[0].into()) as u64;
        }
    }
    let mut out = vec![0u64; batch.num_rows()];
    for column in batch.columns() {
        match column.data_type() {
            DataType::Utf8 => add(&mut out, column.as_string::<i32>().value_offsets()),
            DataType::LargeBinary => add(&mut out, column.as_binary::<i64>().value_offsets()),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanId, SpanRecord};

    #[test]
    fn value_bytes_match_the_core_measure() {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        let mut expected = Vec::new();
        for i in 0..5usize {
            let attrs = vec![7u8; i * 100];
            let before = b.value_bytes();
            b.append(&SpanRecord {
                trace_id: TraceId([1; 16]),
                span_id: SpanId([i as u8; 8]),
                name: "n",
                service_name: (i % 2 == 0).then_some("service"),
                attrs: &attrs,
                events: (i % 3 == 0).then_some(&attrs[..]),
                ..SpanRecord::default()
            });
            expected.push((b.value_bytes() - before) as u64);
        }
        let batch = b.finish();
        assert_eq!(row_value_bytes(&batch), expected);
        // A slice must measure the same as the rows it covers.
        assert_eq!(row_value_bytes(&batch.slice(2, 2)), expected[2..4]);
    }
}
