//! Writing one part.

use crate::index::{self, FileChecksum, FileChecksums, INDEX_FILE, TraceEntry};
use crate::{SPANS_FILE, SUMMARY_FILE};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use nwdb_core::{
    Error, PartMeta, Result, SpanColumns, SummaryBatchBuilder, SummaryBuilder, TraceId,
    TraceSummary, part_dir_name, span_schema, summary_schema,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Version of the part's file formats, recorded in [`PartMeta`].
/// Version 2 added the length and CRC-32C of both Parquet files to `traces.idx`.
pub const FORMAT_VERSION: u32 = 2;

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

/// Writer settings. `spans` selects the span table, whose five raw binary
/// columns get no statistics (nothing prunes by them) and, except `resource`
/// and `scope`, no dictionary: `attrs`, `events` and `links` are close to
/// unique per row, so a dictionary is wasted work, while `resource` and
/// `scope` repeat heavily.
fn parquet_props(row_group_rows: usize, spans: bool) -> WriterProperties {
    let mut builder = WriterProperties::builder()
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
        .set_write_batch_size(WRITE_BATCH_VALUES);
    if spans {
        for name in ["resource", "scope", "attrs", "events", "links"] {
            builder = builder
                .set_column_statistics_enabled(ColumnPath::from(name), EnabledStatistics::None);
        }
        for name in ["attrs", "events", "links"] {
            builder = builder.set_column_dictionary_enabled(ColumnPath::from(name), false);
        }
        for name in ["resource", "scope"] {
            builder = builder.set_column_dictionary_enabled(ColumnPath::from(name), true);
        }
    }
    builder.build()
}

/// Length and CRC-32C of everything written to a file so far.
#[derive(Default)]
struct Digest {
    crc: AtomicU32,
    len: AtomicU64,
}

impl Digest {
    // Only the thread that owns the writer updates it, so plain load and
    // store suffice.
    fn add(&self, bytes: &[u8]) {
        let crc = crc32c::crc32c_append(self.crc.load(Ordering::Relaxed), bytes);
        self.crc.store(crc, Ordering::Relaxed);
        self.len.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }

    fn get(&self) -> FileChecksum {
        FileChecksum {
            len: self.len.load(Ordering::Relaxed),
            crc32c: self.crc.load(Ordering::Relaxed),
        }
    }
}

fn pq(e: parquet::errors::ParquetError) -> Error {
    Error::other(e)
}

/// A file shared between the Parquet writer, which owns it for writing, and
/// the part writer, which syncs it after the Parquet writer has finished. It
/// digests every byte that reaches the file.
struct SharedFile {
    file: Arc<File>,
    digest: Arc<Digest>,
}

impl Write for SharedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = (&*self.file).write(buf)?;
        self.digest.add(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        (&*self.file).flush()
    }
}

type FileWriter = ArrowWriter<BufWriter<SharedFile>>;

fn new_parquet_writer(
    path: &Path,
    schema: SchemaRef,
    rows: usize,
    spans: bool,
) -> Result<(FileWriter, Arc<File>, Arc<Digest>)> {
    let file = Arc::new(File::create_new(path)?);
    let digest = Arc::new(Digest::default());
    let sink = BufWriter::with_capacity(
        IO_BUFFER,
        SharedFile {
            file: file.clone(),
            digest: digest.clone(),
        },
    );
    let writer =
        ArrowWriter::try_new(sink, schema, Some(parquet_props(rows, spans))).map_err(pq)?;
    Ok((writer, file, digest))
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
    spans_digest: Arc<Digest>,
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
    /// directory already exists, if `window_ns` is not positive, or if
    /// `options` has a zero bound.
    ///
    /// After any error from [`write`](Self::write) or
    /// [`finish`](Self::finish) other than [`Error::Fatal`], the writer is
    /// gone or unusable and nothing is left behind; the caller must still
    /// use a new part id for the next attempt. After [`Error::Fatal`] the
    /// part may or may not exist and be durable: do not reuse the id and
    /// restart.
    pub fn create(
        parts_dir: impl AsRef<Path>,
        part_id: u64,
        window_start_ns: i64,
        window_ns: i64,
        options: WriterOptions,
    ) -> Result<Self> {
        options.validate()?;
        if window_ns <= 0 {
            return Err(Error::invalid_argument(format!(
                "window_ns must be positive, got {window_ns}"
            )));
        }
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
        create_dir_all_synced(parts_dir)?;
        fs::create_dir(&tmp_dir)?;

        let (spans, spans_file, spans_digest) = match new_parquet_writer(
            &tmp_dir.join(SPANS_FILE),
            span_schema(),
            options.row_group_rows,
            true,
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
            spans_digest,
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

    /// Writes the summary and the index, makes the part durable and moves it
    /// to its final directory.
    ///
    /// Fails with [`Error::InvalidArgument`] if no rows were written. On any
    /// failure before the directory is renamed, nothing is left behind, but
    /// the caller must use a new part id for the next attempt. If a step
    /// after the rename fails (syncing the parts directory), the part may or
    /// may not be durable and the result is [`Error::Fatal`]: the caller must
    /// not reuse the id and should restart.
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

        let spans_sum = self.spans_digest.get();
        let summary_sum = self.write_summary()?;
        for (name, sum) in [(SPANS_FILE, spans_sum), (SUMMARY_FILE, summary_sum)] {
            let on_disk = fs::metadata(self.tmp_dir.join(name))?.len();
            if on_disk != sum.len {
                return Err(Error::other(format!(
                    "{name} has {on_disk} bytes on disk but {} were written",
                    sum.len
                )));
            }
        }

        self.entries.sort_by_key(|e| e.trace_id);
        let checksums = FileChecksums {
            spans: spans_sum,
            summary: summary_sum,
        };
        write_synced(
            &self.tmp_dir.join(INDEX_FILE),
            &index::encode(&self.entries, checksums),
        )?;

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
        // The rename has happened: from here on the part may exist, so a
        // failure must not look like one that left nothing behind.
        sync_after_rename(parent_of(&self.final_dir)).map_err(|e| {
            Error::fatal(format!(
                "part {} was renamed into place but syncing {} failed ({e}); it may or may not \
                 be durable: do not reuse the part id and restart",
                self.part_id,
                parent_of(&self.final_dir).display()
            ))
        })?;

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

    /// Writes `summary.parquet` and returns its length and checksum.
    fn write_summary(&mut self) -> Result<FileChecksum> {
        self.summaries.sort_by_key(|s| (s.start_ns, s.trace_id));
        let (mut writer, file, digest) = new_parquet_writer(
            &self.tmp_dir.join(SUMMARY_FILE),
            summary_schema(),
            self.options.summary_row_group_rows,
            false,
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
        Ok(digest.get())
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

#[cfg(test)]
thread_local! {
    /// Makes the next [`sync_after_rename`] on this thread fail.
    static FAIL_SYNC_AFTER_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn sync_after_rename(dir: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_SYNC_AFTER_RENAME.with(|f| f.replace(false)) {
        return Err(std::io::Error::other("injected failure").into());
    }
    sync_dir(dir)
}

/// The directory that holds `path`; `.` for a bare relative name.
fn parent_of(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

/// Creates `dir` and any missing parents, and syncs the parent of every
/// directory it created: a new directory is not durable until its parent has
/// been synced.
fn create_dir_all_synced(dir: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut p = dir;
    while !p.as_os_str().is_empty() && !p.exists() {
        missing.push(p);
        match p.parent() {
            Some(parent) => p = parent,
            None => break,
        }
    }
    fs::create_dir_all(dir)?;
    for created in missing {
        sync_dir(parent_of(created))?;
    }
    Ok(())
}

/// Bytes of the key a row holds for each dictionary column.
const KEY_BYTES: u64 = 4;

/// Total length of the values of a dictionary.
fn dictionary_value_bytes(values: &ArrayRef) -> u64 {
    match values.data_type() {
        DataType::Utf8 => values.as_string::<i32>().value_data().len() as u64,
        DataType::LargeBinary => values.as_binary::<i64>().value_data().len() as u64,
        _ => 0,
    }
}

/// Per-row sum of the lengths of every string and binary value, the same
/// measure as [`BatchLimits`](nwdb_core::BatchLimits) uses: a dictionary
/// column counts its 4-byte key in every row and its values once.
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
            // A row holds its key; the values are stored once, so they are
            // charged to the first row of the batch, once, instead of to
            // every row that uses them.
            DataType::Dictionary(_, _) => {
                let dict = column.as_any_dictionary();
                for sum in out.iter_mut() {
                    *sum += KEY_BYTES;
                }
                if let Some(first) = out.first_mut() {
                    *first += dictionary_value_bytes(dict.values());
                }
            }
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
    fn a_failure_after_the_rename_is_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PartWriter::create(tmp.path(), 5, 0, 1_000, WriterOptions::default()).unwrap();
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        b.append(&SpanRecord {
            trace_id: TraceId([1; 16]),
            span_id: SpanId([1; 8]),
            name: "n",
            ..SpanRecord::default()
        });
        w.write(&b.finish()).unwrap();
        FAIL_SYNC_AFTER_RENAME.with(|f| f.set(true));
        let err = w.finish().unwrap_err();
        assert!(
            matches!(&err, Error::Fatal(m) if m.contains("do not reuse")),
            "{err}"
        );
        // The part is in place (it may be durable), and its id is taken.
        let dir = tmp.path().join(part_dir_name(5));
        assert!(dir.is_dir());
        assert!(
            !tmp.path()
                .join(format!("{}.tmp", part_dir_name(5)))
                .exists()
        );
        let again = PartWriter::create(tmp.path(), 5, 0, 1_000, WriterOptions::default());
        assert!(matches!(again.err(), Some(Error::InvalidArgument(_))));
    }

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
        // A slice measures the rows it covers, and carries the dictionary
        // ("service", 7 bytes) on its first row, whether or not that row is
        // the one that introduced the value.
        assert_eq!(
            row_value_bytes(&batch.slice(2, 2)),
            [expected[2] + 7, expected[3]]
        );
    }
}
