//! Writing one part.

use crate::index::{self, FileChecksum, FileChecksums, INDEX_FILE, TraceEntry};
use crate::text::{
    DEFAULT_ROW_LIST_BUDGET_BYTES, DEFAULT_TEXT_INDEX_BUDGET_BYTES, TextFinished, TextWriter,
};
use crate::{PIECES_BLK, PIECES_IDX, SPANS_FILE, SUMMARY_FILE, TEXT_INDEX_FILE, TEXT_ROWS_FILE};
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use nwdb_core::{
    Error, PartMeta, PayloadSplitter, RawColumn, Result, SpanColumns, SummaryBatchBuilder,
    SummaryBuilder, TraceId, TraceSummary, col, part_dir_name, span_schema, summary_schema,
};
use nwdb_payload::{MAX_PIECE_BYTES, PieceObserver, PieceWriter, PieceWriterOptions, rope};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
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

/// Version of the part's file formats, recorded in [`PartMeta`] and in the
/// header of `traces.idx`. It must be raised on any change to the columns of
/// the Parquet files, their types or order, or the meaning of the raw
/// encodings in them. Version 2 added the length and CRC-32C of both Parquet
/// files to `traces.idx`; version 3 made the origin columns dictionary
/// columns; version 4 stores `attrs`, `events` and `links` as ropes and adds
/// the piece store, `pieces.blk` and `pieces.idx`, whose length and CRC-32C
/// are in `traces.idx` too; version 5 adds the CRC-32C of the rebuilt value to
/// every rope that has a reference; version 6 adds the length of the rebuilt
/// value to those ropes, and the text index and the piece-to-rows lists,
/// `text.idx` and `text.rows`, whose length and CRC-32C are in `traces.idx`
/// too; version 7 adds the `arrival` column to `spans.parquet`, adds the
/// CRC-32C of each group to the group index of `text.rows`, and records the
/// footer checksums of `text.idx` and `text.rows` in `traces.idx`.
pub const FORMAT_VERSION: u32 = 7;

/// The default for [`WriterOptions::dedup_window_bytes`]: 64 MiB.
pub const DEFAULT_DEDUP_WINDOW_BYTES: usize = 64 << 20;

/// The default for [`WriterOptions::piece_block_bytes`]: 1 MiB.
pub const DEFAULT_PIECE_BLOCK_BYTES: usize = 1 << 20;

/// The default for [`WriterOptions::min_piece_bytes`]: 8.
pub const DEFAULT_MIN_PIECE_BYTES: usize = 8;

/// The default for [`WriterOptions::max_pieces`]: 16,777,216.
pub const DEFAULT_MAX_PIECES: usize = 1 << 24;

/// Refuses a part written with another format version, saying so plainly.
/// There is no migration: the project has had no release, so parts written by
/// development versions cannot be read.
pub fn check_format_version(found: u32) -> nwdb_core::Result<()> {
    if found == FORMAT_VERSION {
        return Ok(());
    }
    Err(nwdb_core::Error::corrupt(format!(
        "the part has format version {found} but this build reads version {FORMAT_VERSION}; \
         parts written by development versions before this one cannot be read, and there is \
         no migration"
    )))
}

const ZSTD_LEVEL: i32 = 3;
/// `attrs` is most of the bytes of a part and is long text with many repeats:
/// from level 6 on zstd finds them, and a part is 4 to 5 bytes per span
/// smaller than at level 3. Every other column gains almost nothing from a
/// higher level, so only this one pays the time.
const ATTRS_ZSTD_LEVEL: i32 = 6;
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
#[derive(Clone)]
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
    /// Decides which bytes of `attrs`, `events` and `links` are stored once in
    /// the piece store instead of in the row. `None` stores every value
    /// inline. Default `None`.
    pub splitter: Option<Arc<dyn PayloadSplitter>>,
    /// How many uncompressed bytes of recent piece blocks the writer keeps in
    /// memory to recognise repeated pieces (see
    /// [`PieceWriterOptions::dedup_window_bytes`]). Default 64 MiB. A smaller
    /// window never loses data; a repeat that falls outside it is stored
    /// again.
    pub dedup_window_bytes: usize,
    /// Uncompressed size at which a piece block is closed. Default 1 MiB.
    pub piece_block_bytes: usize,
    /// A cut shorter than this is stored inline, whatever the splitter says: a
    /// reference and a table entry cost more than such a piece. Default 8.
    pub min_piece_bytes: usize,
    /// Once the part holds this many pieces, further new text is stored
    /// inline; pieces that exist can still be referenced. The writer and every
    /// open reader keep a few bytes per piece, and a sender decides how many
    /// there are, so this bounds them; sharing is lost beyond it, nothing
    /// else. Default 16,777,216.
    pub max_pieces: usize,
    /// Told about each piece the first time it is stored, before the batch
    /// that holds it is written. `None` by default.
    pub piece_observer: Option<Arc<dyn PieceObserver>>,
    /// Memory the text index builder may hold for its open segment (see
    /// `nwdb_textindex::IndexOptions::segment_budget_bytes`; values below
    /// 1 MiB are raised to it). Default 32 MiB.
    pub text_index_budget_bytes: usize,
    /// Bytes of `(piece, row)` pairs held before they are sorted and written
    /// to a run file in the part's temporary directory; the runs are merged
    /// into `text.rows` when the part is finished. Eight bytes a pair. Default
    /// 32 MiB. Must be greater than zero.
    pub row_list_budget_bytes: usize,
}

impl std::fmt::Debug for WriterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterOptions")
            .field("row_group_rows", &self.row_group_rows)
            .field("row_group_bytes", &self.row_group_bytes)
            .field("summary_row_group_rows", &self.summary_row_group_rows)
            .field("splitter", &self.splitter.as_ref().map(|_| "<splitter>"))
            .field("dedup_window_bytes", &self.dedup_window_bytes)
            .field("piece_block_bytes", &self.piece_block_bytes)
            .field("min_piece_bytes", &self.min_piece_bytes)
            .field("max_pieces", &self.max_pieces)
            .field(
                "piece_observer",
                &self.piece_observer.as_ref().map(|_| "<observer>"),
            )
            .field("text_index_budget_bytes", &self.text_index_budget_bytes)
            .field("row_list_budget_bytes", &self.row_list_budget_bytes)
            .finish()
    }
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            row_group_rows: 16_384,
            row_group_bytes: 64 * 1024 * 1024,
            summary_row_group_rows: 16_384,
            splitter: None,
            dedup_window_bytes: DEFAULT_DEDUP_WINDOW_BYTES,
            piece_block_bytes: DEFAULT_PIECE_BLOCK_BYTES,
            min_piece_bytes: DEFAULT_MIN_PIECE_BYTES,
            max_pieces: DEFAULT_MAX_PIECES,
            piece_observer: None,
            text_index_budget_bytes: DEFAULT_TEXT_INDEX_BUDGET_BYTES,
            row_list_budget_bytes: DEFAULT_ROW_LIST_BUDGET_BYTES,
        }
    }
}

impl WriterOptions {
    fn validate(&self) -> Result<()> {
        if self.row_group_rows == 0
            || self.row_group_bytes == 0
            || self.summary_row_group_rows == 0
            || self.piece_block_bytes == 0
            || self.row_list_budget_bytes == 0
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
        // Chunk statistics are what row group pruning reads. Page statistics
        // (the column index) are kept only for `start_ns`, below: nothing
        // prunes pages by another column and the index costs about a quarter
        // of a byte per span.
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_column_statistics_enabled(ColumnPath::from("start_ns"), EnabledStatistics::Page)
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
        builder = builder.set_column_compression(
            ColumnPath::from("attrs"),
            Compression::ZSTD(
                ZstdLevel::try_new(ATTRS_ZSTD_LEVEL).expect("level is within zstd's range"),
            ),
        );
    }
    // Encodings by column, for both files (the summary has some of the same
    // columns). Ids are random bytes and a dictionary only adds its keys.
    // Timestamps and token counts are numbers of which only the low bytes
    // vary: the delta of a sorted value, or the bytes split by position so
    // that zstd sees the constant ones together, is far smaller than
    // dictionary keys over thousands of distinct values.
    for name in ["trace_id", "span_id", "parent_span_id"] {
        builder = builder
            .set_column_dictionary_enabled(ColumnPath::from(name), false)
            .set_column_encoding(ColumnPath::from(name), Encoding::PLAIN);
    }
    builder = builder
        .set_column_dictionary_enabled(ColumnPath::from("start_ns"), false)
        .set_column_encoding(ColumnPath::from("start_ns"), Encoding::DELTA_BINARY_PACKED);
    for name in ["end_ns", "input_tokens", "output_tokens"] {
        builder = builder
            .set_column_dictionary_enabled(ColumnPath::from(name), false)
            .set_column_encoding(ColumnPath::from(name), Encoding::BYTE_STREAM_SPLIT);
    }
    // Rows of one trace mostly arrived in the same log record, so the deltas
    // are mostly zero.
    builder = builder
        .set_column_dictionary_enabled(ColumnPath::from("arrival"), false)
        .set_column_encoding(ColumnPath::from("arrival"), Encoding::DELTA_BINARY_PACKED);
    builder.build()
}

/// Length and CRC-32C of everything written to a file so far.
#[derive(Default)]
pub(crate) struct Digest {
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

    pub(crate) fn get(&self) -> FileChecksum {
        FileChecksum {
            len: self.len.load(Ordering::Relaxed),
            crc32c: self.crc.load(Ordering::Relaxed),
        }
    }
}

/// A file shared between the Parquet writer, which owns it for writing, and
/// the part writer, which syncs it after the Parquet writer has finished. It
/// digests every byte that reaches the file.
pub(crate) struct SharedFile {
    file: Arc<File>,
    digest: Arc<Digest>,
}

impl SharedFile {
    pub(crate) fn new(file: Arc<File>, digest: Arc<Digest>) -> Self {
        Self { file, digest }
    }
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
    let writer = ArrowWriter::try_new(sink, schema, Some(parquet_props(rows, spans)))
        .map_err(Error::other)?;
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

/// Writes one immutable part: `spans.parquet`, `traces.idx`, `summary.parquet`,
/// `pieces.blk`, `pieces.idx`, `text.idx` and `text.rows` in a directory that appears under its final
/// name only when [`finish`](Self::finish) has made all of them durable.
///
/// Memory use while writing is the Parquet writer's row group (at most
/// [`WriterOptions::row_group_bytes`] of stored value bytes, plus one row), the
/// de-duplication window ([`WriterOptions::dedup_window_bytes`]) with the open
/// piece block and the hash map over its pieces, the length of every piece in
/// its compact on-disk form (one to three bytes each, at most
/// [`WriterOptions::max_pieces`] of them), and one [`TraceEntry`] and one
/// [`TraceSummary`] per trace so far, and the text index and the pairs of
/// pieces and rows, each within its own budget
/// ([`WriterOptions::text_index_budget_bytes`] and
/// [`WriterOptions::row_list_budget_bytes`]). It does not grow with the number of
/// spans or with the amount of unique text.
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
    sink: Option<PayloadSink>,
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
    level: u8,
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
        let sink = match PayloadSink::create(&tmp_dir, &options) {
            Ok(s) => s,
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
            sink: Some(sink),
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
            level: 0,
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
        self.write_batch(batch, true)
    }

    /// Like [`write`](Self::write), for a batch whose `attrs`, `events` and
    /// `links` already are ropes of this part's piece store (their references
    /// name pieces this writer holds, and the references are counted with
    /// [`add_references`](Self::add_references)). The splitter is not asked.
    pub(crate) fn write_stored(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write_batch(batch, false)
    }

    /// `raw` says whether the raw columns of `batch` still have to be turned
    /// into ropes. Everything else, the contiguity rules, the summary and the
    /// row groups, is the same for both.
    fn write_batch(&mut self, batch: &RecordBatch, raw: bool) -> Result<()> {
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

        let written = self.write_rows(batch, raw);
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
    fn write_rows(&mut self, batch: &RecordBatch, raw: bool) -> Result<()> {
        // The raw columns become ropes first, so that the row group bound
        // counts what is stored.
        let stored = if raw {
            self.sink.as_mut().expect("writer is open").store(batch)?
        } else {
            batch.clone()
        };
        let batch = &stored;
        let first_row = self.rows - batch.num_rows() as u64;
        self.sink
            .as_mut()
            .expect("writer is open")
            .note_rows(batch, first_row)?;
        let row_bytes = row_value_bytes(batch);
        let options = self.options.clone();
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
            spans
                .write(&batch.slice(start, end - start))
                .map_err(Error::other)?;
            if group_is_full(&options, self.group_rows, self.group_bytes) {
                spans.flush().map_err(Error::other)?;
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

    /// Sets the compaction level recorded in the part's [`PartMeta`]. A flush
    /// leaves it at 0.
    pub(crate) fn set_level(&mut self, level: u8) {
        self.level = level;
    }

    /// Stores `bytes` as a piece of this part, or finds the piece that holds
    /// them, under the same rules a splitter's cuts follow: once the part
    /// holds `max_pieces` pieces only a piece that exists is used, and `None`
    /// says the bytes must go inline. A new piece is announced to the
    /// observer. The use is not counted; see
    /// [`add_references`](Self::add_references).
    pub(crate) fn intern_piece(&mut self, bytes: &[u8]) -> Result<Option<u32>> {
        self.sink.as_mut().expect("writer is open").intern(bytes)
    }

    /// Counts `bytes` more bytes of references to pieces, for ropes written
    /// with [`write_stored`](Self::write_stored). Every reference in every row
    /// written must be counted once, or `verify` finds the part inconsistent.
    pub(crate) fn add_references(&mut self, bytes: u64) {
        self.sink
            .as_mut()
            .expect("writer is open")
            .pieces
            .add_references(bytes);
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
        spans.close().map_err(Error::other)?;
        self.spans_file.sync_all()?;

        let PayloadSink { pieces, text, .. } = self.sink.take().expect("writer is open");
        let store = pieces.finish()?;
        let TextFinished {
            index: text_index_sum,
            rows: text_rows_sum,
            index_footer: text_index_footer,
            rows_footer: text_rows_footer,
        } = text.finish(store.totals.pieces)?;
        let spans_sum = self.spans_digest.get();
        let summary_sum = self.write_summary()?;
        let (blk_sum, idx_sum) = (store.blocks, store.index);
        for (name, sum) in [
            (SPANS_FILE, spans_sum),
            (SUMMARY_FILE, summary_sum),
            (PIECES_BLK, blk_sum),
            (PIECES_IDX, idx_sum),
            (TEXT_INDEX_FILE, text_index_sum),
            (TEXT_ROWS_FILE, text_rows_sum),
        ] {
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
            pieces_blk: blk_sum,
            pieces_idx: idx_sum,
            text_index: text_index_sum,
            text_rows: text_rows_sum,
            text_index_footer,
            text_rows_footer,
        };
        write_synced(
            &self.tmp_dir.join(INDEX_FILE),
            &index::encode(&self.entries, checksums),
        )?;

        let mut files = BTreeMap::new();
        for name in [
            SPANS_FILE,
            INDEX_FILE,
            SUMMARY_FILE,
            PIECES_BLK,
            PIECES_IDX,
            TEXT_INDEX_FILE,
            TEXT_ROWS_FILE,
        ] {
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
            level: self.level,
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
            payload: store.totals,
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
                writer.write(&builder.finish()).map_err(Error::other)?;
            }
        }
        if !builder.is_empty() {
            writer.write(&builder.finish()).map_err(Error::other)?;
        }
        writer.close().map_err(Error::other)?;
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

/// The three raw columns, in schema terms.
const RAW_COLUMNS: [(&str, RawColumn); 3] = [
    (col::ATTRS, RawColumn::Attrs),
    (col::EVENTS, RawColumn::Events),
    (col::LINKS, RawColumn::Links),
];

/// Turns the raw columns of a batch into ropes, storing the cut-out pieces.
struct PayloadSink {
    splitter: Option<Arc<dyn PayloadSplitter>>,
    observer: Option<Arc<dyn PieceObserver>>,
    min_piece_bytes: usize,
    max_pieces: u64,
    pieces: PieceWriter,
    /// The text index and the lists of rows of each piece. Every piece enters
    /// the part through `intern` and every rope is written by `write_rows`, so
    /// they are fed from there for flushes and merges alike.
    text: TextWriter,
    cuts: Vec<Range<usize>>,
    ids: Vec<u32>,
    rope: Vec<u8>,
    row_pieces: Vec<u32>,
}

impl PayloadSink {
    fn create(dir: &Path, options: &WriterOptions) -> Result<Self> {
        Ok(Self {
            splitter: options.splitter.clone(),
            observer: options.piece_observer.clone(),
            min_piece_bytes: options.min_piece_bytes,
            max_pieces: options.max_pieces as u64,
            pieces: PieceWriter::create(
                dir,
                PieceWriterOptions {
                    block_bytes: options.piece_block_bytes,
                    dedup_window_bytes: options.dedup_window_bytes,
                },
            )?,
            text: TextWriter::create(
                dir,
                options.text_index_budget_bytes,
                options.row_list_budget_bytes,
            )?,
            cuts: Vec::new(),
            ids: Vec::new(),
            rope: Vec::new(),
            row_pieces: Vec::new(),
        })
    }

    /// Notes the pieces every row of a batch of ropes refers to, `first_row`
    /// being the part's row number of the batch's first row.
    fn note_rows(&mut self, batch: &RecordBatch, first_row: u64) -> Result<()> {
        let schema = batch.schema();
        let mut columns = Vec::with_capacity(RAW_COLUMNS.len());
        for (name, _) in RAW_COLUMNS {
            let at = schema.index_of(name).map_err(Error::other)?;
            columns.push(batch.column(at).as_binary::<i64>());
        }
        for row in 0..batch.num_rows() {
            self.row_pieces.clear();
            for column in &columns {
                if column.is_null(row) {
                    continue;
                }
                for id in rope::piece_ids(column.value(row)) {
                    self.row_pieces
                        .push(id.map_err(|e| Error::other(format!("a rope is malformed: {e:?}")))?);
                }
            }
            self.text
                .add_row(first_row + row as u64, &mut self.row_pieces)?;
        }
        Ok(())
    }

    /// The batch with `attrs`, `events` and `links` replaced by their ropes.
    fn store(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (name, raw) in RAW_COLUMNS {
            let at = schema.index_of(name).map_err(Error::other)?;
            let source = batch.column(at).as_binary::<i64>();
            let mut out = LargeBinaryBuilder::with_capacity(source.len(), 0);
            for row in 0..source.len() {
                if source.is_null(row) {
                    out.append_null();
                    continue;
                }
                self.rope.clear();
                self.encode(raw, source.value(row))?;
                out.append_value(&self.rope);
            }
            columns[at] = Arc::new(out.finish());
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    /// The id of the piece that holds `bytes`, stored now if the part may
    /// still take a new piece (the observer hears of it) and has no such piece;
    /// `None` when it may not and has none. The use is not counted.
    fn intern(&mut self, bytes: &[u8]) -> Result<Option<u32>> {
        if self.pieces.piece_count() >= self.max_pieces {
            return Ok(self.pieces.find_unreferenced(bytes));
        }
        let added = self.pieces.add_piece_unreferenced(bytes)?;
        if added.new {
            self.text.add_piece(added.id, bytes)?;
            if let Some(observer) = &self.observer {
                observer.piece_stored(added.id, bytes);
            }
        }
        Ok(Some(added.id))
    }

    /// Appends the rope of one value to `self.rope`. A splitter whose ranges
    /// break the contract gets its value stored whole; a cut that is too short
    /// to be worth a piece, or that would make a new piece in a part that
    /// already holds the most it may, stays inline.
    fn encode(&mut self, raw: RawColumn, value: &[u8]) -> Result<()> {
        self.cuts.clear();
        if let Some(splitter) = &self.splitter
            && !value.is_empty()
        {
            splitter.cuts(raw, value, &mut self.cuts);
            if !rope::cuts_are_valid(value.len(), &self.cuts)
                || self.cuts.iter().any(|c| c.len() > MAX_PIECE_BYTES)
            {
                self.cuts.clear();
            }
        }
        self.ids.clear();
        let mut kept = 0;
        for i in 0..self.cuts.len() {
            let cut = self.cuts[i].clone();
            if cut.len() < self.min_piece_bytes {
                continue;
            }
            let bytes = &value[cut.clone()];
            let Some(id) = self.intern(bytes)? else {
                continue;
            };
            self.pieces.add_references(bytes.len() as u64);
            self.cuts[kept] = cut;
            self.ids.push(id);
            kept += 1;
        }
        self.cuts.truncate(kept);
        rope::encode_cut(value, &self.cuts, &self.ids, &mut self.rope);
        Ok(())
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
pub(crate) fn row_value_bytes(batch: &RecordBatch) -> Vec<u64> {
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
