//! The engine handle: options, start-up, ingest, the applier and flusher
//! threads, and shutdown.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, TryLockError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, UInt64Array};
use nwdb_core::{
    BatchLimits, DEFAULT_WINDOW_NS, Error, PartKind, PayloadSplitter, RecordDecoder, Result,
    SpanColumns, TraceId, TraceSummary, col,
};
use nwdb_manifest::{
    Edit, Manifest, ManifestOptions, OpenMode, has_part_dirs, highest_part_dir_id,
    unreferenced_part_dirs,
};
use nwdb_segment::{
    Annotation, AnnotationReader, MAX_BATCH_ITEMS, PartReader, check_annotation_format_version,
    decode_batch, encode_batch,
};
use nwdb_wal::{Lsn, MAX_RECORD_BYTES, Wal, WalOptions};

use crate::compaction::CompactionOptions;
use crate::lock;
use crate::query::{AggQuery, ListQuery, Snapshot, TokenAgg};
use crate::search::{SearchQuery, SearchResults};
use crate::split::SplitMap;

/// How long the background flusher waits after a failed flush before it tries
/// again. An explicit [`Engine::flush`] is never delayed by it.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long the maintenance thread sleeps between passes when nothing wakes
/// it earlier.
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(10);

/// The tag of a log record that holds annotations: `[0xA1]` followed by
/// `nwdb_segment::encode_batch`. [`Engine::open`] refuses a door decoder
/// registered under it.
pub const ANNOTATION_RECORD_TAG: u8 = 0xA1;

/// What the allocator keeps for a small allocation beyond the bytes asked
/// for: its header and the rounding up to its size class.
const ALLOCATION_SLACK_BYTES: usize = 32;

/// What an annotation counts for in the buffer, the in-flight total and the
/// admission check, besides the bytes of its text: the struct as it sits in
/// its list and the slack of the allocations of its two text fields that every
/// annotation has (name and source). A label and a comment count for their
/// bytes and their own slack. Measured with the counting allocator, a small
/// annotation holds its struct plus its text bytes, so the total is never
/// below the heap held.
const ANNOTATION_OVERHEAD_BYTES: usize =
    std::mem::size_of::<Annotation>() + 2 * ALLOCATION_SLACK_BYTES;

/// The bytes one annotation counts for, everywhere.
fn annotation_bytes(a: &Annotation) -> usize {
    let optional = |s: &Option<String>| s.as_ref().map_or(0, |s| s.len() + ALLOCATION_SLACK_BYTES);
    ANNOTATION_OVERHEAD_BYTES
        + a.name.len()
        + a.source.len()
        + optional(&a.label)
        + optional(&a.comment)
}

/// The result of [`Engine::annotate`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Annotated {
    /// Items that were accepted: logged, or superseded by a later item of the
    /// same request with the same identity (those are not logged).
    pub accepted: usize,
    /// Items that were refused: the index in the request and the reason.
    pub rejected: Vec<(usize, String)>,
}

/// Settings for [`Engine::open`].
#[derive(Clone)]
pub struct EngineOptions {
    /// The data directory, which the engine locks exclusively. It holds
    /// `manifest/`, `parts/` and `wal/`.
    pub data_dir: PathBuf,
    /// Create the data directory and an empty database if there is none.
    /// When false, a missing database is [`Error::NotFound`]. Default true.
    pub create_if_missing: bool,
    /// Width of a part's time window, nanoseconds; must be positive. Default
    /// 15 minutes.
    pub window_ns: i64,
    /// The active buffer is flushed when it reaches this many bytes of memory.
    /// Default 256 MiB.
    pub buffer_bytes: usize,
    /// [`Engine::ingest`] returns `Unavailable` rather than let the active
    /// buffer, the buffer being flushed and the batches of requests still
    /// waiting for the log together grow past this many bytes. A single
    /// request that is larger than the limit is accepted when nothing else is
    /// buffered, so that it can make progress. Default twice `buffer_bytes`.
    /// Setting it below `buffer_bytes` means the size trigger never fires and
    /// only age or an explicit flush empties the buffer.
    pub max_buffered_bytes: usize,
    /// The active buffer is flushed when its oldest data is this old.
    /// Default 30 seconds.
    pub flush_interval: Duration,
    /// [`Engine::ingest`] returns `Unavailable` while the log holds more than
    /// this many bytes of records not yet durable. Default 256 MiB.
    pub max_wal_pending_bytes: u64,
    /// Settings of the write-ahead log. The engine overrides `min_next_lsn`
    /// and `allow_lost_log`.
    pub wal: WalOptions,
    /// Byte and row bounds of the batches built for parts and for query
    /// results.
    pub batch_limits: BatchLimits,
    /// What to do when the log ends below the position the manifest records:
    /// log files were lost, and the records acknowledged after the last flush
    /// are gone. By default `open` returns [`Error::Corrupt`], naming the
    /// missing records, and changes nothing. When true, it starts a new log
    /// above the position and carries on, **accepting that loss**. A new
    /// database is never refused. Default false.
    pub accept_lost_log: bool,
    /// Decides which text of the raw columns a part stores once in its piece
    /// store instead of in every row that holds it (see
    /// [`PayloadSplitter`]). `None` stores every value inline. Default
    /// `None`; the server sets the OTLP splitter.
    pub splitter: Option<Arc<dyn PayloadSplitter>>,
    /// How many bytes of recent piece blocks a flush keeps in memory to
    /// recognise repeated text, per part being written. Default 64 MiB.
    pub dedup_window_bytes: usize,
    /// Memory the text index of a part being written may hold for its open
    /// segment (see `nwdb_segment::WriterOptions::text_index_budget_bytes`),
    /// per part. Default 32 MiB.
    pub text_index_budget_bytes: usize,
    /// Bytes of `(piece, row)` pairs a part being written holds before they
    /// are sorted and spilled to disk (see
    /// `nwdb_segment::WriterOptions::row_list_budget_bytes`), per part.
    /// Default 32 MiB.
    pub row_list_budget_bytes: usize,
    /// The most pieces of text one part holds (see
    /// `nwdb_segment::WriterOptions::max_pieces`); text past it is stored
    /// inline and cannot be searched. Compaction never merges parts whose
    /// pieces add up to more than this. A test facility, in the manner of
    /// [`stage_hook`](Self::stage_hook): leave it alone outside tests. Default
    /// 16,777,216.
    pub max_pieces: usize,
    /// A test facility, in the manner of the log's `sync_hook`: called at each
    /// [`Stage`] of start-up and of a flush. An error it returns at a stage
    /// before the manifest commit is handled exactly as if that step had
    /// failed with that error; at a stage after the commit the return value
    /// is ignored. A panic in it before the commit is a fatal flush error.
    /// Leave it `None` outside tests.
    pub stage_hook: Option<StageHook>,
    /// How often the maintenance thread runs a pass when no flush woke it
    /// sooner; must be positive. Default 10 seconds.
    pub maintenance_interval: Duration,
    /// How long data is kept. A part is removed by a maintenance pass once its
    /// whole time window is older than this: `window_start_ns + window_ns <=
    /// now - retention`. `None` keeps everything. Default `None`.
    pub retention: Option<Duration>,
    /// The clock retention reads, Unix nanoseconds. A test facility, in the
    /// manner of [`stage_hook`](Self::stage_hook); `None` is the system clock.
    pub clock: Option<Clock>,
    /// When and how parts are merged (see [`CompactionOptions`]).
    pub compaction: CompactionOptions,
}

/// The type of [`EngineOptions::stage_hook`].
pub type StageHook = Arc<dyn Fn(Stage) -> Result<()> + Send + Sync>;

/// The type of [`EngineOptions::clock`]: Unix nanoseconds.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// A point of start-up or of a flush, for [`EngineOptions::stage_hook`]. A
/// test facility: the names and the points may change with the engine.
///
/// The open stages are called in the order listed; the first is before the
/// first change to `manifest/` and `parts/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    /// Every check that can refuse the start has passed, including a read of
    /// every log record the replay will decode.
    OpenChecked,
    /// The first manifest version exists if the database is new, and part ids
    /// on disk are below `next_part_id`.
    IdsRaised,
    /// The log was replayed (including any flushes it needed).
    ReplayFinished,
    /// Part directories the manifest does not reference were deleted.
    LeftoversDeleted,
    /// A flush froze the buffer. Nothing is written yet.
    FlushFrozen,
    /// A flush finished one part and opened it; called once per part.
    PartWritten,
    /// All parts are written and open, before the manifest commit.
    AllPartsWritten,
    /// The manifest commit returned, before the parts are published to
    /// queries.
    Committed,
    /// The parts are visible to queries and the frozen buffer is gone, before
    /// the log is trimmed.
    Published,
    /// The log started a new file, so that the trim that follows can remove
    /// the records that are now in parts (only in a flush that trims).
    LogRotated,
    /// The log was trimmed (only in a flush that trims).
    Trimmed,
    /// A merge has its inputs and its part id and is about to call
    /// `merge_parts`; nothing is written. An error returned here is the
    /// merge's own error: it fails as any merge does, and nothing changes.
    MergeStarting,
    /// A merge wrote and opened its output part; nothing is replaced yet. An
    /// error returned here abandons the merge: the output is deleted and
    /// nothing else changes.
    MergeWritten,
    /// A replacement of parts has checked its arguments and holds the publish
    /// gate, just before the manifest commit. An error returned here is
    /// handled as if the commit had failed with it: nothing changes (`Fatal`
    /// puts the engine in the fatal state).
    BeforeReplaceCommit,
    /// A replacement of parts has built the new part list and split map for
    /// the list it saw, and does not hold the publish gate yet. An error
    /// returned here changes nothing (`Fatal` puts the engine in the fatal
    /// state). Flushes may publish after this point; the replacement then
    /// accounts for them under the gate.
    ReplaceMapBuilt,
    /// A replacement of parts committed to the manifest, before the new part
    /// list is published to queries.
    ReplaceCommitted,
    /// The new part list is visible to queries, before any directory of a
    /// removed part is deleted.
    ReplacePublished,
    /// The directory of one removed part was deleted (called once per
    /// directory), before the next one.
    PartDirDeleted,
}

/// Calls the hook, if there is one.
pub(crate) fn run_hook(hook: &Option<StageHook>, stage: Stage) -> Result<()> {
    hook.as_ref().map_or(Ok(()), |h| h(stage))
}

impl std::fmt::Debug for EngineOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineOptions")
            .field("data_dir", &self.data_dir)
            .field("create_if_missing", &self.create_if_missing)
            .field("window_ns", &self.window_ns)
            .field("buffer_bytes", &self.buffer_bytes)
            .field("max_buffered_bytes", &self.max_buffered_bytes)
            .field("flush_interval", &self.flush_interval)
            .field("max_wal_pending_bytes", &self.max_wal_pending_bytes)
            .field("wal", &self.wal)
            .field("batch_limits", &self.batch_limits)
            .field("accept_lost_log", &self.accept_lost_log)
            .field("splitter", &self.splitter.as_ref().map(|_| "<splitter>"))
            .field("dedup_window_bytes", &self.dedup_window_bytes)
            .field("text_index_budget_bytes", &self.text_index_budget_bytes)
            .field("row_list_budget_bytes", &self.row_list_budget_bytes)
            .field("max_pieces", &self.max_pieces)
            .field("stage_hook", &self.stage_hook.as_ref().map(|_| "<hook>"))
            .field("maintenance_interval", &self.maintenance_interval)
            .field("retention", &self.retention)
            .field("clock", &self.clock.as_ref().map(|_| "<clock>"))
            .field("compaction", &self.compaction)
            .finish()
    }
}

impl EngineOptions {
    /// Options for the database in `data_dir`, with every other setting at
    /// its default.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let buffer_bytes = 256 * 1024 * 1024;
        Self {
            data_dir: data_dir.into(),
            create_if_missing: true,
            window_ns: DEFAULT_WINDOW_NS,
            buffer_bytes,
            max_buffered_bytes: 2 * buffer_bytes,
            flush_interval: Duration::from_secs(30),
            max_wal_pending_bytes: 256 * 1024 * 1024,
            wal: WalOptions::default(),
            batch_limits: BatchLimits::DEFAULT,
            accept_lost_log: false,
            splitter: None,
            dedup_window_bytes: nwdb_segment::DEFAULT_DEDUP_WINDOW_BYTES,
            text_index_budget_bytes: nwdb_segment::DEFAULT_TEXT_INDEX_BUDGET_BYTES,
            row_list_budget_bytes: nwdb_segment::DEFAULT_ROW_LIST_BUDGET_BYTES,
            max_pieces: nwdb_segment::DEFAULT_MAX_PIECES,
            stage_hook: None,
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
            retention: None,
            clock: None,
            compaction: CompactionOptions::default(),
        }
    }
}

/// Counters and positions, from [`Engine::stats`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    /// Requests accepted by [`Engine::ingest`] and [`Engine::annotate`] since the engine was opened.
    pub records_ingested: u64,
    /// Bytes of those requests' log records, not counting the tag byte.
    pub bytes_ingested: u64,
    /// Annotations in the active buffer and the buffer being flushed.
    pub annotations_buffered: usize,
    /// Live annotation parts.
    pub annotation_parts: usize,
    /// Bytes on disk of the live annotation parts.
    pub annotation_bytes: u64,
    /// Annotation records written into parts by flushes since the engine was
    /// opened.
    pub annotations_written: u64,
    /// Annotations that flushes dropped because no live part held their
    /// trace any more (retention removed it), since the engine was opened.
    pub annotations_dropped: u64,
    /// Highest log position whose data is in the buffer or in a part, and
    /// whose sender has been (or is being) acknowledged.
    pub acked_lsn: u64,
    /// Highest log position whose data is in live parts, as the manifest says.
    pub flushed_lsn: u64,
    /// Memory estimate of the active buffer plus the buffer being flushed.
    pub buffered_bytes: usize,
    /// Number of live parts.
    pub parts: usize,
    /// Flushes completed since the engine was opened.
    pub flushes: u64,
    /// True while a flush is writing parts or committing.
    pub flushing: bool,
    /// True once the engine has met a fatal error: it accepts and flushes
    /// nothing more, and only a restart recovers it.
    pub fatal: bool,
    /// The most recent error from a flush, the log or trimming, if any.
    pub last_error: Option<String>,
    /// Bytes of large text the live parts' rows refer to, counting each use:
    /// what storing it inside every row would take.
    pub payload_logical_bytes: u64,
    /// The sum of the lengths of the distinct pieces in the live parts' piece
    /// stores: what is stored once, before compression.
    pub payload_unique_bytes: u64,
    /// Bytes the live parts' piece stores take on disk, compressed. The
    /// distance from `payload_logical_bytes` is what sharing saves.
    pub payload_stored_bytes: u64,
    /// Maintenance passes run since the engine was opened, failed ones
    /// included.
    pub maintenance_passes: u64,
    /// Parts removed by retention since the engine was opened.
    pub retention_parts_removed: u64,
    /// The error of the most recent maintenance pass that failed, if any. A
    /// later pass that succeeds does not clear it (like `last_error`); the
    /// pass count shows that passes go on.
    pub last_maintenance_error: Option<String>,
    /// Merges that replaced parts since the engine was opened.
    pub merges_done: u64,
    /// Parts removed by those merges (the parts they took in).
    pub parts_merged_away: u64,
    /// Spans dropped by merges because the same `(trace id, span id)` was in
    /// an earlier input.
    pub duplicate_spans_dropped: u64,
    /// Bytes on disk of the inputs of those merges.
    pub merge_input_bytes: u64,
    /// Bytes on disk of their outputs.
    pub merge_output_bytes: u64,
    /// Annotation records dropped by merges because a newer record of the
    /// same identity (trace, span, name, source) was in the same window.
    pub annotations_superseded: u64,
    /// Pairs of a window and a kind of part (spans, annotations) that now
    /// have more than one live part: 0 means both kinds are settled.
    pub windows_with_many_parts: usize,
    /// True while a merge is being written or replaced.
    pub merging: bool,
    /// The most recent compaction failure, if any, naming the window (and the
    /// part, for damage). A later success does not clear it.
    pub last_compaction_error: Option<String>,
}

/// A live part: its metadata and an open reader.
pub(crate) struct LivePart {
    pub meta: nwdb_core::PartMeta,
    pub reader: PartReader,
    /// When the part became live in this process (start-up for the parts the
    /// manifest listed): compaction waits for a window to be quiet by it.
    pub live_since: Instant,
}

/// A live annotation part: its metadata and an open reader.
pub(crate) struct LiveAnnotationPart {
    pub meta: nwdb_core::PartMeta,
    pub reader: AnnotationReader,
    /// When the part became live in this process, as for [`LivePart`].
    pub live_since: Instant,
}

impl LiveAnnotationPart {
    pub(crate) fn new(meta: nwdb_core::PartMeta, reader: AnnotationReader) -> Self {
        Self {
            meta,
            reader,
            live_since: Instant::now(),
        }
    }
}

/// The window an annotation part belongs to: `(window_start_ns, window_ns)`.
pub(crate) type WindowKey = (i64, i64);

/// The order of windows by when retention removes them: by end
/// (`window_start_ns + window_ns`, in wide arithmetic as retention computes
/// it), then by start. The greatest is the window that expires last.
pub(crate) fn window_order((start, ns): WindowKey) -> (i128, i64) {
    (i128::from(start) + i128::from(ns), start)
}

/// The positions in `parts` of the annotation parts of each window.
pub(crate) fn annotation_windows(
    parts: &[Arc<LiveAnnotationPart>],
) -> HashMap<WindowKey, Vec<u32>> {
    let mut out: HashMap<WindowKey, Vec<u32>> = HashMap::new();
    for (i, p) in parts.iter().enumerate() {
        out.entry((p.meta.window_start_ns, p.meta.window_ns))
            .or_default()
            .push(i as u32);
    }
    out
}

impl LivePart {
    pub(crate) fn new(meta: nwdb_core::PartMeta, reader: PartReader) -> Self {
        Self {
            meta,
            reader,
            live_since: Instant::now(),
        }
    }
}

/// What queries can see, replaced whole under the core lock.
#[derive(Clone, Default)]
pub(crate) struct Visible {
    pub parts: Arc<Vec<Arc<LivePart>>>,
    /// The traces that are in more than one of `parts`. Replaced together
    /// with `parts` in one step, so the two always agree.
    pub split: Arc<SplitMap>,
    /// The buffer being written out, until its parts are live.
    pub frozen: Option<Arc<Vec<Arc<RecordBatch>>>>,
    pub active: Arc<Vec<Arc<RecordBatch>>>,
    /// The annotations of the buffer being written out, one entry per log
    /// record, oldest first.
    pub ann_frozen: Option<AnnBuf>,
    pub ann_active: AnnBuf,
    /// The live annotation parts in part id order, and where the parts of
    /// each window are. Replaced together, in the same step as `parts`.
    pub ann_parts: Arc<Vec<Arc<LiveAnnotationPart>>>,
    pub ann_windows: Arc<HashMap<WindowKey, Vec<u32>>>,
}

/// Buffered annotations: one entry per log record, so a snapshot copies a
/// list of pointers, as for the span batches.
pub(crate) type AnnBuf = Arc<Vec<Arc<Vec<Annotation>>>>;

/// Everything the threads share under one lock. Held only to move pointers
/// and counters, never across I/O.
pub(crate) struct Core {
    pub fatal: Option<String>,
    pub closing: bool,
    pub stop_flusher: bool,
    pub vis: Visible,
    pub active_bytes: usize,
    pub active_oldest: Option<Instant>,
    pub frozen_bytes: usize,
    /// Bytes of batches of requests accepted but not yet in the buffer.
    pub inflight_bytes: usize,
    /// Requests accepted whose callback has not finished.
    pub outstanding: u64,
    /// Highest log position in the buffer or in parts: always a contiguous
    /// prefix of the log.
    pub applied_lsn: u64,
    pub flushed_lsn: u64,
    pub flushing: bool,
    pub retry_after: Option<Instant>,
    pub records: u64,
    pub bytes: u64,
    pub flushes: u64,
    pub annotations_written: u64,
    pub annotations_dropped: u64,
    pub last_error: Option<String>,
    pub stop_maintenance: bool,
    /// A flush finished (or the engine started) since the last pass began.
    pub maintenance_due: bool,
    pub maintenance_passes: u64,
    pub retention_removed: u64,
    pub last_maintenance_error: Option<String>,
    pub compaction: crate::maintenance::CompactionState,
}

/// What one log record holds.
enum Payload {
    Spans(Vec<RecordBatch>),
    Annotations(Vec<Annotation>),
}

struct Pending {
    result: Result<Lsn>,
    payload: Payload,
    bytes: usize,
    done: Box<dyn FnOnce(Result<()>) + Send>,
}

#[derive(Default)]
struct QueueState {
    items: VecDeque<Pending>,
    closed: bool,
}

/// The hand-off from the log's thread to the applier: the log's callbacks only
/// push here.
#[derive(Default)]
struct Queue {
    state: Mutex<QueueState>,
    cv: Condvar,
}

pub(crate) struct Inner {
    pub opts: EngineOptions,
    decoders: HashMap<u8, Arc<dyn RecordDecoder>>,
    pub core: Mutex<Core>,
    /// Wakes the flusher.
    pub flusher_cv: Condvar,
    /// Wakes the maintenance thread.
    pub maintenance_cv: Condvar,
    /// Signals that `outstanding` reached zero.
    idle_cv: Condvar,
    queue: Arc<Queue>,
    pub wal: RwLock<Option<Wal>>,
    pub manifest: Manifest,
    pub parts_dir: PathBuf,
    /// One flush at a time.
    pub flush_gate: Mutex<()>,
    /// Held from just before a manifest commit that changes the part list
    /// until the new list is published, by a flush and by a replacement alike,
    /// so their commit-and-publish steps never interleave. Never held while
    /// parts are written or directories are deleted.
    pub publish_gate: Mutex<()>,
    /// Index lookups made by flushes to find the windows of annotated traces;
    /// for tests (see `Engine::annotation_window_probes`).
    pub window_probes: AtomicU64,
    /// One maintenance pass at a time, whether from the thread or from
    /// [`Engine::run_maintenance`].
    pub maintenance_gate: Mutex<()>,
    /// Set by shutdown before it waits for the maintenance thread: no merge
    /// is started once it is set, and a merge that fails while it is set is
    /// not a failure. It is also the flag a merge is to check between chunks
    /// (see `merge_one`).
    pub merge_stop: Arc<AtomicBool>,
    /// Directories of removed parts that could not be deleted yet; the next
    /// pass tries again. They are unreferenced, so a restart deletes them too.
    pub undeleted: Mutex<Vec<PathBuf>>,
    lock_file: Mutex<Option<File>>,
    threads: Mutex<Threads>,
    /// Held for the whole of `shutdown`, so a second caller waits for the
    /// first; true once shutdown has finished or is under way.
    shut_down: Mutex<bool>,
}

/// The engine threads, until shutdown takes them.
#[derive(Default)]
struct Threads {
    applier: Option<(ThreadId, JoinHandle<()>)>,
    flusher: Option<(ThreadId, JoinHandle<()>)>,
    maintenance: Option<(ThreadId, JoinHandle<()>)>,
}

struct Handle {
    inner: Arc<Inner>,
}

/// An open database. Cheap to clone; every clone is the same engine.
///
/// Dropping the last clone without [`shutdown`](Self::shutdown) stops the
/// threads after the pending acknowledgements, and does not flush: what was
/// acknowledged is in the log and is recovered by the next `open`.
#[derive(Clone)]
pub struct Engine {
    handle: Arc<Handle>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        if self.inner.on_engine_thread() {
            // The last handle was dropped by a callback running on one of the
            // engine's own threads, which cannot join itself.
            let inner = self.inner.clone();
            let _ = std::thread::Builder::new()
                .name("nwdb-cleanup".into())
                .spawn(move || {
                    let _ = inner.shutdown_inner(false);
                });
        } else {
            let _ = self.inner.shutdown_inner(false);
        }
    }
}

/// Decodes one log record (`[tag] + bytes`), refusing with `Corrupt` what
/// start-up cannot replay. Returns what it holds and its memory size.
fn decode_record(
    decoders: &HashMap<u8, Arc<dyn RecordDecoder>>,
    lsn: Lsn,
    payload: &[u8],
) -> Result<(Payload, usize)> {
    let Some((&tag, body)) = payload.split_first() else {
        return Err(Error::corrupt(format!("log record {lsn} is empty")));
    };
    if tag == ANNOTATION_RECORD_TAG {
        let items = decode_batch(body, lsn).map_err(|e| match e {
            Error::Corrupt(m) => Error::corrupt(format!("log record {lsn}: {m}")),
            other => Error::corrupt(format!("log record {lsn} does not decode: {other}")),
        })?;
        let bytes = items.iter().map(annotation_bytes).sum();
        return Ok((Payload::Annotations(items), bytes));
    }
    let decoder = decoders
        .get(&tag)
        .ok_or_else(|| Error::corrupt(format!("log record {lsn} has unknown format tag {tag}")))?;
    let batches = decoder.decode(body).map_err(|e| match e {
        Error::Corrupt(m) => Error::corrupt(format!("log record {lsn}: {m}")),
        other => Error::corrupt(format!("log record {lsn} does not decode: {other}")),
    })?;
    let mut bytes = 0;
    for b in &batches {
        SpanColumns::try_new(b).map_err(|e| {
            Error::corrupt(format!("log record {lsn} decodes to a wrong table: {e}"))
        })?;
        bytes += b.get_array_memory_size();
    }
    Ok((Payload::Spans(batches), bytes))
}

/// `batch` with its `arrival` column replaced by `lsn` in every row; the other
/// columns are shared, not copied.
fn stamp_arrival(batch: RecordBatch, lsn: Lsn) -> RecordBatch {
    let at = batch
        .schema()
        .index_of(col::ARRIVAL)
        .expect("decode_record checked the schema");
    let mut columns = batch.columns().to_vec();
    columns[at] = Arc::new(UInt64Array::from(vec![lsn; batch.num_rows()]));
    RecordBatch::try_new(batch.schema(), columns).expect("the same schema and row count")
}

pub(crate) fn sync_dir(dir: &Path) {
    // Best effort by design: the callers only use it to make a deletion
    // durable sooner, and a leftover is deleted again at the next start-up.
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

impl Engine {
    /// Opens the database in `options.data_dir`, recovering it first.
    ///
    /// `decoders` maps the one-byte tag that precedes every log record to the
    /// decoder that turns such a record back into span batches.
    ///
    /// Nothing under `manifest/` or `parts/` is created, committed, renamed
    /// or deleted until every check that can refuse the start has passed.
    /// Start-up order, each step before the next: validate the arguments;
    /// create the data directory if allowed and lock it (`Conflict` if
    /// another engine holds it); read the manifest without changing it
    /// (never treating a missing manifest next to part directories as an
    /// empty database) and open every live part by its kind; open the log and check it
    /// reaches back to the manifest's position (`Corrupt` if it does not, or
    /// if it ends below it, see [`EngineOptions::accept_lost_log`]); read
    /// every record to be replayed once and check it (`Corrupt` for an
    /// unknown tag, a record that does not decode or decodes to the wrong
    /// table, a gap in the numbers); only
    /// then create the first manifest version of a new database and make
    /// part ids unreusable; replay the log into the buffer; delete the part
    /// directories the manifest does not reference; and start the threads. A
    /// log record with an unknown tag or that does not decode is
    /// [`Error::Corrupt`] and stops start-up.
    pub fn open(
        options: EngineOptions,
        decoders: Vec<(u8, Arc<dyn RecordDecoder>)>,
    ) -> Result<Engine> {
        // 0. Arguments, before the disk is touched.
        if options.window_ns <= 0 {
            return Err(Error::invalid_argument("window_ns must be positive"));
        }
        if options.buffer_bytes == 0 {
            return Err(Error::invalid_argument("buffer_bytes must be positive"));
        }
        if options.maintenance_interval.is_zero() {
            return Err(Error::invalid_argument(
                "maintenance_interval must be positive",
            ));
        }
        if options.retention.is_some_and(|r| r.is_zero()) {
            return Err(Error::invalid_argument(
                "retention must be positive (None keeps everything)",
            ));
        }
        options.compaction.validate()?;
        let mut decoder_map = HashMap::new();
        for (tag, d) in decoders {
            if tag == ANNOTATION_RECORD_TAG {
                return Err(Error::invalid_argument(format!(
                    "tag {tag} is reserved for annotation records"
                )));
            }
            if decoder_map.insert(tag, d).is_some() {
                return Err(Error::invalid_argument(format!(
                    "two decoders registered for tag {tag}"
                )));
            }
        }

        // 1. The directory and its lock. The lock is on the directory itself,
        // so deleting or replacing a file inside it cannot bypass it.
        let dir = options.data_dir.clone();
        if !dir.is_dir() {
            if !options.create_if_missing {
                return Err(Error::not_found(format!(
                    "data directory {} does not exist",
                    dir.display()
                )));
            }
            std::fs::create_dir_all(&dir)?;
            if let Some(parent) = dir.parent() {
                sync_dir(if parent.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    parent
                });
            }
        }
        let lock_file = File::open(&dir)?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::conflict(format!(
                    "{} is in use by another engine",
                    dir.display()
                )));
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }

        // 2. Read what is on disk. A manifest that does not exist next to
        // part directories is a lost manifest, never an empty database.
        let parts_dir = dir.join("parts");
        let manifest_dir = dir.join("manifest");
        let existing = match Manifest::open(
            &manifest_dir,
            OpenMode::MustExist,
            ManifestOptions::default(),
        ) {
            Ok(m) => Some(m),
            Err(Error::NotFound(why)) => {
                if has_part_dirs(&parts_dir)? {
                    return Err(Error::corrupt(format!(
                        "{} holds part directories but no manifest ({why}); \
                         refusing to start as if the database were empty",
                        parts_dir.display()
                    )));
                }
                if !options.create_if_missing {
                    return Err(Error::NotFound(why));
                }
                None
            }
            Err(e) => return Err(e),
        };
        // A new database is at position 0.
        let state = existing.as_ref().map(Manifest::current);
        let flushed = state.as_ref().map_or(0, |s| s.wal_flushed_lsn);
        let mut parts = Vec::new();
        let mut ann_parts = Vec::new();
        for meta in state.iter().flat_map(|s| s.parts.iter()) {
            // The version the manifest recorded is checked first, so an old
            // database is refused by version and not by what differs in its
            // files.
            let dir = parts_dir.join(nwdb_core::part_dir_name(meta.part_id));
            match meta.kind {
                PartKind::Spans => {
                    nwdb_segment::check_format_version(meta.format_version)?;
                    parts.push(Arc::new(LivePart::new(
                        meta.clone(),
                        PartReader::open(dir)?,
                    )));
                }
                PartKind::Annotations => {
                    check_annotation_format_version(meta.format_version)?;
                    ann_parts.push(Arc::new(LiveAnnotationPart::new(
                        meta.clone(),
                        AnnotationReader::open(dir, meta)?,
                    )));
                }
            }
        }
        ann_parts.sort_by_key(|p: &Arc<LiveAnnotationPart>| p.meta.part_id);

        // 3. The log, which must reach back to the manifest's position and
        // must not end below it: the records in between were acknowledged
        // and are gone. The log may repair its own torn tail and remove
        // files that lie wholly inside parts; it changes nothing else.
        let wal_options = WalOptions {
            min_next_lsn: Some(flushed.saturating_add(1)),
            allow_lost_log: options.accept_lost_log,
            ..options.wal.clone()
        };
        let (wal, recovery) = Wal::open(dir.join("wal"), wal_options).map_err(|e| match e {
            Error::Corrupt(m) if m.contains("allow_lost_log") => Error::corrupt(format!(
                "{m}; in the engine the option is EngineOptions::accept_lost_log"
            )),
            e => e,
        })?;
        if recovery.first_lsn > flushed + 1 {
            return Err(Error::corrupt(format!(
                "the log starts at record {} but the manifest says records up to {flushed} are in \
                 parts: records {}..{} were acknowledged and are missing",
                recovery.first_lsn,
                flushed + 1,
                recovery.first_lsn
            )));
        }
        // The replay below can refuse too (an unknown tag, a record that does
        // not decode, a gap), and by then the disk would have changed. Read
        // every record once first and check what the replay would check; a
        // record's batches are dropped at once.
        let expected = recovery.last_lsn.saturating_sub(flushed);
        let mut next = flushed + 1;
        let checked = wal.replay(flushed + 1, |lsn, payload| {
            decode_record(&decoder_map, lsn, payload)?;
            if lsn != next {
                return Err(Error::corrupt(format!(
                    "log record {lsn} follows {}",
                    next - 1
                )));
            }
            next += 1;
            Ok(())
        })?;
        if checked != expected {
            return Err(Error::corrupt(format!(
                "the log holds {checked} records after the manifest's position, expected {expected}"
            )));
        }
        run_hook(&options.stage_hook, Stage::OpenChecked)?;

        // 4. From here the disk changes. A new database gets its empty first
        // manifest version at once, so part directories without a manifest
        // can only mean a lost manifest, never a crash before the first
        // commit.
        let manifest = match existing {
            Some(m) => m,
            None => {
                let m =
                    Manifest::open(&manifest_dir, OpenMode::Create, ManifestOptions::default())?;
                if m.current().version == 0 {
                    m.commit(Edit::default())?;
                }
                m
            }
        };
        // Part ids that exist on disk must never be handed out again. A
        // directory far above `next_part_id` (see `PART_ID_LEFTOVER_WINDOW`)
        // cannot be a leftover of this database and is ignored here; it is
        // deleted with the other unreferenced directories after the replay.
        if let Some(highest) = highest_part_dir_id(&parts_dir, manifest.current().next_part_id)?
            && let Some(next) = highest.checked_add(1)
            && next > manifest.current().next_part_id
        {
            manifest.commit(Edit {
                next_part_id: Some(next),
                ..Edit::default()
            })?;
        }
        run_hook(&options.stage_hook, Stage::IdsRaised)?;

        let inner = Arc::new(Inner {
            decoders: decoder_map,
            core: Mutex::new(Core {
                fatal: None,
                closing: false,
                stop_flusher: false,
                vis: Visible {
                    split: Arc::new(SplitMap::default().extend(&parts)),
                    parts: Arc::new(parts),
                    frozen: None,
                    active: Arc::default(),
                    ann_frozen: None,
                    ann_active: Arc::default(),
                    ann_windows: Arc::new(annotation_windows(&ann_parts)),
                    ann_parts: Arc::new(ann_parts),
                },
                active_bytes: 0,
                active_oldest: None,
                frozen_bytes: 0,
                inflight_bytes: 0,
                outstanding: 0,
                applied_lsn: flushed,
                flushed_lsn: flushed,
                flushing: false,
                retry_after: None,
                records: 0,
                bytes: 0,
                flushes: 0,
                annotations_written: 0,
                annotations_dropped: 0,
                last_error: None,
                stop_maintenance: false,
                // A pass right after start-up removes what expired while the
                // engine was down.
                maintenance_due: true,
                maintenance_passes: 0,
                retention_removed: 0,
                last_maintenance_error: None,
                compaction: Default::default(),
            }),
            flusher_cv: Condvar::new(),
            maintenance_cv: Condvar::new(),
            idle_cv: Condvar::new(),
            queue: Arc::default(),
            wal: RwLock::new(None),
            manifest,
            parts_dir,
            flush_gate: Mutex::new(()),
            publish_gate: Mutex::new(()),
            window_probes: AtomicU64::new(0),
            maintenance_gate: Mutex::new(()),
            merge_stop: Arc::new(AtomicBool::new(false)),
            undeleted: Mutex::new(Vec::new()),
            lock_file: Mutex::new(Some(lock_file)),
            threads: Mutex::new(Threads::default()),
            shut_down: Mutex::new(false),
            opts: options,
        });

        // 5. Replay. The buffer is flushed whenever it fills, so a long log
        // does not need more memory than a running engine.
        let replayed = wal.replay(flushed + 1, |lsn, payload| {
            inner.replay_record(lsn, payload)
        })?;
        let expected = recovery.last_lsn.saturating_sub(flushed);
        if replayed != expected {
            return Err(Error::corrupt(format!(
                "replayed {replayed} log records, expected {expected}"
            )));
        }
        run_hook(&inner.opts.stage_hook, Stage::ReplayFinished)?;

        // 6. Only now are the directories the manifest does not reference
        // known to be garbage: before the replay one of them could have been
        // the only decoded copy of a record that no longer decodes.
        let leftovers = unreferenced_part_dirs(&inner.parts_dir, &inner.manifest.current())?;
        for d in &leftovers {
            std::fs::remove_dir_all(d)?;
        }
        if !leftovers.is_empty() {
            sync_dir(&inner.parts_dir);
        }
        run_hook(&inner.opts.stage_hook, Stage::LeftoversDeleted)?;
        *inner.wal.write().unwrap_or_else(|e| e.into_inner()) = Some(wal);

        // 7. Accept requests.
        if let Err(e) = inner.spawn_threads() {
            let _ = inner.shutdown_inner(false);
            return Err(e);
        }
        Ok(Engine {
            handle: Arc::new(Handle { inner }),
        })
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.handle.inner
    }

    /// Logs a request, makes it visible once it is durable, and then calls
    /// `on_durable`.
    ///
    /// `record` is the request in the door's own wire format and `tag` names
    /// that format; the engine logs `[tag] + record`. `batches` are the same
    /// request already decoded, and must decode from `record` identically, as
    /// they are what replay will rebuild after a crash.
    ///
    /// Returns an error at once, without calling `on_durable` and without
    /// changing anything, when: `tag` has no registered decoder or a batch
    /// does not have the span schema ([`Error::InvalidArgument`]); the buffer
    /// or the log has too much waiting ([`Error::Unavailable`], retry later);
    /// the engine is shut down ([`Error::Unavailable`]); or it is in the
    /// fatal state ([`Error::Fatal`]).
    ///
    /// Otherwise `on_durable` is called exactly once, on an engine thread: with
    /// `Ok` when the record is durable and its batches are visible to
    /// queries, or with the error that stopped it ([`Error::Fatal`] when the
    /// log or the engine failed; the record may still be recovered after a
    /// restart). The callback must not block for long: it delays every
    /// acknowledgement behind it.
    pub fn ingest(
        &self,
        tag: u8,
        mut record: Vec<u8>,
        batches: Vec<RecordBatch>,
        on_durable: impl FnOnce(Result<()>) + Send + 'static,
    ) -> Result<()> {
        let inner = self.inner();
        if !inner.decoders.contains_key(&tag) {
            return Err(Error::invalid_argument(format!(
                "no decoder is registered for tag {tag}"
            )));
        }
        if record.len() >= MAX_RECORD_BYTES {
            return Err(Error::invalid_argument(format!(
                "record of {} bytes is too large",
                record.len()
            )));
        }
        let mut bytes = 0;
        for b in &batches {
            SpanColumns::try_new(b)?;
            bytes += b.get_array_memory_size();
        }

        let wal_guard = inner.wal.read().unwrap_or_else(|e| e.into_inner());
        let Some(wal) = wal_guard.as_ref() else {
            return Err(Error::unavailable("the engine is shut down"));
        };
        inner.admit(wal, bytes, record.len())?;

        record.insert(0, tag);
        let queue = inner.queue.clone();
        let done: Box<dyn FnOnce(Result<()>) + Send> = Box::new(on_durable);
        // The log's callback does almost nothing: it hands the result to the
        // applier. It never calls back into the log and never blocks beyond
        // this queue's short lock.
        wal.append_owned(record, move |result| {
            lock(&queue.state).items.push_back(Pending {
                result,
                payload: Payload::Spans(batches),
                bytes,
                done,
            });
            queue.cv.notify_one();
        });
        Ok(())
    }

    /// Logs annotations (scores, labels, comments, deletes) of traces that are
    /// already known, makes them visible once they are durable, and then calls
    /// `on_durable`.
    ///
    /// Fails as a whole, changing nothing, with [`Error::InvalidArgument`] for
    /// no items or more than [`nwdb_segment::MAX_BATCH_ITEMS`], and with the
    /// errors of [`ingest`](Self::ingest) when the buffer or the log has too
    /// much waiting, the engine is shutting down or it is fatal.
    ///
    /// Each item is checked in turn: `validate` (the message is the reason),
    /// then that its trace is known (rows in the buffers or in a live part;
    /// the span id is not checked, spans may arrive later): "unknown trace".
    /// Rejected items are listed with their index in `items`; the others are
    /// written. If two accepted items of the request have the same identity
    /// the later one wins and the earlier is counted as accepted but not
    /// logged. The log record takes its number when it is applied to the
    /// buffer, and that number is the `arrival` of its items, whatever the
    /// caller set.
    ///
    /// `on_durable` is called exactly once, on an engine thread, after the log
    /// sync that covers the record and after the items became visible to
    /// queries (as for `ingest`; they are not visible earlier), if and only if
    /// at least one item was logged. When every item was rejected nothing is
    /// logged, nothing is admitted (so the admission errors above are not
    /// returned either) and `on_durable` is dropped uncalled.
    pub fn annotate(
        &self,
        items: Vec<Annotation>,
        on_durable: impl FnOnce(Result<()>) + Send + 'static,
    ) -> Result<Annotated> {
        let inner = self.inner();
        if items.is_empty() || items.len() > MAX_BATCH_ITEMS {
            return Err(Error::invalid_argument(format!(
                "a request holds 1 to {MAX_BATCH_ITEMS} annotations, got {}",
                items.len()
            )));
        }
        let mut rejected: Vec<(usize, String)> = Vec::new();
        let mut valid: Vec<usize> = Vec::new();
        for (i, a) in items.iter().enumerate() {
            match a.validate() {
                Ok(()) => valid.push(i),
                Err(why) => rejected.push((i, why)),
            }
        }
        let ids: Vec<TraceId> = valid.iter().map(|&i| items[i].trace_id).collect();
        let known = self.snapshot().known_traces(&ids)?;
        let mut survivors: Vec<usize> = Vec::new();
        for (&i, known) in valid.iter().zip(known) {
            if known {
                survivors.push(i);
            } else {
                rejected.push((i, "unknown trace".into()));
            }
        }
        rejected.sort_by_key(|(i, _)| *i);
        let accepted = survivors.len();
        // Of equal identities only the last of the request is logged.
        survivors.sort_by(|&a, &b| items[a].identity_cmp(&items[b]).then(a.cmp(&b)));
        let mut keep: Vec<usize> = Vec::with_capacity(survivors.len());
        for (k, &i) in survivors.iter().enumerate() {
            let last = survivors
                .get(k + 1)
                .is_none_or(|&n| items[i].identity_cmp(&items[n]).is_ne());
            if last {
                keep.push(i);
            }
        }
        keep.sort_unstable();
        let result = Annotated { accepted, rejected };
        if keep.is_empty() {
            return Ok(result);
        }
        let mut slots: Vec<Option<Annotation>> = items.into_iter().map(Some).collect();
        let logged: Vec<Annotation> = keep
            .iter()
            .map(|&i| slots[i].take().expect("each index is kept once"))
            .collect();
        let bytes: usize = logged.iter().map(annotation_bytes).sum();
        let mut record = vec![ANNOTATION_RECORD_TAG];
        encode_batch(&logged, &mut record)?;
        if record.len() >= MAX_RECORD_BYTES {
            return Err(Error::invalid_argument(format!(
                "record of {} bytes is too large",
                record.len()
            )));
        }

        let wal_guard = inner.wal.read().unwrap_or_else(|e| e.into_inner());
        let Some(wal) = wal_guard.as_ref() else {
            return Err(Error::unavailable("the engine is shut down"));
        };
        inner.admit(wal, bytes, record.len() - 1)?;
        let queue = inner.queue.clone();
        let done: Box<dyn FnOnce(Result<()>) + Send> = Box::new(on_durable);
        wal.append_owned(record, move |r| {
            lock(&queue.state).items.push_back(Pending {
                result: r,
                payload: Payload::Annotations(logged),
                bytes,
                done,
            });
            queue.cv.notify_one();
        });
        Ok(result)
    }

    /// Writes everything acknowledged so far into parts, publishes them in
    /// one manifest commit and trims the log, then returns.
    ///
    /// [`Error::Fatal`] if the engine is in the fatal state or this flush
    /// leaves it there. Any other error leaves the data in the buffer, where
    /// it stays visible, and a later flush tries again with new part ids.
    pub fn flush(&self) -> Result<()> {
        {
            let c = lock(&self.inner().core);
            if let Some(f) = &c.fatal {
                return Err(Error::fatal(f.clone()));
            }
            if c.closing {
                return Err(Error::unavailable("the engine is shut down"));
            }
        }
        self.inner().do_flush(true)
    }

    /// A consistent view of the data: the live parts, the buffer being
    /// flushed if there is one, and the batches in the active buffer at this
    /// instant. Queries on it see exactly that state however long they run.
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner();
        let c = lock(&inner.core);
        Snapshot::new(c.vis.clone(), inner.opts.batch_limits, c.flushed_lsn)
    }

    /// [`Snapshot::get_trace`] on a fresh snapshot.
    pub fn get_trace(&self, trace_id: TraceId) -> Result<Option<Vec<RecordBatch>>> {
        self.snapshot().get_trace(trace_id)
    }

    /// [`Snapshot::list_traces`] on a fresh snapshot.
    pub fn list_traces(&self, query: &ListQuery) -> Result<Vec<TraceSummary>> {
        self.snapshot().list_traces(query)
    }

    /// [`Snapshot::search`] on a fresh snapshot, with the log position the
    /// search covered. Spans still in the memory buffer are not searched; see
    /// [`Snapshot::search`].
    pub fn search(&self, query: &SearchQuery) -> Result<SearchResults> {
        let snapshot = self.snapshot();
        let hits = snapshot.search(query)?;
        Ok(SearchResults {
            hits,
            searched_through_lsn: snapshot.searched_through_lsn(),
        })
    }

    /// [`Snapshot::aggregate_tokens`] on a fresh snapshot.
    pub fn aggregate_tokens(&self, query: &AggQuery) -> Result<Vec<TokenAgg>> {
        self.snapshot().aggregate_tokens(query)
    }

    /// For tests: the index lookups flushes have made to find the windows of
    /// annotated traces since the engine opened.
    #[doc(hidden)]
    pub fn annotation_window_probes(&self) -> u64 {
        self.inner().window_probes.load(Ordering::Relaxed)
    }

    /// Counters and positions.
    pub fn stats(&self) -> EngineStats {
        let c = lock(&self.inner().core);
        let (mut payload_logical_bytes, mut payload_unique_bytes, mut payload_stored_bytes) =
            (0, 0, 0);
        let mut windows: HashMap<(i64, i64, PartKind), usize> = HashMap::new();
        for p in c.vis.parts.iter() {
            let payload = &p.meta.payload;
            payload_logical_bytes += payload.logical_bytes;
            payload_unique_bytes += payload.unique_bytes;
            payload_stored_bytes += payload.stored_bytes;
            *windows
                .entry((p.meta.window_start_ns, p.meta.window_ns, PartKind::Spans))
                .or_default() += 1;
        }
        for p in c.vis.ann_parts.iter() {
            *windows
                .entry((
                    p.meta.window_start_ns,
                    p.meta.window_ns,
                    PartKind::Annotations,
                ))
                .or_default() += 1;
        }
        EngineStats {
            records_ingested: c.records,
            bytes_ingested: c.bytes,
            annotations_buffered: c
                .vis
                .ann_active
                .iter()
                .chain(c.vis.ann_frozen.iter().flat_map(|f| f.iter()))
                .map(|b| b.len())
                .sum(),
            acked_lsn: c.applied_lsn,
            flushed_lsn: c.flushed_lsn,
            buffered_bytes: c.active_bytes + c.frozen_bytes,
            parts: c.vis.parts.len(),
            annotation_parts: c.vis.ann_parts.len(),
            annotation_bytes: c.vis.ann_parts.iter().map(|p| p.meta.size_bytes()).sum(),
            annotations_written: c.annotations_written,
            annotations_dropped: c.annotations_dropped,
            flushes: c.flushes,
            flushing: c.flushing,
            fatal: c.fatal.is_some(),
            last_error: c.last_error.clone(),
            payload_logical_bytes,
            payload_unique_bytes,
            payload_stored_bytes,
            maintenance_passes: c.maintenance_passes,
            retention_parts_removed: c.retention_removed,
            last_maintenance_error: c.last_maintenance_error.clone(),
            merges_done: c.compaction.merges_done,
            parts_merged_away: c.compaction.parts_merged_away,
            duplicate_spans_dropped: c.compaction.duplicates_dropped,
            merge_input_bytes: c.compaction.input_bytes,
            merge_output_bytes: c.compaction.output_bytes,
            annotations_superseded: c.compaction.annotations_superseded,
            windows_with_many_parts: windows.values().filter(|&&n| n > 1).count(),
            merging: c.compaction.merging,
            last_compaction_error: c.compaction.last_error.clone(),
        }
    }

    /// Runs one maintenance pass now, on the calling thread, and returns its
    /// result: the same pass the maintenance thread runs, serialised with it.
    /// For tests and for operators who want a pass without waiting for the
    /// interval. A failure is also recorded in the statistics.
    ///
    /// [`Error::Unavailable`] once the engine is shut down, [`Error::Fatal`]
    /// in the fatal state.
    pub fn run_maintenance(&self) -> Result<()> {
        self.inner().run_pass(true)
    }

    /// Stops accepting requests, waits for every accepted request to be
    /// acknowledged, flushes, shuts the log down and releases the directory
    /// lock. Queries on existing snapshots keep working.
    ///
    /// Returns the first error met; the log is shut down and the lock
    /// released regardless. Calling it from a callback of this engine is an
    /// [`Error::InvalidArgument`], as it would wait for itself.
    pub fn shutdown(&self) -> Result<()> {
        if self.inner().on_engine_thread() {
            return Err(Error::invalid_argument(
                "shutdown cannot be called from an engine callback",
            ));
        }
        self.inner().shutdown_inner(true)
    }
}

impl Inner {
    fn on_engine_thread(&self) -> bool {
        let me = std::thread::current().id();
        let t = lock(&self.threads);
        [&t.applier, &t.flusher, &t.maintenance]
            .into_iter()
            .flatten()
            .any(|(id, _)| *id == me)
    }

    pub(crate) fn set_fatal_locked(&self, c: &mut Core, msg: String) {
        c.last_error = Some(msg.clone());
        c.fatal.get_or_insert(msg);
        self.flusher_cv.notify_all();
    }

    /// The admission check of every request that is logged: refuses with
    /// `Fatal` or `Unavailable` and changes nothing, or counts the request as
    /// in flight. `bytes` is the request's memory size as the buffer will
    /// count it and `record_len` the bytes of its log record, not counting the
    /// tag. `wal` is the log, read-locked by the caller.
    fn admit(&self, wal: &Wal, bytes: usize, record_len: usize) -> Result<()> {
        let wal_pending = wal.stats().pending_bytes;
        let mut c = lock(&self.core);
        if let Some(f) = &c.fatal {
            return Err(Error::fatal(f.clone()));
        }
        if c.closing {
            return Err(Error::unavailable("the engine is shutting down"));
        }
        let used = c.active_bytes + c.frozen_bytes + c.inflight_bytes;
        if used > 0 && used + bytes > self.opts.max_buffered_bytes {
            return Err(Error::unavailable(format!(
                "{used} bytes are buffered, the limit is {}",
                self.opts.max_buffered_bytes
            )));
        }
        if wal_pending > self.opts.max_wal_pending_bytes {
            return Err(Error::unavailable(format!(
                "{wal_pending} bytes wait in the log, the limit is {}",
                self.opts.max_wal_pending_bytes
            )));
        }
        c.inflight_bytes += bytes;
        c.outstanding += 1;
        c.records += 1;
        c.bytes += record_len as u64;
        Ok(())
    }

    /// Adds a record's content to the active buffer. `lsn` is the log
    /// position of the record.
    ///
    /// The `arrival` of every span row and every annotation is set to `lsn`
    /// here, for live requests and log replay alike, so a replay gives the
    /// values the first run gave. The span column has the width the decoder's
    /// had, so `bytes` is still right.
    fn push_active(&self, c: &mut Core, payload: Payload, bytes: usize, lsn: Lsn) {
        let was_empty = c.vis.active.is_empty() && c.vis.ann_active.is_empty();
        let added = match payload {
            Payload::Spans(batches) => {
                let batches: Vec<Arc<RecordBatch>> = batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|b| Arc::new(stamp_arrival(b, lsn)))
                    .collect();
                let added = !batches.is_empty();
                if added {
                    // Copies the list only if a snapshot still holds the old one.
                    Arc::make_mut(&mut c.vis.active).extend(batches);
                }
                added
            }
            Payload::Annotations(mut items) => {
                for a in &mut items {
                    a.arrival = lsn;
                }
                Arc::make_mut(&mut c.vis.ann_active).push(Arc::new(items));
                true
            }
        };
        if added {
            if was_empty {
                c.active_oldest = Some(Instant::now());
                // The flusher sleeps without a deadline while the buffer is
                // empty; this starts the age clock.
                self.flusher_cv.notify_all();
            }
            c.active_bytes += bytes;
        }
        c.applied_lsn = lsn;
        if c.active_bytes >= self.opts.buffer_bytes {
            self.flusher_cv.notify_all();
        }
    }

    /// Start-up: decodes one log record into the buffer, flushing when the
    /// buffer is full.
    fn replay_record(&self, lsn: Lsn, payload: &[u8]) -> Result<()> {
        let (content, bytes) = decode_record(&self.decoders, lsn, payload)?;
        let full = {
            let mut c = lock(&self.core);
            if lsn != c.applied_lsn + 1 {
                return Err(Error::corrupt(format!(
                    "log record {lsn} follows {}",
                    c.applied_lsn
                )));
            }
            self.push_active(&mut c, content, bytes, lsn);
            c.active_bytes >= self.opts.buffer_bytes
        };
        if full {
            // The log is not trimmed here: it is being read.
            self.do_flush(false)?;
        }
        Ok(())
    }

    fn spawn_threads(self: &Arc<Self>) -> Result<()> {
        let spawn = |name: &str, f: fn(Arc<Inner>)| -> Result<(ThreadId, JoinHandle<()>)> {
            let inner = self.clone();
            let handle = std::thread::Builder::new()
                .name(name.into())
                .spawn(move || f(inner))?;
            Ok((handle.thread().id(), handle))
        };
        let applier = spawn("nwdb-applier", Inner::applier_loop)?;
        lock(&self.threads).applier = Some(applier);
        let flusher = spawn("nwdb-flusher", Inner::flusher_loop)?;
        lock(&self.threads).flusher = Some(flusher);
        let maintenance = spawn("nwdb-maintenance", Inner::maintenance_loop)?;
        lock(&self.threads).maintenance = Some(maintenance);
        Ok(())
    }

    /// Takes results from the log's thread in log order, puts durable
    /// requests into the buffer and acknowledges them.
    fn applier_loop(self: Arc<Self>) {
        loop {
            let pending = {
                let mut q = lock(&self.queue.state);
                loop {
                    if let Some(p) = q.items.pop_front() {
                        break Some(p);
                    }
                    if q.closed {
                        break None;
                    }
                    q = self.queue.cv.wait(q).unwrap_or_else(|e| e.into_inner());
                }
            };
            let Some(p) = pending else { return };
            self.apply(p);
        }
    }

    fn apply(&self, p: Pending) {
        let Pending {
            result,
            payload,
            bytes,
            done,
        } = p;
        let outcome = {
            let mut c = lock(&self.core);
            c.inflight_bytes = c.inflight_bytes.saturating_sub(bytes);
            match result {
                Ok(lsn) => {
                    if let Some(f) = &c.fatal {
                        // Durable in the log, so a restart recovers it, but
                        // not visible on this handle: its outcome is unknown.
                        Err(Error::fatal(f.clone()))
                    } else if lsn != c.applied_lsn + 1 {
                        let msg =
                            format!("the log numbered a record {lsn} after {}", c.applied_lsn);
                        self.set_fatal_locked(&mut c, msg.clone());
                        Err(Error::fatal(msg))
                    } else {
                        self.push_active(&mut c, payload, bytes, lsn);
                        Ok(())
                    }
                }
                Err(e) => {
                    if matches!(e, Error::Fatal(_)) {
                        self.set_fatal_locked(&mut c, e.to_string());
                    }
                    Err(e)
                }
            }
        };
        // The callback runs after the data is visible, outside every lock.
        let _ = catch_unwind(AssertUnwindSafe(|| done(outcome)));
        let mut c = lock(&self.core);
        c.outstanding -= 1;
        if c.outstanding == 0 {
            self.idle_cv.notify_all();
        }
    }

    /// Flushes when the buffer is full or old.
    fn flusher_loop(self: Arc<Self>) {
        loop {
            {
                let mut c = lock(&self.core);
                loop {
                    if c.stop_flusher {
                        return;
                    }
                    let now = Instant::now();
                    let mut wake: Option<Instant> = None;
                    if c.fatal.is_none()
                        && !(c.vis.active.is_empty() && c.vis.ann_active.is_empty())
                    {
                        // `None`: too far away to represent, so the buffer is
                        // never due by age (size, flush and shutdown still
                        // flush it).
                        let due = match c.active_oldest {
                            Some(t) => t.checked_add(self.opts.flush_interval),
                            None => Some(now),
                        };
                        let size_due = c.active_bytes >= self.opts.buffer_bytes;
                        // After a failed flush the next try waits out the
                        // retry delay, however full or old the buffer is.
                        if let Some(at) = if size_due { Some(now) } else { due } {
                            let at = at.max(c.retry_after.unwrap_or(now));
                            if at <= now {
                                break;
                            }
                            wake = Some(at);
                        }
                    }
                    c = match wake {
                        Some(t) => {
                            self.flusher_cv
                                .wait_timeout(c, t.saturating_duration_since(now))
                                .unwrap_or_else(|e| e.into_inner())
                                .0
                        }
                        None => self.flusher_cv.wait(c).unwrap_or_else(|e| e.into_inner()),
                    };
                }
            }
            let outcome = self.do_flush(true);
            let mut c = lock(&self.core);
            c.retry_after = outcome.is_err().then(|| Instant::now() + RETRY_DELAY);
        }
    }

    pub(crate) fn shutdown_inner(&self, flush: bool) -> Result<()> {
        // A second caller waits here until the first has finished.
        let mut done = lock(&self.shut_down);
        if *done {
            return Ok(());
        }
        *done = true;
        // No new request is accepted from here on, so `outstanding` can only
        // fall. Maintenance is told to stop in the same critical section, so
        // no merge starts or finishes after ingest is refused, and the wait
        // below does not wait for one. The flusher keeps running: a request in
        // flight may need a flush.
        {
            let mut c = lock(&self.core);
            c.closing = true;
            self.merge_stop.store(true, Ordering::SeqCst);
            c.stop_maintenance = true;
            self.maintenance_cv.notify_all();
            while c.outstanding > 0 {
                c = self.idle_cv.wait(c).unwrap_or_else(|e| e.into_inner());
            }
            c.stop_flusher = true;
            self.flusher_cv.notify_all();
        }
        let Threads {
            applier,
            flusher,
            maintenance,
        } = std::mem::take(&mut *lock(&self.threads));
        if let Some((_, flusher)) = flusher {
            let _ = flusher.join();
        }
        // A pass looks at the stop flag between its steps, so this waits for
        // the step in progress and no longer (see the README).
        if let Some((_, maintenance)) = maintenance {
            let _ = maintenance.join();
        }
        // Also excludes a pass started by `run_maintenance` on another thread;
        // none can start later, the stop flag is set.
        let _pass = lock(&self.maintenance_gate);

        // The gate is held from the final flush until the log is shut down and
        // the directory lock released, so a `flush` that raced with this
        // shutdown either finished before it or finds the engine closed; none
        // can write after the lock is gone.
        let _gate = lock(&self.flush_gate);
        let mut first_error = None;
        let fatal = lock(&self.core).fatal.clone();
        if let Some(f) = fatal {
            first_error = Some(Error::fatal(f));
        } else if flush && let Err(e) = self.flush_locked(true) {
            first_error = Some(e);
        }

        let wal = self.wal.write().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(wal) = wal
            && let Err(e) = wal.shutdown()
        {
            first_error.get_or_insert(e);
        }
        lock(&self.queue.state).closed = true;
        self.queue.cv.notify_all();
        if let Some((_, applier)) = applier {
            let _ = applier.join();
        }
        // Dropping the file releases the directory lock.
        drop(lock(&self.lock_file).take());
        first_error.map_or(Ok(()), Err)
    }
}
