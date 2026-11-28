//! Support shared by the engine's tests and its crash-test helper binary: a
//! small span encoding with a decoder for it (so the log can be replayed
//! without any protocol crate), and a brute-force model of what the queries
//! must return.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, RecordDecoder, Result, SpanBatchBuilder,
    SpanColumns, SpanId, SpanRecord, TraceId, TraceSummary,
};
use nwdb_engine::{
    AggQuery, CompactionOptions, Engine, EngineOptions, ListQuery, Snapshot, TokenAgg,
};
use nwdb_wal::{SyncMode, WalOptions};

/// Lists every trace: the whole time range and no limit.
pub fn list_all() -> ListQuery {
    ListQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        service: None,
        model: None,
        error: None,
        limit: usize::MAX,
    }
}

/// Log options that never fsync.
pub fn wal_no_sync() -> WalOptions {
    WalOptions {
        sync: SyncMode::None,
        ..WalOptions::default()
    }
}

/// Polls `cond` every 5 ms until it holds; panics naming `what` after `timeout`.
pub fn wait_with_timeout(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One single-span trace, with id `trace`.
pub fn one(trace: u128) -> Vec<TSpan> {
    vec![TSpan::new(trace, 1, 100 + trace as i64).root()]
}

/// Options whose log keeps one record per file, so trimming is exact.
pub fn small_files(dir: &Path) -> EngineOptions {
    let mut o = opts(dir);
    o.wal.max_file_bytes = 1;
    o
}

/// The log tag of the test encoding.
pub const TAG: u8 = 1;

/// One span in the test encoding. Only the fields the queries look at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TSpan {
    pub trace: u128,
    pub span: u64,
    pub parent: Option<u64>,
    pub name: String,
    pub start: i64,
    pub end: i64,
    pub status: i32,
    pub service: Option<String>,
    pub operation: Option<String>,
    pub model: Option<String>,
    pub input: Option<i64>,
    pub output: Option<i64>,
    pub error_type: Option<String>,
    pub session: Option<String>,
    pub user: Option<String>,
    pub attrs: Vec<u8>,
}

impl TSpan {
    pub fn new(trace: u128, span: u64, start: i64) -> Self {
        Self {
            trace,
            span,
            parent: Some(1),
            name: format!("span-{span}"),
            start,
            end: start + 10,
            status: 0,
            service: Some("svc".into()),
            operation: None,
            model: None,
            input: None,
            output: None,
            error_type: None,
            session: None,
            user: None,
            attrs: Vec::new(),
        }
    }

    pub fn root(mut self) -> Self {
        self.parent = None;
        self
    }

    pub fn trace_id(&self) -> TraceId {
        TraceId(self.trace.to_be_bytes())
    }
}

// Encoding: u32 count, then per span fixed fields and length-prefixed
// optional strings. Everything is little endian.

fn put_opt_str(out: &mut Vec<u8>, s: &Option<String>) {
    match s {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            out.extend((s.len() as u32).to_le_bytes());
            out.extend(s.as_bytes());
        }
    }
}

fn put_opt_i64(out: &mut Vec<u8>, v: Option<i64>) {
    match v {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            out.extend(v.to_le_bytes());
        }
    }
}

/// Encodes spans as one log record.
pub fn encode(spans: &[TSpan]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend((spans.len() as u32).to_le_bytes());
    for s in spans {
        out.extend(s.trace.to_be_bytes());
        out.extend(s.span.to_be_bytes());
        match s.parent {
            None => out.push(0),
            Some(p) => {
                out.push(1);
                out.extend(p.to_be_bytes());
            }
        }
        out.extend((s.name.len() as u32).to_le_bytes());
        out.extend(s.name.as_bytes());
        out.extend(s.start.to_le_bytes());
        out.extend(s.end.to_le_bytes());
        out.extend(s.status.to_le_bytes());
        put_opt_str(&mut out, &s.service);
        put_opt_str(&mut out, &s.operation);
        put_opt_str(&mut out, &s.model);
        put_opt_i64(&mut out, s.input);
        put_opt_i64(&mut out, s.output);
        put_opt_str(&mut out, &s.error_type);
        put_opt_str(&mut out, &s.session);
        put_opt_str(&mut out, &s.user);
        out.extend((s.attrs.len() as u32).to_le_bytes());
        out.extend(&s.attrs);
    }
    out
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(Error::corrupt("test record is truncated"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.arr::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.arr()?))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.arr()?))
    }
    fn string(&mut self) -> Result<String> {
        let n = self.u32()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| Error::corrupt("bad utf8"))
    }
    fn opt_str(&mut self) -> Result<Option<String>> {
        Ok(match self.u8()? {
            0 => None,
            1 => Some(self.string()?),
            _ => return Err(Error::corrupt("bad flag")),
        })
    }
    fn opt_i64(&mut self) -> Result<Option<i64>> {
        Ok(match self.u8()? {
            0 => None,
            1 => Some(self.i64()?),
            _ => return Err(Error::corrupt("bad flag")),
        })
    }
}

/// Decodes a record of the test encoding into spans.
pub fn decode_spans(bytes: &[u8]) -> Result<Vec<TSpan>> {
    let mut r = Reader(bytes);
    let n = r.u32()? as usize;
    let mut out = Vec::new();
    for _ in 0..n {
        let trace = u128::from_be_bytes(r.arr()?);
        let span = u64::from_be_bytes(r.arr()?);
        let parent = match r.u8()? {
            0 => None,
            1 => Some(u64::from_be_bytes(r.arr()?)),
            _ => return Err(Error::corrupt("bad flag")),
        };
        let name = r.string()?;
        let start = r.i64()?;
        let end = r.i64()?;
        let status = i32::from_le_bytes(r.arr()?);
        let service = r.opt_str()?;
        let operation = r.opt_str()?;
        let model = r.opt_str()?;
        let input = r.opt_i64()?;
        let output = r.opt_i64()?;
        let error_type = r.opt_str()?;
        let session = r.opt_str()?;
        let user = r.opt_str()?;
        let alen = r.u32()? as usize;
        let attrs = r.take(alen)?.to_vec();
        out.push(TSpan {
            trace,
            span,
            parent,
            name,
            start,
            end,
            status,
            service,
            operation,
            model,
            input,
            output,
            error_type,
            session,
            user,
            attrs,
        });
    }
    if !r.0.is_empty() {
        return Err(Error::corrupt("trailing bytes in test record"));
    }
    Ok(out)
}

/// Builds the span batches of spans.
pub fn to_batches(spans: &[TSpan]) -> Vec<RecordBatch> {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for s in spans {
        b.append(&SpanRecord {
            trace_id: s.trace_id(),
            span_id: SpanId(s.span.to_be_bytes()),
            parent_span_id: s.parent.map(|p| SpanId(p.to_be_bytes())),
            name: &s.name,
            start_ns: s.start,
            end_ns: s.end,
            status_code: s.status,
            service_name: s.service.as_deref(),
            operation: s.operation.as_deref(),
            request_model: s.model.as_deref(),
            input_tokens: s.input,
            output_tokens: s.output,
            error_type: s.error_type.as_deref(),
            session_id: s.session.as_deref(),
            user_id: s.user.as_deref(),
            attrs: &s.attrs,
            ..SpanRecord::default()
        });
    }
    if b.is_empty() {
        vec![]
    } else {
        vec![b.finish()]
    }
}

/// Reads spans back out of batches.
pub fn from_batches(batches: &[RecordBatch]) -> Vec<TSpan> {
    let mut out = Vec::new();
    for b in batches {
        let c = SpanColumns::try_new(b).expect("span schema");
        for i in 0..c.len() {
            let r = c.record(i);
            out.push(TSpan {
                trace: u128::from_be_bytes(r.trace_id.0),
                span: u64::from_be_bytes(r.span_id.0),
                parent: r.parent_span_id.map(|p| u64::from_be_bytes(p.0)),
                name: r.name.to_owned(),
                start: r.start_ns,
                end: r.end_ns,
                status: r.status_code,
                service: r.service_name.map(str::to_owned),
                operation: r.operation.map(str::to_owned),
                model: r.request_model.map(str::to_owned),
                input: r.input_tokens,
                output: r.output_tokens,
                error_type: r.error_type.map(str::to_owned),
                session: r.session_id.map(str::to_owned),
                user: r.user_id.map(str::to_owned),
                attrs: r.attrs.to_vec(),
            });
        }
    }
    out
}

/// The decoder of the test encoding.
pub struct TestDecoder;

impl RecordDecoder for TestDecoder {
    fn decode(&self, record: &[u8]) -> Result<Vec<RecordBatch>> {
        Ok(to_batches(&decode_spans(record)?))
    }
}

pub fn decoders() -> Vec<(u8, Arc<dyn RecordDecoder>)> {
    vec![(TAG, Arc::new(TestDecoder))]
}

/// The splitter the tests use: it cuts every raw value into pieces of up to
/// 64 bytes, so repeated values share pieces and a restart, a flush or a crash
/// has real pieces to lose or to keep.
pub struct ChunkSplitter;

impl PayloadSplitter for ChunkSplitter {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<std::ops::Range<usize>>) {
        let mut at = 0;
        while at < value.len() {
            let end = (at + 64).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

/// Options for tests: the log does not sync (a test that needs real syncs
/// says so) and its group interval is short.
pub fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.window_ns = 1_000;
    o.wal = WalOptions {
        sync_interval: Duration::from_millis(1),
        ..wal_no_sync()
    };
    // Flushes happen when a test asks, unless it lowers these.
    o.buffer_bytes = 1 << 30;
    o.max_buffered_bytes = 2 << 30;
    o.flush_interval = Duration::from_secs(3600);
    o.splitter = Some(Arc::new(ChunkSplitter));
    // Merging is off unless a test turns it on, so parts stay as they were
    // flushed.
    o.compaction.enabled = false;
    o
}

/// [`opts`] with compaction on, as a test wants it: a window is quiet at
/// once, a failed merge is tried again at the next pass, and merges happen
/// only when the test calls `run_maintenance`.
pub fn compact_opts(dir: &Path) -> EngineOptions {
    let mut o = opts(dir);
    o.compaction = CompactionOptions {
        enabled: true,
        quiet: Duration::ZERO,
        retry_backoff: Duration::ZERO,
        background: false,
        ..CompactionOptions::default()
    };
    o
}

/// Runs maintenance passes until no window has more than one part, and
/// returns how many passes it took.
pub fn compact_fully(e: &Engine) -> usize {
    for pass in 1..=300 {
        e.run_maintenance().unwrap();
        if e.stats().windows_with_many_parts == 0 {
            return pass;
        }
        // A short quiet period needs a moment to pass.
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("still more than one part in a window after 300 passes");
}

pub fn open(o: EngineOptions) -> Engine {
    Engine::open(o, decoders()).expect("open")
}

/// Ingests spans as one record and waits for the acknowledgement.
pub fn ingest(e: &Engine, spans: &[TSpan]) -> Result<()> {
    let (tx, rx) = mpsc::channel();
    e.ingest(TAG, encode(spans), to_batches(spans), move |r| {
        let _ = tx.send(r);
    })?;
    rx.recv_timeout(Duration::from_secs(60))
        .expect("callback was not called")
}

/// Ingests, retrying while the engine says it is full.
pub fn ingest_retry(e: &Engine, spans: &[TSpan]) -> Result<()> {
    loop {
        match ingest(e, spans) {
            Err(Error::Unavailable(_)) => std::thread::sleep(Duration::from_millis(1)),
            other => return other,
        }
    }
}

/// What the queries must return, computed by brute force.
#[derive(Clone, Default)]
pub struct Model {
    pub spans: Vec<TSpan>,
}

impl Model {
    pub fn add(&mut self, spans: &[TSpan]) {
        self.spans.extend_from_slice(spans);
    }

    pub fn trace_ids(&self) -> BTreeSet<u128> {
        self.spans.iter().map(|s| s.trace).collect()
    }

    /// Every span once per `(trace, span id)`, the first copy sent. Retries
    /// are exact copies in the tests, and they only happen where the engine
    /// promises to remove them (see `Engine` docs: same buffers), so the
    /// model removes them everywhere.
    pub fn unique(&self) -> Vec<&TSpan> {
        let mut seen = BTreeSet::new();
        self.spans
            .iter()
            .filter(|s| seen.insert((s.trace, s.span)))
            .collect()
    }

    /// The spans of a trace, ordered by (start, span id), one per span id.
    pub fn get_trace(&self, trace: u128) -> Vec<TSpan> {
        let mut v: Vec<TSpan> = self
            .unique()
            .into_iter()
            .filter(|s| s.trace == trace)
            .cloned()
            .collect();
        v.sort_by_key(|s| (s.start, s.span));
        v
    }

    /// One summary per trace, by the documented rules: session and user are
    /// the smallest non-null value, the root is the root with the smallest
    /// `(name, service)` pair.
    pub fn summaries(&self) -> BTreeMap<u128, TraceSummary> {
        let mut out = BTreeMap::new();
        for t in self.trace_ids() {
            let spans = self.get_trace(t);
            let root = spans
                .iter()
                .filter(|s| s.parent.is_none())
                .min_by_key(|s| (&s.name, &s.service));
            let smallest =
                |f: fn(&TSpan) -> &Option<String>| spans.iter().filter_map(|s| f(s).clone()).min();
            let mut models: Vec<String> = spans.iter().filter_map(|s| s.model.clone()).collect();
            models.sort();
            models.dedup();
            out.insert(
                t,
                TraceSummary {
                    trace_id: TraceId(t.to_be_bytes()),
                    start_ns: spans.iter().map(|s| s.start).min().unwrap(),
                    end_ns: spans.iter().map(|s| s.end).max().unwrap(),
                    root_service: root.and_then(|r| r.service.clone()),
                    root_name: root.map(|r| r.name.clone()),
                    span_count: spans.len() as u32,
                    error: spans
                        .iter()
                        .any(|s| s.error_type.is_some() || s.status == 2),
                    input_tokens: spans.iter().filter_map(|s| s.input).sum(),
                    output_tokens: spans.iter().filter_map(|s| s.output).sum(),
                    models,
                    session_id: smallest(|s| &s.session),
                    user_id: smallest(|s| &s.user),
                },
            );
        }
        out
    }

    pub fn list(&self, q: &ListQuery) -> Vec<TraceSummary> {
        let mut v: Vec<TraceSummary> = self
            .summaries()
            .into_values()
            .filter(|s| s.start_ns >= q.from_ns && s.start_ns < q.to_ns)
            .filter(|s| {
                q.service
                    .as_ref()
                    .is_none_or(|v| s.root_service.as_ref() == Some(v))
            })
            .filter(|s| q.model.as_ref().is_none_or(|m| s.models.contains(m)))
            .filter(|s| q.error.is_none_or(|e| s.error == e))
            .collect();
        v.sort_by(|a, b| {
            b.start_ns
                .cmp(&a.start_ns)
                .then(a.trace_id.cmp(&b.trace_id))
        });
        v.truncate(q.limit);
        v
    }

    pub fn agg(&self, q: &AggQuery) -> Vec<TokenAgg> {
        let mut m: BTreeMap<Option<String>, TokenAgg> = BTreeMap::new();
        for s in self.unique() {
            if s.start < q.from_ns || s.start >= q.to_ns {
                continue;
            }
            if q.operation
                .as_ref()
                .is_some_and(|o| s.operation.as_ref() != Some(o))
            {
                continue;
            }
            let e = m.entry(s.model.clone()).or_insert(TokenAgg {
                model: s.model.clone(),
                count: 0,
                input_tokens: 0,
                output_tokens: 0,
            });
            e.count += 1;
            e.input_tokens += s.input.unwrap_or(0);
            e.output_tokens += s.output.unwrap_or(0);
        }
        m.into_values().collect()
    }

    /// Checks every query against the engine's answers.
    pub fn check(&self, e: &Engine, what: &str) {
        self.check_snapshot(&e.snapshot(), what);
    }

    /// Checks every query against the answers of one snapshot.
    pub fn check_snapshot(&self, snap: &Snapshot, what: &str) {
        self.check_snapshot_ignoring(snap, what, &BTreeSet::new());
    }

    /// Like [`check_snapshot`](Self::check_snapshot), for a state in which
    /// the traces in `unmerged` have spans that were sent again across a
    /// flush and are not merged yet: `get_trace` must still be exact (it
    /// removes duplicates), the listing must be exact for every other trace
    /// and count at least the model's spans for these, and the token sums
    /// are not compared.
    pub fn check_snapshot_ignoring(&self, snap: &Snapshot, what: &str, unmerged: &BTreeSet<u128>) {
        for t in self.trace_ids() {
            let got = snap.get_trace(TraceId(t.to_be_bytes())).unwrap();
            assert_eq!(
                got.map(|b| from_batches(&b)),
                Some(self.get_trace(t)),
                "{what}: get_trace {t}"
            );
        }
        assert!(
            snap.get_trace(TraceId([0xee; 16])).unwrap().is_none(),
            "{what}: unknown trace"
        );
        let all = list_all();
        let mut listed = snap.list_traces(&all).unwrap();
        let mut want = self.list(&all);
        let is_unmerged = |t: &TraceSummary| unmerged.contains(&u128::from_be_bytes(t.trace_id.0));
        for w in want.iter().filter(|w| is_unmerged(w)) {
            let g = listed
                .iter()
                .find(|g| g.trace_id == w.trace_id)
                .unwrap_or_else(|| panic!("{what}: unmerged trace {} is not listed", w.trace_id));
            assert!(
                g.span_count >= w.span_count,
                "{what}: unmerged trace {} lost spans",
                w.trace_id
            );
        }
        listed.retain(|t| !is_unmerged(t));
        want.retain(|t| !is_unmerged(t));
        assert_eq!(listed.len(), want.len(), "{what}: list all, trace count");
        for (got, want) in listed.iter().zip(&want) {
            assert_eq!(got.trace_id, want.trace_id, "{what}: list all, order");
            assert_eq!(
                got.start_ns, want.start_ns,
                "{what}: {} start",
                got.trace_id
            );
            assert_eq!(got.end_ns, want.end_ns, "{what}: {} end", got.trace_id);
            assert_eq!(
                got.root_service, want.root_service,
                "{what}: {} root_service",
                got.trace_id
            );
            assert_eq!(
                got.root_name, want.root_name,
                "{what}: {} root_name",
                got.trace_id
            );
            assert_eq!(
                got.span_count, want.span_count,
                "{what}: {} span_count",
                got.trace_id
            );
            assert_eq!(got.error, want.error, "{what}: {} error", got.trace_id);
            assert_eq!(
                got.input_tokens, want.input_tokens,
                "{what}: {} input",
                got.trace_id
            );
            assert_eq!(
                got.output_tokens, want.output_tokens,
                "{what}: {} output",
                got.trace_id
            );
            assert_eq!(got.models, want.models, "{what}: {} models", got.trace_id);
            assert_eq!(
                got.session_id, want.session_id,
                "{what}: {} session_id",
                got.trace_id
            );
            assert_eq!(
                got.user_id, want.user_id,
                "{what}: {} user_id",
                got.trace_id
            );
        }
        if !unmerged.is_empty() {
            return;
        }
        assert_eq!(
            snap.aggregate_tokens(&AggQuery {
                from_ns: i64::MIN,
                to_ns: i64::MAX,
                operation: None
            })
            .unwrap(),
            self.agg(&AggQuery {
                from_ns: i64::MIN,
                to_ns: i64::MAX,
                operation: None
            }),
            "{what}: aggregate all"
        );
    }
}

/// Part directories on disk (final or temporary), by name.
pub fn part_dir_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("parts"))
        .into_iter()
        .flatten()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

/// Bytes the files under `dir/parts` take.
pub fn parts_bytes(dir: &Path) -> u64 {
    fn walk(p: &Path) -> u64 {
        std::fs::read_dir(p)
            .into_iter()
            .flatten()
            .map(|e| {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    walk(&e.path())
                } else {
                    e.metadata().unwrap().len()
                }
            })
            .sum()
    }
    walk(&dir.join("parts"))
}

/// Lists the files in a directory whose name starts with `prefix`.
pub fn count_files(dir: &Path, prefix: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
                .count()
        })
        .unwrap_or(0)
}
