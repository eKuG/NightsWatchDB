//! Helpers for the recovery tests: running the crash-test child, copying and
//! comparing data directories, and checking what a recovered engine holds.
#![allow(dead_code)]

pub use nwdb_testkit::fs::copy_dir;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hasher;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nwdb_core::TraceId;
use nwdb_engine::{AggQuery, Engine};
use nwdb_manifest::{Manifest, ManifestOptions, ManifestState, OpenMode};

use crate::common::*;

/// What the child printed before it died.
pub struct ChildRun {
    pub acked: Vec<u64>,
    /// A flush was in progress (frozen, not yet trimmed) at the last marker.
    pub flushing_at_end: bool,
    pub success: bool,
    /// The stage named by the child's `ABORT` line, if it printed one.
    pub aborted_at: Option<String>,
    /// The signal that ended the child, if one did.
    pub signal: Option<i32>,
}

/// The signal `std::process::abort` raises.
pub const SIGABRT: i32 = 6;

impl ChildRun {
    /// Panics unless the child announced an abort at `stage` and then died of
    /// SIGABRT. A child that panicked or exited early fails here.
    pub fn assert_aborted_at(&self, stage: &str) {
        assert_eq!(
            self.aborted_at.as_deref(),
            Some(stage),
            "the child did not reach the abort at {stage} (success: {}, signal: {:?})",
            self.success,
            self.signal
        );
        assert_eq!(
            self.signal,
            Some(SIGABRT),
            "the child announced the abort at {stage} but was not ended by SIGABRT"
        );
    }
}

/// Starts the child on `dir` and waits until it dies by itself (it aborts at
/// a stage) or `kill_after` has passed, then kills it. `env` is passed to it.
pub fn run_child(
    dir: &Path,
    first_id: u64,
    pause_us: u64,
    env: &[(&str, String)],
    kill_after: Option<Duration>,
) -> ChildRun {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nwdb-crashtest"));
    cmd.arg(dir)
        .arg(first_id.to_string())
        .arg(pause_us.to_string())
        .stdout(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("start the child");
    let out = BufReader::new(child.stdout.take().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut acked = Vec::new();
        let mut flushing = false;
        let mut aborted_at = None;
        for line in out.lines() {
            let Ok(line) = line else { break };
            match line.split_once(' ') {
                None if line == "READY" => {
                    // A flush during the child's own recovery is not one the
                    // marker follows to its end.
                    flushing = false;
                    let _ = tx.send(());
                }
                Some(("A", id)) => acked.push(id.parse::<u64>().unwrap()),
                Some(("F", f)) => flushing = f == "1",
                Some(("ABORT", stage)) => aborted_at = Some(stage.to_string()),
                _ => {}
            }
        }
        (acked, flushing, aborted_at)
    });
    let started = Instant::now();
    let mut killed_at = None;
    let mut ready = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if !ready && rx.try_recv().is_ok() {
            ready = true;
            killed_at = kill_after.map(|d| Instant::now() + d);
        }
        if let Some(t) = killed_at
            && Instant::now() >= t
        {
            child.kill().expect("SIGKILL");
            break child.wait().unwrap();
        }
        if started.elapsed() > Duration::from_secs(180) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the child neither died nor became ready in time");
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    let (acked, flushing_at_end, aborted_at) = reader.join().unwrap();
    ChildRun {
        acked,
        flushing_at_end,
        success: status.success(),
        aborted_at,
        signal: std::os::unix::process::ExitStatusExt::signal(&status),
    }
}

/// Every file and directory under `dir` with its size and a checksum of its
/// contents, keyed by path relative to `dir`.
pub fn tree(dir: &Path) -> BTreeMap<String, (u64, u64)> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, (u64, u64)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd {
            let e = e.unwrap();
            let rel = e
                .path()
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if e.file_type().unwrap().is_dir() {
                out.insert(format!("{rel}/"), (0, 0));
                walk(root, &e.path(), out);
            } else {
                let bytes = std::fs::read(e.path()).unwrap();
                let mut h = std::hash::DefaultHasher::new();
                h.write(&bytes);
                out.insert(rel, (bytes.len() as u64, h.finish()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// [`tree`] restricted to the given top-level directories.
pub fn tree_of(dir: &Path, top: &[&str]) -> BTreeMap<String, (u64, u64)> {
    tree(dir)
        .into_iter()
        .filter(|(k, _)| top.iter().any(|t| k.starts_with(&format!("{t}/"))))
        .collect()
}

/// The manifest state, and the part directories it does not reference, of a
/// data directory that is not running. Works on a copy, so that nothing under
/// `dir` is touched, not even by the manifest's own start-up clean-up.
pub fn disk_state(dir: &Path) -> (Arc<ManifestState>, Vec<String>) {
    let scratch = tempfile::tempdir().unwrap();
    copy_dir(dir, scratch.path());
    let state = Manifest::open(
        scratch.path().join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current();
    let mut unreferenced = Vec::new();
    for e in std::fs::read_dir(dir.join("parts")).into_iter().flatten() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        let id = name.strip_suffix(".tmp").unwrap_or(&name).parse::<u64>();
        if let Ok(id) = id
            && (name.ends_with(".tmp") || state.part(id).is_none())
        {
            unreferenced.push(name);
        }
    }
    unreferenced.sort();
    (state, unreferenced)
}

/// Log files that lie wholly below `flushed + 1` and are not the newest: the
/// ones a trim would remove.
pub fn obsolete_log_files(dir: &Path, flushed: u64) -> usize {
    let mut firsts: Vec<u64> = std::fs::read_dir(dir.join("wal"))
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.strip_prefix("wal-")?
                .strip_suffix(".log")?
                .parse()
                .ok()
        })
        .collect();
    firsts.sort_unstable();
    firsts.windows(2).filter(|w| w[1] <= flushed + 1).count()
}

/// The ids (of the crash-test child's records) the engine holds, each exactly
/// once, with the token aggregate agreeing.
pub fn present_ids(e: &Engine) -> BTreeSet<u64> {
    let all = e.list_traces(&crate::common::list_all()).unwrap();
    let mut ids = BTreeSet::new();
    for s in &all {
        // Each record is one span, so a trace with two was replayed twice.
        assert_eq!(
            s.span_count, 1,
            "trace {} appears more than once",
            s.trace_id
        );
        let id = u128::from_be_bytes(s.trace_id.0) as u64 - 1;
        assert!(ids.insert(id));
    }
    let agg = e
        .aggregate_tokens(&AggQuery {
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            operation: None,
        })
        .unwrap();
    assert_eq!(agg.iter().map(|a| a.count).sum::<u64>(), ids.len() as u64);
    ids
}

/// Opens `dir`, checks that every acknowledged record is present exactly once,
/// that queries work, and that a new record is accepted and flushed. Returns
/// the engine, still open.
pub fn recover_and_check(
    dir: &Path,
    acked: &BTreeSet<u64>,
    small_buffer: bool,
    what: &str,
    sentinel: u128,
) -> Engine {
    let mut o = opts(dir);
    o.wal.sync = nwdb_wal::SyncMode::Data;
    if small_buffer {
        // The replay flushes while it reads.
        o.buffer_bytes = 2048;
    }
    let e = open(o);
    let present = present_ids(&e);
    for id in acked {
        assert!(
            present.contains(id),
            "{what}: acknowledged record {id} is lost"
        );
        let spans = e
            .get_trace(TraceId((u128::from(*id) + 1).to_be_bytes()))
            .unwrap()
            .map(|b| from_batches(&b))
            .unwrap_or_default();
        assert_eq!(spans.len(), 1, "{what}: record {id}");
    }
    let s = TSpan::new(u128::MAX - sentinel, 1, 5).root();
    ingest(&e, std::slice::from_ref(&s)).unwrap();
    e.flush().unwrap();
    assert!(e.get_trace(s.trace_id()).unwrap().is_some());
    e
}
