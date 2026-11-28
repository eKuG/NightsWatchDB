//! After `window_ns` is narrowed at a restart, retention never brings back an
//! overwritten value or a deleted annotation: the newer records go to the
//! window that expires last, written with the exact `(window_start_ns,
//! window_ns)` of the span part whose window was chosen (not with the
//! engine's current width).

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;
use nwdb_engine::EngineOptions;

fn with(dir: &Path, window_ns: i64, now: &Arc<AtomicI64>) -> EngineOptions {
    let mut o = opts(dir);
    o.window_ns = window_ns;
    o.retention = Some(Duration::from_nanos(100));
    let now = now.clone();
    o.clock = Some(Arc::new(move || now.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_secs(3600);
    o
}

/// A flush writes an annotation into the window that ends last
/// (`window_start_ns + window_ns`, the quantity retention expires by) among
/// those that hold its trace, whatever the order of the windows by start.
/// `window_ns` is an open option that nothing records or checks, so after a
/// restart with a narrower window a window with a larger start can end first.
///
/// Sequence: window 1000; trace 1 in window (0, 1000); "q" = 1 and "d" = 1
/// flushed into it. Restart with window 100; a late span of the trace at 550
/// goes to window (500, 100); "q" = 2 and a delete of "d" are flushed with it
/// and go to (0, 1000), which ends at 1000, not to (500, 100), which ends at
/// 600. The clock reaches 700 with a retention of 100: (500, 100) expires,
/// (0, 1000) stays.
///
/// Guaranteed: the trace shows "q" = 2, never "q" = 1, and never "d".
#[test]
fn retention_after_a_narrower_window_brings_back_overwritten_and_deleted_values() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));

    let e = open(with(dir.path(), 1_000, &now));
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![score(1, None, "q", "s", 1.0), score(1, None, "d", "s", 1.0)],
    );
    e.flush().unwrap();
    e.shutdown().unwrap();

    let e = open(with(dir.path(), 100, &now));
    ingest(&e, &[TSpan::new(1, 2, 550)]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![score(1, None, "q", "s", 2.0), delete(1, None, "d", "s")],
    );
    e.flush().unwrap();
    let windows: Vec<(i64, i64)> = e
        .snapshot()
        .annotation_part_info()
        .iter()
        .map(|x| (x.1, x.2))
        .collect();
    // Both flushes put their annotations in (0, 1000), which ends at 1000: the
    // window of the span part, with its own width, not the engine's current
    // 100.
    assert_eq!(windows, vec![(0, 1_000), (0, 1_000)], "setup");
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![(None, "q".into(), "s".into(), Some(2.0))]
    );

    now.store(700, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(
        e.get_trace(tid(1)).unwrap().is_some(),
        "window (0, 1000) holds the trace"
    );
    let got = brief(&shown(&e, 1));
    assert!(
        got.iter().all(|a| a.3 != Some(1.0)),
        "an overwritten or deleted value came back after retention: {got:?}"
    );
    e.shutdown().unwrap();
}
