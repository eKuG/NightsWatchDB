//! Retention after a change of `window_ns` brings back an overwritten value
//! and a deleted annotation.

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

/// Defect: a flush writes an annotation into the largest window, by
/// `(window_start_ns, window_ns)`, that holds its trace, on the assumption
/// that a larger window never expires before a smaller one. Retention expires
/// a window by its end (`window_start_ns + window_ns`). `window_ns` is an open
/// option that nothing records or checks, so a restart with a narrower window
/// makes windows whose order by start is not their order of expiry: the newer
/// record lands in a window that expires first and the older record comes back.
///
/// Sequence: window 1000; trace 1 in window (0, 1000); "q" = 1 and "d" = 1
/// flushed into it. Restart with window 100; a late span of the trace at 550
/// goes to window (500, 100); "q" = 2 and a delete of "d" are flushed into
/// (500, 100), the larger by start. The clock reaches 700 with a retention of
/// 100: (500, 100) ends at 600 and expires, (0, 1000) ends at 1000 and stays.
///
/// Expected: the trace never shows "q" = 1 again and never shows "d" (the
/// newest record of each is 2 and a delete; losing them with their window is
/// the documented price, an older record coming back is not).
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
    assert_eq!(windows, vec![(0, 1_000), (500, 100)], "setup");
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
