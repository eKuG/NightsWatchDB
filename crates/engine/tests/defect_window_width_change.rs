//! Defect: the window rule orders windows by `(window_start_ns, window_ns)`,
//! but retention removes a window when `window_start_ns + window_ns` has
//! passed. Once `EngineOptions::window_ns` differs between two runs on one
//! data directory (nothing refuses that), the "largest" window of a trace can
//! be the first one retention removes, and an overwritten annotation comes
//! back.
//!
//! Sequence: run 1 with 10,000 ns windows: trace 7 has a span at 100 (window
//! `(0, 10000)`), score 1.0 is annotated and flushed into window `(0, 10000)`.
//! Run 2 with 1,000 ns windows: a late span of trace 7 at 5,500 (window
//! `(5000, 1000)`) and an overwrite, score 2.0, flushed together. The rule
//! picks `(5000, 1000)` for the overwrite because 5000 > 0, although that
//! window ends at 6,000 and the other at 10,000. The clock moves to 7,000
//! with a retention of 1,000 ns: `(5000, 1000)` expires, `(0, 10000)` does
//! not. The trace is still there (its span at 100).
//!
//! Expected: the trace shows 2.0, or nothing; never the overwritten 1.0.
//! Observed: it shows 1.0.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;

#[test]
fn an_overwritten_annotation_does_not_come_back_after_the_window_width_changes() {
    let dir = tempfile::tempdir().unwrap();

    let mut o = opts(dir.path());
    o.window_ns = 10_000;
    let e = open(o);
    ingest(&e, &[TSpan::new(7, 1, 100).root()]).unwrap();
    annotate(&e, vec![score(7, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    e.shutdown().unwrap();

    let now = Arc::new(AtomicI64::new(0));
    let mut o = opts(dir.path());
    o.window_ns = 1_000;
    o.retention = Some(Duration::from_nanos(1_000));
    let clock = now.clone();
    o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_secs(3600);
    let e = open(o);
    ingest(&e, &[TSpan::new(7, 2, 5_500)]).unwrap();
    annotate(&e, vec![score(7, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    assert_eq!(
        brief(&shown(&e, 7)),
        vec![(None, "q".into(), "s".into(), Some(2.0))]
    );

    // Window (5000, 1000) ends at 6000 <= 7000 - 1000; (0, 10000) does not.
    now.store(7_000, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(
        e.get_trace(tid(7)).unwrap().is_some(),
        "the trace is still held by window (0, 10000)"
    );
    let values: Vec<Option<f64>> = shown(&e, 7).iter().map(|a| a.value).collect();
    assert!(
        !values.contains(&Some(1.0)),
        "the overwritten value 1.0 came back: {values:?}"
    );
    e.shutdown().unwrap();
}
