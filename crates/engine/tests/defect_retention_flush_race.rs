//! A flush that publishes into an expiring window while retention prepares
//! its replacement leaves the window's spans visible without its annotations.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nwdb_engine::{Engine, Stage};

/// Defect: `apply_retention` decides which parts are expired from the lists
/// it reads before `replace_parts` takes the publish gate. A flush that
/// publishes a part into an expired window in between (late spans of a trace
/// whose window is expiring) is not in that decision, so the replacement takes
/// the window's annotation parts and leaves that span part. The README
/// (Retention) says a snapshot sees the expired parts or not, "never some of
/// them, and never a window's spans without its annotations or the other way
/// round".
///
/// Sequence: window 0 holds trace 1 (span part) and its annotation "q" = 1
/// (annotation part); the clock makes window 0 expired; a pass starts; while
/// its replacement waits at `ReplaceMapBuilt`, a late span of trace 1 that
/// starts in window 0 is ingested and flushed; the replacement publishes.
///
/// Expected: a snapshot taken right after that publish (at
/// `ReplacePublished`, the pass still holding the maintenance gate) shows the
/// trace's spans and its annotation together, or neither. On this tree it
/// shows the late span and no annotation (until the next pass).
#[test]
fn retention_never_leaves_a_window_s_spans_without_its_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(false));
    let observe = Arc::new(AtomicBool::new(false));
    // (spans shown, annotations shown) right after the publish.
    let seen: Arc<Mutex<Option<(bool, usize)>>> = Arc::default();
    let mut o = opts(dir.path());
    o.retention = Some(Duration::from_nanos(1_000));
    o.maintenance_interval = Duration::from_secs(3600);
    o.clock = Some({
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    });
    o.stage_hook = Some({
        let (slot, armed, observe, seen) =
            (slot.clone(), armed.clone(), observe.clone(), seen.clone());
        Arc::new(move |s| {
            if s == Stage::ReplaceMapBuilt && armed.swap(false, Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone().unwrap();
                ingest(&e, &[TSpan::new(1, 2, 20)]).unwrap();
                e.flush().unwrap();
                observe.store(true, Ordering::SeqCst);
            }
            if s == Stage::ReplacePublished && observe.swap(false, Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone().unwrap();
                let snap = e.snapshot();
                let spans = snap.get_trace(tid(1)).unwrap().is_some();
                let anns = snap.annotations(tid(1), usize::MAX).unwrap().0.len();
                *seen.lock().unwrap() = Some((spans, anns));
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    assert_eq!(shown(&e, 1).len(), 1);

    armed.store(true, Ordering::SeqCst);
    now.store(2_000, Ordering::SeqCst);
    // The thread may run the pass first; either way the hook records.
    let _ = e.run_maintenance();
    let t = Instant::now();
    let got = loop {
        if let Some(v) = *seen.lock().unwrap() {
            break v;
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "no replacement published"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    slot.lock().unwrap().take();
    e.shutdown().unwrap();
    assert!(
        got.0 == (got.1 > 0),
        "after retention the trace shows spans: {}, annotations: {}",
        got.0,
        got.1
    );
}
