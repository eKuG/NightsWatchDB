//! A flush that places annotations from the part list it read, and then finds
//! that retention removed parts before its commit, starts over instead of
//! committing a decision that may be stale; and a retention replacement
//! decides everything it removes from its cut-off, in one place.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use nwdb_core::Error;
use nwdb_engine::{Engine, EngineOptions, Stage};

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

struct Rig {
    e: Engine,
    now: Arc<AtomicI64>,
    slot: Arc<Mutex<Option<Engine>>>,
    armed: Arc<AtomicBool>,
}

/// An engine with a retention of one window and a clock the test moves. When
/// `armed` is set, the next time the stage `at` is reached `during` runs, on
/// the thread that reached it.
fn rig(
    dir: &std::path::Path,
    at: Stage,
    during: impl Fn(&Engine, &AtomicI64) + Send + Sync + 'static,
) -> Rig {
    let now = Arc::new(AtomicI64::new(0));
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(false));
    let mut o: EngineOptions = opts(dir);
    o.retention = Some(Duration::from_nanos(W as u64));
    o.maintenance_interval = Duration::from_secs(3600);
    o.clock = Some({
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    });
    o.stage_hook = Some({
        let (slot, armed, now) = (slot.clone(), armed.clone(), now.clone());
        Arc::new(move |s| {
            if s == at && armed.swap(false, Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone().unwrap();
                during(&e, &now);
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    Rig {
        e,
        now,
        slot,
        armed,
    }
}

impl Rig {
    fn finish(self) {
        self.slot.lock().unwrap().take();
        self.e.shutdown().unwrap();
    }
}

/// Moves the clock to 7 windows, runs a retention pass and moves it back to 0.
fn retention_at_7w_then_back(e: &Engine, now: &AtomicI64) {
    now.store(7 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    now.store(0, Ordering::SeqCst);
}

fn ann_windows(e: &Engine) -> Vec<i64> {
    e.snapshot()
        .annotation_part_info()
        .iter()
        .map(|x| x.1)
        .collect()
}

/// Another trace's span part is in the same flush, in the window the decision
/// chose; retention removes the trace's own span part between the decision
/// and the commit. A check by window would find the window alive (the flush's
/// own part for the other trace) and commit the annotation into it, where it
/// outlives its trace: after the clock steps back, a newer value of the same
/// identity goes to an older window, and when that expires the old value shows
/// again. The flush fails instead, and the next attempt drops the record.
///
/// Sequence: trace 1 in window 5. "q" = 1 and a span of trace 2 in window 5
/// are flushed; a pass at clock 7W removes trace 1's span part before the
/// commit, and the clock goes back to 0. A late span of trace 1 in window 4
/// and "q" = 2 are flushed (window 4); a late span of trace 1 in window 5 is
/// flushed. The clock reaches 6W: window 4 expires, window 5 does not.
///
/// Guaranteed: "q" = 1 is never shown, and is counted dropped once.
#[test]
fn another_trace_s_part_in_the_window_does_not_keep_an_orphaned_annotation() {
    let dir = tempfile::tempdir().unwrap();
    let r = rig(
        dir.path(),
        Stage::AllPartsWritten,
        retention_at_7w_then_back,
    );
    let e = r.e.clone();
    ingest(&e, &[TSpan::new(1, 1, 5 * W + 10).root()]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(2, 1, 5 * W + 20).root()]).unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    r.armed.store(true, Ordering::SeqCst);
    assert!(matches!(e.flush(), Err(Error::Unavailable(_))));
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (0, 0), "nothing committed");
    assert_eq!(
        (s.annotations_written, s.annotations_dropped),
        (0, 0),
        "a failed attempt counts nothing"
    );
    assert_eq!(s.annotations_buffered, 1);
    assert_eq!(s.retention_parts_removed, 1);
    // The next attempt decides again: no live part holds trace 1.
    e.flush().unwrap();
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (1, 0));
    assert_eq!((s.annotations_written, s.annotations_dropped), (0, 1));
    assert_eq!(s.annotations_buffered, 0);

    ingest(&e, &[TSpan::new(1, 2, 4 * W + 10)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    assert_eq!(ann_windows(&e), vec![4 * W]);
    ingest(&e, &[TSpan::new(1, 3, 5 * W + 20)]).unwrap();
    e.flush().unwrap();
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![(None, "q".into(), "s".into(), Some(2.0))]
    );
    r.now.store(6 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(e.get_trace(tid(1)).unwrap().is_some(), "window 5 holds it");
    let got = brief(&shown(&e, 1));
    assert!(got.is_empty(), "an overwritten value came back: {got:?}");
    r.finish();
}

/// The trace has a live span part in window 5 and a new span in the flush, in
/// window 4. The annotation is decided for window 5, which ends last. Retention
/// removes window 5's part before the commit, the clock goes back, and the
/// trace lives on in the flush's own part. Dropping the annotation then would
/// lose an acknowledged annotation of a live trace. The flush fails, and the
/// next attempt places the annotation in window 4.
#[test]
fn an_annotation_of_a_trace_that_lives_on_in_the_flush_is_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let r = rig(
        dir.path(),
        Stage::AllPartsWritten,
        retention_at_7w_then_back,
    );
    let e = r.e.clone();
    ingest(&e, &[TSpan::new(1, 1, 5 * W + 10).root()]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(1, 2, 4 * W + 10)]).unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    r.armed.store(true, Ordering::SeqCst);
    assert!(matches!(e.flush(), Err(Error::Unavailable(_))));
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (0, 0));
    assert_eq!((s.annotations_written, s.annotations_dropped), (0, 0));
    assert_eq!(s.annotations_buffered, 1);
    // Both the span and the annotation are still readable from the buffers.
    assert_eq!(shown(&e, 1).len(), 1);

    e.flush().unwrap();
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (1, 1));
    assert_eq!((s.annotations_written, s.annotations_dropped), (1, 0));
    assert_eq!(ann_windows(&e), vec![4 * W]);
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![(None, "q".into(), "s".into(), Some(1.0))]
    );
    assert!(e.get_trace(tid(1)).unwrap().is_some());
    r.finish();
}

/// A flush without annotations does not look at the counter: retention
/// removing parts while it waits does not fail it.
#[test]
fn a_flush_without_annotations_is_not_failed_by_retention() {
    let dir = tempfile::tempdir().unwrap();
    let r = rig(
        dir.path(),
        Stage::AllPartsWritten,
        retention_at_7w_then_back,
    );
    let e = r.e.clone();
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(2, 1, 5 * W + 10).root()]).unwrap();
    r.armed.store(true, Ordering::SeqCst);
    e.flush().unwrap();
    let s = e.stats();
    assert_eq!(s.retention_parts_removed, 1, "window 0 was removed");
    assert_eq!(s.parts, 1);
    assert!(e.get_trace(tid(2)).unwrap().is_some());
    assert!(e.get_trace(tid(1)).unwrap().is_none());
    r.finish();
}

/// A maintenance pass that removes nothing between a flush's decision and its
/// commit does not move the counter, so the flush with annotations commits.
#[test]
fn a_retention_pass_that_removes_nothing_does_not_fail_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let r = rig(dir.path(), Stage::AllPartsWritten, |e, _| {
        e.run_maintenance().unwrap();
    });
    let e = r.e.clone();
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    r.armed.store(true, Ordering::SeqCst);
    e.flush().unwrap();
    assert_eq!(e.stats().annotation_parts, 1);
    assert_eq!(e.stats().annotations_written, 1);
    r.finish();
}

/// `apply_retention` decides that something is expired, and a flush publishes
/// a span part and an annotation part into the expiring window before the
/// replacement takes its first look at the part list. The replacement removes
/// every part of the window, of either kind, from the lists it builds on, so
/// no span part is left without its annotation parts, and the count of removed
/// parts is what the commit removed.
///
/// Sequence: window 0 holds trace 1 (span part S1) and "q" (A1). The clock
/// makes window 0 expire; at `RetentionDecided` a late span of trace 1 and
/// "r" are flushed (S2, A2); the replacement then removes S1, S2, A1 and A2.
#[test]
fn a_flush_between_the_retention_decision_and_the_replacement_goes_with_its_window() {
    let dir = tempfile::tempdir().unwrap();
    let r = rig(dir.path(), Stage::RetentionDecided, |e, _| {
        ingest(e, &[TSpan::new(1, 2, 20)]).unwrap();
        annotate(e, vec![score(1, None, "r", "s", 2.0)]);
        e.flush().unwrap();
    });
    let e = r.e.clone();
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (1, 1));

    r.armed.store(true, Ordering::SeqCst);
    r.now.store(2 * W, Ordering::SeqCst);
    // The thread may run the pass first; either way the hook runs once.
    let _ = e.run_maintenance();
    let t = std::time::Instant::now();
    while e.stats().retention_parts_removed == 0 {
        assert!(t.elapsed() < Duration::from_secs(30), "no retention ran");
        std::thread::sleep(Duration::from_millis(5));
    }
    let s = e.stats();
    assert_eq!(
        (s.parts, s.annotation_parts),
        (0, 0),
        "the whole window went: {s:?}"
    );
    assert_eq!(s.retention_parts_removed, 4, "S1, A1, S2 and A2");
    assert!(e.get_trace(tid(1)).unwrap().is_none());
    assert!(shown(&e, 1).is_empty());
    r.finish();
}
