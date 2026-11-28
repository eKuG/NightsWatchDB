//! An annotation part left in a window whose span part retention removed is
//! shown again once a late span lands in that window, and brings back a value
//! that a newer record overwrote.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use nwdb_engine::{Engine, Stage};

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

/// Defect: the README (Retention) says an annotation part whose window holds
/// no span part any more (a retention pass removed the trace's part between a
/// flush's window decision and its commit) "is removed when its own window
/// expires, like any other; until then `annotations` does not show it", and
/// that of two records of one identity the newer is never in an older window.
/// Neither holds once the window stops being expired (the clock steps back,
/// or `retention` is raised at a restart): the orphan part stays, the trace
/// gets a newer record in an older window (its only span part is there), and
/// when a late span lands in the orphan's window again and the older window
/// expires, the orphan's older record is the trace's annotation again.
///
/// Sequence (retention one window): trace 1 in window 5; "q" = 1 is frozen
/// by a flush that decides window 5; before that flush commits, the clock is
/// at 7W and a pass removes window 5's span part; the clock goes back to 0;
/// the flush commits "q" = 1 into window 5, alone. A late span of the trace
/// in window 4 is flushed; "q" = 2 goes to window 4 (the trace's only
/// window). A late span in window 5 is flushed: "q" = 2 is shown (newer
/// arrival). The clock reaches 6W: window 4 expires, window 5 does not.
///
/// Expected: "q" = 1 is never shown again (2, or nothing, is right). On this
/// tree "q" = 1 comes back.
#[test]
fn an_orphaned_annotation_part_does_not_bring_back_an_overwritten_value() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(false));
    let mut o = opts(dir.path());
    o.retention = Some(Duration::from_nanos(W as u64));
    o.maintenance_interval = Duration::from_secs(3600);
    o.clock = Some({
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    });
    o.stage_hook = Some({
        let (slot, armed, now) = (slot.clone(), armed.clone(), now.clone());
        Arc::new(move |s| {
            if s == Stage::AllPartsWritten && armed.swap(false, Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone().unwrap();
                now.store(7 * W, Ordering::SeqCst);
                e.run_maintenance().unwrap();
                now.store(0, Ordering::SeqCst);
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());

    ingest(&e, &[TSpan::new(1, 1, 5 * W + 10).root()]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    armed.store(true, Ordering::SeqCst);
    e.flush().unwrap();
    // Window 5 has the annotation part and no span part.
    assert_eq!(e.stats().parts, 0);
    assert_eq!(e.snapshot().annotation_part_info().len(), 1);

    ingest(&e, &[TSpan::new(1, 2, 4 * W + 10)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(1, 3, 5 * W + 20)]).unwrap();
    e.flush().unwrap();
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![(None, "q".into(), "s".into(), Some(2.0))],
        "before the expiry"
    );

    now.store(6 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(
        e.get_trace(tid(1)).unwrap().is_some(),
        "window 5 holds the trace"
    );
    let got = brief(&shown(&e, 1));
    assert!(
        got.iter().all(|a| a.3 != Some(1.0)),
        "the overwritten value came back: {got:?}"
    );
    slot.lock().unwrap().take();
    e.shutdown().unwrap();
}
