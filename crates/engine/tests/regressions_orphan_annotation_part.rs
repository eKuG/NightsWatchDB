//! No annotation part is committed into a window that has no span part, so
//! none can come back later and show an overwritten value.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use nwdb_engine::{Engine, Stage};

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

/// A flush decides the window of an annotation before it takes the publish
/// gate; retention can remove that window's span part before the commit. The
/// flush notices that retention removed parts since it decided, commits
/// nothing and fails; the next attempt decides again and, as no live span part
/// holds the trace, drops the record. Otherwise such a part would sit in its
/// window until the window stops being expired (the clock steps back, or
/// `retention` is raised at a restart), and a later span in that window would
/// show its older record again once the newer record's window expired.
///
/// Sequence (retention one window): trace 1 in window 5; "q" = 1 is frozen by
/// a flush that decides window 5; before that flush commits, the clock is at
/// 7W and a pass removes window 5's span part; the clock goes back to 0. The
/// flush fails, the next one drops "q" = 1. A late span of the
/// trace in window 4 is flushed; "q" = 2 goes to window 4. A late span in
/// window 5 is flushed. The clock reaches 6W: window 4 expires, window 5 does
/// not.
///
/// Guaranteed: "q" = 1 is never shown.
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
    // Window 5 has no span part any more: the flush that decided it commits
    // nothing and fails, and the record stays in the buffer.
    assert!(matches!(e.flush(), Err(nwdb_core::Error::Unavailable(_))));
    assert_eq!(e.stats().parts, 0);
    assert_eq!(e.snapshot().annotation_part_info().len(), 0);
    assert_eq!(e.stats().annotations_dropped, 0);
    assert_eq!(e.stats().annotations_buffered, 1);
    // The next flush decides again: no live span part holds the trace, so
    // the record is dropped.
    e.flush().unwrap();
    assert_eq!(e.snapshot().annotation_part_info().len(), 0);
    assert_eq!(e.stats().annotations_dropped, 1);
    assert_eq!(e.stats().annotations_buffered, 0);

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
