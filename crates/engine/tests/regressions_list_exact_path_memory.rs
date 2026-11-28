//! The exact path of the list's annotation filter (a candidate split over
//! several span parts, or with buffered annotations) tests the winning
//! annotations of the trace as they stream out of the merge, and stops at the
//! first match, so the memory of one list query does not grow with the number
//! of annotations of one trace. `Snapshot::annotations(trace, limit)` stops
//! reading once it holds `limit` winners and has seen one more, so its memory
//! is bounded by `limit` too.
//!
//! Sequence: trace 1 is split over two span parts (windows 0 and 2000), so a
//! list with an annotation filter takes the exact path for it. It gets
//! `n` annotations, all flushed into parts, the first of which (in identity
//! order) matches the filter. The peak heap of one list query is measured for
//! `n` = 1,000 and `n` = 40,000.
//!
//! Guaranteed: the same peak, within 1 MiB.

mod common;

use common::*;
use nwdb_engine::{Annotation, ListQuery};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};

#[global_allocator]
static ALLOC: Counting = Counting;

/// The counters are global: one test at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Peak heap growth of one filtered list over a split trace with `n`
/// annotations in parts.
fn list_peak(n: usize) -> isize {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(1, 2, 2_500)]).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 2);
    // "a-hit" sorts before every "n..." name: the first winner matches.
    annotate(&e, vec![score(1, None, "a-hit", "s", 1.0)]);
    let mut i = 0;
    while i < n {
        let items: Vec<Annotation> = (i..(i + 1000).min(n))
            .map(|k| score(1, None, &format!("n{k:06}"), "s", 1.0))
            .collect();
        i += items.len();
        annotate(&e, items);
    }
    e.flush().unwrap();
    assert_eq!(e.stats().annotations_buffered, 0);
    let q = ListQuery {
        annotation: Some(filter("a-hit")),
        ..list_all()
    };
    let snap = e.snapshot();
    // Warm up once, so lazily built state does not count.
    let (rows, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(stats.annotation_exact_lookups, 1);
    let before = live();
    reset_peak();
    let rows = snap.list_traces(&q).unwrap();
    let grown = peak() - before;
    assert_eq!(rows.len(), 1);
    drop(snap);
    e.shutdown().unwrap();
    grown
}

#[test]
fn a_filtered_list_does_not_hold_every_annotation_of_a_trace() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let small = list_peak(1_000);
    let large = list_peak(40_000);
    assert!(
        large < small + (1 << 20),
        "one list query peaked {small} bytes above the start with 1,000 annotations on the \
         trace and {large} bytes with 40,000"
    );
}

/// The same for `annotations(trace, 1)`.
#[test]
fn reading_a_few_annotations_of_a_trace_does_not_hold_all_of_them() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    fn read_peak(n: usize) -> isize {
        let dir = tempfile::tempdir().unwrap();
        let e = open(opts(dir.path()));
        ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
        e.flush().unwrap();
        let mut i = 0;
        while i < n {
            let items: Vec<Annotation> = (i..(i + 1000).min(n))
                .map(|k| score(1, None, &format!("n{k:06}"), "s", 1.0))
                .collect();
            i += items.len();
            annotate(&e, items);
        }
        e.flush().unwrap();
        let snap = e.snapshot();
        let (v, more) = snap.annotations(tid(1), 1).unwrap();
        assert_eq!((v.len(), more), (1, true));
        let before = live();
        reset_peak();
        let (v, more) = snap.annotations(tid(1), 1).unwrap();
        let grown = peak() - before;
        assert_eq!((v.len(), more), (1, true));
        drop(snap);
        e.shutdown().unwrap();
        grown
    }
    let small = read_peak(1_000);
    let large = read_peak(40_000);
    assert!(
        large < small + (1 << 20),
        "reading one annotation peaked {small} bytes above the start with 1,000 annotations on \
         the trace and {large} bytes with 40,000"
    );
}
