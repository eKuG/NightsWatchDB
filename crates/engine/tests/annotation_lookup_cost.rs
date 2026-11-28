//! Looking up the annotations of a trace costs nothing in a database that has
//! none, and searches only the span parts whose window has an annotation part
//! in one that has some.

mod common;

use common::*;
use nwdb_engine::{Engine, ListQuery};

const PARTS: u128 = 95;

/// One span part per window, one trace in each (`w * 10 + 1`).
fn engine_with_parts(dir: &std::path::Path) -> Engine {
    let e = open(opts(dir));
    for w in 0..PARTS {
        ingest(
            &e,
            &[TSpan::new(w * 10 + 1, 1, w as i64 * 1_000 + 10).root()],
        )
        .unwrap();
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, PARTS as usize);
    e
}

/// No annotation part and no buffered annotation: no span part index is
/// searched, by a single read, by a read of many traces, or by the list
/// filter's exact path (which is the same lookup).
#[test]
fn a_database_without_annotations_probes_no_span_part() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_parts(dir.path());
    let snap = e.snapshot();
    let mut total = 0;
    for w in 0..PARTS {
        let (v, more, stats) = snap.annotations_with_stats(tid(w * 10 + 1), 10).unwrap();
        assert!(v.is_empty() && !more);
        total += stats.span_index_probes + stats.parts_probed + stats.blocks_read;
    }
    eprintln!("{total} probes and reads for {PARTS} traces without annotations");
    assert_eq!(total, 0);
    let ids: Vec<_> = (0..PARTS).map(|w| tid(w * 10 + 1)).collect();
    let many = snap.annotations_many(&ids, 10).unwrap();
    assert!(many.iter().all(|(v, more)| v.is_empty() && !more));
    let q = ListQuery {
        annotation: Some(filter("q")),
        ..list_all()
    };
    assert!(snap.list_traces(&q).unwrap().is_empty());
    e.shutdown().unwrap();
}

/// Annotation parts exist in one window only: a trace of another window reads
/// no block and probes the one span part whose window has an annotation part
/// (not the other 94); a trace of that window finds its part with one probe.
#[test]
fn only_span_parts_of_windows_with_annotation_parts_are_searched() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_parts(dir.path());
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    assert_eq!(e.stats().annotation_parts, 1);
    let snap = e.snapshot();
    let mut elsewhere = (0, 0, 0);
    for w in 1..PARTS {
        let (v, _, s) = snap.annotations_with_stats(tid(w * 10 + 1), 10).unwrap();
        assert!(v.is_empty());
        elsewhere.0 += s.span_index_probes;
        elsewhere.1 += s.parts_probed;
        elsewhere.2 += s.blocks_read;
    }
    eprintln!(
        "traces without annotation parts: {} probes, {} parts, {} blocks for {} traces",
        elsewhere.0,
        elsewhere.1,
        elsewhere.2,
        PARTS - 1
    );
    assert_eq!(elsewhere, (PARTS as u64 - 1, 0, 0));
    let (v, _, s) = snap.annotations_with_stats(tid(1), 10).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!((s.span_index_probes, s.parts_probed), (1, 1));
    e.shutdown().unwrap();
}
