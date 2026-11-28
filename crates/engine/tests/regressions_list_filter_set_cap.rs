//! A list with an annotation filter never fails because many traces match.
//! The fast path builds the set of the matching traces of a window; when that
//! set would pass its cap (1,048,576 ids; lowered here through
//! `with_annotation_set_cap`) the set is dropped, the window is remembered as
//! over the cap for the rest of the query, and its candidates are answered by
//! the exact path, which has no cap. The answer is the same whichever path a
//! trace takes.
//!
//! Sequence: traces 1 and 2 in window 0, each annotated "q" and flushed. Each
//! gets a second "q" annotation of another source, left in the buffer, so
//! both take the exact path: a list with limit 1 and a cap of 1. After a
//! flush both take the fast path: the same list.
//!
//! Guaranteed: both lists return trace 2 (the newest match).

mod common;

use common::*;
use nwdb_engine::ListQuery;

#[test]
fn a_filtered_list_with_a_small_limit_answers_whatever_path_its_traces_take() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &[TSpan::new(1, 1, 101).root()]).unwrap();
    ingest(&e, &[TSpan::new(2, 1, 102).root()]).unwrap();
    annotate(
        &e,
        vec![score(1, None, "q", "a", 1.0), score(2, None, "q", "a", 1.0)],
    );
    e.flush().unwrap();
    let q = ListQuery {
        annotation: Some(filter("q")),
        limit: 1,
        ..list_all()
    };
    let ids = |r: Vec<nwdb_core::TraceSummary>| -> Vec<u128> {
        r.iter()
            .map(|s| u128::from_be_bytes(s.trace_id.0))
            .collect()
    };

    // Both traces take the exact path: buffered annotations.
    annotate(
        &e,
        vec![score(1, None, "q", "b", 1.0), score(2, None, "q", "b", 1.0)],
    );
    let exact = e.snapshot().with_annotation_set_cap(1).list_traces(&q);
    assert_eq!(ids(exact.unwrap()), vec![2]);

    // The same traces after a flush take the fast path.
    e.flush().unwrap();
    let fast = e.snapshot().with_annotation_set_cap(1).list_traces(&q);
    assert_eq!(
        ids(fast.expect("the list failed on the fast path")),
        vec![2]
    );
    e.shutdown().unwrap();
}

/// A window over the cap is scanned once per query, and every candidate of it
/// is answered exactly; a window under the cap is answered by its set.
#[test]
fn a_window_over_the_cap_is_scanned_once_and_its_candidates_answered_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for t in 1..=20u128 {
        ingest(&e, &[TSpan::new(t, 1, 100 + t as i64).root()]).unwrap();
    }
    // Window 2000: one trace.
    ingest(&e, &[TSpan::new(99, 1, 2_100).root()]).unwrap();
    annotate(
        &e,
        (1..=20u128)
            .map(|t| score(t, None, "q", "a", 1.0))
            .chain([score(99, None, "q", "a", 1.0)])
            .collect(),
    );
    e.flush().unwrap();
    let q = ListQuery {
        annotation: Some(filter("q")),
        ..list_all()
    };
    let snap = e.snapshot().with_annotation_set_cap(5);
    let (rows, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert_eq!(rows.len(), 21);
    // Window 2000 (one match) by its set; window 0 (20 matches, cap 5)
    // scanned once, then 20 exact lookups.
    assert_eq!(stats.annotation_windows_scanned, 2);
    assert_eq!(stats.annotation_exact_lookups, 20);
    // The same rows as with no cap in the way.
    let (uncapped, stats) = e.snapshot().list_traces_with_stats(&q).unwrap();
    assert_eq!(rows, uncapped);
    assert_eq!(stats.annotation_exact_lookups, 0);
    drop(snap);
    e.shutdown().unwrap();
}
