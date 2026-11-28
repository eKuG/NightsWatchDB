//! Defect: whether a list with an annotation filter answers or fails depends
//! on which path its candidates take. The fast path builds the set of every
//! matching trace of a window and fails the whole query with
//! `InvalidArgument` once that set passes its cap (1,048,576 ids; lowered
//! here through `with_annotation_set_cap`), whatever the query's `limit`. The
//! exact path has no such cap, so the same traces with the same winning
//! annotations are listed when they take the exact path. The set's size is
//! the number of annotated traces in one window, which senders control: once
//! a window holds more matching traces than the cap, every list whose filter
//! they match fails, even with a limit of 1.
//!
//! Sequence: traces 1 and 2 in window 0, each annotated "q" and flushed. Each
//! gets a second "q" annotation of another source, left in the buffer, so
//! both take the exact path: a list with limit 1 and a cap of 1. After a
//! flush both take the fast path: the same list.
//!
//! Expected: both lists return trace 2 (the newest match). Observed: the
//! exact path answers, the fast path fails with "more than 1 traces of one
//! window match".

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
