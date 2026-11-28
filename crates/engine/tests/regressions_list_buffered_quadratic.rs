//! A list with an annotation filter makes one pass over the buffered
//! annotations per query, however many candidate traces have buffered
//! annotations: the time of the list grows in proportion to the traces and
//! the buffered annotations, not with their product. The buffered records of
//! a trace are found by lookup in an index built once (`buffer_index`), and
//! `Snapshot::annotations_many` gives the annotations of a page of traces
//! with one pass over the buffer.
//!
//! Sequence: `n` traces in one span part, one buffered annotation each, and a
//! list whose filter matches none of them, timed for `n` = 1,000 and
//! `n` = 16,000 (16 times more traces and 16 times more buffered
//! annotations).
//!
//! Guaranteed: the time grows about in proportion to `n` (16 times; the test
//! allows 48).

mod common;

use std::time::{Duration, Instant};

use common::*;
use nwdb_engine::{Annotation, ListQuery};

fn list_time(n: u128) -> Duration {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut t = 1;
    while t <= n {
        let end = (t + 999).min(n);
        let spans: Vec<TSpan> = (t..=end)
            .map(|k| TSpan::new(k, 1, 100 + (k % 800) as i64).root())
            .collect();
        ingest(&e, &spans).unwrap();
        t = end + 1;
    }
    e.flush().unwrap();
    let mut t = 1;
    while t <= n {
        let end = (t + 999).min(n);
        let items: Vec<Annotation> = (t..=end).map(|k| score(k, None, "x", "s", 1.0)).collect();
        annotate(&e, items);
        t = end + 1;
    }
    assert_eq!(e.stats().annotations_buffered, n as usize);
    let q = ListQuery {
        annotation: Some(filter("y")),
        ..list_all()
    };
    let snap = e.snapshot();
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let started = Instant::now();
        let (rows, stats) = snap.list_traces_with_stats(&q).unwrap();
        best = best.min(started.elapsed());
        assert!(rows.is_empty());
        assert_eq!(stats.annotation_exact_lookups, n as u64);
    }
    drop(snap);
    e.shutdown().unwrap();
    best
}

#[test]
fn a_filtered_list_over_buffered_annotations_is_not_quadratic() {
    let small = list_time(1_000);
    let large = list_time(16_000);
    eprintln!("{small:?} for 1,000, {large:?} for 16,000");
    assert!(
        large < small * 48,
        "one list took {small:?} over 1,000 traces with buffered annotations and {large:?} over \
         16,000 ({}x)",
        large.as_secs_f64() / small.as_secs_f64()
    );
}

#[test]
fn annotations_many_gives_what_annotations_gives_for_each_trace() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let spans: Vec<TSpan> = (1..=6u128).map(|k| TSpan::new(k, 1, 100).root()).collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    // Parts and buffer both: trace 1 has three annotations (one overwritten
    // in the buffer), trace 2 two, trace 3 a deleted one, trace 4 one only in
    // the buffer, trace 5 none.
    annotate(
        &e,
        vec![
            score(1, None, "a", "s", 1.0),
            score(1, None, "b", "s", 2.0),
            score(2, None, "a", "s", 3.0),
            score(3, None, "a", "s", 4.0),
        ],
    );
    e.flush().unwrap();
    annotate(
        &e,
        vec![
            score(1, None, "a", "s", 9.0),
            score(1, Some(1), "c", "s", 5.0),
            score(2, None, "b", "s", 6.0),
            delete(3, None, "a", "s"),
            score(4, None, "a", "s", 7.0),
        ],
    );
    let snap = e.snapshot();
    let ids: Vec<nwdb_core::TraceId> = [5u128, 1, 4, 3, 2, 1, 6].iter().map(|&t| tid(t)).collect();
    for limit in [usize::MAX, 2, 1, 0] {
        let many = snap.annotations_many(&ids, limit).unwrap();
        assert_eq!(many.len(), ids.len());
        for (id, got) in ids.iter().zip(&many) {
            assert_eq!(got, &snap.annotations(*id, limit).unwrap(), "limit {limit}");
        }
    }
    let all = snap.annotations_many(&ids, usize::MAX).unwrap();
    assert_eq!(all[1].0.len(), 3);
    assert_eq!(all[1].0[0].value, Some(9.0));
    assert!(all[3].0.is_empty(), "the delete hides trace 3's annotation");
    assert!(snap.annotations_many(&[], 5).unwrap().is_empty());
    drop(snap);
    e.shutdown().unwrap();
}
