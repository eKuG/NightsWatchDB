//! Defect: a list with an annotation filter costs time proportional to the
//! number of candidate traces with buffered annotations times the number of
//! buffered annotations. Every such candidate takes the exact path, and the
//! exact path (`Snapshot::annotations`) finds the trace's buffered records by
//! reading every buffered annotation (`buffer_annotations_of`). With the
//! default 256 MiB buffer that is about a million annotations, and a filter
//! that few traces match visits every one of those traces: on the order of
//! 10^12 record visits for one query. The README states the cost per trace
//! (proportional to the buffer) but not that a list multiplies it by the
//! number of candidates.
//!
//! Sequence: `n` traces in one span part, one buffered annotation each, and a
//! list whose filter matches none of them, timed for `n` = 1,000 and
//! `n` = 16,000 (16 times more traces and 16 times more buffered
//! annotations).
//!
//! Expected: the time grows about in proportion to `n` (16 times; the test
//! allows 48). Observed: it grows with `n` squared (far above 48 times).

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
    assert!(
        large < small * 48,
        "one list took {small:?} over 1,000 traces with buffered annotations and {large:?} over \
         16,000 ({}x)",
        large.as_secs_f64() / small.as_secs_f64()
    );
}
