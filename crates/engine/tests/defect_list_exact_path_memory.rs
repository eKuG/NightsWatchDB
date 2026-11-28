//! Defect: the exact path of the list's annotation filter collects every
//! winning annotation of a candidate trace into memory
//! (`self.annotations(id, usize::MAX)` in `list_traces_with_stats`) before it
//! tests them, so the memory of one list query grows with the number of
//! annotations of one trace. Nothing bounds that number: each request holds
//! up to 1000 items, and a sender can send as many requests for one trace as
//! it likes. The README promises a list holds the results, the ids seen, one
//! summary row group per part and the buffer's summaries.
//!
//! Sequence: trace 1 is split over two span parts (windows 0 and 2000), so a
//! list with an annotation filter takes the exact path for it. It gets
//! `n` annotations, all flushed into parts, the first of which (in identity
//! order) matches the filter. The peak heap of one list query is measured for
//! `n` = 1,000 and `n` = 40,000.
//!
//! Expected: the same peak, within 1 MiB (the test can stop at the first
//! match, and needs one block per annotation part either way). Observed: the
//! peak grows by several MiB with the annotations of the trace.

mod common;

use common::*;
use nwdb_engine::{Annotation, ListQuery};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};

#[global_allocator]
static ALLOC: Counting = Counting;

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
    let small = list_peak(1_000);
    let large = list_peak(40_000);
    assert!(
        large < small + (1 << 20),
        "one list query peaked {small} bytes above the start with 1,000 annotations on the \
         trace and {large} bytes with 40,000"
    );
}
