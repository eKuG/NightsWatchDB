//! `list_traces` reads a part's summary a row group at a time, and a group of
//! more than 8,192 rows comes back as several batches. A `limit` that reaches
//! back from the newest batch into an older one, and a `to` bound that cuts
//! away rows at the newest end, are checked against the brute-force answer.
//! The work counters check that rows newer than the bound are skipped before
//! anything is looked up for them.

mod common;

use common::*;
use nwdb_engine::{Engine, ListQuery};

/// Options with one window for the whole test, so that a flush writes one part.
fn one_window(dir: &std::path::Path) -> nwdb_engine::EngineOptions {
    let mut o = opts(dir);
    o.window_ns = 1 << 40;
    o
}

fn ingest_all(e: &Engine, spans: &[TSpan]) {
    for chunk in spans.chunks(1_000) {
        ingest(e, chunk).unwrap();
    }
}

/// The brute-force answer for traces of one span each, as (start, trace id):
/// the newest first, ties by trace id. The model's summaries cost a pass over
/// every span for every trace, too slow for thousands of traces in a debug
/// build.
fn brute(spans: &[TSpan], q: &ListQuery) -> Vec<(i64, nwdb_core::TraceId)> {
    let mut v: Vec<_> = spans
        .iter()
        .filter(|s| s.start >= q.from_ns && s.start < q.to_ns)
        .map(|s| (s.start, s.trace_id()))
        .collect();
    v.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    v.truncate(q.limit);
    v
}

fn query(to_ns: i64, limit: usize) -> ListQuery {
    ListQuery {
        to_ns,
        limit,
        ..list_all()
    }
}

/// One part of 8,300 traces: its summary is one row group of two batches (8,192
/// rows and 108), the newest traces in the second.
#[test]
fn a_limit_that_reaches_into_the_older_batch_agrees_with_brute_force() {
    const TRACES: u64 = 8_300;
    let dir = tempfile::tempdir().unwrap();
    let e = open(one_window(dir.path()));
    let spans: Vec<TSpan> = (1..=TRACES)
        .map(|i| TSpan::new(u128::from(i), i, i as i64 * 10).root())
        .collect();
    ingest_all(&e, &spans);
    e.flush().unwrap();

    let snap = e.snapshot();
    let parts = snap.part_traces();
    assert_eq!(parts.len(), 1);
    assert!(parts[0].1.len() > 8_192, "{} traces", parts[0].1.len());

    // The newest 108 traces are in the second batch. `to` values that cut away
    // none of them, some of them, all of them and some of the older batch.
    let start_of = |i: u64| i as i64 * 10;
    let tos = [
        i64::MAX,
        start_of(TRACES - 50) + 1,
        start_of(TRACES - 107),
        start_of(TRACES - 108),
        start_of(TRACES - 200),
    ];
    for to in tos {
        for limit in [1, 107, 109, 150, 8_300] {
            let q = query(to, limit);
            let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
            let got: Vec<_> = got.iter().map(|s| (s.start_ns, s.trace_id)).collect();
            assert_eq!(got, brute(&spans, &q), "to {to}, limit {limit}");
            // The summary is one group, read once.
            assert!(stats.row_groups_read <= 1, "{stats:?}");
        }
    }
    e.shutdown().unwrap();
}

/// Part 1 holds 300 old traces and 2,000 new ones, in one summary group; part 2
/// holds a second span of each new trace, so each of them is split across the
/// two parts. A query whose `to` excludes the new traces must not pay for
/// them: a row newer than the bound is skipped before the split map or a
/// part's index is asked about its trace.
#[test]
fn rows_newer_than_the_bound_are_skipped_before_anything_is_looked_up() {
    const OLD: u64 = 300;
    const NEW: u64 = 2_000;
    const TO: i64 = 50_000;
    let dir = tempfile::tempdir().unwrap();
    let e = open(one_window(dir.path()));
    let mut model = Model::default();

    let mut first: Vec<TSpan> = (1..=OLD)
        .map(|i| TSpan::new(u128::from(i), i, 1_000 + i as i64).root())
        .collect();
    first.extend(
        (1..=NEW).map(|i| TSpan::new(u128::from(OLD + i), OLD + i, 100_000 + i as i64).root()),
    );
    ingest_all(&e, &first);
    model.add(&first);
    e.flush().unwrap();

    let second: Vec<TSpan> = (1..=NEW)
        .map(|i| TSpan::new(u128::from(OLD + i), 10_000 + i, 105_000 + i as i64))
        .collect();
    ingest_all(&e, &second);
    model.add(&second);
    e.flush().unwrap();

    let snap = e.snapshot();
    assert_eq!(snap.part_traces().len(), 2);
    assert_eq!(snap.split_traces().len() as u64, NEW);

    for limit in [1, 10, 50] {
        let q = query(TO, limit);
        let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
        assert_eq!(got, model.list(&q), "limit {limit}");
        assert_eq!(got.len(), limit);
        // Only the traces that can be in the answer are looked up: a few more
        // than the limit at most. Without the skip, every one of the 2,000 new
        // traces would have been, each with its other part's index.
        let bound = 2 * limit as u64;
        assert!(stats.split_lookups <= bound, "{stats:?}");
        assert!(stats.index_lookups <= bound, "{stats:?}");
    }

    // Without a bound the new traces are the answer and are looked up.
    let q = query(i64::MAX, 10);
    let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert_eq!(got, model.list(&q));
    assert!(
        stats.split_lookups >= 10 && stats.index_lookups >= 10,
        "{stats:?}"
    );
    e.shutdown().unwrap();
}
