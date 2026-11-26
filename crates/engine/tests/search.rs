//! Full-text search through the engine: newest first with ties, the time
//! range and the limit, a span in two parts returned once, what a flush makes
//! searchable, the walk that stops early (with counters), and a restart.

mod common;
mod search_model;

use std::sync::Arc;

use common::*;
use nwdb_core::{Error, SpanId};
use nwdb_engine::textindex::parse;
use nwdb_engine::{SearchQuery, Snapshot};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use search_model::*;

/// Window of the test engine: every part covers 1,000 ns.
const WINDOW: i64 = 1_000;

fn search_opts(dir: &std::path::Path) -> nwdb_engine::EngineOptions {
    let mut o = opts(dir);
    o.splitter = Some(Arc::new(Lines));
    o
}

fn span_with(trace: u128, span: u64, start: i64, lines: &[&str]) -> TSpan {
    let mut s = TSpan::new(trace, span, start);
    s.attrs = lines.join("\n").into_bytes();
    s
}

fn q(text: &str, from: i64, to: i64, limit: usize) -> SearchQuery {
    SearchQuery {
        query: parse(text).unwrap(),
        from_ns: from,
        to_ns: to,
        limit,
    }
}

/// Traces of 1 to 5 spans, a trace starting anywhere in `windows` windows
/// and its spans every 100 ns after, so starts tie a lot and a trace reaches
/// into the next window.
fn world(rng: &mut StdRng, windows: i64, traces: u128, first_trace: u128) -> Vec<TSpan> {
    let mut out = Vec::new();
    for t in 0..traces {
        let trace = first_trace + t;
        let t0 = rng.random_range(0..windows) * WINDOW + 100 * rng.random_range(0..10);
        for k in 0..rng.random_range(1..=5u64) {
            let mut s = TSpan::new(trace, trace as u64 * 10 + k, t0 + 100 * k as i64);
            s.service = Some(format!("svc{}", trace % 3));
            s.operation = (k % 2 == 0).then(|| "chat".to_string());
            s.model = (k % 3 == 0).then(|| "m1".to_string());
            let lines: Vec<String> = (0..rng.random_range(0..4)).map(|_| message(rng)).collect();
            s.attrs = lines.join("\n").into_bytes();
            out.push(s);
        }
    }
    out
}

/// Ingests the spans in chunks, flushing after each, so that a window ends up
/// in several parts.
fn ingest_flushing(e: &nwdb_engine::Engine, spans: &[TSpan], chunk: usize) {
    for c in spans.chunks(chunk) {
        ingest(e, c).unwrap();
        e.flush().unwrap();
    }
}

fn check(snap: &Snapshot, model: &Model, rng: &mut StdRng, n: usize, what: &str) {
    let mut nonempty = 0;
    for i in 0..n {
        let query = gen_query(rng, 3);
        let from = [i64::MIN, 0, 1_500, 3_000][rng.random_range(0..4)];
        let to = [i64::MAX, 2_000, 5_000, 6_500][rng.random_range(0..4)];
        let limit = [1, 3, 10, 1_000][rng.random_range(0..4)];
        let sq = SearchQuery {
            query,
            from_ns: from,
            to_ns: to,
            limit,
        };
        let got = snap.search(&sq).unwrap();
        let want = expected(model.unique(), &sq);
        assert_eq!(got, want, "{what}: query {i}: {sq:?}");
        nonempty += usize::from(!got.is_empty());
    }
    // Agreeing on empty answers alone would prove little.
    assert!(
        nonempty * 4 >= n,
        "{what}: only {nonempty} of {n} queries found spans"
    );
}

#[test]
fn search_matches_a_model_over_many_parts_with_ties_ranges_and_limits() {
    for seed in 0..3u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let dir = tempfile::tempdir().unwrap();
        let e = open(search_opts(dir.path()));
        let spans = world(&mut rng, 8, 60, 1);
        ingest_flushing(&e, &spans, 17);
        let mut model = Model::default();
        model.add(&spans);
        assert!(e.snapshot().part_traces().len() > 8, "several parts");
        check(&e.snapshot(), &model, &mut rng, 120, "live");

        // After a restart.
        e.shutdown().unwrap();
        drop(e);
        let e = open(search_opts(dir.path()));
        check(&e.snapshot(), &model, &mut rng, 60, "after restart");
    }
}

#[test]
fn hits_are_newest_first_ties_by_trace_then_span_and_carry_the_columns() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    let mut spans = Vec::new();
    // Four spans start at 500, in two traces, plus older and newer ones.
    for (trace, span, start) in [
        (2, 7, 500),
        (1, 9, 500),
        (1, 8, 500),
        (2, 6, 500),
        (3, 1, 900),
        (3, 2, 100),
    ] {
        let mut s = span_with(trace, span, start, &["the needle is here"]);
        s.operation = Some("chat".into());
        s.model = Some("gpt".into());
        s.service = Some("api".into());
        spans.push(s);
    }
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    let got = e.search(&q("needle", i64::MIN, i64::MAX, 100)).unwrap();
    let order: Vec<(u128, u64, i64)> = got
        .hits
        .iter()
        .map(|h| {
            (
                u128::from_be_bytes(h.trace_id.0),
                u64::from_be_bytes(h.span_id.0),
                h.start_ns,
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            (3, 1, 900),
            (1, 8, 500),
            (1, 9, 500),
            (2, 6, 500),
            (2, 7, 500),
            (3, 2, 100)
        ]
    );
    let first = &got.hits[0];
    assert_eq!(first.name, "span-1");
    assert_eq!(first.service_name.as_deref(), Some("api"));
    assert_eq!(first.operation.as_deref(), Some("chat"));
    assert_eq!(first.request_model.as_deref(), Some("gpt"));
    assert_eq!(first.end_ns, 910);
    assert_eq!(first.span_id, SpanId(1u64.to_be_bytes()));

    // The time range is [from, to).
    let n = |from, to| e.search(&q("needle", from, to, 100)).unwrap().hits.len();
    assert_eq!(n(500, 501), 4);
    assert_eq!(n(501, 900), 0);
    assert_eq!(n(100, 500), 1);
    assert_eq!(n(0, 901), 6);
    assert_eq!(n(0, 900), 5);
    // The limit keeps the newest, and the tie order decides at the edge.
    let two = e.search(&q("needle", i64::MIN, i64::MAX, 3)).unwrap();
    assert_eq!(two.hits.len(), 3);
    assert_eq!(two.hits[2].span_id, SpanId(9u64.to_be_bytes()));
    // Empty range or limit: nothing, and no error.
    assert!(e.search(&q("needle", 10, 10, 5)).unwrap().hits.is_empty());
    assert!(e.search(&q("needle", 0, 1_000, 0)).unwrap().hits.is_empty());
}

#[test]
fn a_span_in_two_parts_is_returned_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    let spans: Vec<TSpan> = (1..=6u64)
        .map(|k| span_with(1, k, 100 * k as i64, &["a retried span of a trace"]))
        .collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    // The sender retries the same spans, and the retry lands after a flush.
    ingest(&e, &spans[..4]).unwrap();
    e.flush().unwrap();
    ingest(&e, &spans[2..]).unwrap();
    e.flush().unwrap();
    assert!(e.snapshot().part_traces().len() >= 3);
    let got = e.search(&q("retried", i64::MIN, i64::MAX, 100)).unwrap();
    let ids: Vec<u64> = got
        .hits
        .iter()
        .map(|h| u64::from_be_bytes(h.span_id.0))
        .collect();
    assert_eq!(ids, [6, 5, 4, 3, 2, 1]);
    // The limit counts spans, not copies.
    assert_eq!(
        e.search(&q("retried", i64::MIN, i64::MAX, 4))
            .unwrap()
            .hits
            .len(),
        4
    );
}

#[test]
fn only_flushed_parts_are_searched_and_the_answer_says_how_far() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    let before = e.search(&q("needle", i64::MIN, i64::MAX, 10)).unwrap();
    assert!(before.hits.is_empty());
    assert_eq!(before.searched_through_lsn, 0);

    ingest(&e, &[span_with(1, 1, 100, &["first needle here"])]).unwrap();
    e.flush().unwrap();
    let first = e.search(&q("needle", i64::MIN, i64::MAX, 10)).unwrap();
    assert_eq!(first.hits.len(), 1);
    assert_eq!(first.searched_through_lsn, e.stats().flushed_lsn);
    assert!(first.searched_through_lsn >= 1);

    // Acknowledged, in the buffer, not yet flushed: not found, and the
    // position says why.
    ingest(&e, &[span_with(2, 1, 200, &["second needle here"])]).unwrap();
    let held = e.search(&q("needle", i64::MIN, i64::MAX, 10)).unwrap();
    assert_eq!(held.hits.len(), 1);
    assert_eq!(held.searched_through_lsn, first.searched_through_lsn);
    assert!(e.stats().acked_lsn > held.searched_through_lsn);

    // A snapshot taken before the flush does not see the part the flush
    // writes; one taken after does.
    let old = e.snapshot();
    e.flush().unwrap();
    assert_eq!(
        old.search(&q("needle", i64::MIN, i64::MAX, 10))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(old.searched_through_lsn(), first.searched_through_lsn);
    let now = e.search(&q("needle", i64::MIN, i64::MAX, 10)).unwrap();
    assert_eq!(now.hits.len(), 2);
    assert_eq!(now.searched_through_lsn, e.stats().acked_lsn);
}

#[test]
fn a_query_over_a_limit_is_an_invalid_argument_and_the_engine_is_unharmed() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    // 1,100 distinct words with one prefix: more than a prefix may expand to.
    let spans: Vec<TSpan> = (0..1_100u64)
        .map(|i| {
            span_with(
                1 + i as u128,
                1,
                100,
                &[&format!("line with word zzz{i:04} in it")],
            )
        })
        .collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    let broad = SearchQuery {
        query: parse("zzz*").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit: 5,
    };
    match e.search(&broad) {
        Err(Error::InvalidArgument(m)) => {
            assert!(m.contains("prefix") && m.contains("1024"), "{m}")
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    assert_eq!(
        e.search(&q("zzz0007", i64::MIN, i64::MAX, 5))
            .unwrap()
            .hits
            .len(),
        1
    );
}

#[test]
fn the_walk_stops_when_older_parts_cannot_matter() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    // Twelve windows, each its own part of five spans that all match.
    let mut spans = Vec::new();
    let mut model = Model::default();
    for w in 0..12i64 {
        for k in 0..5u64 {
            let s = span_with(
                1 + w as u128,
                1 + k,
                w * WINDOW + 100 * k as i64,
                &["needle in a haystack"],
            );
            spans.push(s);
        }
    }
    model.add(&spans);
    ingest_flushing(&e, &spans, 5);
    let snap = e.snapshot();
    assert_eq!(snap.part_traces().len(), 12);

    let run = |limit: usize, from: i64, to: i64| {
        let sq = q("needle", from, to, limit);
        let (hits, stats) = snap.search_with_stats(&sq).unwrap();
        assert_eq!(hits, expected(model.unique(), &sq), "limit {limit}");
        stats
    };
    // The newest part holds five spans: a limit of 3 is answered from it.
    let s = run(3, i64::MIN, i64::MAX);
    assert_eq!((s.parts_searched, s.parts_skipped_early), (1, 11), "{s:?}");
    assert_eq!(s.rows_matched, 5);
    // Reading the rows of one part is all the I/O: not twelve.
    assert!(s.rows_read <= 5, "{s:?}");
    // A limit of 7 needs the two newest parts, and a third is ruled out.
    let s = run(7, i64::MIN, i64::MAX);
    assert_eq!((s.parts_searched, s.parts_skipped_early), (2, 10), "{s:?}");
    // Everything is asked for: every part is searched.
    let s = run(1_000, i64::MIN, i64::MAX);
    assert_eq!((s.parts_searched, s.parts_skipped_early), (12, 0), "{s:?}");
    // A time range rules parts out by their bounds before any is searched.
    let s = run(1_000, 3 * WINDOW, 5 * WINDOW);
    assert_eq!(s.parts_searched, 2, "{s:?}");
    assert_eq!(s.parts_out_of_range, 10, "{s:?}");
}

#[test]
fn the_stop_is_strict_a_tie_with_the_worst_hit_can_still_win() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    // Part 1 (window 0): trace 9 with a span at 1500 (crossing into window 1).
    // Part 2 (window 1): trace 1 with a span at 1500. Limit 1: the answer is
    // the span of trace 1, which sorts first on trace id, although the part
    // that holds it is the one found second when the bounds tie.
    ingest(
        &e,
        &[
            span_with(9, 1, 0, &["needle"]),
            span_with(9, 2, 1_500, &["needle needle"]),
        ],
    )
    .unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_with(1, 1, 1_500, &["needle needle"])]).unwrap();
    e.flush().unwrap();
    let got = e.search(&q("needle", i64::MIN, i64::MAX, 1)).unwrap();
    assert_eq!(got.hits.len(), 1);
    assert_eq!(got.hits[0].trace_id.0, 1u128.to_be_bytes());
}

#[test]
fn search_agrees_with_the_model_for_texts_the_parser_accepts() {
    let mut rng = StdRng::seed_from_u64(99);
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    let spans = world(&mut rng, 6, 50, 1);
    ingest_flushing(&e, &spans, 23);
    let mut model = Model::default();
    model.add(&spans);
    let snap = e.snapshot();
    for text in [
        "error timeout",
        "error OR retry",
        "alpha -beta",
        "\"go go\"",
        "\"user assistant\" -paris",
        "zq00*",
        "(alpha OR beta) (gamma OR delta)",
    ] {
        for limit in [2, 50] {
            let sq = q(text, i64::MIN, i64::MAX, limit);
            assert_eq!(
                snap.search(&sq).unwrap(),
                expected(model.unique(), &sq),
                "{text}"
            );
        }
    }
    let words: Vec<&str> = WORDS.to_vec();
    let w = words.choose(&mut rng).unwrap();
    assert!(
        !snap
            .search(&q(w, i64::MIN, i64::MAX, 5))
            .unwrap()
            .is_empty()
    );
}

fn starts(e: &nwdb_engine::Engine, limit: usize) -> Vec<i64> {
    let q = SearchQuery {
        query: parse("needle").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit,
    };
    e.search(&q)
        .unwrap()
        .hits
        .iter()
        .map(|h| h.start_ns)
        .collect()
}

/// The walk visits parts newest bound first and stops once the limit is held
/// and the next part's bound is below the worst held start. The copy of a
/// span that arrived first must be the one returned all the same: with a
/// small limit, with room, and after the window is merged.
#[test]
fn a_span_in_two_parts_is_the_earlier_copy_whatever_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    o.splitter = Some(Arc::new(Lines));
    let e = open(o);
    // The same trace id and span id twice, in two flushes of one window; the
    // second copy has a later start.
    let mut first = TSpan::new(1, 1, 100);
    first.attrs = b"the needle is here".to_vec();
    let mut again = first.clone();
    again.start = 500;
    again.end = 510;
    ingest(&e, &[first]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[again]).unwrap();
    e.flush().unwrap();

    let with_room = starts(&e, 10);
    assert_eq!(with_room, [100], "the copy of the earlier arrival");
    let one = starts(&e, 1);
    compact_fully(&e);
    let merged = starts(&e, 1);
    assert_eq!(merged, [100], "a merge keeps the earlier copy");
    assert_eq!(one, merged, "the limit changes nothing");
}

/// Spans sent again with other text and starts, in other flushes and so in
/// other parts and windows: every search agrees with a model that keeps the
/// copy that arrived first, with ties, ranges and limits, before and after
/// the merges and after a restart.
#[test]
fn search_matches_a_model_when_spans_are_sent_again_with_other_content() {
    for seed in 0..3u64 {
        let mut rng = StdRng::seed_from_u64(100 + seed);
        let dir = tempfile::tempdir().unwrap();
        let mut o = compact_opts(dir.path());
        o.splitter = Some(Arc::new(Lines));
        let e = open(o);
        let spans = world(&mut rng, 4, 40, 1);
        let mut model = Model::default();
        model.add(&spans);
        ingest_flushing(&e, &spans, 11);
        for round in 0..3 {
            let again: Vec<TSpan> = spans
                .iter()
                .filter(|_| rng.random_bool(0.3))
                .map(|s| {
                    let mut c = s.clone();
                    c.name = format!("again-{round}");
                    if rng.random_bool(0.5) {
                        c.start = (c.start - rng.random_range(0..=200)).max(0);
                        c.end = c.start + 10;
                    }
                    let lines: Vec<String> = (0..rng.random_range(0..3))
                        .map(|_| message(&mut rng))
                        .collect();
                    c.attrs = lines.join("\n").into_bytes();
                    c
                })
                .collect();
            model.add(&again);
            ingest_flushing(&e, &again, 7);
        }
        check(&e.snapshot(), &model, &mut rng, 100, "before any merge");
        e.run_maintenance().unwrap();
        check(&e.snapshot(), &model, &mut rng, 60, "after one pass");
        compact_fully(&e);
        check(
            &e.snapshot(),
            &model,
            &mut rng,
            60,
            "after the final merges",
        );

        e.shutdown().unwrap();
        drop(e);
        let mut o = compact_opts(dir.path());
        o.splitter = Some(Arc::new(Lines));
        let e = open(o);
        check(&e.snapshot(), &model, &mut rng, 60, "after a restart");
        e.shutdown().unwrap();
    }
}
