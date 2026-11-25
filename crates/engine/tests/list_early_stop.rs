//! `list_traces` goes newest first and stops early. These tests would catch a
//! stop that comes too early, and pin that the work done does not grow with
//! the number of traces.

mod common;

use std::collections::BTreeMap;

use common::*;
use nwdb_core::TraceSummary;
use nwdb_engine::{Engine, ListQuery};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// The model's answer to a list query, from summaries computed once.
fn list_from(sums: &BTreeMap<u128, TraceSummary>, q: &ListQuery) -> Vec<TraceSummary> {
    let mut v: Vec<TraceSummary> = sums
        .values()
        .filter(|s| s.start_ns >= q.from_ns && s.start_ns < q.to_ns)
        .filter(|s| {
            q.service
                .as_ref()
                .is_none_or(|v| s.root_service.as_ref() == Some(v))
        })
        .filter(|s| q.model.as_ref().is_none_or(|m| s.models.contains(m)))
        .filter(|s| q.error.is_none_or(|e| s.error == e))
        .cloned()
        .collect();
    v.sort_by(|a, b| {
        b.start_ns
            .cmp(&a.start_ns)
            .then(a.trace_id.cmp(&b.trace_id))
    });
    v.truncate(q.limit);
    v
}

fn all_time(limit: usize) -> ListQuery {
    ListQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        service: None,
        model: None,
        error: None,
        limit,
    }
}

const TIME: i64 = 8_000;
/// Starts are multiples of this, so many traces and partials tie.
const STEP: i64 = 100;

struct World {
    rng: StdRng,
    next_trace: u128,
    next_span: u64,
    model: Model,
}

impl World {
    fn start(&mut self) -> i64 {
        self.rng.random_range(0..TIME / STEP) * STEP
    }

    fn span(&mut self, trace: u128, start: i64, root: bool) -> TSpan {
        self.next_span += 1;
        let mut s = TSpan::new(trace, self.next_span, start);
        if root {
            s.parent = None;
            // "d" is rare, so a filter on it rejects almost everything.
            s.service = Some(if self.rng.random_bool(0.06) {
                "d".into()
            } else {
                ["a", "b", "c"][self.rng.random_range(0..3)].into()
            });
        }
        if self.rng.random_bool(0.3) {
            s.model = Some(["m1", "m2", "m3"][self.rng.random_range(0..3)].into());
        }
        if self.rng.random_bool(0.04) {
            s.status = 2;
        }
        s.end = start + self.rng.random_range(1..300);
        s
    }

    /// New traces, and late spans for old ones that start anywhere: before
    /// the trace's first span, or far after it.
    fn round(&mut self, e: &Engine, new: usize, late: usize) {
        let mut spans = Vec::new();
        for _ in 0..new {
            self.next_trace += 1;
            let t = self.next_trace;
            let first = self.start();
            spans.push(self.span(t, first, true));
            for _ in 0..self.rng.random_range(0..3) {
                let later = first + self.rng.random_range(0..4) * STEP;
                spans.push(self.span(t, later, false));
            }
        }
        for _ in 0..late {
            if self.next_trace == 0 {
                break;
            }
            let t = self.rng.random_range(1..=self.next_trace);
            let start = self.start();
            spans.push(self.span(t, start, false));
        }
        for chunk in spans.chunks(7) {
            ingest(e, chunk).unwrap();
            self.model.add(chunk);
        }
    }

    fn query(&mut self) -> ListQuery {
        let edge = |rng: &mut StdRng| -> i64 {
            match rng.random_range(0..4) {
                // Exactly on a start time, so the bounds' inclusivity matters.
                0 | 1 => rng.random_range(0..TIME / STEP) * STEP,
                2 => rng.random_range(-100..TIME + 100),
                _ => [i64::MIN, i64::MAX][rng.random_range(0..2)],
            }
        };
        let (a, b) = (edge(&mut self.rng), edge(&mut self.rng));
        let r = &mut self.rng;
        ListQuery {
            from_ns: a.min(b),
            to_ns: a.max(b),
            service: r
                .random_bool(0.35)
                .then(|| ["a", "b", "c", "d", "d", "zzz"][r.random_range(0..6)].to_owned()),
            model: r
                .random_bool(0.25)
                .then(|| ["m1", "m2", "m3"][r.random_range(0..3)].to_owned()),
            error: r.random_bool(0.25).then(|| r.random_bool(0.5)),
            limit: [1, 2, 3, 5, 17, 100_000][r.random_range(0..6)],
        }
    }
}

fn check_queries(w: &mut World, e: &Engine, n: usize, what: &str) {
    let sums = w.model.summaries();
    let snap = e.snapshot();
    for _ in 0..n {
        let q = w.query();
        let got = snap.list_traces(&q).unwrap();
        assert_eq!(got, list_from(&sums, &q), "{what}: {q:?}");
    }
    for limit in [0, 1, 2, 7, 1_000_000] {
        let q = all_time(limit);
        assert_eq!(
            snap.list_traces(&q).unwrap(),
            list_from(&sums, &q),
            "{what}: whole range, limit {limit}"
        );
    }
}

fn randomized(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut w = World {
        rng: StdRng::seed_from_u64(seed),
        next_trace: 0,
        next_span: 0,
        model: Model::default(),
    };
    let mut e = open(opts(dir.path()));
    for round in 0..16 {
        let (new, late) = (w.rng.random_range(10..30), w.rng.random_range(0..12));
        w.round(&e, new, late);
        let what = format!("seed {seed:#x} round {round}");
        // Some queries see the buffer on top of the parts.
        check_queries(&mut w, &e, 6, &format!("{what}, with a buffer"));
        match w.rng.random_range(0..10) {
            0..2 => {}
            2..4 => {
                e.shutdown().unwrap();
                drop(e);
                e = open(opts(dir.path()));
            }
            _ => e.flush().unwrap(),
        }
        check_queries(&mut w, &e, 14, &what);
    }
    assert!(
        e.stats().parts >= 20,
        "the test needs many parts: {}",
        e.stats().parts
    );
    e.shutdown().unwrap();
}

#[test]
fn early_stop_agrees_with_the_model_seed_1() {
    randomized(0x1a2b);
}

#[test]
fn early_stop_agrees_with_the_model_seed_2() {
    randomized(0x3c4d);
}

#[test]
fn early_stop_agrees_with_the_model_seed_3() {
    randomized(0x5e6f);
}

/// A trace whose newest partial is far newer than its oldest: the newest
/// partial is met first, and the trace still has to be placed by its oldest.
#[test]
fn a_trace_moves_down_when_an_older_partial_is_merged() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    // Trace 1 starts at 100 and has a late span at 9_000 (a later window and
    // part). Traces 2..=5 start at 5_000..8_000.
    let mut spans = vec![TSpan::new(1, 1, 100).root()];
    for t in 2..=5u64 {
        spans.push(TSpan::new(t as u128, 10 + t, 3_000 + 1_000 * t as i64).root());
    }
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(1, 99, 9_000)]).unwrap();
    e.flush().unwrap();
    let got = e.list_traces(&all_time(2)).unwrap();
    let ids: Vec<_> = got.iter().map(|s| s.trace_id).collect();
    // Trace 1's newest partial starts at 9_000, but the trace starts at 100.
    assert_eq!(ids, vec![spans[4].trace_id(), spans[3].trace_id()]);
    assert_eq!(e.list_traces(&all_time(10)).unwrap().len(), 5);
    // Cut so that the newest partial is out of the range and the oldest in.
    let q = ListQuery {
        from_ns: 0,
        to_ns: 1_000,
        ..all_time(5)
    };
    let got = e.list_traces(&q).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].start_ns, 100);
    assert_eq!(got[0].span_count, 2);
    // The newest partial is in the range, but the merged start is not.
    let q = ListQuery {
        from_ns: 8_000,
        to_ns: 10_000,
        ..all_time(5)
    };
    let ids: Vec<_> = e
        .list_traces(&q)
        .unwrap()
        .iter()
        .map(|s| s.trace_id)
        .collect();
    assert_eq!(ids, vec![spans[4].trace_id()]);
}

/// Equal starts: the tie goes to the smaller trace id, whichever part the
/// partial comes from and however many tie at the cut.
#[test]
fn equal_starts_are_cut_by_trace_id() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut model = Model::default();
    // 30 traces all starting at 500, in 3 flushes, ids interleaved.
    for round in 0..3u128 {
        let spans: Vec<TSpan> = (0..10u128)
            .map(|i| TSpan::new(1 + i * 3 + (2 - round), (100 * round + i) as u64 + 1, 500).root())
            .collect();
        ingest(&e, &spans).unwrap();
        model.add(&spans);
        e.flush().unwrap();
    }
    let sums = model.summaries();
    for limit in [1, 2, 9, 10, 29, 30, 31] {
        let q = all_time(limit);
        assert_eq!(e.list_traces(&q).unwrap(), list_from(&sums, &q), "{limit}");
    }
}

/// Builds `traces` single-span traces in `flushes` flushes and returns the
/// model.
fn load(e: &Engine, rng: &mut StdRng, traces: usize, flushes: usize, span: i64) -> Model {
    let mut model = Model::default();
    let per = traces / flushes;
    let mut id = 0;
    for _ in 0..flushes {
        let spans: Vec<TSpan> = (0..per)
            .map(|_| {
                id += 1;
                // Distinct starts, in no particular order.
                let start = (id as i64) * 7919 % span;
                let mut s = TSpan::new(id, id as u64, start).root();
                s.service = Some(
                    if rng.random_bool(0.02) {
                        "rare"
                    } else {
                        "common"
                    }
                    .into(),
                );
                s
            })
            .collect();
        for chunk in spans.chunks(500) {
            ingest(e, chunk).unwrap();
        }
        model.add(&spans);
        e.flush().unwrap();
    }
    model
}

/// Parts that do not overlap in time: the newest `limit` come from the newest
/// part and nothing else is read.
#[test]
fn newest_over_disjoint_parts_reads_one_group() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    // 50 windows of 1_000, 120 traces in each, starts on a 20 grid, so ties.
    let mut model = Model::default();
    for w in 0..50i64 {
        let spans: Vec<TSpan> = (0..120i64)
            .map(|i| {
                let id = (w * 120 + i + 1) as u128;
                TSpan::new(id, id as u64, w * 1_000 + (i % 50) * 20).root()
            })
            .collect();
        ingest(&e, &spans).unwrap();
        model.add(&spans);
    }
    e.flush().unwrap();
    // A few traces get a late span in a far window: split across parts.
    let late: Vec<TSpan> = [3u128, 1_000, 3_000, 5_990]
        .iter()
        .map(|&t| TSpan::new(t, 90_000 + t as u64, 49_500))
        .collect();
    ingest(&e, &late).unwrap();
    model.add(&late);
    e.flush().unwrap();

    let snap = e.snapshot();
    let parts = e.stats().parts;
    assert!(parts >= 50, "{parts} parts");
    let sums = model.summaries();
    assert_eq!(sums.len(), 6_000);
    let limit = 10;
    let q = all_time(limit);
    let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert_eq!(got, list_from(&sums, &q));
    eprintln!("disjoint parts, {parts} parts, 6000 traces, limit {limit}: {stats:?}");
    // The old algorithm read every summary row (6004) and made one index
    // lookup per candidate per other part (hundreds of thousands).
    assert!(stats.summary_rows_read <= 4 * limit as u64, "{stats:?}");
    assert!(stats.row_groups_read <= 4, "{stats:?}");
    assert!(stats.split_lookups <= 4 * limit as u64, "{stats:?}");
    assert!(stats.index_lookups <= 4, "{stats:?}");
}

/// Parts that all cover the same time: every part is read, one group each,
/// and a row only when it can be among the newest.
#[test]
fn newest_over_overlapping_parts_is_bounded_by_limit_and_parts() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.window_ns = 1_000_000;
    let e = open(o);
    let mut rng = StdRng::seed_from_u64(7);
    let model = load(&e, &mut rng, 8_000, 40, 500_000);
    let snap = e.snapshot();
    let parts = e.stats().parts;
    assert_eq!(parts, 40);
    let sums = model.summaries();
    for limit in [1, 10, 50] {
        let q = all_time(limit);
        let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
        assert_eq!(got, list_from(&sums, &q));
        eprintln!("overlapping, {parts} parts, 8000 traces, limit {limit}: {stats:?}");
        assert!(stats.row_groups_read <= parts as u64, "{stats:?}");
        assert!(stats.summary_rows_read <= 2 * limit as u64 + 2, "{stats:?}");
        assert!(stats.split_lookups <= 2 * limit as u64 + 2, "{stats:?}");
        assert_eq!(stats.index_lookups, 0, "no trace is split: {stats:?}");
    }

    // A filter that rejects 98% of the traces: the answer is still right,
    // and the scan goes further back, but not past what the limit needs.
    let q = ListQuery {
        service: Some("rare".into()),
        limit: 10,
        ..all_time(10)
    };
    let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert_eq!(got, list_from(&sums, &q));
    assert_eq!(got.len(), 10);
    eprintln!("overlapping, rare service, limit 10: {stats:?}");
    assert!(stats.summary_rows_read < 8_000 / 2, "{stats:?}");

    // A filter nothing satisfies reads to the lower bound, and no further.
    let q = ListQuery {
        service: Some("nobody".into()),
        from_ns: 250_000,
        ..all_time(10)
    };
    let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
    assert!(got.is_empty());
    let in_range = sums.values().filter(|s| s.start_ns >= 250_000).count() as u64;
    assert!(stats.summary_rows_read <= in_range, "{stats:?}");
    assert_eq!(stats.summary_rows_read, in_range, "{stats:?}");

    // Empty limit and empty range do no work.
    for q in [
        all_time(0),
        ListQuery {
            from_ns: 5,
            to_ns: 5,
            ..all_time(5)
        },
    ] {
        let (got, stats) = snap.list_traces_with_stats(&q).unwrap();
        assert!(got.is_empty());
        assert_eq!(stats, Default::default());
    }
}
