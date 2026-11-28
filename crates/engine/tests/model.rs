//! A long random sequence of ingests, flushes, restarts and queries, compared
//! with a brute-force model after every step.

mod common;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;
use nwdb_core::TraceSummary;
use nwdb_engine::{AggQuery, Annotation, ListQuery};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

const SEED: u64 = 0x4e57_4442;
const STEPS: usize = 200;
const TRACES: u128 = 40;
const TIME: i64 = 8_000;

struct Gen {
    /// Large values that spans reuse, whole or in part, so that parts hold
    /// repeated pieces.
    docs: Vec<Vec<u8>>,
    next_span: u64,
    rooted: HashSet<u128>,
    /// The id each trace's spans mostly carry, so most traces agree.
    session: std::collections::HashMap<u128, String>,
}

impl Gen {
    /// A request of one to four spans. With `floor`, a trace lives in one
    /// window, `trace % 8`, and only windows from `floor` on are used: the
    /// spans of a trace are then never spread over windows, so what
    /// compaction and retention do to a window can be modelled.
    fn request(&mut self, rng: &mut StdRng, floor: Option<i64>) -> Vec<TSpan> {
        let n = rng.random_range(1..=4);
        (0..n)
            .map(|_| {
                let (trace, start) = match floor {
                    None => (rng.random_range(1..=TRACES), rng.random_range(0..TIME)),
                    Some(f) => {
                        let t = loop {
                            let t = rng.random_range(1..=TRACES);
                            if (t % 8) as i64 >= f {
                                break t;
                            }
                        };
                        (t, (t % 8) as i64 * 1_000 + rng.random_range(0..900))
                    }
                };
                self.next_span += 1;
                let mut s = TSpan::new(trace, self.next_span, start);
                // The first span of a trace is its root; now and then a
                // second root appears (a malformed trace).
                if self.rooted.insert(trace) || rng.random_bool(0.03) {
                    s.parent = None;
                }
                let home = self
                    .session
                    .entry(trace)
                    .or_insert_with(|| format!("s{}", rng.random_range(0..4)))
                    .clone();
                s.session = match rng.random_range(0..10) {
                    0 => None,
                    1 => Some(format!("s{}", rng.random_range(0..4))),
                    _ => Some(home),
                };
                s.user = match rng.random_range(0..10) {
                    0 => None,
                    1 => Some(format!("u{}", rng.random_range(0..4))),
                    _ => Some(format!("u{}", trace % 4)),
                };
                s.end = start + rng.random_range(1..200);
                s.service = [None, Some("a"), Some("b"), Some("c")][rng.random_range(0..4)]
                    .map(str::to_owned);
                s.model = [None, Some("m1"), Some("m2"), Some("m3")][rng.random_range(0..4)]
                    .map(str::to_owned);
                s.operation =
                    [None, Some("chat"), Some("embed")][rng.random_range(0..3)].map(str::to_owned);
                if rng.random_bool(0.6) {
                    s.input = Some(rng.random_range(0..1000));
                }
                if rng.random_bool(0.6) {
                    s.output = Some(rng.random_range(0..1000));
                }
                if rng.random_bool(0.05) {
                    s.status = 2;
                }
                if rng.random_bool(0.05) {
                    s.error_type = Some("e".into());
                }
                // Mostly a prefix of one of a few large documents, sometimes
                // with a tail of its own, sometimes small or empty.
                s.attrs = match rng.random_range(0..10) {
                    0 => Vec::new(),
                    1 => vec![7; rng.random_range(1..64)],
                    _ => {
                        let doc = &self.docs[rng.random_range(0..self.docs.len())];
                        let mut v = doc[..rng.random_range(1..=doc.len())].to_vec();
                        if rng.random_bool(0.3) {
                            v.extend(self.next_span.to_le_bytes().repeat(8));
                        }
                        v
                    }
                };
                s
            })
            .collect()
    }
}

/// The spans sent again with other content: the same trace, span id, start
/// and end, but another name, model, token counts and text. The engine keeps
/// the copy that arrived first, and so does the model, so a resend that kept
/// the wrong copy shows in `get_trace`, and in every summary once the copies
/// are merged.
fn conflicting(spans: &[TSpan], tag: u64) -> Vec<TSpan> {
    spans
        .iter()
        .cloned()
        .map(|mut s| {
            s.name = format!("again-{tag}");
            s.model = Some("m9".into());
            s.input = Some(tag as i64);
            s.output = Some(1);
            s.attrs.extend_from_slice(b"a copy with other content");
            s
        })
        .collect()
}

fn random_list(rng: &mut StdRng) -> ListQuery {
    let a = rng.random_range(-100..TIME + 100);
    let b = rng.random_range(-100..TIME + 100);
    ListQuery {
        from_ns: a.min(b),
        to_ns: a.max(b),
        service: rng
            .random_bool(0.3)
            .then(|| ["a", "b", "c"][rng.random_range(0..3)].to_owned()),
        model: rng
            .random_bool(0.3)
            .then(|| ["m1", "m2", "m3"][rng.random_range(0..3)].to_owned()),
        error: rng.random_bool(0.3).then(|| rng.random_bool(0.5)),
        annotation: None,
        limit: rng.random_range(1..12),
    }
}

fn open_random(dir: &std::path::Path, rng: &mut StdRng) -> (nwdb_engine::Engine, bool) {
    let mut o = opts(dir);
    let background = rng.random_bool(0.3);
    if background {
        // Flushes now also happen in the background, between steps.
        o.flush_interval = Duration::from_millis(15);
    }
    (open(o), background)
}

#[test]
fn random_operations_agree_with_the_model() {
    let mut rng = StdRng::seed_from_u64(SEED);
    let dir = tempfile::tempdir().unwrap();
    let (mut e, mut background) = open_random(dir.path(), &mut rng);
    let mut model = Model::default();
    let mut gen_ = Gen {
        docs: (0..5)
            .map(|_| {
                let mut v = vec![0u8; rng.random_range(500..4_000)];
                rng.fill_bytes(&mut v);
                v
            })
            .collect(),
        next_span: 0,
        rooted: HashSet::new(),
        session: Default::default(),
    };
    // The last request sent, while a retry of it would still land in the same
    // buffers. A flush or restart makes a retry cross into a part, where the
    // engine documents it is counted twice, so those clear it. A retry carries
    // other content: the copy that arrived first must stay.
    let mut retryable: Option<Vec<TSpan>> = None;
    let (mut flushes, mut reopens, mut queries) = (0, 0, 0);

    for step in 0..STEPS {
        let what = format!("step {step}");
        match rng.random_range(0..100) {
            0..55 => {
                let mut spans = gen_.request(&mut rng, None);
                if rng.random_bool(0.15) {
                    // A retried request: the same spans sent again.
                    if let Some(prev) = retryable.take().filter(|_| !background) {
                        spans = conflicting(&prev, step as u64);
                    }
                } else if rng.random_bool(0.1) {
                    // A duplicate inside one request.
                    let again = conflicting(&spans[..1], step as u64);
                    spans.extend(again);
                }
                ingest(&e, &spans).unwrap();
                model.add(&spans);
                retryable = Some(spans);
            }
            55..67 => {
                e.flush().unwrap();
                retryable = None;
                flushes += 1;
            }
            67..75 => {
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                drop(e);
                (e, background) = open_random(dir.path(), &mut rng);
                retryable = None;
                reopens += 1;
            }
            _ => {
                for _ in 0..3 {
                    let q = random_list(&mut rng);
                    assert_eq!(e.list_traces(&q).unwrap(), model.list(&q), "{what}: {q:?}");
                    let a = rng.random_range(-100..TIME + 100);
                    let b = rng.random_range(-100..TIME + 100);
                    let q = AggQuery {
                        from_ns: a.min(b),
                        to_ns: a.max(b),
                        operation: rng
                            .random_bool(0.4)
                            .then(|| ["chat", "embed"][rng.random_range(0..2)].to_owned()),
                    };
                    assert_eq!(
                        e.aggregate_tokens(&q).unwrap(),
                        model.agg(&q),
                        "{what}: {q:?}"
                    );
                    queries += 1;
                }
            }
        }
        model.check(&e, &what);
    }
    eprintln!(
        "{STEPS} steps: {} spans, {flushes} explicit flushes, {reopens} restarts, {queries} \
         random queries, {} parts at the end, {} background flushes in the last run",
        model.spans.len(),
        e.stats().parts,
        e.stats().flushes,
    );
    e.shutdown().unwrap();
}

/// The same walk with compaction and retention: the maintenance thread
/// merges now and then, explicit passes merge at chosen moments, a clock the
/// test moves expires windows, and spans are sent again after a flush. Until
/// a window is merged such a span is counted twice by the listing (the known
/// limit), and the resent copy has other content than the first one, which
/// must be the one every query shows; the model says so exactly: `get_trace`
/// is always right, the
/// listing is right for every other trace and not below the model for these,
/// and after a flush and a merge of everything every query is exact.
#[test]
fn random_operations_with_compaction_and_retention_agree_with_the_model() {
    const WINDOWS: i64 = 8;
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xc0);
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let open_c = |rng: &mut StdRng| {
        let mut o = compact_opts(dir.path());
        let background = rng.random_bool(0.4);
        if background {
            o.flush_interval = Duration::from_millis(15);
        }
        // Whether the thread merges as well as the explicit passes.
        o.compaction.background = rng.random_bool(0.5);
        o.retention = Some(Duration::from_nanos(1_000));
        let clock = now.clone();
        o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
        (open(o), background)
    };
    let (mut e, mut background) = open_c(&mut rng);
    let mut model = Model::default();
    let mut gen_ = Gen {
        docs: (0..5)
            .map(|_| {
                let mut v = vec![0u8; rng.random_range(500..4_000)];
                rng.fill_bytes(&mut v);
                v
            })
            .collect(),
        next_span: 0,
        rooted: HashSet::new(),
        session: Default::default(),
    };
    let mut floor = 0i64;
    // Requests sent so far with the epoch they were sent in; a flush or a
    // restart starts a new epoch, so a resend of an older request crosses
    // into a part.
    let mut epoch = 0u64;
    let mut sent: Vec<(Vec<TSpan>, u64)> = Vec::new();
    let mut unmerged: BTreeSet<u128> = BTreeSet::new();
    let (mut merges, mut dropped, mut resends, mut retired) = (0u64, 0u64, 0u64, 0u64);
    let tally = |e: &nwdb_engine::Engine, merges: &mut u64, dropped: &mut u64| {
        let s = e.stats();
        *merges += s.merges_done;
        *dropped += s.duplicate_spans_dropped;
    };

    for step in 0..300 {
        let what = format!("step {step}");
        match rng.random_range(0..100) {
            0..45 => {
                let mut spans = gen_.request(&mut rng, Some(floor));
                // Resends cross a flush only where the walk controls the
                // flushes: the background flusher would make the epoch a guess.
                if !background && rng.random_bool(0.2) && !sent.is_empty() {
                    let (old, at) = sent[rng.random_range(0..sent.len())].clone();
                    if old.iter().all(|s| s.start / 1_000 >= floor) {
                        if at != epoch {
                            unmerged.extend(old.iter().map(|s| s.trace));
                            resends += 1;
                        }
                        spans = conflicting(&old, step as u64);
                    }
                } else if rng.random_bool(0.1) {
                    let again = conflicting(&spans[..1], step as u64);
                    spans.extend(again);
                }
                ingest(&e, &spans).unwrap();
                model.add(&spans);
                sent.push((spans, epoch));
            }
            45..57 => {
                e.flush().unwrap();
                epoch += 1;
            }
            57..65 => {
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                tally(&e, &mut merges, &mut dropped);
                drop(e);
                (e, background) = open_c(&mut rng);
                epoch += 1;
            }
            65..75 => {
                // Merges only what is in parts: spans resent into the buffer
                // stay unmerged.
                e.run_maintenance().unwrap();
            }
            75..81 => {
                e.flush().unwrap();
                epoch += 1;
                compact_fully(&e);
                assert_eq!(e.stats().windows_with_many_parts, 0, "{what}");
                assert!(e.stats().parts as i64 <= WINDOWS, "{what}");
                unmerged.clear();
            }
            81..85 if floor < 6 => {
                // The clock moves past the end of window `floor`: with a
                // retention of one window, windows below the new floor expire.
                floor += 1;
                now.store((floor + 1) * 1_000, Ordering::SeqCst);
                e.flush().unwrap();
                epoch += 1;
                e.run_maintenance().unwrap();
                model.spans.retain(|s| s.start >= floor * 1_000);
                unmerged.retain(|t| (*t % 8) as i64 >= floor);
                retired += 1;
            }
            _ => {
                if unmerged.is_empty() {
                    for _ in 0..3 {
                        let q = random_list(&mut rng);
                        assert_eq!(e.list_traces(&q).unwrap(), model.list(&q), "{what}: {q:?}");
                    }
                }
            }
        }
        // `unmerged` also holds traces whose duplicates sit in the buffer
        // next to their parts (an engine counts those twice as well).
        model.check_snapshot_ignoring(&e.snapshot(), &what, &unmerged);
    }
    tally(&e, &mut merges, &mut dropped);
    eprintln!(
        "300 steps: {} spans, {merges} merges, {dropped} duplicates dropped by merges, {resends} \
         resends across a flush, {retired} retention steps, {} parts at the end",
        model.spans.len(),
        e.stats().parts,
    );
    assert!(
        merges > 3 && resends > 0,
        "{merges} merges, {resends} resends"
    );
    // And one last time, everything merged: exact.
    e.flush().unwrap();
    compact_fully(&e);
    model.check(&e, "at the end");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    model.check(&e, "after the last restart");
    e.shutdown().unwrap();
}

/// The winners of every identity, by the documented rule: the last annotation
/// of an identity wins, and a delete leaves nothing.
type Identity = (u128, Option<u64>, String, String);

fn model_shown(model: &BTreeMap<Identity, Annotation>, trace: u128) -> Vec<Annotation> {
    model
        .range((trace, None, String::new(), String::new())..)
        .take_while(|(k, _)| k.0 == trace)
        .map(|(_, a)| a.clone())
        .filter(|a| !a.delete)
        .collect()
}

fn same_annotation(a: &Annotation, b: &Annotation) -> bool {
    Annotation {
        arrival: 0,
        ..a.clone()
    } == Annotation {
        arrival: 0,
        ..b.clone()
    }
}

/// Spans, annotations (overwrites and deletes), flushes and restarts at
/// random: after every step every trace's annotations, and a few listings
/// with an annotation filter, equal a brute-force model.
#[test]
fn random_operations_with_annotations_agree_with_the_model() {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xa7);
    let dir = tempfile::tempdir().unwrap();
    let (mut e, mut _background) = open_random(dir.path(), &mut rng);
    let mut model = Model::default();
    let mut gen_ = Gen {
        docs: vec![vec![1u8; 300]],
        next_span: 0,
        rooted: HashSet::new(),
        session: Default::default(),
    };
    let mut ann: BTreeMap<Identity, Annotation> = BTreeMap::new();
    let (mut annotated, mut flushes, mut reopens, mut listed) = (0, 0, 0, 0);

    for step in 0..250 {
        let what = format!("step {step}");
        match rng.random_range(0..100) {
            0..25 => {
                let spans = gen_.request(&mut rng, None);
                ingest(&e, &spans).unwrap();
                model.add(&spans);
            }
            25..60 => {
                // A request of annotations; some traces may be unknown.
                let known: Vec<u128> = model.trace_ids().into_iter().collect();
                if known.is_empty() {
                    continue;
                }
                let n = rng.random_range(1..5);
                let items: Vec<Annotation> = (0..n)
                    .map(|_| {
                        let trace = if rng.random_bool(0.05) {
                            TRACES + 5
                        } else {
                            known[rng.random_range(0..known.len())]
                        };
                        let span = [None, Some(1), Some(2), Some(3)][rng.random_range(0..4)];
                        let name = ["a", "b", "c"][rng.random_range(0..3)];
                        let source = ["", "s1", "s2"][rng.random_range(0..3)];
                        let mut a = score(trace, span, name, source, 0.0);
                        a.time_ns = rng.random_range(0..100);
                        match rng.random_range(0..10) {
                            0..2 => {
                                a.value = None;
                                a.delete = true;
                            }
                            2..4 => {
                                a.value = None;
                                a.label = Some(["x", "y"][rng.random_range(0..2)].into());
                            }
                            4..6 => {
                                a.value = Some(f64::from(rng.random_range(0..10)));
                                a.comment = Some("c".into());
                            }
                            _ => a.value = Some(f64::from(rng.random_range(0..10))),
                        }
                        a
                    })
                    .collect();
                let r = annotate(&e, items.clone());
                let rejected: HashSet<usize> = r.rejected.iter().map(|x| x.0).collect();
                for (i, a) in items.into_iter().enumerate() {
                    let t = u128::from_be_bytes(a.trace_id.0);
                    assert_eq!(rejected.contains(&i), t == TRACES + 5, "{what}: item {i}");
                    if !rejected.contains(&i) {
                        let key = (
                            t,
                            a.span_id.map(|s| u64::from_be_bytes(s.0)),
                            a.name.clone(),
                            a.source.clone(),
                        );
                        ann.insert(key, a);
                        annotated += 1;
                    }
                }
            }
            60..72 => {
                e.flush().unwrap();
                flushes += 1;
            }
            72..80 => {
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                drop(e);
                (e, _background) = open_random(dir.path(), &mut rng);
                reopens += 1;
            }
            _ => {
                for _ in 0..3 {
                    let mut q = random_list(&mut rng);
                    let limit = q.limit;
                    q.limit = usize::MAX;
                    let mut f = filter(["a", "b", "c", "zz"][rng.random_range(0..4)]);
                    if rng.random_bool(0.3) {
                        f.source = Some(["", "s1", "s2"][rng.random_range(0..3)].into());
                    }
                    if rng.random_bool(0.4) {
                        f.min = Some(f64::from(rng.random_range(0..10)));
                    }
                    if rng.random_bool(0.4) {
                        f.max = Some(f64::from(rng.random_range(0..10)));
                    }
                    if rng.random_bool(0.2) {
                        f.label = Some(["x", "y"][rng.random_range(0..2)].into());
                    }
                    let matches = |t: u128| {
                        model_shown(&ann, t).iter().any(|a| {
                            a.name == f.name
                                && f.source.as_ref().is_none_or(|s| *s == a.source)
                                && f.min.is_none_or(|m| a.value.is_some_and(|v| v >= m))
                                && f.max.is_none_or(|m| a.value.is_some_and(|v| v <= m))
                                && f.label.as_ref().is_none_or(|l| a.label.as_ref() == Some(l))
                        })
                    };
                    let want: Vec<TraceSummary> = model
                        .list(&q)
                        .into_iter()
                        .filter(|s| matches(u128::from_be_bytes(s.trace_id.0)))
                        .take(limit)
                        .collect();
                    q.limit = limit;
                    q.annotation = Some(f.clone());
                    assert_eq!(e.list_traces(&q).unwrap(), want, "{what}: {q:?}");
                    listed += 1;
                }
            }
        }
        model.check(&e, &what);
        for t in 1..=TRACES {
            let got = shown(&e, t);
            let want = model_shown(&ann, t);
            assert_eq!(got.len(), want.len(), "{what}: trace {t}");
            for (g, w) in got.iter().zip(&want) {
                assert!(same_annotation(g, w), "{what}: trace {t}: {g:?} vs {w:?}");
            }
        }
    }
    eprintln!(
        "250 steps: {annotated} annotations, {flushes} flushes, {reopens} restarts, {listed} \
         filtered listings, {} annotation parts at the end",
        e.stats().annotation_parts
    );
    assert!(annotated > 50 && listed > 5);
    e.shutdown().unwrap();
}

/// Spans, annotations (overwrites and deletes), flushes, restarts, merge
/// passes and retention at random, with the clock moving past one window at a
/// time: after every step every trace's annotations, the spans, and a few
/// filtered listings equal a brute-force model. A trace lives in one window
/// (`trace % 8`), so the model can say what expiry takes: a window's spans and
/// the annotations of its traces go together, and nothing of them comes back.
/// Not modelled: a trace spread over windows (its annotations live as long as
/// the window that ends last among those that held the trace when they were
/// flushed), which the
/// engine tests cover with fixed cases.
#[test]
fn random_operations_with_annotations_compaction_and_retention_agree_with_the_model() {
    const WINDOWS: i64 = 8;
    let mut rng = StdRng::seed_from_u64(SEED ^ 0xa8);
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let open_c = |rng: &mut StdRng| {
        let mut o = compact_opts(dir.path());
        if rng.random_bool(0.3) {
            o.flush_interval = Duration::from_millis(15);
        }
        // Whether the thread merges as well as the explicit passes.
        o.compaction.background = rng.random_bool(0.5);
        o.retention = Some(Duration::from_nanos(1_000));
        let clock = now.clone();
        o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
        open(o)
    };
    let mut e = open_c(&mut rng);
    let mut model = Model::default();
    let mut gen_ = Gen {
        docs: vec![vec![1u8; 300]],
        next_span: 0,
        rooted: HashSet::new(),
        session: Default::default(),
    };
    let mut ann: BTreeMap<Identity, Annotation> = BTreeMap::new();
    let mut floor = 0i64;
    let (mut annotated, mut merges, mut superseded, mut retired, mut listed) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let tally = |e: &nwdb_engine::Engine, merges: &mut u64, superseded: &mut u64| {
        let s = e.stats();
        *merges += s.merges_done;
        *superseded += s.annotations_superseded;
    };

    for step in 0..300 {
        let what = format!("step {step}");
        match rng.random_range(0..100) {
            0..25 => {
                let spans = gen_.request(&mut rng, Some(floor));
                ingest(&e, &spans).unwrap();
                model.add(&spans);
            }
            25..60 => {
                let known: Vec<u128> = model.trace_ids().into_iter().collect();
                if known.is_empty() {
                    continue;
                }
                let items: Vec<Annotation> = (0..rng.random_range(1..5))
                    .map(|_| {
                        let trace = known[rng.random_range(0..known.len())];
                        let span = [None, Some(1), Some(2)][rng.random_range(0..3)];
                        let name = ["a", "b"][rng.random_range(0..2)];
                        let source = ["", "s1"][rng.random_range(0..2)];
                        let mut a = score(trace, span, name, source, 0.0);
                        match rng.random_range(0..10) {
                            0..3 => {
                                a.value = None;
                                a.delete = true;
                            }
                            3..5 => {
                                a.value = None;
                                a.label = Some(["x", "y"][rng.random_range(0..2)].into());
                            }
                            _ => a.value = Some(f64::from(rng.random_range(0..10))),
                        }
                        a
                    })
                    .collect();
                let r = annotate(&e, items.clone());
                assert!(r.rejected.is_empty(), "{what}: {:?}", r.rejected);
                for a in items {
                    let key = (
                        u128::from_be_bytes(a.trace_id.0),
                        a.span_id.map(|s| u64::from_be_bytes(s.0)),
                        a.name.clone(),
                        a.source.clone(),
                    );
                    ann.insert(key, a);
                    annotated += 1;
                }
            }
            60..68 => e.flush().unwrap(),
            68..74 => {
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                tally(&e, &mut merges, &mut superseded);
                drop(e);
                e = open_c(&mut rng);
            }
            74..80 => e.run_maintenance().unwrap(),
            80..86 => {
                e.flush().unwrap();
                compact_fully(&e);
                let s = e.stats();
                assert_eq!(s.windows_with_many_parts, 0, "{what}");
                assert!(s.parts as i64 <= WINDOWS, "{what}");
                assert!(s.annotation_parts as i64 <= WINDOWS, "{what}");
            }
            86..90 if floor < 6 => {
                // The clock moves past the end of window `floor`: with a
                // retention of one window, the windows below the new floor
                // expire.
                floor += 1;
                now.store((floor + 1) * 1_000, Ordering::SeqCst);
                e.flush().unwrap();
                e.run_maintenance().unwrap();
                model.spans.retain(|s| s.start >= floor * 1_000);
                ann.retain(|k, _| (k.0 % 8) as i64 >= floor);
                retired += 1;
            }
            _ => {
                for _ in 0..3 {
                    let mut q = random_list(&mut rng);
                    let limit = q.limit;
                    q.limit = usize::MAX;
                    let mut f = filter(["a", "b", "zz"][rng.random_range(0..3)]);
                    if rng.random_bool(0.3) {
                        f.source = Some(["", "s1"][rng.random_range(0..2)].into());
                    }
                    if rng.random_bool(0.4) {
                        f.min = Some(f64::from(rng.random_range(0..10)));
                    }
                    if rng.random_bool(0.2) {
                        f.label = Some(["x", "y"][rng.random_range(0..2)].into());
                    }
                    let matches = |t: u128| {
                        model_shown(&ann, t).iter().any(|a| {
                            a.name == f.name
                                && f.source.as_ref().is_none_or(|s| *s == a.source)
                                && f.min.is_none_or(|m| a.value.is_some_and(|v| v >= m))
                                && f.label.as_ref().is_none_or(|l| a.label.as_ref() == Some(l))
                        })
                    };
                    let want: Vec<TraceSummary> = model
                        .list(&q)
                        .into_iter()
                        .filter(|s| matches(u128::from_be_bytes(s.trace_id.0)))
                        .take(limit)
                        .collect();
                    q.limit = limit;
                    q.annotation = Some(f.clone());
                    assert_eq!(e.list_traces(&q).unwrap(), want, "{what}: {q:?}");
                    listed += 1;
                }
            }
        }
        model.check(&e, &what);
        for t in 1..=TRACES {
            let got = shown(&e, t);
            let want = model_shown(&ann, t);
            assert_eq!(got.len(), want.len(), "{what}: trace {t}");
            for (g, w) in got.iter().zip(&want) {
                assert!(same_annotation(g, w), "{what}: trace {t}: {g:?} vs {w:?}");
            }
        }
    }
    tally(&e, &mut merges, &mut superseded);
    eprintln!(
        "300 steps: {annotated} annotations, {merges} merges, {superseded} annotation records \
         superseded by merges, {retired} retention steps, {listed} filtered listings, {} \
         annotation parts at the end",
        e.stats().annotation_parts,
    );
    assert!(annotated > 100 && merges > 3 && retired > 0 && listed > 5);
    assert!(superseded > 0, "no merge dropped an overwritten record");
    // And one last time, everything merged: exact, also after a restart.
    e.flush().unwrap();
    compact_fully(&e);
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    model.check(&e, "after the last restart");
    for t in 1..=TRACES {
        let got = shown(&e, t);
        let want = model_shown(&ann, t);
        assert_eq!(got.len(), want.len(), "after the last restart: trace {t}");
    }
    e.shutdown().unwrap();
}
