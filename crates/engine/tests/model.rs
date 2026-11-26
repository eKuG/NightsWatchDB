//! A long random sequence of ingests, flushes, restarts and queries, compared
//! with a brute-force model after every step.

mod common;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;
use nwdb_engine::{AggQuery, ListQuery};
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
