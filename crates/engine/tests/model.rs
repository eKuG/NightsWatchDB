//! A long random sequence of ingests, flushes, restarts and queries, compared
//! with a brute-force model after every step.

mod common;

use std::collections::HashSet;
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
    fn request(&mut self, rng: &mut StdRng) -> Vec<TSpan> {
        let n = rng.random_range(1..=4);
        (0..n)
            .map(|_| {
                let trace = rng.random_range(1..=TRACES);
                self.next_span += 1;
                let start = rng.random_range(0..TIME);
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
    // engine documents it is counted twice, so those clear it.
    let mut retryable: Option<Vec<TSpan>> = None;
    let (mut flushes, mut reopens, mut queries) = (0, 0, 0);

    for step in 0..STEPS {
        let what = format!("step {step}");
        match rng.random_range(0..100) {
            0..55 => {
                let mut spans = gen_.request(&mut rng);
                if rng.random_bool(0.15) {
                    // A retried request: the same spans sent again.
                    if let Some(prev) = retryable.take().filter(|_| !background) {
                        spans = prev;
                    }
                } else if rng.random_bool(0.1) {
                    // A duplicate inside one request.
                    let first = spans[0].clone();
                    spans.push(first);
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
