//! The split map holds exactly the traces that are in more than one part, and
//! is never out of step with the part list of the snapshot it belongs to.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use nwdb_core::TraceId;
use nwdb_engine::{Engine, Snapshot};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Every trace that is in more than one part of the snapshot, with its parts,
/// worked out by brute force from the parts' indexes.
fn brute_force(snap: &Snapshot) -> Vec<(TraceId, Vec<u64>)> {
    let mut holders: BTreeMap<TraceId, Vec<u64>> = BTreeMap::new();
    for (part, ids) in snap.part_traces() {
        for id in ids {
            holders.entry(id).or_default().push(part);
        }
    }
    holders.into_iter().filter(|(_, p)| p.len() > 1).collect()
}

fn check(e: &Engine, what: &str) -> usize {
    let snap = e.snapshot();
    let want = brute_force(&snap);
    let mut got = snap.split_traces();
    for (_, parts) in &mut got {
        parts.sort_unstable();
    }
    assert_eq!(got, want, "{what}");
    want.len()
}

#[test]
fn the_map_is_exactly_the_split_traces_after_flushes_and_restarts() {
    let mut rng = StdRng::seed_from_u64(0x5911);
    let dir = tempfile::tempdir().unwrap();
    let mut e = open(opts(dir.path()));
    let mut span = 0u64;
    let mut max_split = 0;
    for step in 0..60 {
        let n = rng.random_range(1..40);
        let spans: Vec<TSpan> = (0..n)
            .map(|_| {
                span += 1;
                // Few trace ids and starts over several windows, so that
                // traces are often in several parts, even three or four.
                TSpan::new(rng.random_range(1..=120), span, rng.random_range(0..6_000))
            })
            .collect();
        ingest(&e, &spans).unwrap();
        match rng.random_range(0..10) {
            0..2 => {}
            2..4 => {
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                drop(e);
                e = open(opts(dir.path()));
            }
            _ => e.flush().unwrap(),
        }
        max_split = max_split.max(check(&e, &format!("step {step}")));
    }
    e.flush().unwrap();
    max_split = max_split.max(check(&e, "end"));
    assert!(max_split > 10, "the test needs split traces: {max_split}");
    let tripled = e.snapshot().split_traces();
    assert!(
        tripled.iter().any(|(_, p)| p.len() >= 3),
        "the test needs traces in three parts"
    );
    e.shutdown().unwrap();
}

#[test]
fn an_empty_database_and_a_database_without_split_traces_have_an_empty_map() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(check(&e, "empty"), 0);
    let spans: Vec<TSpan> = (1..=50u128)
        .map(|t| TSpan::new(t, t as u64, (t as i64) * 100))
        .collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    assert!(e.stats().parts > 1);
    assert_eq!(check(&e, "disjoint"), 0);
}

/// While one thread ingests and flushes, snapshots taken by another always
/// hold a map that matches their own part list.
#[test]
fn a_snapshot_never_sees_a_map_that_disagrees_with_its_parts() {
    let dir = tempfile::tempdir().unwrap();
    let e = Arc::new(open(opts(dir.path())));
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (e, done) = (e.clone(), done.clone());
        std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(9);
            let mut span = 0u64;
            for _ in 0..25 {
                let spans: Vec<TSpan> = (0..30)
                    .map(|_| {
                        span += 1;
                        TSpan::new(rng.random_range(1..=60), span, rng.random_range(0..5_000))
                    })
                    .collect();
                ingest(&e, &spans).unwrap();
                e.flush().unwrap();
            }
            done.store(true, Ordering::SeqCst);
        })
    };
    let mut looked = 0;
    let all = list_all();
    while !done.load(Ordering::SeqCst) {
        let snap = e.snapshot();
        let mut got = snap.split_traces();
        for (_, parts) in &mut got {
            parts.sort_unstable();
        }
        assert_eq!(got, brute_force(&snap));
        // A trace is listed once, with all its spans, in the same snapshot.
        let listed = snap.list_traces(&all).unwrap();
        let total: u64 = listed.iter().map(|s| u64::from(s.span_count)).sum();
        let ids: std::collections::HashSet<_> = listed.iter().map(|s| s.trace_id).collect();
        assert_eq!(ids.len(), listed.len());
        for s in listed.iter().take(3) {
            let rows: usize = snap
                .get_trace(s.trace_id)
                .unwrap()
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, s.span_count as usize);
        }
        assert!(total <= 25 * 30);
        looked += 1;
    }
    writer.join().unwrap();
    eprintln!("{looked} snapshots checked while flushing");
    assert!(looked > 0);
}
