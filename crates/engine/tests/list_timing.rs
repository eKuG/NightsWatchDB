//! A rough timing of "the newest 50 traces over the whole range" on a few
//! hundred thousand generated traces in about two hundred parts, some of them
//! split. Run it by hand, in whichever profile is wanted:
//! `cargo test -p nwdb-engine --test list_timing -- --ignored --nocapture`.

mod common;

use std::time::Instant;

use common::*;
use nwdb_engine::ListQuery;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[test]
#[ignore = "a timing, not a check; run by hand"]
fn newest_fifty_over_many_parts() {
    const TRACES: usize = 300_000;
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // 100 windows of 3_000 traces.
    o.window_ns = 1_000;
    let e = open(o);
    let mut rng = StdRng::seed_from_u64(5);
    let t0 = Instant::now();
    for chunk in 0..TRACES / 1_000 {
        let spans: Vec<TSpan> = (0..1_000)
            .map(|i| {
                let id = (chunk * 1_000 + i + 1) as u128;
                TSpan::new(id, id as u64, (id as i64) / 3).root()
            })
            .collect();
        ingest(&e, &spans).unwrap();
    }
    e.flush().unwrap();
    // 1% of the traces get a late span in another window.
    let late: Vec<TSpan> = (0..TRACES / 100)
        .map(|i| {
            let t = rng.random_range(1..=TRACES as u128);
            TSpan::new(t, (TRACES + i + 1) as u64, rng.random_range(0..100_000))
        })
        .collect();
    ingest(&e, &late).unwrap();
    e.flush().unwrap();
    eprintln!(
        "built {TRACES} traces in {} parts in {:?}",
        e.stats().parts,
        t0.elapsed()
    );

    let snap = e.snapshot();
    let q = ListQuery {
        limit: 50,
        ..list_all()
    };
    for run in 0..3 {
        let t = Instant::now();
        let got = snap.list_traces(&q).unwrap();
        eprintln!("list_traces newest 50, run {run}: {:?}", t.elapsed());
        assert_eq!(got.len(), 50);
    }
}
