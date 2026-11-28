//! Several ingest threads and query threads running across many flushes.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use nwdb_core::TraceId;
use nwdb_engine::AggQuery;
use nwdb_wal::{SyncMode, WalOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const INGESTERS: usize = 4;
const RECORDS: usize = 80;
const TRACES: u128 = 30;

#[test]
fn ingest_query_and_flush_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // Real syncs, small buffers and a short age limit: flushes of every kind
    // happen all through the run.
    o.wal = WalOptions {
        sync: SyncMode::Data,
        sync_interval: Duration::from_millis(1),
        ..WalOptions::default()
    };
    o.buffer_bytes = 120_000;
    o.max_buffered_bytes = 400_000;
    o.flush_interval = Duration::from_millis(20);
    let e = open(o);

    // Every trace gets its root first, so each has exactly one.
    let mut model = Model::default();
    let roots: Vec<TSpan> = (1..=TRACES)
        .map(|t| TSpan::new(t, 1, 1_000 * t as i64).root())
        .collect();
    ingest(&e, &roots).unwrap();
    model.add(&roots);

    let stop = Arc::new(AtomicBool::new(false));
    let queries = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::new();
    for r in 0..3u64 {
        let (e, stop, queries) = (e.clone(), stop.clone(), queries.clone());
        readers.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(r);
            let mut seen_before = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let snap = e.snapshot();
                let t = rng.random_range(1..=TRACES);
                let spans = snap
                    .get_trace(TraceId(t.to_be_bytes()))
                    .unwrap()
                    .map(|b| from_batches(&b))
                    .unwrap_or_default();
                assert!(
                    spans
                        .windows(2)
                        .all(|w| (w[0].start, w[0].span) < (w[1].start, w[1].span))
                );
                let listed = snap.list_traces(&list_all()).unwrap();
                let agg = snap
                    .aggregate_tokens(&AggQuery {
                        from_ns: i64::MIN,
                        to_ns: i64::MAX,
                        operation: None,
                    })
                    .unwrap();
                // Within one snapshot every span is counted exactly once, so
                // two different queries must agree.
                let by_list: u64 = listed.iter().map(|s| u64::from(s.span_count)).sum();
                let by_agg: u64 = agg.iter().map(|a| a.count).sum();
                assert_eq!(by_list, by_agg, "a snapshot counted spans differently");
                // Acknowledged data never disappears.
                assert!(by_list >= seen_before, "spans vanished");
                seen_before = by_list;
                queries.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    let flusher = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                e.flush().unwrap();
                std::thread::sleep(Duration::from_millis(15));
            }
        })
    };

    let writers: Vec<_> = (0..INGESTERS)
        .map(|w| {
            let e = e.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(100 + w as u64);
                let mut mine = Vec::new();
                for n in 0..RECORDS {
                    let spans: Vec<TSpan> = (0..rng.random_range(1..4))
                        .map(|k| {
                            let t = rng.random_range(1..=TRACES);
                            let id = ((w as u64 + 1) << 32) | ((n * 4 + k) as u64 + 10);
                            let mut s = TSpan::new(t, id, rng.random_range(0..40_000));
                            s.model = Some(format!("m{}", rng.random_range(0..3)));
                            s.input = Some(rng.random_range(0..100));
                            s.output = Some(rng.random_range(0..100));
                            s.attrs = vec![1; rng.random_range(0..2_000)];
                            s
                        })
                        .collect();
                    ingest_retry(&e, &spans).unwrap();
                    mine.extend(spans);
                }
                mine
            })
        })
        .collect();
    for w in writers {
        model.add(&w.join().unwrap());
    }
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        r.join().unwrap();
    }
    flusher.join().unwrap();

    e.flush().unwrap();
    let st = e.stats();
    eprintln!(
        "{} spans, {} flushes, {} parts, {} reader iterations",
        model.spans.len(),
        st.flushes,
        st.parts,
        queries.load(Ordering::SeqCst)
    );
    assert!(st.flushes > 3, "the run did not flush often enough");
    assert_eq!(st.buffered_bytes, 0);
    model.check(&e, "after the run");
    e.shutdown().unwrap();

    let e = open(opts(dir.path()));
    model.check(&e, "after restart");
    e.shutdown().unwrap();
}
