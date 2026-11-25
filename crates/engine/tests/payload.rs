//! Large values are stored once per part and read back unchanged: before a
//! flush, after it and after a restart.

mod common;

use std::collections::BTreeMap;

use common::*;
use nwdb_core::TraceId;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

/// Spans of a few traces whose attributes are a shared prefix plus a growing
/// private part, in two time windows (so two parts).
fn spans() -> Vec<TSpan> {
    let mut rng = StdRng::seed_from_u64(77);
    let mut shared = vec![0u8; 2_048];
    rng.fill_bytes(&mut shared);
    let mut out = Vec::new();
    let mut span = 0;
    for window in 0..2i64 {
        for trace in 1..=3u128 {
            let mut conversation = Vec::new();
            for call in 0..10i64 {
                span += 1;
                let mut more = vec![0u8; 512];
                rng.fill_bytes(&mut more);
                conversation.extend(more);
                let mut s = TSpan::new(trace + 10 * window as u128, span, window * 1_000 + call);
                if call == 0 {
                    s = s.root();
                }
                s.attrs = [shared.clone(), conversation.clone()].concat();
                out.push(s);
            }
        }
    }
    out
}

fn read_all(e: &nwdb_engine::Engine, spans: &[TSpan]) -> BTreeMap<u128, Vec<TSpan>> {
    let mut out = BTreeMap::new();
    for s in spans {
        out.entry(s.trace).or_insert_with(|| {
            let batches = e
                .get_trace(TraceId(s.trace.to_be_bytes()))
                .unwrap()
                .expect("the trace is there");
            from_batches(&batches)
        });
    }
    out
}

#[test]
fn traces_read_back_identical_before_a_flush_after_it_and_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let all = spans();
    let mut model = Model::default();

    let e = open(opts(dir.path()));
    for chunk in all.chunks(7) {
        ingest(&e, chunk).unwrap();
        model.add(chunk);
    }
    let before = read_all(&e, &all);
    for (trace, got) in &before {
        assert_eq!(*got, model.get_trace(*trace), "trace {trace} in memory");
    }
    let stats = e.stats();
    assert_eq!(
        (
            stats.payload_logical_bytes,
            stats.payload_unique_bytes,
            stats.payload_stored_bytes
        ),
        (0, 0, 0)
    );

    e.flush().unwrap();
    let stats = e.stats();
    assert_eq!(stats.parts, 2);
    eprintln!(
        "engine payload: logical {} unique {} stored {}",
        stats.payload_logical_bytes, stats.payload_unique_bytes, stats.payload_stored_bytes
    );
    assert!(stats.payload_logical_bytes > 0);
    // The saving is sharing: the distinct text is a fraction of what the rows
    // refer to. Stored bytes alone would also fall under compression, which
    // this text (random bytes) does not offer, so the assertion is on
    // `unique`, and stored cannot be more than a little over it.
    assert!(
        stats.payload_unique_bytes * 3 < stats.payload_logical_bytes,
        "{stats:?}"
    );
    assert!(
        stats.payload_stored_bytes <= stats.payload_unique_bytes + stats.payload_unique_bytes / 8,
        "{stats:?}"
    );
    assert_eq!(read_all(&e, &all), before, "after the flush");
    model.check(&e, "after the flush");
    e.shutdown().unwrap();
    drop(e);

    // From the parts on disk, found through the manifest.
    let e = open(opts(dir.path()));
    assert_eq!(read_all(&e, &all), before, "after the restart");
    model.check(&e, "after the restart");
    let again = e.stats();
    assert_eq!(again.payload_logical_bytes, stats.payload_logical_bytes);
    assert_eq!(again.payload_stored_bytes, stats.payload_stored_bytes);
    assert_eq!(again.payload_unique_bytes, stats.payload_unique_bytes);
    e.shutdown().unwrap();
}

#[test]
fn a_part_without_a_splitter_stores_nothing_out_of_line() {
    let dir = tempfile::tempdir().unwrap();
    let all = spans();
    let mut o = opts(dir.path());
    o.splitter = None;
    let e = open(o);
    ingest(&e, &all).unwrap();
    let before = read_all(&e, &all);
    e.flush().unwrap();
    let stats = e.stats();
    assert_eq!(
        (
            stats.payload_logical_bytes,
            stats.payload_unique_bytes,
            stats.payload_stored_bytes
        ),
        (0, 0, 0)
    );
    assert_eq!(read_all(&e, &all), before);
    e.shutdown().unwrap();
}
