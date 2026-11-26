//! Tests of search that need the engine's internals: which files a search
//! touches, and searches running while flushes and merges replace parts. Like
//! the replace tests, they reach the replace operation, which has no public
//! caller yet.

// The replace tests include the same support file; one more copy is simpler
// than sharing it across two modules that other work edits.
#[allow(clippy::duplicate_mod)]
#[path = "../tests/common/mod.rs"]
mod common;
#[path = "../tests/search_model/mod.rs"]
mod search_model;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::*;
use nwdb_core::part_dir_name;
use nwdb_segment::{MergeOptions, PartReader, WriterOptions, merge_parts};
use nwdb_textindex::parse;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use search_model::*;

use crate::engine::LivePart;
use crate::lock;
use crate::{Engine, SearchQuery};

fn search_opts(dir: &std::path::Path) -> crate::EngineOptions {
    let mut o = opts(dir);
    o.splitter = Some(Arc::new(Lines));
    o
}

fn span_with(trace: u128, span: u64, start: i64, text: &str) -> TSpan {
    let mut s = TSpan::new(trace, span, start);
    s.attrs = text.as_bytes().to_vec();
    s
}

fn blocks_loaded(snap: &crate::Snapshot) -> u64 {
    snap.vis
        .parts
        .iter()
        .map(|p| p.reader.payload_blocks_loaded())
        .sum()
}

#[test]
fn a_search_reads_no_piece_block_unless_a_phrase_needs_one() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    for t in 1..=4u128 {
        let spans: Vec<TSpan> = (1..=3u64)
            .map(|k| {
                span_with(
                    t,
                    k,
                    t as i64 * 1_000 + k as i64,
                    "the quick brown fox\njumps over the lazy dog again",
                )
            })
            .collect();
        ingest(&e, &spans).unwrap();
        e.flush().unwrap();
    }
    let snap = e.snapshot();
    assert_eq!(snap.vis.parts.len(), 4);
    for text in [
        "fox",
        "fox dog",
        "quick -cat",
        "ju* OR lazy",
        "absent OR brown",
    ] {
        let sq = SearchQuery {
            query: parse(text).unwrap(),
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            limit: 100,
        };
        assert!(!snap.search(&sq).unwrap().is_empty() || text.contains("absent"));
    }
    // The index answered; the rows' eight columns came from Parquet; no raw
    // column was rebuilt and no piece was read.
    assert_eq!(blocks_loaded(&snap), 0);

    // A phrase has to confirm its candidates against their text.
    let sq = SearchQuery {
        query: parse("\"quick brown fox\"").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit: 100,
    };
    assert_eq!(snap.search(&sq).unwrap().len(), 12);
    assert!(blocks_loaded(&snap) > 0);
}

/// Merges the two oldest live parts into one with `merge_parts`, and swaps
/// them in. False if there are fewer than two.
fn merge_oldest_two(e: &Engine) -> bool {
    let inner = e.inner();
    let parts = lock(&inner.core).vis.parts.clone();
    if parts.len() < 2 {
        return false;
    }
    let srcs = &parts[..2];
    let id = inner.manifest.reserve_part_ids(1).unwrap().start;
    let start = srcs.iter().map(|p| p.meta.window_start_ns).min().unwrap();
    let end = srcs
        .iter()
        .map(|p| p.meta.window_start_ns + p.meta.window_ns)
        .max()
        .unwrap();
    let readers: Vec<&PartReader> = srcs.iter().map(|p| &p.reader).collect();
    let (meta, _) = merge_parts(
        &readers,
        &inner.parts_dir,
        id,
        start,
        end - start,
        MergeOptions {
            writer: WriterOptions::default(),
            ..MergeOptions::default()
        },
    )
    .unwrap();
    let reader = PartReader::open(inner.parts_dir.join(part_dir_name(id))).unwrap();
    let remove: Vec<u64> = srcs.iter().map(|p| p.meta.part_id).collect();
    inner
        .replace_parts(&remove, vec![LivePart::new(meta, reader)])
        .unwrap();
    true
}

#[test]
fn searches_during_flushes_and_merges_see_a_consistent_prefix_of_the_log() {
    const BATCHES: usize = 40;
    let mut rng = StdRng::seed_from_u64(5);
    // Batch b holds three traces of two spans each; from the fourth batch on,
    // it also repeats the first spans of the batch three before it (a sender's
    // retry that lands after a flush).
    let mut batches: Vec<Vec<TSpan>> = Vec::new();
    for b in 0..BATCHES {
        let mut spans = Vec::new();
        for t in 0..3u128 {
            let trace = b as u128 * 3 + t + 1;
            for k in 0..2u64 {
                let text = (0..rng.random_range(1..4))
                    .map(|_| message(&mut rng))
                    .collect::<Vec<_>>()
                    .join("\n");
                spans.push(span_with(
                    trace,
                    trace as u64 * 10 + k,
                    (b % 8) as i64 * 1_000 + 100 * (t as i64 + k as i64),
                    &text,
                ));
            }
        }
        if b >= 3 {
            let earlier: Vec<TSpan> = batches[b - 3][..2].to_vec();
            spans.extend(earlier);
        }
        batches.push(spans);
    }
    let batches = Arc::new(batches);

    let dir = tempfile::tempdir().unwrap();
    let e = open(search_opts(dir.path()));
    let stop = Arc::new(AtomicBool::new(false));
    let searches = Arc::new(AtomicUsize::new(0));
    let merges = Arc::new(AtomicUsize::new(0));

    let mut searchers = Vec::new();
    for r in 0..4u64 {
        let (e, stop, batches, searches) =
            (e.clone(), stop.clone(), batches.clone(), searches.clone());
        searchers.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(100 + r);
            while !stop.load(Ordering::SeqCst) {
                let sq = SearchQuery {
                    query: gen_query(&mut rng, 3),
                    from_ns: i64::MIN,
                    to_ns: i64::MAX,
                    limit: [5, 1_000][rng.random_range(0..2)],
                };
                // One snapshot: what it searched is the log up to its
                // position, whatever flushes and merges happen meanwhile.
                let snap = e.snapshot();
                let through = snap.searched_through_lsn() as usize;
                let got = snap.search(&sq).unwrap();
                let mut seen = HashSet::new();
                let spans = batches[..through]
                    .iter()
                    .flatten()
                    .filter(|s| seen.insert((s.trace, s.span)));
                let want = expected(spans, &sq);
                assert_eq!(got, want, "through lsn {through}: {sq:?}");
                searches.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for (b, batch) in batches.iter().enumerate() {
        ingest(&e, batch).unwrap();
        // Records are numbered from 1 in order: the model relies on it.
        assert_eq!(e.stats().acked_lsn, b as u64 + 1);
        if b % 3 == 2 {
            e.flush().unwrap();
        }
        if b % 6 == 5 && merge_oldest_two(&e) {
            merges.fetch_add(1, Ordering::SeqCst);
        }
    }
    e.flush().unwrap();
    // Let the searchers run against the final state too.
    let floor = searches.load(Ordering::SeqCst);
    while searches.load(Ordering::SeqCst) < floor + 8 {
        std::thread::yield_now();
    }
    stop.store(true, Ordering::SeqCst);
    for s in searchers {
        s.join().unwrap();
    }
    assert!(merges.load(Ordering::SeqCst) >= 3, "{merges:?}");
    assert!(searches.load(Ordering::SeqCst) >= 20);

    // And at the end everything is searchable, the same after a restart.
    let all = SearchQuery {
        query: parse("alpha OR beta OR gamma OR error OR user OR tool OR go").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit: 1_000,
    };
    let mut seen = HashSet::new();
    let model: Vec<&TSpan> = batches
        .iter()
        .flatten()
        .filter(|s| seen.insert((s.trace, s.span)))
        .collect();
    let want = expected(model.iter().copied(), &all);
    assert!(!want.is_empty());
    let found = e.search(&all).unwrap();
    assert_eq!(found.hits, want);
    assert_eq!(found.searched_through_lsn, BATCHES as u64);
    e.shutdown().unwrap();
    drop(e);
    let e = open(search_opts(dir.path()));
    assert_eq!(e.search(&all).unwrap().hits, want);
}
