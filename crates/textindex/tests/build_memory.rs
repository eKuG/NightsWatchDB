//! Building is bounded by bytes, not by input size: with a small segment
//! budget and far more text than the budget, the heap in use while building
//! stays within a fixed multiple of the budget, and a larger budget gives a
//! proportionally larger peak (so the budget is what bounds it).
//!
//! The text is generated document by document and the output goes to a
//! writer that only counts, so what is measured is the builder.

#[path = "support/alloc.rs"]
mod alloc;
mod support;

use std::io::Write;

use nwdb_textindex::{IndexBuilder, IndexOptions};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

#[global_allocator]
static ALLOC: alloc::Counting = alloc::Counter;

/// The allocator counts the whole process, so the two tests take turns.
static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct CountingSink(u64);

impl Write for CountingSink {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 += b.len() as u64;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A word for a rank, with no vocabulary table: the same rank is always the
/// same word.
fn word(rank: u64, out: &mut Vec<u8>) {
    let mut h = rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let len = 3 + (h % 8) as usize;
    for _ in 0..len {
        h = h
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push(b'a' + ((h >> 33) % 26) as u8);
    }
}

/// Builds from `docs` documents of `words` words each; returns the peak heap
/// above the starting level, the segments written, and the bytes of text.
fn run(budget: usize, docs: u32, words: usize) -> (usize, u32, u64) {
    let mut rng = StdRng::seed_from_u64(7);
    let mut text = Vec::with_capacity(words * 12);
    let mut sink = CountingSink(0);
    let before = alloc::live();
    alloc::reset_peak();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: budget,
        },
        &mut sink,
    );
    let mut total = 0u64;
    for id in 0..docs {
        text.clear();
        for _ in 0..words {
            // a mix of common and rare words, so postings and terms both grow
            let rank = if rng.random_bool(0.5) {
                rng.random_range(0..2000)
            } else {
                rng.random_range(0..5_000_000)
            };
            word(rank, &mut text);
            text.push(b' ');
        }
        total += text.len() as u64;
        b.add(id, &text).unwrap();
    }
    let meta = b.finish().unwrap();
    let peak = alloc::peak().saturating_sub(before);
    assert_eq!(meta.docs, u64::from(docs));
    assert_eq!(meta.index_bytes, sink.0);
    (peak, meta.segments, total)
}

#[test]
fn memory_follows_the_budget_not_the_input() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    // 40,000 documents of 100 words: about 30 MB of text. The smallest budget
    // the builder accepts is 1 MiB, so the budgets are 1, 2 and 4 MiB.
    let mut results = Vec::new();
    for budget in [1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024] {
        let (peak, segments, text) = run(budget, 40_000, 100);
        println!(
            "budget {budget:>8}: peak {peak:>9} bytes ({:.2} x budget), {segments} segments, {text} bytes of text",
            peak as f64 / budget as f64
        );
        assert!(text > 5 * budget as u64);
        assert!(segments >= 5, "{segments} segments");
        // the open segment, plus growth of the table before a close, plus the
        // sorted order and dictionary built while closing, plus about 100 KB
        // of fixed buffers, plus 144 bytes for each closed segment
        assert!(
            peak <= budget * 3 / 2 + 256 * 1024 + 144 * segments as usize,
            "peak {peak} for budget {budget}"
        );
        results.push((budget, peak));
    }
    // a larger budget really does use more: the budget is what limits memory
    assert!(results[2].1 > 2 * results[0].1);
    assert!(alloc::max_single() < 16 * 1024 * 1024);
}

/// What an open reader holds: the sparse indexes and footers. Measured on an
/// index of about 250,000 distinct terms in one segment and again with the
/// terms spread over many segments.
#[test]
fn an_open_reader_holds_only_sparse_indexes() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    for budget in [usize::MAX, 1024 * 1024] {
        let mut rng = StdRng::seed_from_u64(9);
        let mut out = Vec::new();
        let mut text = Vec::new();
        let mut b = IndexBuilder::new(
            IndexOptions {
                segment_budget_bytes: budget,
            },
            &mut out,
        );
        for id in 0..2500u32 {
            text.clear();
            for _ in 0..100 {
                word(rng.random_range(0..50_000_000), &mut text);
                text.push(b' ');
            }
            b.add(id, &text).unwrap();
        }
        let meta = b.finish().unwrap();
        let before = alloc::live();
        let reader = nwdb_textindex::IndexReader::open(out.as_slice()).unwrap();
        let held = alloc::live() - before;
        let per_million = held as f64 * 1e6 / meta.terms as f64;
        println!(
            "{} terms in {} segments: reader holds {held} bytes = {:.2} MB per million terms (sparse index on disk: {} bytes)",
            meta.terms,
            meta.segments,
            per_million / 1e6,
            meta.sparse_bytes
        );
        assert!(per_million < 1.5e6, "{per_million} bytes per million terms");
        assert_eq!(reader.meta().terms, meta.terms);
    }
}
