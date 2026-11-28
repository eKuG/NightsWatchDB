//! What a search holds per open posting list: bounded by a constant, not by
//! the length of the list, and with no read-ahead when many lists are open.
//! Measured with the counting allocator, so the tests take turns.

#[path = "support/alloc.rs"]
mod alloc;
mod support;

use std::io::Write;

use nwdb_textindex::{IndexBuilder, IndexOptions, IndexReader};

#[global_allocator]
static ALLOC: alloc::Counting = alloc::Counter;

static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// `docs` documents: `common` in every one, `rare` in about a dozen of them
/// (a list on disk with a few ids), `c0`..`c3` in a quarter each.
fn long_lists(docs: u32) -> Vec<u8> {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: usize::MAX,
        },
        &mut out,
    );
    for i in 0..docs {
        let mut t = format!("common c{}", i % 4);
        if i % (docs / 12) == 3 {
            t.push_str(" rare");
        }
        b.add(i, t.as_bytes()).unwrap();
    }
    b.finish().unwrap();
    out
}

/// Peak heap above the start while intersecting `common` with `rare`.
fn intersect_peak(idx: &[u8]) -> (usize, usize) {
    let r = IndexReader::open(idx).unwrap();
    r.verify().unwrap();
    let before = alloc::live();
    alloc::reset_peak();
    let found = r.intersect(&["common", "rare"]).unwrap();
    let peak = alloc::peak().saturating_sub(before);
    (peak, found.ids.len())
}

/// A cursor reads the skip table of a list a page at a time, so what a
/// search holds does not grow with the list: 50,000 and 1,200,000 ids cost the
/// same, where a whole table (8 bytes per 128 ids) would differ by 75 KB.
#[test]
fn a_cursors_memory_does_not_depend_on_the_length_of_its_list() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let short = long_lists(48_000);
    let long = long_lists(1_200_000);
    let (small, n1) = intersect_peak(&short);
    let (large, n2) = intersect_peak(&long);
    println!("intersect over 48k docs held {small} bytes, over 1.2M docs {large}");
    assert!(n1 >= 12 && n2 >= 12, "{n1} {n2}");
    // two lists open: a page, a decoded block and its bytes each, two
    // dictionary blocks, and the output
    assert!(small <= 24 * 1024, "held {small} bytes");
    assert!(large <= small + 2 * 1024, "{small} bytes grew to {large}");
}

/// README, "Memory of an open reader": with many lists open nothing reads
/// ahead, so a prefix over 1,000 terms holds about 2 KiB for each list and not
/// 8 KiB more.
#[test]
fn a_prefix_over_many_lists_reads_no_further_ahead_than_a_block() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(IndexOptions::default(), &mut out);
    let mut text = Vec::new();
    // 1,000 terms, each in 300 documents (three blocks on disk)
    for d in 0..300u32 {
        text.clear();
        for t in 0..1000 {
            write!(text, "t{t:04} ").unwrap();
        }
        b.add(d * 3, &text).unwrap();
    }
    b.finish().unwrap();
    let r = IndexReader::open(out.as_slice()).unwrap();
    r.verify().unwrap();
    let before = alloc::live();
    alloc::reset_peak();
    let got = r.prefix("t", 1000).unwrap();
    let peak = alloc::peak().saturating_sub(before);
    assert_eq!(got.len(), 300);
    println!(
        "prefix over 1000 terms held {peak} bytes, {} per list",
        peak / 1000
    );
    // the 1,000 matching entries and names, and the output, are a few hundred
    // KB; each list is a page of 3 entries, a block and its bytes
    assert!(peak <= 1000 * 2100 + 400 * 1024, "held {peak} bytes");
}
