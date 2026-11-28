//! Memory claims of the builder and of a search, measured with the
//! counting allocator.

#[path = "support/alloc.rs"]
mod alloc;
mod support;

use std::io::Write;

use nwdb_core::{Error, put_varint};
use nwdb_textindex::{IndexBuilder, IndexOptions, IndexReader, MIN_SEGMENT_BUDGET};

#[global_allocator]
static ALLOC: alloc::Counting = alloc::Counter;

/// The allocator counts the whole process, so the tests take turns.
static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct CountingSink;

impl Write for CountingSink {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Peak heap above the start while building `docs` documents of twenty
/// distinct words each, and the segments written.
fn build_peak(budget: usize, docs: u32) -> (usize, u32) {
    let mut text = Vec::new();
    let before = alloc::live();
    alloc::reset_peak();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: budget,
        },
        CountingSink,
    );
    for id in 0..docs {
        text.clear();
        for k in 0..20 {
            write!(text, "w{id}x{k} ").unwrap();
        }
        b.add(id, &text).unwrap();
    }
    let meta = b.finish().unwrap();
    (alloc::peak().saturating_sub(before), meta.segments)
}

/// README, "Memory bound": the open segment grows with the input only up to
/// the budget, which is at least 1 MiB however small a value is asked for.
/// Each closed segment adds at most 144 bytes (a 72 byte directory entry and
/// the vector's spare room) until `finish`. So the peak stays under
/// 1.5 x minimum budget + 256 KiB + 144 bytes per segment, and the number of
/// segments follows the 1 MiB minimum, not the number of documents.
#[test]
fn builder_memory_is_bounded_by_the_minimum_budget_and_the_documented_cost_per_segment() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let asked = 1024;
    let min = MIN_SEGMENT_BUDGET;
    let (small, small_segments) = build_peak(asked, 5_000);
    let (large, large_segments) = build_peak(asked, 60_000);
    println!(
        "budget {asked} (raised to {min}): {small_segments} segments peak {small}, {large_segments} segments peak {large}"
    );
    // a document of twenty new words costs well under 4 KiB of the budget,
    // so a segment holds hundreds of documents; without the minimum this
    // would be one segment per document
    assert!(
        large_segments >= 20 && large_segments <= (60_000 * 4096 / min) as u32 + 1,
        "{large_segments} segments"
    );
    assert!(
        large <= min * 3 / 2 + 256 * 1024 + 144 * large_segments as usize,
        "peak {large} bytes for a budget of {min} and {large_segments} segments"
    );
    // growing the input twelvefold adds only what the extra segments cost
    let more = (large_segments - small_segments) as usize;
    assert!(
        large <= small + 144 * more + 32 * 1024,
        "peak grew from {small} to {large} bytes with {more} more segments"
    );
}

/// An index of one segment, written by hand from the documented layout, whose
/// `terms` terms all point at the same posting list of `blocks` x 128
/// consecutive ids. Every checksum is valid and every term passes the
/// per-entry checks. The last term points at a list whose skip table is
/// wrong, so a search that opens every list ends in `Corrupt` without
/// merging anything.
fn shared_list_index(terms: usize, blocks: usize) -> Vec<u8> {
    let docs = (blocks * 128) as u64;
    let mut postings = Vec::new();
    for k in 0..blocks {
        postings.extend_from_slice(&((k * 128 + 127) as u32).to_le_bytes());
        postings.extend_from_slice(&(k as u32).to_le_bytes());
    }
    // width-zero blocks: one byte each
    postings.resize(postings.len() + blocks, 0);

    let names: Vec<Vec<u8>> = (0..terms)
        .map(|i| format!("t{i:05}").into_bytes())
        .collect();
    let (mut dict, mut sparse) = (Vec::new(), Vec::new());
    let (mut prev, mut dict_blocks, mut total) = (&[][..], 0u32, 0u64);
    for (i, t) in names.iter().enumerate() {
        if i % 32 == 0 {
            prev = &[];
            put_varint(&mut sparse, t.len() as u64);
            sparse.extend_from_slice(t);
            sparse.extend_from_slice(&(dict.len() as u64).to_le_bytes());
            dict_blocks += 1;
        }
        let shared = prev.iter().zip(t).take_while(|(a, b)| a == b).count();
        put_varint(&mut dict, shared as u64);
        put_varint(&mut dict, (t.len() - shared) as u64);
        dict.extend_from_slice(&t[shared..]);
        prev = t.as_slice();
        let (df, off) = if i + 1 == terms { (9, 8) } else { (docs, 0) };
        put_varint(&mut dict, df);
        put_varint(&mut dict, off);
        total += df;
    }

    let mut file = postings.clone();
    file.extend_from_slice(&dict);
    file.extend_from_slice(&sparse);
    let body_crc = crc32c::crc32c_append(crc32c::crc32c(&postings), &dict);
    let mut dir = Vec::new();
    for v in [
        postings.len() as u64,
        dict.len() as u64,
        sparse.len() as u64,
        terms as u64,
        total,
    ] {
        dir.extend_from_slice(&v.to_le_bytes());
    }
    for v in [
        docs as u32,
        0,
        docs as u32 - 1,
        dict_blocks,
        body_crc,
        crc32c::crc32c(&sparse),
    ] {
        dir.extend_from_slice(&v.to_le_bytes());
    }
    dir.extend_from_slice(&0u64.to_le_bytes());
    let mut footer = [0u8; 64];
    footer[0..4].copy_from_slice(&2u32.to_le_bytes());
    footer[4..8].copy_from_slice(&1u32.to_le_bytes());
    footer[8..12].copy_from_slice(&32u32.to_le_bytes());
    footer[16..24].copy_from_slice(&docs.to_le_bytes());
    footer[32..40].copy_from_slice(&(terms as u64).to_le_bytes());
    footer[40..48].copy_from_slice(&total.to_le_bytes());
    footer[48..56].copy_from_slice(&(file.len() as u64).to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&dir), &footer[..56]);
    footer[56..60].copy_from_slice(&crc.to_le_bytes());
    footer[60..64].copy_from_slice(b"NWTX");
    file.extend_from_slice(&dir);
    file.extend_from_slice(&footer);
    file
}

/// README, "Reading and failure rules": a hostile file with valid checksums
/// meets the structural checks, and nothing from a file sizes an allocation
/// beyond what the file can back. Here a 27 KB file makes one prefix search
/// hold a 16 KB skip table for each of 999 terms at once, because nothing
/// checks that posting lists do not overlap.
#[test]
fn a_prefix_search_does_not_hold_more_than_the_file_can_back() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let file = shared_list_index(1000, 2000);
    let r = IndexReader::open(file.as_slice()).unwrap();
    r.verify().unwrap();
    assert_eq!(r.term("t00000").unwrap().ids.len(), 2000 * 128);
    let before = alloc::live();
    alloc::reset_peak();
    let res = r.prefix("t", 1000);
    let used = alloc::peak().saturating_sub(before);
    println!("file {} bytes, prefix search held {used} bytes", file.len());
    assert!(
        matches!(res, Ok(_) | Err(Error::Corrupt(_))),
        "{:?}",
        res.map(|d| d.len())
    );
    assert!(
        used <= 4 * file.len() + (1 << 20),
        "a {} byte file made one search hold {used} bytes",
        file.len()
    );
}

/// README, "Memory of an open reader": a search holds, for each posting list
/// it opens, a page of its skip table and one 128-id block, and no read-ahead
/// while several lists are open.
#[test]
fn a_prefix_search_holds_about_one_block_per_matching_term() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: usize::MAX,
        },
        &mut out,
    );
    let mut text = Vec::new();
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
    let used = alloc::peak().saturating_sub(before);
    assert_eq!(got.len(), 300);
    // a skip table of 24 bytes, a 128-id block decoded (512 bytes) and its
    // bytes (at most 513), with room to spare, per term
    let allowed = 1000 * 2048 + (1 << 20);
    println!("prefix over 1000 terms held {used} bytes, allowed {allowed}");
    assert!(used <= allowed, "held {used} bytes");
}
