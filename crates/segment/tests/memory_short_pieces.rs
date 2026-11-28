//! The memory of a part writer and of an open part must not follow the number
//! of pieces, which a sender decides: the writer keeps the length of every
//! piece (one byte for a short one, in the form it has on disk), the reader
//! the same table with a small index, and the part writer stores a cut shorter
//! than `min_piece_bytes` inline and stops making pieces at `max_pieces`.
//! `memory.rs` writes pieces of 4 KiB, where the per-piece cost never shows;
//! here they are 12 bytes.
//!
//! The peak is the kernel's high-water mark of the process's resident memory
//! (`VmHWM`), read from `/proc`. The file holds one test, so that nothing else
//! in the process disturbs the measure. Removing the floor, the ceiling or the
//! compact tables makes it fail: 12 bytes a piece in the reader, 16 in the
//! writer, and 2 million pieces where the floor says none.

#![cfg(target_os = "linux")]

use nwdb_core::{
    BatchLimits, PartMeta, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord,
    TraceId, part_dir_name,
};
use nwdb_segment::{PartReader, PartWriter, WriterOptions};
use nwdb_testkit::mem::memory;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

const MIB: usize = 1 << 20;
const WINDOW: usize = 4 * MIB;

/// The most the writer's peak may grow for each short piece it holds.
const BYTES_PER_PIECE: usize = 3;
/// What the peak may move for reasons that do not follow the pieces.
const FLAT_ALLOWANCE: usize = 4 * MIB;

struct Chunks(usize);

impl PayloadSplitter for Chunks {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut at = 0;
        while at < value.len() {
            let end = (at + self.0).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

/// Writes a part of `unique_mib` MiB of incompressible, never repeated text
/// cut into chunks of `chunk` bytes, and returns the process's peak resident
/// memory afterwards.
fn write(
    dir: &Path,
    id: u64,
    unique_mib: usize,
    chunk: usize,
    max_pieces: usize,
) -> (usize, PartMeta) {
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(chunk))),
        dedup_window_bytes: WINDOW,
        // Small row groups, so that what Parquet buffers (the ropes of the
        // row group) does not grow with the part and hide what is measured.
        row_group_rows: 4,
        // The text index and the row lists have budgets of their own; they are
        // kept small so that what these tests measure is not hidden by them.
        text_index_budget_bytes: 1 << 20,
        row_list_budget_bytes: 1 << 20,
        max_pieces,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(dir, id, 0, 900_000_000_000, options).unwrap();
    let mut rng = StdRng::seed_from_u64(id);
    let mut span = 0u64;
    for _ in 0..unique_mib {
        // One MiB of new text per batch, in four rows of one trace each.
        let values: Vec<Vec<u8>> = (0..4)
            .map(|_| {
                let mut v = vec![0u8; MIB / 4];
                rng.fill_bytes(&mut v);
                v
            })
            .collect();
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 100,
            max_bytes: 1 << 30,
        });
        for v in &values {
            span += 1;
            b.append(&SpanRecord {
                trace_id: TraceId((span as u128).to_be_bytes()),
                span_id: SpanId(span.to_be_bytes()),
                name: "s",
                attrs: v,
                ..SpanRecord::default()
            });
        }
        w.write(&b.finish()).unwrap();
    }
    let meta = w.finish().unwrap();
    (memory().0, meta)
}

#[test]
fn writer_memory_grows_by_at_most_three_bytes_per_short_piece_and_the_reader_by_less() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, start) = memory();

    // 12-byte pieces: 0.7 million of them, then 3.5 million.
    let (peak_small, small) = write(tmp.path(), 1, 8, 12, usize::MAX);
    let (peak_big, big) = write(tmp.path(), 2, 40, 12, usize::MAX);
    eprintln!(
        "resident at start {} MiB; peak after {} pieces {} MiB, after {} pieces {} MiB \
         (window {} MiB)",
        start / MIB,
        small.payload.pieces,
        peak_small / MIB,
        big.payload.pieces,
        peak_big / MIB,
        WINDOW / MIB
    );
    assert!(big.payload.pieces > 3_000_000, "{:?}", big.payload);
    // The writer holds the length of every piece, one byte for a short one,
    // in a vector that doubles: up to three times its length while it grows,
    // so 3 bytes a piece (the bound the README states), plus a flat allowance
    // for what the process does besides. At 16 bytes a piece the writer would
    // have held 45 MiB more than that.
    let extra = (big.payload.pieces - small.payload.pieces) as usize;
    assert!(
        peak_big <= peak_small + BYTES_PER_PIECE * extra + FLAT_ALLOWANCE,
        "peak grew from {} to {} MiB for {} more pieces",
        peak_small / MIB,
        peak_big / MIB,
        extra
    );

    // What an open part holds for 3.5 million pieces: the lengths as they are
    // on disk (a byte each) and a checkpoint for every 64th, not 12 bytes each.
    let pieces = big.payload.pieces as usize;
    let (_, before) = memory();
    let reader = PartReader::open(tmp.path().join(part_dir_name(2))).unwrap();
    let (_, after) = memory();
    let held = reader.piece_store().memory_bytes();
    eprintln!(
        "an open part with {pieces} 12-byte pieces holds {held} bytes for them ({:.2} a piece); \
         resident memory grew {} KiB",
        held as f64 / pieces as f64,
        after.saturating_sub(before) / 1024
    );
    assert!(held < 2 * pieces, "{held} bytes for {pieces} pieces");
    assert!(
        after.saturating_sub(before) < 3 * pieces + 2 * MIB,
        "resident memory grew {} bytes for {pieces} pieces",
        after.saturating_sub(before)
    );
    drop(reader);

    // The floor: 4-byte cuts are not pieces, however many there are.
    let (_, tiny) = write(tmp.path(), 3, 8, 4, usize::MAX);
    assert_eq!(tiny.payload.pieces, 0, "{:?}", tiny.payload);

    // The ceiling: a part that may hold 500,000 pieces holds exactly that
    // many, and still reads back.
    let (_, capped) = write(tmp.path(), 4, 40, 12, 500_000);
    assert_eq!(capped.payload.pieces, 500_000);
    assert_eq!(capped.payload.unique_bytes, 12 * 500_000);
    PartReader::open(tmp.path().join(part_dir_name(4)))
        .unwrap()
        .verify()
        .unwrap();
}
