//! The writer's memory is bounded by its window, not by the amount of unique
//! text: a part with far more unique text than the window takes no more
//! memory than one with a few times less.
//!
//! The peak is the kernel's high-water mark of the process's resident memory
//! (`VmHWM`), which is read from `/proc`: a counting allocator needs `unsafe`
//! code, which the workspace forbids. The two runs are in one test, so that
//! nothing else in the process disturbs the measure.

#![cfg(target_os = "linux")]

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
};
use nwdb_segment::{PartReader, PartWriter, WriterOptions};
use nwdb_testkit::mem::memory;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::sync::Arc;

const MIB: usize = 1 << 20;
const WINDOW: usize = 4 * MIB;

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
/// and returns the process's peak resident memory afterwards.
fn write(dir: &std::path::Path, id: u64, unique_mib: usize) -> (usize, nwdb_core::PartMeta) {
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(4096))),
        dedup_window_bytes: WINDOW,
        // The text index and the row lists have budgets of their own; they are
        // kept small so that what these tests measure is not hidden by them.
        text_index_budget_bytes: 1 << 20,
        row_list_budget_bytes: 1 << 20,
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
fn writer_memory_follows_the_window_not_the_unique_text() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, start) = memory();
    let (peak_small, small) = write(tmp.path(), 1, 24);
    let (peak_big, big) = write(tmp.path(), 2, 120);
    eprintln!(
        "resident at start {} MiB; peak after 24 MiB of unique text {} MiB, after 120 MiB {} MiB \
         (window {} MiB); pieces {} and {}",
        start / MIB,
        peak_small / MIB,
        peak_big / MIB,
        WINDOW / MIB,
        small.payload.pieces,
        big.payload.pieces
    );
    assert!(big.payload.unique_bytes >= 120 * MIB as u64 - 1);
    assert!(big.payload.unique_bytes > 25 * WINDOW as u64);
    // The part with five times the unique text took essentially no more
    // memory (the margin covers allocator noise).
    assert!(
        peak_big <= peak_small + 8 * MIB,
        "peak grew from {} to {} MiB",
        peak_small / MIB,
        peak_big / MIB
    );
    // And the peak itself is the window plus a bounded amount of buffers.
    assert!(
        peak_big <= start + WINDOW + 40 * MIB,
        "peak {} MiB over a start of {} MiB and a window of {} MiB",
        peak_big / MIB,
        start / MIB,
        WINDOW / MIB
    );
    // The part is fine.
    PartReader::open(tmp.path().join(nwdb_core::part_dir_name(2)))
        .unwrap()
        .verify()
        .unwrap();
}
