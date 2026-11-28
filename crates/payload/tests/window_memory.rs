//! The de-duplication window is charged for what it holds, and what the
//! process holds in memory follows it, not the amount of text written.
//!
//! The peak is the kernel's high-water mark of the process's resident memory
//! (`VmHWM`), read from `/proc`: a counting allocator needs `unsafe` code,
//! which the workspace forbids. It is measured, not taken from the writer's own
//! estimate, so a buffer that is held but not charged shows. A window four
//! times as large must cost about three windows more, not more (a buffer that
//! kept the capacity it grew to would cost half as much again). The file holds
//! one test, so nothing else in the process disturbs the measure.

#![cfg(target_os = "linux")]

use nwdb_payload::{PieceReader, PieceWriter, PieceWriterOptions};
use nwdb_testkit::mem::peak;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::path::Path;

const MIB: usize = 1 << 20;
const BLOCK: usize = MIB;
const TOTAL: usize = 192 * MIB;
const SMALL: usize = 16 * MIB;
const LARGE: usize = 64 * MIB;

/// The pieces of a run: random bytes of 1 KiB to 48 KiB, never repeated.
fn pieces(seed: u64) -> impl Iterator<Item = Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut written = 0;
    std::iter::from_fn(move || {
        if written >= TOTAL {
            return None;
        }
        let mut t = vec![0u8; rng.random_range(1024..48 * 1024usize)];
        rng.fill_bytes(&mut t);
        written += t.len();
        Some(t)
    })
}

/// Writes the run's pieces through a window of `window` bytes into `dir` and
/// returns the ids, the most the writer charged for memory at any moment and
/// the process's peak resident memory afterwards.
fn run(dir: &Path, window: usize, seed: u64) -> (Vec<u32>, usize, usize) {
    let mut w = PieceWriter::create(
        dir,
        PieceWriterOptions {
            block_bytes: BLOCK,
            dedup_window_bytes: window,
        },
    )
    .unwrap();
    let (mut charged, mut ids) = (0, Vec::new());
    for t in pieces(seed) {
        ids.push(w.add(&t).unwrap());
        charged = charged.max(w.memory_bytes());
    }
    let sum = w.finish().unwrap();
    assert!(sum.totals.unique_bytes as usize >= TOTAL);
    (ids, charged, peak())
}

#[test]
fn the_window_bounds_memory_and_the_data_survives() {
    let (small_dir, large_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let start = peak();
    let (_, small_charged, small_peak) = run(small_dir.path(), SMALL, 1);
    let (ids, large_charged, large_peak) = run(large_dir.path(), LARGE, 2);
    eprintln!(
        "192 MiB of unique text: peak resident memory {} MiB at the start, {} MiB after a {} MiB \
         window (writer charged at most {} MiB), {} MiB after a {} MiB window (charged {} MiB)",
        start / MIB,
        small_peak / MIB,
        SMALL / MIB,
        small_charged / MIB,
        large_peak / MIB,
        LARGE / MIB,
        large_charged / MIB
    );
    // What the writer charges is the window, the open block and a little more
    // (the piece that closed a block, the tables).
    for (charged, window) in [(small_charged, SMALL), (large_charged, LARGE)] {
        assert!(charged <= window + 2 * BLOCK + 2 * MIB, "charged {charged}");
    }
    // What the process held: 48 MiB more window cost no more than 48 MiB more
    // memory and a little slack. A window kept in buffers that had grown by
    // doubling would have cost up to half as much again.
    let more_window = LARGE - SMALL;
    assert!(
        large_peak <= small_peak + more_window + 6 * MIB,
        "a window {} MiB larger raised the peak from {} to {} MiB",
        more_window / MIB,
        small_peak / MIB,
        large_peak / MIB
    );

    // The data survives: every piece reads back as itself.
    let r = PieceReader::open(large_dir.path()).unwrap();
    let mut session = r.session();
    for (id, t) in ids.iter().zip(pieces(2)) {
        let mut out = Vec::new();
        r.read_piece(&mut session, *id, &mut out).unwrap();
        assert!(out == t, "piece {id}");
    }
}
