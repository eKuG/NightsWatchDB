//! What annotations cost in memory and in work: the bytes the engine counts
//! for a buffered annotation against the heap they really hold, the memory of
//! a flush that writes them, and the index lookups a flush makes to find the
//! windows of annotated traces.
//!
//! The allocator counters are global, so the tests of this file run one at a
//! time.

mod common;

use std::sync::Mutex;

use common::*;
use nwdb_engine::{Annotation, Engine};
use nwdb_segment::{MAX_COMMENT_BYTES, MAX_LABEL_BYTES, MAX_NAME_BYTES, MAX_SOURCE_BYTES};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};

#[global_allocator]
static ALLOC: Counting = Counting;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn small(k: usize) -> Annotation {
    score(1 + (k % 50) as u128, None, &format!("n{k:07}"), "s", 1.0)
}

fn largest(k: usize) -> Annotation {
    let mut a = score(
        1 + (k % 50) as u128,
        Some(1 + k as u64),
        &format!("{k:07}{}", "n".repeat(MAX_NAME_BYTES - 7)),
        &"s".repeat(MAX_SOURCE_BYTES),
        1.0,
    );
    a.label = Some("l".repeat(MAX_LABEL_BYTES));
    a.comment = Some("c".repeat(MAX_COMMENT_BYTES));
    a
}

/// An engine with 50 known traces.
fn engine_with_traces(dir: &std::path::Path) -> Engine {
    let e = open(opts(dir));
    let spans: Vec<TSpan> = (1..=50u128).map(|t| TSpan::new(t, 1, 100).root()).collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    e
}

/// The bytes the engine counts for `n` annotations from `make`, and the heap
/// they hold.
fn counted_and_held(make: fn(usize) -> Annotation, n: usize, per_request: usize) -> (usize, isize) {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_traces(dir.path());
    let counted_before = e.stats().buffered_bytes;
    let live_before = live();
    let mut k = 0;
    while k < n {
        let end = (k + per_request).min(n);
        annotate(&e, (k..end).map(make).collect());
        k = end;
    }
    let held = live() - live_before;
    let counted = e.stats().buffered_bytes - counted_before;
    assert_eq!(e.stats().annotations_buffered, n);
    e.shutdown().unwrap();
    (counted, held)
}

#[test]
fn the_bytes_counted_for_buffered_annotations_are_never_below_the_heap_they_hold() {
    let _serial = serial();
    // Requests of a size whose log buffers are small next to the total: the
    // log keeps a buffer of the size of its largest record.
    for (what, make, n, per_request) in [
        ("small", small as fn(usize) -> Annotation, 20_000, 500),
        ("largest", largest as fn(usize) -> Annotation, 500, 5),
    ] {
        let (counted, held) = counted_and_held(make, n, per_request);
        eprintln!(
            "{what}: counted {} bytes each, heap {} bytes each",
            counted / n,
            held / n as isize
        );
        assert!(
            counted as isize >= held,
            "{what}: {n} annotations count for {counted} bytes but hold {held}"
        );
        // Not absurdly high either: within 1.5 times the heap.
        assert!(
            (counted as isize) * 2 <= held * 3,
            "{what}: {n} annotations count for {counted} bytes but hold only {held}"
        );
    }
}

/// The flush reduces the frozen annotations to one record per identity
/// through references, so its memory is the buffer plus the references
/// (8 bytes per annotation) and one block, not a second copy of the buffer.
#[test]
fn a_flush_does_not_copy_the_buffered_annotations() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_traces(dir.path());
    let before = live();
    for k in (0..40_000).step_by(500) {
        annotate(&e, (k..k + 500).map(small).collect());
    }
    let held = live() - before;
    assert!(held > 4 << 20, "the buffer holds {held} bytes");
    let base = live();
    reset_peak();
    e.flush().unwrap();
    let grown = peak() - base;
    assert_eq!(e.stats().annotations_written, 40_000);
    assert!(
        grown * 2 < held,
        "a flush of a buffer of {held} bytes peaked {grown} bytes above its start"
    );
    e.shutdown().unwrap();
}

/// One part per window, 50 traces each with ids in a range of their own, and
/// every trace annotated in one flush: the windows are looked up in one walk
/// over the parts, and a part is asked only about the ids inside its range.
/// Looking each annotated trace up in every part would take about
/// `traces * parts / 2` probes.
#[test]
fn a_flush_finds_the_windows_of_many_traces_over_many_parts_in_one_walk() {
    let _serial = serial();
    const PARTS: u128 = 40;
    const PER: u128 = 50;
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for w in 0..PARTS {
        let spans: Vec<TSpan> = (1..=PER)
            .map(|k| TSpan::new(w * 1000 + k, 1, w as i64 * 1_000 + 10).root())
            .collect();
        ingest(&e, &spans).unwrap();
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, PARTS as usize);
    let all: Vec<Annotation> = (0..PARTS)
        .flat_map(|w| (1..=PER).map(move |k| score(w * 1000 + k, None, "q", "s", 1.0)))
        .collect();
    for chunk in all.chunks(500) {
        annotate(&e, chunk.to_vec());
    }
    let probes_before = e.annotation_window_probes();
    e.flush().unwrap();
    let probes = e.annotation_window_probes() - probes_before;
    let traces = (PARTS * PER) as u64;
    eprintln!("{probes} index probes for {traces} traces over {PARTS} parts");
    assert!(
        probes <= traces,
        "{probes} index probes for {traces} traces over {PARTS} parts"
    );
    // Each trace is in the annotation part of its own window.
    let info = e.snapshot().annotation_part_info();
    assert_eq!(info.len(), PARTS as usize);
    assert!(info.iter().all(|&(_, _, _, rows)| rows == PER as u64));
    assert_eq!(e.stats().annotations_dropped, 0);
    e.shutdown().unwrap();
}
