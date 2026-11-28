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
use nwdb_engine::{Annotation, Engine, ListQuery};
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

/// One annotation of trace 1 with a name, source, label and comment of the
/// largest sizes; `identity` picks which identity it has.
fn big_of_trace_1(identity: usize) -> Annotation {
    let mut a = score(
        1,
        Some(1 + identity as u64),
        &format!("{identity:07}{}", "n".repeat(MAX_NAME_BYTES - 7)),
        &"s".repeat(MAX_SOURCE_BYTES),
        1.0,
    );
    a.label = Some("l".repeat(MAX_LABEL_BYTES));
    a.comment = Some("c".repeat(MAX_COMMENT_BYTES));
    a
}

/// The filtered list's exact path for a trace with many buffered versions of
/// one identity, each with a comment: the versions are looked at through
/// references and never copied, so the query holds the 24 bytes per buffered
/// annotation of its index and a few records, not the versions. Asserted: the
/// peak above the start stays below 5% of the heap the buffer holds and below
/// 1 MiB.
#[test]
fn a_filtered_list_over_many_buffered_versions_holds_references_not_copies() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_traces(dir.path());
    let before = live();
    // One request each: the items of one request with one identity would be
    // reduced to the last one before they are logged.
    for _ in 0..3_000 {
        annotate(&e, vec![big_of_trace_1(0)]);
    }
    let held = (live() - before) as usize;
    assert_eq!(e.stats().annotations_buffered, 3_000);
    assert!(held > 12 << 20, "the buffer holds {held} bytes");
    let snap = e.snapshot();
    let q = ListQuery {
        annotation: Some(filter("no-such-name")),
        ..list_all()
    };
    let base = live();
    reset_peak();
    let (list, stats) = snap.list_traces_with_stats(&q).unwrap();
    let grown = (peak() - base) as usize;
    eprintln!("buffer {held} bytes, list peaked {grown} bytes above its start");
    assert!(list.is_empty());
    assert_eq!(
        stats.annotation_exact_lookups, 1,
        "trace 1 takes the exact path"
    );
    assert!(
        grown * 20 < held,
        "{grown} bytes against a buffer of {held}"
    );
    assert!(grown < 1 << 20, "{grown} bytes");
    drop(snap);
    e.shutdown().unwrap();
}

/// `annotations(trace, 1000)` over 1000 buffered records of the largest size:
/// besides the buffer, the call holds the records it returns (one copy) and
/// nothing proportional to the buffer. Asserted: the peak above the start is
/// at most 1.25 times the heap of the returned records.
#[test]
fn reading_a_trace_s_largest_annotations_holds_about_one_copy() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let e = engine_with_traces(dir.path());
    let before = live();
    for k in (0..1_000).step_by(100) {
        annotate(&e, (k..k + 100).map(big_of_trace_1).collect());
    }
    // The records returned are copies of the buffered ones: the same heap.
    let returned = (live() - before) as usize;
    let snap = e.snapshot();
    let base = live();
    reset_peak();
    let (v, more) = snap.annotations(tid(1), 1_000).unwrap();
    let grown = (peak() - base) as usize;
    eprintln!("returned {returned} bytes, peaked {grown} bytes above the start");
    assert_eq!(v.len(), 1_000);
    assert!(!more);
    assert!(returned > 4 << 20, "{returned}");
    assert!(
        grown * 4 <= returned * 5,
        "peaked {grown} bytes for {returned} bytes returned"
    );
    drop(v);
    drop(snap);
    e.shutdown().unwrap();
}

/// The same flush with trace ids drawn at random: every part's id range then
/// covers nearly every id, so the range pruning does not help and each
/// annotated trace is probed in the parts walked before the one that holds it
/// (and in that one): the parts are walked latest window first, so a trace in
/// the part at rank `r` costs `r + 1` probes, and over all traces that is
/// about `traces * parts / 2`. Asserted: at least 0.9 and at most 1.0 times
/// `traces * (parts + 1) / 2`.
#[test]
fn a_flush_probes_about_half_the_parts_per_trace_when_the_ids_are_random() {
    let _serial = serial();
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    const PARTS: usize = 40;
    const PER: usize = 50;
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut rng = StdRng::seed_from_u64(77);
    let mut ids: Vec<u128> = Vec::new();
    for w in 0..PARTS {
        let batch: Vec<u128> = (0..PER).map(|_| rng.random::<u128>() | 1).collect();
        let spans: Vec<TSpan> = batch
            .iter()
            .map(|&t| TSpan::new(t, 1, w as i64 * 1_000 + 10).root())
            .collect();
        ingest(&e, &spans).unwrap();
        e.flush().unwrap();
        ids.extend(batch);
    }
    assert_eq!(e.stats().parts, PARTS);
    let all: Vec<Annotation> = ids.iter().map(|&t| score(t, None, "q", "s", 1.0)).collect();
    for chunk in all.chunks(500) {
        annotate(&e, chunk.to_vec());
    }
    let before = e.annotation_window_probes();
    e.flush().unwrap();
    let probes = (e.annotation_window_probes() - before) as usize;
    let traces = PARTS * PER;
    let upper = traces * (PARTS + 1) / 2;
    eprintln!("{probes} index probes for {traces} random-id traces over {PARTS} parts");
    assert!(probes <= upper, "{probes} probes, at most {upper} possible");
    assert!(
        probes * 10 >= upper * 9,
        "{probes} probes, expected about {upper}"
    );
    assert_eq!(e.stats().annotations_dropped, 0);
    assert_eq!(e.snapshot().annotation_part_info().len(), PARTS);
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
