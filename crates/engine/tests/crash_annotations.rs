//! Deterministic crash injection for flushes and start-ups that carry
//! annotations: the crash-test child annotates the traces it ingests (or,
//! alone, traces that are already stored), aborts itself at a named stage, and
//! the test checks that every acknowledged annotation is there once as the
//! winner after the restart and that no part directory is left over.

mod common;
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::*;
use nwdb_engine::Engine;
use support::{copy_dir, disk_state, present_ids, recover_and_check, run_child};

fn env(stage: &str, n: u64, mode: &str) -> Vec<(&'static str, String)> {
    vec![
        ("NWDB_ABORT_AT", format!("{stage}:{n}")),
        ("NWDB_ANNOTATE", mode.to_string()),
    ]
}

/// The newest value acknowledged for each record id.
fn last_acked(annotated: &[(u64, u64)]) -> BTreeMap<u64, u64> {
    let mut last = BTreeMap::new();
    for &(id, v) in annotated {
        let e = last.entry(id).or_insert(v);
        *e = (*e).max(v);
    }
    last
}

/// Every acknowledged annotation is shown once, with a value at least as new
/// as the acknowledged one (a later one that was not acknowledged may have
/// been logged).
fn check_annotations(e: &Engine, last: &BTreeMap<u64, u64>, what: &str) {
    for (&id, &v) in last {
        let shown = e
            .snapshot()
            .annotations(tid(u128::from(id) + 1), usize::MAX)
            .unwrap()
            .0;
        let mine: Vec<_> = shown
            .iter()
            .filter(|a| a.name == "q" && a.source == "c")
            .collect();
        assert_eq!(mine.len(), 1, "{what}: record {id}: {shown:?}");
        assert!(
            mine[0].value.unwrap() >= v as f64,
            "{what}: record {id} shows {:?}, acknowledged {v}",
            mine[0].value
        );
    }
}

fn nothing_left_over(dir: &Path, what: &str) {
    let (_, unreferenced) = disk_state(dir);
    assert!(unreferenced.is_empty(), "{what}: {unreferenced:?}");
}

/// Recovers `dir`, checks spans and annotations, restarts and checks again,
/// and that nothing is left over.
fn recover_all(
    dir: &Path,
    acked: &BTreeSet<u64>,
    last: &BTreeMap<u64, u64>,
    small_buffer: bool,
    what: &str,
    sentinel: u128,
) {
    let e = recover_and_check(dir, acked, small_buffer, what, sentinel);
    check_annotations(&e, last, what);
    e.shutdown().unwrap();
    nothing_left_over(dir, what);
    let e = open(opts(dir));
    check_annotations(&e, last, &format!("{what}, after another restart"));
    assert!(present_ids(&e).is_superset(acked));
    e.shutdown().unwrap();
}

fn has_annotation_part(dir: &Path, names: &[String]) -> bool {
    names
        .iter()
        .any(|n| dir.join("parts").join(n).join("annotations.nwa").exists())
}

/// A flush that carries spans and annotations, aborted at each stage.
#[test]
fn every_flush_stage_with_spans_and_annotations() {
    let mut annotation_leftover = false;
    let stages = [
        ("FlushFrozen", 2),
        ("PartWritten", 3),
        ("PartWritten", 6),
        ("PartWritten", 11),
        ("AllPartsWritten", 2),
        ("Committed", 2),
        ("Published", 2),
        ("LogRotated", 2),
        ("Trimmed", 2),
    ];
    for (i, (stage, n)) in stages.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let run = run_child(dir.path(), 0, 0, &env(stage, n, "with"), None);
        run.assert_aborted_at(stage);
        assert!(!run.annotated.is_empty(), "{stage}: nothing was annotated");
        let (_, unreferenced) = disk_state(dir.path());
        if matches!(stage, "PartWritten" | "AllPartsWritten") {
            assert!(
                unreferenced.iter().all(|n| !n.ends_with(".tmp")),
                "{stage}: {unreferenced:?}"
            );
            annotation_leftover |= has_annotation_part(dir.path(), &unreferenced);
        }
        let acked: BTreeSet<u64> = run.acked.iter().copied().collect();
        let label = format!("{stage}:{n}");
        recover_all(
            dir.path(),
            &acked,
            &last_acked(&run.annotated),
            i % 2 == 1,
            &label,
            i as u128,
        );
    }
    assert!(
        annotation_leftover,
        "no abort left a finished annotation part behind"
    );
}

/// A flush that carries only annotations, aborted at each stage.
#[test]
fn every_flush_stage_with_only_annotations() {
    // The base: some records, all flushed, so the child's flushes below hold
    // annotations only.
    let base = tempfile::tempdir().unwrap();
    let run = run_child(base.path(), 0, 0, &env("Trimmed", 3, ""), None);
    run.assert_aborted_at("Trimmed");
    let acked_before: BTreeSet<u64> = run.acked.iter().copied().collect();
    assert!(acked_before.len() >= 20);
    {
        let e = open(opts(base.path()));
        e.flush().unwrap();
        e.shutdown().unwrap();
    }
    let ids: Vec<u64> = acked_before.iter().copied().take(20).collect();
    let mode = format!("existing:{}:{}", ids[0], ids[ids.len() - 1]);
    // The ids are contiguous from 0: the child acknowledges in order.
    assert_eq!(ids[19] - ids[0], 19);

    let stages = [
        ("FlushFrozen", 2),
        ("PartWritten", 2),
        ("PartWritten", 5),
        ("AllPartsWritten", 2),
        ("Committed", 2),
        ("Published", 2),
        ("LogRotated", 2),
        ("Trimmed", 2),
    ];
    let mut annotation_leftover = false;
    for (i, (stage, n)) in stages.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(base.path(), dir.path());
        let run = run_child(dir.path(), 5_000_000, 0, &env(stage, n, &mode), None);
        run.assert_aborted_at(stage);
        assert!(!run.annotated.is_empty(), "{stage}");
        let (state, unreferenced) = disk_state(dir.path());
        if matches!(stage, "PartWritten" | "AllPartsWritten") {
            annotation_leftover |= has_annotation_part(dir.path(), &unreferenced);
        }
        // The flush that crashed or an earlier one committed annotation
        // parts, or none had yet.
        let _ = state;
        let label = format!("annotations only, {stage}:{n}");
        recover_all(
            dir.path(),
            &acked_before,
            &last_acked(&run.annotated),
            i % 2 == 0,
            &label,
            1_000 + i as u128,
        );
    }
    assert!(
        annotation_leftover,
        "no abort left a finished annotation part behind"
    );
}

/// A crash inside start-up, with a log that holds annotation records and
/// leftover parts, and a buffer so small that the replay itself flushes.
#[test]
fn every_open_stage_with_annotation_records_to_replay() {
    let base = tempfile::tempdir().unwrap();
    let run = run_child(base.path(), 0, 0, &env("AllPartsWritten", 2, "with"), None);
    run.assert_aborted_at("AllPartsWritten");
    let acked: BTreeSet<u64> = run.acked.iter().copied().collect();
    let last = last_acked(&run.annotated);
    assert!(!last.is_empty());
    let (_, leftovers) = disk_state(base.path());
    assert!(!leftovers.is_empty());

    for (i, stage) in [
        "OpenChecked",
        "IdsRaised",
        "ReplayFinished",
        "LeftoversDeleted",
    ]
    .into_iter()
    .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(base.path(), dir.path());
        let mut e = env(stage, 1, "");
        e.push(("NWDB_BUFFER_BYTES", "2048".to_string()));
        let run = run_child(dir.path(), 5_000_000, 0, &e, None);
        run.assert_aborted_at(stage);
        recover_all(
            dir.path(),
            &acked,
            &last,
            i % 2 == 0,
            stage,
            2_000 + i as u128,
        );
    }
}
