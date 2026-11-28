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
use nwdb_core::PartKind;
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

// Maintenance: merging and retention of annotation parts.

const BASE_TRACES: u64 = 24;
/// The windows of the base are 10 000 ns wide, as the child's are.
const BASE_WINDOW: i64 = 10_000;

/// A base the maintenance crash tests start from: 24 traces (record id `i` is
/// trace `i + 1`, with one span) in four windows (`i % 4`), and four flushes
/// of annotations on every trace, so four annotation parts in each window. The
/// last flush deletes the annotation of the records with `i % 6 == 5`. Returns
/// the winner of each record.
fn maintenance_base(dir: &Path) -> BTreeMap<u64, Option<f64>> {
    let mut o = opts(dir);
    o.window_ns = BASE_WINDOW;
    let e = open(o);
    let spans: Vec<TSpan> = (0..BASE_TRACES)
        .map(|i| {
            let mut s = TSpan::new(
                u128::from(i) + 1,
                1,
                (i % 4) as i64 * BASE_WINDOW + 100 + i as i64,
            )
            .root();
            s.attrs = vec![(i % 251) as u8; 200];
            s
        })
        .collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    let mut want = BTreeMap::new();
    for v in 1..=4u64 {
        let items = (0..BASE_TRACES)
            .map(|i| {
                let t = u128::from(i) + 1;
                if v == 4 && i % 6 == 5 {
                    want.insert(i, None);
                    delete(t, None, "q", "c")
                } else {
                    want.insert(i, Some(v as f64));
                    score(t, None, "q", "c", v as f64)
                }
            })
            .collect();
        annotate(&e, items);
        e.flush().unwrap();
    }
    assert_eq!(e.stats().annotation_parts, 16);
    e.shutdown().unwrap();
    want
}

/// Every record shows its winner (a deleted one nothing), except the ones
/// `gone` names, which must show no span and no annotation.
fn check_winners(
    e: &Engine,
    want: &BTreeMap<u64, Option<f64>>,
    gone: &dyn Fn(u64) -> bool,
    what: &str,
) {
    for (&i, v) in want {
        let t = u128::from(i) + 1;
        let shown = e.snapshot().annotations(tid(t), usize::MAX).unwrap().0;
        let mine: Vec<_> = shown
            .iter()
            .filter(|a| a.name == "q" && a.source == "c")
            .collect();
        if gone(i) {
            assert!(mine.is_empty(), "{what}: record {i} is expired: {shown:?}");
            assert!(e.get_trace(tid(t)).unwrap().is_none(), "{what}: record {i}");
            continue;
        }
        match v {
            Some(x) => {
                assert_eq!(mine.len(), 1, "{what}: record {i}: {shown:?}");
                assert_eq!(mine[0].value, Some(*x), "{what}: record {i}");
            }
            None => assert!(mine.is_empty(), "{what}: record {i} is deleted: {shown:?}"),
        }
    }
}

fn annotation_ids(state: &nwdb_manifest::ManifestState) -> BTreeSet<u64> {
    state
        .parts
        .iter()
        .filter(|p| p.kind == PartKind::Annotations)
        .map(|p| p.part_id)
        .collect()
}

/// A crash at each stage of a merge of annotation parts and its replacement,
/// in the child's maintenance thread. Before the commit the manifest lists
/// every base part and a finished merged directory is a leftover; after it
/// the merged part replaces the four parts of its window; either way, every
/// annotation is there once as the winner and no directory is left over, also
/// after the merges are finished.
#[test]
fn every_stage_of_an_annotation_merge() {
    let base = tempfile::tempdir().unwrap();
    let want = maintenance_base(base.path());
    let acked_base: BTreeSet<u64> = (0..BASE_TRACES).collect();
    let (base_state, _) = disk_state(base.path());
    let base_ann = annotation_ids(&base_state);
    assert_eq!(base_ann.len(), 16);
    let base_spans: BTreeSet<u64> = base_state
        .parts
        .iter()
        .filter(|p| p.kind == PartKind::Spans)
        .map(|p| p.part_id)
        .collect();

    let cases = [
        ("MergeStarting", 1),
        ("MergeWritten", 1),
        ("BeforeReplaceCommit", 1),
        ("ReplaceMapBuilt", 1),
        ("ReplaceCommitted", 1),
        ("ReplacePublished", 1),
        ("PartDirDeleted", 1),
        ("MergeWritten", 3),
        ("PartDirDeleted", 5),
        ("ReplaceCommitted", 4),
    ];
    for (i, (stage, n)) in cases.into_iter().enumerate() {
        let label = format!("merge, {stage}:{n}");
        let dir = tempfile::tempdir().unwrap();
        copy_dir(base.path(), dir.path());
        let mut env = env(stage, n, "");
        env.push(("NWDB_COMPACT", "2".to_string()));
        env.push(("NWDB_BUFFER_BYTES", (1u64 << 30).to_string()));
        let run = run_child(dir.path(), 5_000_000, 0, &env, None);
        run.assert_aborted_at(stage);

        let (state, unreferenced) = disk_state(dir.path());
        let live_ann = annotation_ids(&state);
        let removed: Vec<u64> = base_ann.difference(&live_ann).copied().collect();
        // The span parts are never touched, and each merge takes the four
        // parts of a window.
        assert!(
            base_spans.iter().all(|id| state.part(*id).is_some()),
            "{label}"
        );
        // The merges whose commit the abort did not precede. The four
        // directories of a merge are deleted before the next merge starts:
        // the `n`-th deletion is in merge `(n - 1) / 4 + 1`.
        let committed = match stage {
            "MergeStarting" | "MergeWritten" | "BeforeReplaceCommit" | "ReplaceMapBuilt" => n - 1,
            "PartDirDeleted" => (n - 1) / 4 + 1,
            _ => n,
        };
        assert_eq!(removed.len() as u64, 4 * committed, "{label}: {removed:?}");
        let merged_live = state
            .parts
            .iter()
            .filter(|p| p.kind == PartKind::Annotations && p.level > 0)
            .count();
        assert_eq!(merged_live as u64, committed, "{label}");
        match stage {
            "MergeStarting" => assert!(unreferenced.is_empty(), "{label}: {unreferenced:?}"),
            "MergeWritten" | "BeforeReplaceCommit" | "ReplaceMapBuilt" => {
                assert_eq!(unreferenced.len(), 1, "{label}: {unreferenced:?}");
                assert!(
                    unreferenced.iter().all(|u| !u.ends_with(".tmp")),
                    "{label}: {unreferenced:?}"
                );
            }
            "ReplaceCommitted" | "ReplacePublished" => {
                assert_eq!(unreferenced.len(), 4, "{label}: {unreferenced:?}");
            }
            _ => assert!(
                unreferenced.len() < 4 && !unreferenced.is_empty(),
                "{label}: {unreferenced:?}"
            ),
        }

        let acked: BTreeSet<u64> = acked_base.iter().chain(&run.acked).copied().collect();
        let e = recover_and_check(dir.path(), &acked, i % 2 == 0, &label, 3_000 + i as u128);
        check_winners(&e, &want, &|_| false, &label);
        e.shutdown().unwrap();
        nothing_left_over(dir.path(), &label);
        // Finishing the merges changes no winner.
        let e = open(compact_opts(dir.path()));
        check_winners(&e, &want, &|_| false, &format!("{label}, merged"));
        compact_fully(&e);
        assert_eq!(e.stats().annotation_parts, 4, "{label}");
        check_winners(&e, &want, &|_| false, &format!("{label}, all merged"));
        e.shutdown().unwrap();
        nothing_left_over(dir.path(), &label);
        let e = open(opts(dir.path()));
        check_winners(&e, &want, &|_| false, &format!("{label}, restarted"));
        e.shutdown().unwrap();
    }
}

/// A crash at each stage of a retention replacement that holds annotation
/// parts: windows 0 and 1 expire, with their two span parts and their eight
/// annotation parts, in one manifest version. Before the commit nothing is
/// removed; after it nothing of those windows is live, in either kind; the
/// directories that were not deleted are leftovers the restart removes. A
/// record of an expired window is gone or, if the crash was before the commit,
/// still there with its winner; no record is half there.
#[test]
fn every_stage_of_a_retention_replacement_with_annotation_parts() {
    let base = tempfile::tempdir().unwrap();
    let want = maintenance_base(base.path());
    let acked_base: BTreeSet<u64> = (0..BASE_TRACES).collect();
    let (base_state, _) = disk_state(base.path());
    let expired = |i: u64| i % 4 < 2;
    let expired_ids: Vec<u64> = base_state
        .parts
        .iter()
        .filter(|p| p.window_start_ns < 2 * BASE_WINDOW)
        .map(|p| p.part_id)
        .collect();
    assert_eq!(expired_ids.len(), 10);
    let retention_ns = 1_000;
    let clock_ns = 2 * BASE_WINDOW + retention_ns;

    let cases = [
        ("BeforeReplaceCommit", 1),
        ("ReplaceMapBuilt", 1),
        ("ReplaceCommitted", 1),
        ("ReplacePublished", 1),
        ("PartDirDeleted", 1),
        ("PartDirDeleted", 6),
    ];
    for (i, (stage, n)) in cases.into_iter().enumerate() {
        let label = format!("retention, {stage}:{n}");
        let dir = tempfile::tempdir().unwrap();
        copy_dir(base.path(), dir.path());
        let mut env = env(stage, n, "");
        env.push(("NWDB_RETENTION_NS", retention_ns.to_string()));
        env.push(("NWDB_CLOCK_NS", clock_ns.to_string()));
        let run = run_child(dir.path(), 5_000_000, 0, &env, None);
        run.assert_aborted_at(stage);

        let (state, unreferenced) = disk_state(dir.path());
        let live_expired = expired_ids
            .iter()
            .filter(|id| state.part(**id).is_some())
            .count();
        let committed = !matches!(stage, "BeforeReplaceCommit" | "ReplaceMapBuilt");
        // One manifest version: all ten or none.
        assert_eq!(live_expired, if committed { 0 } else { 10 }, "{label}");
        if committed {
            let left = expired_ids
                .iter()
                .filter(|id| unreferenced.contains(&nwdb_core::part_dir_name(**id)))
                .count();
            let deleted = if stage == "PartDirDeleted" {
                n as usize
            } else {
                0
            };
            assert_eq!(left, 10 - deleted, "{label}: {unreferenced:?}");
        } else {
            assert!(unreferenced.is_empty(), "{label}: {unreferenced:?}");
        }

        // Recovery: after the commit the expired records are gone, before it
        // they are still there (the recovery does not apply retention).
        let acked: BTreeSet<u64> = acked_base
            .iter()
            .chain(&run.acked)
            .copied()
            .filter(|id| !(committed && *id < BASE_TRACES && expired(*id)))
            .collect();
        let e = recover_and_check(dir.path(), &acked, i % 2 == 0, &label, 4_000 + i as u128);
        check_winners(&e, &want, &|i| committed && expired(i), &label);
        e.shutdown().unwrap();
        nothing_left_over(dir.path(), &label);

        // With retention applied the records of the expired windows go, and
        // the others keep their winners.
        let mut o = opts(dir.path());
        o.retention = Some(std::time::Duration::from_nanos(retention_ns as u64));
        o.clock = Some(std::sync::Arc::new(move || clock_ns));
        let e = open(o);
        e.run_maintenance().unwrap();
        check_winners(&e, &want, &|i| expired(i), &format!("{label}, retained"));
        assert_eq!(e.stats().annotation_parts, 8, "{label}");
        e.shutdown().unwrap();
        nothing_left_over(dir.path(), &label);
    }
}
