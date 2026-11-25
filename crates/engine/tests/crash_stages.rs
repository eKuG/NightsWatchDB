//! Deterministic crash injection. The crash-test child aborts itself at a
//! named stage of a flush or of start-up; the test checks what that stage
//! leaves on disk, then recovers and checks that every acknowledged record
//! is present exactly once.

mod common;
mod support;

use std::collections::BTreeSet;

use support::{copy_dir, disk_state, obsolete_log_files, recover_and_check, run_child, tree_of};

fn abort_env(stage: &str, n: u64) -> Vec<(&'static str, String)> {
    vec![("NWDB_ABORT_AT", format!("{stage}:{n}"))]
}

/// Runs a child on `dir` until it aborts at `stage`, the `n`-th time.
fn crash_at(dir: &std::path::Path, first_id: u64, stage: &str, n: u64) -> Vec<u64> {
    let run = run_child(dir, first_id, 0, &abort_env(stage, n), None);
    assert!(!run.success, "the child did not abort at {stage}");
    run.acked
}

/// After each stage of a flush: what is on disk, and that recovery is right.
#[test]
fn every_flush_stage_leaves_a_recoverable_directory() {
    // The second flush of the child: the first one has already committed, so
    // the manifest holds parts and a position, and the log was trimmed once.
    let stages = [
        "FlushFrozen",
        "PartWritten",
        "AllPartsWritten",
        "Committed",
        "Published",
        "Trimmed",
    ];
    for (i, stage) in stages.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let n = if stage == "PartWritten" { 3 } else { 2 };
        let acked = crash_at(dir.path(), 0, stage, n);
        assert!(!acked.is_empty(), "{stage}: nothing was acknowledged");

        let (state, unreferenced) = disk_state(dir.path());
        let obsolete = obsolete_log_files(dir.path(), state.wal_flushed_lsn);
        match stage {
            "FlushFrozen" => {
                assert!(unreferenced.is_empty(), "{stage}: {unreferenced:?}");
            }
            "PartWritten" | "AllPartsWritten" => {
                // Finished parts the manifest does not know; none is half
                // written, because the writer renames a part into place only
                // when it is complete.
                assert!(!unreferenced.is_empty(), "{stage}");
                assert!(
                    unreferenced.iter().all(|n| !n.ends_with(".tmp")),
                    "{stage}: {unreferenced:?}"
                );
            }
            "Committed" | "Published" => {
                assert!(unreferenced.is_empty(), "{stage}: {unreferenced:?}");
                assert!(state.wal_flushed_lsn > 0, "{stage}");
                assert!(obsolete > 0, "{stage}: no obsolete log file is left");
            }
            "Trimmed" => {
                assert!(unreferenced.is_empty(), "{stage}: {unreferenced:?}");
                assert_eq!(obsolete, 0, "{stage}: the log was not trimmed");
            }
            _ => unreachable!(),
        }

        let acked: BTreeSet<u64> = acked.into_iter().collect();
        let e = recover_and_check(dir.path(), &acked, i % 2 == 1, stage, i as u128);
        e.shutdown().unwrap();
    }
}

/// A crash inside start-up, with leftovers on disk and a log to replay, and a
/// buffer so small that the replay itself flushes.
#[test]
fn every_open_stage_leaves_a_recoverable_directory() {
    // The base: a child that dies with finished parts the manifest does not
    // know, a committed first flush, and acknowledged records in the log.
    let base = tempfile::tempdir().unwrap();
    let acked: BTreeSet<u64> = crash_at(base.path(), 0, "AllPartsWritten", 2)
        .into_iter()
        .collect();
    let (base_state, base_leftovers) = disk_state(base.path());
    assert!(!base_leftovers.is_empty());

    let recovery_env = |stage: &str| {
        let mut env = abort_env(stage, 1);
        env.push(("NWDB_BUFFER_BYTES", "2048".to_string()));
        env
    };
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
        let before = tree_of(dir.path(), &["manifest", "parts"]);

        let run = run_child(dir.path(), 5_000_000, 0, &recovery_env(stage), None);
        assert!(!run.success, "the child did not abort at {stage}");
        let after = tree_of(dir.path(), &["manifest", "parts"]);
        let (state, unreferenced) = disk_state(dir.path());
        match stage {
            "OpenChecked" => {
                assert_eq!(
                    after, before,
                    "start-up changed the disk before its checks passed"
                );
            }
            "IdsRaised" => {
                // The leftovers hold ids at or above `next_part_id`; the only
                // change is the manifest version that raises it.
                let parts = |t: &std::collections::BTreeMap<String, (u64, u64)>| {
                    t.iter()
                        .filter(|(k, _)| k.starts_with("parts/"))
                        .map(|(k, v)| (k.clone(), *v))
                        .collect::<Vec<_>>()
                };
                assert_eq!(parts(&after), parts(&before));
                assert!(state.next_part_id > base_state.next_part_id);
                assert_eq!(state.wal_flushed_lsn, base_state.wal_flushed_lsn);
            }
            "ReplayFinished" => {
                // The replay flushed, so the position moved; the leftovers
                // are still there until the replay is known to have worked.
                assert!(state.wal_flushed_lsn > base_state.wal_flushed_lsn);
                for d in &base_leftovers {
                    assert!(unreferenced.contains(d), "{d} was deleted too early");
                }
            }
            "LeftoversDeleted" => {
                assert!(unreferenced.is_empty(), "{unreferenced:?}");
            }
            _ => unreachable!(),
        }

        let e = recover_and_check(dir.path(), &acked, i % 2 == 0, stage, 100 + i as u128);
        e.shutdown().unwrap();
        let (_, unreferenced) = disk_state(dir.path());
        assert!(unreferenced.is_empty(), "{stage}: {unreferenced:?}");
    }
}
