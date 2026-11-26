//! Deterministic crash injection. The crash-test child aborts itself at a
//! named stage of a flush or of start-up; the test checks what that stage
//! leaves on disk, then recovers and checks that every acknowledged record
//! is present exactly once.

mod common;
mod support;

use std::collections::BTreeSet;

use support::{
    copy_dir, disk_state, obsolete_log_files, present_ids, recover_and_check, run_child, tree_of,
};

fn abort_env(stage: &str, n: u64) -> Vec<(&'static str, String)> {
    vec![("NWDB_ABORT_AT", format!("{stage}:{n}"))]
}

/// Runs a child on `dir` until it aborts at `stage`, the `n`-th time.
fn crash_at(dir: &std::path::Path, first_id: u64, stage: &str, n: u64) -> Vec<u64> {
    let run = run_child(dir, first_id, 0, &abort_env(stage, n), None);
    run.assert_aborted_at(stage);
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
        "LogRotated",
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
            "Committed" | "Published" | "LogRotated" => {
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
        run.assert_aborted_at(stage);
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

/// A crash in the middle of a replacement of parts, which retention starts
/// with a fixed clock when the child opens a directory that holds expired
/// parts. Nothing of the expired parts may come back, nothing acknowledged
/// that was not in them may be lost, and the directories the manifest no
/// longer references are deleted by the recovery.
#[test]
fn every_replace_stage_leaves_a_recoverable_directory() {
    // The base: eight flushes, so parts of several windows, and a log with
    // the records after the last flush.
    let base = tempfile::tempdir().unwrap();
    let acked_before: BTreeSet<u64> = crash_at(base.path(), 0, "Trimmed", 8).into_iter().collect();
    let (state, _) = disk_state(base.path());
    let mut parts: Vec<_> = state.parts.iter().collect();
    parts.sort_by_key(|p| p.window_start_ns);
    // Expire the oldest windows until at least three parts are expired, and
    // keep at least one.
    let mut expired_end = i64::MIN;
    let mut expired = Vec::new();
    for p in &parts {
        if expired.len() >= 3 && p.window_start_ns + p.window_ns > expired_end {
            break;
        }
        expired_end = expired_end.max(p.window_start_ns + p.window_ns);
        expired.push(p.part_id);
    }
    assert!(expired.len() >= 3, "the base has too few parts");
    assert!(
        expired.len() < parts.len(),
        "the base has too few windows: {:?}",
        parts.iter().map(|p| p.window_start_ns).collect::<Vec<_>>()
    );
    let expired_names: Vec<String> = expired
        .iter()
        .map(|id| nwdb_core::part_dir_name(*id))
        .collect();
    // The ids that the expired parts hold, from a copy of the base opened
    // without retention.
    let in_expired: BTreeSet<u64> = {
        let copy = tempfile::tempdir().unwrap();
        copy_dir(base.path(), copy.path());
        let e = common::open(common::opts(copy.path()));
        let ids: BTreeSet<u64> = e
            .snapshot()
            .part_traces()
            .into_iter()
            .filter(|(id, _)| expired.contains(id))
            .flat_map(|(_, traces)| traces)
            .map(|t| u128::from_be_bytes(t.0) as u64 - 1)
            .collect();
        e.shutdown().unwrap();
        ids
    };
    assert!(!in_expired.is_empty());
    let retention_ns = 1_000;
    let env = |stage: &str| {
        let mut env = abort_env(stage, 1);
        env.push(("NWDB_RETENTION_NS", retention_ns.to_string()));
        env.push(("NWDB_CLOCK_NS", (expired_end + retention_ns).to_string()));
        env
    };

    for (i, stage) in ["ReplaceCommitted", "ReplacePublished", "PartDirDeleted"]
        .into_iter()
        .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        copy_dir(base.path(), dir.path());
        let run = run_child(dir.path(), 5_000_000, 0, &env(stage), None);
        run.assert_aborted_at(stage);

        // The commit is one manifest version: no expired part is live.
        let (state, unreferenced) = disk_state(dir.path());
        assert!(
            expired.iter().all(|id| state.part(*id).is_none()),
            "{stage}: an expired part is still live"
        );
        let left: Vec<&String> = expired_names
            .iter()
            .filter(|n| unreferenced.contains(n))
            .collect();
        match stage {
            "ReplaceCommitted" | "ReplacePublished" => {
                assert_eq!(left.len(), expired.len(), "{stage}: {unreferenced:?}");
            }
            // The abort came right after the first directory was deleted.
            _ => assert_eq!(left.len(), expired.len() - 1, "{stage}: {unreferenced:?}"),
        }

        // Recovery: every acknowledged id that was not in an expired part is
        // there, none of the expired parts' ids is, and the leftovers are
        // gone.
        let acked: BTreeSet<u64> = acked_before
            .iter()
            .chain(&run.acked)
            .copied()
            .filter(|id| !in_expired.contains(id))
            .collect();
        let e = recover_and_check(dir.path(), &acked, i % 2 == 0, stage, 200 + i as u128);
        let present = present_ids(&e);
        assert!(
            present.is_disjoint(&in_expired),
            "{stage}: expired data came back"
        );
        e.shutdown().unwrap();
        let (_, unreferenced) = disk_state(dir.path());
        assert!(unreferenced.is_empty(), "{stage}: {unreferenced:?}");
    }
}
