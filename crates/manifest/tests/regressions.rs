//! Regression tests for the defects found in review. The first two tests are
//! the reviewer's, adapted only for the API changes the fixes made
//! (`Manifest::open` takes an `OpenMode`, `reserve_part_ids` returns a
//! `Result`, and `commit` now rejects an implausible `next_part_id`, so the
//! damaged file is written by hand).

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use nwdb_core::{Error, PartMeta, part_dir_name};
use nwdb_manifest::{
    Edit, MAX_NEXT_PART_ID, Manifest, ManifestOptions, ManifestState, OpenMode,
    highest_part_dir_id, unreferenced_part_dirs,
};

fn part(id: u64) -> PartMeta {
    PartMeta {
        part_id: id,
        level: 0,
        window_start_ns: 0,
        window_ns: 900_000_000_000,
        rows: 1,
        traces: 1,
        min_start_ns: 0,
        max_end_ns: 1,
        files: BTreeMap::new(),
        created_unix_ms: 0,
        format_version: 1,
        payload: Default::default(),
    }
}

fn add(ids: &[u64]) -> Edit {
    Edit {
        add: ids.iter().map(|&i| part(i)).collect(),
        ..Edit::default()
    }
}

fn opts() -> ManifestOptions {
    ManifestOptions::default()
}

fn vfile(v: u64) -> String {
    format!("{v:020}.json")
}

/// Finding 1 (reviewer's test): a missing manifest directory must not make
/// every part deletable. `MustExist` refuses; `Create` yields version 0, and
/// `unreferenced_part_dirs` then refuses to list final part directories.
#[test]
fn a_missing_manifest_does_not_make_every_part_deletable() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    let dir = tmp.path().join("manifest");
    let m = Manifest::open(&dir, OpenMode::Create, opts()).unwrap();
    m.commit(Edit {
        wal_flushed_lsn: Some(500),
        ..add(&[1, 2])
    })
    .unwrap();
    for id in [1, 2] {
        fs::create_dir_all(parts.join(part_dir_name(id))).unwrap();
    }
    drop(m);
    fs::remove_dir_all(&dir).unwrap();

    // Restart: the directory must exist.
    let err = Manifest::open(&dir, OpenMode::MustExist, opts())
        .err()
        .unwrap();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
    assert!(!dir.exists(), "MustExist must not create the directory");

    // Even if the directory is opened as version 0, the parts are never
    // offered for deletion.
    let m = Manifest::open(&dir, OpenMode::Create, opts()).unwrap();
    assert_eq!(m.current().version, 0);
    let err = unreferenced_part_dirs(&parts, &m.current()).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

/// Finding 5 (reviewer's test): a loaded `next_part_id` near `u64::MAX` is
/// rejected on load, so reservation can never panic.
#[test]
fn a_loaded_next_part_id_cannot_make_reservation_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let m = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();

    // An edit asking for it is invalid.
    for n in [u64::MAX, MAX_NEXT_PART_ID + 1] {
        let err = m
            .commit(Edit {
                next_part_id: Some(n),
                ..Edit::default()
            })
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    }
    // The limit itself is fine, and reservation beyond it is an error, not a
    // panic.
    m.commit(Edit {
        next_part_id: Some(MAX_NEXT_PART_ID),
        ..Edit::default()
    })
    .unwrap();
    let err = m.reserve_part_ids(1).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    assert!(m.reserve_part_ids(0).unwrap().is_empty());
    drop(m);

    // A damaged or hostile file (valid checksum) is Corrupt on load.
    let tmp = tempfile::tempdir().unwrap();
    let bad = ManifestState {
        version: 1,
        next_part_id: u64::MAX,
        ..ManifestState::empty()
    };
    fs::write(tmp.path().join(vfile(1)), bad.encode().unwrap()).unwrap();
    let err = Manifest::open(tmp.path(), OpenMode::Create, opts())
        .err()
        .unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

/// Finding 1: open modes.
#[test]
fn open_modes() {
    let tmp = tempfile::tempdir().unwrap();

    // MustExist: missing directory.
    let dir = tmp.path().join("nested").join("manifest");
    let err = Manifest::open(&dir, OpenMode::MustExist, opts())
        .err()
        .unwrap();
    assert!(matches!(err, Error::NotFound(_)), "{err}");

    // Create makes it (including parents) and starts at version 0.
    let m = Manifest::open(&dir, OpenMode::Create, opts()).unwrap();
    assert_eq!(*m.current(), ManifestState::empty());

    // MustExist: existing but empty (a temp file is not a manifest).
    fs::write(dir.join(".tmp-1-1-1"), "x").unwrap();
    let err = Manifest::open(&dir, OpenMode::MustExist, opts())
        .err()
        .unwrap();
    assert!(matches!(err, Error::NotFound(_)), "{err}");

    // Once a version exists both modes load it.
    m.commit(add(&[1])).unwrap();
    for mode in [OpenMode::MustExist, OpenMode::Create] {
        let again = Manifest::open(&dir, mode, opts()).unwrap();
        assert_eq!(again.current().version, 1);
    }
}

/// Finding 1: version 0 next to temporary directories only is fine, final
/// directories are not.
#[test]
fn version_zero_with_parts_refuses_but_temp_dirs_are_listed() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    let empty = ManifestState::empty();
    assert!(unreferenced_part_dirs(&parts, &empty).unwrap().is_empty());

    let t = parts.join(format!("{}.tmp", part_dir_name(3)));
    fs::create_dir_all(&t).unwrap();
    assert_eq!(unreferenced_part_dirs(&parts, &empty).unwrap(), [t]);

    fs::create_dir_all(parts.join(part_dir_name(3))).unwrap();
    assert!(matches!(
        unreferenced_part_dirs(&parts, &empty).unwrap_err(),
        Error::Corrupt(_)
    ));
}

/// Finding 3: a failure after the link poisons the handle.
#[test]
fn directory_sync_failure_after_publish_poisons_the_handle() {
    let tmp = tempfile::tempdir().unwrap();
    let m = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    let durable = m.commit(add(&[1])).unwrap();

    m.set_fail_dir_sync(true);
    let err = m.commit(add(&[2])).unwrap_err();
    assert!(matches!(err, Error::Fatal(_)), "{err}");
    // The file is on disk, but current() did not advance.
    assert!(tmp.path().join(vfile(2)).exists());
    assert_eq!(*m.current(), *durable);

    // Even once the disk "recovers", the handle stays unusable.
    m.set_fail_dir_sync(false);
    assert!(matches!(m.commit(add(&[3])).unwrap_err(), Error::Fatal(_)));
    assert!(matches!(m.reload().unwrap_err(), Error::Fatal(_)));
    assert_eq!(*m.current(), *durable);
    assert!(!tmp.path().join(vfile(3)).exists());

    // A restart recovers whatever was published.
    let again = Manifest::open(tmp.path(), OpenMode::MustExist, opts()).unwrap();
    assert_eq!(again.current().version, 2);
}

/// Finding 3: failures before the link leave everything unchanged and keep
/// the original error; the handle is not poisoned.
#[test]
fn failures_before_the_link_do_not_poison() {
    let tmp = tempfile::tempdir().unwrap();
    let a = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    let b = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    a.commit(add(&[1])).unwrap();

    assert!(matches!(
        b.commit(add(&[2])).unwrap_err(),
        Error::Conflict(_)
    ));
    assert!(matches!(
        b.commit(Edit {
            remove: vec![77],
            ..Edit::default()
        })
        .unwrap_err(),
        Error::InvalidArgument(_)
    ));
    b.reload().unwrap();
    assert_eq!(b.commit(add(&[2])).unwrap().version, 2);
}

/// Finding 4: readers do not wait for a commit in progress.
#[test]
fn current_does_not_wait_for_a_commit_in_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let m = Arc::new(Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap());
    m.commit(add(&[1])).unwrap();

    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    m.set_commit_hook(Some(Arc::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    })));

    let writer = {
        let m = m.clone();
        std::thread::spawn(move || m.commit(add(&[2])).unwrap().version)
    };
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    // The commit is parked inside the writer lock. A reader still returns at
    // once, with the old state.
    let (tx, rx) = mpsc::channel();
    let reader = {
        let m = m.clone();
        std::thread::spawn(move || tx.send(m.current().version).unwrap())
    };
    let seen = rx.recv_timeout(Duration::from_secs(5));
    release_tx.send(()).unwrap();
    assert_eq!(seen, Ok(1), "current() blocked behind a commit");
    reader.join().unwrap();
    assert_eq!(writer.join().unwrap(), 2);
    assert_eq!(m.current().version, 2);
}

/// Finding 5: edits that would pass the limit are rejected, and a part id
/// just below it is accepted.
#[test]
fn implausible_ids_are_rejected_in_edits() {
    let tmp = tempfile::tempdir().unwrap();
    let m = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    for id in [MAX_NEXT_PART_ID, u64::MAX] {
        let err = m.commit(add(&[id])).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    }
    let s = m.commit(add(&[MAX_NEXT_PART_ID - 1])).unwrap();
    assert_eq!(s.next_part_id, MAX_NEXT_PART_ID);
}

/// Finding 6: highest id among final and temporary directories.
#[test]
fn highest_part_dir_id_sees_final_and_temporary_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), None);
    fs::create_dir_all(&parts).unwrap();
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), None);

    fs::create_dir_all(parts.join(part_dir_name(4))).unwrap();
    fs::create_dir_all(parts.join("scratch")).unwrap();
    fs::write(parts.join(part_dir_name(50)), "a file").unwrap();
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), Some(4));

    fs::create_dir_all(parts.join(format!("{}.tmp", part_dir_name(9)))).unwrap();
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), Some(9));
    fs::create_dir_all(parts.join(part_dir_name(7))).unwrap();
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), Some(9));
}

/// Finding 6: the documented startup rule, end to end.
#[test]
fn startup_rule_never_reuses_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    let dir = tmp.path().join("manifest");
    let m = Manifest::open(&dir, OpenMode::Create, opts()).unwrap();
    m.commit(add(&[1, 2])).unwrap();
    // A crashed flush left ids 3 (final) and 4 (temporary) unreferenced.
    fs::create_dir_all(parts.join(part_dir_name(1))).unwrap();
    fs::create_dir_all(parts.join(part_dir_name(2))).unwrap();
    fs::create_dir_all(parts.join(part_dir_name(3))).unwrap();
    fs::create_dir_all(parts.join(format!("{}.tmp", part_dir_name(4)))).unwrap();
    drop(m);

    let m = Manifest::open(&dir, OpenMode::MustExist, opts()).unwrap();
    let highest = highest_part_dir_id(&parts, m.current().next_part_id)
        .unwrap()
        .unwrap();
    let s = m
        .commit(Edit {
            next_part_id: Some(highest + 1),
            ..Edit::default()
        })
        .unwrap();
    let doomed = unreferenced_part_dirs(&parts, &s).unwrap();
    assert_eq!(doomed.len(), 2);
    for d in doomed {
        fs::remove_dir_all(d).unwrap();
    }
    assert_eq!(m.reserve_part_ids(1).unwrap(), 5..6);
}

/// Finding 7: which error means what after a lost race.
#[test]
fn error_meanings_after_a_lost_race() {
    let tmp = tempfile::tempdir().unwrap();
    let a = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    let b = Manifest::open(tmp.path(), OpenMode::Create, opts()).unwrap();
    a.commit(add(&[1, 2])).unwrap();
    b.reload().unwrap();
    a.commit(Edit {
        remove: vec![1],
        ..Edit::default()
    })
    .unwrap();

    // Stale handle: its edit is valid against what it knows, so the answer
    // is Conflict, not InvalidArgument.
    let stale_edit = Edit {
        remove: vec![1],
        ..Edit::default()
    };
    assert!(matches!(
        b.commit(stale_edit.clone()).unwrap_err(),
        Error::Conflict(_)
    ));
    // After reloading, the same edit is invalid against the state the caller
    // now sees: InvalidArgument, a caller bug rather than a race.
    b.reload().unwrap();
    assert!(matches!(
        b.commit(stale_edit).unwrap_err(),
        Error::InvalidArgument(_)
    ));
    // Adding an id that is already live likewise.
    assert!(matches!(
        b.commit(add(&[2])).unwrap_err(),
        Error::InvalidArgument(_)
    ));
}

/// Directories far above `next_part_id`, or at or above the limit, are
/// strays: they do not count for the highest id, but they still count as part
/// directories.
#[test]
fn huge_part_dir_ids_do_not_raise_the_highest_id() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    assert!(!nwdb_manifest::has_part_dirs(&parts).unwrap());
    fs::create_dir_all(parts.join(part_dir_name(3))).unwrap();
    fs::create_dir_all(parts.join(part_dir_name(MAX_NEXT_PART_ID - 1))).unwrap();
    fs::create_dir_all(parts.join(part_dir_name(u64::MAX))).unwrap();
    fs::create_dir_all(parts.join(format!("{}.tmp", part_dir_name(MAX_NEXT_PART_ID)))).unwrap();
    assert_eq!(highest_part_dir_id(&parts, 1).unwrap(), Some(3));
    assert!(nwdb_manifest::has_part_dirs(&parts).unwrap());
}

/// The leftover window: an id just below `next_part_id + window` counts, one
/// at or above it does not, whatever `next_part_id` is.
#[test]
fn highest_part_dir_id_ignores_ids_a_window_above_next_part_id() {
    use nwdb_manifest::PART_ID_LEFTOVER_WINDOW as W;
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    for next in [1, 1000, MAX_NEXT_PART_ID - 5, MAX_NEXT_PART_ID - 1] {
        let _ = fs::remove_dir_all(&parts);
        fs::create_dir_all(parts.join(part_dir_name(next.saturating_sub(1)))).unwrap();
        // Just inside the window, unless that is past the limit.
        let inside = (next + W - 1).min(MAX_NEXT_PART_ID - 1);
        fs::create_dir_all(parts.join(part_dir_name(inside))).unwrap();
        assert_eq!(highest_part_dir_id(&parts, next).unwrap(), Some(inside));
        // At the window's edge (or at the limit) and above: ignored.
        let _ = fs::remove_dir_all(parts.join(part_dir_name(inside)));
        for edge in [next + W, next + W + 1, MAX_NEXT_PART_ID, u64::MAX] {
            fs::create_dir_all(parts.join(part_dir_name(edge))).unwrap();
        }
        assert_eq!(
            highest_part_dir_id(&parts, next).unwrap(),
            Some(next.saturating_sub(1)),
            "next_part_id {next}"
        );
    }
}
