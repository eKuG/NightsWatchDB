//! A start-up that refuses must leave the data directory exactly as it found
//! it, so that restoring what was lost lets the database open with all its
//! data.

mod common;
mod support;

use std::path::Path;

use common::*;
use nwdb_core::Error;
use nwdb_engine::{Engine, EngineOptions};
use nwdb_wal::{SyncMode, Wal, WalOptions};
use support::{copy_dir, tree};

fn one(trace: u128) -> Vec<TSpan> {
    vec![TSpan::new(trace, 1, 100 + trace as i64).root()]
}

/// One record per log file, so trimming is exact.
fn small_files(dir: &Path) -> EngineOptions {
    let mut o = opts(dir);
    o.wal.max_file_bytes = 1;
    o
}

/// A database with six records in two flushes, shut down cleanly. The model
/// holds what it must contain.
fn two_flushes(dir: &Path) -> Model {
    let mut m = Model::default();
    let e = open(small_files(dir));
    for t in 1..=6 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
        if t == 3 {
            e.flush().unwrap();
        }
    }
    e.flush().unwrap();
    e.shutdown().unwrap();
    m
}

/// Opens, expecting a refusal, and checks the directory did not change.
fn refused(o: EngineOptions, dir: &Path) -> Error {
    let before = tree(dir);
    let err = Engine::open(o, decoders())
        .err()
        .expect("start-up accepted a damaged directory");
    assert_eq!(
        tree(dir),
        before,
        "the refused start-up changed the data directory ({err})"
    );
    err
}

fn is_corrupt(e: &Error) -> bool {
    matches!(e, Error::Corrupt(_))
}

fn opens_with_everything(o: EngineOptions, m: &Model, what: &str) {
    let e = open(o);
    m.check(&e, what);
    e.shutdown().unwrap();
}

fn manifest_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir.join("manifest"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    v.sort();
    v
}

#[test]
fn a_lost_newest_manifest_file() {
    let dir = tempfile::tempdir().unwrap();
    let m = two_flushes(dir.path());
    let newest = manifest_files(dir.path()).pop().unwrap();
    let saved = std::fs::read(&newest).unwrap();
    std::fs::remove_file(&newest).unwrap();

    let err = refused(small_files(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    std::fs::write(&newest, saved).unwrap();
    opens_with_everything(small_files(dir.path()), &m, "after restoring the file");
}

#[test]
fn an_older_manifest_restored() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(small_files(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    e.flush().unwrap();
    let old = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("manifest"), old.path());
    for t in 4..=6 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    e.flush().unwrap();
    e.shutdown().unwrap();

    let current = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("manifest"), current.path());
    std::fs::remove_dir_all(dir.path().join("manifest")).unwrap();
    copy_dir(old.path(), &dir.path().join("manifest"));

    let err = refused(small_files(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    std::fs::remove_dir_all(dir.path().join("manifest")).unwrap();
    copy_dir(current.path(), &dir.path().join("manifest"));
    opens_with_everything(small_files(dir.path()), &m, "after restoring the manifest");
}

#[test]
fn manifest_and_parts_lost_with_a_trimmed_log() {
    let dir = tempfile::tempdir().unwrap();
    let m = two_flushes(dir.path());
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("manifest"), &saved.path().join("manifest"));
    copy_dir(&dir.path().join("parts"), &saved.path().join("parts"));
    std::fs::remove_dir_all(dir.path().join("manifest")).unwrap();
    std::fs::remove_dir_all(dir.path().join("parts")).unwrap();

    let err = refused(small_files(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    assert!(
        !dir.path().join("manifest").exists(),
        "a first manifest version was committed before the log was checked"
    );
    copy_dir(&saved.path().join("manifest"), &dir.path().join("manifest"));
    copy_dir(&saved.path().join("parts"), &dir.path().join("parts"));
    opens_with_everything(small_files(dir.path()), &m, "after restoring both");
}

#[test]
fn a_log_that_starts_too_late() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=5 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    drop(e);
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), saved.path());
    // A log whose first record is number 3, while the manifest says nothing
    // has been flushed: records 1 and 2 are missing.
    let wal_dir = dir.path().join("wal");
    std::fs::remove_dir_all(&wal_dir).unwrap();
    let (wal, _) = Wal::open(
        &wal_dir,
        WalOptions {
            sync: SyncMode::None,
            min_next_lsn: Some(3),
            allow_lost_log: true,
            ..WalOptions::default()
        },
    )
    .unwrap();
    wal.shutdown().unwrap();

    let err = refused(opts(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    std::fs::remove_dir_all(&wal_dir).unwrap();
    copy_dir(saved.path(), &wal_dir);
    opens_with_everything(opts(dir.path()), &m, "after restoring the log");
}

#[test]
fn a_lost_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    e.flush().unwrap();
    // Acknowledged and not flushed: only the log holds them.
    for t in 4..=5 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    drop(e);
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), saved.path());
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();

    let err = refused(opts(dir.path()), dir.path());
    let text = err.to_string();
    assert!(is_corrupt(&err), "{text}");
    assert!(text.contains("accept_lost_log"), "{text}");
    // The range of records the log lacks, and what may be gone.
    assert!(text.contains("1..=3"), "{text}");
    assert!(text.contains("4 and above"), "{text}");

    // The newest file lost, not the whole directory: the same refusal.
    copy_dir(saved.path(), &dir.path().join("wal"));
    let mut newest: Vec<_> = std::fs::read_dir(dir.path().join("wal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    newest.sort();
    let newest = newest.pop().unwrap();
    let bytes = std::fs::read(&newest).unwrap();
    // Only one file here: removing it leaves an empty log directory.
    std::fs::remove_file(&newest).unwrap();
    let err = refused(opts(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    std::fs::write(&newest, bytes).unwrap();
    opens_with_everything(opts(dir.path()), &m, "after restoring the log");
}

/// An accepted loss is a choice: the records after the last flush are gone,
/// the rest is intact and the engine numbers on above the position.
#[test]
fn a_lost_log_can_be_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    e.flush().unwrap();
    ingest(&e, &one(4)).unwrap();
    drop(e);
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();

    let mut o = opts(dir.path());
    o.accept_lost_log = true;
    let e = open(o);
    m.check(&e, "everything flushed is there");
    assert!(e.get_trace(one(4)[0].trace_id()).unwrap().is_none());
    ingest(&e, &one(5)).unwrap();
    assert_eq!(e.stats().acked_lsn, 4);
    e.shutdown().unwrap();
}

#[test]
fn an_undecodable_log_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    drop(e);
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), saved.path());
    let (wal, _) = Wal::open(
        dir.path().join("wal"),
        WalOptions {
            sync: SyncMode::None,
            ..WalOptions::default()
        },
    )
    .unwrap();
    wal.append_blocking(&[77, 1, 2, 3]).unwrap();
    wal.shutdown().unwrap();

    let err = refused(opts(dir.path()), dir.path());
    assert!(is_corrupt(&err), "{err}");
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
    copy_dir(saved.path(), &dir.path().join("wal"));
    opens_with_everything(opts(dir.path()), &m, "after restoring the log");
}

/// Part directories the manifest does not reference may be the only decoded
/// copy of data whose log record no longer decodes: they are deleted only
/// after the replay worked.
#[test]
fn leftover_parts_survive_a_refused_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        m.add(&one(t));
    }
    drop(e);
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&dir.path().join("wal"), saved.path());
    let (wal, _) = Wal::open(
        dir.path().join("wal"),
        WalOptions {
            sync: SyncMode::None,
            ..WalOptions::default()
        },
    )
    .unwrap();
    wal.append_blocking(&[77, 1, 2, 3]).unwrap();
    wal.shutdown().unwrap();
    let leftover = dir.path().join("parts").join("00000000000000000007");
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("spans.parquet"), b"junk").unwrap();

    let err = Engine::open(opts(dir.path()), decoders())
        .err()
        .expect("start-up accepted an undecodable record");
    assert!(is_corrupt(&err), "{err}");
    assert!(leftover.is_dir(), "the refused start-up deleted a leftover");

    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
    copy_dir(saved.path(), &dir.path().join("wal"));
    opens_with_everything(opts(dir.path()), &m, "after restoring the log");
    assert!(!leftover.exists());
}

#[test]
fn a_live_part_that_does_not_open() {
    let dir = tempfile::tempdir().unwrap();
    let m = two_flushes(dir.path());
    let part = std::fs::read_dir(dir.path().join("parts"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let saved = tempfile::tempdir().unwrap();
    copy_dir(&part, saved.path());
    std::fs::remove_dir_all(&part).unwrap();

    let err = refused(small_files(dir.path()), dir.path());
    assert!(
        matches!(err, Error::NotFound(_) | Error::Io(_) | Error::Corrupt(_)),
        "{err}"
    );
    copy_dir(saved.path(), &part);
    opens_with_everything(small_files(dir.path()), &m, "after restoring the part");
}

#[test]
fn bad_arguments_are_refused_before_the_disk_is_touched() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("db");
    let mut o = opts(&dir);
    o.window_ns = 0;
    assert!(matches!(
        Engine::open(o, decoders()),
        Err(Error::InvalidArgument(_))
    ));
    assert!(!dir.exists());
    let mut two = decoders();
    two.extend(decoders());
    assert!(matches!(
        Engine::open(opts(&dir), two),
        Err(Error::InvalidArgument(_))
    ));
    assert!(!dir.exists());
}

#[test]
fn the_directory_lock_does_not_depend_on_a_file_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    assert!(!dir.path().join("LOCK").exists());
    // Whatever is deleted or created in the directory, a second engine is
    // refused while the first runs.
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let p = entry.unwrap().path();
        if p.is_file() {
            std::fs::remove_file(p).unwrap();
        }
    }
    std::fs::write(dir.path().join("LOCK"), b"").unwrap();
    std::fs::remove_file(dir.path().join("LOCK")).unwrap();
    assert!(matches!(
        Engine::open(opts(dir.path()), decoders()),
        Err(Error::Conflict(_))
    ));
    e.shutdown().unwrap();
    open(opts(dir.path())).shutdown().unwrap();
}
