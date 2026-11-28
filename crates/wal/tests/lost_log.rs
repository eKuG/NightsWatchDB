//! The log's position against the caller's: a log that ends below
//! `min_next_lsn` has lost files and is refused, and files wholly below it are
//! removed before anything else is checked.

use std::path::Path;

use nwdb_core::Error;
use nwdb_wal::{Recovery, Wal, WalOptions};

mod common;
use common::{fill, opts};

fn file_name(first: u64) -> String {
    format!("wal-{first:020}.log")
}

/// Names and contents of every file, to show a refusal changed nothing.
fn listing(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().into_string().unwrap(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

fn names(dir: &Path) -> Vec<String> {
    listing(dir).into_iter().map(|(n, _)| n).collect()
}

fn assert_lost_log(r: nwdb_core::Result<(Wal, Recovery)>) {
    match r {
        Err(Error::Corrupt(m)) => {
            assert!(m.contains("allow_lost_log"), "{m}");
            assert!(m.contains("log files were lost"), "{m}");
        }
        other => panic!("expected a refusal, got {:?}", other.map(|(_, r)| r)),
    }
}

fn with_min(min: u64) -> WalOptions {
    WalOptions {
        min_next_lsn: Some(min),
        ..opts(100)
    }
}

#[test]
fn a_missing_log_directory_is_refused_above_position_one_and_not_created() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("wal");
    assert_lost_log(Wal::open(&dir, with_min(8)));
    assert!(!dir.exists(), "the refusal created the directory");
    // A brand-new database is at position 1 and is not refused.
    let (wal, rec) = Wal::open(&dir, with_min(1)).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn, rec.reset_to), (1, 0, None));
    wal.shutdown().unwrap();
}

#[test]
fn an_empty_log_directory_is_refused_above_position_one_and_left_empty() {
    let dir = tempfile::tempdir().unwrap();
    assert_lost_log(Wal::open(dir.path(), with_min(8)));
    assert!(names(dir.path()).is_empty());
    let (wal, _) = Wal::open(dir.path(), with_min(1)).unwrap();
    wal.shutdown().unwrap();
}

#[test]
fn a_log_whose_newest_file_was_deleted_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 6); // files [1,2] [3,4] [5,6]
    let newest = dir.path().join(file_name(5));
    let saved = std::fs::read(&newest).unwrap();
    std::fs::remove_file(&newest).unwrap();

    // The log now ends at 4 while the caller has 6 in data files. File [1,2]
    // is wholly below 7 and goes, as a trim would; [3,4] is the newest.
    assert_lost_log(Wal::open(dir.path(), with_min(7)));
    assert_eq!(names(dir.path()), vec![file_name(3)]);
    let before = listing(dir.path());
    assert_lost_log(Wal::open(dir.path(), with_min(7)));
    assert_eq!(listing(dir.path()), before);

    // Restoring the file lets the log open, and numbering goes on from 7.
    // File [3,4] is wholly below 7 too, and goes now that it is not the newest.
    std::fs::write(&newest, saved).unwrap();
    let (wal, rec) = Wal::open(dir.path(), with_min(7)).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn, rec.reset_to), (5, 6, None));
    assert_eq!(wal.append_blocking(b"x").unwrap(), 7);
    wal.shutdown().unwrap();
}

#[test]
fn a_log_that_ends_exactly_at_the_position_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 4);
    let (wal, rec) = Wal::open(dir.path(), with_min(5)).unwrap();
    assert_eq!((rec.last_lsn, rec.reset_to), (4, None));
    wal.shutdown().unwrap();
}

#[test]
fn the_switch_allows_the_reset() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 4);
    let o = WalOptions {
        allow_lost_log: true,
        ..with_min(50)
    };
    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!(rec.reset_to, Some(50));
    assert_eq!(wal.append_blocking(b"x").unwrap(), 50);
    wal.shutdown().unwrap();
}

/// In healthy operation the engine's position is at most the log's last
/// number, and trimming keeps the newest file, so a trim past the end still
/// leaves a log that numbers on from where it was.
#[test]
fn trimming_never_removes_the_newest_file() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 6);
    let (wal, _) = Wal::open(dir.path(), opts(100)).unwrap();
    wal.truncate_before(u64::MAX).unwrap();
    assert_eq!(wal.stats().files, 1);
    wal.shutdown().unwrap();
    let (wal, rec) = Wal::open(dir.path(), with_min(7)).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn, rec.reset_to), (5, 6, None));
    assert_eq!(wal.append_blocking(b"x").unwrap(), 7);
    wal.shutdown().unwrap();
}

#[test]
fn files_wholly_below_the_position_are_removed_first_even_if_damaged() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 6); // [1,2] [3,4] [5,6]
    // Damage the oldest file and leave a gap after it.
    let oldest = dir.path().join(file_name(1));
    let mut bytes = std::fs::read(&oldest).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&oldest, bytes).unwrap();
    std::fs::remove_file(dir.path().join(file_name(3))).unwrap();

    // Without a position the damage is an error.
    assert!(matches!(
        Wal::open(dir.path(), opts(100)),
        Err(Error::Corrupt(_))
    ));
    // With position 5, records 1..=4 are in data files and the file goes.
    let (wal, rec) = Wal::open(dir.path(), with_min(5)).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn), (5, 6));
    assert_eq!(names(dir.path()), vec![file_name(5)]);
    wal.shutdown().unwrap();
}

#[test]
fn damage_in_a_file_that_is_still_needed_is_still_an_error() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(100), 6);
    let middle = dir.path().join(file_name(3));
    let mut bytes = std::fs::read(&middle).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&middle, bytes).unwrap();
    // Position 4 needs record 4, which is in the damaged file.
    assert!(matches!(
        Wal::open(dir.path(), with_min(4)),
        Err(Error::Corrupt(_))
    ));
}
