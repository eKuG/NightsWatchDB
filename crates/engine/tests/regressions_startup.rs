//! Review tests: start-up on on-disk states built by hand. Each test fails on
//! the code it was written against and names the defect it shows.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use nwdb_core::{Error, TraceId};
use nwdb_engine::{Engine, EngineOptions};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};

fn manifest_state(dir: &Path) -> std::sync::Arc<nwdb_manifest::ManifestState> {
    Manifest::open(
        dir.join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current()
}

fn one(trace: u128) -> Vec<TSpan> {
    vec![TSpan::new(trace, 1, 100 + trace as i64).root()]
}

/// Options whose log keeps one record per file, so trimming is exact.
fn small_files(dir: &Path) -> EngineOptions {
    let mut o = opts(dir);
    o.wal.max_file_bytes = 1;
    o
}

fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out: Vec<(PathBuf, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| (e.path(), std::fs::read(e.path()).unwrap()))
        .collect();
    out.sort();
    out
}

/// The newest manifest file is lost (or an older copy of the manifest
/// directory is restored) after the log was trimmed past the older version's
/// position. Start-up first commits a new manifest version and deletes the
/// part directories the older version does not list, and only then opens the
/// log and finds that it does not reach back far enough. It refuses to start,
/// but the parts it deleted held the only copy of acknowledged records.
#[test]
fn a_refused_start_up_keeps_the_parts_of_the_lost_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    let o = small_files(dir.path());
    let e = open(o.clone());
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
    }
    e.flush().unwrap();
    for t in 4..=6 {
        ingest(&e, &one(t)).unwrap();
    }
    e.flush().unwrap();
    e.shutdown().unwrap();
    drop(e);

    let st = manifest_state(dir.path());
    assert_eq!(st.wal_flushed_lsn, 6);
    assert_eq!(st.parts.len(), 2);
    let newest = dir
        .path()
        .join("manifest")
        .join(format!("{:020}.json", st.version));
    let second_part = dir
        .path()
        .join("parts")
        .join(nwdb_core::part_dir_name(st.parts[1].part_id));
    let manifest_files_before = files(&dir.path().join("manifest"));

    // Lose the newest manifest version: the older one says records up to 3
    // are in parts, but the log was trimmed up to 6.
    std::fs::remove_file(&newest).unwrap();
    let manifest_files_lost: Vec<_> = manifest_files_before
        .iter()
        .filter(|(p, _)| *p != newest)
        .cloned()
        .collect();

    let r = Engine::open(o, decoders());
    assert!(
        r.is_err(),
        "start-up accepted a log that does not reach back"
    );
    assert!(
        second_part.is_dir(),
        "the refused start-up deleted {} (records 4 and 5, acknowledged, exist nowhere else)",
        second_part.display()
    );
    assert_eq!(
        files(&dir.path().join("manifest")),
        manifest_files_lost,
        "the refused start-up committed a new manifest version"
    );
}

/// A crash between the manifest commit and the trim leaves log files that
/// lie wholly inside parts. If one of them is damaged, start-up refuses,
/// although nothing in it is needed: every record in it is below
/// `wal_flushed_lsn + 1`.
#[test]
fn a_damaged_log_file_wholly_inside_parts_does_not_block_start_up() {
    let dir = tempfile::tempdir().unwrap();
    let o = small_files(dir.path());
    let e = open(o.clone());
    let mut m = Model::default();
    for t in 1..=4 {
        let s = one(t);
        ingest(&e, &s).unwrap();
        m.add(&s);
    }
    let wal = dir.path().join("wal");
    let before_trim = files(&wal);
    e.flush().unwrap();
    e.shutdown().unwrap();
    drop(e);
    assert_eq!(manifest_state(dir.path()).wal_flushed_lsn, 4);

    // The state of a crash after the commit and before the trim.
    for (p, b) in &before_trim {
        if !p.exists() {
            std::fs::write(p, b).unwrap();
        }
    }
    // Damage the payload of the oldest file, which only holds record 1.
    let oldest = &before_trim[0].0;
    let mut bytes = std::fs::read(oldest).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(oldest, bytes).unwrap();

    let r = Engine::open(o, decoders());
    let e = match r {
        Ok(e) => e,
        Err(err) => {
            panic!("start-up refused a database whose damaged log file is not needed: {err}")
        }
    };
    m.check(&e, "after start-up");
    e.shutdown().unwrap();
}

/// A stray directory whose name is a 20-digit number near the id limit is
/// taken as a part id that must never be reused. Start-up deletes the
/// directory but raises `next_part_id` to the limit for good, and from then
/// on every flush fails: the engine can never write a part again.
#[test]
fn a_stray_part_directory_with_a_huge_id_does_not_stop_later_flushes() {
    let dir = tempfile::tempdir().unwrap();
    open(opts(dir.path())).shutdown().unwrap();
    let stray = dir.path().join("parts").join(nwdb_core::part_dir_name(
        nwdb_manifest::MAX_NEXT_PART_ID - 1,
    ));
    std::fs::create_dir_all(&stray).unwrap();

    let e = open(opts(dir.path()));
    assert!(!stray.exists());
    ingest(&e, &one(1)).unwrap();
    let r = e.flush();
    assert!(r.is_ok(), "flush failed after a stray directory: {r:?}");
    assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_some());
    e.shutdown().unwrap();
}

/// The directory lock is a lock on the directory itself, not on a file in
/// it: there is no `LOCK` file to delete, and removing the files that are
/// there does not let a second engine open the directory while the first is
/// still running. (Adjusted from the reviewer's test, which unlinked `LOCK`:
/// that file no longer exists.)
#[test]
fn deleting_files_in_the_directory_does_not_let_a_second_engine_open_it() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    assert!(!dir.path().join("LOCK").exists());
    let _ = std::fs::remove_file(dir.path().join("LOCK"));
    let second = Engine::open(opts(dir.path()), decoders());
    let refused = matches!(second, Err(Error::Conflict(_)));
    drop(second);
    e.shutdown().unwrap();
    assert!(
        refused,
        "a second engine opened a directory that a running engine uses"
    );
}
