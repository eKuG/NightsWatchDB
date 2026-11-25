//! A database written with an older part format is refused at start-up, by
//! version, and nothing on disk changes.

mod common;
mod support;

use common::*;
use nwdb_core::Error;
use nwdb_engine::Engine;
use nwdb_segment::{FORMAT_VERSION, INDEX_FILE};
use support::{disk_state, tree};

fn flushed_database(dir: &std::path::Path) {
    let e = open(opts(dir));
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    e.flush().unwrap();
    e.shutdown().unwrap();
}

fn assert_refused_by_version(dir: &std::path::Path, old: u32) {
    let before = tree(dir);
    let err = Engine::open(opts(dir), decoders())
        .err()
        .expect("start-up accepted a part of an older format");
    let Error::Corrupt(msg) = &err else {
        panic!("{err}")
    };
    assert!(
        msg.contains(&format!(
            "format version {old} but this build reads version {FORMAT_VERSION}"
        )) && msg.contains("development versions")
            && msg.contains("no migration"),
        "{msg}"
    );
    assert_eq!(tree(dir), before, "the refused start-up changed the data");
}

#[test]
fn a_part_whose_index_records_the_previous_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    flushed_database(dir.path());
    let id = disk_state(dir.path()).0.parts[0].part_id;
    let index = dir
        .path()
        .join("parts")
        .join(nwdb_core::part_dir_name(id))
        .join(INDEX_FILE);
    let mut bytes = std::fs::read(&index).unwrap();
    bytes[4..8].copy_from_slice(&(FORMAT_VERSION - 1).to_le_bytes());
    std::fs::write(&index, bytes).unwrap();
    assert_refused_by_version(dir.path(), FORMAT_VERSION - 1);
}

#[test]
fn a_manifest_that_records_the_previous_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    flushed_database(dir.path());
    let mut state = (*disk_state(dir.path()).0).clone();
    assert_eq!(state.parts[0].format_version, FORMAT_VERSION);
    state.parts[0].format_version = FORMAT_VERSION - 1;
    let file = dir
        .path()
        .join("manifest")
        .join(format!("{:020}.json", state.version));
    std::fs::write(&file, state.encode().unwrap()).unwrap();
    assert_refused_by_version(dir.path(), FORMAT_VERSION - 1);
}
