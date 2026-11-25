//! Failures injected through the stage hook: an error or a panic at a named
//! point of a flush or of start-up, in the running engine.

mod common;
mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use nwdb_core::Error;
use nwdb_engine::{Engine, EngineOptions, Stage, StageHook};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use support::tree;

#[derive(Clone, Copy)]
enum Action {
    NonFatal,
    Fatal,
    Panic,
}

/// A hook that does `action` once, the first time `stage` is reached.
fn once(stage: Stage, action: Action) -> (StageHook, Arc<AtomicBool>) {
    let armed = Arc::new(AtomicBool::new(true));
    let flag = armed.clone();
    let hook: StageHook = Arc::new(move |s| {
        if s == stage && flag.swap(false, Ordering::SeqCst) {
            match action {
                Action::NonFatal => return Err(Error::other("injected failure")),
                Action::Fatal => return Err(Error::fatal("injected fatal failure")),
                Action::Panic => panic!("injected panic"),
            }
        }
        Ok(())
    });
    (hook, armed)
}

fn with_hook(dir: &Path, hook: StageHook) -> EngineOptions {
    let mut o = opts(dir);
    o.stage_hook = Some(hook);
    o
}

/// Five traces in five different windows, so a flush writes five parts.
fn spread(t: u128) -> Vec<TSpan> {
    vec![TSpan::new(t, 1, t as i64 * 5_000).root()]
}

fn fill(e: &Engine, m: &mut Model, range: std::ops::RangeInclusive<u128>) {
    for t in range {
        ingest(e, &spread(t)).unwrap();
        m.add(&spread(t));
    }
}

fn part_dirs(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir.join("parts"))
        .into_iter()
        .flatten()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

fn manifest_ids(dir: &Path) -> Vec<u64> {
    Manifest::open(
        dir.join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current()
    .parts
    .iter()
    .map(|p| p.part_id)
    .collect()
}

#[test]
fn a_non_fatal_error_before_the_commit_changes_nothing_and_the_retry_works() {
    for stage in [
        Stage::FlushFrozen,
        Stage::PartWritten,
        Stage::AllPartsWritten,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, armed) = once(stage, Action::NonFatal);
        let e = open(with_hook(dir.path(), hook));
        let mut m = Model::default();
        fill(&e, &mut m, 1..=5);
        let before = e.stats();

        let err = e.flush().unwrap_err();
        assert!(!armed.load(Ordering::SeqCst));
        assert!(!matches!(err, Error::Fatal(_)), "{stage:?}: {err}");
        let after = e.stats();
        assert_eq!(after.flushes, 0, "{stage:?}");
        assert_eq!(after.flushed_lsn, 0, "{stage:?}");
        assert_eq!(after.parts, 0, "{stage:?}");
        assert_eq!(after.acked_lsn, before.acked_lsn, "{stage:?}");
        assert_eq!(after.buffered_bytes, before.buffered_bytes, "{stage:?}");
        assert_eq!(after.records_ingested, before.records_ingested);
        assert!(after.last_error.is_some());
        assert!(!after.flushing);
        m.check(&e, "the data stays visible after the failed flush");
        assert!(
            part_dirs(dir.path()).is_empty(),
            "{stage:?}: {:?}",
            part_dirs(dir.path())
        );

        // The engine is healthy: it accepts more, and the retry succeeds.
        fill(&e, &mut m, 6..=6);
        e.flush().unwrap();
        assert_eq!(e.stats().flushes, 1);
        assert_eq!(e.stats().flushed_lsn, 6);
        m.check(&e, "after the retry");
        e.shutdown().unwrap();
        let ids = manifest_ids(dir.path());
        assert_eq!(ids.len(), 6, "{stage:?}");
        if stage != Stage::FlushFrozen {
            // The failed attempt reserved ids 1..=5; none is used again.
            assert!(ids.iter().all(|&id| id > 5), "{stage:?}: {ids:?}");
        }
        assert_eq!(part_dirs(dir.path()).len(), 6, "{stage:?}");
    }
}

#[test]
fn a_fatal_error_stops_the_engine_and_a_restart_recovers_everything() {
    let dir = tempfile::tempdir().unwrap();
    let (hook, _) = once(Stage::AllPartsWritten, Action::Fatal);
    let e = open(with_hook(dir.path(), hook));
    let mut m = Model::default();
    fill(&e, &mut m, 1..=5);

    assert!(matches!(e.flush(), Err(Error::Fatal(_))));
    assert!(matches!(e.flush(), Err(Error::Fatal(_))));
    assert!(matches!(ingest(&e, &spread(9)), Err(Error::Fatal(_))));
    // Reads still work, and see the frozen data.
    m.check(&e, "reads in the fatal state");
    let _ = e.shutdown();
    drop(e);

    let e = open(opts(dir.path()));
    m.check(&e, "after the restart");
    e.shutdown().unwrap();
}

#[test]
fn a_panic_before_the_commit_is_a_fatal_error_and_loses_nothing() {
    for stage in [
        Stage::FlushFrozen,
        Stage::PartWritten,
        Stage::AllPartsWritten,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, _) = once(stage, Action::Panic);
        let e = open(with_hook(dir.path(), hook));
        let mut m = Model::default();
        fill(&e, &mut m, 1..=5);

        let err = e.flush().unwrap_err();
        assert!(
            matches!(&err, Error::Fatal(m) if m.contains("injected panic")),
            "{stage:?}: {err}"
        );
        // Without the fatal state, this flush would freeze the active buffer
        // over the frozen one and commit a position covering both.
        assert!(matches!(e.flush(), Err(Error::Fatal(_))), "{stage:?}");
        assert!(matches!(ingest(&e, &spread(9)), Err(Error::Fatal(_))));
        m.check(&e, "the frozen buffer stays visible");
        assert_eq!(e.stats().flushed_lsn, 0);
        let _ = e.shutdown();
        drop(e);

        let e = open(opts(dir.path()));
        m.check(&e, "after the restart");
        e.shutdown().unwrap();
    }
}

#[test]
fn a_panic_after_the_commit_is_ignored() {
    for stage in [Stage::Committed, Stage::Published, Stage::Trimmed] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, armed) = once(stage, Action::Panic);
        let e = open(with_hook(dir.path(), hook));
        let mut m = Model::default();
        fill(&e, &mut m, 1..=3);
        e.flush().unwrap();
        assert!(!armed.load(Ordering::SeqCst), "{stage:?} was not reached");
        assert_eq!(e.stats().flushed_lsn, 3);
        m.check(&e, "after a flush whose hook panicked");
        fill(&e, &mut m, 4..=4);
        e.flush().unwrap();
        e.shutdown().unwrap();
        let e = open(opts(dir.path()));
        m.check(&e, "after the restart");
        e.shutdown().unwrap();
    }
}

#[test]
fn an_error_after_the_commit_is_ignored() {
    for stage in [Stage::Committed, Stage::Published, Stage::Trimmed] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, armed) = once(stage, Action::Fatal);
        let e = open(with_hook(dir.path(), hook));
        let mut m = Model::default();
        fill(&e, &mut m, 1..=3);
        e.flush().unwrap();
        assert!(!armed.load(Ordering::SeqCst), "{stage:?} was not reached");
        m.check(&e, "after the flush");
        e.shutdown().unwrap();
    }
}

#[test]
fn an_error_at_an_open_stage_fails_the_open_and_the_next_open_works() {
    for stage in [
        Stage::OpenChecked,
        Stage::IdsRaised,
        Stage::ReplayFinished,
        Stage::LeftoversDeleted,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Model::default();
        let e = open(opts(dir.path()));
        fill(&e, &mut m, 1..=3);
        drop(e);
        // A leftover and a buffer small enough that the replay flushes.
        let leftover = dir.path().join("parts").join("00000000000000000007");
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("spans.parquet"), b"junk").unwrap();
        let before = tree(dir.path());

        let (hook, _) = once(stage, Action::NonFatal);
        let mut o = with_hook(dir.path(), hook);
        o.buffer_bytes = 2048;
        let err = Engine::open(o, decoders()).err().expect("the hook failed");
        assert!(err.to_string().contains("injected failure"), "{err}");
        if stage == Stage::OpenChecked {
            assert_eq!(tree(dir.path()), before, "changes before the first stage");
        }
        if stage != Stage::LeftoversDeleted {
            assert!(leftover.is_dir(), "{stage:?}: the leftover is gone");
        }

        let mut o = opts(dir.path());
        o.buffer_bytes = 2048;
        let e = open(o);
        m.check(&e, "after the failed open");
        assert!(!leftover.exists());
        e.shutdown().unwrap();
    }
}
