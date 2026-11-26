//! After a flush the log starts a new file, so the trim that follows removes
//! everything that is now in parts; and a copy of the directory taken at each
//! point of the flush starts and replays exactly the records not in parts.

mod common;
mod support;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use common::*;
use nwdb_engine::{Engine, Stage};
use support::{copy_dir, present_ids};

/// The files of the log and their sizes.
fn wal_files(dir: &Path) -> Vec<(String, u64)> {
    let mut v: Vec<_> = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().unwrap().len(),
            )
        })
        .collect();
    v.sort();
    v
}

fn wal_bytes(dir: &Path) -> u64 {
    wal_files(dir).iter().map(|(_, n)| n).sum()
}

/// A record of about 4 KiB that is one span; `id` is its trace.
fn big(id: u64) -> TSpan {
    let mut s = TSpan::new(u128::from(id) + 1, 1, (id as i64) * 100).root();
    s.attrs = vec![(id % 251) as u8; 4_000];
    s
}

#[test]
fn after_a_flush_the_log_holds_only_a_small_file() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for id in 0..500 {
        ingest(&e, &[big(id)]).unwrap();
    }
    let before = wal_bytes(dir.path());
    assert!(before > 2_000_000, "{before}");
    e.flush().unwrap();
    let after = wal_files(dir.path());
    eprintln!("wal/ before the flush: {before} bytes in 1 file, after: {after:?}");
    // One file, holding a header and nothing else.
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(after[0].1, 20, "{after:?}");

    // The same when nothing is flushed by this call but the log is full of
    // flushed records (here: a restart that replayed and flushed, then a
    // flush with nothing new).
    for id in 500..520 {
        ingest(&e, &[big(id)]).unwrap();
    }
    e.flush().unwrap();
    assert_eq!(wal_files(dir.path()).len(), 1);
    e.flush().unwrap();
    assert_eq!(wal_files(dir.path()).len(), 1);
    assert_eq!(e.stats().flushed_lsn, 520);

    // Records that arrive after a flush stay in the log until the next one.
    ingest(&e, &[big(600)]).unwrap();
    assert!(wal_bytes(dir.path()) > 4_000);
    e.flush().unwrap();
    assert_eq!(wal_bytes(dir.path()), 20);
    e.shutdown().unwrap();
    assert_eq!(wal_bytes(dir.path()), 20);

    // It opens, replays nothing and numbers on from where it was.
    let e = open(opts(dir.path()));
    let s = e.stats();
    assert_eq!((s.acked_lsn, s.flushed_lsn), (521, 521));
    ingest(&e, &[big(700)]).unwrap();
    assert_eq!(e.stats().acked_lsn, 522);
    e.shutdown().unwrap();
}

#[test]
fn shutdown_leaves_the_log_small_even_when_the_last_flush_had_nothing_to_do() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for id in 0..50 {
        ingest(&e, &[big(id)]).unwrap();
    }
    e.shutdown().unwrap();
    assert_eq!(wal_bytes(dir.path()), 20);
    // A restart with a log that is all flushed (a state older versions left
    // behind: one file with records) is cleaned by the first flush.
    let e = open(opts(dir.path()));
    e.flush().unwrap();
    assert_eq!(wal_bytes(dir.path()), 20);
    e.shutdown().unwrap();
}

/// A copy of the data directory at each stage of the second flush, with two
/// more records acknowledged after the freeze.
#[test]
fn a_copy_taken_at_each_point_of_a_flush_replays_exactly_the_unflushed_records() {
    const FIRST: u64 = 10;
    const IN_FLUSH: u64 = 10;
    const LATE: u64 = 2;
    let live = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let copies: Arc<Mutex<Vec<(Stage, std::path::PathBuf)>>> = Arc::default();
    let engine: Arc<Mutex<Option<Engine>>> = Arc::default();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut o = opts(live.path());
    o.wal.sync = nwdb_wal::SyncMode::Data;
    o.stage_hook = Some({
        let (copies, engine, armed) = (copies.clone(), engine.clone(), armed.clone());
        let live = live.path().to_path_buf();
        let scratch = scratch.path().to_path_buf();
        Arc::new(move |stage| {
            use std::sync::atomic::Ordering::SeqCst;
            if !armed.load(SeqCst) {
                return Ok(());
            }
            if stage == Stage::FlushFrozen {
                // Acknowledged after the freeze: they stay in the log.
                let e = engine.lock().unwrap().clone().unwrap();
                for id in FIRST + IN_FLUSH..FIRST + IN_FLUSH + LATE {
                    ingest(&e, &[big(id)]).unwrap();
                }
            }
            let mut c = copies.lock().unwrap();
            let to = scratch.join(format!("{}-{stage:?}", c.len()));
            copy_dir(&live, &to);
            c.push((stage, to));
            Ok(())
        })
    });
    let e = open(o);
    *engine.lock().unwrap() = Some(e.clone());
    for id in 0..FIRST {
        ingest(&e, &[big(id)]).unwrap();
    }
    e.flush().unwrap();
    for id in FIRST..FIRST + IN_FLUSH {
        ingest(&e, &[big(id)]).unwrap();
    }
    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    e.flush().unwrap();
    armed.store(false, std::sync::atomic::Ordering::SeqCst);
    let everything: BTreeSet<u64> = (0..FIRST + IN_FLUSH + LATE).collect();

    let copies = copies.lock().unwrap().clone();
    let stages: Vec<Stage> = copies.iter().map(|(s, _)| *s).collect();
    // The ten records of the flush are all in one window: one part.
    assert_eq!(
        stages,
        [
            Stage::FlushFrozen,
            Stage::PartWritten,
            Stage::AllPartsWritten,
            Stage::Committed,
            Stage::Published,
            Stage::LogRotated,
            Stage::Trimmed,
        ]
    );
    for (stage, copy) in &copies {
        let r = open_copy(copy);
        let s = r.stats();
        // Before the commit the manifest still says FIRST records are in
        // parts; from the commit on it says FIRST + IN_FLUSH.
        let committed = !matches!(
            stage,
            Stage::FlushFrozen | Stage::PartWritten | Stage::AllPartsWritten
        );
        let in_parts = if committed { FIRST + IN_FLUSH } else { FIRST };
        assert_eq!(s.flushed_lsn, in_parts, "{stage:?}");
        assert_eq!(
            s.acked_lsn - s.flushed_lsn,
            FIRST + IN_FLUSH + LATE - in_parts,
            "{stage:?}: replayed the wrong records"
        );
        assert_eq!(present_ids(&r), everything, "{stage:?}");
        // And the log of the recovered copy is usable and small once flushed.
        r.flush().unwrap();
        assert_eq!(wal_bytes(copy), 20, "{stage:?}");
        r.shutdown().unwrap();
    }
    e.shutdown().unwrap();
    // The hook holds a handle to the engine: drop it so the test ends clean.
    engine.lock().unwrap().take();
}

fn open_copy(dir: &Path) -> Engine {
    let mut o = opts(dir);
    o.wal.sync = nwdb_wal::SyncMode::Data;
    open(o)
}
