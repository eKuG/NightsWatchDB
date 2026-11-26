//! The maintenance pass and the log rotation against what the README promises:
//! retention within its bound while compaction is busy, a failed rotation that
//! is reported and retried (or fatal when the sync failed), option values that
//! are accepted by `open`, and shutdown during a pass.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::Error;
use nwdb_engine::{Engine, ListQuery, Stage, StageHook};

fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let t = Instant::now();
    while !cond() {
        if t.elapsed() > limit {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
}

/// Retention is applied at least every maintenance interval, also in a pass
/// that keeps finding merges because flushes keep giving it work.
#[test]
fn retention_is_not_held_up_by_a_pass_that_keeps_finding_merges() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(0));
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let feeding = Arc::new(AtomicBool::new(true));
    let fed = Arc::new(AtomicU64::new(0));
    // Far ahead of the test clock, so never expired in this test.
    const LATER: i64 = 2_000_000;
    let hook: StageHook = {
        let (slot, feeding, fed) = (slot.clone(), feeding.clone(), fed.clone());
        Arc::new(move |s| {
            if s == Stage::MergeWritten && feeding.load(Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone();
                if let Some(e) = e {
                    // A flush of the busy window lands while the merge runs,
                    // as one would under steady ingest.
                    std::thread::sleep(Duration::from_millis(10));
                    let n = fed.fetch_add(1, Ordering::SeqCst);
                    let s = TSpan::new(10_000 + u128::from(n), 1, LATER + (n as i64 % 500));
                    ingest(&e, &[s.root()])?;
                    e.flush()?;
                }
            }
            Ok(())
        })
    };
    let mut o = compact_opts(dir.path());
    o.compaction.background = true;
    o.maintenance_interval = Duration::from_millis(20);
    o.retention = Some(Duration::from_nanos(10_000));
    o.clock = Some({
        let clock = clock.clone();
        Arc::new(move || clock.load(Ordering::SeqCst))
    });
    o.stage_hook = Some(hook);
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());

    // A part in window 0, not expired at the clock's 0.
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    e.flush().unwrap();
    // Two parts of a window far ahead: the thread merges them, and each merge
    // brings one more part of that window.
    for t in 2..=3u128 {
        ingest(&e, &[TSpan::new(t, 1, LATER).root()]).unwrap();
        e.flush().unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(30), || fed.load(Ordering::SeqCst) >= 3),
        "setup: the merges did not start"
    );

    // Window 0 expires now. The bound is a few intervals of 20 ms.
    clock.store(1_000_000, Ordering::SeqCst);
    let expired_at = Instant::now();
    let removed = wait_until(Duration::from_secs(2), || {
        e.stats().retention_parts_removed >= 1
    });
    let waited = expired_at.elapsed();
    let merges = e.stats().merges_done;

    feeding.store(false, Ordering::SeqCst);
    e.shutdown().unwrap();
    slot.lock().unwrap().take();
    assert!(
        removed,
        "the expired part was still live {waited:?} after it expired (maintenance interval \
         20 ms); the pass did {merges} merges meanwhile"
    );
}

fn list_all() -> ListQuery {
    ListQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        service: None,
        model: None,
        error: None,
        limit: usize::MAX,
    }
}

/// The log files and their sizes.
fn wal_files(dir: &std::path::Path) -> Vec<(String, u64)> {
    let mut v: Vec<_> = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().unwrap().len(),
            )
        })
        .filter(|(name, _)| name.ends_with(".log"))
        .collect();
    v.sort();
    v
}

/// If the new log file cannot be created, the log keeps writing to the current
/// one, the flush reports the error, and the next flush tries again. Nothing
/// is poisoned.
#[test]
fn a_log_file_that_cannot_be_created_is_reported_and_retried_and_ingest_goes_on() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    // The new file would start at record 2 and be built under this name: a
    // directory there makes creating it fail.
    let blocker = dir.path().join("wal/wal-00000000000000000002.log.tmp");
    std::fs::create_dir(&blocker).unwrap();
    let err = e.flush().unwrap_err();
    assert!(!matches!(err, Error::Fatal(_)), "{err}");
    let s = e.stats();
    assert!(
        s.last_error
            .as_deref()
            .is_some_and(|m| m.contains("rotating the log failed")),
        "{:?}",
        s.last_error
    );
    assert!(!s.fatal);
    // The data is in a part, and ingest goes on into the same log file.
    assert_eq!(s.parts, 1);
    ingest(&e, &[TSpan::new(2, 1, 200).root()]).unwrap();
    assert_eq!(wal_files(dir.path()).len(), 1);

    std::fs::remove_dir(&blocker).unwrap();
    e.flush().unwrap();
    assert!(!e.stats().fatal);
    // The next flush started the file and trimmed the old one.
    assert_eq!(wal_files(dir.path()).len(), 1);
    assert_eq!(wal_files(dir.path())[0].1, 20);
    ingest(&e, &[TSpan::new(3, 1, 300).root()]).unwrap();
    e.shutdown().unwrap();

    // Nothing was lost or doubled.
    let e = open(opts(dir.path()));
    let all = e.list_traces(&list_all()).unwrap();
    assert_eq!(all.len(), 3);
    e.shutdown().unwrap();
}

/// A failed sync of the current file in a rotation is a failed sync like any
/// other: the engine is fatal at once, and the flush says so.
#[test]
fn a_failed_sync_in_a_log_rotation_makes_the_engine_fatal_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let fail_next = Arc::new(AtomicBool::new(false));
    let mut o = opts(dir.path());
    o.wal.sync_hook = Some({
        let f = fail_next.clone();
        Arc::new(move || {
            if f.swap(false, Ordering::SeqCst) {
                Err(std::io::Error::other("injected sync failure"))
            } else {
                Ok(())
            }
        })
    });
    let e = open(o);
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    // The next sync of the log is the one of the rotation after this flush.
    fail_next.store(true, Ordering::SeqCst);
    let flushed = e.flush();
    let fatal = e.stats().fatal;
    let _ = e.shutdown();
    assert!(
        matches!(flushed, Err(Error::Fatal(_))),
        "the flush reported {flushed:?}"
    );
    assert!(fatal, "the engine is not fatal right after the failed sync");
}

/// `open` accepts any positive `maintenance_interval`, and the thread "wakes
/// every `maintenance_interval` ... and after every flush", with a first pass
/// at once. With a very long interval the thread panics computing its first
/// deadline and never runs a pass, so retention never happens.
#[test]
fn a_very_long_maintenance_interval_still_runs_passes_after_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.maintenance_interval = Duration::MAX;
    let e = open(o);
    ingest(&e, &[TSpan::new(1, 1, 100).root()]).unwrap();
    e.flush().unwrap();
    let ran = wait_until(Duration::from_secs(5), || e.stats().maintenance_passes >= 1);
    let _ = e.shutdown();
    assert!(ran, "no maintenance pass ran after start-up and a flush");
}

/// `open` accepts any `retry_backoff`. A long one (meaning: do not retry a
/// failing window until a restart) overflows when the failure is recorded,
/// the pass panics, and a merge that failed harmlessly puts the engine in the
/// fatal state.
#[test]
fn a_very_long_retry_backoff_does_not_make_a_harmless_merge_failure_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let failed = Arc::new(AtomicBool::new(false));
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::MAX;
    o.stage_hook = Some({
        let failed = failed.clone();
        Arc::new(move |s| {
            if s == Stage::MergeWritten && !failed.swap(true, Ordering::SeqCst) {
                return Err(Error::other("injected failure"));
            }
            Ok(())
        })
    });
    let e = open(o);
    for t in 1..=2u128 {
        ingest(&e, &[TSpan::new(t, 1, 100).root()]).unwrap();
        e.flush().unwrap();
    }
    let err = e.run_maintenance().unwrap_err();
    let fatal = e.stats().fatal;
    let _ = e.shutdown();
    assert!(
        !fatal && !matches!(err, Error::Fatal(_)),
        "a non-fatal merge failure made the engine fatal: {err}"
    );
}

/// Shutdown during a pass with merges still to do: no further merge is
/// started, and an error of the merge that is running is not counted as a
/// failure of its window (no backoff, not fatal). The error is injected at the
/// stage after the merge's output is written, once shutdown has begun, as the
/// error of a merge that was asked to stop would arrive.
#[test]
fn shutdown_during_a_pass_starts_no_further_merge_and_a_merge_error_then_is_no_failure() {
    let dir = tempfile::tempdir().unwrap();
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let shutdown: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
    let merges = Arc::new(AtomicU64::new(0));
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    o.stage_hook = Some({
        let (slot, shutdown, merges) = (slot.clone(), shutdown.clone(), merges.clone());
        Arc::new(move |s| {
            if s != Stage::MergeWritten {
                return Ok(());
            }
            if merges.fetch_add(1, Ordering::SeqCst) == 0 {
                let e = slot.lock().unwrap().clone().unwrap();
                let e2 = e.clone();
                *shutdown.lock().unwrap() = Some(std::thread::spawn(move || {
                    let _ = e2.shutdown();
                }));
                // Ingest is refused once shutdown has begun, which is also
                // when it has asked maintenance to stop.
                let t = Instant::now();
                while ingest(&e, &[TSpan::new(99, 1, 5).root()]).is_ok() {
                    assert!(
                        t.elapsed() < Duration::from_secs(30),
                        "shutdown did not begin"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                return Err(Error::other("injected failure"));
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    // Three windows with two parts each: three merges are due.
    for round in 0..2u128 {
        for w in 0..3i64 {
            ingest(
                &e,
                &[TSpan::new(round * 10 + w as u128 + 1, 1, w * 1_000 + 1).root()],
            )
            .unwrap();
        }
        e.flush().unwrap();
    }
    let outcome = e.run_maintenance();
    shutdown.lock().unwrap().take().unwrap().join().unwrap();
    let s = e.stats();
    slot.lock().unwrap().take();
    assert_eq!(
        merges.load(Ordering::SeqCst),
        1,
        "a merge started after shutdown began"
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(s.last_compaction_error, None);
    assert!(!s.fatal);
}

/// Shutdown while a merge is in progress: the merge ends without finishing,
/// leaves no merged part and no directory behind, is no failure, and after a
/// restart every span is there (also the ones the hook sent while shutdown was
/// beginning, which are flushed by the shutdown) and the merge can be done. The hook starts the
/// shutdown just before `merge_parts` is called, so the merge begins with the
/// stop flag already set and ends at its first check.
#[test]
fn shutdown_while_a_merge_is_in_progress_abandons_it_and_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let shutdown: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
    let starts = Arc::new(AtomicU64::new(0));
    let model = Arc::new(Mutex::new(Model::default()));
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    o.stage_hook = Some({
        let (slot, shutdown, starts) = (slot.clone(), shutdown.clone(), starts.clone());
        let model = model.clone();
        Arc::new(move |s| {
            if s == Stage::MergeStarting && starts.fetch_add(1, Ordering::SeqCst) == 0 {
                let e = slot.lock().unwrap().clone().unwrap();
                let e2 = e.clone();
                *shutdown.lock().unwrap() = Some(std::thread::spawn(move || {
                    let _ = e2.shutdown();
                }));
                // Ingest is refused once shutdown has begun, which is also
                // when it has set the stop flag.
                // What it accepts is in the model.
                let t = Instant::now();
                loop {
                    let n = starts.fetch_add(1, Ordering::SeqCst) as u128;
                    let span = TSpan::new(1_000 + n, 1, 5).root();
                    if ingest(&e, std::slice::from_ref(&span)).is_err() {
                        break;
                    }
                    model.lock().unwrap().add(&[span]);
                    assert!(
                        t.elapsed() < Duration::from_secs(30),
                        "shutdown did not begin"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    for round in 0..3u128 {
        let spans: Vec<TSpan> = (0..5u128)
            .map(|k| TSpan::new(round * 10 + k + 1, 1, 1 + k as i64).root())
            .collect();
        model.lock().unwrap().add(&spans);
        ingest(&e, &spans).unwrap();
        e.flush().unwrap();
    }
    let parts_before = part_dir_names(dir.path());
    assert_eq!(parts_before.len(), 3);
    let outcome = e.run_maintenance();
    shutdown.lock().unwrap().take().unwrap().join().unwrap();
    let s = e.stats();
    slot.lock().unwrap().take();
    drop(e);
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(s.merges_done, 0, "{s:?}");
    assert_eq!(s.last_compaction_error, None);
    assert!(!s.fatal);
    // The shutdown's own flush may have added one part; the merge added none and
    // left no temporary directory.
    let parts_after = part_dir_names(dir.path());
    assert!(
        parts_before.iter().all(|p| parts_after.contains(p)),
        "an input was removed: {parts_after:?}"
    );
    assert!(
        parts_after.len() <= parts_before.len() + 1
            && parts_after.iter().all(|p| !p.contains("tmp")),
        "the abandoned merge left a directory: {parts_after:?}"
    );

    // After a restart every span is there, and the merge can be done.
    let e = open(compact_opts(dir.path()));
    let model = model.lock().unwrap().clone();
    model.check(&e, "after the restart");
    compact_fully(&e);
    assert!(e.stats().merges_done >= 1);
    model.check(&e, "after the merge");
    e.shutdown().unwrap();
}
