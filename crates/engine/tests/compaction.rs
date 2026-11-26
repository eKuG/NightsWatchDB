//! Compaction: merging the parts of a window, driven by explicit maintenance
//! passes where a test needs to decide when, and by the maintenance thread
//! where it tests the thread.

mod common;
mod support;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::{Error, TraceId, part_dir_name};
use nwdb_engine::{CompactionOptions, Engine, EngineOptions, ListQuery, Stage, StageHook};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use support::disk_state;

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

fn all() -> ListQuery {
    ListQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        service: None,
        model: None,
        error: None,
        limit: usize::MAX,
    }
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let t = Instant::now();
    while !cond() {
        assert!(t.elapsed() < Duration::from_secs(60), "timed out: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Feeds an engine and a model: one part per window per flush.
struct Feeder {
    next_span: u64,
}

impl Feeder {
    fn new() -> Self {
        Feeder { next_span: 0 }
    }

    /// One flush's worth of data: three traces of two spans in each of
    /// `windows`. Every span of a trace is in the window of the trace, so a
    /// trace is never spread over windows.
    fn round(&mut self, e: &Engine, m: &mut Model, windows: &[i64]) {
        for &w in windows {
            let spans: Vec<TSpan> = (0..6)
                .map(|i| {
                    self.next_span += 1;
                    // Traces `w*10 + 0..3`: the same trace gets spans in
                    // several rounds, so traces are split over parts.
                    let trace = (w as u128) * 10 + (i % 3) as u128 + 1;
                    let mut s =
                        TSpan::new(trace, self.next_span, w * W + (self.next_span as i64 % 900));
                    s.attrs = vec![(self.next_span % 7) as u8; 150];
                    if i < 3 && self.next_span % 6 == 1 {
                        s = s.root();
                    }
                    s
                })
                .collect();
            ingest(e, &spans).unwrap();
            m.add(&spans);
        }
        e.flush().unwrap();
    }
}

fn manifest_ids(dir: &Path) -> Vec<u64> {
    disk_state(dir).0.parts.iter().map(|p| p.part_id).collect()
}

fn snapshot_ids(e: &Engine) -> Vec<u64> {
    e.snapshot()
        .part_traces()
        .iter()
        .map(|(id, _)| *id)
        .collect()
}

/// The disk, the manifest and the queries agree on the parts, and nothing
/// else is on disk.
fn assert_consistent(dir: &Path, e: &Engine, what: &str) {
    let ids = snapshot_ids(e);
    assert_eq!(manifest_ids(dir), ids, "{what}: manifest");
    let names: Vec<String> = ids.iter().map(|i| part_dir_name(*i)).collect();
    assert_eq!(part_dir_names(dir), names, "{what}: directories");
}

/// A hook that stops at a stage until released.
struct Pause {
    stage: Stage,
    reached: AtomicBool,
    released: Mutex<bool>,
    cv: Condvar,
}

impl Pause {
    fn new(stage: Stage) -> Arc<Self> {
        Arc::new(Pause {
            stage,
            reached: AtomicBool::new(false),
            released: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    fn hook(self: &Arc<Self>) -> StageHook {
        let p = self.clone();
        Arc::new(move |s| {
            if s == p.stage {
                p.reached.store(true, Ordering::SeqCst);
                let mut r = p.released.lock().unwrap();
                while !*r {
                    r = p.cv.wait(r).unwrap();
                }
            }
            Ok(())
        })
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

#[derive(Clone, Copy)]
enum Inject {
    NonFatal,
    Fatal,
    Panic,
}

/// A hook that does `what` the first `times` times `stage` is reached.
fn fail(stage: Stage, what: Inject, times: u64) -> (StageHook, Arc<AtomicU64>) {
    let left = Arc::new(AtomicU64::new(times));
    let l = left.clone();
    let hook: StageHook = Arc::new(move |s| {
        if s == stage
            && l.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            match what {
                Inject::NonFatal => return Err(Error::other("injected failure")),
                Inject::Fatal => return Err(Error::fatal("injected fatal failure")),
                Inject::Panic => panic!("injected panic"),
            }
        }
        Ok(())
    });
    (hook, left)
}

#[test]
fn a_span_sent_again_across_a_flush_is_counted_twice_until_the_merge_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let spans = vec![
        TSpan::new(1, 1, 10).root(),
        TSpan::new(1, 2, 20),
        TSpan::new(2, 1, 30).root(),
    ];
    let mut m = Model::default();
    ingest(&e, &spans).unwrap();
    m.add(&spans);
    e.flush().unwrap();
    // The same request again, after the flush: the sender retried.
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 2);

    let counts = |e: &Engine| -> Vec<u32> {
        let mut l = e.list_traces(&all()).unwrap();
        l.sort_by_key(|s| s.trace_id);
        l.iter().map(|s| s.span_count).collect()
    };
    let agg = |e: &Engine| -> u64 {
        e.aggregate_tokens(&nwdb_engine::AggQuery {
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            operation: None,
        })
        .unwrap()
        .iter()
        .map(|a| a.count)
        .sum()
    };
    assert_eq!(counts(&e), vec![4, 2], "doubled before the merge");
    assert_eq!(agg(&e), 6);
    // get_trace removes them at once; the model does too.
    m.check_snapshot_ignoring(&e.snapshot(), "before", &BTreeSet::from([1, 2]));
    assert_eq!(
        from_batches(&e.get_trace(spans[0].trace_id()).unwrap().unwrap()).len(),
        2
    );

    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1);
    assert_eq!(counts(&e), vec![2, 1], "once after the merge");
    assert_eq!(agg(&e), 3);
    m.check(&e, "after the merge");
    let s = e.stats();
    assert_eq!(s.duplicate_spans_dropped, 3);
    assert_eq!((s.merges_done, s.parts_merged_away), (1, 2));
    assert_eq!(s.last_compaction_error, None);
    assert_eq!(s.windows_with_many_parts, 0);
    assert!(!s.merging);
    // The output is one part of a flush's size, not the sum of its inputs.
    assert!(s.merge_output_bytes > 0 && s.merge_input_bytes > 0);
    assert_consistent(dir.path(), &e, "after");
    e.shutdown().unwrap();
}

#[test]
fn a_trace_flushed_in_two_halves_is_in_one_part_after_the_merge() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let mut m = Model::default();
    let first = vec![TSpan::new(7, 1, 10).root(), TSpan::new(7, 2, 20)];
    let second = vec![TSpan::new(7, 3, 30), TSpan::new(7, 4, 40)];
    for half in [&first, &second] {
        ingest(&e, half).unwrap();
        m.add(half);
        e.flush().unwrap();
    }
    let t = TraceId(7u128.to_be_bytes());
    let split = e.snapshot().split_traces();
    assert_eq!(split.len(), 1);
    assert_eq!(split[0].0, t);
    assert_eq!(split[0].1.len(), 2);
    m.check(&e, "split");

    e.run_maintenance().unwrap();
    let snap = e.snapshot();
    assert!(snap.split_traces().is_empty(), "the map still lists it");
    let parts = snap.part_traces();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].1, vec![t]);
    m.check(&e, "merged");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert!(e.snapshot().split_traces().is_empty());
    m.check(&e, "after a restart");
    e.shutdown().unwrap();
}

#[test]
fn the_tiered_rule_waits_for_min_parts_and_levels_climb() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    // Never quiet within the test: only the tiered rule can fire.
    o.compaction.quiet = Duration::from_secs(3600);
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    for round in 1..=3 {
        f.round(&e, &mut m, &[0]);
        e.run_maintenance().unwrap();
        assert_eq!(e.stats().parts, round, "below min_parts nothing merges");
    }
    f.round(&e, &mut m, &[0]);
    assert_eq!(e.stats().parts, 4);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "the fourth part completes a run");
    assert_eq!(e.stats().merges_done, 1);
    m.check(&e, "tiered");
    let state = disk_state(dir.path()).0;
    assert_eq!(state.parts.len(), 1);
    assert_eq!(state.parts[0].level, 1);
    e.shutdown().unwrap();

    // A final merge of a level-1 part and a flush is level 2.
    let mut o = compact_opts(dir.path());
    o.compaction.quiet = Duration::ZERO;
    let e = open(o);
    f.round(&e, &mut m, &[0]);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1);
    assert_eq!(disk_state(dir.path()).0.parts[0].level, 2);
    m.check(&e, "level 2");
    e.shutdown().unwrap();
}

#[test]
fn compaction_off_merges_nothing_and_options_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    o.compaction.enabled = false;
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    for _ in 0..5 {
        f.round(&e, &mut m, &[0, 1]);
    }
    e.run_maintenance().unwrap();
    let s = e.stats();
    assert_eq!(
        (s.parts, s.merges_done, s.windows_with_many_parts),
        (10, 0, 2)
    );
    e.shutdown().unwrap();

    for bad in [
        CompactionOptions {
            min_parts: 1,
            ..CompactionOptions::default()
        },
        CompactionOptions {
            size_ratio: 0,
            ..CompactionOptions::default()
        },
        CompactionOptions {
            max_parts: 2,
            ..CompactionOptions::default()
        },
        CompactionOptions {
            max_input_bytes: 0,
            ..CompactionOptions::default()
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut o = opts(dir.path());
        o.compaction = bad;
        assert!(matches!(
            Engine::open(o, decoders()).err(),
            Some(Error::InvalidArgument(_))
        ));
        assert!(
            !dir.path().join("manifest").exists(),
            "the disk was touched"
        );
    }
}

#[test]
fn late_data_is_merged_in_after_the_quiet_period() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    // Long enough that a slow machine does not cross it by accident between
    // a flush and the pass that follows it.
    let quiet = Duration::from_millis(1500);
    o.compaction.quiet = quiet;
    o.compaction.min_parts = 100;
    o.compaction.max_parts = 100;
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    for _ in 0..3 {
        f.round(&e, &mut m, &[0, 1]);
    }
    assert_eq!(e.stats().parts, 6);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 6, "not quiet yet");
    m.check(&e, "before the quiet period");

    std::thread::sleep(quiet + Duration::from_millis(100));
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 2, "one part per window");
    m.check(&e, "merged to one part per window");
    let big = snapshot_ids(&e);

    // A late small part for window 0, and the window is not quiet again
    // until it has been live for the quiet period.
    let late = vec![TSpan::new(1, 999, 5), TSpan::new(55, 1000, 6).root()];
    ingest(&e, &late).unwrap();
    m.add(&late);
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 3);
    m.check(&e, "late part arrived");
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 3, "the late part is too new");
    m.check(&e, "late part, not merged");

    std::thread::sleep(quiet + Duration::from_millis(100));
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 2);
    m.check(&e, "late part merged in");
    let now = snapshot_ids(&e);
    // Window 1 was not touched.
    assert!(now.iter().any(|id| big.contains(id)), "{big:?} {now:?}");
    assert_eq!(e.stats().merges_done, 3);
    assert_consistent(dir.path(), &e, "late data");
    e.shutdown().unwrap();
}

#[test]
fn quiet_counts_from_the_start_for_parts_loaded_at_start_up() {
    let dir = tempfile::tempdir().unwrap();
    let (mut f, mut m) = (Feeder::new(), Model::default());
    let e = open(opts(dir.path()));
    for _ in 0..3 {
        f.round(&e, &mut m, &[0]);
    }
    e.shutdown().unwrap();
    let mut o = compact_opts(dir.path());
    o.compaction.quiet = Duration::from_millis(1200);
    o.compaction.min_parts = 100;
    o.compaction.max_parts = 100;
    let e = open(o);
    assert_eq!(e.stats().parts, 3);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 3, "start-up counts as becoming live");
    std::thread::sleep(Duration::from_millis(1300));
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1);
    m.check(&e, "merged");
    e.shutdown().unwrap();
}

#[test]
fn a_flush_and_queries_work_during_a_merge_and_old_snapshots_read_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let pause = Pause::new(Stage::MergeWritten);
    let mut o = compact_opts(dir.path());
    o.stage_hook = Some(pause.hook());
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    for _ in 0..3 {
        f.round(&e, &mut m, &[0, 1]);
    }
    let before_model = m.clone();
    let before = e.snapshot();
    let pass = {
        let e = e.clone();
        std::thread::spawn(move || e.run_maintenance())
    };
    wait_for("the merge to be written", || {
        pause.reached.load(Ordering::SeqCst)
    });
    assert!(e.stats().merging);
    // Nothing is held: ingest, queries and a flush go on, and the flush's
    // part lands in the window being merged.
    m.check(&e, "during the merge");
    f.round(&e, &mut m, &[0]);
    let mid = e.snapshot();
    let mid_model = m.clone();
    assert_eq!(e.stats().parts, 7);
    pause.release();
    pass.join().unwrap().unwrap();

    // The merged parts replaced the three parts of the window that were
    // there when the merge began, not the one a flush added meanwhile.
    m.check(&e, "after the merge");
    let s = e.stats();
    assert!(!s.merging);
    // Window 0 twice (the three parts, then the result and the part the
    // flush added) and window 1 once.
    assert_eq!(s.merges_done, 3, "{s:?}");
    assert_eq!(s.parts, 2, "{s:?}");
    // Both old snapshots read their data from files that were deleted.
    before_model.check_snapshot(&before, "snapshot from before the merge");
    mid_model.check_snapshot(&mid, "snapshot from during the merge");
    // The list stays in id order although the first merge's output has a
    // lower id than the part the flush added meanwhile.
    compact_fully(&e);
    let ids = snapshot_ids(&e);
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    assert_eq!(ids.len(), 2);
    m.check(&e, "all merged");
    assert_consistent(dir.path(), &e, "after");
    drop((before, mid));
    e.shutdown().unwrap();
}

#[test]
fn a_non_fatal_error_before_the_commit_changes_nothing_and_the_next_pass_merges() {
    for (stage, what) in [
        (Stage::MergeWritten, Inject::NonFatal),
        (Stage::BeforeReplaceCommit, Inject::NonFatal),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, left) = fail(stage, what, 1);
        let mut o = compact_opts(dir.path());
        o.stage_hook = Some(hook);
        let e = open(o);
        let (mut f, mut m) = (Feeder::new(), Model::default());
        for _ in 0..3 {
            f.round(&e, &mut m, &[0]);
        }
        let ids = snapshot_ids(&e);
        let version = disk_state(dir.path()).0.version;

        let err = e.run_maintenance().unwrap_err();
        assert!(
            err.to_string().contains("injected failure"),
            "{stage:?}: {err}"
        );
        assert_eq!(left.load(Ordering::SeqCst), 0);
        let s = e.stats();
        assert!(!s.fatal, "{stage:?}");
        assert_eq!(s.merges_done, 0);
        assert!(!s.merging);
        let why = s.last_compaction_error.expect("recorded");
        assert!(why.contains("injected failure"), "{why}");
        assert!(s.last_maintenance_error.is_some());
        // Nothing changed, and the new directory is gone.
        assert_eq!(snapshot_ids(&e), ids, "{stage:?}");
        assert_eq!(disk_state(dir.path()).0.version, version, "{stage:?}");
        assert_consistent(dir.path(), &e, "after the failure");
        m.check(&e, "after the failure");

        // The next pass tries again and succeeds.
        e.run_maintenance().unwrap();
        assert_eq!(e.stats().parts, 1, "{stage:?}");
        assert_eq!(e.stats().merges_done, 1);
        assert!(
            e.stats().last_compaction_error.is_some(),
            "kept for the record"
        );
        m.check(&e, "after the retry");
        assert_consistent(dir.path(), &e, "after the retry");
        e.shutdown().unwrap();
        let e = open(opts(dir.path()));
        m.check(&e, "after a restart");
        e.shutdown().unwrap();
    }
}

#[test]
fn a_fatal_error_or_a_panic_before_the_commit_stops_the_engine_and_a_restart_recovers() {
    for (stage, what) in [
        (Stage::BeforeReplaceCommit, Inject::Fatal),
        (Stage::BeforeReplaceCommit, Inject::Panic),
        (Stage::MergeWritten, Inject::Fatal),
        (Stage::MergeWritten, Inject::Panic),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (hook, _) = fail(stage, what, 1);
        let mut o = compact_opts(dir.path());
        o.stage_hook = Some(hook);
        let e = open(o);
        let (mut f, mut m) = (Feeder::new(), Model::default());
        for _ in 0..3 {
            f.round(&e, &mut m, &[0]);
        }
        let ids = snapshot_ids(&e);
        let err = e.run_maintenance().unwrap_err();
        assert!(
            matches!(err, Error::Fatal(_) | Error::Unavailable(_)),
            "{stage:?}: {err}"
        );
        let s = e.stats();
        // A fatal error at MergeWritten is the hook's judgement of the merge,
        // handled as a failed merge that must not be retried blindly: it is
        // fatal too. Either way, reads work and nothing was replaced.
        assert!(s.fatal, "{stage:?}");
        assert_eq!(snapshot_ids(&e), ids);
        m.check(&e, "reads in the fatal state");
        assert!(matches!(e.run_maintenance(), Err(Error::Fatal(_))));
        assert!(matches!(
            ingest(&e, &[TSpan::new(99, 1, 5)]),
            Err(Error::Fatal(_))
        ));
        let _ = e.shutdown();
        drop(e);

        // The directory of the merge may be on disk, unreferenced; the
        // restart deletes it and keeps every span.
        let e = open(opts(dir.path()));
        m.check(&e, "after the restart");
        assert_eq!(snapshot_ids(&e), ids);
        assert_consistent(dir.path(), &e, "after the restart");
        e.shutdown().unwrap();
    }
}

#[test]
fn a_window_that_keeps_failing_backs_off_and_does_not_hold_back_other_windows() {
    let dir = tempfile::tempdir().unwrap();
    let (hook, _) = fail(Stage::MergeWritten, Inject::NonFatal, 1);
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    o.stage_hook = Some(hook);
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    // Window 0 has three parts and goes first, and fails; window 1 has two.
    f.round(&e, &mut m, &[0, 1]);
    f.round(&e, &mut m, &[0, 1]);
    f.round(&e, &mut m, &[0]);
    assert_eq!(e.stats().parts, 5);

    let err = e.run_maintenance().unwrap_err();
    assert!(err.to_string().contains("injected failure"), "{err}");
    let s = e.stats();
    assert_eq!(s.merges_done, 1, "window 1 was merged in the same pass");
    assert_eq!((s.parts, s.windows_with_many_parts), (4, 1));
    m.check(&e, "one window merged");

    // The failing window is left alone: the pass is a success that does
    // nothing, however often it runs.
    for _ in 0..3 {
        e.run_maintenance().unwrap();
    }
    let s = e.stats();
    assert_eq!((s.merges_done, s.parts), (1, 4));
    assert!(!s.merging);
    // New data in another window is still merged.
    f.round(&e, &mut m, &[2]);
    f.round(&e, &mut m, &[2]);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, 2);
    assert_eq!(e.stats().windows_with_many_parts, 1);
    m.check(&e, "other windows go on");

    // A restart forgets the backoff.
    e.shutdown().unwrap();
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    let e = open(o);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().windows_with_many_parts, 0);
    m.check(&e, "after the restart");
    e.shutdown().unwrap();
}

#[test]
fn a_damaged_input_fails_the_merge_without_harm_and_is_not_retried_until_restart() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let (mut f, mut m) = (Feeder::new(), Model::default());
    f.round(&e, &mut m, &[0, 1]);
    f.round(&e, &mut m, &[0, 1]);
    f.round(&e, &mut m, &[0]);
    let m_window1: Model = Model {
        spans: m.spans.iter().filter(|s| s.start >= W).cloned().collect(),
    };
    // Damage a part of window 0 on disk: one byte of its rows. The rows are at
    // the start of the file, right after the 4-byte magic; the middle of a
    // small file is its footer, and a part with a damaged footer does not open.
    let victim = e
        .snapshot()
        .part_traces()
        .iter()
        .map(|(id, _)| *id)
        .next()
        .unwrap();
    let file = dir
        .path()
        .join("parts")
        .join(part_dir_name(victim))
        .join("spans.parquet");
    let mut bytes = std::fs::read(&file).unwrap();
    let at = 64;
    assert!(bytes.len() > 4 * at, "the part is too small for this test");
    bytes[at] ^= 0xff;
    std::fs::write(&file, &bytes).unwrap();

    let err = e.run_maintenance().unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    let s = e.stats();
    assert!(!s.fatal);
    let why = s.last_compaction_error.clone().expect("recorded");
    assert!(why.contains("merge input"), "{why}");
    assert!(
        why.contains(&part_dir_name(victim)),
        "names the part: {why}"
    );
    assert!(why.contains("damaged"), "{why}");
    // Window 1 was merged in the same pass; window 0 is as it was.
    assert_eq!(s.merges_done, 1);
    assert_eq!(s.parts, 4);
    assert!(snapshot_ids(&e).contains(&victim));
    // Window 0 is damaged on disk, so only window 1 is read back.
    for t in m_window1.trace_ids() {
        let got = e.get_trace(TraceId(t.to_be_bytes())).unwrap();
        assert_eq!(got.map(|b| from_batches(&b)), Some(m_window1.get_trace(t)));
    }

    // Not tried again: a pass is a success that does nothing.
    e.run_maintenance().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, 1);
    // The engine keeps working: new data, other windows, flushes, merges.
    let mut mm = Model::default();
    f.round(&e, &mut mm, &[2]);
    f.round(&e, &mut mm, &[2]);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, 2);
    assert!(!e.stats().fatal);
    for s in &mm.spans {
        assert!(e.get_trace(s.trace_id()).unwrap().is_some());
    }
    e.shutdown().unwrap();

    // After a restart the window is tried again, and fails again the same way.
    let e = open(compact_opts(dir.path()));
    let err = e.run_maintenance().unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(!e.stats().fatal);
    e.shutdown().unwrap();
}

#[test]
fn shutdown_waits_for_the_merge_in_progress_finishes_it_and_starts_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let pause = Pause::new(Stage::MergeWritten);
    let mut o = compact_opts(dir.path());
    o.compaction.background = true;
    o.stage_hook = Some(pause.hook());
    o.maintenance_interval = Duration::from_secs(3600);
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    // The thread's pass after the second flush finds two windows with two
    // parts each and takes the older one first; with the hook closed that
    // merge stops at its stage. Window 1 is the one that must not start.
    f.round(&e, &mut m, &[0, 1]);
    f.round(&e, &mut m, &[0, 1]);
    wait_for("a merge to reach its stage", || {
        pause.reached.load(Ordering::SeqCst)
    });
    assert!(e.stats().merging);
    let done = Arc::new(AtomicBool::new(false));
    let shutdown = {
        let (e, done) = (e.clone(), done.clone());
        std::thread::spawn(move || {
            let r = e.shutdown();
            done.store(true, Ordering::SeqCst);
            r
        })
    };
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        !done.load(Ordering::SeqCst),
        "shutdown did not wait for the merge"
    );
    let released_at = Instant::now();
    pause.release();
    shutdown.join().unwrap().unwrap();
    assert!(
        released_at.elapsed() < Duration::from_secs(10),
        "{:?}",
        released_at.elapsed()
    );
    drop(e);

    // The merge was finished: window 0 is one part, nothing unreferenced is
    // left, and window 1, which the pass had not reached, still has its two
    // parts.
    let (state, unreferenced) = disk_state(dir.path());
    // Shutdown does not delete the directories of the parts the merge
    // replaced (it stops at once; the next start deletes them), and nothing
    // else is left.
    let replaced: Vec<String> = [1u64, 3].iter().map(|i| part_dir_name(*i)).collect();
    assert_eq!(unreferenced, replaced, "{unreferenced:?}");
    let in_window = |w: i64| {
        state
            .parts
            .iter()
            .filter(|p| p.window_start_ns == w * W)
            .count()
    };
    assert_eq!(in_window(0), 1, "the merge in progress was finished");
    assert_eq!(in_window(1), 2, "no merge was started after shutdown began");
    let e = open(opts(dir.path()));
    m.check(&e, "after the restart");
    assert_consistent(dir.path(), &e, "after the restart");
    e.shutdown().unwrap();
}

#[test]
fn an_abandoned_merge_leaves_nothing_behind() {
    // The merge's output is written and then the replacement is refused: the
    // output directory is deleted, the manifest is untouched, and a later
    // start finds no stray directory.
    let dir = tempfile::tempdir().unwrap();
    let (hook, _) = fail(Stage::BeforeReplaceCommit, Inject::NonFatal, 1);
    let mut o = compact_opts(dir.path());
    o.stage_hook = Some(hook);
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    f.round(&e, &mut m, &[0]);
    f.round(&e, &mut m, &[0]);
    let before = support::tree_of(dir.path(), &["manifest", "parts"]);
    assert!(e.run_maintenance().is_err());
    assert_eq!(
        support::tree_of(dir.path(), &["parts"]),
        before
            .iter()
            .filter(|(k, _)| k.starts_with("parts/"))
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
        "the parts directory is as it was"
    );
    e.shutdown().unwrap();
}

#[test]
fn a_merge_interrupted_while_writing_leaves_a_temporary_directory_the_start_removes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut f, mut m) = (Feeder::new(), Model::default());
    let e = open(opts(dir.path()));
    f.round(&e, &mut m, &[0]);
    f.round(&e, &mut m, &[0]);
    e.shutdown().unwrap();
    drop(e);
    // What a killed merge leaves: a half-written temporary directory with an
    // id above every live part.
    let tmp = dir.path().join("parts").join("00000000000000000042.tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(tmp.join("spans.parquet"), b"half").unwrap();
    let e = open(compact_opts(dir.path()));
    assert!(!tmp.exists());
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1);
    // The id of the leftover is not reused.
    assert!(snapshot_ids(&e)[0] > 42, "{:?}", snapshot_ids(&e));
    m.check(&e, "after the merge");
    e.shutdown().unwrap();
}

#[test]
fn span_only_data_flushed_in_many_parts_shrinks_a_lot_when_merged() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    // Tiered merges would shrink the data on the way; this test measures
    // the many-parts state and then the final one.
    o.compaction.min_parts = 1000;
    o.compaction.max_parts = 1000;
    let e = open(o);
    let mut next = 0u64;
    let mut m = Model::default();
    for _ in 0..30 {
        for w in 0..4i64 {
            let spans: Vec<TSpan> = (0..12)
                .map(|i| {
                    next += 1;
                    let mut s = TSpan::new(
                        w as u128 * 10 + (i % 4) as u128 + 1,
                        next,
                        w * W + (next as i64 % 900),
                    );
                    s.model = Some("m1".into());
                    s.input = Some(i as i64);
                    s
                })
                .collect();
            ingest(&e, &spans).unwrap();
            m.add(&spans);
        }
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, 120);
    let before = parts_bytes(dir.path());
    compact_fully(&e);
    let after = parts_bytes(dir.path());
    eprintln!(
        "span-only: {before} bytes in 120 parts, {after} bytes in {} parts after compaction ({:.1}%)",
        e.stats().parts,
        100.0 * after as f64 / before as f64
    );
    assert_eq!(e.stats().parts, 4);
    assert!(after * 4 < before, "{before} -> {after}");
    m.check(&e, "after compaction");
    let s = e.stats();
    assert_eq!(s.merge_input_bytes, before);
    assert_eq!(s.merge_output_bytes, after);
    e.shutdown().unwrap();
}

/// Ingest, flush, queries, retention and merging at once for a few seconds;
/// at the end the engine holds exactly what the model holds, and snapshots
/// taken before a merge read the same after it.
#[test]
fn concurrent_ingest_flush_queries_retention_and_merging_agree_with_the_model() {
    const R: i64 = 6_000;
    const RUN: Duration = Duration::from_secs(4);
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let mut o: EngineOptions = compact_opts(dir.path());
    o.retention = Some(Duration::from_nanos(R as u64));
    let clock = now.clone();
    o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_millis(15);
    o.buffer_bytes = 64 * 1024;
    o.max_buffered_bytes = 8 << 20;
    o.flush_interval = Duration::from_millis(30);
    o.compaction.background = true;
    o.compaction.quiet = Duration::from_millis(60);
    o.compaction.min_parts = 3;
    let e = open(o);
    let model = Arc::new(Mutex::new(Model::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let span_ids = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut threads = Vec::new();
    threads.push({
        let (now, stop) = (now.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let frac = started.elapsed().as_secs_f64() / RUN.as_secs_f64();
                now.store((frac * 50.0 * W as f64) as i64, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    });
    for n in 0..3u64 {
        let (e, model, stop, now, span_ids) = (
            e.clone(),
            model.clone(),
            stop.clone(),
            now.clone(),
            span_ids.clone(),
        );
        threads.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(200 + n);
            while !stop.load(Ordering::SeqCst) {
                let t = now.load(Ordering::SeqCst);
                let w = (t / W + rng.random_range(-9..=3)).max(0);
                let trace = (w as u128) * 100 + rng.random_range(0..20);
                let spans: Vec<TSpan> = (0..rng.random_range(1..5))
                    .map(|_| {
                        let id = span_ids.fetch_add(1, Ordering::SeqCst) + 1;
                        let mut s = TSpan::new(trace, id, w * W + rng.random_range(0..W - 10));
                        s.attrs = vec![id as u8; 200];
                        s
                    })
                    .collect();
                ingest_retry(&e, &spans).unwrap();
                model.lock().unwrap().add(&spans);
            }
        }));
    }
    threads.push({
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                e.flush().unwrap();
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    });
    // Readers: a snapshot answers the same however long it is kept, and
    // whatever merges and deletions happen meanwhile.
    for _ in 0..2 {
        let (e, stop) = (e.clone(), stop.clone());
        threads.push(std::thread::spawn(move || {
            let mut looked = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let snap = e.snapshot();
                let a = snap.list_traces(&all()).unwrap();
                let first: Vec<_> = a
                    .iter()
                    .step_by(5)
                    .map(|s| {
                        (
                            s.trace_id,
                            snap.get_trace(s.trace_id)
                                .unwrap()
                                .map(|b| from_batches(&b)),
                        )
                    })
                    .collect();
                std::thread::sleep(Duration::from_millis(30));
                let b = snap.list_traces(&all()).unwrap();
                assert_eq!(a, b, "a snapshot changed");
                for (id, spans) in first {
                    assert_eq!(
                        snap.get_trace(id).unwrap().map(|b| from_batches(&b)),
                        spans,
                        "a trace of a kept snapshot changed"
                    );
                }
                looked += 1;
            }
            assert!(looked > 0);
        }));
    }
    std::thread::sleep(RUN);
    stop.store(true, Ordering::SeqCst);
    for t in threads {
        t.join().unwrap();
    }

    let t = now.load(Ordering::SeqCst);
    assert!(t > 30 * W, "the clock did not move: {t}");
    e.flush().unwrap();
    compact_fully(&e);
    let mut m = model.lock().unwrap().clone();
    m.spans.retain(|s| s.start / W * W + W > t - R);
    assert!(!m.spans.is_empty());
    m.check(&e, "after the run");
    let s = e.stats();
    eprintln!(
        "{} merges took in {} parts ({} -> {} bytes), {} passes, {} retention removals",
        s.merges_done,
        s.parts_merged_away,
        s.merge_input_bytes,
        s.merge_output_bytes,
        s.maintenance_passes,
        s.retention_parts_removed
    );
    assert!(s.merges_done > 5, "{s:?}");
    assert!(s.retention_parts_removed > 3, "{s:?}");
    assert!(!s.fatal, "{s:?}");
    assert_eq!(s.windows_with_many_parts, 0);
    assert_consistent(dir.path(), &e, "after the run");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    m.check(&e, "after the restart");
    e.shutdown().unwrap();
}

/// A random walk of the checks that need no model: the stats move the way
/// the merges say.
#[test]
fn the_statistics_add_up() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let (mut f, mut m) = (Feeder::new(), Model::default());
    assert_eq!(e.stats().merges_done, 0);
    for round in 1..=4u64 {
        f.round(&e, &mut m, &[0, 1, 2]);
        assert_eq!(
            e.stats().windows_with_many_parts,
            if round == 1 { 0 } else { 3 }
        );
        compact_fully(&e);
        let s = e.stats();
        assert_eq!(s.parts, 3);
        if round > 1 {
            assert_eq!(s.merges_done, 3 * (round - 1));
            assert_eq!(s.parts_merged_away, 2 * 3 * (round - 1));
        }
        assert_eq!(s.windows_with_many_parts, 0);
    }
    m.check(&e, "stats");
    e.shutdown().unwrap();
}

#[test]
fn a_failure_that_persists_ends_the_pass_instead_of_repeating_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let (hook, left) = fail(Stage::MergeWritten, Inject::NonFatal, 1_000_000);
    let mut o = compact_opts(dir.path());
    // No backoff at all: only the pass itself can stop the repetition.
    o.compaction.retry_backoff = Duration::ZERO;
    o.stage_hook = Some(hook);
    let e = open(o);
    let (mut f, mut m) = (Feeder::new(), Model::default());
    f.round(&e, &mut m, &[0]);
    f.round(&e, &mut m, &[0]);
    for pass in 1..=3u64 {
        assert!(e.run_maintenance().is_err());
        assert_eq!(
            left.load(Ordering::SeqCst),
            1_000_000 - pass,
            "one attempt per pass"
        );
    }
    assert_eq!(e.stats().parts, 2);
    m.check(&e, "unchanged");
    e.shutdown().unwrap();
}
