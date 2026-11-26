//! Retention and the maintenance thread, with a clock the test moves.

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::{Error, TraceId};
use nwdb_engine::{Engine, EngineOptions, ListQuery, Stage, StageHook};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

type TestClock = Arc<AtomicI64>;

fn clock(at: i64) -> TestClock {
    Arc::new(AtomicI64::new(at))
}

/// Options with a retention and a clock the test controls. The thread's
/// interval is long: it still runs after every flush and at start-up, so a
/// test that needs a quiet engine only moves the clock when it wants a pass.
fn with_clock(dir: &Path, retention: Option<i64>, now: &TestClock) -> EngineOptions {
    let mut o = opts(dir);
    o.retention = retention.map(|r| Duration::from_nanos(r as u64));
    let now = now.clone();
    o.clock = Some(Arc::new(move || now.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_secs(3600);
    o
}

/// One span at `start`; the trace id is `t`.
fn at(t: u128, span: u64, start: i64) -> TSpan {
    let mut s = TSpan::new(t, span, start).root();
    s.attrs = vec![t as u8; 100];
    s
}

fn part_dirs(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir.join("parts"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().parse().unwrap())
        .collect();
    v.sort_unstable();
    v
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

fn snapshot_ids(e: &Engine) -> Vec<u64> {
    e.snapshot()
        .part_traces()
        .iter()
        .map(|(id, _)| *id)
        .collect()
}

/// What a part must be gone for: its whole window is older than the
/// retention at `now`.
fn expired(window_start: i64, retention: i64, now: i64) -> bool {
    window_start + W <= now - retention
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let t = Instant::now();
    while !cond() {
        assert!(t.elapsed() < Duration::from_secs(30), "timed out: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn queries_match_a_model_that_drops_expired_windows() {
    const R: i64 = 4_500;
    let mut rng = StdRng::seed_from_u64(0xA11CE);
    let dir = tempfile::tempdir().unwrap();
    let now = clock(0);
    let mut e = open(with_clock(dir.path(), Some(R), &now));
    let mut model = Model::default();
    let mut span = 0u64;
    let mut removed_total = 0;
    let mut restarts = 0;
    for step in 0..120 {
        let t = now.load(Ordering::SeqCst);
        // Traces whose window is up to eight windows in the past (already
        // expired, some of them) and three in the future. The trace id names
        // the window, so every span of a trace, however late it comes, is in
        // one window.
        for _ in 0..rng.random_range(0..4) {
            let w = (t / W + rng.random_range(-8..=3)).max(0);
            let trace = (w as u128) * 1_000 + rng.random_range(0..6);
            let spans: Vec<TSpan> = (0..rng.random_range(1..4))
                .map(|_| {
                    span += 1;
                    let mut s = TSpan::new(trace, span, w * W + rng.random_range(0..W - 10));
                    s.attrs = vec![span as u8; 120];
                    s
                })
                .collect();
            ingest(&e, &spans).unwrap();
            model.add(&spans);
        }
        if rng.random_bool(0.7) {
            e.flush().unwrap();
        }
        match rng.random_range(0..12) {
            0 => {
                // A restart, clean or not, with the clock where it is.
                removed_total += e.stats().retention_parts_removed;
                if rng.random_bool(0.5) {
                    e.shutdown().unwrap();
                }
                drop(e);
                e = open(with_clock(dir.path(), Some(R), &now));
                restarts += 1;
            }
            _ => {
                now.fetch_add(rng.random_range(0..1_400), Ordering::SeqCst);
            }
        }
        // Everything is in parts before the pass, so what it must remove is
        // exactly what the model drops. (Data still in the buffer is removed
        // by the pass after the flush that writes it; see the other tests.)
        e.flush().unwrap();
        e.run_maintenance().unwrap();
        let t = now.load(Ordering::SeqCst);
        model.spans.retain(|s| !expired(s.start / W * W, R, t));
        model.check(&e, &format!("step {step}, clock {t}"));

        // Disk, manifest and snapshot agree, and nothing expired is live.
        let ids = snapshot_ids(&e);
        assert_eq!(manifest_ids(dir.path()), ids, "step {step}");
        assert_eq!(
            part_dirs(dir.path()),
            ids,
            "step {step}: expired parts on disk"
        );
    }
    removed_total += e.stats().retention_parts_removed;
    assert!(
        removed_total > 20 && restarts > 3,
        "{removed_total} {restarts}"
    );
    assert!(!model.spans.is_empty());
    let s = e.stats();
    assert!(s.maintenance_passes > 0);
    assert_eq!(s.last_maintenance_error, None);
    e.shutdown().unwrap();
}

#[test]
fn a_part_goes_exactly_when_its_whole_window_is_older_than_the_retention() {
    // (retention, window start of the part, first clock at which it is gone)
    // A retention longer than a window, one shorter, and one of exactly a
    // window; the part is [W, 2W).
    for (retention, gone_at) in [(500, 2 * W + 500), (100, 2 * W + 100), (W, 3 * W)] {
        let dir = tempfile::tempdir().unwrap();
        let now = clock(0);
        let e = open(with_clock(dir.path(), Some(retention), &now));
        ingest(&e, &[at(1, 1, W + 5), at(1, 2, 2 * W - 1)]).unwrap();
        now.store(gone_at - 1, Ordering::SeqCst);
        e.flush().unwrap();
        e.run_maintenance().unwrap();
        assert_eq!(e.stats().parts, 1, "retention {retention}: gone too early");
        assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_some());

        now.store(gone_at, Ordering::SeqCst);
        e.run_maintenance().unwrap();
        assert_eq!(e.stats().parts, 0, "retention {retention}: kept too long");
        assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_none());
        assert_eq!(e.stats().retention_parts_removed, 1);
        assert!(part_dirs(dir.path()).is_empty());
        e.shutdown().unwrap();
    }
}

#[test]
fn without_a_retention_nothing_is_ever_removed() {
    let dir = tempfile::tempdir().unwrap();
    let now = clock(i64::MAX / 2);
    let e = open(with_clock(dir.path(), None, &now));
    ingest(&e, &[at(1, 1, 5), at(2, 1, 3 * W)]).unwrap();
    e.flush().unwrap();
    for _ in 0..3 {
        e.run_maintenance().unwrap();
    }
    assert_eq!(e.stats().parts, 2);
    assert_eq!(e.stats().retention_parts_removed, 0);
    assert!(e.stats().maintenance_passes >= 3);
    e.shutdown().unwrap();

    // The largest retention and the largest clock do not overflow.
    let dir = tempfile::tempdir().unwrap();
    let now = clock(i64::MAX);
    let mut o = with_clock(dir.path(), None, &now);
    o.retention = Some(Duration::MAX);
    let e = open(o);
    ingest(&e, &[at(1, 1, 5)]).unwrap();
    e.flush().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1);
    e.shutdown().unwrap();
}

#[test]
fn data_that_is_already_expired_is_logged_flushed_and_then_removed() {
    const R: i64 = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let now = clock(1_000_000);
    let e = open(with_clock(dir.path(), Some(R), &now));
    let old = at(1, 1, 5);
    ingest(&e, std::slice::from_ref(&old)).unwrap();
    // Nothing special at arrival: it is acknowledged, durable and visible.
    assert!(e.get_trace(old.trace_id()).unwrap().is_some());
    assert_eq!(e.stats().acked_lsn, 1);
    // A flush writes it like any other data; a later pass removes it.
    e.flush().unwrap();
    assert_eq!(e.stats().flushed_lsn, 1);
    e.run_maintenance().unwrap();
    assert!(e.get_trace(old.trace_id()).unwrap().is_none());
    assert_eq!(e.stats().parts, 0);
    assert_eq!(e.stats().retention_parts_removed, 1);
    e.shutdown().unwrap();
    let e = open(with_clock(dir.path(), Some(R), &now));
    assert!(e.get_trace(old.trace_id()).unwrap().is_none());
    e.shutdown().unwrap();
}

#[test]
fn data_with_timestamps_in_the_future_is_kept_until_its_window_ages_out() {
    const R: i64 = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let now = clock(10_000);
    let e = open(with_clock(dir.path(), Some(R), &now));
    // A year ahead.
    let far = 10_000 + 365 * 86_400 * 1_000_000_000i64;
    ingest(&e, &[at(1, 1, far), at(2, 1, 20_000)]).unwrap();
    e.flush().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 2);
    now.store(20_000 + W + R, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "the near one ages out first");
    assert!(e.get_trace(TraceId(2u128.to_be_bytes())).unwrap().is_none());
    assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_some());
    now.store(far / W * W + W + R, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 0);
    e.shutdown().unwrap();
}

#[test]
fn what_expired_while_the_engine_was_down_goes_at_start_up() {
    const R: i64 = 1_000;
    let dir = tempfile::tempdir().unwrap();
    let now = clock(500);
    let e = open(with_clock(dir.path(), Some(R), &now));
    ingest(&e, &[at(1, 1, 5), at(2, 1, 5 * W)]).unwrap();
    e.shutdown().unwrap();
    assert_eq!(part_dirs(dir.path()).len(), 2);

    now.store(2 * W + R, Ordering::SeqCst);
    let e = open(with_clock(dir.path(), Some(R), &now));
    wait_for("the start-up pass", || e.stats().parts == 1);
    assert_eq!(e.stats().retention_parts_removed, 1);
    assert_eq!(part_dirs(dir.path()), manifest_ids(dir.path()));
    e.shutdown().unwrap();
}

#[test]
fn the_thread_runs_after_a_flush_and_every_interval() {
    const R: i64 = 1_000;
    let dir = tempfile::tempdir().unwrap();
    // Long interval: only a flush wakes it.
    let now = clock(1_000_000);
    let e = open(with_clock(dir.path(), Some(R), &now));
    wait_for("the first pass", || e.stats().maintenance_passes >= 1);
    let passes = e.stats().maintenance_passes;
    ingest(&e, &[at(1, 1, 5)]).unwrap();
    e.flush().unwrap();
    wait_for("a pass after the flush", || {
        e.stats().maintenance_passes > passes && e.stats().parts == 0
    });
    assert_eq!(e.stats().retention_parts_removed, 1);
    e.shutdown().unwrap();

    // Short interval: no flush, the clock moves.
    let dir = tempfile::tempdir().unwrap();
    let now = clock(0);
    let mut o = with_clock(dir.path(), Some(R), &now);
    o.maintenance_interval = Duration::from_millis(20);
    let e = open(o);
    ingest(&e, &[at(1, 1, 5)]).unwrap();
    e.flush().unwrap();
    wait_for("quiet", || e.stats().maintenance_passes >= 2);
    assert_eq!(e.stats().parts, 1);
    now.store(10 * W, Ordering::SeqCst);
    wait_for("the interval's pass", || e.stats().parts == 0);
    let before = e.stats().maintenance_passes;
    wait_for("passes go on", || e.stats().maintenance_passes > before + 2);
    e.shutdown().unwrap();
}

#[test]
fn a_zero_interval_or_retention_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.maintenance_interval = Duration::ZERO;
    assert!(matches!(
        Engine::open(o, decoders()).err(),
        Some(Error::InvalidArgument(_))
    ));
    let mut o = opts(dir.path());
    o.retention = Some(Duration::ZERO);
    assert!(matches!(
        Engine::open(o, decoders()).err(),
        Some(Error::InvalidArgument(_))
    ));
    assert!(!dir.path().join("manifest").exists());
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

#[test]
fn shutdown_during_a_pass_waits_for_the_step_in_progress_only() {
    const R: i64 = 1_000;
    let dir = tempfile::tempdir().unwrap();
    // Five expired parts; the pass stops after deleting the first directory.
    let now = clock(0);
    let pause = Pause::new(Stage::PartDirDeleted);
    let mut o = with_clock(dir.path(), Some(R), &now);
    o.stage_hook = Some(pause.hook());
    let e = open(o);
    let spans: Vec<TSpan> = (0..5).map(|w| at(w as u128 + 1, 1, w * W + 5)).collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 5);
    // Nothing is expired yet, so no pass has deleted a directory and the hook
    // has not been reached. Now everything is expired: whichever pass runs
    // first, the thread's (after the flush) or ours, stops in the hook.
    now.store(100 * W, Ordering::SeqCst);
    let pass = {
        let e = e.clone();
        std::thread::spawn(move || e.run_maintenance())
    };
    wait_for("the pass to stop in the hook", || {
        pause.reached.load(Ordering::SeqCst)
    });
    // The pass holds no lock while it deletes: the engine works.
    assert_eq!(e.stats().parts, 0);
    ingest(&e, &[at(99, 1, 100 * W)]).unwrap();
    assert!(
        e.get_trace(TraceId(99u128.to_be_bytes()))
            .unwrap()
            .is_some()
    );
    e.snapshot()
        .list_traces(&ListQuery {
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            service: None,
            model: None,
            error: None,
            limit: 10,
        })
        .unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let shutdown = {
        let (e, done) = (e.clone(), done.clone());
        std::thread::spawn(move || {
            let r = e.shutdown();
            done.store(true, Ordering::SeqCst);
            r
        })
    };
    // Shutdown waits for the step that is in progress.
    std::thread::sleep(Duration::from_millis(300));
    assert!(!done.load(Ordering::SeqCst));
    let released_at = Instant::now();
    pause.release();
    shutdown.join().unwrap().unwrap();
    assert!(
        released_at.elapsed() < Duration::from_secs(5),
        "shutdown took {:?} after the step ended",
        released_at.elapsed()
    );
    let _ = pass.join().unwrap();
    // The directories the pass did not reach are unreferenced leftovers, and
    // the next start deletes them.
    let live = manifest_ids(dir.path());
    let leftovers = part_dirs(dir.path())
        .into_iter()
        .filter(|id| !live.contains(id))
        .count();
    // One directory was deleted before the hook; the other four were left.
    assert_eq!(
        leftovers, 4,
        "the pass went on deleting after shutdown began"
    );
    assert!(matches!(e.run_maintenance(), Err(Error::Unavailable(_))));
    let e = open(with_clock(dir.path(), Some(R), &now));
    assert!(
        e.get_trace(TraceId(99u128.to_be_bytes()))
            .unwrap()
            .is_some()
    );
    e.shutdown().unwrap();
    assert_eq!(part_dirs(dir.path()), manifest_ids(dir.path()));
}

/// Ingest, flush, queries and maintenance at once for a few seconds, the
/// clock moving; at the end the engine holds exactly what the model holds.
#[test]
fn concurrent_ingest_flush_queries_and_maintenance_agree_with_the_model() {
    const R: i64 = 6_000;
    const RUN: Duration = Duration::from_secs(3);
    let dir = tempfile::tempdir().unwrap();
    let now = clock(0);
    let mut o = with_clock(dir.path(), Some(R), &now);
    o.maintenance_interval = Duration::from_millis(15);
    o.buffer_bytes = 64 * 1024;
    o.max_buffered_bytes = 8 << 20;
    o.flush_interval = Duration::from_millis(40);
    let e = open(o);
    let model = Arc::new(Mutex::new(Model::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let span_ids = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut threads = Vec::new();
    // The clock: 0 to about 60 windows over the run.
    threads.push({
        let (now, stop) = (now.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let frac = started.elapsed().as_secs_f64() / RUN.as_secs_f64();
                now.store((frac * 60.0 * W as f64) as i64, Ordering::SeqCst);
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
            let mut rng = StdRng::seed_from_u64(100 + n);
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
    // Explicit flushes on top of the engine's own.
    threads.push({
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                e.flush().unwrap();
                std::thread::sleep(Duration::from_millis(25));
            }
        })
    });
    // Readers: every snapshot is consistent with itself.
    for _ in 0..2 {
        let (e, stop) = (e.clone(), stop.clone());
        threads.push(std::thread::spawn(move || {
            let all = ListQuery {
                from_ns: i64::MIN,
                to_ns: i64::MAX,
                service: None,
                model: None,
                error: None,
                limit: usize::MAX,
            };
            let mut looked = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let snap = e.snapshot();
                // The split map agrees with the parts it belongs to.
                let mut holders = std::collections::BTreeMap::new();
                for (part, ids) in snap.part_traces() {
                    for id in ids {
                        holders.entry(id).or_insert_with(Vec::new).push(part);
                    }
                }
                let want: Vec<_> = holders.into_iter().filter(|(_, p)| p.len() > 1).collect();
                let mut got = snap.split_traces();
                for (_, p) in &mut got {
                    p.sort_unstable();
                }
                assert_eq!(got, want);
                // A listed trace reads back with as many spans as it lists.
                let listed = snap.list_traces(&all).unwrap();
                let ids: BTreeSet<_> = listed.iter().map(|s| s.trace_id).collect();
                assert_eq!(ids.len(), listed.len());
                for s in listed.iter().step_by(7) {
                    let rows: usize = snap
                        .get_trace(s.trace_id)
                        .unwrap()
                        .expect("a listed trace is readable")
                        .iter()
                        .map(|b| b.num_rows())
                        .sum();
                    assert_eq!(rows, s.span_count as usize);
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

    // The clock stays where the last tick left it. Anything expired at that
    // value is gone after one more flush and pass, whatever happened before.
    let t = now.load(Ordering::SeqCst);
    assert!(t > 30 * W, "the clock did not move: {t}");
    e.flush().unwrap();
    e.run_maintenance().unwrap();
    let mut m = model.lock().unwrap().clone();
    m.spans.retain(|s| !expired(s.start / W * W, R, t));
    assert!(!m.spans.is_empty());
    m.check(&e, "after the run");
    let s = e.stats();
    assert!(s.retention_parts_removed > 10, "{s:?}");
    assert!(s.maintenance_passes > 20, "{s:?}");
    assert_eq!(s.last_maintenance_error, None);
    assert!(!s.fatal);
    let ids = snapshot_ids(&e);
    assert_eq!(manifest_ids(dir.path()), ids);
    assert_eq!(part_dirs(dir.path()), ids);
    e.shutdown().unwrap();

    // And a restart agrees.
    let e = open(with_clock(dir.path(), Some(R), &now));
    m.check(&e, "after the restart");
    e.shutdown().unwrap();
}
