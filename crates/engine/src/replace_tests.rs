//! Tests of the replace operation, which needs access to the engine's
//! internals: its callers are inside the crate (retention now, merging
//! later), so there is no public way to hand it a part.

#[path = "../tests/common/mod.rs"]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::*;
use nwdb_core::{Error, TraceId, part_dir_name};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use nwdb_segment::{AnnotationReader, PartReader, PartWriter, WriterOptions};

use crate::engine::{LiveAnnotationPart, LivePart, Stage, StageHook};
use crate::lock;
use crate::{Engine, Snapshot};

/// Writes one part holding every trace of the live parts `from`, the way a
/// merge would: a new id from the manifest, traces in id order, each trace's
/// rows from every source together.
fn merged(e: &Engine, from: &[u64]) -> LivePart {
    let inner = e.inner();
    let parts = lock(&inner.core).vis.parts.clone();
    let srcs: Vec<_> = parts
        .iter()
        .filter(|p| from.contains(&p.meta.part_id))
        .collect();
    assert_eq!(srcs.len(), from.len());
    let traces: BTreeSet<TraceId> = srcs
        .iter()
        .flat_map(|p| p.reader.trace_entries().iter().map(|t| t.trace_id))
        .collect();
    let id = inner.manifest.reserve_part_ids(1).unwrap().start;
    let start = srcs.iter().map(|p| p.meta.window_start_ns).min().unwrap();
    let end = srcs
        .iter()
        .map(|p| p.meta.window_start_ns + p.meta.window_ns)
        .max()
        .unwrap();
    let mut w = PartWriter::create(
        &inner.parts_dir,
        id,
        start,
        end - start,
        WriterOptions {
            splitter: inner.opts.splitter.clone(),
            ..WriterOptions::default()
        },
    )
    .unwrap();
    for t in traces {
        for p in &srcs {
            for b in p.reader.read_trace(t, None).unwrap().into_iter().flatten() {
                w.write(&b).unwrap();
            }
        }
    }
    let meta = w.finish().unwrap();
    let reader = PartReader::open(inner.parts_dir.join(part_dir_name(id))).unwrap();
    LivePart::new(meta, reader)
}

fn part_ids(e: &Engine) -> Vec<u64> {
    e.snapshot()
        .part_traces()
        .iter()
        .map(|(id, _)| *id)
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

fn dirs_on_disk(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir.join("parts"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().parse().unwrap())
        .collect();
    v.sort_unstable();
    v
}

/// The split map must be exactly the traces in more than one part.
fn check_split(snap: &Snapshot, what: &str) {
    let mut holders: BTreeMap<TraceId, Vec<u64>> = BTreeMap::new();
    for (part, ids) in snap.part_traces() {
        for id in ids {
            holders.entry(id).or_default().push(part);
        }
    }
    let want: Vec<_> = holders.into_iter().filter(|(_, p)| p.len() > 1).collect();
    let mut got = snap.split_traces();
    for (_, parts) in &mut got {
        parts.sort_unstable();
    }
    assert_eq!(got, want, "{what}: split map");
}

fn span(t: u128, k: u64) -> TSpan {
    // Every span of a trace is in the window of its id, so a trace is never
    // spread over windows however it is split over flushes.
    let mut s = TSpan::new(t, k, (t as i64 % 10) * 1_000 + k as i64);
    s.attrs = vec![t as u8; 150];
    if k == 1 {
        s = s.root();
    }
    s
}

/// Traces 1..=30 in ten windows, 15..=24 sent twice in two flushes (so they
/// are split) and 1..=5 once more in a third.
fn populated(e: &Engine, model: &mut Model) {
    for (range, k) in [(1..=24u128, 1u64), (15..=30, 2), (1..=5, 3)] {
        let spans: Vec<TSpan> = range.map(|t| span(t, k)).collect();
        ingest(e, &spans).unwrap();
        model.add(&spans);
        e.flush().unwrap();
    }
}

fn windows_of(e: &Engine) -> Vec<(u64, i64)> {
    lock(&e.inner().core)
        .vis
        .parts
        .iter()
        .map(|p| (p.meta.part_id, p.meta.window_start_ns))
        .collect()
}

#[test]
fn replace_swaps_parts_in_one_step_and_old_snapshots_keep_reading() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    populated(&e, &mut m);
    check_split(&e.snapshot(), "before");
    assert!(!e.snapshot().split_traces().is_empty());

    let before = part_ids(&e);
    let remove: Vec<u64> = windows_of(&e)
        .into_iter()
        .filter(|(_, w)| *w < 5_000)
        .map(|(id, _)| id)
        .collect();
    assert!(remove.len() >= 5 && remove.len() < before.len());

    let old = e.snapshot();
    let new_part = merged(&e, &remove);
    let new_id = new_part.meta.part_id;
    let replaced = e
        .inner()
        .replace_parts(&remove, vec![new_part], Vec::new())
        .unwrap();
    assert!(replaced.deletion_errors.is_empty());

    // A new snapshot sees the new part and none of the removed ones, a map
    // that matches, and the same data.
    let now = e.snapshot();
    let ids: Vec<u64> = part_ids(&e);
    assert!(ids.contains(&new_id));
    assert!(remove.iter().all(|r| !ids.contains(r)));
    assert_eq!(ids.len(), before.len() - remove.len() + 1);
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    check_split(&now, "after");
    m.check_snapshot(&now, "new snapshot");
    assert_eq!(e.stats().parts, ids.len());

    // The directories are gone, the manifest agrees, and the old snapshot
    // still reads every trace from its open files.
    for r in &remove {
        assert!(!dir.path().join("parts").join(part_dir_name(*r)).exists());
    }
    assert_eq!(manifest_ids(dir.path()), ids);
    assert_eq!(dirs_on_disk(dir.path()), ids);
    m.check_snapshot(&old, "old snapshot after the removal");
    assert_eq!(old.part_traces().len(), before.len());
    check_split(&old, "old snapshot");

    // And so does a restart.
    drop(old);
    e.shutdown().unwrap();
    drop(e);
    let e = open(opts(dir.path()));
    assert_eq!(part_ids(&e), ids);
    m.check(&e, "after restart");
    check_split(&e.snapshot(), "after restart");
    e.shutdown().unwrap();
}

#[test]
fn removing_without_adding_and_adding_without_removing_work() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    populated(&e, &mut m);
    let all = windows_of(&e);
    // Remove only: the traces of the removed parts that were in no other part
    // are gone, the others are what the other parts hold.
    let gone: Vec<u64> = all
        .iter()
        .filter(|(_, w)| *w >= 8_000)
        .map(|(id, _)| *id)
        .collect();
    e.inner()
        .replace_parts(&gone, Vec::new(), Vec::new())
        .unwrap();
    m.spans.retain(|s| s.start < 8_000);
    m.check(&e, "removed only");
    check_split(&e.snapshot(), "removed only");

    // Add only: a part holding a copy of a part's rows would duplicate
    // them, so add one holding nothing new by merging then removing is
    // covered above; here the part is an exact merge of one part and the
    // original is removed in the same step.
    let one = all[0].0;
    let copy = merged(&e, &[one]);
    e.inner()
        .replace_parts(&[one], vec![copy], Vec::new())
        .unwrap();
    m.check(&e, "one part replaced by its copy");
    check_split(&e.snapshot(), "one part replaced by its copy");
    // Nothing at all is a no-op, not a manifest version.
    let v = e.inner().manifest.current().version;
    e.inner()
        .replace_parts(&[], Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(e.inner().manifest.current().version, v);
    e.shutdown().unwrap();
}

#[test]
fn a_part_that_a_flush_published_in_between_keeps_the_list_in_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    populated(&e, &mut m);
    let remove: Vec<u64> = windows_of(&e)
        .into_iter()
        .filter(|(_, w)| *w < 3_000)
        .map(|(id, _)| id)
        .collect();
    // The merged part gets its id now; a flush then commits parts with higher
    // ids, which also hold more rows of traces in the merged part.
    let new_part = merged(&e, &remove);
    let late: Vec<TSpan> = (1..=12u128).map(|t| span(t, 9)).collect();
    ingest(&e, &late).unwrap();
    m.add(&late);
    e.flush().unwrap();
    e.inner()
        .replace_parts(&remove, vec![new_part], Vec::new())
        .unwrap();

    let ids = part_ids(&e);
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    assert_eq!(manifest_ids(dir.path()), ids);
    check_split(&e.snapshot(), "out-of-order publish");
    m.check(&e, "out-of-order publish");
    e.shutdown().unwrap();
}

#[test]
fn an_error_before_the_commit_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    populated(&e, &mut m);
    let ids = part_ids(&e);
    let version = e.inner().manifest.current().version;
    let snap = e.snapshot();
    let unchanged = |what: &str| {
        assert_eq!(part_ids(&e), ids, "{what}");
        assert_eq!(manifest_ids(dir.path()), ids, "{what}");
        assert_eq!(e.inner().manifest.current().version, version, "{what}");
        assert_eq!(dirs_on_disk(dir.path()).len(), ids.len(), "{what}");
        assert!(!e.stats().fatal);
        m.check(&e, what);
    };

    // A part that is not live, and one listed twice.
    let err = e
        .inner()
        .replace_parts(&[9_999], Vec::new(), Vec::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    unchanged("unknown id");
    let err = e
        .inner()
        .replace_parts(&[ids[0], ids[0]], Vec::new(), Vec::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    unchanged("id twice");

    // The manifest refuses a new part whose id is already live.
    let mut dup = merged(&e, &[ids[1]]);
    let dup_dir = dir
        .path()
        .join("parts")
        .join(part_dir_name(dup.meta.part_id));
    dup.meta.part_id = ids[2];
    let err = e
        .inner()
        .replace_parts(&[ids[1]], vec![dup], Vec::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    // Only the caller's own directory is extra, and it is the caller's to
    // delete.
    assert_eq!(dirs_on_disk(dir.path()).len(), ids.len() + 1);
    std::fs::remove_dir_all(dup_dir).unwrap();
    unchanged("duplicate live id");
    m.check_snapshot(&snap, "snapshot");
    e.shutdown().unwrap();
}

#[test]
fn hooks_after_the_commit_cannot_undo_it() {
    for panic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let hook: StageHook = Arc::new(move |s| match s {
            Stage::ReplaceCommitted | Stage::ReplacePublished | Stage::PartDirDeleted => {
                if panic {
                    panic!("injected panic");
                }
                Err(Error::other("injected failure"))
            }
            _ => Ok(()),
        });
        let mut o = opts(dir.path());
        o.stage_hook = Some(hook);
        let e = open(o);
        let mut m = Model::default();
        populated(&e, &mut m);
        let remove: Vec<u64> = windows_of(&e)
            .into_iter()
            .filter(|(_, w)| *w < 4_000)
            .map(|(id, _)| id)
            .collect();
        let new_part = merged(&e, &remove);
        e.inner()
            .replace_parts(&remove, vec![new_part], Vec::new())
            .unwrap();
        assert!(!e.stats().fatal);
        m.check(&e, "hooks failed");
        assert_eq!(manifest_ids(dir.path()), part_ids(&e));
        assert_eq!(dirs_on_disk(dir.path()), part_ids(&e));
        e.shutdown().unwrap();
    }
}

#[test]
fn a_directory_that_cannot_be_deleted_is_recorded_and_retried() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    populated(&e, &mut m);
    // The passes the flushes triggered are over before the test counts.
    let mut last = u64::MAX;
    while last != e.stats().maintenance_passes {
        last = e.stats().maintenance_passes;
        std::thread::sleep(Duration::from_millis(200));
    }
    let base = last;
    let ids = part_ids(&e);
    let victim = ids[0];
    let path = dir.path().join("parts").join(part_dir_name(victim));
    // The open files stay readable when the directory is moved aside; a plain
    // file in its place cannot be removed as a directory.
    let aside = dir.path().join("aside");
    std::fs::rename(&path, &aside).unwrap();
    std::fs::write(&path, b"in the way").unwrap();

    let replaced = e
        .inner()
        .replace_parts(&[victim], Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(replaced.deletion_errors.len(), 1, "{replaced:?}");
    assert_eq!(lock(&e.inner().undeleted).len(), 1);
    assert!(!part_ids(&e).contains(&victim));

    // The pass retries, fails again and says so; the engine is not fatal.
    let err = e.run_maintenance().unwrap_err();
    assert!(err.to_string().contains("could not be deleted"), "{err}");
    let s = e.stats();
    assert_eq!(s.maintenance_passes, base + 1);
    assert!(s.last_maintenance_error.is_some());
    assert!(!s.fatal);
    assert_eq!(lock(&e.inner().undeleted).len(), 1);

    // Once the obstacle is gone the next pass deletes it.
    std::fs::remove_file(&path).unwrap();
    e.run_maintenance().unwrap();
    assert!(lock(&e.inner().undeleted).is_empty());
    assert_eq!(e.stats().maintenance_passes, base + 2);
    e.shutdown().unwrap();
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

    fn wait_reached(&self) {
        let t = std::time::Instant::now();
        while !self.reached.load(Ordering::SeqCst) {
            assert!(t.elapsed() < Duration::from_secs(30), "stage not reached");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

#[test]
fn a_flush_and_a_replacement_never_interleave_their_commit_and_publish() {
    // Which of the two is paused after its commit; the other must wait for
    // the pause to end before it can commit.
    for flush_pauses in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let pause = Pause::new(if flush_pauses {
            Stage::Committed
        } else {
            Stage::ReplaceCommitted
        });
        let mut o = opts(dir.path());
        o.stage_hook = Some(pause.hook());
        // The hook is called only for the stage it is set for.
        let e = open(o);
        let mut m = Model::default();
        // The first flush of `populated` would pause too: let it through.
        pause.release();
        populated(&e, &mut m);
        *pause.released.lock().unwrap() = false;
        pause.reached.store(false, Ordering::SeqCst);

        let remove: Vec<u64> = windows_of(&e)
            .into_iter()
            .filter(|(_, w)| *w < 4_000)
            .map(|(id, _)| id)
            .collect();
        let new_part = merged(&e, &remove);
        let late: Vec<TSpan> = (20..=28u128).map(|t| span(t, 7)).collect();
        ingest(&e, &late).unwrap();
        m.add(&late);

        let flush_done = Arc::new(AtomicBool::new(false));
        let replace_done = Arc::new(AtomicBool::new(false));
        let (flusher, replacer);
        if flush_pauses {
            flusher = {
                let (e, d) = (e.clone(), flush_done.clone());
                std::thread::spawn(move || {
                    e.flush().unwrap();
                    d.store(true, Ordering::SeqCst);
                })
            };
            pause.wait_reached();
            replacer = {
                let (e, d) = (e.clone(), replace_done.clone());
                std::thread::spawn(move || {
                    e.inner()
                        .replace_parts(&remove, vec![new_part], Vec::new())
                        .unwrap();
                    d.store(true, Ordering::SeqCst);
                })
            };
        } else {
            replacer = {
                let (e, d) = (e.clone(), replace_done.clone());
                std::thread::spawn(move || {
                    e.inner()
                        .replace_parts(&remove, vec![new_part], Vec::new())
                        .unwrap();
                    d.store(true, Ordering::SeqCst);
                })
            };
            pause.wait_reached();
            flusher = {
                let (e, d) = (e.clone(), flush_done.clone());
                std::thread::spawn(move || {
                    e.flush().unwrap();
                    d.store(true, Ordering::SeqCst);
                })
            };
        }
        // The one that is not paused cannot finish (nor commit) meanwhile,
        // and the paused one has committed but not published, so a snapshot
        // still sees the list from before both.
        std::thread::sleep(Duration::from_millis(300));
        let (waiting_done, paused_done) = if flush_pauses {
            (&replace_done, &flush_done)
        } else {
            (&flush_done, &replace_done)
        };
        assert!(!waiting_done.load(Ordering::SeqCst), "it did not wait");
        assert!(!paused_done.load(Ordering::SeqCst));
        pause.release();
        flusher.join().unwrap();
        replacer.join().unwrap();

        let ids = part_ids(&e);
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
        assert_eq!(manifest_ids(dir.path()), ids);
        assert_eq!(dirs_on_disk(dir.path()), ids);
        check_split(&e.snapshot(), "after both");
        m.check(&e, "after both");
        e.shutdown().unwrap();
    }
}

#[test]
fn an_error_returned_before_the_commit_is_honoured_and_a_fatal_one_stops_the_engine() {
    for fatal in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let hook: StageHook = Arc::new(move |s| match s {
            Stage::BeforeReplaceCommit if fatal => Err(Error::fatal("injected fatal failure")),
            Stage::BeforeReplaceCommit => Err(Error::other("injected failure")),
            _ => Ok(()),
        });
        let mut o = opts(dir.path());
        o.stage_hook = Some(hook);
        let e = open(o);
        let mut m = Model::default();
        populated(&e, &mut m);
        let ids = part_ids(&e);
        let version = e.inner().manifest.current().version;
        let remove: Vec<u64> = windows_of(&e)
            .into_iter()
            .filter(|(_, w)| *w < 4_000)
            .map(|(id, _)| id)
            .collect();
        let new_part = merged(&e, &remove);
        let new_dir = dir
            .path()
            .join("parts")
            .join(part_dir_name(new_part.meta.part_id));

        let err = e
            .inner()
            .replace_parts(&remove, vec![new_part], Vec::new())
            .unwrap_err();
        // Nothing changed: the list, the manifest, the old directories and the
        // data. The new directory is the caller's to delete.
        assert_eq!(matches!(err, Error::Fatal(_)), fatal, "{err}");
        assert_eq!(part_ids(&e), ids);
        assert_eq!(e.inner().manifest.current().version, version);
        assert_eq!(dirs_on_disk(dir.path()).len(), ids.len() + 1);
        assert!(new_dir.is_dir());
        m.check(&e, "after the refused replacement");
        assert_eq!(e.stats().fatal, fatal);
        if fatal {
            // The engine is stopped: another replacement is refused, reads
            // work, and a restart recovers everything and deletes the new
            // directory.
            let again = e.inner().replace_parts(&[], Vec::new(), Vec::new());
            assert!(matches!(again, Err(Error::Fatal(_))));
            let _ = e.shutdown();
            drop(e);
            let e = open(opts(dir.path()));
            m.check(&e, "after the restart");
            assert_eq!(part_ids(&e), ids);
            assert_eq!(dirs_on_disk(dir.path()), ids);
            e.shutdown().unwrap();
        } else {
            std::fs::remove_dir_all(new_dir).unwrap();
            e.shutdown().unwrap();
        }
    }
}

/// The list and the split map of a replacement are built before the publish
/// gate is taken, so a flush can publish in between (and is not held up by
/// the replacement meanwhile). The result accounts for that flush's parts, and
/// its split map is the one built from scratch.
#[test]
fn a_flush_that_publishes_while_the_map_is_built_is_accounted_for() {
    let dir = tempfile::tempdir().unwrap();
    let pause = Pause::new(Stage::ReplaceMapBuilt);
    let mut o = opts(dir.path());
    o.stage_hook = Some(pause.hook());
    let e = open(o);
    let mut m = Model::default();
    populated(&e, &mut m);
    let remove: Vec<u64> = windows_of(&e)
        .into_iter()
        .filter(|(_, w)| *w < 4_000)
        .map(|(id, _)| id)
        .collect();
    let new_part = merged(&e, &remove);
    let new_id = new_part.meta.part_id;
    let replacer = {
        let e = e.clone();
        let remove = remove.clone();
        std::thread::spawn(move || {
            e.inner()
                .replace_parts(&remove, vec![new_part], Vec::new())
                .unwrap()
        })
    };
    pause.wait_reached();

    // The replacement has built its list and is not holding the gate: a flush
    // goes through, with traces that are in the merged part and in others.
    let late: Vec<TSpan> = (1..=30u128).map(|t| span(t, 7)).collect();
    ingest(&e, &late).unwrap();
    m.add(&late);
    let flush = {
        let e = e.clone();
        std::thread::spawn(move || e.flush().unwrap())
    };
    let t = std::time::Instant::now();
    while !flush.is_finished() {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "the flush waited for the replacement"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    flush.join().unwrap();
    let in_between = part_ids(&e);
    pause.release();
    replacer.join().unwrap();

    let ids = part_ids(&e);
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    assert!(ids.contains(&new_id));
    assert!(remove.iter().all(|r| !ids.contains(r)));
    // Every part the flush published is there.
    let flushed: Vec<u64> = in_between
        .iter()
        .copied()
        .filter(|id| !remove.contains(id) && *id > new_id)
        .collect();
    assert!(!flushed.is_empty());
    assert!(flushed.iter().all(|id| ids.contains(id)));
    assert_eq!(ids.len(), in_between.len() - remove.len() + 1);
    check_split(&e.snapshot(), "after both");
    m.check(&e, "after both");
    assert_eq!(manifest_ids(dir.path()), ids);
    assert_eq!(dirs_on_disk(dir.path()), ids);
    e.shutdown().unwrap();
}

/// A part that a flush publishes after the list was seen cannot be one of the
/// parts to remove: the map was built for a list without it. The replacement
/// is refused before the commit and nothing changes.
#[test]
fn a_part_published_while_the_map_is_built_cannot_be_removed_by_that_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let pause = Pause::new(Stage::ReplaceMapBuilt);
    let mut o = opts(dir.path());
    o.stage_hook = Some(pause.hook());
    let e = open(o);
    let mut m = Model::default();
    populated(&e, &mut m);
    let mut remove: Vec<u64> = windows_of(&e)
        .into_iter()
        .filter(|(_, w)| *w < 4_000)
        .map(|(id, _)| id)
        .collect();
    let new_part = merged(&e, &remove);
    let new_id = new_part.meta.part_id;
    // The flush below is the next to take a part id.
    let flushed_id = new_id + 1;
    remove.push(flushed_id);
    let before = part_ids(&e);
    let version = e.inner().manifest.current().version;
    let replacer = {
        let e = e.clone();
        let remove = remove.clone();
        std::thread::spawn(move || e.inner().replace_parts(&remove, vec![new_part], Vec::new()))
    };
    pause.wait_reached();
    let late: Vec<TSpan> = (1..=30u128).map(|t| span(t, 7)).collect();
    ingest(&e, &late).unwrap();
    m.add(&late);
    e.flush().unwrap();
    assert!(
        part_ids(&e).contains(&flushed_id),
        "the id was not predicted"
    );
    pause.release();
    let err = replacer.join().unwrap().unwrap_err();
    assert!(
        err.to_string().contains("changed while a replacement"),
        "{err}"
    );
    // The flush's part is still there, the old parts too, and the manifest
    // is as the flush left it.
    let ids = part_ids(&e);
    assert!(ids.contains(&flushed_id));
    assert!(before.iter().all(|id| ids.contains(id)));
    assert!(!ids.contains(&new_id));
    assert_eq!(e.inner().manifest.current().version, version + 1);
    check_split(&e.snapshot(), "after the refusal");
    m.check(&e, "after the refusal");
    std::fs::remove_dir_all(dir.path().join("parts").join(part_dir_name(new_id))).unwrap();
    assert_eq!(manifest_ids(dir.path()), ids);
    e.shutdown().unwrap();
}

// Both kinds of part in one replacement.

/// The annotation parts `from` (all of one window) merged into one part, the
/// way a merge would.
fn merged_annotations(e: &Engine, from: &[u64]) -> LiveAnnotationPart {
    let inner = e.inner();
    let parts = lock(&inner.core).vis.ann_parts.clone();
    let srcs: Vec<_> = parts
        .iter()
        .filter(|p| from.contains(&p.meta.part_id))
        .collect();
    assert_eq!(srcs.len(), from.len());
    let id = inner.manifest.reserve_part_ids(1).unwrap().start;
    let (start, width) = (srcs[0].meta.window_start_ns, srcs[0].meta.window_ns);
    let readers: Vec<&AnnotationReader> = srcs.iter().map(|p| &p.reader).collect();
    let (meta, _) =
        nwdb_segment::merge_annotation_parts(&readers, &inner.parts_dir, id, start, width, 1, None)
            .unwrap();
    let reader = AnnotationReader::open(inner.parts_dir.join(part_dir_name(id)), &meta).unwrap();
    LiveAnnotationPart::new(meta, reader)
}

/// `(id, window start)` of the live annotation parts.
fn ann_windows_of(e: &Engine) -> Vec<(u64, i64)> {
    lock(&e.inner().core)
        .vis
        .ann_parts
        .iter()
        .map(|p| (p.meta.part_id, p.meta.window_start_ns))
        .collect()
}

fn window_count(e: &Engine, w: i64) -> usize {
    windows_of(e).iter().filter(|x| x.1 == w * 1_000).count()
}

fn ann_ids(e: &Engine) -> Vec<u64> {
    e.snapshot()
        .annotation_part_info()
        .iter()
        .map(|x| x.0)
        .collect()
}

fn manifest_ann_ids(dir: &Path) -> Vec<u64> {
    Manifest::open(
        dir.join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current()
    .parts
    .iter()
    .filter(|p| p.kind == nwdb_core::PartKind::Annotations)
    .map(|p| p.part_id)
    .collect()
}

fn values_of(s: &Snapshot, t: u128) -> Vec<Option<f64>> {
    s.annotations(tid(t), usize::MAX)
        .unwrap()
        .0
        .iter()
        .map(|a| a.value)
        .collect()
}

/// `populated`, then two flushes of annotations (values 1 and 2) on every
/// trace: ten windows, two annotation parts each, the winner 2.
fn with_annotations(e: &Engine, m: &mut Model) {
    populated(e, m);
    for v in [1.0, 2.0] {
        annotate(
            e,
            (1..=30u128).map(|t| score(t, None, "q", "s", v)).collect(),
        );
        e.flush().unwrap();
    }
}

#[test]
fn both_kinds_are_replaced_in_one_step_and_old_snapshots_keep_reading() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    with_annotations(&e, &mut m);
    assert_eq!(ann_ids(&e).len(), 20);
    let spans_before = part_ids(&e).len();
    let (w3, w4) = (window_count(&e, 3), window_count(&e, 4));
    let window = |w: i64| -> (Vec<u64>, Vec<u64>) {
        (
            windows_of(&e)
                .into_iter()
                .filter(|x| x.1 == w * 1_000)
                .map(|x| x.0)
                .collect(),
            ann_windows_of(&e)
                .into_iter()
                .filter(|x| x.1 == w * 1_000)
                .map(|x| x.0)
                .collect(),
        )
    };

    let old = e.snapshot();
    let stop = Arc::new(AtomicBool::new(false));
    // Readers: the traces of window 3 (3, 13, 23) always show their spans and
    // their winner (the merged parts are never missing, one kind is never
    // swapped before the other); the traces of window 4 show their spans only
    // together with their annotation, until both are removed.
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (e, stop) = (e.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut rounds = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    let s = e.snapshot();
                    for t in [3u128, 13, 23] {
                        assert!(s.get_trace(tid(t)).unwrap().is_some(), "spans of {t}");
                        assert_eq!(values_of(&s, t), vec![Some(2.0)], "trace {t}");
                    }
                    for t in [4u128, 14, 24] {
                        if s.get_trace(tid(t)).unwrap().is_some() {
                            assert_eq!(values_of(&s, t), vec![Some(2.0)], "trace {t}");
                        }
                    }
                    rounds += 1;
                }
                rounds
            })
        })
        .collect();

    // Window 3: swapped for copies, again and again.
    for _ in 0..15 {
        let (spans, anns) = window(3);
        let mut remove = spans.clone();
        remove.extend(&anns);
        let sp = merged(&e, &spans);
        let an = merged_annotations(&e, &anns);
        let r = e
            .inner()
            .replace_parts(&remove, vec![sp], vec![an])
            .unwrap();
        assert!(r.deletion_errors.is_empty());
    }
    // Window 4: removed, both kinds, no replacement.
    let (spans, anns) = window(4);
    let remove: Vec<u64> = spans.iter().chain(&anns).copied().collect();
    let version = e.inner().manifest.current().version;
    e.inner()
        .replace_parts(&remove, Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(e.inner().manifest.current().version, version + 1);
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }

    // The state after: one annotation part for window 3, none for window 4.
    let now = e.snapshot();
    assert_eq!(part_ids(&e).len(), spans_before - (w3 - 1) - w4);
    assert_eq!(ann_ids(&e).len(), 20 - 2 - 2 + 1);
    assert!(now.get_trace(tid(4)).unwrap().is_none());
    assert_eq!(values_of(&now, 4), Vec::<Option<f64>>::new());
    assert_eq!(values_of(&now, 5), vec![Some(2.0)]);
    check_split(&now, "after");
    // The old snapshot still reads the removed annotation parts from their
    // open files.
    for t in [3u128, 4, 14, 23] {
        assert_eq!(
            values_of(&old, t),
            vec![Some(2.0)],
            "old snapshot, trace {t}"
        );
        assert!(old.get_trace(tid(t)).unwrap().is_some());
    }
    // The manifest, the lists and the directories agree.
    let mut live: Vec<u64> = part_ids(&e);
    live.extend(ann_ids(&e));
    live.sort_unstable();
    assert_eq!(dirs_on_disk(dir.path()), live);
    assert_eq!(manifest_ann_ids(dir.path()), ann_ids(&e));
    drop((old, now));
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(ann_ids(&e).len(), 17);
    assert_eq!(values_of(&e.snapshot(), 13), vec![Some(2.0)]);
    e.shutdown().unwrap();
}

#[test]
fn a_removed_id_must_be_live_in_one_of_the_two_lists() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    with_annotations(&e, &mut m);
    let version = e.inner().manifest.current().version;
    let a = ann_ids(&e)[0];
    let s = part_ids(&e)[0];
    for remove in [vec![a, a], vec![s, s], vec![a, 99_999]] {
        let err = e
            .inner()
            .replace_parts(&remove, Vec::new(), Vec::new())
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    }
    assert_eq!(e.inner().manifest.current().version, version);
    assert_eq!(ann_ids(&e).len(), 20);
    // One of each kind in one call is fine.
    e.inner()
        .replace_parts(&[a, s], Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(e.inner().manifest.current().version, version + 1);
    e.shutdown().unwrap();
}

/// The annotation list is rebuilt under the gate from the list as it is then,
/// so annotation parts a flush published while the replacement was between
/// its stages survive, also in the window being replaced.
#[test]
fn a_flush_publishing_annotation_parts_during_a_replacement_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let pause = Pause::new(Stage::ReplaceMapBuilt);
    let mut o = opts(dir.path());
    o.stage_hook = Some(pause.hook());
    let e = open(o);
    let mut m = Model::default();
    with_annotations(&e, &mut m);
    // Window 2 (traces 2, 12, 22) is replaced.
    let spans: Vec<u64> = windows_of(&e)
        .into_iter()
        .filter(|x| x.1 == 2_000)
        .map(|x| x.0)
        .collect();
    let anns: Vec<u64> = ann_windows_of(&e)
        .into_iter()
        .filter(|x| x.1 == 2_000)
        .map(|x| x.0)
        .collect();
    let before = ann_ids(&e);
    let mut remove = spans.clone();
    remove.extend(&anns);
    let sp = merged(&e, &spans);
    let an = merged_annotations(&e, &anns);
    let replacer = {
        let e = e.clone();
        std::thread::spawn(move || {
            e.inner()
                .replace_parts(&remove, vec![sp], vec![an])
                .unwrap()
        })
    };
    pause.wait_reached();
    // Between the stages: annotations of window 2 and of window 5 are
    // flushed.
    annotate(
        &e,
        vec![score(2, None, "q", "s", 3.0), score(5, None, "q", "s", 3.0)],
    );
    e.flush().unwrap();
    let flushed: Vec<u64> = ann_ids(&e)
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert_eq!(flushed.len(), 2, "{flushed:?}");
    pause.release();
    replacer.join().unwrap();

    let now = e.snapshot();
    assert_eq!(values_of(&now, 2), vec![Some(3.0)]);
    assert_eq!(values_of(&now, 5), vec![Some(3.0)]);
    assert_eq!(values_of(&now, 12), vec![Some(2.0)]);
    let ids = ann_ids(&e);
    assert!(flushed.iter().all(|f| ids.contains(f)), "{ids:?}");
    assert!(anns.iter().all(|a| !ids.contains(a)));
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    assert_eq!(ids.len(), 20 - anns.len() + 1 + 2);
    assert_eq!(manifest_ann_ids(dir.path()), ids);
    drop(now);
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(values_of(&e.snapshot(), 2), vec![Some(3.0)]);
    assert_eq!(values_of(&e.snapshot(), 5), vec![Some(3.0)]);
    assert_eq!(ann_ids(&e), ids);
    e.shutdown().unwrap();
}
