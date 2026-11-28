//! Retention and merging of annotation parts: both kinds expire together,
//! annotation parts of one window are merged into one without losing a
//! winner or a delete, a damaged or failing annotation window does not hold
//! back the span merges of its window, and shutdown abandons an annotation
//! merge.

mod common;
mod support;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::{Error, PartKind, part_dir_name};
use nwdb_engine::{Engine, EngineOptions, Stage};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use support::disk_state;

/// The test windows are 1000 ns wide (see `common::opts`).
const W: i64 = 1_000;

type Clock = Arc<AtomicI64>;

fn span_at(trace: u128, span: u64, start: i64) -> TSpan {
    TSpan::new(trace, span, start).root()
}

/// Options with a retention of one window and a clock the test moves. With
/// `merge`, merging is on and happens only in `run_maintenance`.
fn with_clock(dir: &Path, merge: bool, now: &Clock) -> EngineOptions {
    let mut o = if merge { compact_opts(dir) } else { opts(dir) };
    o.retention = Some(Duration::from_nanos(W as u64));
    let now = now.clone();
    o.clock = Some(Arc::new(move || now.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_secs(3600);
    o
}

/// Ids of the live parts of both kinds.
fn live_ids(e: &Engine) -> Vec<u64> {
    let s = e.snapshot();
    let mut ids: Vec<u64> = s.part_traces().iter().map(|(id, _)| *id).collect();
    ids.extend(s.annotation_part_info().iter().map(|x| x.0));
    ids.sort_unstable();
    ids
}

fn ann_info(e: &Engine) -> Vec<(u64, i64, i64, u64)> {
    e.snapshot().annotation_part_info()
}

/// The directories on disk are exactly the live parts of both kinds.
fn assert_dirs_are_live(dir: &Path, e: &Engine, what: &str) {
    let mut names: Vec<String> = live_ids(e).iter().map(|i| part_dir_name(*i)).collect();
    names.sort();
    assert_eq!(part_dir_names(dir), names, "{what}");
    let (state, unreferenced) = disk_state(dir);
    assert!(unreferenced.is_empty(), "{what}: {unreferenced:?}");
    let listed: Vec<u64> = {
        let mut v: Vec<u64> = state.parts.iter().map(|p| p.part_id).collect();
        v.sort_unstable();
        v
    };
    assert_eq!(listed, live_ids(e), "{what}: manifest");
}

fn values(e: &Engine, t: u128) -> Vec<Option<f64>> {
    shown(e, t).iter().map(|a| a.value).collect()
}

// Retention.

/// Four windows with a span part and an annotation part each. Retention
/// removes the parts of the two expired windows, of both kinds, in one
/// manifest version.
#[test]
fn retention_removes_both_kinds_of_an_expired_window_in_one_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let e = open(with_clock(dir.path(), false, &now));
    for w in 0..4i64 {
        let t = (w as u128) + 1;
        ingest(&e, &[span_at(t, 1, w * W + 10)]).unwrap();
        e.flush().unwrap();
        annotate(&e, vec![score(t, None, "q", "s", w as f64)]);
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, 4);
    assert_eq!(e.stats().annotation_parts, 4);
    let before = ann_info(&e);
    let version = disk_state(dir.path()).0.version;

    // Windows 0 and 1 are older than the retention.
    now.store(3 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    let (state, _) = disk_state(dir.path());
    assert_eq!(state.version, version + 1, "one manifest version");
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (2, 2), "{s:?}");
    assert_eq!(s.retention_parts_removed, 4, "both kinds are counted");
    // Nothing of a live window was touched.
    assert_eq!(ann_info(&e), before[2..].to_vec());
    assert_eq!(values(&e, 3), vec![Some(2.0)]);
    assert_eq!(values(&e, 4), vec![Some(3.0)]);
    assert!(values(&e, 1).is_empty() && values(&e, 2).is_empty());
    assert!(e.get_trace(tid(1)).unwrap().is_none());
    assert!(
        state
            .parts
            .iter()
            .all(|p| p.window_start_ns >= 2 * W && (p.kind == PartKind::Spans || p.rows == 1))
    );
    // The directories are gone.
    assert_dirs_are_live(dir.path(), &e, "after retention");
    // A pass with nothing expired writes no version.
    e.run_maintenance().unwrap();
    assert_eq!(disk_state(dir.path()).0.version, version + 1);
    e.shutdown().unwrap();

    let e = open(opts(dir.path()));
    assert_eq!(e.stats().annotation_parts, 2);
    assert_eq!(values(&e, 4), vec![Some(3.0)]);
    e.shutdown().unwrap();
}

/// A flush decides the window of an annotation, then retention removes the
/// trace's span part before the flush commits: the annotation part is alone in
/// its window, and goes when that window expires.
#[test]
fn an_annotation_part_alone_in_an_expired_window_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(false));
    let mut o = with_clock(dir.path(), false, &now);
    o.stage_hook = Some({
        let (slot, armed, now) = (slot.clone(), armed.clone(), now.clone());
        Arc::new(move |s| {
            if s == Stage::AllPartsWritten && armed.swap(false, Ordering::SeqCst) {
                let e = slot.lock().unwrap().clone().unwrap();
                // Retention takes the span part, then the clock goes back so
                // that no later pass takes the annotation part yet.
                now.store(5 * W, Ordering::SeqCst);
                e.run_maintenance().unwrap();
                now.store(0, Ordering::SeqCst);
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    ingest(&e, &[span_at(1, 1, 10), span_at(2, 1, 4 * W + 10)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    armed.store(true, Ordering::SeqCst);
    e.flush().unwrap();
    // The span part of window 0 is gone, its annotation part is not.
    let info = ann_info(&e);
    assert_eq!(info.len(), 1, "{info:?}");
    assert_eq!(info[0].1, 0);
    assert_eq!(e.stats().parts, 1, "only window 4 is left");
    assert_eq!(e.stats().annotations_dropped, 0);

    now.store(5 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().annotation_parts, 0);
    assert_eq!(e.stats().parts, 1);
    assert_eq!(e.stats().retention_parts_removed, 2);
    assert_dirs_are_live(dir.path(), &e, "after the second pass");
    slot.lock().unwrap().take();
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(e.stats().annotation_parts, 0);
    e.shutdown().unwrap();
}

/// The trace has span parts in windows 0 and 1. a1 is flushed when only
/// window 0 holds the trace, a2 when both do: a2 goes to window 1. When
/// window 0 expires a1 goes with it, and a2 is what is shown; a1 does not
/// come back, and neither does anything older.
#[test]
fn an_overwritten_annotation_does_not_come_back_when_its_older_window_expires() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let e = open(with_clock(dir.path(), true, &now));
    ingest(&e, &[span_at(7, 1, 10), span_at(8, 1, 20)]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![
            score(7, None, "q", "s", 1.0),
            score(8, None, "q", "s", 1.0),
            score(8, None, "gone", "s", 1.0),
        ],
    );
    e.flush().unwrap();
    // New spans of both traces in window 1; a2 for trace 7, a delete for 8.
    ingest(&e, &[TSpan::new(7, 2, W + 10), TSpan::new(8, 2, W + 20)]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![score(7, None, "q", "s", 2.0), delete(8, None, "q", "s")],
    );
    e.flush().unwrap();
    let windows: Vec<i64> = ann_info(&e).iter().map(|x| x.1).collect();
    assert_eq!(windows, vec![0, W], "a2 and the delete are in window 1");
    assert_eq!(values(&e, 7), vec![Some(2.0)]);
    assert_eq!(shown(&e, 8).len(), 1, "only 'gone' is left on trace 8");

    // Window 0 expires, window 1 does not.
    now.store(2 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert_eq!(
        ann_info(&e).iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![W]
    );
    assert_eq!(values(&e, 7), vec![Some(2.0)], "a2 wins, a1 is gone");
    // The delete of window 1 hides nothing that came back: 'gone' was in
    // window 0 and is gone with it, and q is deleted.
    assert!(shown(&e, 8).is_empty(), "{:?}", shown(&e, 8));
    assert!(e.get_trace(tid(7)).unwrap().is_some());
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(values(&e, 7), vec![Some(2.0)]);
    assert!(shown(&e, 8).is_empty());
    e.shutdown().unwrap();
}

/// The mirror: the annotation's own window expires while a later window
/// still holds part of the trace. The annotation is gone, and nothing older
/// appears.
#[test]
fn an_annotation_goes_with_its_window_although_a_later_window_holds_the_trace() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let e = open(with_clock(dir.path(), true, &now));
    ingest(&e, &[span_at(9, 1, 10)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(9, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    annotate(&e, vec![score(9, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(9, 2, W + 10)]).unwrap();
    e.flush().unwrap();
    assert_eq!(values(&e, 9), vec![Some(2.0)]);
    now.store(2 * W, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(values(&e, 9).is_empty(), "{:?}", shown(&e, 9));
    assert_eq!(e.stats().annotation_parts, 0);
    let spans = e
        .get_trace(tid(9))
        .unwrap()
        .expect("window 1 still holds it");
    assert_eq!(from_batches(&spans).len(), 1);
    // After a restart the same.
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert!(values(&e, 9).is_empty());
    e.shutdown().unwrap();
}

// Merging.

/// A delete is a record like any other and is never dropped by a merge: a
/// merge of a window's parts that holds a1 and then the delete keeps the
/// delete, and the identity stays deleted, also after a restart.
#[test]
fn a_delete_survives_the_merge_of_its_window_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    ingest(&e, &[span_at(1, 1, 10), span_at(2, 1, 20)]).unwrap();
    e.flush().unwrap();
    for items in [
        vec![score(1, None, "q", "s", 1.0), score(2, None, "q", "s", 1.0)],
        vec![delete(1, None, "q", "s")],
        vec![score(2, None, "q", "s", 2.0)],
    ] {
        annotate(&e, items);
        e.flush().unwrap();
    }
    assert_eq!(e.stats().annotation_parts, 3);
    assert!(values(&e, 1).is_empty());
    compact_fully(&e);
    let info = ann_info(&e);
    assert_eq!(info.len(), 1);
    // Trace 1 is a delete record, trace 2 its winner: two records.
    assert_eq!(info[0].3, 2, "the delete record is kept");
    assert!(values(&e, 1).is_empty());
    assert_eq!(values(&e, 2), vec![Some(2.0)]);
    assert!(listed(&e, filter("q")) == vec![2]);
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert!(values(&e, 1).is_empty());
    assert_eq!(values(&e, 2), vec![Some(2.0)]);
    e.shutdown().unwrap();
}

type Key = (u128, String, String);
type Shown = Vec<(Option<u64>, String, String, Option<f64>)>;

/// What every trace shows and what filtered listings return, for comparing
/// before and after a merge.
fn picture(e: &Engine) -> (Vec<(u128, Shown)>, Vec<Vec<u128>>) {
    let traces = (1..=12u128).map(|t| (t, brief(&shown(e, t)))).collect();
    let mut f1 = filter("a");
    f1.source = Some("s1".into());
    let mut f2 = filter("b");
    f2.min = Some(3.0);
    f2.max = Some(7.0);
    let lists = vec![
        listed(e, filter("a")),
        listed(e, filter("b")),
        listed(e, f1),
        listed(e, f2),
    ];
    (traces, lists)
}

/// Many flushes of annotations into three windows: after `compact_fully` one
/// annotation part per window, the same winners, the same filtered listings,
/// also after a restart; the superseded records are counted.
#[test]
fn many_flushes_of_annotations_end_as_one_part_per_window_with_the_same_winners() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let spans: Vec<TSpan> = (1..=12u128)
        .map(|t| span_at(t, 1, (t as i64 % 3) * W + 10))
        .collect();
    ingest(&e, &spans).unwrap();
    e.flush().unwrap();
    let mut rng = StdRng::seed_from_u64(0xA77);
    let mut model: BTreeMap<Key, Option<f64>> = BTreeMap::new();
    for _ in 0..9 {
        let items: Vec<_> = (0..14)
            .map(|_| {
                let t = rng.random_range(1..=12u128);
                let name = ["a", "b"][rng.random_range(0..2)];
                let source = ["s1", "s2"][rng.random_range(0..2)];
                let key = (t, name.to_string(), source.to_string());
                if rng.random_bool(0.2) {
                    model.insert(key, None);
                    delete(t, None, name, source)
                } else {
                    let v = f64::from(rng.random_range(0..10));
                    model.insert(key, Some(v));
                    score(t, None, name, source, v)
                }
            })
            .collect();
        annotate(&e, items);
        e.flush().unwrap();
    }
    let before = picture(&e);
    for t in 1..=12u128 {
        let want: Vec<Option<f64>> = model
            .iter()
            .filter(|(k, v)| k.0 == t && v.is_some())
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(values(&e, t).len(), want.len(), "trace {t}");
    }
    let s = e.stats();
    assert_eq!(s.parts, 3);
    assert!(s.annotation_parts >= 9, "{s:?}");
    // The span windows are settled, the annotation windows are not.
    assert!(s.windows_with_many_parts > 0, "{s:?}");
    let written = s.annotations_written;
    // Windows with a single part are not merged.
    let mut per_window: BTreeMap<i64, u64> = BTreeMap::new();
    for x in ann_info(&e) {
        *per_window.entry(x.1).or_default() += 1;
    }
    let merged_away: u64 = per_window.values().filter(|n| **n > 1).sum();

    compact_fully(&e);
    let s = e.stats();
    assert_eq!(s.annotation_parts, 3, "{s:?}");
    assert_eq!(s.parts, 3);
    assert_eq!(s.windows_with_many_parts, 0);
    let rows: u64 = ann_info(&e).iter().map(|x| x.3).sum();
    assert!(s.annotations_superseded > 0, "{s:?}");
    assert_eq!(
        s.annotations_superseded,
        written - rows,
        "every record that is not in the result was overwritten"
    );
    assert_eq!(s.parts_merged_away, merged_away);
    assert_eq!(s.duplicate_spans_dropped, 0);
    assert_eq!(picture(&e), before, "after the merge");
    assert_dirs_are_live(dir.path(), &e, "after the merge");
    // Settled for good: another pass merges nothing.
    let merges = s.merges_done;
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, merges);
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(picture(&e), before, "after a restart");
    e.shutdown().unwrap();
}

/// "Settled" is about both kinds: a window of span parts that has not been
/// merged keeps `windows_with_many_parts` above zero, and so does one of
/// annotation parts, until a merge settles it.
#[test]
fn windows_with_many_parts_counts_both_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    // Window 0: two span parts, one annotation part. Window 1: one span part,
    // two annotation parts.
    ingest(&e, &[span_at(1, 1, 10)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_at(2, 1, 20)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_at(3, 1, W + 10)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    annotate(&e, vec![score(3, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    annotate(&e, vec![score(3, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    assert_eq!(e.stats().windows_with_many_parts, 2);
    e.shutdown().unwrap();

    // Both are unsettled; one pass merges the span window and the
    // annotation window, each on its own.
    let e = open(compact_opts(dir.path()));
    assert_eq!(e.stats().windows_with_many_parts, 2);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, 2);
    assert_eq!(e.stats().windows_with_many_parts, 0);
    let s = e.stats();
    assert_eq!((s.parts, s.annotation_parts), (2, 2), "{s:?}");
    e.shutdown().unwrap();
}

/// Annotation parts that are merged are never merged with span parts of the
/// same window, and the span merge of a window goes on while the annotation
/// merge of it fails: its window backs off.
#[test]
fn a_failing_annotation_window_backs_off_without_holding_back_span_merges() {
    let dir = tempfile::tempdir().unwrap();
    let fail = Arc::new(AtomicU64::new(1));
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    o.stage_hook = Some({
        let fail = fail.clone();
        Arc::new(move |s| {
            if s == Stage::MergeWritten
                && fail
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                return Err(Error::other("injected failure"));
            }
            Ok(())
        })
    });
    let e = open(o);
    // Window 0: three annotation parts (more than the two span parts, so
    // they are merged first) and two span parts.
    ingest(&e, &[span_at(1, 1, 10)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_at(2, 1, 20)]).unwrap();
    e.flush().unwrap();
    for v in 1..=3 {
        annotate(&e, vec![score(1, None, "q", "s", f64::from(v))]);
        e.flush().unwrap();
    }
    let err = e.run_maintenance().unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    let s = e.stats();
    // The span window was merged in the same pass; the annotation window
    // failed and nothing of it changed.
    assert_eq!(
        (s.merges_done, s.parts, s.annotation_parts),
        (1, 1, 3),
        "{s:?}"
    );
    let why = s.last_compaction_error.unwrap();
    assert!(why.contains("injected"), "{why}");
    assert_eq!(values(&e, 1), vec![Some(3.0)]);
    // Backing off: later passes leave it alone and succeed.
    e.run_maintenance().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().annotation_parts, 3);
    assert_dirs_are_live(dir.path(), &e, "after the failure");
    e.shutdown().unwrap();
    // After a restart it is tried again, and merges.
    let e = open(compact_opts(dir.path()));
    compact_fully(&e);
    assert_eq!(e.stats().annotation_parts, 1);
    assert_eq!(values(&e, 1), vec![Some(3.0)]);
    e.shutdown().unwrap();
}

/// An error at a stage of an annotation merge before the commit abandons it:
/// the output directory is deleted, nothing else changes, the next pass
/// merges.
#[test]
fn an_annotation_merge_abandoned_at_a_stage_deletes_its_output() {
    for stage in [Stage::MergeWritten, Stage::BeforeReplaceCommit] {
        let dir = tempfile::tempdir().unwrap();
        let fail = Arc::new(AtomicU64::new(1));
        let mut o = compact_opts(dir.path());
        o.stage_hook = Some({
            let fail = fail.clone();
            Arc::new(move |s| {
                if s == stage
                    && fail
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                {
                    return Err(Error::other("injected failure"));
                }
                Ok(())
            })
        });
        let e = open(o);
        ingest(&e, &[span_at(1, 1, 10)]).unwrap();
        e.flush().unwrap();
        for v in 1..=3 {
            annotate(&e, vec![score(1, None, "q", "s", f64::from(v))]);
            e.flush().unwrap();
        }
        let version = disk_state(dir.path()).0.version;
        let dirs_before = part_dir_names(dir.path());
        assert!(e.run_maintenance().is_err(), "{stage:?}");
        assert_eq!(part_dir_names(dir.path()), dirs_before, "{stage:?}");
        assert_eq!(disk_state(dir.path()).0.version, version, "{stage:?}");
        assert_eq!(e.stats().annotation_parts, 3);
        assert_eq!(values(&e, 1), vec![Some(3.0)]);
        // Retried at the next pass (the backoff is zero).
        e.run_maintenance().unwrap();
        assert_eq!(e.stats().annotation_parts, 1, "{stage:?}");
        assert_eq!(values(&e, 1), vec![Some(3.0)]);
        assert_dirs_are_live(dir.path(), &e, "after the retry");
        e.shutdown().unwrap();
    }
}

/// A damaged annotation input fails its merge with the part named; its
/// window of annotation parts is left alone until a restart, the span merges
/// of the same window go on, and readers of undamaged parts are unaffected.
#[test]
fn a_damaged_annotation_input_leaves_its_window_alone_and_span_merges_go_on() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    // Window 0: traces 1 and 2, window 1: trace 3.
    ingest(&e, &[span_at(1, 1, 10)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_at(2, 1, 20), span_at(3, 1, W + 10)]).unwrap();
    e.flush().unwrap();
    for v in 1..=3 {
        annotate(
            &e,
            vec![
                score(1, None, "q", "s", f64::from(v)),
                score(3, None, "q", "s", f64::from(v)),
            ],
        );
        e.flush().unwrap();
    }
    // Window 0 has two span parts, window 1 one; both have three annotation
    // parts.
    let victim = ann_info(&e).iter().find(|x| x.1 == 0).unwrap().0;
    let file = dir
        .path()
        .join("parts")
        .join(part_dir_name(victim))
        .join("annotations.nwa");
    let mut bytes = std::fs::read(&file).unwrap();
    // Inside the first block, which comes first in the file; the table and
    // the footer at the end were read when the part was opened.
    let at = 16;
    assert!(bytes.len() > 4 * at);
    bytes[at] ^= 0xff;
    std::fs::write(&file, &bytes).unwrap();

    let err = e.run_maintenance().unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    let s = e.stats();
    assert!(!s.fatal);
    let why = s.last_compaction_error.clone().expect("recorded");
    assert!(
        why.contains(&part_dir_name(victim)),
        "names the part: {why}"
    );
    assert!(why.contains("damaged"), "{why}");
    // Window 1's annotation parts were merged in the same pass, and so were
    // the span parts of window 0; window 0's annotation parts are as they
    // were.
    assert_eq!(s.parts, 2, "{s:?}");
    let left: Vec<_> = ann_info(&e).into_iter().filter(|x| x.1 == 0).collect();
    assert_eq!(left.len(), 3, "{left:?}");
    assert_eq!(ann_info(&e).iter().filter(|x| x.1 == W).count(), 1);
    assert_eq!(values(&e, 3), vec![Some(3.0)]);
    // Not tried again before a restart, and the engine goes on.
    let merges = s.merges_done;
    e.run_maintenance().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, merges);
    ingest(&e, &[span_at(4, 1, 30)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[span_at(5, 1, 40)]).unwrap();
    e.flush().unwrap();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 2, "span merges of window 0 go on");
    assert!(!e.stats().fatal);
    e.shutdown().unwrap();
    // After a restart the window is tried again, and fails the same way.
    let e = open(compact_opts(dir.path()));
    let err = e.run_maintenance().unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    e.shutdown().unwrap();
}

/// Shutdown that begins while an annotation merge is about to run stops it:
/// no part is removed, no directory is left, and every annotation is there
/// after the restart.
#[test]
fn shutdown_while_an_annotation_merge_is_in_progress_abandons_it_and_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let slot: Arc<Mutex<Option<Engine>>> = Arc::default();
    let shutdown: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
    let starts = Arc::new(AtomicU64::new(0));
    let mut o = compact_opts(dir.path());
    o.compaction.retry_backoff = Duration::from_secs(3600);
    o.stage_hook = Some({
        let (slot, shutdown, starts) = (slot.clone(), shutdown.clone(), starts.clone());
        Arc::new(move |s| {
            if s == Stage::MergeStarting && starts.fetch_add(1, Ordering::SeqCst) == 0 {
                let e = slot.lock().unwrap().clone().unwrap();
                let e2 = e.clone();
                // A request whose acknowledgement is held back keeps shutdown
                // waiting, so the stop flag is set while the merge runs.
                let (gate, held) = std::sync::mpsc::channel::<()>();
                let first = TSpan::new(1_000, 1, 5).root();
                e.ingest(
                    TAG,
                    encode(std::slice::from_ref(&first)),
                    to_batches(std::slice::from_ref(&first)),
                    move |_| {
                        let _ = held.recv_timeout(Duration::from_secs(30));
                    },
                )
                .unwrap();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(500));
                    drop(gate);
                });
                *shutdown.lock().unwrap() = Some(std::thread::spawn(move || {
                    let _ = e2.shutdown();
                }));
                // Ingest is refused once shutdown has begun, which is also
                // when it has set the stop flag.
                let t = Instant::now();
                loop {
                    let n = starts.fetch_add(1, Ordering::SeqCst) as u128;
                    let span = TSpan::new(2_000 + n, 1, 5).root();
                    let sent = e.ingest(
                        TAG,
                        encode(std::slice::from_ref(&span)),
                        to_batches(std::slice::from_ref(&span)),
                        |_| {},
                    );
                    if sent.is_err() {
                        break;
                    }
                    assert!(t.elapsed() < Duration::from_secs(30), "no shutdown");
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            Ok(())
        })
    });
    let e = open(o);
    *slot.lock().unwrap() = Some(e.clone());
    ingest(
        &e,
        &(1..=5u128)
            .map(|t| span_at(t, 1, t as i64))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    e.flush().unwrap();
    for v in 1..=3 {
        annotate(
            &e,
            (1..=5u128)
                .map(|t| score(t, None, "q", "s", f64::from(v)))
                .collect(),
        );
        e.flush().unwrap();
    }
    let before = part_dir_names(dir.path());
    let outcome = e.run_maintenance();
    shutdown.lock().unwrap().take().unwrap().join().unwrap();
    let s = e.stats();
    slot.lock().unwrap().take();
    drop(e);
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(s.merges_done, 0, "{s:?}");
    assert_eq!(s.last_compaction_error, None);
    assert!(!s.fatal);
    let after = part_dir_names(dir.path());
    assert!(
        before.iter().all(|p| after.contains(p)),
        "an input was removed: {after:?}"
    );
    assert!(after.iter().all(|p| !p.contains("tmp")), "{after:?}");
    let (_, unreferenced) = disk_state(dir.path());
    assert!(unreferenced.is_empty(), "{unreferenced:?}");

    let e = open(compact_opts(dir.path()));
    for t in 1..=5u128 {
        assert_eq!(values(&e, t), vec![Some(3.0)], "trace {t}");
    }
    compact_fully(&e);
    assert_eq!(e.stats().annotation_parts, 1);
    for t in 1..=5u128 {
        assert_eq!(values(&e, t), vec![Some(3.0)], "trace {t} after the merge");
    }
    e.shutdown().unwrap();
}

// Everything at once.

fn window_of_first_group(t: u128) -> i64 {
    (t % 4) as i64
}

fn window_of_second_group(t: u128) -> i64 {
    (t % 5) as i64
}

/// Spans, annotations, flushes, queries, retention (the clock moves past two
/// windows while the rest runs) and merging at once agree with a model at the
/// end: traces of the expired windows are gone with their annotations, the
/// others show their last acknowledged values.
#[test]
fn ingest_annotate_flush_query_retention_and_merging_at_once_agree_with_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(3 * W / 2));
    let mut o = with_clock(dir.path(), true, &now);
    o.compaction.background = true;
    o.maintenance_interval = Duration::from_millis(5);
    let e = open(o);
    for t in 1..=8u128 {
        ingest(&e, &[span_at(t, 1, window_of_first_group(t) * W + 5)]).unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for k in 0..3u64 {
        let e = e.clone();
        writers.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(500 + k);
            let mut last: BTreeMap<Key, Option<f64>> = BTreeMap::new();
            for n in 0..120 {
                let t = rng.random_range(1..=8u128);
                let name = ["a", "b"][rng.random_range(0..2)];
                let source = format!("s{k}");
                let key = (t, name.to_string(), source.clone());
                if rng.random_bool(0.2) {
                    annotate(&e, vec![delete(t, None, name, &source)]);
                    last.insert(key, None);
                } else {
                    annotate(&e, vec![score(t, None, name, &source, f64::from(n))]);
                    last.insert(key, Some(f64::from(n)));
                }
            }
            last
        }));
    }
    let ingester = {
        let e = e.clone();
        std::thread::spawn(move || {
            let mut last: BTreeMap<Key, Option<f64>> = BTreeMap::new();
            for t in 100..140u128 {
                ingest(&e, &[span_at(t, 1, window_of_second_group(t) * W + 7)]).unwrap();
                annotate(&e, vec![score(t, None, "a", "ing", 1.0)]);
                last.insert((t, "a".into(), "ing".into()), Some(1.0));
            }
            last
        })
    };
    let background: Vec<_> = (0..4)
        .map(|which| {
            let (e, stop, now) = (e.clone(), stop.clone(), now.clone());
            std::thread::spawn(move || {
                let mut rounds = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    match which {
                        0 => e.flush().unwrap(),
                        1 => e.run_maintenance().unwrap(),
                        2 => {
                            let q = nwdb_engine::ListQuery {
                                annotation: Some(filter("a")),
                                ..list_all()
                            };
                            e.list_traces(&q).unwrap();
                            for t in 1..=8u128 {
                                let _ = e.snapshot().annotations(tid(t), usize::MAX).unwrap();
                            }
                        }
                        _ => {
                            // Windows 0 and 1 expire a little way in.
                            if rounds == 20 {
                                now.store(3 * W, Ordering::SeqCst);
                            }
                        }
                    }
                    rounds += 1;
                    std::thread::sleep(Duration::from_millis(2));
                }
                rounds
            })
        })
        .collect();
    let mut model: BTreeMap<Key, Option<f64>> = BTreeMap::new();
    for h in writers {
        model.extend(h.join().unwrap());
    }
    model.extend(ingester.join().unwrap());
    // Make sure the clock has moved, then let the background finish.
    now.store(3 * W, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    for h in background {
        assert!(h.join().unwrap() > 0);
    }
    e.flush().unwrap();
    compact_fully(&e);

    let expired = |t: u128| {
        let w = if t < 100 {
            window_of_first_group(t)
        } else {
            window_of_second_group(t)
        };
        w < 2
    };
    let check = |e: &Engine, what: &str| {
        for t in (1..=8u128).chain(100..140) {
            if expired(t) {
                assert!(e.get_trace(tid(t)).unwrap().is_none(), "{what}: trace {t}");
                assert!(values(e, t).is_empty(), "{what}: trace {t}");
                continue;
            }
            let want: Vec<(Option<u64>, String, String, Option<f64>)> = model
                .iter()
                .filter(|(k, v)| k.0 == t && v.is_some())
                .map(|(k, v)| (None, k.1.clone(), k.2.clone(), *v))
                .collect();
            assert_eq!(brief(&shown(e, t)), want, "{what}: trace {t}");
        }
    };
    check(&e, "at the end");
    let s = e.stats();
    assert_eq!(s.windows_with_many_parts, 0, "{s:?}");
    assert!(s.retention_parts_removed > 0 && s.merges_done > 0, "{s:?}");
    assert!(s.annotation_parts <= 3, "{s:?}");
    assert_dirs_are_live(dir.path(), &e, "at the end");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    check(&e, "after a restart");
    e.shutdown().unwrap();
}
