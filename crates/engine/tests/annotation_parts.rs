//! Annotations in parts: the flush, the window rule, start-up by kind, reads
//! across parts and the buffers, the list filter's fast and exact paths, and
//! maintenance with annotation parts live.

mod common;
mod support;

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use nwdb_core::{Error, PartKind};
use nwdb_engine::{Annotation, AnnotationFilter, Engine, ListQuery, Stage};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use support::{disk_state, tree};

const W: i64 = 1_000;

type Seen = Vec<(Stage, Vec<Option<f64>>)>;

/// One trace with a single span starting at `start`.
fn span_at(trace: u128, span: u64, start: i64) -> TSpan {
    TSpan::new(trace, span, start).root()
}

fn windows_of_parts(e: &Engine) -> Vec<(i64, i64)> {
    e.snapshot()
        .annotation_part_info()
        .iter()
        .map(|&(_, start, ns, _)| (start, ns))
        .collect()
}

fn values(v: &[Annotation]) -> Vec<Option<f64>> {
    v.iter().map(|a| a.value).collect()
}

#[test]
fn a_round_trip_through_parts_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
    }
    annotate(
        &e,
        vec![
            score(1, None, "q", "s", 1.0),
            score(1, Some(1), "q", "s", 2.0),
            score(2, None, "q", "s", 3.0),
        ],
    );
    e.flush().unwrap();
    assert_eq!(e.stats().annotation_parts, 1);
    assert_eq!(e.stats().annotations_buffered, 0);
    assert_eq!(shown(&e, 2).len(), 1);

    // An overwrite and a delete in a later part of the same window.
    annotate(
        &e,
        vec![score(1, None, "q", "s", 4.0), delete(2, None, "q", "s")],
    );
    e.flush().unwrap();
    assert_eq!(e.stats().annotation_parts, 2);
    assert_eq!(windows_of_parts(&e), vec![(0, W), (0, W)]);
    assert!(shown(&e, 2).is_empty(), "a delete in a later part hides");
    assert_eq!(values(&shown(&e, 1)), vec![Some(4.0), Some(2.0)]);

    // An annotation after the delete shows again, from the buffer.
    annotate(&e, vec![score(2, None, "q", "s", 5.0)]);
    assert_eq!(values(&shown(&e, 2)), vec![Some(5.0)]);
    let stats = e.stats();
    assert_eq!(stats.annotations_written, 5);
    assert_eq!(stats.annotations_buffered, 1);
    assert!(stats.annotation_bytes > 0);

    let before: Vec<Vec<Annotation>> = (1..=3).map(|t| shown(&e, t)).collect();
    drop(e);
    let e = open(opts(dir.path()));
    let after: Vec<Vec<Annotation>> = (1..=3).map(|t| shown(&e, t)).collect();
    assert_eq!(after, before, "the same winners with the same arrival");
    assert_eq!(e.stats().annotation_parts, 2);
    e.flush().unwrap();
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    let again: Vec<Vec<Annotation>> = (1..=3).map(|t| shown(&e, t)).collect();
    assert_eq!(again, before);
    assert_eq!(e.stats().annotation_parts, 3);
    e.shutdown().unwrap();
}

#[test]
fn an_annotation_goes_to_the_largest_window_of_its_trace() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    // Trace 1: a span in window 0, then a new span in window 2000 and an
    // annotation in the same flush: this flush's window is the larger.
    ingest(&e, &[span_at(1, 1, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(1, 2, 2_500)]).unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    assert_eq!(windows_of_parts(&e), vec![(2 * W, W)]);

    // Trace 2: its part is in window 2000; a late span with an early start
    // is flushed with the annotation. This flush's window (0) is below the
    // part's, and the annotation still goes to 2000.
    ingest(&e, &[span_at(2, 1, 2_100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(2, 2, 50)]).unwrap();
    annotate(&e, vec![score(2, None, "q", "s", 2.0)]);
    e.flush().unwrap();
    assert_eq!(windows_of_parts(&e), vec![(2 * W, W), (2 * W, W)]);

    // Trace 3: parts in windows 0 and 3000 and the annotation alone in its
    // flush: the larger of the holders' windows.
    ingest(&e, &[span_at(3, 1, 10)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(3, 2, 3_500)]).unwrap();
    e.flush().unwrap();
    annotate(&e, vec![score(3, None, "q", "s", 3.0)]);
    e.flush().unwrap();
    assert_eq!(
        windows_of_parts(&e),
        vec![(2 * W, W), (2 * W, W), (3 * W, W)]
    );
    for t in 1..=3 {
        assert_eq!(values(&shown(&e, t)), vec![Some(t as f64)], "trace {t}");
    }
    assert_eq!(e.stats().annotations_dropped, 0);
    e.shutdown().unwrap();
}

fn retention_opts(dir: &Path, now: &Arc<AtomicI64>) -> nwdb_engine::EngineOptions {
    let mut o = opts(dir);
    o.retention = Some(Duration::from_nanos(1_000));
    let clock = now.clone();
    o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
    o.maintenance_interval = Duration::from_secs(3600);
    o
}

#[test]
fn an_annotation_of_a_trace_that_retention_removed_is_dropped_and_counted_once() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let fail = Arc::new(AtomicBool::new(true));
    let f2 = fail.clone();
    let mut o = retention_opts(dir.path(), &now);
    o.stage_hook = Some(Arc::new(move |s| {
        if s == Stage::AllPartsWritten && f2.swap(false, Ordering::SeqCst) {
            Err(Error::other("injected"))
        } else {
            Ok(())
        }
    }));
    let e = open(o);
    // The flush the hook fails is the first one the test makes, so hold it
    // back until the setup is done.
    fail.store(false, Ordering::SeqCst);
    ingest(&e, &[span_at(5, 1, 100), span_at(6, 1, 5_000)]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![score(5, None, "q", "s", 1.0), score(6, None, "q", "s", 2.0)],
    );
    // The clock passes window 0 and its retention: trace 5 goes.
    now.store(3_000, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    assert!(e.get_trace(tid(5)).unwrap().is_none());
    assert!(e.get_trace(tid(6)).unwrap().is_some());

    fail.store(true, Ordering::SeqCst);
    let before = e.stats();
    assert!(e.flush().is_err());
    let failed = e.stats();
    assert_eq!(failed.annotations_dropped, 0, "counted by a failed flush");
    assert_eq!(failed.annotations_written, 0);
    assert_eq!(failed.annotations_buffered, 2, "the buffer was restored");
    assert_eq!(failed.flushed_lsn, before.flushed_lsn);
    assert_eq!(
        shown(&e, 5).len(),
        1,
        "still visible after the failed flush"
    );
    assert!(part_dirs_only_live(dir.path(), &e));

    e.flush().unwrap();
    let done = e.stats();
    assert_eq!(done.annotations_dropped, 1);
    assert_eq!(done.annotations_written, 1);
    assert_eq!(done.annotation_parts, 1);
    assert!(done.flushed_lsn > before.flushed_lsn);
    assert!(shown(&e, 5).is_empty());
    assert_eq!(values(&shown(&e, 6)), vec![Some(2.0)]);
    // A flush with nothing to do does not count again.
    e.flush().unwrap();
    assert_eq!(e.stats().annotations_dropped, 1);
    e.shutdown().unwrap();
}

/// Every part directory on disk is live in the engine: nothing a failed
/// flush wrote is left.
fn part_dirs_only_live(dir: &Path, e: &Engine) -> bool {
    let snap = e.snapshot();
    let mut live: Vec<String> = snap
        .part_traces()
        .iter()
        .map(|(id, _)| nwdb_core::part_dir_name(*id))
        .collect();
    live.extend(
        snap.annotation_part_info()
            .iter()
            .map(|(id, ..)| nwdb_core::part_dir_name(*id)),
    );
    let on_disk = part_dir_names(dir);
    on_disk.iter().all(|d| live.contains(d))
}

#[test]
fn an_annotation_only_flush_commits_trims_the_log_and_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(small_files(dir.path()));
    ingest(&e, &one(1)).unwrap();
    ingest(&e, &one(2)).unwrap();
    e.flush().unwrap();
    for v in 1..=3 {
        annotate(&e, vec![score(1, None, "q", "s", f64::from(v))]);
    }
    let before = e.stats();
    assert!(before.acked_lsn > before.flushed_lsn);
    assert!(count_files(&dir.path().join("wal"), "wal-") >= 3);

    e.flush().unwrap();
    let after = e.stats();
    assert_eq!(after.flushed_lsn, after.acked_lsn);
    assert_eq!(after.annotation_parts, 1);
    assert_eq!(after.annotations_written, 1, "one record per identity");
    assert_eq!(after.annotations_buffered, 0);
    assert_eq!(count_files(&dir.path().join("wal"), "wal-"), 1);
    let want = shown(&e, 1);
    drop(e);

    let e = open(small_files(dir.path()));
    assert_eq!(e.stats().annotations_buffered, 0, "replayed twice");
    assert_eq!(e.stats().flushed_lsn, after.flushed_lsn);
    assert_eq!(shown(&e, 1), want);
    e.shutdown().unwrap();
}

#[test]
fn a_buffer_of_only_annotations_is_flushed_by_age() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.flush_interval = Duration::from_millis(100);
    let e = open(o);
    ingest(&e, &one(1)).unwrap();
    nwdb_testkit::wait_for("the spans are flushed", Duration::from_secs(30), || {
        e.stats().parts == 1
    });
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    nwdb_testkit::wait_for(
        "the annotations are flushed",
        Duration::from_secs(30),
        || e.stats().annotation_parts == 1,
    );
    assert_eq!(e.stats().annotations_buffered, 0);
    assert_eq!(values(&shown(&e, 1)), vec![Some(1.0)]);
    e.shutdown().unwrap();
}

#[test]
fn a_buffer_of_only_annotations_is_flushed_by_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.buffer_bytes = 2_000;
    let e = open(o);
    ingest(&e, &one(1)).unwrap();
    // The span batch alone may pass the limit: wait for its flush.
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 1);
    let items: Vec<Annotation> = (0..30)
        .map(|i| score(1, None, &format!("n{i}"), "s", f64::from(i)))
        .collect();
    annotate(&e, items);
    nwdb_testkit::wait_for(
        "the annotations are flushed by size",
        Duration::from_secs(30),
        || e.stats().annotation_parts == 1,
    );
    assert_eq!(e.stats().annotations_buffered, 0);
    assert_eq!(shown(&e, 1).len(), 30);
    e.shutdown().unwrap();
}

#[test]
fn an_annotation_is_visible_exactly_once_at_every_stage_of_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<Mutex<Option<Engine>>> = Arc::default();
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let (e2, s2) = (engine.clone(), seen.clone());
    let mut o = opts(dir.path());
    o.stage_hook = Some(Arc::new(move |stage| {
        let e = e2.lock().unwrap().clone();
        if let Some(e) = e {
            let v = values(&e.snapshot().annotations(tid(1), 10).unwrap().0);
            s2.lock().unwrap().push((stage, v));
        }
        Ok(())
    }));
    let e = open(o);
    *engine.lock().unwrap() = Some(e.clone());
    ingest(&e, &one(1)).unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 7.0)]);
    seen.lock().unwrap().clear();
    e.flush().unwrap();
    let seen = seen.lock().unwrap().clone();
    let stages: Vec<Stage> = seen.iter().map(|s| s.0).collect();
    for want in [
        Stage::FlushFrozen,
        Stage::PartWritten,
        Stage::AllPartsWritten,
        Stage::Committed,
        Stage::Published,
    ] {
        assert!(stages.contains(&want), "{want:?} was not reached");
    }
    // Two parts were written (spans, then annotations).
    assert_eq!(
        stages.iter().filter(|s| **s == Stage::PartWritten).count(),
        2
    );
    for (stage, v) in &seen {
        assert_eq!(*v, vec![Some(7.0)], "at {stage:?}");
    }
    *engine.lock().unwrap() = None;
    e.shutdown().unwrap();
}

#[test]
fn readers_looping_during_flushes_never_miss_a_winner_or_see_an_older_value() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &one(1)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (e, stop) = (e.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut last = 0.0f64;
                let mut reads = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    let v = e.snapshot().annotations(tid(1), 10).unwrap().0;
                    if v.is_empty() {
                        assert_eq!(last, 0.0, "a winner went missing");
                    } else {
                        assert_eq!(v.len(), 1);
                        let now = v[0].value.unwrap();
                        assert!(now >= last, "{now} after {last}");
                        last = now;
                    }
                    reads += 1;
                }
                reads
            })
        })
        .collect();
    for v in 1..=80 {
        annotate(&e, vec![score(1, None, "q", "s", f64::from(v))]);
        assert_eq!(shown(&e, 1)[0].value, Some(f64::from(v)));
        if v % 3 == 0 {
            e.flush().unwrap();
        }
    }
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    e.shutdown().unwrap();
}

/// Six traces: 1 to 5 in window 0 and trace 6 split over windows 0 and 2000.
/// Annotations: parts first, then the buffer.
fn filter_engine(dir: &Path) -> Engine {
    let e = open(opts(dir));
    for t in 1..=6u128 {
        ingest(&e, &[span_at(t, 1, 100 + t as i64), TSpan::new(t, 2, 150)]).unwrap();
    }
    e.flush().unwrap();
    ingest(&e, &[TSpan::new(6, 3, 2_500)]).unwrap();
    e.flush().unwrap();
    annotate(
        &e,
        vec![
            score(1, None, "score", "judge", 0.9),
            score(2, None, "score", "human", 0.3),
            with_label(score(2, None, "tag", "human", 0.0), "bad"),
            with_label(score(3, Some(2), "score", "judge", 0.5), "ok"),
            score(4, None, "score", "judge", 0.7),
            score(6, None, "score", "judge", 0.6),
        ],
    );
    e.flush().unwrap();
    annotate(&e, vec![delete(4, None, "score", "judge")]);
    e.flush().unwrap();
    // The buffer: an overwrite that changes trace 1's answer, and a trace
    // with nothing but a buffered annotation.
    annotate(
        &e,
        vec![
            score(1, None, "score", "judge", 0.1),
            score(5, None, "score", "judge", 0.8),
        ],
    );
    e
}

#[test]
fn the_list_filter_over_parts_the_buffer_and_both() {
    let dir = tempfile::tempdir().unwrap();
    let e = filter_engine(dir.path());
    assert_eq!(e.stats().annotation_parts, 3);
    let f = filter;
    let none = Vec::<u128>::new();
    // Newest first: the traces start at 106 (6), 105, 103, 102, 101.
    assert_eq!(
        listed(&e, f("score")),
        vec![6, 5, 3, 2, 1],
        "name; 4 is deleted"
    );
    assert_eq!(listed(&e, f("nothing")), none);
    // Trace 6 reaches the listing through its older part only; its annotation
    // is in the newer window, which the exact path finds through the split
    // map.
    let early = ListQuery {
        annotation: Some(f("score")),
        to_ns: 2 * W,
        ..list_all()
    };
    let ids: Vec<u128> = e
        .list_traces(&early)
        .unwrap()
        .iter()
        .map(|s| u128::from_be_bytes(s.trace_id.0))
        .collect();
    assert_eq!(ids, vec![6, 5, 3, 2, 1]);
    let with = |g: fn(&mut AnnotationFilter)| {
        let mut x = f("score");
        g(&mut x);
        listed(&e, x)
    };
    assert_eq!(with(|x| x.source = Some("judge".into())), vec![6, 5, 3, 1]);
    assert_eq!(with(|x| x.source = Some("human".into())), vec![2]);
    assert_eq!(
        with(|x| x.min = Some(0.5)),
        vec![6, 5, 3],
        "trace 1 is 0.1 now"
    );
    assert_eq!(with(|x| x.max = Some(0.3)), vec![2, 1]);
    assert_eq!(
        with(|x| {
            x.min = Some(0.2);
            x.max = Some(0.6)
        }),
        vec![6, 3, 2]
    );
    assert_eq!(
        with(|x| x.label = Some("ok".into())),
        vec![3],
        "a span-level match"
    );
    let mut tag = f("tag");
    tag.label = Some("bad".into());
    assert_eq!(listed(&e, tag), vec![2]);

    // The same after the buffer is flushed and after a restart.
    let want = listed(&e, f("score"));
    e.flush().unwrap();
    assert_eq!(listed(&e, f("score")), want);
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(listed(&e, f("score")), want);
    assert_eq!(with_min(&e, 0.5), vec![6, 5, 3]);
    e.shutdown().unwrap();
}

fn with_min(e: &Engine, min: f64) -> Vec<u128> {
    let mut f = filter("score");
    f.min = Some(min);
    listed(e, f)
}

#[test]
fn the_exact_path_serves_split_traces_and_buffered_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let e = filter_engine(dir.path());
    let q = ListQuery {
        annotation: Some(filter("score")),
        ..list_all()
    };
    let (rows, stats) = e.snapshot().list_traces_with_stats(&q).unwrap();
    assert_eq!(rows.len(), 5);
    // Trace 6 is split, traces 1 and 5 have buffered annotations.
    assert_eq!(stats.annotation_exact_lookups, 3);
    // Window 0 (the only one the other candidates are in) is scanned once.
    assert_eq!(stats.annotation_windows_scanned, 1);
    e.shutdown().unwrap();
}

/// `windows` windows of `per` traces each: trace `1000 * w + i`, its span in
/// window `w`. Everything flushed once, then every trace annotated `name`
/// (when `all`) and flushed again.
fn many_windows(dir: &Path, windows: i64, per: i64, name: &str, only: &[i64]) -> Engine {
    let e = open(opts(dir));
    for w in 0..windows {
        let spans: Vec<TSpan> = (0..per)
            .map(|i| span_at((1000 * w + i + 1) as u128, 1, w * W + 10 + i))
            .collect();
        ingest(&e, &spans).unwrap();
    }
    e.flush().unwrap();
    assert_eq!(e.stats().parts as i64, windows);
    for w in 0..windows {
        if !only.is_empty() && !only.contains(&w) {
            continue;
        }
        let n = if only.is_empty() { per } else { 1 };
        let items: Vec<Annotation> = (0..n)
            .map(|i| score((1000 * w + i + 1) as u128, None, name, "s", 1.0))
            .collect();
        annotate(&e, items);
    }
    e.flush().unwrap();
    e
}

#[test]
fn a_filter_that_most_traces_match_stops_early() {
    let dir = tempfile::tempdir().unwrap();
    let e = many_windows(dir.path(), 10, 60, "all", &[]);
    assert_eq!(e.stats().annotation_parts, 10);
    let q = ListQuery {
        annotation: Some(filter("all")),
        limit: 50,
        ..list_all()
    };
    let (rows, stats) = e.snapshot().list_traces_with_stats(&q).unwrap();
    assert_eq!(rows.len(), 50);
    assert!(stats.annotation_windows_scanned <= 2, "{stats:?}");
    assert_eq!(stats.annotation_exact_lookups, 0);

    // Nothing matches: every window is scanned once, and no trace is looked
    // up on its own.
    let q = ListQuery {
        annotation: Some(filter("rare")),
        ..list_all()
    };
    let (rows, stats) = e.snapshot().list_traces_with_stats(&q).unwrap();
    assert!(rows.is_empty());
    assert_eq!(stats.annotation_windows_scanned, 10);
    assert_eq!(stats.annotation_exact_lookups, 0);

    // The cap on one window's set.
    let q = ListQuery {
        annotation: Some(filter("all")),
        ..list_all()
    };
    assert_eq!(
        e.snapshot()
            .with_annotation_set_cap(60)
            .list_traces(&q)
            .unwrap()
            .len(),
        600
    );
    let err = e
        .snapshot()
        .with_annotation_set_cap(59)
        .list_traces(&q)
        .unwrap_err();
    let Error::InvalidArgument(msg) = &err else {
        panic!("{err}")
    };
    assert!(msg.contains("more than 59 traces of one window"), "{msg}");
    e.shutdown().unwrap();
}

#[test]
fn a_filter_that_few_traces_match_scans_each_window_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = many_windows(dir.path(), 10, 60, "rare", &[2, 5, 8]);
    assert_eq!(e.stats().annotation_parts, 3);
    let q = ListQuery {
        annotation: Some(filter("rare")),
        ..list_all()
    };
    let (rows, stats) = e.snapshot().list_traces_with_stats(&q).unwrap();
    let ids: Vec<u128> = rows
        .iter()
        .map(|s| u128::from_be_bytes(s.trace_id.0))
        .collect();
    assert_eq!(ids, vec![8001, 5001, 2001]);
    assert_eq!(stats.annotation_windows_scanned, 3);
    assert_eq!(stats.annotation_exact_lookups, 0);
    // Reading one trace's annotations touches one part and one block.
    let (v, more, st) = e.snapshot().annotations_with_stats(tid(5001), 10).unwrap();
    assert_eq!((v.len(), more), (1, false));
    assert_eq!((st.parts_probed, st.blocks_read), (1, 1));
    let (v, _, st) = e.snapshot().annotations_with_stats(tid(5002), 10).unwrap();
    assert!(v.is_empty());
    assert_eq!(st.parts_probed, 1);
    e.shutdown().unwrap();
}

#[test]
fn an_annotation_part_that_cannot_be_opened_refuses_the_start() {
    let make = || {
        let dir = tempfile::tempdir().unwrap();
        let e = open(opts(dir.path()));
        ingest(&e, &one(1)).unwrap();
        annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
        e.shutdown().unwrap();
        let (state, _) = disk_state(dir.path());
        let part = state
            .parts
            .iter()
            .find(|p| p.kind == PartKind::Annotations)
            .expect("an annotation part")
            .clone();
        (dir, state, part)
    };
    let refuse = |dir: &Path| {
        let before = tree(dir);
        let err = Engine::open(opts(dir), decoders())
            .err()
            .expect("a damaged annotation part was accepted");
        assert!(matches!(err, Error::Corrupt(_)), "{err}");
        assert_eq!(tree(dir), before, "the refused start changed the directory");
        err
    };
    let file = |dir: &Path, id: u64| {
        dir.join("parts")
            .join(nwdb_core::part_dir_name(id))
            .join("annotations.nwa")
    };

    // Another format version, in the manifest.
    let (dir, state, part) = make();
    let mut state = (*state).clone();
    for p in &mut state.parts {
        if p.part_id == part.part_id {
            p.format_version = 99;
        }
    }
    let manifest = dir
        .path()
        .join("manifest")
        .join(format!("{:020}.json", state.version));
    std::fs::write(&manifest, state.encode().unwrap()).unwrap();
    let err = refuse(dir.path());
    assert!(err.to_string().contains("version 99"), "{err}");

    // A truncated file.
    let (dir, _, part) = make();
    let f = file(dir.path(), part.part_id);
    let len = std::fs::metadata(&f).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&f)
        .unwrap()
        .set_len(len / 2)
        .unwrap();
    refuse(dir.path());

    // A missing file.
    let (dir, _, part) = make();
    std::fs::remove_file(file(dir.path(), part.part_id)).unwrap();
    refuse(dir.path());

    // Undamaged, it opens.
    let (dir, _, _) = make();
    let e = open(opts(dir.path()));
    assert_eq!(shown(&e, 1).len(), 1);
    assert_eq!(e.stats().annotation_parts, 1);
    e.shutdown().unwrap();
}

#[test]
fn an_unreferenced_annotation_part_directory_is_removed_at_start_up() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &one(1)).unwrap();
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    e.flush().unwrap();
    e.shutdown().unwrap();
    let (state, _) = disk_state(dir.path());
    let part = state
        .parts
        .iter()
        .find(|p| p.kind == PartKind::Annotations)
        .unwrap();
    // A copy of the live annotation part under an id the manifest does not
    // know, as a crashed flush leaves.
    let stray = state.next_part_id + 3;
    support::copy_dir(
        &dir.path()
            .join("parts")
            .join(nwdb_core::part_dir_name(part.part_id)),
        &dir.path()
            .join("parts")
            .join(nwdb_core::part_dir_name(stray)),
    );
    let e = open(opts(dir.path()));
    assert_eq!(e.stats().annotation_parts, 1);
    e.shutdown().unwrap();
    assert!(disk_state(dir.path()).1.is_empty());
}

#[test]
fn an_injected_error_while_an_annotation_part_is_written_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let parts_written = Arc::new(Mutex::new(0u32));
    let armed = Arc::new(AtomicBool::new(false));
    let (p2, a2) = (parts_written.clone(), armed.clone());
    let mut o = opts(dir.path());
    o.stage_hook = Some(Arc::new(move |s| {
        if s == Stage::PartWritten && a2.load(Ordering::SeqCst) {
            let mut n = p2.lock().unwrap();
            *n += 1;
            // The first part is the spans'; fail after the annotation part.
            if *n == 2 {
                return Err(Error::other("injected"));
            }
        }
        Ok(())
    }));
    let e = open(o);
    ingest(&e, &one(1)).unwrap();
    e.flush().unwrap();
    // Two annotation records, then spans and annotations in one flush.
    annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
    ingest(&e, &[span_at(2, 1, 2_100)]).unwrap();
    annotate(
        &e,
        vec![score(1, None, "q", "s", 2.0), score(2, None, "q", "s", 3.0)],
    );
    let before = e.stats();
    let order: Vec<Vec<Annotation>> = (1..=2).map(|t| shown(&e, t)).collect();
    armed.store(true, Ordering::SeqCst);
    assert!(e.flush().is_err());
    let after = e.stats();
    assert_eq!(after.annotations_written, before.annotations_written);
    assert_eq!(after.annotations_dropped, 0);
    assert_eq!(after.annotations_buffered, before.annotations_buffered);
    assert_eq!(after.flushed_lsn, before.flushed_lsn);
    assert_eq!(after.annotation_parts, 0);
    assert_eq!(after.parts, 1);
    assert!(part_dirs_only_live(dir.path(), &e));
    let restored: Vec<Vec<Annotation>> = (1..=2).map(|t| shown(&e, t)).collect();
    assert_eq!(restored, order, "the buffer was restored in order");

    e.flush().unwrap();
    let done = e.stats();
    assert_eq!(done.annotations_written, 2);
    assert_eq!(done.annotation_parts, 2);
    assert_eq!(done.annotations_buffered, 0);
    assert_eq!(values(&shown(&e, 1)), vec![Some(2.0)]);
    assert_eq!(values(&shown(&e, 2)), vec![Some(3.0)]);
    e.shutdown().unwrap();
}

#[test]
fn maintenance_with_annotation_parts_live_keeps_them_listed_and_published() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(0));
    let mut o = compact_opts(dir.path());
    o.retention = Some(Duration::from_nanos(1_000));
    let clock = now.clone();
    o.clock = Some(Arc::new(move || clock.load(Ordering::SeqCst)));
    let e = open(o);
    // Four windows, two span parts each, and annotations on every trace.
    let mut traces = Vec::new();
    for w in 0..4i64 {
        for k in 0..2u128 {
            let t = (w as u128) * 10 + k + 1;
            traces.push(t);
            ingest(&e, &[span_at(t, 1, w * W + 10)]).unwrap();
            e.flush().unwrap();
        }
        let ids: Vec<Annotation> = (0..2u128)
            .map(|k| score((w as u128) * 10 + k + 1, None, "q", "s", w as f64))
            .collect();
        annotate(&e, ids);
        e.flush().unwrap();
    }
    assert_eq!(e.stats().annotation_parts, 4);
    let ann_before = e.snapshot().annotation_part_info();

    // Retention removes window 0 and merging joins the others' span parts.
    now.store(2_500, Ordering::SeqCst);
    e.run_maintenance().unwrap();
    let s = e.stats();
    assert!(s.retention_parts_removed >= 2, "{s:?}");
    assert!(s.merges_done >= 1, "{s:?}");
    // The annotation part of window 0 expired with it; the others are
    // untouched.
    assert_eq!(s.annotation_parts, 3, "{s:?}");
    assert_eq!(
        e.snapshot().annotation_part_info(),
        ann_before[1..].to_vec()
    );
    // Window 0's traces are gone; the rest keep their annotations.
    for t in traces.iter().filter(|t| **t >= 10) {
        assert_eq!(shown(&e, *t).len(), 1, "trace {t}");
    }
    assert_eq!(shown(&e, 21).len(), 1);
    e.shutdown().unwrap();

    let state = Manifest::open(
        dir.path().join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current();
    let listed: Vec<u64> = state
        .parts
        .iter()
        .filter(|p| p.kind == PartKind::Annotations)
        .map(|p| p.part_id)
        .collect();
    let ids: Vec<u64> = ann_before[1..].iter().map(|x| x.0).collect();
    assert_eq!(listed, ids);
    let e = open(opts(dir.path()));
    assert_eq!(e.stats().annotation_parts, 3);
    assert_eq!(shown(&e, 31).len(), 1);
    e.shutdown().unwrap();
}

#[test]
fn annotate_ingest_flush_list_open_and_maintenance_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    o.compaction.background = true;
    let e = open(o);
    for t in 1..=8 {
        ingest(&e, &[span_at(t, 1, (t as i64 % 4) * W + 5)]).unwrap();
    }
    type Key = (u128, String, String);
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for k in 0..3u64 {
        let e = e.clone();
        writers.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(100 + k);
            let mut last: BTreeMap<Key, Option<f64>> = BTreeMap::new();
            for n in 0..100 {
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
                ingest(&e, &[span_at(t, 1, (t as i64 % 5) * W + 7)]).unwrap();
                annotate(&e, vec![score(t, None, "a", "ing", 1.0)]);
                last.insert((t, "a".into(), "ing".into()), Some(1.0));
            }
            last
        })
    };
    let path = dir.path().to_path_buf();
    let background: Vec<_> = (0..4)
        .map(|which| {
            let (e, stop, path) = (e.clone(), stop.clone(), path.clone());
            std::thread::spawn(move || {
                let mut rounds = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    match which {
                        0 => e.flush().unwrap(),
                        1 => e.run_maintenance().unwrap(),
                        2 => {
                            let q = ListQuery {
                                annotation: Some(filter("a")),
                                ..list_all()
                            };
                            e.list_traces(&q).unwrap();
                        }
                        _ => {
                            let r = Engine::open(opts(&path), decoders());
                            assert!(matches!(r.err(), Some(Error::Conflict(_))));
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
    stop.store(true, Ordering::SeqCst);
    for h in background {
        assert!(h.join().unwrap() > 0);
    }
    let check = |e: &Engine, what: &str| {
        let traces: HashSet<u128> = model.keys().map(|k| k.0).collect();
        for t in traces {
            let want: Vec<(Option<u64>, String, String, Option<f64>)> = model
                .iter()
                .filter(|(k, v)| k.0 == t && v.is_some())
                .map(|(k, v)| (None, k.1.clone(), k.2.clone(), *v))
                .collect();
            assert_eq!(brief(&shown(e, t)), want, "{what}: trace {t}");
        }
    };
    check(&e, "at the end");
    e.flush().unwrap();
    check(&e, "after a flush");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    check(&e, "after a restart");
    e.shutdown().unwrap();
}
