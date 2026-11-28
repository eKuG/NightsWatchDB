//! Annotations: the write path, the buffer, replay and the reads from the
//! buffer.
//!
//! No test here flushes or shuts down cleanly while annotations are in the
//! buffer; a restart is simulated by dropping the engine.

mod common;
mod support;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

use common::*;
use nwdb_core::{Error, SpanId, TraceId};
use nwdb_engine::{
    ANNOTATION_RECORD_TAG, Annotated, Annotation, AnnotationFilter, Engine, ListQuery,
};
use nwdb_segment::MAX_BATCH_ITEMS;
use nwdb_wal::{SyncHook, Wal};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use support::tree;

fn tid(t: u128) -> TraceId {
    TraceId(t.to_be_bytes())
}

fn sid(s: u64) -> SpanId {
    SpanId(s.to_be_bytes())
}

fn score(trace: u128, span: Option<u64>, name: &str, source: &str, v: f64) -> Annotation {
    Annotation {
        trace_id: tid(trace),
        span_id: span.map(sid),
        name: name.into(),
        source: source.into(),
        time_ns: 5,
        value: Some(v),
        label: None,
        comment: None,
        delete: false,
        arrival: 0,
    }
}

fn delete(trace: u128, span: Option<u64>, name: &str, source: &str) -> Annotation {
    Annotation {
        value: None,
        delete: true,
        ..score(trace, span, name, source, 0.0)
    }
}

fn with_label(mut a: Annotation, label: &str) -> Annotation {
    a.label = Some(label.into());
    a
}

/// Annotates and waits for the acknowledgement when something was logged.
fn annotate(e: &Engine, items: Vec<Annotation>) -> Annotated {
    let (tx, rx) = mpsc::channel();
    let r = e
        .annotate(items, move |r| {
            let _ = tx.send(r);
        })
        .expect("annotate");
    if r.accepted > 0 {
        rx.recv_timeout(Duration::from_secs(60))
            .expect("callback was not called")
            .expect("durable");
    } else {
        assert!(
            matches!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected)),
            "the callback of a request with nothing logged must not be called"
        );
    }
    r
}

fn shown(e: &Engine, t: u128) -> Vec<Annotation> {
    let (v, more) = e.snapshot().annotations(tid(t), usize::MAX).unwrap();
    assert!(!more);
    v
}

/// (span, name, source, value) of what is shown.
fn brief(v: &[Annotation]) -> Vec<(Option<u64>, String, String, Option<f64>)> {
    v.iter()
        .map(|a| {
            (
                a.span_id.map(|s| u64::from_be_bytes(s.0)),
                a.name.clone(),
                a.source.clone(),
                a.value,
            )
        })
        .collect()
}

fn base(dir: &Path) -> Engine {
    let e = open(opts(dir));
    for t in 1..=4 {
        ingest(
            &e,
            &[
                TSpan::new(t, 1, 100 + t as i64).root(),
                TSpan::new(t, 2, 105 + t as i64),
            ],
        )
        .unwrap();
    }
    e
}

fn filter(name: &str) -> AnnotationFilter {
    AnnotationFilter {
        name: name.into(),
        source: None,
        min: None,
        max: None,
        label: None,
    }
}

fn listed(e: &Engine, f: AnnotationFilter) -> Vec<u128> {
    listed_limit(e, f, usize::MAX)
}

fn listed_limit(e: &Engine, f: AnnotationFilter, limit: usize) -> Vec<u128> {
    e.list_traces(&ListQuery {
        annotation: Some(f),
        limit,
        ..list_all()
    })
    .unwrap()
    .iter()
    .map(|s| u128::from_be_bytes(s.trace_id.0))
    .collect()
}

#[test]
fn visible_when_durable_not_before_and_the_callback_runs_once() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new((Mutex::new(true), Condvar::new()));
    let hook_gate = gate.clone();
    let hook: SyncHook = Arc::new(move || {
        let (m, cv) = &*hook_gate;
        let mut open = m.lock().unwrap();
        while !*open {
            open = cv.wait(open).unwrap();
        }
        Ok(())
    });
    let mut o = opts(dir.path());
    o.wal.sync_hook = Some(hook);
    let e = open(o);
    ingest(&e, &one(1)).unwrap();

    *gate.0.lock().unwrap() = false;
    let (tx, rx) = mpsc::channel();
    let e2 = e.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = calls.clone();
    let r = e
        .annotate(vec![score(1, None, "q", "s", 0.5)], move |r| {
            calls2.fetch_add(1, Ordering::SeqCst);
            // Read your own write: visible when the callback runs.
            let seen = e2.snapshot().annotations(tid(1), 10).unwrap().0.len();
            tx.send((r, seen)).unwrap();
        })
        .unwrap();
    assert_eq!((r.accepted, r.rejected.len()), (1, 0));
    std::thread::sleep(Duration::from_millis(150));
    assert!(rx.try_recv().is_err(), "acknowledged before the sync");
    assert!(
        shown(&e, 1).is_empty(),
        "visible before the log sync covered it"
    );
    assert_eq!(e.stats().annotations_buffered, 0);

    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let (res, seen) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    res.unwrap();
    assert_eq!(seen, 1);
    assert_eq!(shown(&e, 1).len(), 1);
    assert_eq!(e.stats().annotations_buffered, 1);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn last_wins_overwrite_delete_and_levels() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    let acked = e.stats().acked_lsn;

    // Inside one request the later item wins; the earlier counts as accepted
    // and is not logged: one record, one buffered item.
    let r = annotate(
        &e,
        vec![
            score(1, None, "q", "s", 1.0),
            score(1, None, "q", "s", 2.0),
            score(1, None, "q", "other", 7.0),
        ],
    );
    assert_eq!((r.accepted, r.rejected.len()), (3, 0));
    assert_eq!(e.stats().acked_lsn, acked + 1);
    assert_eq!(e.stats().annotations_buffered, 2);
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![
            (None, "q".into(), "other".into(), Some(7.0)),
            (None, "q".into(), "s".into(), Some(2.0)),
        ]
    );

    // Across requests the newer arrival wins.
    annotate(&e, vec![score(1, None, "q", "s", 3.0)]);
    assert_eq!(shown(&e, 1)[1].value, Some(3.0));
    assert!(shown(&e, 1)[1].arrival > shown(&e, 1)[0].arrival);

    // A delete hides, and an annotation after it shows again.
    annotate(&e, vec![delete(1, None, "q", "s")]);
    assert_eq!(
        brief(&shown(&e, 1)),
        vec![(None, "q".into(), "other".into(), Some(7.0))]
    );
    annotate(&e, vec![score(1, None, "q", "s", 4.0)]);
    assert_eq!(shown(&e, 1)[1].value, Some(4.0));

    // Span-level and trace-level annotations of one name are two identities;
    // the trace-level one sorts first.
    annotate(
        &e,
        vec![
            score(2, Some(1), "q", "s", 1.0),
            score(2, None, "q", "s", 2.0),
        ],
    );
    annotate(&e, vec![delete(2, Some(1), "q", "s")]);
    assert_eq!(
        brief(&shown(&e, 2)),
        vec![(None, "q".into(), "s".into(), Some(2.0))]
    );
    annotate(
        &e,
        vec![
            score(2, Some(2), "q", "s", 9.0),
            score(2, Some(1), "q", "s", 8.0),
        ],
    );
    assert_eq!(
        brief(&shown(&e, 2)),
        vec![
            (None, "q".into(), "s".into(), Some(2.0)),
            (Some(1), "q".into(), "s".into(), Some(8.0)),
            (Some(2), "q".into(), "s".into(), Some(9.0)),
        ]
    );

    // The limit and the more flag.
    let (two, more) = e.snapshot().annotations(tid(2), 2).unwrap();
    assert_eq!((two.len(), more), (2, true));
    let (three, more) = e.snapshot().annotations(tid(2), 3).unwrap();
    assert_eq!((three.len(), more), (3, false));
    assert_eq!(
        e.snapshot().annotations(tid(3), 5).unwrap(),
        (vec![], false)
    );
}

#[test]
fn arrival_is_the_record_number_whatever_the_caller_sets() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    let lsn = e.stats().acked_lsn + 1;
    let mut a = score(1, None, "q", "s", 1.0);
    a.arrival = 999;
    annotate(&e, vec![a]);
    assert_eq!(shown(&e, 1)[0].arrival, lsn);
}

#[test]
fn unknown_traces_are_rejected_with_their_index() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    // A trace that is only in the buffer is known.
    let r = annotate(
        &e,
        vec![
            score(1, None, "q", "s", 1.0),
            score(77, None, "q", "s", 1.0),
            score(2, Some(999), "q", "s", 1.0),
            score(88, None, "q", "s", 1.0),
        ],
    );
    assert_eq!(r.accepted, 2);
    assert_eq!(
        r.rejected,
        vec![
            (1, "unknown trace".to_string()),
            (3, "unknown trace".to_string())
        ]
    );
    assert_eq!(shown(&e, 1).len(), 1);
    // A span id is not checked against the trace's spans.
    assert_eq!(shown(&e, 2).len(), 1);
    assert!(shown(&e, 77).is_empty());
}

#[test]
fn a_trace_that_is_only_in_a_part_is_known() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &one(5)).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 1);
    let r = annotate(
        &e,
        vec![score(5, None, "q", "s", 1.0), score(6, None, "q", "s", 1.0)],
    );
    assert_eq!(
        (r.accepted, r.rejected),
        (1, vec![(1, "unknown trace".to_string())])
    );
    assert_eq!(shown(&e, 5).len(), 1);
}

#[test]
fn a_request_of_only_rejected_items_logs_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    let stats = e.stats();
    let r = annotate(&e, vec![score(77, None, "q", "s", 1.0)]);
    assert_eq!((r.accepted, r.rejected.len()), (0, 1));
    let mut bad = score(1, None, "q", "s", 1.0);
    bad.name.clear();
    let r = annotate(&e, vec![bad, score(77, None, "q", "s", 1.0)]);
    assert_eq!((r.accepted, r.rejected.len()), (0, 2));
    let after = e.stats();
    assert_eq!(after.acked_lsn, stats.acked_lsn);
    assert_eq!(after.records_ingested, stats.records_ingested);
    assert_eq!(after.annotations_buffered, 0);
    drop(e);
    let (wal, _) = Wal::open(dir.path().join("wal"), wal_no_sync()).unwrap();
    assert_eq!(wal.replay(1, |_, _| Ok(())).unwrap(), 4);
    wal.shutdown().unwrap();
}

#[test]
fn invalid_items_are_rejected_one_by_one_with_the_message() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    let mut empty_name = score(1, None, "q", "s", 1.0);
    empty_name.name.clear();
    let mut nan = score(1, None, "n", "s", 1.0);
    nan.value = Some(f64::NAN);
    let zero_span = score(1, Some(0), "z", "s", 1.0);
    let zero_trace = score(0, None, "q", "s", 1.0);
    let mut delete_with_value = delete(1, None, "d", "s");
    delete_with_value.value = Some(1.0);
    let mut nothing = score(1, None, "e", "s", 1.0);
    nothing.value = None;
    let bad = [
        empty_name,
        nan,
        zero_span,
        zero_trace,
        delete_with_value,
        nothing,
    ];
    let mut items = vec![score(2, None, "good", "s", 1.0)];
    items.extend(bad.iter().cloned());
    let r = annotate(&e, items);
    assert_eq!(r.accepted, 1);
    assert_eq!(r.rejected.len(), bad.len());
    for (k, (i, why)) in r.rejected.iter().enumerate() {
        assert_eq!(*i, k + 1);
        assert_eq!(*why, bad[k].validate().unwrap_err());
        assert!(!why.is_empty());
    }
    assert_eq!(shown(&e, 2).len(), 1);
    assert!(shown(&e, 1).is_empty());
}

#[test]
fn zero_and_too_many_items_are_invalid_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    let acked = e.stats().acked_lsn;
    let many = |n: usize| -> Vec<Annotation> {
        (0..n)
            .map(|i| score(1, None, &format!("n{i}"), "s", 1.0))
            .collect()
    };
    for items in [vec![], many(MAX_BATCH_ITEMS + 1)] {
        assert!(matches!(
            e.annotate(items, |_| panic!("called")),
            Err(Error::InvalidArgument(_))
        ));
    }
    assert_eq!(e.stats().acked_lsn, acked);
    let r = annotate(&e, many(MAX_BATCH_ITEMS));
    assert_eq!(r.accepted, MAX_BATCH_ITEMS);
    assert_eq!(shown(&e, 1).len(), MAX_BATCH_ITEMS);
}

#[test]
fn a_full_buffer_refuses_annotations_by_the_bytes_they_count_for() {
    let dir = tempfile::tempdir().unwrap();
    let (spans, each) = {
        let e = base(dir.path());
        let spans = e.stats().buffered_bytes;
        annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
        (spans, e.stats().buffered_bytes - spans)
    };
    assert!(each > 0);
    // Replay counts the same bytes; room for exactly one more of that size.
    let mut o = opts(dir.path());
    o.max_buffered_bytes = spans + 2 * each;
    let e = open(o);
    assert_eq!(e.stats().buffered_bytes, spans + each);
    annotate(&e, vec![score(1, None, "r", "s", 1.0)]);
    assert_eq!(e.stats().buffered_bytes, spans + 2 * each);
    let at_limit = e.stats();
    let called = Arc::new(AtomicBool::new(false));
    let c2 = called.clone();
    let r = e.annotate(vec![score(1, None, "t", "s", 1.0)], move |_| {
        c2.store(true, Ordering::SeqCst)
    });
    assert!(matches!(r, Err(Error::Unavailable(_))), "{r:?}");
    std::thread::sleep(Duration::from_millis(50));
    assert!(!called.load(Ordering::SeqCst));
    let after = e.stats();
    assert_eq!(after.acked_lsn, at_limit.acked_lsn);
    assert_eq!(after.records_ingested, at_limit.records_ingested);
    assert_eq!(after.annotations_buffered, 2);
    // Spans are refused by the same check.
    assert!(matches!(ingest(&e, &one(9)), Err(Error::Unavailable(_))));
}

#[test]
fn a_closing_engine_refuses_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &one(1)).unwrap();
    e.shutdown().unwrap();
    let r = e.annotate(vec![score(1, None, "q", "s", 1.0)], |_| panic!("called"));
    assert!(matches!(r, Err(Error::Unavailable(_))), "{r:?}");
}

#[test]
fn a_fatal_engine_refuses_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let f2 = fail.clone();
    let mut o = opts(dir.path());
    o.wal.sync_hook = Some(Arc::new(move || {
        if f2.load(Ordering::SeqCst) {
            Err(std::io::Error::other("injected sync failure"))
        } else {
            Ok(())
        }
    }));
    let e = open(o);
    ingest(&e, &one(1)).unwrap();
    fail.store(true, Ordering::SeqCst);
    let (tx, rx) = mpsc::channel();
    e.annotate(vec![score(1, None, "q", "s", 1.0)], move |r| {
        tx.send(r).unwrap()
    })
    .unwrap();
    let r = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(matches!(r, Err(Error::Fatal(_))), "{r:?}");
    let r = e.annotate(vec![score(1, None, "q2", "s", 1.0)], |_| panic!("called"));
    assert!(matches!(r, Err(Error::Fatal(_))), "{r:?}");
    assert!(e.stats().fatal);
}

#[test]
fn a_door_decoder_under_the_annotation_tag_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let r = Engine::open(
        opts(dir.path()),
        vec![
            (TAG, Arc::new(TestDecoder)),
            (ANNOTATION_RECORD_TAG, Arc::new(TestDecoder)),
        ],
    );
    assert!(matches!(r.err(), Some(Error::InvalidArgument(_))));
}

#[test]
fn restart_without_a_flush_restores_the_same_winners() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    annotate(
        &e,
        vec![
            score(1, None, "q", "s", 1.0),
            with_label(score(1, Some(2), "q", "s", 2.0), "ok"),
        ],
    );
    annotate(
        &e,
        vec![score(1, None, "q", "s", 3.0), score(2, None, "q", "s", 4.0)],
    );
    annotate(
        &e,
        vec![delete(2, None, "q", "s"), score(3, None, "x", "", 1.5)],
    );
    annotate(&e, vec![score(2, None, "q", "s", 5.0)]);
    let before: Vec<Vec<Annotation>> = (1..=4).map(|t| shown(&e, t)).collect();
    assert!(before.iter().map(Vec::len).sum::<usize>() >= 4);
    let stats = e.stats();
    drop(e);

    let e = open(opts(dir.path()));
    let after: Vec<Vec<Annotation>> = (1..=4).map(|t| shown(&e, t)).collect();
    assert_eq!(after, before);
    assert_eq!(e.stats().acked_lsn, stats.acked_lsn);
    assert_eq!(e.stats().annotations_buffered, stats.annotations_buffered);
    // And the numbering goes on.
    annotate(&e, vec![score(1, None, "q", "s", 6.0)]);
    assert_eq!(shown(&e, 1)[0].arrival, stats.acked_lsn + 1);
    assert_eq!(shown(&e, 1)[0].value, Some(6.0));
}

#[test]
fn restart_replays_annotations_after_a_flush_of_the_spans() {
    let dir = tempfile::tempdir().unwrap();
    let e = base(dir.path());
    e.flush().unwrap();
    annotate(&e, vec![score(2, None, "q", "s", 1.0)]);
    let before = shown(&e, 2);
    drop(e);
    let e = open(opts(dir.path()));
    assert_eq!(shown(&e, 2), before);
    assert_eq!(e.stats().parts, 1);
}

#[test]
fn an_undecodable_annotation_record_refuses_the_start_and_changes_nothing() {
    let append_bad = |dir: &Path, record: &[u8]| {
        let (wal, _) = Wal::open(dir.join("wal"), wal_no_sync()).unwrap();
        wal.append_blocking(record).unwrap();
        wal.shutdown().unwrap();
    };
    let refused = |dir: &Path| {
        let before = tree(dir);
        let err = Engine::open(opts(dir), decoders()).err().expect("accepted");
        assert!(matches!(err, Error::Corrupt(_)), "{err}");
        assert_eq!(tree(dir), before, "the refused start changed the directory");
    };

    // A new database: nothing but a log, so a refusal after the first
    // manifest was written would show.
    let dir = tempfile::tempdir().unwrap();
    append_bad(dir.path(), &[ANNOTATION_RECORD_TAG, 1, 2, 3]);
    refused(dir.path());
    assert!(!dir.path().join("manifest").exists());

    // An existing database, with a good annotation record before the bad one.
    for bad in [
        vec![ANNOTATION_RECORD_TAG],
        vec![ANNOTATION_RECORD_TAG, 0, 0],
        vec![ANNOTATION_RECORD_TAG, 9, 9, 9, 9, 9, 9],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let e = base(dir.path());
        e.flush().unwrap();
        annotate(&e, vec![score(1, None, "q", "s", 1.0)]);
        drop(e);
        append_bad(dir.path(), &bad);
        refused(dir.path());
    }
}

fn scored_engine(dir: &Path) -> Engine {
    let e = base(dir);
    annotate(
        &e,
        vec![
            score(1, None, "score", "judge", 0.9),
            score(2, None, "score", "human", 0.3),
            with_label(score(2, None, "tag", "human", 0.0), "bad"),
            with_label(score(3, Some(2), "score", "judge", 0.5), "ok"),
            score(4, None, "score", "judge", 0.7),
        ],
    );
    annotate(&e, vec![delete(4, None, "score", "judge")]);
    e
}

#[test]
fn the_list_filter_over_buffered_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let e = scored_engine(dir.path());
    let mut m = Model::default();
    for t in 1..=4u128 {
        m.add(&[
            TSpan::new(t, 1, 100 + t as i64).root(),
            TSpan::new(t, 2, 105 + t as i64),
        ]);
    }
    assert_eq!(
        e.list_traces(&list_all()).unwrap().len(),
        4,
        "the unfiltered listing is unchanged"
    );
    m.check(&e, "with annotations buffered");

    // Newest first: 4, 3, 2, 1.
    let f = filter;
    let none = Vec::<u128>::new();
    assert_eq!(
        listed(&e, f("score")),
        vec![3, 2, 1],
        "a deleted one does not match"
    );
    assert_eq!(listed(&e, f("nothing")), none);
    let judge = AnnotationFilter {
        source: Some("judge".into()),
        ..f("score")
    };
    assert_eq!(listed(&e, judge), vec![3, 1]);
    let nobody = AnnotationFilter {
        source: Some("nobody".into()),
        ..f("score")
    };
    assert_eq!(listed(&e, nobody), none);
    let min = |v: f64| AnnotationFilter {
        min: Some(v),
        ..f("score")
    };
    let max = |v: f64| AnnotationFilter {
        max: Some(v),
        ..f("score")
    };
    assert_eq!(listed(&e, min(0.5)), vec![3, 1], "min is inclusive");
    assert_eq!(listed(&e, min(0.6)), vec![1]);
    assert_eq!(listed(&e, max(0.5)), vec![3, 2], "max is inclusive");
    let both = AnnotationFilter {
        min: Some(0.3),
        max: Some(0.5),
        ..f("score")
    };
    assert_eq!(listed(&e, both), vec![3, 2]);
    let gap = AnnotationFilter {
        min: Some(0.4),
        max: Some(0.45),
        ..f("score")
    };
    assert_eq!(listed(&e, gap), none);
    let ok = AnnotationFilter {
        label: Some("ok".into()),
        ..f("score")
    };
    assert_eq!(
        listed(&e, ok),
        vec![3],
        "a span-level annotation matches its trace"
    );
    let bad = AnnotationFilter {
        label: Some("bad".into()),
        ..f("tag")
    };
    assert_eq!(listed(&e, bad), vec![2]);
    let zero = AnnotationFilter {
        min: Some(-1.0),
        ..f("tag")
    };
    assert_eq!(listed(&e, zero), vec![2], "0.0 is a value");
    let other_label = AnnotationFilter {
        max: Some(1.0),
        label: Some("nolabel".into()),
        ..f("tag")
    };
    assert_eq!(listed(&e, other_label), none);
    // The stop rule: the limit keeps the newest matching traces.
    assert_eq!(listed_limit(&e, f("score"), 1), vec![3]);
    assert_eq!(listed_limit(&e, f("score"), 2), vec![3, 2]);
    // Annotating a deleted identity again makes it match.
    annotate(&e, vec![score(4, None, "score", "judge", 0.1)]);
    assert_eq!(listed(&e, f("score")), vec![4, 3, 2, 1]);
    assert_eq!(listed(&e, max(0.2)), vec![4]);
    // A later winner without a value: the older value does not count.
    let mut label_only = score(1, None, "score", "judge", 0.0);
    label_only.value = None;
    label_only.label = Some("l".into());
    annotate(&e, vec![label_only]);
    assert_eq!(listed(&e, min(0.0)), vec![4, 3, 2]);
}

#[test]
fn known_traces_matches_a_brute_force_check() {
    let mut rng = StdRng::seed_from_u64(0xA110);
    let mut split_seen = false;
    for round in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let e = open(opts(dir.path()));
        let mut have: BTreeSet<u128> = BTreeSet::new();
        let mut span = 0u64;
        for _ in 0..40 {
            let n = rng.random_range(1..4);
            let spans: Vec<TSpan> = (0..n)
                .map(|_| {
                    span += 1;
                    let t = 10 * rng.random_range(1..60u128);
                    have.insert(t);
                    TSpan::new(t, span, rng.random_range(0..8_000))
                })
                .collect();
            ingest(&e, &spans).unwrap();
            if rng.random_bool(0.2) {
                e.flush().unwrap();
            }
        }
        // Parts and a buffer; some traces in both.
        if round % 2 == 0 {
            e.flush().unwrap();
            let t = 10 * rng.random_range(1..60u128);
            have.insert(t);
            span += 1;
            ingest(&e, &[TSpan::new(t, span, 10)]).unwrap();
        }
        let snap = e.snapshot();
        split_seen |= !snap.split_traces().is_empty();
        // Before, between and after the stored ids, with repeats, unordered.
        let mut ids: Vec<u128> = (0..=620).collect();
        ids.extend([u128::MAX, 5, 5, 10, 10, 600, 600]);
        for i in (1..ids.len()).rev() {
            ids.swap(i, rng.random_range(0..=i));
        }
        let got = snap
            .known_traces(&ids.iter().map(|&t| tid(t)).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(got.len(), ids.len());
        for (t, k) in ids.iter().zip(&got) {
            assert_eq!(*k, have.contains(t), "round {round}, trace {t}");
        }
        assert!(snap.known_traces(&[]).unwrap().is_empty());
    }
    assert!(split_seen, "no round had a trace in two parts");
}

type Key = (u128, String, String);

#[test]
fn threads_annotating_ingesting_and_querying_agree_with_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for t in 1..=8 {
        ingest(&e, &one(t)).unwrap();
    }
    let mut annotators = Vec::new();
    for k in 0..4u64 {
        let e = e.clone();
        annotators.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(k);
            let mut last: BTreeMap<Key, Option<f64>> = BTreeMap::new();
            for n in 0..120 {
                let t = rng.random_range(1..=8u128);
                let name = ["a", "b", "c"][rng.random_range(0..3)];
                // Each thread has its own sources, so its last write wins.
                let source = format!("s{k}");
                let key = (t, name.to_string(), source.clone());
                if rng.random_bool(0.25) {
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
                ingest(&e, &one(t)).unwrap();
                annotate(&e, vec![score(t, None, "a", "ing", 1.0)]);
                last.insert((t, "a".into(), "ing".into()), Some(1.0));
            }
            last
        })
    };
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut reads = 0;
            while !stop.load(Ordering::SeqCst) {
                let snap = e.snapshot();
                for t in 1..=8 {
                    let (v, _) = snap.annotations(tid(t), usize::MAX).unwrap();
                    assert!(v.windows(2).all(|w| w[0].identity_cmp(&w[1]).is_lt()));
                    assert!(v.iter().all(|a| !a.delete));
                }
                snap.list_traces(&ListQuery {
                    annotation: Some(filter("a")),
                    ..list_all()
                })
                .unwrap();
                reads += 1;
            }
            reads
        })
    };
    let mut model: BTreeMap<Key, Option<f64>> = BTreeMap::new();
    for h in annotators {
        model.extend(h.join().unwrap());
    }
    model.extend(ingester.join().unwrap());
    stop.store(true, Ordering::SeqCst);
    assert!(reader.join().unwrap() > 0);

    let traces: HashSet<u128> = model.keys().map(|k| k.0).collect();
    for t in traces {
        let want: Vec<(Option<u64>, String, String, Option<f64>)> = model
            .iter()
            .filter(|(k, v)| k.0 == t && v.is_some())
            .map(|(k, v)| (None, k.1.clone(), k.2.clone(), *v))
            .collect();
        assert_eq!(brief(&shown(&e, t)), want, "trace {t}");
    }
}
