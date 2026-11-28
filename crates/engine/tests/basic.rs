//! Behaviour tests of the engine, one scenario each.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::{Error, TraceId};
use nwdb_engine::{AggQuery, Engine, ListQuery};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use nwdb_wal::{SyncHook, Wal, WalOptions};

#[test]
fn handles_can_cross_threads() {
    fn check<T: Send + Sync + 'static>() {}
    check::<Engine>();
    check::<nwdb_engine::Snapshot>();
}

fn manifest_state(dir: &std::path::Path) -> Arc<nwdb_manifest::ManifestState> {
    Manifest::open(
        dir.join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current()
}

fn wait_until(what: &str, cond: impl FnMut() -> bool) {
    wait_with_timeout(what, Duration::from_secs(30), cond);
}

fn sample(trace: u128, base: i64) -> Vec<TSpan> {
    let mut root = TSpan::new(trace, 1, base).root();
    root.operation = Some("chat".into());
    root.model = Some("m1".into());
    root.input = Some(10);
    root.output = Some(5);
    let mut child = TSpan::new(trace, 2, base + 5);
    child.model = Some("m2".into());
    child.input = Some(7);
    vec![root, child]
}

#[test]
fn not_visible_before_durable_and_visible_after() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
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

    let spans = sample(1, 100);
    let (tx, rx) = mpsc::channel();
    let e2 = e.clone();
    e.ingest(TAG, encode(&spans), to_batches(&spans), move |r| {
        // Read your own write: the data is visible when the callback runs.
        let seen = e2.get_trace(TraceId(1u128.to_be_bytes())).unwrap();
        tx.send((r, seen.is_some())).unwrap();
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert!(rx.try_recv().is_err(), "acknowledged before the sync");
    assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_none());
    assert!(e.list_traces(&list_all()).unwrap().is_empty());

    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let (r, seen) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    r.unwrap();
    assert!(seen);
    let mut m = Model::default();
    m.add(&spans);
    m.check(&e, "after ack");
    e.shutdown().unwrap();
}

#[test]
fn queries_filters_and_aggregates_match_the_model() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    let mut all = Vec::new();
    for t in 0..6u128 {
        let mut s = sample(t + 1, 100 + 10 * t as i64);
        if t % 2 == 1 {
            s[1].error_type = Some("boom".into());
            s[0].service = Some("other".into());
        }
        if t == 4 {
            s[1].model = None;
            s[1].operation = Some("embed".into());
        }
        all.extend(s);
    }
    for chunk in all.chunks(3) {
        ingest(&e, chunk).unwrap();
        m.add(chunk);
    }
    for flushed in [false, true] {
        if flushed {
            e.flush().unwrap();
        }
        m.check(&e, "all");
        for q in [
            ListQuery {
                service: Some("other".into()),
                ..list_all()
            },
            ListQuery {
                model: Some("m2".into()),
                ..list_all()
            },
            ListQuery {
                error: Some(true),
                ..list_all()
            },
            ListQuery {
                error: Some(false),
                limit: 2,
                ..list_all()
            },
            ListQuery {
                from_ns: 110,
                to_ns: 140,
                ..list_all()
            },
        ] {
            assert_eq!(e.list_traces(&q).unwrap(), m.list(&q), "{q:?}");
        }
        for q in [
            AggQuery {
                from_ns: 0,
                to_ns: 1000,
                operation: Some("chat".into()),
            },
            AggQuery {
                from_ns: 110,
                to_ns: 125,
                operation: None,
            },
            AggQuery {
                from_ns: 0,
                to_ns: 1000,
                operation: Some("embed".into()),
            },
        ] {
            assert_eq!(e.aggregate_tokens(&q).unwrap(), m.agg(&q), "{q:?}");
        }
    }
    e.shutdown().unwrap();
}

#[test]
fn explicit_flush_empties_buffer_advances_manifest_and_trims_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // One log file per record, so trimming is visible.
    o.wal.max_file_bytes = 1;
    let e = open(o);
    let mut m = Model::default();
    for t in 0..10u128 {
        let s = sample(t + 1, 100 + t as i64);
        ingest(&e, &s).unwrap();
        m.add(&s);
    }
    let wal_dir = dir.path().join("wal");
    assert!(count_files(&wal_dir, "wal-") >= 10);
    assert!(e.stats().buffered_bytes > 0);
    m.check(&e, "before flush");

    e.flush().unwrap();
    let st = e.stats();
    assert_eq!(st.buffered_bytes, 0);
    assert!(st.parts >= 1);
    assert_eq!(st.flushes, 1);
    assert_eq!(st.acked_lsn, 10);
    assert_eq!(st.flushed_lsn, 10);
    assert_eq!(manifest_state(dir.path()).wal_flushed_lsn, 10);
    assert!(count_files(&wal_dir, "wal-") <= 2, "log was not trimmed");
    m.check(&e, "after flush");

    // A flush with nothing to write changes nothing.
    e.flush().unwrap();
    assert_eq!(e.stats().flushes, 1);
    e.shutdown().unwrap();
}

#[test]
fn flush_by_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.buffer_bytes = 200_000;
    o.max_buffered_bytes = 100_000_000;
    let e = open(o);
    let mut m = Model::default();
    let mut t = 0u128;
    while e.stats().flushes == 0 {
        t += 1;
        let s = sample(t, 100 + t as i64);
        ingest(&e, &s).unwrap();
        m.add(&s);
        assert!(t < 100_000, "never flushed by size");
    }
    wait_until("flush to finish", || !e.stats().flushing);
    assert!(e.stats().parts >= 1);
    m.check(&e, "after size flush");
    e.shutdown().unwrap();
}

#[test]
fn flush_by_age() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.flush_interval = Duration::from_millis(100);
    let e = open(o);
    let s = sample(1, 100);
    ingest(&e, &s).unwrap();
    wait_until("age flush", || e.stats().flushes >= 1);
    let st = e.stats();
    assert_eq!(st.buffered_bytes, 0);
    assert_eq!(st.flushed_lsn, st.acked_lsn);
    let mut m = Model::default();
    m.add(&s);
    m.check(&e, "after age flush");
    e.shutdown().unwrap();
}

#[test]
fn restart_after_shutdown_and_after_drop() {
    let dir = tempfile::tempdir().unwrap();
    let mut m = Model::default();
    let e = open(opts(dir.path()));
    for t in 0..6u128 {
        let s = sample(t + 1, 100 + 500 * t as i64);
        ingest(&e, &s).unwrap();
        m.add(&s);
        if t == 2 {
            e.flush().unwrap();
        }
    }
    e.shutdown().unwrap();
    drop(e);

    let e = open(opts(dir.path()));
    m.check(&e, "after clean shutdown");
    assert_eq!(e.stats().acked_lsn, 6);
    for t in 6..10u128 {
        let s = sample(t + 1, 100 + 500 * t as i64);
        ingest(&e, &s).unwrap();
        m.add(&s);
        if t == 7 {
            e.flush().unwrap();
        }
    }
    // No shutdown: the unflushed records exist only in the log.
    drop(e);

    let e = open(opts(dir.path()));
    m.check(&e, "after drop without shutdown");
    assert_eq!(e.stats().acked_lsn, 10);
    // Replayed records are in the buffer, and a flush moves them to parts
    // without duplicating anything.
    e.flush().unwrap();
    m.check(&e, "after flushing replayed records");
    e.shutdown().unwrap();
}

fn late_span_engine(dir: &std::path::Path) -> (Engine, Model) {
    let e = open(opts(dir));
    let mut m = Model::default();
    let t = 7u128;
    // Part 1: the root and a child, in window 0.
    let mut a = vec![TSpan::new(t, 1, 100).root(), TSpan::new(t, 2, 110)];
    a[1].model = Some("m1".into());
    a[1].input = Some(3);
    // Part 2: a late span, same window.
    let mut b = vec![TSpan::new(t, 3, 150)];
    b[0].model = Some("m2".into());
    b[0].input = Some(4);
    // Part 3: a late span that starts before the root, so the trace's start
    // moves to an earlier window.
    let mut c = vec![TSpan::new(t, 4, 50)];
    c[0].error_type = Some("late-error".into());
    for (i, batch) in [a, b, c].into_iter().enumerate() {
        ingest(&e, &batch).unwrap();
        m.add(&batch);
        if i < 3 {
            e.flush().unwrap();
        }
    }
    // The buffer: another late span, starting even earlier.
    let mut d = vec![TSpan::new(t, 5, 20)];
    d[0].model = Some("m3".into());
    ingest(&e, &d).unwrap();
    m.add(&d);
    // And a second trace that is not split, to check nothing else leaks in.
    let other = vec![TSpan::new(8, 1, 60).root()];
    ingest(&e, &other).unwrap();
    m.add(&other);
    (e, m)
}

#[test]
fn late_spans_across_parts_and_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let (e, m) = late_span_engine(dir.path());
    assert_eq!(e.stats().parts, 3);
    m.check(&e, "late spans");

    let t = TraceId(7u128.to_be_bytes());
    let spans = from_batches(&e.get_trace(t).unwrap().unwrap());
    let ids: Vec<u64> = spans.iter().map(|s| s.span).collect();
    assert_eq!(ids, [5, 4, 1, 2, 3], "every span once, by start time");

    let q = ListQuery {
        from_ns: 0,
        to_ns: 10_000,
        ..list_all()
    };
    let got = e.list_traces(&q).unwrap();
    let merged = got.iter().find(|s| s.trace_id == t).unwrap();
    assert_eq!(got.iter().filter(|s| s.trace_id == t).count(), 1);
    assert_eq!(merged.start_ns, 20);
    assert_eq!(merged.end_ns, 160);
    assert_eq!(merged.span_count, 5);
    assert!(merged.error);
    assert_eq!(merged.models, ["m1", "m2", "m3"]);
    assert_eq!(merged.input_tokens, 7);
    assert_eq!(merged.root_name.as_deref(), Some("span-1"));

    // Ranges against the model, including the two cases that matter: the
    // trace's start is in the range but only the buffer's partial summary is;
    // and a part's partial summary is in the range but the merged start is
    // not.
    for (from, to) in [
        (0, 30),    // only the buffer's partial start (20) is inside
        (40, 60),   // only part 3's partial (50) is inside
        (100, 200), // part 1 and 2 partials inside, merged start 20 is not
        (20, 21),
        (21, 50),
        (0, 20),
        (150, 151),
    ] {
        let q = ListQuery {
            from_ns: from,
            to_ns: to,
            ..list_all()
        };
        let want = m.list(&q);
        assert_eq!(e.list_traces(&q).unwrap(), want, "range {from}..{to}");
    }
    let q = ListQuery {
        from_ns: 100,
        to_ns: 200,
        ..list_all()
    };
    assert!(e.list_traces(&q).unwrap().iter().all(|s| s.trace_id != t));
    let q = ListQuery {
        from_ns: 0,
        to_ns: 30,
        ..list_all()
    };
    let got = e.list_traces(&q).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].span_count, 5, "the merge reaches the parts");

    // Same answers after the buffer is flushed and after a restart.
    e.flush().unwrap();
    m.check(&e, "late spans after flush");
    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    m.check(&e, "late spans after restart");
    e.shutdown().unwrap();
}

#[test]
fn several_windows_in_one_flush_make_one_part_each_in_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    // Window width is 1000: four windows, two traces in the first.
    for (t, start) in [(1u128, 100), (2, 200), (3, 1500), (4, 2500), (5, 3999)] {
        let s = vec![TSpan::new(t, 1, start).root(), TSpan::new(t, 2, start + 1)];
        ingest(&e, &s).unwrap();
        m.add(&s);
    }
    let before = manifest_state(dir.path());
    e.flush().unwrap();
    let after = manifest_state(dir.path());
    assert_eq!(after.version, before.version + 1, "one commit");
    assert_eq!(after.parts.len(), 4);
    let mut windows: Vec<i64> = after.parts.iter().map(|p| p.window_start_ns).collect();
    windows.sort();
    assert_eq!(windows, [0, 1000, 2000, 3000]);
    assert_eq!(after.parts.iter().map(|p| p.traces).sum::<u64>(), 5);
    assert_eq!(after.wal_flushed_lsn, 5);
    m.check(&e, "after multi-window flush");
    e.shutdown().unwrap();
}

#[test]
fn duplicate_spans_appear_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let t = TraceId(1u128.to_be_bytes());
    let s = vec![TSpan::new(1, 1, 100).root(), TSpan::new(1, 2, 110)];
    let ids = |e: &Engine| {
        from_batches(&e.get_trace(t).unwrap().unwrap())
            .iter()
            .map(|s| s.span)
            .collect::<Vec<_>>()
    };

    // Twice in one record, twice in two records: both copies in the buffer.
    let mut twice = s.clone();
    twice.push(s[1].clone());
    ingest(&e, &twice).unwrap();
    ingest(&e, &s[..1]).unwrap();
    assert_eq!(ids(&e), [1, 2]);

    // Both copies in one flush.
    e.flush().unwrap();
    assert_eq!(ids(&e), [1, 2]);
    assert_eq!(e.list_traces(&list_all()).unwrap()[0].span_count, 2);

    // Copies in different parts, and in a part and the buffer.
    ingest(&e, &s).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 2);
    assert_eq!(ids(&e), [1, 2]);
    ingest(&e, &s[1..]).unwrap();
    assert_eq!(ids(&e), [1, 2]);

    e.shutdown().unwrap();
    let e = open(opts(dir.path()));
    assert_eq!(ids(&e), [1, 2]);
    e.shutdown().unwrap();
}

#[test]
fn back_pressure_returns_unavailable_without_a_callback() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // Nothing flushes by itself: the size trigger is far away.
    o.max_buffered_bytes = 60_000;
    let e = open(o);
    let mut m = Model::default();
    let mut t = 0u128;
    let refused = loop {
        t += 1;
        let s = sample(t, 100);
        let called = Arc::new(AtomicBool::new(false));
        let c2 = called.clone();
        match e.ingest(TAG, encode(&s), to_batches(&s), move |_| {
            c2.store(true, Ordering::SeqCst);
        }) {
            Ok(()) => {
                // Wait for it so the buffer's size is settled.
                wait_until("ack", || called.load(Ordering::SeqCst));
                m.add(&s);
            }
            Err(Error::Unavailable(_)) => break called,
            Err(other) => panic!("unexpected {other:?}"),
        }
        assert!(t < 10_000, "never pushed back");
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !refused.load(Ordering::SeqCst),
        "callback called on refusal"
    );
    // The same request is refused again until a flush makes room.
    let s = sample(t, 100);
    assert!(matches!(
        e.ingest(TAG, encode(&s), to_batches(&s), |_| {}),
        Err(Error::Unavailable(_))
    ));
    m.check(&e, "refusal changed nothing");

    e.flush().unwrap();
    ingest(&e, &s).unwrap();
    m.add(&s);
    m.check(&e, "after making room");
    e.shutdown().unwrap();
}

#[test]
fn bad_arguments_are_refused_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let s = sample(1, 100);
    let called = Arc::new(AtomicBool::new(false));
    let c = called.clone();
    let r = e.ingest(9, encode(&s), to_batches(&s), move |_| {
        c.store(true, Ordering::SeqCst)
    });
    assert!(matches!(r, Err(Error::InvalidArgument(_))));
    let wrong = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let r = e.ingest(TAG, encode(&s), vec![wrong], |_| {});
    assert!(matches!(r, Err(Error::InvalidArgument(_))));
    std::thread::sleep(Duration::from_millis(50));
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(e.stats().records_ingested, 0);
    e.shutdown().unwrap();
    assert!(matches!(
        e.ingest(TAG, encode(&s), to_batches(&s), |_| {}),
        Err(Error::Unavailable(_))
    ));
}

#[test]
fn the_directory_lock_excludes_a_second_engine() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let second = Engine::open(opts(dir.path()), decoders());
    assert!(
        matches!(second, Err(Error::Conflict(_))),
        "{:?}",
        second.err()
    );
    e.shutdown().unwrap();
    // Other clones of the handle may still exist; the lock is released.
    let e2 = open(opts(dir.path()));
    e2.shutdown().unwrap();

    let mut o = opts(&dir.path().join("nope"));
    o.create_if_missing = false;
    assert!(matches!(
        Engine::open(o, decoders()),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn part_directories_without_a_manifest_refuse_to_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("parts/00000000000000000001")).unwrap();
    let r = Engine::open(opts(dir.path()), decoders());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{:?}", r.err());
    // Nothing was deleted or created in its place.
    assert!(dir.path().join("parts/00000000000000000001").is_dir());
    assert!(!dir.path().join("manifest").exists());

    // Also when the manifest directory exists but is empty.
    std::fs::create_dir_all(dir.path().join("manifest")).unwrap();
    let r = Engine::open(opts(dir.path()), decoders());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{:?}", r.err());
    assert!(dir.path().join("parts/00000000000000000001").is_dir());
}

#[test]
fn a_new_database_has_a_manifest_before_any_part_exists() {
    // So that part directories next to no manifest can only mean a lost one,
    // even after a crash during the very first flush.
    let dir = tempfile::tempdir().unwrap();
    drop(open(opts(dir.path())));
    assert_eq!(manifest_state(dir.path()).version, 1);
    // A crash during the first flush leaves a finished part nobody committed.
    std::fs::create_dir_all(dir.path().join("parts/00000000000000000001")).unwrap();
    let e = open(opts(dir.path()));
    assert!(!dir.path().join("parts/00000000000000000001").exists());
    e.shutdown().unwrap();
}

#[test]
fn leftover_part_directories_are_removed_and_ids_are_not_reused() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    let s = sample(1, 100);
    ingest(&e, &s).unwrap();
    m.add(&s);
    e.flush().unwrap();
    e.shutdown().unwrap();
    drop(e);
    assert_eq!(manifest_state(dir.path()).next_part_id, 2);

    let parts = dir.path().join("parts");
    let final_leftover = parts.join("00000000000000000007");
    let tmp_leftover = parts.join("00000000000000000009.tmp");
    std::fs::create_dir_all(&final_leftover).unwrap();
    std::fs::write(final_leftover.join("spans.parquet"), b"junk").unwrap();
    std::fs::create_dir_all(&tmp_leftover).unwrap();

    let e = open(opts(dir.path()));
    assert!(!final_leftover.exists() && !tmp_leftover.exists());
    assert!(parts.join("00000000000000000001").is_dir());
    assert!(manifest_state(dir.path()).next_part_id >= 10);
    m.check(&e, "after cleaning leftovers");
    let s2 = sample(2, 100);
    ingest(&e, &s2).unwrap();
    m.add(&s2);
    e.flush().unwrap();
    let st = manifest_state(dir.path());
    assert!(st.parts.iter().all(|p| p.part_id == 1 || p.part_id >= 10));
    m.check(&e, "after flushing with fresh ids");
    e.shutdown().unwrap();
}

#[test]
fn a_deleted_log_is_renumbered_above_the_manifest_position_when_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut m = Model::default();
    for t in 0..100u128 {
        let s = sample(t + 1, 100);
        ingest(&e, &s).unwrap();
        m.add(&s);
    }
    e.flush().unwrap();
    e.shutdown().unwrap();
    drop(e);
    assert_eq!(manifest_state(dir.path()).wal_flushed_lsn, 100);
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();

    let mut o = opts(dir.path());
    o.accept_lost_log = true;
    let e = open(o.clone());
    m.check(&e, "after losing the log");
    for t in 100..103u128 {
        let s = sample(t + 1, 100);
        ingest(&e, &s).unwrap();
        m.add(&s);
    }
    assert_eq!(e.stats().acked_lsn, 103);
    drop(e);

    let e = open(opts(dir.path()));
    m.check(&e, "new records survived a restart");
    assert_eq!(e.stats().acked_lsn, 103);
    e.shutdown().unwrap();
}

#[test]
fn a_log_that_starts_too_late_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    for t in 0..5u128 {
        ingest(&e, &sample(t + 1, 100)).unwrap();
    }
    drop(e);
    // Replace the log by one whose first record is number 3, while the
    // manifest says nothing has been flushed: records 1 and 2 are missing.
    let wal_dir = dir.path().join("wal");
    std::fs::remove_dir_all(&wal_dir).unwrap();
    let (wal, _) = Wal::open(
        &wal_dir,
        WalOptions {
            min_next_lsn: Some(3),
            allow_lost_log: true,
            ..wal_no_sync()
        },
    )
    .unwrap();
    let mut rec = vec![TAG];
    rec.extend(encode(&sample(99, 100)));
    assert_eq!(wal.append_blocking(&rec).unwrap(), 3);
    wal.shutdown().unwrap();

    let r = Engine::open(opts(dir.path()), decoders());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{:?}", r.err());
    // The refusal released the lock and changed nothing.
    assert_eq!(manifest_state(dir.path()).wal_flushed_lsn, 0);
}

#[test]
fn unknown_tag_or_undecodable_record_in_the_log_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest(&e, &sample(1, 100)).unwrap();
    drop(e);
    let (wal, _) = Wal::open(dir.path().join("wal"), wal_no_sync()).unwrap();
    wal.append_blocking(&[77, 1, 2, 3]).unwrap();
    wal.shutdown().unwrap();
    let r = Engine::open(opts(dir.path()), decoders());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{:?}", r.err());
}

#[test]
fn a_failed_sync_puts_the_engine_in_the_fatal_state() {
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
    let good = sample(1, 100);
    ingest(&e, &good).unwrap();
    e.flush().unwrap();
    let good2 = sample(2, 100);
    ingest(&e, &good2).unwrap();

    fail.store(true, Ordering::SeqCst);
    let (tx, rx) = mpsc::channel();
    for t in 10..14u128 {
        let s = sample(t, 100);
        let tx = tx.clone();
        e.ingest(TAG, encode(&s), to_batches(&s), move |r| {
            tx.send(r).unwrap();
        })
        .unwrap();
    }
    for _ in 0..4 {
        let r = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(matches!(r, Err(Error::Fatal(_))), "{r:?}");
    }
    // Later requests and flushes are refused; reads keep working.
    let s = sample(20, 100);
    assert!(matches!(
        e.ingest(TAG, encode(&s), to_batches(&s), |_| {}),
        Err(Error::Fatal(_))
    ));
    assert!(matches!(e.flush(), Err(Error::Fatal(_))));
    let mut m = Model::default();
    m.add(&good);
    m.add(&good2);
    m.check(&e, "reads in the fatal state");
    assert!(
        e.get_trace(TraceId(10u128.to_be_bytes()))
            .unwrap()
            .is_none()
    );
    assert!(e.stats().last_error.is_some());
    drop(e);

    // A restart recovers everything that was acknowledged.
    let e = open(opts(dir.path()));
    let have = e.list_traces(&list_all()).unwrap();
    assert!(
        have.iter()
            .any(|s| s.trace_id == TraceId(2u128.to_be_bytes()))
    );
    assert!(
        have.iter()
            .any(|s| s.trace_id == TraceId(1u128.to_be_bytes()))
    );
    e.shutdown().unwrap();
}

#[test]
fn dropping_the_last_handle_does_not_hang() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let (tx, rx) = mpsc::channel();
    let s = sample(1, 100);
    // The callback owns a clone: the last handle may be dropped by the engine's
    // own thread.
    let e2 = e.clone();
    e.ingest(TAG, encode(&s), to_batches(&s), move |r| {
        let _ = e2.stats();
        tx.send(r).unwrap();
    })
    .unwrap();
    drop(e);
    rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    // The cleanup runs on its own thread; the directory is usable again soon.
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        match Engine::open(opts(dir.path()), decoders()) {
            Ok(e) => {
                assert!(e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().is_some());
                e.shutdown().unwrap();
                break;
            }
            Err(Error::Conflict(_)) if Instant::now() < end => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(other) => panic!("{other:?}"),
        }
    }
}
