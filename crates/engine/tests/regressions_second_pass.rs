//! A second set of tests for the start-up and query fixes. Each test fails on the code
//! it was written against and names the defect it shows.

mod common;
mod support;

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::*;
use nwdb_core::{Error, RecordDecoder};
use nwdb_engine::{AggQuery, Engine};
use nwdb_manifest::{MAX_NEXT_PART_ID, PART_ID_LEFTOVER_WINDOW};
use nwdb_wal::Wal;
use support::{disk_state, tree, tree_of};

/// A database whose log holds records 1..=5, none flushed, followed by a
/// record whose tag no decoder knows (as a door added by a newer version
/// would write).
fn log_with_an_unknown_tag_at_the_end(dir: &Path) {
    let e = open(opts(dir));
    for t in 1..=5 {
        ingest(&e, &one(t)).unwrap();
    }
    drop(e);
    let (wal, _) = Wal::open(dir.join("wal"), wal_no_sync()).unwrap();
    wal.append_blocking(&[77, 1, 2, 3]).unwrap();
    wal.shutdown().unwrap();
}

/// A stray directory with an id two below the limit must not raise
/// `next_part_id` to `MAX_NEXT_PART_ID - 1`: it is then deleted as a
/// leftover, one id is left, and every flush after the first fails with
/// "part id space exhausted", forever and across restarts.
#[test]
fn a_stray_part_directory_two_below_the_id_limit_does_not_stop_later_flushes() {
    let dir = tempfile::tempdir().unwrap();
    open(opts(dir.path())).shutdown().unwrap();
    let stray = dir
        .path()
        .join("parts")
        .join(nwdb_core::part_dir_name(MAX_NEXT_PART_ID - 2));
    std::fs::create_dir_all(&stray).unwrap();

    let e = open(opts(dir.path()));
    assert!(!stray.exists());
    for t in 1..=3 {
        ingest(&e, &one(t)).unwrap();
        let r = e.flush();
        assert!(
            r.is_ok(),
            "flush {t} failed after a stray directory named {}: {r:?}",
            stray.display()
        );
    }
    e.shutdown().unwrap();
}

/// The replay is a check that can refuse the start (an unknown tag, a record
/// that does not decode). With a buffer small enough that the replay
/// flushes, the refused start-up has already written parts and committed
/// manifest versions that move `wal_flushed_lsn`, so the next start removes
/// the log files below the new position.
#[test]
fn a_refused_replay_writes_no_parts_and_commits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    log_with_an_unknown_tag_at_the_end(dir.path());
    let before = tree_of(dir.path(), &["manifest", "parts"]);
    let (state_before, _) = disk_state(dir.path());

    let mut o = opts(dir.path());
    // The replay flushes after every record.
    o.buffer_bytes = 1;
    let err = Engine::open(o, decoders())
        .err()
        .expect("start-up accepted a record with an unknown tag");
    assert!(matches!(err, Error::Corrupt(_)), "{err}");

    let (state_after, _) = disk_state(dir.path());
    let key = |s: &nwdb_manifest::ManifestState| (s.version, s.wal_flushed_lsn, s.parts.len());
    assert_eq!(
        key(&state_after),
        key(&state_before),
        "the refused start-up committed manifest versions (version, wal_flushed_lsn, parts)"
    );
    assert_eq!(
        tree_of(dir.path(), &["manifest", "parts"]),
        before,
        "the refused start-up changed manifest/ or parts/"
    );
}

/// The same refusal with a leftover part directory and the default buffer:
/// the `next_part_id` raise is committed before the replay refuses.
#[test]
fn a_refused_replay_does_not_commit_a_raised_part_id() {
    let dir = tempfile::tempdir().unwrap();
    log_with_an_unknown_tag_at_the_end(dir.path());
    let leftover = dir.path().join("parts").join("00000000000000000007");
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("spans.parquet"), b"junk").unwrap();
    let before = tree_of(dir.path(), &["manifest"]);

    let err = Engine::open(opts(dir.path()), decoders())
        .err()
        .expect("start-up accepted a record with an unknown tag");
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(leftover.is_dir());
    assert_eq!(
        tree_of(dir.path(), &["manifest"]),
        before,
        "the refused start-up committed a manifest version (next_part_id {})",
        disk_state(dir.path()).0.next_part_id
    );
}

fn with_input(span: u64, start: i64, input: i64) -> TSpan {
    let mut s = TSpan::new(9, span, start);
    if span == 1 {
        s = s.root();
    }
    s.input = Some(input);
    s.model = Some("m".into());
    s
}

/// The summary's token sums saturate, and a saturating sum of values of both
/// signs depends on the grouping. The same three spans, summarised together
/// (all buffered) or as a part holding two of them merged with the buffer
/// holding the third, give different `input_tokens`, in `list_traces` and in
/// `aggregate_tokens`: the result depends on when the flush happened. The
/// field is an `i64`, so a negative count is a value the schema accepts.
#[test]
fn token_sums_do_not_depend_on_flush_timing() {
    let a = with_input(1, 10, i64::MAX);
    let b = with_input(2, 20, 1);
    let c = with_input(3, 30, -1);
    let list = list_all();
    let agg = AggQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        operation: None,
    };
    let mut results = Vec::new();
    for flush_between in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let e = open(opts(dir.path()));
        ingest(&e, &[b.clone(), c.clone()]).unwrap();
        if flush_between {
            e.flush().unwrap();
        }
        ingest(&e, std::slice::from_ref(&a)).unwrap();
        let listed = e.list_traces(&list).unwrap();
        assert_eq!(listed.len(), 1);
        let summed = e.aggregate_tokens(&agg).unwrap();
        assert_eq!(summed.len(), 1);
        results.push((listed[0].input_tokens, summed[0].input_tokens));
        e.shutdown().unwrap();
    }
    assert_eq!(
        results[0], results[1],
        "(list_traces, aggregate_tokens) input sums: all buffered vs. split by a flush"
    );
}

// ---------------------------------------------------------------------------
// Part ids near the limit.

fn part_dir(dir: &Path, id: u64) -> std::path::PathBuf {
    dir.join("parts").join(nwdb_core::part_dir_name(id))
}

/// A new database, opened and shut down once; returns its `next_part_id`.
fn fresh_database(dir: &Path) -> u64 {
    open(opts(dir)).shutdown().unwrap();
    disk_state(dir).0.next_part_id
}

/// Ids just below `next_part_id + PART_ID_LEFTOVER_WINDOW` can be leftovers
/// of this database and raise `next_part_id`; ids at or above it never do.
/// Either way the directory is deleted after the replay.
#[test]
fn ids_just_below_and_just_above_the_leftover_window() {
    for (offset, raises) in [
        (PART_ID_LEFTOVER_WINDOW - 1, true),
        (PART_ID_LEFTOVER_WINDOW, false),
        (PART_ID_LEFTOVER_WINDOW + 1, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let next = fresh_database(dir.path());
        let stray = part_dir(dir.path(), next + offset);
        std::fs::create_dir_all(&stray).unwrap();

        open(opts(dir.path())).shutdown().unwrap();
        assert!(!stray.exists(), "offset {offset}: the directory is kept");
        let after = disk_state(dir.path()).0.next_part_id;
        let want = if raises { next + offset + 1 } else { next };
        assert_eq!(after, want, "offset {offset}");
    }
}

/// Strays far above `next_part_id` (one near the limit, one just past the
/// window) do not use up the id space: many flushes work, across restarts.
#[test]
fn many_flushes_work_after_start_up_with_strays_present() {
    let dir = tempfile::tempdir().unwrap();
    let next = fresh_database(dir.path());
    let mut t = 0;
    for round in 0..3 {
        // The window moves up with `next_part_id`, so the one just past it is
        // worked out again each round.
        let current = disk_state(dir.path()).0.next_part_id;
        let strays = [
            MAX_NEXT_PART_ID - 1,
            MAX_NEXT_PART_ID - 2,
            current + PART_ID_LEFTOVER_WINDOW,
            u64::MAX,
        ];
        for id in strays {
            std::fs::create_dir_all(part_dir(dir.path(), id)).unwrap();
        }
        let e = open(opts(dir.path()));
        for id in strays {
            assert!(!part_dir(dir.path(), id).exists(), "round {round}: {id}");
        }
        for _ in 0..30 {
            t += 1;
            ingest(&e, &one(t)).unwrap();
            let r = e.flush();
            assert!(r.is_ok(), "round {round}, flush of trace {t}: {r:?}");
        }
        e.shutdown().unwrap();
    }
    let state = disk_state(dir.path()).0;
    assert_eq!(state.parts.len(), 90);
    assert!(state.next_part_id < next + 1000, "{}", state.next_part_id);
}

/// A real leftover (an id a little above `next_part_id`, as a flush that did
/// not commit leaves) still raises `next_part_id`, so its id is not reused.
#[test]
fn a_leftover_just_above_next_part_id_still_raises_it() {
    let dir = tempfile::tempdir().unwrap();
    let next = fresh_database(dir.path());
    let finished = part_dir(dir.path(), next + 2);
    std::fs::create_dir_all(&finished).unwrap();
    std::fs::write(finished.join("spans.parquet"), b"junk").unwrap();
    let temporary = dir
        .path()
        .join("parts")
        .join(format!("{}.tmp", nwdb_core::part_dir_name(next + 5)));
    std::fs::create_dir_all(&temporary).unwrap();

    let e = open(opts(dir.path()));
    assert!(!finished.exists() && !temporary.exists());
    ingest(&e, &one(1)).unwrap();
    e.flush().unwrap();
    e.shutdown().unwrap();
    let state = disk_state(dir.path()).0;
    assert_eq!(state.parts.len(), 1);
    assert!(
        state.parts[0].part_id >= next + 6,
        "the new part took id {}, a leftover had {}",
        state.parts[0].part_id,
        next + 5
    );
}

// ---------------------------------------------------------------------------
// Refused replays change nothing.

struct Empty;
impl RecordDecoder for Empty {
    fn decode(&self, _: &[u8]) -> nwdb_core::Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
}

/// Decodes to a table that is not the span table.
struct WrongTable;
impl RecordDecoder for WrongTable {
    fn decode(&self, _: &[u8]) -> nwdb_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let column: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        Ok(vec![RecordBatch::try_new(schema, vec![column]).unwrap()])
    }
}

type Decoders = Vec<(u8, Arc<dyn RecordDecoder>)>;

fn with_tag(tag: u8, d: impl RecordDecoder + 'static) -> Decoders {
    let mut v = decoders();
    v.push((tag, Arc::new(d)));
    v
}

/// What removes the cause of a refusal.
enum Fix {
    /// Open with these decoders instead.
    Decoders(Decoders),
    /// Put the saved log back.
    RestoreLog,
}

/// A log with records 1..=5, then `bad_record`; a leftover part directory
/// above `next_part_id`; a buffer so small that the replay would flush after
/// every record. The refused open leaves the data directory byte for byte as
/// it was, and after `fix` the database opens with every record.
fn a_refused_replay_changes_nothing(bad_record: &[u8], refusing: Decoders, fix: Fix) {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::default();
    let e = open(opts(dir.path()));
    for t in 1..=5 {
        ingest(&e, &one(t)).unwrap();
        model.add(&one(t));
    }
    drop(e);
    let saved = tempfile::tempdir().unwrap();
    support::copy_dir(&dir.path().join("wal"), saved.path());
    let (wal, _) = Wal::open(dir.path().join("wal"), wal_no_sync()).unwrap();
    wal.append_blocking(bad_record).unwrap();
    wal.shutdown().unwrap();
    let next = disk_state(dir.path()).0.next_part_id;
    let leftover = part_dir(dir.path(), next + 2);
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("spans.parquet"), b"junk").unwrap();

    let mut o = opts(dir.path());
    o.buffer_bytes = 1;
    let before = tree(dir.path());
    let err = Engine::open(o.clone(), refusing)
        .err()
        .expect("start-up accepted a log it cannot replay");
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert_eq!(
        tree(dir.path()),
        before,
        "the refused start-up changed the data directory ({err})"
    );

    let good = match fix {
        Fix::Decoders(d) => d,
        Fix::RestoreLog => {
            std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
            support::copy_dir(saved.path(), &dir.path().join("wal"));
            decoders()
        }
    };
    let e = Engine::open(o, good).expect("open after the cause was removed");
    model.check(&e, "after the cause was removed");
    assert!(!leftover.exists());
    e.shutdown().unwrap();
}

#[test]
fn a_replay_refused_for_an_unknown_tag_changes_nothing() {
    a_refused_replay_changes_nothing(
        &[77, 1, 2, 3],
        decoders(),
        Fix::Decoders(with_tag(77, Empty)),
    );
}

#[test]
fn a_replay_refused_for_a_record_that_does_not_decode_changes_nothing() {
    a_refused_replay_changes_nothing(&[TAG, 0xff, 0xff, 0xff], decoders(), Fix::RestoreLog);
}

#[test]
fn a_replay_refused_for_an_empty_record_changes_nothing() {
    a_refused_replay_changes_nothing(&[], decoders(), Fix::RestoreLog);
}

#[test]
fn a_replay_refused_for_the_wrong_table_changes_nothing() {
    a_refused_replay_changes_nothing(
        &[78, 1],
        with_tag(78, WrongTable),
        Fix::Decoders(with_tag(78, Empty)),
    );
}

// ---------------------------------------------------------------------------
// Token counts below zero.

/// A negative count in a column counts as 0, in `list_traces`, in
/// `aggregate_tokens`, and whether or not the spans were flushed together.
#[test]
fn a_negative_token_count_counts_as_zero() {
    let list = list_all();
    let agg = AggQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        operation: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut a = with_input(1, 10, 5);
    a.output = Some(i64::MIN);
    let mut b = with_input(2, 20, -100);
    b.output = Some(-7);
    ingest(&e, &[a, b]).unwrap();
    for flushed in [false, true] {
        if flushed {
            e.flush().unwrap();
        }
        let listed = e.list_traces(&list).unwrap();
        assert_eq!((listed[0].input_tokens, listed[0].output_tokens), (5, 0));
        let summed = e.aggregate_tokens(&agg).unwrap();
        assert_eq!((summed[0].input_tokens, summed[0].output_tokens), (5, 0));
    }
    e.shutdown().unwrap();
}
