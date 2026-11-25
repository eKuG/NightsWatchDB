//! Reviewer tests: each asserts the behaviour the crate should have; a failure
//! demonstrates a defect.

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use nwdb_core::Error;
use nwdb_wal::{SyncMode, Wal, WalOptions};

fn opts() -> WalOptions {
    WalOptions {
        max_file_bytes: 1 << 20,
        sync_interval: Duration::from_millis(1),
        sync: SyncMode::None,
        ..WalOptions::default()
    }
}

fn header(first: u64) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(b"NWAL");
    h.extend_from_slice(&1u32.to_le_bytes());
    h.extend_from_slice(&first.to_le_bytes());
    let crc = crc32c::crc32c(&h);
    h.extend_from_slice(&crc.to_le_bytes());
    h
}

fn record(payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() as u32).to_le_bytes();
    let crc = crc32c::crc32c_append(crc32c::crc32c(&len), payload);
    let mut out = len.to_vec();
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn write_file(dir: &Path, first: u64, bytes: &[u8]) {
    std::fs::write(dir.join(format!("wal-{first:020}.log")), bytes).unwrap();
}

/// Sequence number 0 is never valid (the first record ever is 1). A file
/// claiming to start at 0 must be rejected, not crash recovery.
#[test]
fn a_file_starting_at_lsn_zero_is_corrupt_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), 0, &header(0));
    let got = std::panic::catch_unwind(|| Wal::open(dir.path(), opts()).map(|(_, r)| r));
    match got {
        Ok(Err(Error::Corrupt(_))) => {}
        Ok(other) => panic!("accepted: {other:?}"),
        Err(_) => panic!("Wal::open panicked on a file starting at sequence number 0"),
    }
}

/// A header number near u64::MAX must be rejected, not overflow.
#[test]
fn a_header_number_near_u64_max_is_corrupt_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = header(u64::MAX);
    bytes.extend(record(b"x"));
    write_file(dir.path(), u64::MAX, &bytes);
    let got = std::panic::catch_unwind(|| Wal::open(dir.path(), opts()).map(|(_, r)| r));
    match got {
        Ok(Err(Error::Corrupt(_))) => {}
        Ok(other) => panic!("accepted: {other:?}"),
        Err(_) => panic!("Wal::open panicked on a header numbered u64::MAX"),
    }
}

/// Concern (a), beyond "hides": a flipped byte in an acknowledged record in
/// the middle of the last file makes `open` physically cut every later
/// acknowledged record (before the caller can look at `Recovery`), and the
/// next append reuses the sequence number of an acknowledged record.
#[test]
fn mid_file_damage_destroys_acknowledged_records_and_reuses_their_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), opts()).unwrap();
    for i in 0..4u8 {
        assert_eq!(wal.append_blocking(&[i; 30]).unwrap(), u64::from(i) + 1);
    }
    wal.shutdown().unwrap();
    let path = dir.path().join(format!("wal-{:020}.log", 1));
    let mut bytes = std::fs::read(&path).unwrap();
    let before = bytes.len();
    bytes[20 + 38 + 8] ^= 1; // payload of record 2
    std::fs::write(&path, &bytes).unwrap();

    // API change: `open` now refuses the damaged file (Corrupt) instead of
    // cutting it and handing back a log that reuses acknowledged numbers.
    let err = Wal::open(dir.path(), opts())
        .err()
        .expect("open must refuse");
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    let after = std::fs::metadata(&path).unwrap().len() as usize;
    assert_eq!(after, before, "open modified the damaged file");
    assert_eq!(std::fs::read(&path).unwrap(), bytes, "contents changed");
}

/// Concern (b): asking to replay from a position the log no longer holds is
/// a gap, not an empty or short replay.
#[test]
fn replay_from_a_trimmed_position_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100; // two 30-byte records per file
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    for i in 0..6u8 {
        wal.append_blocking(&[i; 30]).unwrap();
    }
    // The engine trims too far by mistake (or a bug elsewhere did).
    wal.truncate_before(5).unwrap();
    wal.shutdown().unwrap();

    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!(rec.first_lsn, 5);
    // Records 3 and 4 are gone; the manifest said only 2 was flushed.
    let r = wal.replay(3, |_, _| Ok(()));
    assert!(
        r.is_err(),
        "replay(3) on a log starting at 5 returned {r:?} instead of reporting the gap"
    );
}

/// The manifest can be ahead of the log (log lost its tail, or was wiped).
/// The log has no way to be told to continue numbering above that point, so
/// new acknowledged records get numbers the engine will skip on replay.
#[test]
fn numbering_cannot_be_moved_above_a_flushed_position() {
    let dir = tempfile::tempdir().unwrap();
    // API change: the engine passes the position it already flushed through
    // `min_next_lsn`, which is the new way to move numbering above it.
    let flushed_in_manifest = 100;
    let mut o = opts();
    o.min_next_lsn = Some(flushed_in_manifest + 1);
    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!(rec.last_lsn, flushed_in_manifest);
    assert_eq!(rec.reset_to, Some(flushed_in_manifest + 1));
    assert_eq!(
        wal.replay(flushed_in_manifest + 1, |_, _| Ok(())).unwrap(),
        0
    );
    let lsn = wal.append_blocking(b"acked").unwrap();
    assert!(
        lsn > flushed_in_manifest,
        "acknowledged record got {lsn}, at or below the flushed position \
         {flushed_in_manifest}: a restart replaying from {} skips it",
        flushed_in_manifest + 1
    );
}

/// The crate says "Any exit other than a clean shutdown poisons the log so
/// that no sender is left waiting". A panic on the log thread while a batch
/// is in flight drops that batch's callbacks without calling them.
#[test]
fn a_panic_on_the_log_thread_still_answers_in_flight_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.sync_hook = Some(Arc::new(|| panic!("simulated panic during sync")));
    let (wal, _) = Wal::open(dir.path(), o).unwrap();
    let (tx, rx) = mpsc::channel();
    wal.append(b"x", move |r| {
        let _ = tx.send(r.is_err());
    });
    let answered = rx.recv_timeout(Duration::from_secs(5));
    assert!(
        answered.is_ok(),
        "the in-flight record's callback was never called: {answered:?}"
    );
}

/// A 20-byte last file of zeros is what a crash during `create_file` can
/// leave on filesystems that persist the size before the data. No record can
/// follow a header that was never synced, so this is safe to discard.
#[test]
fn a_zero_filled_header_only_last_file_does_not_block_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100;
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    for i in 0..2u8 {
        wal.append_blocking(&[i; 30]).unwrap();
    }
    wal.shutdown().unwrap();
    write_file(dir.path(), 3, &[0u8; 20]);
    let r = Wal::open(dir.path(), o).map(|(_, r)| r);
    assert!(r.is_ok(), "{r:?}");
}

// ---- further regression tests for the remaining findings ----

fn list(dir: &Path) -> Vec<String> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn fill(dir: &Path, o: &WalOptions, n: u8) {
    let (wal, _) = Wal::open(dir, o.clone()).unwrap();
    for i in 0..n {
        wal.append_blocking(&[i; 30]).unwrap();
    }
    wal.shutdown().unwrap();
}

#[test]
fn replay_beyond_the_end_of_the_log_is_an_error_but_one_past_is_fine() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(), 3);
    let (wal, rec) = Wal::open(dir.path(), opts()).unwrap();
    assert_eq!(rec.last_lsn, 3);
    assert_eq!(wal.replay(4, |_, _| Ok(())).unwrap(), 0);
    assert!(matches!(
        wal.replay(5, |_, _| Ok(())),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(wal.replay(2, |_, _| Ok(())).unwrap(), 2);
}

#[test]
fn min_next_lsn_at_or_below_the_next_number_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(), 3);
    for min in [0, 1, 3, 4] {
        let mut o = opts();
        o.min_next_lsn = Some(min);
        let (wal, rec) = Wal::open(dir.path(), o).unwrap();
        assert_eq!((rec.first_lsn, rec.last_lsn), (1, 3));
        assert_eq!((rec.reset_to, rec.records_discarded), (None, 0));
        wal.shutdown().unwrap();
    }
}

#[test]
fn min_next_lsn_above_the_log_discards_every_file_and_restarts_there() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100;
    fill(dir.path(), &o, 5);
    assert_eq!(list(dir.path()).len(), 3);

    o.min_next_lsn = Some(50);
    let (wal, rec) = Wal::open(dir.path(), o.clone()).unwrap();
    assert_eq!(rec.reset_to, Some(50));
    assert_eq!(rec.records_discarded, 5);
    assert_eq!((rec.first_lsn, rec.last_lsn, rec.files), (50, 49, 1));
    assert_eq!(list(dir.path()), vec![format!("wal-{:020}.log", 50)]);
    assert_eq!(wal.durable_lsn(), 49);
    assert_eq!(wal.append_blocking(b"a").unwrap(), 50);
    wal.shutdown().unwrap();

    // The next open with the same setting finds nothing to reset.
    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn, rec.reset_to), (50, 50, None));
    let mut got = Vec::new();
    wal.replay(50, |l, p| {
        got.push((l, p.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(got, vec![(50, b"a".to_vec())]);
}

#[test]
fn durable_lsn_tracks_acknowledged_records() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path(), &opts(), 2);
    let (wal, rec) = Wal::open(dir.path(), opts()).unwrap();
    assert_eq!(wal.durable_lsn(), rec.last_lsn);
    assert_eq!(wal.append_blocking(b"x").unwrap(), 3);
    assert_eq!(wal.durable_lsn(), 3);
}

#[test]
fn real_sync_open_of_an_existing_log_and_nested_new_directory() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.sync = SyncMode::Data;
    let nested = dir.path().join("a").join("b");
    fill(&nested, &o, 3);
    let (wal, rec) = Wal::open(&nested, o).unwrap();
    assert_eq!(rec.last_lsn, 3);
    assert_eq!(wal.durable_lsn(), 3);
}

#[test]
fn leftover_temporary_files_and_invalid_header_only_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100;
    fill(dir.path(), &o, 2);
    let tmp = format!("wal-{:020}.log.tmp", 3);
    std::fs::write(dir.path().join(&tmp), header(3)).unwrap();
    // A header-only last file with a wrong checksum.
    let mut bad = header(3);
    bad[10] ^= 1;
    write_file(dir.path(), 3, &bad);
    let (wal, rec) = Wal::open(dir.path(), o.clone()).unwrap();
    assert_eq!((rec.last_lsn, rec.files), (2, 1));
    assert!(!list(dir.path()).contains(&tmp));
    assert_eq!(wal.append_blocking(b"n").unwrap(), 3);
    wal.shutdown().unwrap();

    // A last file with an invalid header and more than a header is not
    // silently dropped.
    let mut bad = header(4);
    bad[10] ^= 1;
    bad.extend(record(b"x"));
    write_file(dir.path(), 4, &bad);
    assert!(matches!(Wal::open(dir.path(), o), Err(Error::Corrupt(_))));
}

#[test]
fn no_temporary_file_is_left_after_rolling() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100;
    fill(dir.path(), &o, 6);
    assert!(list(dir.path()).iter().all(|n| !n.ends_with(".tmp")));
}

#[test]
fn truncate_before_leaves_a_contiguous_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 100;
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    for i in 0..8u8 {
        wal.append_blocking(&[i; 30]).unwrap();
    }
    assert_eq!(wal.truncate_before(6).unwrap(), 2);
    assert_eq!(
        list(dir.path()),
        vec![format!("wal-{:020}.log", 5), format!("wal-{:020}.log", 7)]
    );
    wal.shutdown().unwrap();
    let (_, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!((rec.first_lsn, rec.last_lsn), (5, 8));
}

#[test]
fn big_and_small_records_round_trip_through_the_write_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_file_bytes = 1 << 30;
    let payloads: Vec<Vec<u8>> = vec![
        vec![1; 10],
        vec![2; 70_000],
        vec![3; 65_536 - 8 - 10 - 1],
        vec![4; 3],
        vec![5; 200_000],
        vec![],
    ];
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    let (tx, rx) = mpsc::channel();
    for p in &payloads {
        let tx = tx.clone();
        wal.append_owned(p.clone(), move |r| tx.send(r.unwrap()).unwrap());
    }
    for i in 1..=payloads.len() as u64 {
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), i);
    }
    wal.shutdown().unwrap();
    let (wal, _) = Wal::open(dir.path(), o).unwrap();
    let mut got = Vec::new();
    wal.replay(1, |_, p| {
        got.push(p.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(got, payloads);
}

#[test]
fn a_poisoned_log_answers_fatal_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.sync_hook = Some(Arc::new(|| Err(std::io::Error::other("disk gone"))));
    let (wal, _) = Wal::open(dir.path(), o).unwrap();
    assert!(matches!(wal.append_blocking(b"x"), Err(Error::Fatal(_))));
    assert!(matches!(wal.append_blocking(b"y"), Err(Error::Fatal(_))));
    // The caller's own mistake stays an argument error.
    let big = vec![0u8; nwdb_wal::MAX_RECORD_BYTES + 1];
    let (tx, rx) = mpsc::channel();
    wal.append(&big, move |r| tx.send(r).unwrap());
    assert!(matches!(rx.recv().unwrap(), Err(Error::InvalidArgument(_))));
    assert!(matches!(wal.shutdown(), Err(Error::Fatal(_))));
}

#[test]
fn a_panic_on_the_log_thread_fails_every_record_with_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.sync_hook = Some(Arc::new(|| panic!("simulated panic during sync")));
    let (wal, _) = Wal::open(dir.path(), o).unwrap();
    let (tx, rx) = mpsc::channel();
    for _ in 0..3 {
        let tx = tx.clone();
        wal.append(b"x", move |r| tx.send(r).unwrap());
    }
    for _ in 0..3 {
        let r = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(r, Err(Error::Fatal(_))), "{r:?}");
    }
    assert!(matches!(wal.append_blocking(b"z"), Err(Error::Fatal(_))));
}
