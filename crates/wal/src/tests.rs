use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use nwdb_core::Error;

use super::*;

fn opts(max_file_bytes: u64) -> WalOptions {
    WalOptions {
        max_file_bytes,
        sync_interval: Duration::from_millis(1),
        sync: SyncMode::None,
        sync_hook: None,
    }
}

fn replay_all(wal: &Wal, from: Lsn) -> Vec<(Lsn, Vec<u8>)> {
    let mut out = Vec::new();
    let n = wal
        .replay(from, |lsn, p| {
            out.push((lsn, p.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(n as usize, out.len());
    out
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    v.sort();
    v
}

/// Writes `payloads` in order, shuts down cleanly and returns the file list.
fn build(dir: &Path, options: WalOptions, payloads: &[Vec<u8>]) -> Vec<PathBuf> {
    let (wal, _) = Wal::open(dir, options).unwrap();
    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(wal.append_blocking(p).unwrap(), i as u64 + 1);
    }
    wal.shutdown().unwrap();
    log_files(dir)
}

/// 30-byte payloads: with a 100-byte file limit two records fit per file.
fn thirty(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| vec![i as u8; 30]).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn crc32c_matches_the_standard_check_value() {
    assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn known_answer_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let payloads = vec![b"hello".to_vec(), Vec::new(), b"wal".to_vec()];
    let files = build(dir.path(), opts(1 << 20), &payloads);
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].file_name().unwrap().to_str().unwrap(),
        "wal-00000000000000000001.log"
    );
    let bytes = std::fs::read(&files[0]).unwrap();
    let expected = concat!(
        // magic "NWAL", version 1, first sequence number 1, header CRC32C
        "4e57414c",
        "01000000",
        "0100000000000000",
        "8b15a459",
        // length 5, CRC32C over length and payload, "hello"
        "05000000",
        "a1b1174e",
        "68656c6c6f",
        // length 0, CRC32C, no payload
        "00000000",
        "c74b6748",
        // length 3, CRC32C, "wal"
        "03000000",
        "d89cf39e",
        "77616c",
    );
    assert_eq!(hex(&bytes), expected);
}

#[test]
fn reopen_replays_in_order_and_numbering_continues() {
    let dir = tempfile::tempdir().unwrap();
    let first = thirty(7);
    build(dir.path(), opts(100), &first);

    let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
    assert_eq!(rec.first_lsn, 1);
    assert_eq!(rec.last_lsn, 7);
    assert_eq!(rec.files, 4);
    assert_eq!(rec.torn_bytes_discarded, 0);
    let all = replay_all(&wal, 0);
    assert_eq!(all.len(), 7);
    for (i, (lsn, p)) in all.iter().enumerate() {
        assert_eq!(*lsn, i as u64 + 1);
        assert_eq!(p, &first[i]);
    }
    // From the middle of a file (file 2 holds records 3 and 4), from a later
    // file, and from beyond the end.
    assert_eq!(replay_all(&wal, 4).first().unwrap().0, 4);
    assert_eq!(replay_all(&wal, 4).len(), 4);
    assert_eq!(replay_all(&wal, 7).len(), 1);
    assert_eq!(replay_all(&wal, 8).len(), 0);

    assert_eq!(wal.append_blocking(b"next").unwrap(), 8);
    assert_eq!(wal.append_blocking(b"after").unwrap(), 9);
    wal.shutdown().unwrap();

    let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
    assert_eq!(rec.last_lsn, 9);
    let all = replay_all(&wal, 8);
    assert_eq!(all, vec![(8, b"next".to_vec()), (9, b"after".to_vec())]);
}

#[test]
fn replay_after_append_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), opts(100)).unwrap();
    wal.append_blocking(b"x").unwrap();
    let err = wal.replay(0, |_, _| Ok(())).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
}

#[test]
fn replay_stops_at_a_callback_error() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), opts(100), &thirty(5));
    let (wal, _) = Wal::open(dir.path(), opts(100)).unwrap();
    let mut seen = 0;
    let err = wal
        .replay(0, |_, _| {
            seen += 1;
            if seen == 3 {
                Err(Error::conflict("stop"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
    assert_eq!(seen, 3);
}

#[test]
fn rolls_at_the_limit_and_gives_a_large_record_its_own_file() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), opts(100)).unwrap();
    // 20 + 38 + 38 = 96 fits; a third 38 would make 134.
    for p in thirty(3) {
        wal.append_blocking(&p).unwrap();
    }
    assert_eq!(wal.stats().files, 2);
    // Larger than the limit by itself: alone in a file, and the next record
    // starts another.
    let big = vec![9u8; 500];
    assert_eq!(wal.append_blocking(&big).unwrap(), 4);
    assert_eq!(wal.append_blocking(b"after").unwrap(), 5);
    assert_eq!(wal.stats().files, 4);
    wal.shutdown().unwrap();

    let files = log_files(dir.path());
    assert_eq!(files.len(), 4);
    assert_eq!(std::fs::metadata(&files[2]).unwrap().len(), 20 + 8 + 500);
    let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
    assert_eq!(rec.last_lsn, 5);
    let all = replay_all(&wal, 4);
    assert_eq!(all, vec![(4, big), (5, b"after".to_vec())]);
}

#[test]
fn truncate_before_removes_only_whole_old_files() {
    let dir = tempfile::tempdir().unwrap();
    // Files hold records [1,2] [3,4] [5,6] [7,8] [9,10].
    let (wal, _) = Wal::open(dir.path(), opts(100)).unwrap();
    for p in thirty(10) {
        wal.append_blocking(&p).unwrap();
    }
    assert_eq!(wal.stats().files, 5);
    assert_eq!(wal.truncate_before(1).unwrap(), 0);
    assert_eq!(
        wal.truncate_before(2).unwrap(),
        0,
        "record 2 is still needed"
    );
    assert_eq!(wal.truncate_before(4).unwrap(), 1, "file [3,4] holds 4");
    assert_eq!(wal.stats().files, 4);
    assert_eq!(wal.truncate_before(4).unwrap(), 0);
    // Far beyond the end: everything but the active file goes.
    assert_eq!(wal.truncate_before(1000).unwrap(), 3);
    assert_eq!(wal.stats().files, 1);
    assert_eq!(log_files(dir.path()).len(), 1);
    assert_eq!(wal.append_blocking(b"more").unwrap(), 11);
    wal.shutdown().unwrap();

    let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
    assert_eq!(rec.first_lsn, 9);
    assert_eq!(rec.last_lsn, 11);
    let all = replay_all(&wal, 0);
    assert_eq!(all.len(), 3);
    assert_eq!(all[0], (9, thirty(10)[8].clone()));
    assert_eq!(all[2], (11, b"more".to_vec()));
}

#[test]
fn every_torn_length_of_the_tail_recovers_a_clean_prefix() {
    let src = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = ["alpha", "", "gamma-gamma", "d", "epsilon-record"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    let files = build(src.path(), opts(1 << 20), &payloads);
    assert_eq!(files.len(), 1);
    let name = files[0].file_name().unwrap().to_owned();
    let full = std::fs::read(&files[0]).unwrap();

    let mut ends = Vec::new();
    let mut pos = HEADER_LEN;
    for p in &payloads {
        pos += 8 + p.len() as u64;
        ends.push(pos);
    }
    assert_eq!(pos, full.len() as u64);
    let n = payloads.len();
    let start = ends[n - 3]; // start of the second-to-last record

    for len in start..=full.len() as u64 {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(&name), &full[..len as usize]).unwrap();
        let (wal, rec) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
        let k = ends.iter().filter(|&&e| e <= len).count();
        let good_end = if k == 0 { HEADER_LEN } else { ends[k - 1] };
        assert_eq!(rec.last_lsn, k as u64, "length {len}");
        assert_eq!(rec.torn_bytes_discarded, len - good_end, "length {len}");
        assert_eq!(
            std::fs::metadata(dir.path().join(&name)).unwrap().len(),
            good_end
        );
        let got = replay_all(&wal, 0);
        assert_eq!(got.len(), k, "length {len}");
        for (i, (lsn, p)) in got.iter().enumerate() {
            assert_eq!((*lsn, p), (i as u64 + 1, &payloads[i]));
        }

        assert_eq!(wal.append_blocking(b"after-tear").unwrap(), k as u64 + 1);
        wal.shutdown().unwrap();
        let (wal, rec) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
        assert_eq!(rec.last_lsn, k as u64 + 1);
        assert_eq!(rec.torn_bytes_discarded, 0);
        let got = replay_all(&wal, 0);
        assert_eq!(got.len(), k + 1);
        assert_eq!(got[k], (k as u64 + 1, b"after-tear".to_vec()));
    }
}

#[test]
fn flipped_byte_in_the_last_record_drops_it() {
    let dir = tempfile::tempdir().unwrap();
    let files = build(dir.path(), opts(1 << 20), &thirty(3));
    let mut bytes = std::fs::read(&files[0]).unwrap();
    *bytes.last_mut().unwrap() ^= 0x01;
    std::fs::write(&files[0], &bytes).unwrap();

    let (wal, rec) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(rec.last_lsn, 2);
    assert_eq!(rec.torn_bytes_discarded, 38);
    assert_eq!(replay_all(&wal, 0).len(), 2);
    assert_eq!(wal.append_blocking(b"x").unwrap(), 3);
}

#[test]
fn damage_in_the_middle_of_the_last_file_hides_what_follows() {
    let dir = tempfile::tempdir().unwrap();
    let files = build(dir.path(), opts(1 << 20), &thirty(4));
    let mut bytes = std::fs::read(&files[0]).unwrap();
    bytes[20 + 38 + 8] ^= 0x01; // payload of record 2
    std::fs::write(&files[0], &bytes).unwrap();
    let (_wal, rec) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(rec.last_lsn, 1);
    assert_eq!(rec.torn_bytes_discarded, 38 * 3);
}

#[test]
fn damage_before_the_last_file_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let files = build(dir.path(), opts(100), &thirty(6));
    assert_eq!(files.len(), 3);

    let original = std::fs::read(&files[0]).unwrap();
    // A flipped payload byte.
    let mut bytes = original.clone();
    bytes[40] ^= 0x01;
    std::fs::write(&files[0], &bytes).unwrap();
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    // A short record at the end of a file that is not the last.
    std::fs::write(&files[0], &original[..original.len() - 3]).unwrap();
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    // A bad header.
    let mut bytes = original.clone();
    bytes[0] = b'X';
    std::fs::write(&files[0], &bytes).unwrap();
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    // A header that disagrees with the file name.
    std::fs::write(&files[0], &original).unwrap();
    std::fs::rename(&files[0], dir.path().join(format::file_name(2))).unwrap();
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn a_gap_between_files_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let files = build(dir.path(), opts(100), &thirty(8));
    assert_eq!(files.len(), 4);
    std::fs::remove_file(&files[1]).unwrap();
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn overlapping_files_are_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let files = build(dir.path(), opts(100), &thirty(6));
    // A file claiming to start at 2 overlaps the one holding 1 and 2. Its
    // header must say 2 for the name check to pass, so write one by hand.
    let mut fake = format::encode_header(2).to_vec();
    format::encode_record(b"x", &mut fake);
    std::fs::write(dir.path().join(format::file_name(2)), fake).unwrap();
    assert_eq!(log_files(dir.path()).len(), files.len() + 1);
    let err = Wal::open(dir.path(), opts(100)).err().unwrap();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn a_last_file_with_an_incomplete_header_is_removed() {
    for keep in [0usize, 5, 19] {
        let dir = tempfile::tempdir().unwrap();
        let files = build(dir.path(), opts(100), &thirty(5));
        assert_eq!(files.len(), 3);
        // The file for record 5 is the last. Cut it to a short header.
        let bytes = std::fs::read(&files[2]).unwrap();
        std::fs::write(&files[2], &bytes[..keep]).unwrap();

        let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
        assert_eq!(rec.last_lsn, 4);
        assert_eq!(rec.files, 2);
        assert_eq!(replay_all(&wal, 0).len(), 4);
        assert_eq!(wal.append_blocking(b"again").unwrap(), 5);
        wal.shutdown().unwrap();
        let (wal, rec) = Wal::open(dir.path(), opts(100)).unwrap();
        assert_eq!(rec.last_lsn, 5);
        assert_eq!(replay_all(&wal, 5)[0].1, b"again");
    }
}

/// A hook that counts its calls and blocks while the gate is closed.
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
    calls: AtomicUsize,
    fail: AtomicBool,
}

impl Gate {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Gate {
            open: Mutex::new(open),
            cv: Condvar::new(),
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        })
    }

    fn set_open(&self, open: bool) {
        *self.open.lock().unwrap() = open;
        self.cv.notify_all();
    }

    fn hook(self: &Arc<Self>) -> SyncHook {
        let g = Arc::clone(self);
        Arc::new(move || {
            g.calls.fetch_add(1, Ordering::SeqCst);
            let mut open = g.open.lock().unwrap();
            while !*open {
                open = g.cv.wait(open).unwrap();
            }
            if g.fail.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("injected sync failure"));
            }
            Ok(())
        })
    }
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

type Results = Arc<Mutex<Vec<nwdb_core::Result<Lsn>>>>;

fn collect(results: &Results) -> impl FnOnce(nwdb_core::Result<Lsn>) + Send + 'static + use<> {
    let results = Arc::clone(results);
    move |r| results.lock().unwrap().push(r)
}

#[test]
fn nothing_is_acknowledged_before_the_sync_returns() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Gate::new(false);
    let mut o = opts(1 << 20);
    o.sync_hook = Some(gate.hook());
    let (wal, _) = Wal::open(dir.path(), o).unwrap();

    let results: Results = Arc::default();
    for i in 0..5u8 {
        wal.append(&[i], collect(&results));
    }
    wait_for("the sync to start", || {
        gate.calls.load(Ordering::SeqCst) == 1
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        results.lock().unwrap().is_empty(),
        "acknowledged before sync"
    );

    gate.set_open(true);
    wait_for("the acknowledgements", || {
        results.lock().unwrap().len() == 5
    });
    let got: Vec<Lsn> = results
        .lock()
        .unwrap()
        .iter()
        .map(|r| *r.as_ref().unwrap())
        .collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);
    wal.shutdown().unwrap();
}

#[test]
fn a_failed_sync_poisons_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Gate::new(true);
    let mut o = opts(1 << 20);
    o.sync_hook = Some(gate.hook());
    let (wal, _) = Wal::open(dir.path(), o).unwrap();

    assert_eq!(wal.append_blocking(b"ok").unwrap(), 1);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);

    // Queue three records behind a sync that will fail.
    gate.set_open(false);
    gate.fail.store(true, Ordering::SeqCst);
    let results: Results = Arc::default();
    for _ in 0..3 {
        wal.append(b"doomed", collect(&results));
    }
    wait_for("the sync to start", || {
        gate.calls.load(Ordering::SeqCst) == 2
    });
    gate.set_open(true);
    wait_for("the failures", || results.lock().unwrap().len() == 3);
    assert!(results.lock().unwrap().iter().all(|r| r.is_err()));

    // Later appends fail at once, without another sync attempt.
    let err = wal.append_blocking(b"late").unwrap_err();
    assert!(err.to_string().contains("poisoned"), "{err}");
    wal.append(b"later", collect(&results));
    assert_eq!(results.lock().unwrap().len(), 4);
    assert!(results.lock().unwrap()[3].is_err());
    assert_eq!(
        gate.calls.load(Ordering::SeqCst),
        2,
        "a failed sync was retried"
    );
    assert_eq!(wal.stats().records, 1);
    assert!(wal.shutdown().is_err());
}

#[test]
fn eight_threads_share_syncs_and_every_record_survives() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), opts(64 * 1024)).unwrap();
    let wal = Arc::new(wal);

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let wal = Arc::clone(&wal);
            std::thread::spawn(move || {
                let mut lsns = Vec::new();
                for seq in 0..PER_THREAD {
                    let mut p = vec![t as u8];
                    p.extend_from_slice(&(seq as u32).to_le_bytes());
                    lsns.push(wal.append_blocking(&p).unwrap());
                }
                lsns
            })
        })
        .collect();
    let per_thread: Vec<Vec<Lsn>> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let mut all: Vec<Lsn> = per_thread.iter().flatten().copied().collect();
    all.sort_unstable();
    let total = (THREADS * PER_THREAD) as u64;
    assert_eq!(all, (1..=total).collect::<Vec<_>>());
    for lsns in &per_thread {
        assert!(lsns.windows(2).all(|w| w[0] < w[1]));
    }
    let stats = wal.stats();
    assert_eq!(stats.records, total);
    assert!(stats.max_group > 1, "no group larger than one: {stats:?}");
    assert!(stats.groups < total);
    assert!(stats.files > 1);
    assert_eq!(stats.pending_bytes, 0);
    Arc::into_inner(wal).unwrap().shutdown().unwrap();

    let (wal, rec) = Wal::open(dir.path(), opts(64 * 1024)).unwrap();
    assert_eq!(rec.last_lsn, total);
    let mut next_seq = [0u32; THREADS];
    let mut count = 0;
    for (lsn, p) in replay_all(&wal, 0) {
        count += 1;
        let t = p[0] as usize;
        let seq = u32::from_le_bytes(p[1..5].try_into().unwrap());
        assert_eq!(seq, next_seq[t], "thread {t} out of order");
        assert_eq!(lsn, per_thread[t][seq as usize]);
        next_seq[t] += 1;
    }
    assert_eq!(count, total);
}

#[test]
fn empty_records_and_the_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(wal.append_blocking(&[]).unwrap(), 1);
    assert_eq!(wal.append_blocking(b"x").unwrap(), 2);
    assert_eq!(wal.append_blocking(&[]).unwrap(), 3);

    let too_big = vec![0u8; MAX_RECORD_BYTES + 1];
    let err = wal.append_blocking(&too_big).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    // The rejection does not poison the log or use a sequence number.
    assert_eq!(wal.append_blocking(b"y").unwrap(), 4);
    wal.shutdown().unwrap();

    let (wal, _) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
    let got = replay_all(&wal, 0);
    assert_eq!(
        got.iter().map(|(_, p)| p.len()).collect::<Vec<_>>(),
        vec![0, 1, 0, 1]
    );
}

#[test]
fn shutdown_writes_everything_queued() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(1 << 20);
    // A long interval would hold the queue back; shutdown must cut it short.
    o.sync_interval = Duration::from_secs(30);
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    let results: Results = Arc::default();
    for i in 0..50u8 {
        wal.append(&[i], collect(&results));
    }
    let started = Instant::now();
    wal.shutdown().unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 50);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(*r.as_ref().unwrap(), i as u64 + 1);
    }
    drop(results);

    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!(rec.last_lsn, 50);
    assert_eq!(replay_all(&wal, 0).len(), 50);
}

#[test]
fn dropping_without_shutdown_does_not_hang() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(1 << 20);
    o.sync_interval = Duration::from_secs(30);
    let path = dir.path().to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (wal, _) = Wal::open(&path, o).unwrap();
        for i in 0..20u8 {
            wal.append(&[i], |_| {});
        }
        drop(wal);
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("drop did not return");

    // Whatever was written is a clean prefix.
    let (wal, rec) = Wal::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(rec.torn_bytes_discarded, 0);
    assert_eq!(replay_all(&wal, 0).len() as u64, rec.last_lsn);
}

#[test]
fn real_sync_mode_works() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(100);
    o.sync = SyncMode::Data;
    let (wal, _) = Wal::open(dir.path(), o.clone()).unwrap();
    for p in thirty(5) {
        wal.append_blocking(&p).unwrap();
    }
    assert!(wal.stats().syncs >= 5);
    wal.truncate_before(3).unwrap();
    wal.shutdown().unwrap();
    let (wal, rec) = Wal::open(dir.path(), o).unwrap();
    assert_eq!(rec.first_lsn, 3);
    assert_eq!(replay_all(&wal, 0).len(), 3);
}
