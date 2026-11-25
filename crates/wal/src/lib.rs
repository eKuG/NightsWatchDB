//! The write-ahead log of NightsWatchDB.
//!
//! An ingest request is appended to the log and acknowledged to its sender
//! only after the log has been synced to disk. After a crash the engine opens
//! the log, replays it from the last record it had already written into data
//! files, and carries on. Once data is safely in data files the log is trimmed
//! with [`Wal::truncate_before`].
//!
//! # Group commit
//!
//! One dedicated thread owns the active file. It takes everything queued,
//! writes it, syncs once, then runs the callbacks of the records the sync
//! covered, in sequence-number order. [`WalOptions::sync_interval`] is the
//! minimum spacing between syncs: if the previous sync finished longer ago
//! than the interval the next one starts at once, otherwise the thread waits
//! out the interval and gathers more records meanwhile. A lone sender is
//! therefore delayed by no more than its own sync, and many concurrent senders
//! share syncs.
//!
//! The queue has no size limit. A caller that must bound memory should watch
//! [`WalStats::pending_bytes`] and hold back its own senders.
//!
//! # Failure
//!
//! If a write or a sync fails, the log is poisoned. Every record not yet
//! acknowledged receives the error, and every later append fails immediately.
//! A failed sync is never retried and nothing is acknowledged after it, because
//! after a failed `fsync` the contents of the file are unknown. The process
//! must restart and recover; recovery decides what the file really holds.
//!
//! # Format
//!
//! See the crate README for the file layout.

mod format;
mod writer;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use nwdb_core::{Error, Result};

use crate::format::{HEADER_LEN, Mode};

/// The sequence number of a record. The first record ever written is 1;
/// numbers grow by exactly 1 per record, across files and restarts, and are
/// never reused.
pub type Lsn = u64;

/// The largest record [`Wal::append`] accepts, in bytes (256 MiB).
pub const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;

/// What the log does to make a write durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Call `File::sync_data` on the active file at every sync point.
    Data,
    /// Never sync. **Records are acknowledged without being durable and a
    /// power loss or kernel crash can lose them.** Only for tests and
    /// benchmarks; never for a database that holds real data.
    None,
}

/// Called at each sync point, before the real sync.
pub type SyncHook = Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>;

/// Settings for [`Wal::open`].
#[derive(Clone)]
pub struct WalOptions {
    /// A new file is started when the active one would grow past this many
    /// bytes. A single record larger than the limit gets a file to itself.
    /// Default 256 MiB.
    pub max_file_bytes: u64,
    /// Minimum spacing between syncs. Default 10 ms.
    pub sync_interval: Duration,
    /// How writes are made durable. Default [`SyncMode::Data`].
    pub sync: SyncMode,
    /// A seam for tests. When set, it runs at every sync of log data (the
    /// group sync after each write, and the sync of a finished file before
    /// the next one is used), before the real sync and also when `sync` is
    /// [`SyncMode::None`]. An error it returns is treated as a failed sync.
    /// It is not called during recovery or for directory syncs.
    pub sync_hook: Option<SyncHook>,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            max_file_bytes: 256 * 1024 * 1024,
            sync_interval: Duration::from_millis(10),
            sync: SyncMode::Data,
            sync_hook: None,
        }
    }
}

impl std::fmt::Debug for WalOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalOptions")
            .field("max_file_bytes", &self.max_file_bytes)
            .field("sync_interval", &self.sync_interval)
            .field("sync", &self.sync)
            .field("sync_hook", &self.sync_hook.as_ref().map(|_| "<hook>"))
            .finish()
    }
}

/// What [`Wal::open`] found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// Sequence number of the first record still in the log. When the log is
    /// empty this is the number the next record will get.
    pub first_lsn: Lsn,
    /// Sequence number of the last valid record, or 0 when the log has never
    /// held a record.
    pub last_lsn: Lsn,
    /// Number of log files after recovery.
    pub files: usize,
    /// Bytes cut from the end of the last file because the record there was
    /// incomplete or failed its checksum.
    pub torn_bytes_discarded: u64,
}

/// Counters since the log was opened.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalStats {
    /// Records written and synced.
    pub records: u64,
    /// Bytes of those records on disk, including their 8-byte frames and not
    /// counting file headers.
    pub bytes: u64,
    /// Syncs performed (or skipped under [`SyncMode::None`] but counted).
    pub syncs: u64,
    /// Groups committed: batches of records that shared one sync.
    pub groups: u64,
    /// Records in the largest group.
    pub max_group: u64,
    /// Log files currently on disk.
    pub files: usize,
    /// Payload bytes accepted by `append` and not yet acknowledged.
    pub pending_bytes: u64,
}

pub(crate) struct Item {
    pub lsn: Lsn,
    pub payload: Vec<u8>,
    pub done: Box<dyn FnOnce(Result<Lsn>) + Send>,
}

#[derive(Default)]
pub(crate) struct State {
    pub queue: Vec<Item>,
    pub next_lsn: Lsn,
    pub poisoned: Option<String>,
    pub shutdown: bool,
    pub appended: bool,
    pub pending_bytes: u64,
    pub stats: WalStats,
}

pub(crate) struct Shared {
    pub dir: PathBuf,
    pub state: Mutex<State>,
    /// Signals the log thread: work queued or shutdown requested.
    pub wake: Condvar,
    /// First sequence number of each file on disk, ascending. Locked on its
    /// own, never while holding `state`.
    pub files: Mutex<Vec<Lsn>>,
}

/// Locks, ignoring poisoning: no code holding these locks leaves the data in
/// a half-updated state, and a panic elsewhere must not stop callers.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn poison_error(msg: &str) -> Error {
    Error::other(format!(
        "write-ahead log is poisoned by an earlier failure and must be reopened: {msg}"
    ))
}

/// An open write-ahead log. Appends may come from any number of threads.
pub struct Wal {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    /// Last sequence number that existed when the log was opened; replay
    /// stops there so it never reads what the log thread is writing.
    recovered_last: Lsn,
}

impl Wal {
    /// Opens the log in `dir`, creating the directory if needed.
    ///
    /// Validates every file, repairs a torn tail (see the README for the
    /// exact rules) and starts the log thread, ready to append after the last
    /// valid record.
    ///
    /// Damage in the middle of the last file cannot be told apart from a torn
    /// write: the first bad record ends the log and everything after it in
    /// that file is cut, valid records included. `torn_bytes_discarded`
    /// reports how much. Damage in any earlier file is [`Error::Corrupt`].
    ///
    /// Nothing prevents two processes opening the same directory; the caller
    /// must make sure only one does.
    pub fn open(dir: impl AsRef<Path>, options: WalOptions) -> Result<(Wal, Recovery)> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let mut firsts = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(first) = entry.file_name().to_str().and_then(format::parse_file_name) {
                firsts.push(first);
            }
        }
        firsts.sort_unstable();

        // A last file with an incomplete header was being created when the
        // process died; it never held a record, so it is removed.
        let mut removed_start = None;
        if let Some(&last) = firsts.last() {
            let path = format::file_path(dir, last);
            if std::fs::metadata(&path)?.len() < HEADER_LEN {
                std::fs::remove_file(&path)?;
                writer::sync_dir(dir, options.sync)?;
                firsts.pop();
                removed_start = Some(last);
            }
        }

        let mut torn = 0;
        let mut next = None::<Lsn>;
        let mut active = None;
        for (i, &first) in firsts.iter().enumerate() {
            if let Some(expected) = next
                && first != expected
            {
                return Err(Error::corrupt(format!(
                    "log files are not contiguous: expected a file starting at {expected}, found one starting at {first}"
                )));
            }
            // Only a file that was last when the process died may have a torn
            // tail. If the real last file was just removed, its predecessor
            // was synced before that file was created and must be intact.
            let is_last = i + 1 == firsts.len() && removed_start.is_none();
            let mode = if is_last { Mode::Lenient } else { Mode::Strict };
            let path = format::file_path(dir, first);
            let scan = format::scan_file(&path, first, mode, Lsn::MAX, |_, _| Ok(()))?;
            next = Some(first + scan.records);
            if i + 1 == firsts.len() {
                if scan.good_end < scan.file_len {
                    let f = std::fs::OpenOptions::new().write(true).open(&path)?;
                    f.set_len(scan.good_end)?;
                    if options.sync == SyncMode::Data {
                        f.sync_data()?;
                    }
                    torn = scan.file_len - scan.good_end;
                }
                active = Some((first, scan.good_end, scan.records));
            }
        }

        let (file, file_size, file_records) = match active {
            Some((first, size, records)) => {
                let file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(format::file_path(dir, first))?;
                (file, size, records)
            }
            None => {
                let start = removed_start.unwrap_or(1);
                firsts.push(start);
                next = Some(start);
                (
                    writer::create_file(dir, start, options.sync)?,
                    HEADER_LEN,
                    0,
                )
            }
        };
        let next_lsn = next.expect("set above");
        let recovery = Recovery {
            first_lsn: firsts[0],
            last_lsn: next_lsn - 1,
            files: firsts.len(),
            torn_bytes_discarded: torn,
        };

        let shared = Arc::new(Shared {
            dir: dir.to_path_buf(),
            state: Mutex::new(State {
                next_lsn,
                ..State::default()
            }),
            wake: Condvar::new(),
            files: Mutex::new(firsts),
        });
        let log_thread = writer::Writer {
            shared: Arc::clone(&shared),
            options,
            file,
            file_size,
            file_records,
            last_sync: None,
        };
        let thread = std::thread::Builder::new()
            .name("nwdb-wal".into())
            .spawn(move || log_thread.run())?;
        let wal = Wal {
            shared,
            thread: Some(thread),
            recovered_last: recovery.last_lsn,
        };
        Ok((wal, recovery))
    }

    /// Calls `f` with every record whose sequence number is at least `from`
    /// and that existed when the log was opened, in order, and returns how
    /// many it called. Records already removed by [`truncate_before`] are
    /// not replayed. The first error from `f` stops the replay and is
    /// returned. Files are re-verified as they are read.
    ///
    /// Must be called before the first append; afterwards it returns
    /// [`Error::InvalidArgument`].
    ///
    /// [`truncate_before`]: Wal::truncate_before
    pub fn replay(&self, from: Lsn, mut f: impl FnMut(Lsn, &[u8]) -> Result<()>) -> Result<u64> {
        if lock(&self.shared.state).appended {
            return Err(Error::invalid_argument(
                "replay must be called before the first append",
            ));
        }
        let files = lock(&self.shared.files).clone();
        let last = self.recovered_last;
        let mut count = 0;
        for (i, &first) in files.iter().enumerate() {
            if first > last {
                break;
            }
            let file_last = files.get(i + 1).map_or(last, |&n| n - 1);
            if file_last < from {
                continue;
            }
            let path = format::file_path(&self.shared.dir, first);
            format::scan_file(&path, first, Mode::Strict, last, |lsn, payload| {
                if lsn < from {
                    return Ok(());
                }
                count += 1;
                f(lsn, payload)
            })?;
        }
        Ok(count)
    }

    /// Queues `record` and returns at once. `on_durable` runs on the log
    /// thread, after the sync that covers the record has returned, with the
    /// record's sequence number, or with the error if the write or sync
    /// failed. Callbacks run in sequence-number order. A callback must not
    /// block for long, or it delays every acknowledgement behind it; a panic
    /// in a callback is caught and ignored.
    ///
    /// If the record is too large ([`MAX_RECORD_BYTES`]) or the log is already
    /// poisoned, `on_durable` runs immediately on the calling thread with the
    /// error and nothing is queued.
    pub fn append(&self, record: &[u8], on_durable: impl FnOnce(Result<Lsn>) + Send + 'static) {
        if record.len() > MAX_RECORD_BYTES {
            return on_durable(Err(Error::invalid_argument(format!(
                "record of {} bytes exceeds the limit of {MAX_RECORD_BYTES}",
                record.len()
            ))));
        }
        let payload = record.to_vec();
        let mut st = lock(&self.shared.state);
        if let Some(msg) = &st.poisoned {
            let err = poison_error(msg);
            drop(st);
            return on_durable(Err(err));
        }
        let lsn = st.next_lsn;
        st.next_lsn += 1;
        st.appended = true;
        st.pending_bytes += payload.len() as u64;
        st.queue.push(Item {
            lsn,
            payload,
            done: Box::new(on_durable),
        });
        drop(st);
        self.shared.wake.notify_one();
    }

    /// [`append`](Wal::append), then waits for the acknowledgement.
    pub fn append_blocking(&self, record: &[u8]) -> Result<Lsn> {
        let (tx, rx) = mpsc::channel();
        self.append(record, move |r| {
            let _ = tx.send(r);
        });
        rx.recv()
            .unwrap_or_else(|_| Err(Error::other("the log stopped before acknowledging")))
    }

    /// Deletes every file all of whose records have sequence numbers below
    /// `lsn`, never the file currently being written, and returns how many
    /// files were removed. The directory is synced afterwards.
    ///
    /// Files go oldest first, so a failure part-way leaves a contiguous log.
    pub fn truncate_before(&self, lsn: Lsn) -> Result<u64> {
        let mut files = lock(&self.shared.files);
        // A file's records all lie below the next file's first number.
        let removable = files.windows(2).take_while(|w| w[1] <= lsn).count();
        let mut removed = 0;
        let mut outcome = Ok(());
        for &first in &files[..removable] {
            match std::fs::remove_file(format::file_path(&self.shared.dir, first)) {
                Ok(()) => removed += 1,
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        files.drain(..removed);
        drop(files);
        if removed > 0 {
            // Always a real directory sync, whatever the sync mode: a caller
            // that sees this return may rely on the removal.
            writer::sync_dir(&self.shared.dir, SyncMode::Data)?;
        }
        outcome?;
        Ok(removed as u64)
    }

    /// A snapshot of the counters.
    pub fn stats(&self) -> WalStats {
        let st = lock(&self.shared.state);
        let mut stats = st.stats.clone();
        stats.pending_bytes = st.pending_bytes;
        drop(st);
        stats.files = lock(&self.shared.files).len();
        stats
    }

    /// Writes and syncs everything queued, then stops the log thread. Returns
    /// the error that poisoned the log, if there was one.
    pub fn shutdown(mut self) -> Result<()> {
        self.stop()
    }

    fn stop(&mut self) -> Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        lock(&self.shared.state).shutdown = true;
        self.shared.wake.notify_one();
        let _ = thread.join();
        match &lock(&self.shared.state).poisoned {
            Some(msg) => Err(poison_error(msg)),
            None => Ok(()),
        }
    }
}

/// Dropping the log stops its thread. Records still queued are written and
/// acknowledged the same way as in [`Wal::shutdown`], but errors cannot be
/// reported; a caller that needs to know the outcome must call `shutdown`.
impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
