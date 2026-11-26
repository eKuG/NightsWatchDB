//! The write-ahead log of NightsWatchDB.
//!
//! An ingest request is appended to the log and acknowledged to its sender
//! only after the log has been synced to disk. After a crash the engine opens
//! the log, replays it from the last record it had already written into data
//! files, and carries on. Once data is safely in data files the log is trimmed
//! with [`Wal::truncate_before`], after [`Wal::rotate`] has started a new file
//! so that the newest file, which a trim never removes, holds no record that
//! is already in data files.
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
//! The error is [`Error::Fatal`]: the outcome of the record is unknown, and it
//! may reappear on replay after the restart. Every record accepted by
//! [`Wal::append`] has its callback run exactly once, with that error if the
//! log thread dies, panics included.
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
    /// Operator override for damage in the middle of the last file. By default
    /// `open` returns [`Error::Corrupt`] and leaves the file alone when a bad
    /// record is followed by a valid one. When true, the log is cut at the
    /// first bad record instead, as for a torn tail. **This throws away every
    /// record after the damage, acknowledged ones included, and their sequence
    /// numbers will be reused.** Default false.
    pub discard_after_damage: bool,
    /// The lowest sequence number the next record may get. The engine sets it
    /// to one past the highest position it has already written into data
    /// files.
    ///
    /// Before anything else is checked, every file all of whose records are
    /// numbered below it (the following file starts at or below it) is
    /// removed, oldest first, never the newest file: it is the removal a trim
    /// would do, and damage in such a file is not an error.
    ///
    /// If the log then ends below it (the files that held the records between
    /// were lost), `open` returns [`Error::Corrupt`] and changes nothing more,
    /// unless [`allow_lost_log`](Self::allow_lost_log) is set: then every file
    /// is removed and a new one starts at this number, and
    /// [`Recovery::reset_to`] says so. If it is at or below the next number it
    /// has no effect. Must be below `u64::MAX`. Default `None`.
    pub min_next_lsn: Option<Lsn>,
    /// What to do when the log ends below `min_next_lsn`. In healthy
    /// operation that cannot happen, because records are written into data
    /// files only after they are durable in the log and trimming never
    /// removes the newest file; so it proves that log files were lost, and
    /// records acknowledged after the data files were last written are gone.
    /// By default `open` refuses. When true, it starts a new log at
    /// `min_next_lsn` and carries on, **accepting that loss**. Default false.
    pub allow_lost_log: bool,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            max_file_bytes: 256 * 1024 * 1024,
            sync_interval: Duration::from_millis(10),
            sync: SyncMode::Data,
            sync_hook: None,
            discard_after_damage: false,
            min_next_lsn: None,
            allow_lost_log: false,
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
            .field("discard_after_damage", &self.discard_after_damage)
            .field("min_next_lsn", &self.min_next_lsn)
            .field("allow_lost_log", &self.allow_lost_log)
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
    /// Set when [`WalOptions::min_next_lsn`] was above the log's own next
    /// number: every file was removed and the log restarted at this number.
    pub reset_to: Option<Lsn>,
    /// Records that were on disk and removed by that reset.
    pub records_discarded: u64,
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
    /// Highest sequence number covered by a completed sync.
    pub durable_lsn: Lsn,
    pub shutdown: bool,
    pub appended: bool,
    pub pending_bytes: u64,
    pub stats: WalStats,
    /// Callers of `rotate` waiting for the log thread, oldest first.
    pub rotations: Vec<mpsc::Sender<Result<bool>>>,
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
    Error::fatal(format!(
        "write-ahead log is poisoned by an earlier failure and must be reopened: {msg}"
    ))
}

/// An open write-ahead log. Appends may come from any number of threads.
pub struct Wal {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    /// First and last sequence numbers that existed when the log was opened;
    /// replay stops at the last so it never reads what the log thread is
    /// writing.
    recovered_first: Lsn,
    recovered_last: Lsn,
}

impl Wal {
    /// Opens the log in `dir`, creating the directory if needed.
    ///
    /// Validates every file, repairs a torn tail (see the README for the
    /// exact rules) and starts the log thread, ready to append after the last
    /// valid record. With [`SyncMode::Data`] the last file and the directory
    /// are synced before returning, so everything the returned [`Recovery`]
    /// describes is durable.
    ///
    /// In the last file, a bad record with nothing valid after it is a torn
    /// tail and is cut. A bad record followed by a valid one is damage, not a
    /// tear: `open` returns [`Error::Corrupt`] and does not modify the file,
    /// unless [`WalOptions::discard_after_damage`] is set. Damage in any
    /// earlier file is [`Error::Corrupt`].
    ///
    /// With [`WalOptions::min_next_lsn`] set, files wholly below it are removed
    /// first, and a log that ends below it is [`Error::Corrupt`] unless
    /// [`WalOptions::allow_lost_log`] is set; see there.
    ///
    /// Nothing prevents two processes opening the same directory; the caller
    /// must make sure only one does.
    pub fn open(dir: impl AsRef<Path>, options: WalOptions) -> Result<(Wal, Recovery)> {
        let dir = dir.as_ref();
        if options.min_next_lsn == Some(Lsn::MAX) {
            return Err(Error::invalid_argument(
                "min_next_lsn must be below u64::MAX",
            ));
        }
        if !dir.is_dir()
            && let Some(min) = options.min_next_lsn
            && min > 1
            && !options.allow_lost_log
        {
            // Refuse before creating anything: the whole directory is lost.
            return Err(lost_log(dir, 1, min));
        }
        create_dir_durably(dir, options.sync)?;

        let mut firsts = Vec::new();
        let mut removed_tmp = false;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if let Some(first) = format::parse_file_name(&name) {
                firsts.push(first);
            } else if format::is_tmp_name(&name) {
                // A file whose creation never finished; it never held a record.
                std::fs::remove_file(entry.path())?;
                removed_tmp = true;
            }
        }
        if removed_tmp {
            writer::sync_dir(dir, options.sync)?;
        }
        firsts.sort_unstable();
        if let Some(&bad) = firsts.iter().find(|&&f| format::check_first(f).is_err()) {
            return Err(Error::corrupt(format!(
                "{}: the file name's sequence number is out of range",
                format::file_path(dir, bad).display()
            )));
        }

        // Files wholly below the caller's position are not needed, so they are
        // removed before anything is validated: a crash between a data-file
        // commit and the trim leaves them, and damage in them must not block
        // start-up. The newest file is never among them.
        if let Some(min) = options.min_next_lsn {
            let obsolete = firsts.windows(2).take_while(|w| w[1] <= min).count();
            remove_files_oldest_first(dir, &firsts[..obsolete])?;
            firsts.drain(..obsolete);
        }

        // A last file that cannot hold a record yet was being created when the
        // process died, on a filesystem that persists size before data; it is
        // removed: one shorter than a header, or exactly a header whose
        // contents are invalid.
        let mut removed_start = None;
        if let Some(&last) = firsts.last() {
            let path = format::file_path(dir, last);
            let len = std::fs::metadata(&path)?.len();
            if len < HEADER_LEN || (len == HEADER_LEN && format::read_header(&path)?.is_none()) {
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
            // scan_file guarantees every number it counted is below u64::MAX.
            next = Some(first + scan.records);
            if i + 1 == firsts.len() {
                if scan.good_end < scan.file_len {
                    if !options.discard_after_damage
                        && let Some(at) = format::find_valid_record_after(&path, scan.good_end)?
                    {
                        return Err(Error::corrupt(format!(
                            "{}: record {} at offset {} is bad ({}) but a valid record follows at offset {at}; \
                             the file is damaged, not torn, and was left unchanged \
                             (WalOptions::discard_after_damage cuts the log here and loses the records after it)",
                            path.display(),
                            first + scan.records,
                            scan.good_end,
                            scan.damage.as_deref().unwrap_or("unknown damage"),
                        )));
                    }
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

        let mut next_lsn = match (next, removed_start) {
            (Some(n), _) => n,
            (None, Some(start)) => start,
            (None, None) => 1,
        };
        let mut reset_to = None;
        let mut records_discarded = 0;
        if let Some(min) = options.min_next_lsn
            && min > next_lsn
        {
            if !options.allow_lost_log {
                return Err(lost_log(dir, next_lsn, min));
            }
            // The caller has already written everything below `min` into data
            // files, so nothing on disk is needed and none of it may keep
            // numbers below `min` in use.
            records_discarded = firsts.first().map_or(0, |&f| next_lsn - f);
            remove_files_oldest_first(dir, &firsts)?;
            firsts.clear();
            active = None;
            removed_start = Some(min);
            next_lsn = min;
            reset_to = Some(min);
        }

        let (file, file_first, file_size, file_records) = match active {
            Some((first, size, records)) => {
                let file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(format::file_path(dir, first))?;
                if options.sync == SyncMode::Data {
                    // What recovery reports must survive a power loss: the
                    // records may have been read from the page cache.
                    file.sync_data()?;
                    writer::sync_dir(dir, options.sync)?;
                }
                (file, first, size, records)
            }
            None => {
                let start = removed_start.unwrap_or(1);
                firsts.push(start);
                (
                    writer::create_file(dir, start, options.sync)?,
                    start,
                    HEADER_LEN,
                    0,
                )
            }
        };
        let recovery = Recovery {
            first_lsn: firsts[0],
            last_lsn: next_lsn - 1,
            files: firsts.len(),
            torn_bytes_discarded: torn,
            reset_to,
            records_discarded,
        };

        let shared = Arc::new(Shared {
            dir: dir.to_path_buf(),
            state: Mutex::new(State {
                next_lsn,
                durable_lsn: recovery.last_lsn,
                ..State::default()
            }),
            wake: Condvar::new(),
            files: Mutex::new(firsts),
        });
        let log_thread = writer::Writer {
            shared: Arc::clone(&shared),
            options,
            file,
            file_first,
            file_size,
            file_records,
            last_sync: None,
            buf: Vec::new(),
        };
        let thread = std::thread::Builder::new()
            .name("nwdb-wal".into())
            .spawn(move || log_thread.run())?;
        let wal = Wal {
            shared,
            thread: Some(thread),
            recovered_first: recovery.first_lsn,
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
    /// Returns [`Error::Corrupt`] instead of replaying less than asked for:
    /// when the log is not empty and `from` is below the first number still on
    /// disk (those records were trimmed), and when `from` is above the last
    /// number plus one (the log is behind what the caller believes was
    /// written). `from == last + 1` replays nothing and is fine. `from` of 0
    /// means the start of the log, like 1.
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
        let from = from.max(1);
        let (first, last) = (self.recovered_first, self.recovered_last);
        if last >= first && from < first {
            return Err(Error::corrupt(format!(
                "cannot replay from {from}: the log starts at {first}, earlier records were trimmed"
            )));
        }
        if from > last + 1 {
            return Err(Error::corrupt(format!(
                "cannot replay from {from}: the log ends at {last}, it is behind what was written"
            )));
        }
        let files = lock(&self.shared.files).clone();
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
    /// failed. Callbacks run in sequence-number order, and every accepted
    /// record's callback runs exactly once, with [`Error::Fatal`] if the log
    /// thread dies.
    ///
    /// If the record is too large ([`MAX_RECORD_BYTES`], [`Error::InvalidArgument`])
    /// or the log is already poisoned ([`Error::Fatal`]), `on_durable` runs
    /// immediately on the calling thread with the error and nothing is queued.
    ///
    /// # Calling rules
    ///
    /// - Callbacks run on the log thread (immediate rejections on the calling
    ///   thread). A callback must not block: it delays every acknowledgement
    ///   behind it. It must not call [`append_blocking`](Wal::append_blocking),
    ///   which would wait for the thread it is running on, and must not drop
    ///   the last handle to the log, which would join that thread from itself.
    ///   The caller must not hold a lock that the callback takes while calling
    ///   `append`, since a rejection runs the callback inside the call.
    ///   A panic in a callback is caught and ignored.
    /// - The queue is unbounded. A caller that must bound memory watches
    ///   [`WalStats::pending_bytes`] and holds back its own senders.
    /// - [`Error::Fatal`] means the record's outcome is unknown: it may or may
    ///   not be on disk, and it may reappear on replay after a restart.
    ///
    /// The record is copied once; [`append_owned`](Wal::append_owned) avoids that.
    pub fn append(&self, record: &[u8], on_durable: impl FnOnce(Result<Lsn>) + Send + 'static) {
        if record.len() > MAX_RECORD_BYTES {
            return on_durable(Err(too_large(record.len())));
        }
        self.enqueue(record.to_vec(), Box::new(on_durable));
    }

    /// Like [`append`](Wal::append) but takes ownership of the record, so it
    /// is not copied. The same calling rules apply.
    pub fn append_owned(
        &self,
        record: Vec<u8>,
        on_durable: impl FnOnce(Result<Lsn>) + Send + 'static,
    ) {
        if record.len() > MAX_RECORD_BYTES {
            return on_durable(Err(too_large(record.len())));
        }
        self.enqueue(record, Box::new(on_durable));
    }

    fn enqueue(&self, payload: Vec<u8>, done: Box<dyn FnOnce(Result<Lsn>) + Send>) {
        let mut st = lock(&self.shared.state);
        if let Some(msg) = &st.poisoned {
            let err = poison_error(msg);
            drop(st);
            return done(Err(err));
        }
        let lsn = st.next_lsn;
        st.next_lsn += 1;
        st.appended = true;
        st.pending_bytes += payload.len() as u64;
        st.queue.push(Item { lsn, payload, done });
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
            .unwrap_or_else(|_| Err(Error::fatal("the log stopped before acknowledging")))
    }

    /// Deletes every file all of whose records have sequence numbers below
    /// `lsn`, never the file currently being written, and returns how many
    /// files were removed.
    ///
    /// Files go oldest first and the directory is synced after each removal,
    /// so even a filesystem that does not preserve unlink order cannot leave a
    /// gap in the file sequence, and a failure part-way leaves a contiguous log.
    pub fn truncate_before(&self, lsn: Lsn) -> Result<u64> {
        let mut files = lock(&self.shared.files);
        // A file's records all lie below the next file's first number.
        let removable = files.windows(2).take_while(|w| w[1] <= lsn).count();
        let mut removed = 0;
        let mut outcome = Ok(());
        for &first in &files[..removable] {
            let step = std::fs::remove_file(format::file_path(&self.shared.dir, first))
                .map_err(Error::from)
                // Always a real directory sync, whatever the sync mode: a
                // caller that sees this return may rely on the removal.
                .and_then(|()| Ok(writer::sync_dir(&self.shared.dir, SyncMode::Data)?));
            match step {
                Ok(()) => removed += 1,
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        files.drain(..removed);
        drop(files);
        outcome?;
        Ok(removed as u64)
    }

    /// Starts a new log file at the current write position, so that the file
    /// holding the records written so far can be removed by
    /// [`truncate_before`](Wal::truncate_before) once they are in data files.
    /// Without it the newest file, which a trim never removes, would keep
    /// records that are long since in data files until it reaches its size
    /// limit.
    ///
    /// Returns `Ok(true)` if a file was started and `Ok(false)` if the newest
    /// file holds no records (nothing to do). The work is done by the log
    /// thread between two groups: everything it has written is synced first,
    /// the new file is created the way a rolled file is (header synced under a
    /// temporary name, renamed, directory synced), and the records still
    /// queued go to the new file. Numbering stays gapless and the newest file
    /// is never removed. A crash at any step leaves a log that opens: a
    /// leftover temporary file is removed by `open`, and a new file that holds
    /// only its header is simply the newest file.
    ///
    /// Waits for the log thread, so it takes as long as the group being
    /// written plus one file creation. A failure of the sync or of the file
    /// creation poisons the log like the same failure in a roll, and is
    /// returned as [`Error::Fatal`].
    pub fn rotate(&self) -> Result<bool> {
        let (tx, rx) = mpsc::channel();
        {
            let mut st = lock(&self.shared.state);
            if let Some(msg) = &st.poisoned {
                return Err(poison_error(msg));
            }
            st.rotations.push(tx);
        }
        self.shared.wake.notify_one();
        rx.recv()
            .unwrap_or_else(|_| Err(Error::fatal("the log stopped before it could rotate")))
    }

    /// Highest sequence number covered by a completed sync. Right after
    /// [`open`](Wal::open) it equals [`Recovery::last_lsn`].
    pub fn durable_lsn(&self) -> Lsn {
        lock(&self.shared.state).durable_lsn
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

/// The refusal for a log that ends below the caller's position. `from` is the
/// first record the log lacks, `below` the caller's `min_next_lsn`.
fn lost_log(dir: &Path, from: Lsn, below: Lsn) -> Error {
    Error::corrupt(format!(
        "the log in {} ends at record {} but records up to {} are already in data files, \
         so log files were lost: records {from}..={} are missing from the log (they are safe \
         in data files), and records {below} and above that only the lost files held are gone \
         (WalOptions::allow_lost_log starts a new log at {below} and accepts the loss)",
        dir.display(),
        from - 1,
        below - 1,
        below - 1,
    ))
}

fn too_large(len: usize) -> Error {
    Error::invalid_argument(format!(
        "record of {len} bytes exceeds the limit of {MAX_RECORD_BYTES}"
    ))
}

/// Creates `dir` and any missing parents, then syncs the parent of each
/// directory created so the new entries are durable.
fn create_dir_durably(dir: &Path, sync: SyncMode) -> Result<()> {
    let missing: Vec<&Path> = dir.ancestors().take_while(|p| !p.exists()).collect();
    std::fs::create_dir_all(dir)?;
    for created in missing {
        let parent = match created.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        // Best effort by construction: `sync_dir` does nothing where a
        // directory cannot be opened and synced (non-unix platforms).
        writer::sync_dir(parent, sync)?;
    }
    Ok(())
}

/// Removes log files oldest first, syncing the directory after each so a
/// crash cannot leave a gap in the sequence.
fn remove_files_oldest_first(dir: &Path, firsts: &[Lsn]) -> Result<()> {
    for &first in firsts {
        std::fs::remove_file(format::file_path(dir, first))?;
        writer::sync_dir(dir, SyncMode::Data)?;
    }
    Ok(())
}
