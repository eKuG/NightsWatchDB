//! The log thread: the only code that writes to log files while the log is
//! open. It batches queued records, writes them, syncs once, then acknowledges.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::time::Instant;

use nwdb_core::{Error, Result};

use crate::format::{self, FRAME_LEN};
use crate::{Item, Lsn, Shared, SyncMode, WalOptions, lock, poison_error};

/// Frames are gathered into a buffer of about this size before each write;
/// larger payloads are written straight from the queue.
const WRITE_CHUNK: usize = 64 * 1024;

/// Creates a new log file holding only its header and makes it durable,
/// including its directory entry. The header is written and synced under a
/// temporary name and the file is renamed into place only afterwards, so a
/// file with a log name never exists with a missing or unsynced header.
pub(crate) fn create_file(dir: &Path, first: Lsn, mode: SyncMode) -> std::io::Result<File> {
    let tmp = format::tmp_file_path(dir, first);
    // A stale temporary file is never trusted; open removes them, this covers
    // one left by a failed attempt in this process.
    match std::fs::remove_file(&tmp) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&tmp)?;
    file.write_all(&format::encode_header(first))?;
    if mode == SyncMode::Data {
        file.sync_data()?;
    }
    let path = format::file_path(dir, first);
    if path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("{} already exists", path.display()),
        ));
    }
    std::fs::rename(&tmp, &path)?;
    if let Err(e) = sync_dir(dir, mode) {
        // Not durable, so not to be left in the directory under its name; if
        // this fails too the caller sees the file and treats it as damage.
        let _ = std::fs::remove_file(&path);
        return Err(e);
    }
    Ok(file)
}

/// Syncs a directory so that file creations and removals in it are durable.
pub(crate) fn sync_dir(dir: &Path, mode: SyncMode) -> std::io::Result<()> {
    if mode == SyncMode::None {
        return Ok(());
    }
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// What the log thread does next.
enum Work {
    Batch(Vec<Item>),
    Rotate(mpsc::Sender<Result<bool>>),
}

pub(crate) struct Writer {
    pub shared: Arc<Shared>,
    pub options: WalOptions,
    pub file: File,
    /// Sequence number of the active file's first record.
    pub file_first: Lsn,
    /// Size of the active file including bytes written but not yet synced.
    pub file_size: u64,
    pub file_records: u64,
    pub last_sync: Option<Instant>,
    /// Reused write buffer, at most about `WRITE_CHUNK` bytes.
    pub buf: Vec<u8>,
}

impl Writer {
    /// Thread body. Any exit other than a clean shutdown poisons the log so
    /// that no sender is left waiting for an acknowledgement, including the
    /// senders of the batch being written when a panic hit.
    pub fn run(mut self) {
        let shared = Arc::clone(&self.shared);
        let mut inflight = Vec::new();
        if catch_unwind(AssertUnwindSafe(|| self.run_loop(&mut inflight))).is_err() {
            shared.poison("the log thread panicked".into(), mem::take(&mut inflight));
        }
    }

    fn run_loop(&mut self, inflight: &mut Vec<Item>) {
        while let Some(work) = self.next_batch() {
            let batch = match work {
                Work::Rotate(reply) => {
                    match self.rotate() {
                        Ok(rotated) => {
                            let _ = reply.send(Ok(rotated));
                        }
                        Err((e, true)) => {
                            // A failed sync is a failed sync like any other:
                            // the log is poisoned.
                            let _ = reply.send(Err(poison_error(&e.to_string())));
                            self.shared.poison(e.to_string(), Vec::new());
                            return;
                        }
                        Err((e, false)) => {
                            // The new file could not be made. The active
                            // file is intact and still written to; the caller
                            // may ask again.
                            let _ = reply.send(Err(e));
                        }
                    }
                    continue;
                }
                Work::Batch(batch) => batch,
            };
            *inflight = batch;
            match self.commit(inflight) {
                Ok(()) => {
                    let batch = mem::take(inflight);
                    self.publish_stats(&batch);
                    for item in batch {
                        // A panicking callback must not take the log down
                        // with it; the log still owes everyone else an ack.
                        let lsn = item.lsn;
                        let _ = catch_unwind(AssertUnwindSafe(|| (item.done)(Ok(lsn))));
                    }
                }
                Err(e) => {
                    // After a failed write or sync the file's contents are
                    // unknown. Nothing is retried and nothing is acknowledged.
                    self.shared.poison(e.to_string(), mem::take(inflight));
                    return;
                }
            }
        }
    }

    /// Waits for work and returns it, honouring the minimum spacing between
    /// syncs. Returns `None` once shutdown was requested and the queue is empty.
    fn next_batch(&mut self) -> Option<Work> {
        let mut st = lock(&self.shared.state);
        loop {
            // A rotation goes between two groups, before the records that are
            // still queued; they are written to the new file.
            if !st.rotations.is_empty() {
                return Some(Work::Rotate(st.rotations.remove(0)));
            }
            while st.queue.is_empty() && !st.shutdown && st.rotations.is_empty() {
                st = self.shared.wake.wait(st).unwrap_or_else(|e| e.into_inner());
            }
            if !st.rotations.is_empty() {
                continue;
            }
            if st.queue.is_empty() {
                return None;
            }
            if !st.shutdown
                && let Some(t) = self.last_sync
            {
                let due = t + self.options.sync_interval;
                let now = Instant::now();
                if now < due {
                    // More records may arrive while we wait; they share the sync.
                    st = self
                        .shared
                        .wake
                        .wait_timeout(st, due - now)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                    continue;
                }
            }
            return Some(Work::Batch(std::mem::take(&mut st.queue)));
        }
    }

    fn commit(&mut self, batch: &[Item]) -> Result<()> {
        for item in batch {
            let frame = FRAME_LEN + item.payload.len() as u64;
            // A file always takes at least one record, so a record larger than
            // the limit ends up alone in its own file.
            if self.file_records > 0 && self.file_size + frame > self.options.max_file_bytes {
                self.flush_buf()?;
                self.roll(item.lsn)?;
            }
            format::encode_frame(&item.payload, &mut self.buf);
            if item.payload.len() >= WRITE_CHUNK {
                // Written from the queue as it is; no second copy.
                self.flush_buf()?;
                self.file.write_all(&item.payload)?;
            } else {
                self.buf.extend_from_slice(&item.payload);
                if self.buf.len() >= WRITE_CHUNK {
                    self.flush_buf()?;
                }
            }
            self.file_size += frame;
            self.file_records += 1;
        }
        self.flush_buf()?;
        self.sync_active()?;
        self.last_sync = Some(Instant::now());
        Ok(())
    }

    fn flush_buf(&mut self) -> std::io::Result<()> {
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// Starts a new file at the write position unless the active file holds no
    /// records. Everything in the active file has been synced by the group
    /// that wrote it; the sync is repeated before the new file exists.
    ///
    /// The error says whether the log must be poisoned. A failed sync must.
    /// A failure to create the new file does not, provided that no file with
    /// the new name is left behind (`create_file` removes the one it made if
    /// its last step fails): the active file is untouched and the next
    /// rotation starts again. If one is left, the files on disk no longer
    /// match what is being written, so it must.
    fn rotate(&mut self) -> std::result::Result<bool, (Error, bool)> {
        if self.file_records == 0 {
            return Ok(false);
        }
        self.sync_active().map_err(|e| (e, true))?;
        let first = self.file_first + self.file_records;
        match self.start_file(first) {
            Ok(()) => Ok(true),
            Err(e) => {
                let stuck = format::file_path(&self.shared.dir, first).exists();
                Err((e, stuck))
            }
        }
    }

    /// Finishes the active file and starts one whose first record is `first`.
    fn roll(&mut self, first: Lsn) -> Result<()> {
        // The old file must be durable before the new one is used, otherwise
        // a crash could leave a hole between two files.
        self.sync_active()?;
        self.start_file(first)
    }

    /// Creates the file whose first record is `first` and makes it the active
    /// one. On an error nothing has changed in memory.
    fn start_file(&mut self, first: Lsn) -> Result<()> {
        let mut files = lock(&self.shared.files);
        self.file = create_file(&self.shared.dir, first, self.options.sync)?;
        files.push(first);
        self.file_first = first;
        self.file_size = format::HEADER_LEN;
        self.file_records = 0;
        Ok(())
    }

    fn sync_active(&mut self) -> Result<()> {
        if let Some(hook) = &self.options.sync_hook {
            hook()?;
        }
        if self.options.sync == SyncMode::Data {
            self.file.sync_data()?;
        }
        lock(&self.shared.state).stats.syncs += 1;
        Ok(())
    }

    fn publish_stats(&self, batch: &[Item]) {
        let payload: u64 = batch.iter().map(|i| i.payload.len() as u64).sum();
        let mut st = lock(&self.shared.state);
        if let Some(last) = batch.last() {
            st.durable_lsn = last.lsn;
        }
        let n = batch.len() as u64;
        st.stats.records += n;
        st.stats.bytes += payload + n * FRAME_LEN;
        st.stats.groups += 1;
        st.stats.max_group = st.stats.max_group.max(n);
        st.pending_bytes -= payload;
    }
}

impl Shared {
    /// Marks the log failed and fails every record that has not been
    /// acknowledged: `inflight` (taken off the queue by the log thread) first,
    /// then whatever is still queued.
    pub(crate) fn poison(&self, msg: String, inflight: Vec<Item>) {
        let (queued, rotations) = {
            let mut st = lock(&self.state);
            st.poisoned.get_or_insert_with(|| msg.clone());
            st.pending_bytes = 0;
            (
                std::mem::take(&mut st.queue),
                std::mem::take(&mut st.rotations),
            )
        };
        for r in rotations {
            let _ = r.send(Err(poison_error(&msg)));
        }
        for item in inflight.into_iter().chain(queued) {
            let err: Error = poison_error(&msg);
            let _ = catch_unwind(AssertUnwindSafe(|| (item.done)(Err(err))));
        }
    }
}
