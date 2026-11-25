//! The log thread: the only code that writes to log files while the log is
//! open. It batches queued records, writes them, syncs once, then acknowledges.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use nwdb_core::{Error, Result};

use crate::format::{self, FRAME_LEN};
use crate::{Item, Lsn, Shared, SyncMode, WalOptions, lock, poison_error};

/// Creates a new log file holding only its header and makes it durable,
/// including its directory entry. The header is synced before any record can
/// be written after it, so a file that exists after a crash but has a short
/// header can only be one that never held a record.
pub(crate) fn create_file(dir: &Path, first: Lsn, mode: SyncMode) -> std::io::Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(format::file_path(dir, first))?;
    file.write_all(&format::encode_header(first))?;
    if mode == SyncMode::Data {
        file.sync_data()?;
    }
    sync_dir(dir, mode)?;
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

pub(crate) struct Writer {
    pub shared: Arc<Shared>,
    pub options: WalOptions,
    pub file: File,
    /// Size of the active file including bytes written but not yet synced.
    pub file_size: u64,
    pub file_records: u64,
    pub last_sync: Option<Instant>,
}

impl Writer {
    /// Thread body. Any exit other than a clean shutdown poisons the log so
    /// that no sender is left waiting for an acknowledgement.
    pub fn run(mut self) {
        let shared = Arc::clone(&self.shared);
        if catch_unwind(AssertUnwindSafe(|| self.run_loop())).is_err() {
            shared.poison("the log thread panicked".into(), Vec::new());
        }
    }

    fn run_loop(&mut self) {
        while let Some(batch) = self.next_batch() {
            match self.commit(&batch) {
                Ok(()) => {
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
                    self.shared.poison(e.to_string(), batch);
                    return;
                }
            }
        }
    }

    /// Waits for work and returns it, honouring the minimum spacing between
    /// syncs. Returns `None` once shutdown was requested and the queue is empty.
    fn next_batch(&mut self) -> Option<Vec<Item>> {
        let mut st = lock(&self.shared.state);
        loop {
            while st.queue.is_empty() && !st.shutdown {
                st = self.shared.wake.wait(st).unwrap_or_else(|e| e.into_inner());
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
            return Some(std::mem::take(&mut st.queue));
        }
    }

    fn commit(&mut self, batch: &[Item]) -> Result<()> {
        let mut buf = Vec::new();
        for item in batch {
            let frame = FRAME_LEN + item.payload.len() as u64;
            // A file always takes at least one record, so a record larger than
            // the limit ends up alone in its own file.
            if self.file_records > 0 && self.file_size + frame > self.options.max_file_bytes {
                self.file.write_all(&buf)?;
                buf.clear();
                self.roll(item.lsn)?;
            }
            format::encode_record(&item.payload, &mut buf);
            self.file_size += frame;
            self.file_records += 1;
        }
        self.file.write_all(&buf)?;
        self.sync_active()?;
        self.last_sync = Some(Instant::now());
        Ok(())
    }

    /// Finishes the active file and starts one whose first record is `first`.
    fn roll(&mut self, first: Lsn) -> Result<()> {
        // The old file must be durable before the new one is used, otherwise
        // a crash could leave a hole between two files.
        self.sync_active()?;
        let mut files = lock(&self.shared.files);
        self.file = create_file(&self.shared.dir, first, self.options.sync)?;
        files.push(first);
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
        let queued = {
            let mut st = lock(&self.state);
            st.poisoned.get_or_insert_with(|| msg.clone());
            st.pending_bytes = 0;
            std::mem::take(&mut st.queue)
        };
        for item in inflight.into_iter().chain(queued) {
            let err: Error = poison_error(&msg);
            let _ = catch_unwind(AssertUnwindSafe(|| (item.done)(Err(err))));
        }
    }
}
