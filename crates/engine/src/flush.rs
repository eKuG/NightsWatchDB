//! Writing the buffer out as parts and publishing them.
//!
//! The order of the steps is what makes a crash at any point recoverable; the
//! README walks through each one.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, MutexGuard};

use arrow_array::RecordBatch;
use nwdb_core::{
    Error, PartMeta, Result, SpanBatchBuilder, SpanColumns, part_dir_name, window_start,
};
use nwdb_manifest::Edit;
use nwdb_segment::{PartReader, PartWriter, WriterOptions};

use crate::engine::{Inner, LivePart, Stage, run_hook};
use crate::lock;
use crate::rows::group_traces;
use crate::split::change_parts;

/// Parts written and committed by one flush, opened and not yet published.
/// Holds the publish gate, which is released when the parts are published.
struct Written<'a> {
    metas: Vec<PartMeta>,
    readers: Vec<PartReader>,
    gate: MutexGuard<'a, ()>,
}

impl Inner {
    /// The settings every part of this engine is written with. A flush and a
    /// merge both use it, so a merged part is written exactly as a flushed
    /// one: the same text cuts, the same sharing window, the same index
    /// budgets.
    pub(crate) fn writer_options(&self) -> WriterOptions {
        WriterOptions {
            splitter: self.opts.splitter.clone(),
            dedup_window_bytes: self.opts.dedup_window_bytes,
            text_index_budget_bytes: self.opts.text_index_budget_bytes,
            row_list_budget_bytes: self.opts.row_list_budget_bytes,
            max_pieces: self.opts.max_pieces,
            ..WriterOptions::default()
        }
    }

    /// Flushes the whole active buffer: one frozen buffer, one part per
    /// window, one manifest commit that also moves `wal_flushed_lsn`, and then
    /// (if `trim`) the log is trimmed.
    ///
    /// Flushes run one at a time. Returns `Fatal` after putting the engine in
    /// the fatal state when the log, the manifest or a part writer reports
    /// `Fatal`. After any other error the frozen buffer is merged back in
    /// front of the active one and nothing is lost.
    pub(crate) fn do_flush(&self, trim: bool) -> Result<()> {
        let _gate = lock(&self.flush_gate);
        // Checked with the gate held: shutdown holds it from its final flush
        // until the lock on the directory is released, so a flush that got
        // here after shutdown began must not write anything.
        {
            let c = lock(&self.core);
            if let Some(f) = &c.fatal {
                return Err(Error::fatal(f.clone()));
            }
            if c.closing {
                return Err(Error::unavailable("the engine is shut down"));
            }
        }
        self.flush_locked(trim)
    }

    /// [`do_flush`](Self::do_flush) for a caller that already holds the flush
    /// gate (shutdown, for its final flush).
    pub(crate) fn flush_locked(&self, trim: bool) -> Result<()> {
        let (frozen, lsn) = {
            let mut c = lock(&self.core);
            if let Some(f) = &c.fatal {
                return Err(Error::fatal(f.clone()));
            }
            if c.vis.frozen.is_some() {
                // Freezing again would overwrite the buffer whose data may
                // not be in a part, and the next commit would then cover
                // records that are in neither a part nor the log.
                let msg = "a frozen buffer is still present at the start of a flush".to_string();
                self.set_fatal_locked(&mut c, msg.clone());
                return Err(Error::fatal(msg));
            }
            if c.vis.active.is_empty() && c.applied_lsn == c.flushed_lsn {
                drop(c);
                return self.rotate_and_trim_log();
            }
            // Everything with a log position up to `applied_lsn` is in the
            // active buffer or in parts, because requests enter the buffer in
            // log order; the freeze happens under the same lock as an insert.
            let frozen = std::mem::take(&mut c.vis.active);
            c.vis.frozen = Some(frozen.clone());
            c.frozen_bytes = std::mem::take(&mut c.active_bytes);
            c.active_oldest = None;
            c.flushing = true;
            (frozen, c.applied_lsn)
        };

        // A panic after the freeze must not unwind past this point with the
        // frozen buffer in place: it is a fatal error, and a restart recovers
        // from the log.
        let outcome = catch_unwind(AssertUnwindSafe(|| self.write_and_commit(&frozen, lsn)))
            .unwrap_or_else(|panic| {
                Err(Error::fatal(format!(
                    "a flush panicked: {}",
                    panic_message(panic.as_ref())
                )))
            });
        match outcome {
            Ok(written) => {
                // The publish gate is held since before the commit, so no
                // replacement changed the list since: what is read here is
                // what the new list extends. The split map is built outside
                // the core lock and published with the list.
                let Written {
                    metas,
                    readers,
                    gate,
                } = written;
                let (old_parts, old_split) = {
                    let c = lock(&self.core);
                    (c.vis.parts.clone(), c.vis.split.clone())
                };
                let added = metas
                    .into_iter()
                    .zip(readers)
                    .map(|(meta, reader)| Arc::new(LivePart::new(meta, reader)))
                    .collect();
                let (parts, split) = change_parts(&old_parts, &old_split, &[], added);
                let split = Arc::new(split);
                {
                    let mut c = lock(&self.core);
                    // Parts and buffer change in one step, so a snapshot sees
                    // the data exactly once.
                    c.vis.parts = Arc::new(parts);
                    c.vis.split = split;
                    c.vis.frozen = None;
                    c.frozen_bytes = 0;
                    c.flushed_lsn = lsn;
                    c.flushes += 1;
                    c.flushing = false;
                    // Expired parts may be waiting for the next pass.
                    c.maintenance_due = true;
                    self.maintenance_cv.notify_all();
                }
                drop(gate);
                self.stage_after_commit(Stage::Published);
                if trim {
                    let rotated = self.rotate_and_trim_log();
                    self.stage_after_commit(Stage::Trimmed);
                    return rotated;
                }
                Ok(())
            }
            Err(e) => {
                let mut c = lock(&self.core);
                c.flushing = false;
                c.last_error = Some(e.to_string());
                if matches!(e, Error::Fatal(_)) {
                    // The frozen buffer stays visible: the data is in it, and
                    // may or may not be in a part.
                    c.fatal.get_or_insert(e.to_string());
                    self.flusher_cv.notify_all();
                } else {
                    // Older data first, then what arrived while writing.
                    let mut batches: Vec<Arc<RecordBatch>> = frozen.as_ref().clone();
                    batches.extend(c.vis.active.iter().cloned());
                    // The age clock restarts; the flusher's retry delay keeps
                    // a persistent failure from spinning.
                    c.active_oldest.get_or_insert_with(std::time::Instant::now);
                    c.vis.active = Arc::new(batches);
                    c.vis.frozen = None;
                    c.active_bytes += std::mem::take(&mut c.frozen_bytes);
                }
                Err(e)
            }
        }
    }

    /// Calls the stage hook at a point after the manifest commit, where its
    /// result no longer matters: the flush has happened.
    pub(crate) fn stage_after_commit(&self, stage: Stage) {
        let _ = catch_unwind(AssertUnwindSafe(|| run_hook(&self.opts.stage_hook, stage)));
    }

    /// Starts a new log file and then removes the log files that are entirely
    /// inside parts. The trim never removes the newest file, so without the
    /// new file the records of this flush would stay on disk until that file
    /// reached its size limit.
    ///
    /// The data is in parts either way. If the new file cannot be created the
    /// log keeps writing to the current one: the error is recorded and
    /// returned, and the next flush tries again. If syncing the current file
    /// fails the log is poisoned: the engine is put in the fatal state at
    /// once and the error is returned. A failed trim is only recorded, and
    /// the next flush tries it again.
    fn rotate_and_trim_log(&self) -> Result<()> {
        let flushed = lock(&self.core).flushed_lsn;
        let wal = self.wal.read().unwrap_or_else(|e| e.into_inner());
        let Some(wal) = wal.as_ref() else {
            return Ok(());
        };
        let mut rotated = Ok(());
        match wal.rotate() {
            Ok(_) => self.stage_after_commit(Stage::LogRotated),
            Err(e) => {
                let mut c = lock(&self.core);
                c.last_error = Some(format!("rotating the log failed: {e}"));
                if matches!(e, Error::Fatal(_)) {
                    self.set_fatal_locked(&mut c, e.to_string());
                    return Err(e);
                }
                rotated = Err(e);
            }
        }
        if let Err(e) = wal.truncate_before(flushed + 1) {
            lock(&self.core).last_error = Some(format!("trimming the log failed: {e}"));
        }
        rotated
    }

    fn write_and_commit(&self, frozen: &[Arc<RecordBatch>], lsn: u64) -> Result<Written<'_>> {
        run_hook(&self.opts.stage_hook, Stage::FlushFrozen)?;
        let cols = frozen
            .iter()
            .map(|b| SpanColumns::try_new(b))
            .collect::<Result<Vec<_>>>()?;
        let grouped = group_traces(&cols);
        let window_ns = self.opts.window_ns;

        // Traces are ordered by (earliest start, trace id), so those of one
        // window are adjacent.
        let mut windows: Vec<(i64, std::ops::Range<usize>)> = Vec::new();
        for (i, t) in grouped.traces.iter().enumerate() {
            let w = window_start(t.start_ns, window_ns);
            match windows.last_mut() {
                Some((last, range)) if *last == w => range.end = i + 1,
                _ => windows.push((w, i..i + 1)),
            }
        }

        // Ids come from the manifest and are never reused, even by a retry
        // after an error.
        let ids = self.manifest.reserve_part_ids(windows.len() as u64)?;
        let mut metas: Vec<PartMeta> = Vec::new();
        let mut readers: Vec<PartReader> = Vec::new();
        let mut gate = None;
        let result: Result<()> = (|| {
            for ((w, range), id) in windows.iter().zip(ids) {
                let mut writer =
                    PartWriter::create(&self.parts_dir, id, *w, window_ns, self.writer_options())?;
                let mut builder = SpanBatchBuilder::new(self.opts.batch_limits);
                for t in &grouped.traces[range.clone()] {
                    for e in &grouped.entries[t.from..t.to] {
                        builder.append_from(
                            &cols[e.batch as usize],
                            e.batch as usize,
                            e.row as usize,
                        );
                        if builder.is_full() {
                            writer.write(&builder.finish())?;
                        }
                    }
                }
                if !builder.is_empty() {
                    writer.write(&builder.finish())?;
                }
                let meta = writer.finish()?;
                // Recorded before the open so a failure still cleans it up.
                metas.push(meta);
                readers.push(PartReader::open(self.parts_dir.join(part_dir_name(id)))?);
                run_hook(&self.opts.stage_hook, Stage::PartWritten)?;
            }
            run_hook(&self.opts.stage_hook, Stage::AllPartsWritten)?;
            // From here to the publish no replacement may commit: its edit and
            // ours must each see the list the other left. Taken only now, so
            // the long work above never holds it.
            gate = Some(lock(&self.publish_gate));
            self.manifest.commit(Edit {
                add: metas.clone(),
                wal_flushed_lsn: Some(lsn),
                ..Edit::default()
            })?;
            self.stage_after_commit(Stage::Committed);
            Ok(())
        })();
        match result {
            Ok(()) => Ok(Written {
                metas,
                readers,
                gate: gate.expect("the publish gate is taken before the commit"),
            }),
            Err(e) => {
                // Cleaning up is long work, which the gate does not cover.
                drop(gate.take());
                if !matches!(e, Error::Fatal(_)) {
                    // Nothing was published, so the directories are garbage.
                    // If this fails they are removed at the next start-up.
                    for m in &metas {
                        let _ =
                            std::fs::remove_dir_all(self.parts_dir.join(part_dir_name(m.part_id)));
                    }
                }
                Err(e)
            }
        }
    }
}

/// The text of a panic payload, for the error message.
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}
