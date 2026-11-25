//! Writing the buffer out as parts and publishing them.
//!
//! The order of the steps is what makes a crash at any point recoverable; the
//! README walks through each one.

use std::sync::Arc;

use arrow_array::RecordBatch;
use nwdb_core::{
    Error, PartMeta, Result, SpanBatchBuilder, SpanColumns, part_dir_name, window_start,
};
use nwdb_manifest::Edit;
use nwdb_segment::{PartReader, PartWriter, WriterOptions};

use crate::engine::{Inner, LivePart};
use crate::lock;
use crate::rows::group_traces;

/// Parts written by one flush, opened and not yet published.
struct Written {
    metas: Vec<PartMeta>,
    readers: Vec<PartReader>,
}

impl Inner {
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

        let (frozen, lsn) = {
            let mut c = lock(&self.core);
            if let Some(f) = &c.fatal {
                return Err(Error::fatal(f.clone()));
            }
            if c.vis.active.is_empty() && c.applied_lsn == c.flushed_lsn {
                drop(c);
                self.trim_log();
                return Ok(());
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

        match self.write_and_commit(&frozen, lsn) {
            Ok(written) => {
                {
                    let mut c = lock(&self.core);
                    let mut parts: Vec<Arc<LivePart>> = c.vis.parts.as_ref().clone();
                    parts.extend(
                        written
                            .metas
                            .into_iter()
                            .zip(written.readers)
                            .map(|(meta, reader)| Arc::new(LivePart { meta, reader })),
                    );
                    // Parts and buffer change in one step, so a snapshot sees
                    // the data exactly once.
                    c.vis.parts = Arc::new(parts);
                    c.vis.frozen = None;
                    c.frozen_bytes = 0;
                    c.flushed_lsn = lsn;
                    c.flushes += 1;
                    c.flushing = false;
                }
                if trim {
                    self.trim_log();
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

    /// Removes log files that are entirely inside parts. A failure is only
    /// noted: the data is safe and the next flush tries again.
    fn trim_log(&self) {
        let flushed = lock(&self.core).flushed_lsn;
        let wal = self.wal.read().unwrap_or_else(|e| e.into_inner());
        if let Some(wal) = wal.as_ref()
            && let Err(e) = wal.truncate_before(flushed + 1)
        {
            lock(&self.core).last_error = Some(format!("trimming the log failed: {e}"));
        }
    }

    fn write_and_commit(&self, frozen: &[Arc<RecordBatch>], lsn: u64) -> Result<Written> {
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
        let result: Result<()> = (|| {
            for ((w, range), id) in windows.iter().zip(ids) {
                let mut writer = PartWriter::create(
                    &self.parts_dir,
                    id,
                    *w,
                    window_ns,
                    WriterOptions::default(),
                )?;
                let mut builder = SpanBatchBuilder::new(self.opts.batch_limits);
                for t in &grouped.traces[range.clone()] {
                    for e in &grouped.entries[t.from..t.to] {
                        builder.append(&cols[e.batch as usize].record(e.row as usize));
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
            }
            self.manifest.commit(Edit {
                add: metas.clone(),
                wal_flushed_lsn: Some(lsn),
                ..Edit::default()
            })?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(Written { metas, readers }),
            Err(e) => {
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
