//! Taking parts away: the replace operation, the maintenance thread,
//! retention and compaction.
//!
//! Parts are immutable and only ever added by a flush. Everything that removes
//! them (retention and merging) goes through [`Inner::replace_parts`], which
//! removes some live parts and adds zero or more new ones in one step. The
//! README walks through what a crash at each point leaves.

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nwdb_core::{Error, Result, part_dir_name};
use nwdb_manifest::Edit;
use nwdb_segment::{MergeOptions, PartReader, WriterOptions, merge_parts};

use crate::compaction::{MergePlan, PartInfo, WindowKey, choose_merge};
use crate::engine::{Inner, LivePart, Stage, run_hook, sync_dir};
use crate::flush::panic_message;
use crate::lock;
use crate::split::change_parts;

/// What compaction has done and what it is leaving alone, under the core lock.
#[derive(Default)]
pub(crate) struct CompactionState {
    pub merges_done: u64,
    pub parts_merged_away: u64,
    pub duplicates_dropped: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub merging: bool,
    pub last_error: Option<String>,
    /// Windows whose merge failed: how many times in a row, and when to try
    /// again. Other windows are not held back by them.
    failing: HashMap<WindowKey, (u32, Instant)>,
    /// Windows with a damaged input: left alone until a restart.
    damaged: HashSet<WindowKey>,
}

/// Sets the "a merge is running" flag for as long as it lives, also when the
/// merge panics.
struct Merging<'a>(&'a Inner);

impl<'a> Merging<'a> {
    fn new(inner: &'a Inner) -> Self {
        lock(&inner.core).compaction.merging = true;
        Merging(inner)
    }
}

impl Drop for Merging<'_> {
    fn drop(&mut self) {
        lock(&self.0.core).compaction.merging = false;
    }
}

/// What a replacement did after its commit.
#[derive(Debug, Default)]
pub(crate) struct Replaced {
    /// Why directories of removed parts could not be deleted. The parts are
    /// gone from the manifest and from queries either way; the next pass
    /// retries, and a restart deletes them too.
    pub deletion_errors: Vec<String>,
}

impl Replaced {
    fn into_result(self) -> Result<()> {
        match self.deletion_errors.into_iter().next() {
            None => Ok(()),
            Some(first) => Err(Error::other(format!(
                "parts were removed but a directory could not be deleted (the next pass retries): {first}"
            ))),
        }
    }
}

impl Inner {
    /// Replaces the live parts `remove` by the parts `add`, which are already
    /// written to `parts/` (each with its metadata and an open reader, ids
    /// reserved from the manifest). In this order:
    ///
    /// 1. one manifest commit that removes and adds them;
    /// 2. the new part list and a split map that matches it are published in
    ///    one step under the core lock, so a snapshot never sees a list and a
    ///    map that disagree, nor a removed part and its replacement both
    ///    present or both absent;
    /// 3. the directories of the removed parts are deleted and `parts/` is
    ///    synced.
    ///
    /// Steps 1 and 2 hold the publish gate, which a flush also holds from its
    /// commit to its publish; nothing is held while directories are deleted.
    ///
    /// An error before the commit changes nothing: `InvalidArgument` if an id
    /// in `remove` is not live (or is listed twice), anything else from the
    /// manifest as it is. The new parts are then not live, and their
    /// directories stay the caller's to delete (a restart removes them
    /// otherwise). `Fatal` from the manifest puts the engine in the fatal
    /// state, as in a flush. After the commit the replacement is done: a
    /// failure to delete a directory is reported in the result and retried by
    /// the next pass. Deletion stops early, leaving the rest for the next
    /// pass or restart, when shutdown asks maintenance to stop.
    ///
    /// Queries on snapshots taken before the replacement keep reading the
    /// removed parts from their open files.
    pub(crate) fn replace_parts(&self, remove: &[u64], add: Vec<LivePart>) -> Result<Replaced> {
        let dirs: Vec<PathBuf> = remove
            .iter()
            .map(|&id| self.parts_dir.join(part_dir_name(id)))
            .collect();
        {
            let gate = lock(&self.publish_gate);
            let (old_parts, old_split) = {
                let c = lock(&self.core);
                if let Some(f) = &c.fatal {
                    return Err(Error::fatal(f.clone()));
                }
                (c.vis.parts.clone(), c.vis.split.clone())
            };
            let live: HashSet<u64> = old_parts.iter().map(|p| p.meta.part_id).collect();
            let mut seen = HashSet::new();
            for id in remove {
                if !live.contains(id) || !seen.insert(*id) {
                    return Err(Error::invalid_argument(format!(
                        "cannot remove part {id}: it is not live or is listed twice"
                    )));
                }
            }
            if remove.is_empty() && add.is_empty() {
                return Ok(Replaced::default());
            }
            // The last point where nothing has changed yet.
            if let Err(e) = run_hook(&self.opts.stage_hook, Stage::BeforeReplaceCommit) {
                if matches!(e, Error::Fatal(_)) {
                    let mut c = lock(&self.core);
                    self.set_fatal_locked(&mut c, e.to_string());
                }
                return Err(e);
            }

            // 1. The commit.
            let edit = Edit {
                add: add.iter().map(|p| p.meta.clone()).collect(),
                remove: remove.to_vec(),
                ..Edit::default()
            };
            if let Err(e) = self.manifest.commit(edit) {
                if matches!(e, Error::Fatal(_)) {
                    let mut c = lock(&self.core);
                    self.set_fatal_locked(&mut c, e.to_string());
                }
                return Err(e);
            }
            self.stage_after_commit(Stage::ReplaceCommitted);

            // 2. The publish. The map is built outside the core lock and
            // costs what `change_parts` says.
            let add = add.into_iter().map(Arc::new).collect();
            let (parts, split) = change_parts(&old_parts, &old_split, remove, add);
            {
                let mut c = lock(&self.core);
                c.vis.parts = Arc::new(parts);
                c.vis.split = Arc::new(split);
            }
            drop(gate);
        }
        self.stage_after_commit(Stage::ReplacePublished);

        // 3. The deletion.
        Ok(self.delete_dirs(dirs))
    }

    /// Deletes directories of parts that are no longer in the manifest, one at
    /// a time, then syncs `parts/`. One that cannot be deleted is kept for the
    /// next pass; so are the ones not reached because shutdown asked
    /// maintenance to stop.
    fn delete_dirs(&self, dirs: Vec<PathBuf>) -> Replaced {
        let mut out = Replaced::default();
        let mut left = dirs.into_iter();
        let mut deleted_any = false;
        for dir in left.by_ref() {
            if lock(&self.core).stop_maintenance {
                lock(&self.undeleted).push(dir);
                break;
            }
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => deleted_any = true,
                // Already gone: nothing to do.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    out.deletion_errors
                        .push(format!("deleting {}: {e}", dir.display()));
                    lock(&self.undeleted).push(dir);
                    continue;
                }
            }
            self.stage_after_commit(Stage::PartDirDeleted);
        }
        lock(&self.undeleted).extend(left);
        if deleted_any {
            sync_dir(&self.parts_dir);
        }
        out
    }

    /// Wakes every `maintenance_interval` and after every flush, and runs one
    /// pass.
    pub(crate) fn maintenance_loop(self: Arc<Self>) {
        loop {
            {
                let mut c = lock(&self.core);
                let deadline = Instant::now() + self.opts.maintenance_interval;
                loop {
                    if c.stop_maintenance {
                        return;
                    }
                    if c.maintenance_due {
                        c.maintenance_due = false;
                        break;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    c = self
                        .maintenance_cv
                        .wait_timeout(c, deadline - now)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
            }
            // The outcome is in the statistics; there is nobody to return it
            // to, and the next pass tries again.
            let _ = self.run_pass(self.opts.compaction.background);
        }
    }

    /// One maintenance pass, one at a time. The outcome is also recorded in
    /// the statistics.
    pub(crate) fn run_pass(&self, merge: bool) -> Result<()> {
        let _pass = lock(&self.maintenance_gate);
        {
            let c = lock(&self.core);
            if c.stop_maintenance {
                return Err(Error::unavailable("the engine is shut down"));
            }
            if let Some(f) = &c.fatal {
                return Err(Error::fatal(f.clone()));
            }
        }
        let outcome =
            catch_unwind(AssertUnwindSafe(|| self.pass_steps(merge))).unwrap_or_else(|panic| {
                // Whether the panic fell between a commit and its publish
                // cannot be told, and a manifest ahead of the visible parts
                // must not be built on: a restart recovers.
                let msg = format!(
                    "a maintenance pass panicked: {}",
                    panic_message(panic.as_ref())
                );
                let mut c = lock(&self.core);
                self.set_fatal_locked(&mut c, msg.clone());
                Err(Error::fatal(msg))
            });
        let mut c = lock(&self.core);
        c.maintenance_passes += 1;
        if let Err(e) = &outcome {
            c.last_maintenance_error = Some(e.to_string());
        }
        outcome
    }

    /// True once shutdown has asked maintenance to stop. Checked between the
    /// steps of a pass, so shutdown waits for the step in progress only.
    fn maintenance_stopping(&self) -> bool {
        lock(&self.core).stop_maintenance
    }

    /// The steps of a pass. A step that fails does not keep the later ones
    /// from running; the first error is the pass's.
    fn pass_steps(&self, merge: bool) -> Result<()> {
        let mut result = self.retry_deletions();
        let mut then = |r: Result<()>| {
            if result.is_ok() {
                result = r;
            }
        };
        if !self.maintenance_stopping() {
            then(self.apply_retention());
        }
        if merge && !self.maintenance_stopping() {
            then(self.choose_and_merge_parts());
        }
        result
    }

    /// Tries again the directories earlier passes could not delete.
    fn retry_deletions(&self) -> Result<()> {
        let dirs = std::mem::take(&mut *lock(&self.undeleted));
        if dirs.is_empty() {
            return Ok(());
        }
        self.delete_dirs(dirs).into_result()
    }

    /// Removes every part whose whole window is older than the retention, in
    /// one replacement.
    fn apply_retention(&self) -> Result<()> {
        let Some(retention) = self.opts.retention else {
            return Ok(());
        };
        // Wide arithmetic: a window near the end of the range, a huge
        // retention or an odd clock must not overflow.
        let keep = i128::try_from(retention.as_nanos()).unwrap_or(i128::MAX);
        let cutoff = i128::from(self.now_ns()).saturating_sub(keep);
        let parts = lock(&self.core).vis.parts.clone();
        let expired: Vec<u64> = parts
            .iter()
            .filter(|p| i128::from(p.meta.window_start_ns) + i128::from(p.meta.window_ns) <= cutoff)
            .map(|p| p.meta.part_id)
            .collect();
        if expired.is_empty() {
            return Ok(());
        }
        let replaced = self.replace_parts(&expired, Vec::new())?;
        lock(&self.core).retention_removed += expired.len() as u64;
        replaced.into_result()
    }

    /// Merges parts while the policy finds something to merge, one merge at a
    /// time, and stops between merges when shutdown asks. A merge that is
    /// running when shutdown asks is finished: `merge_parts` cannot be
    /// interrupted, and what it wrote costs only a commit to keep (the README
    /// has the bound this puts on shutdown).
    ///
    /// A merge that fails is recorded and its window is left alone for a
    /// while (until a restart if the cause is damage in an input); the other
    /// windows go on. The first error is the step's result.
    fn choose_and_merge_parts(&self) -> Result<()> {
        if !self.opts.compaction.enabled {
            return Ok(());
        }
        let mut first_error = None;
        // A window that failed is left alone for the rest of the pass, so a
        // failure that persists ends the pass instead of repeating in it,
        // whatever the backoff.
        let mut failed_now = HashSet::new();
        while !self.maintenance_stopping() {
            let parts = lock(&self.core).vis.parts.clone();
            let Some(plan) = self.next_merge(&parts, &failed_now) else {
                break;
            };
            match self.merge_one(&parts, &plan) {
                Ok(replaced) => {
                    if let Err(e) = replaced.into_result() {
                        first_error.get_or_insert(e);
                    }
                }
                Err(e) => {
                    failed_now.insert((plan.window_start_ns, plan.window_ns));
                    self.note_merge_failure(&plan, &e);
                    if matches!(e, Error::Fatal(_)) {
                        return Err(e);
                    }
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// The policy's choice for the parts that are live now, leaving out the
    /// windows that are backing off or damaged.
    fn next_merge(
        &self,
        parts: &[Arc<LivePart>],
        failed_now: &HashSet<WindowKey>,
    ) -> Option<MergePlan> {
        let infos: Vec<PartInfo> = parts
            .iter()
            .map(|p| PartInfo {
                id: p.meta.part_id,
                window_start_ns: p.meta.window_start_ns,
                window_ns: p.meta.window_ns,
                size_bytes: p.meta.size_bytes(),
                level: p.meta.level,
                live_for: p.live_since.elapsed(),
            })
            .collect();
        let now = Instant::now();
        let (damaged, waiting) = {
            let mut c = lock(&self.core);
            let live: HashSet<WindowKey> = infos
                .iter()
                .map(|p| (p.window_start_ns, p.window_ns))
                .collect();
            // A window that is gone (expired) has nothing to remember.
            c.compaction.failing.retain(|k, _| live.contains(k));
            let waiting: HashSet<WindowKey> = c
                .compaction
                .failing
                .iter()
                .filter(|(_, (_, retry_at))| *retry_at > now)
                .map(|(k, _)| *k)
                .collect();
            (c.compaction.damaged.clone(), waiting)
        };
        choose_merge(&self.opts.compaction, &infos, &|start, width| {
            let key = (start, width);
            damaged.contains(&key) || waiting.contains(&key) || failed_now.contains(&key)
        })
    }

    /// Merges the parts of `plan` into one and replaces them by it. On any
    /// error before the commit the database is as it was and the new
    /// directory is gone (except after `Fatal`, where whether the commit
    /// happened is not known and a restart decides).
    fn merge_one(&self, live: &[Arc<LivePart>], plan: &MergePlan) -> Result<Replaced> {
        // Oldest first: ids grow with time.
        let mut inputs: Vec<&Arc<LivePart>> = live
            .iter()
            .filter(|p| plan.parts.contains(&p.meta.part_id))
            .collect();
        inputs.sort_by_key(|p| p.meta.part_id);
        if inputs.len() != plan.parts.len() {
            return Err(Error::other("a part chosen for a merge is not live"));
        }
        let _busy = Merging::new(self);
        let id = self.manifest.reserve_part_ids(1)?.start;
        let dir = self.parts_dir.join(part_dir_name(id));
        let level = inputs
            .iter()
            .map(|p| p.meta.level)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let readers: Vec<&PartReader> = inputs.iter().map(|p| &p.reader).collect();
        // The same writer settings a flush uses.
        let options = MergeOptions {
            writer: WriterOptions {
                splitter: self.opts.splitter.clone(),
                dedup_window_bytes: self.opts.dedup_window_bytes,
                ..WriterOptions::default()
            },
            level,
            ..MergeOptions::default()
        };
        let (meta, stats) = merge_parts(
            &readers,
            &self.parts_dir,
            id,
            plan.window_start_ns,
            plan.window_ns,
            options,
        )?;

        // From here the output exists on disk. Until the commit an error
        // removes it.
        let abandon = |e: Error| {
            if !matches!(e, Error::Fatal(_)) {
                let _ = std::fs::remove_dir_all(&dir);
            }
            e
        };
        let reader = PartReader::open(&dir).map_err(abandon)?;
        let output_bytes = meta.size_bytes();
        let part = LivePart::new(meta, reader);
        run_hook(&self.opts.stage_hook, Stage::MergeWritten).map_err(abandon)?;
        let remove: Vec<u64> = inputs.iter().map(|p| p.meta.part_id).collect();
        let replaced = self.replace_parts(&remove, vec![part]).map_err(abandon)?;

        let input_bytes: u64 = inputs.iter().map(|p| p.meta.size_bytes()).sum();
        let mut c = lock(&self.core);
        c.compaction.merges_done += 1;
        c.compaction.parts_merged_away += remove.len() as u64;
        c.compaction.duplicates_dropped += stats.duplicates_dropped;
        c.compaction.input_bytes += input_bytes;
        c.compaction.output_bytes += output_bytes;
        c.compaction
            .failing
            .remove(&(plan.window_start_ns, plan.window_ns));
        Ok(replaced)
    }

    /// Records a failed merge in the statistics and decides when its window
    /// is tried again: not before a restart for damage, otherwise after a
    /// backoff that doubles with every failure in a row. A `Fatal` error also
    /// puts the engine in the fatal state.
    fn note_merge_failure(&self, plan: &MergePlan, e: &Error) {
        let key = (plan.window_start_ns, plan.window_ns);
        let mut c = lock(&self.core);
        let damaged = matches!(e, Error::Corrupt(_));
        c.compaction.last_error = Some(format!(
            "merging parts {:?} of the window starting at {}: {e}{}",
            plan.parts,
            plan.window_start_ns,
            if damaged {
                " (damaged data: this window is left alone until the next start)"
            } else {
                ""
            }
        ));
        if matches!(e, Error::Fatal(_)) {
            self.set_fatal_locked(&mut c, e.to_string());
        } else if damaged {
            c.compaction.damaged.insert(key);
        } else {
            let n = c.compaction.failing.get(&key).map_or(0, |(n, _)| *n) + 1;
            let factor = 1u32 << (n - 1).min(6);
            let delay = self.opts.compaction.retry_backoff * factor;
            c.compaction
                .failing
                .insert(key, (n, Instant::now() + delay));
        }
    }

    fn now_ns(&self) -> i64 {
        match &self.opts.clock {
            Some(clock) => clock(),
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)),
        }
    }
}
