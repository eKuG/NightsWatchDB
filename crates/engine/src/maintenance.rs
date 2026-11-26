//! Taking parts away: the replace operation, the maintenance thread and
//! retention.
//!
//! Parts are immutable and only ever added by a flush. Everything that removes
//! them (retention now, merging later) goes through [`Inner::replace_parts`],
//! which removes some live parts and adds zero or more new ones in one step.
//! The README walks through what a crash at each point leaves.

use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nwdb_core::{Error, Result, part_dir_name};
use nwdb_manifest::Edit;

use crate::engine::{Inner, LivePart, Stage, sync_dir};
use crate::flush::panic_message;
use crate::lock;
use crate::split::change_parts;

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
            let _ = self.run_pass();
        }
    }

    /// One maintenance pass, one at a time. The outcome is also recorded in
    /// the statistics.
    pub(crate) fn run_pass(&self) -> Result<()> {
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
            catch_unwind(AssertUnwindSafe(|| self.pass_steps())).unwrap_or_else(|panic| {
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
    fn pass_steps(&self) -> Result<()> {
        let mut result = self.retry_deletions();
        let mut then = |r: Result<()>| {
            if result.is_ok() {
                result = r;
            }
        };
        if !self.maintenance_stopping() {
            then(self.apply_retention());
        }
        if !self.maintenance_stopping() {
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

    /// The place where merging will go: choose parts that are worth merging
    /// (for example several small parts of one window), write the merged part
    /// with the merge function of `nwdb-segment`, reserve its id with
    /// `self.manifest.reserve_part_ids`, open it, and publish it with
    /// [`replace_parts`](Self::replace_parts), removing the parts it replaces.
    /// Nothing is merged yet.
    fn choose_and_merge_parts(&self) -> Result<()> {
        Ok(())
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
