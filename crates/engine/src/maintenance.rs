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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nwdb_core::{Error, PartKind, Result, part_dir_name};
use nwdb_manifest::Edit;
use nwdb_segment::{
    AnnotationReader, MergeOptions, PartReader, merge_annotation_parts, merge_parts,
};

use crate::compaction::{MergeKey, MergePlan, PartInfo, Rule, choose_merge};
use crate::engine::{
    Inner, LiveAnnotationPart, LivePart, Stage, annotation_windows, run_hook, sync_dir,
};
use crate::flush::panic_message;
use crate::lock;
use crate::split::change_parts;

/// What compaction has done and what it is leaving alone, under the core lock.
#[derive(Default)]
pub(crate) struct CompactionState {
    pub merges_done: u64,
    pub parts_merged_away: u64,
    pub duplicates_dropped: u64,
    pub annotations_superseded: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub merging: bool,
    pub last_error: Option<String>,
    /// Windows whose merge failed, per kind of part: how many times in a row,
    /// and when to try again (`None`: a time that cannot be represented, so
    /// not by the clock). Other windows, and the other kind of the same
    /// window, are not held back by them.
    failing: HashMap<MergeKey, (u32, Option<Instant>)>,
    /// Windows with a damaged input, per kind: left alone until a restart.
    damaged: HashSet<MergeKey>,
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

/// The error of an annotation merge that found damage, with the damaged input
/// named (the merge itself reports the file, not the part). Other errors pass
/// through. Looks only on this error path, so a healthy merge verifies each
/// input once, and not at all once `stop` is set: the error is not recorded
/// then, and shutdown does not wait for a second read of every input.
fn name_damaged_input(inputs: &[&Arc<LiveAnnotationPart>], stop: &AtomicBool, e: Error) -> Error {
    if !matches!(e, Error::Corrupt(_)) || stop.load(Ordering::SeqCst) {
        return e;
    }
    for p in inputs {
        if let Err(why) = p.reader.verify() {
            // `Corrupt` already prints as "corrupt data: ..."; the new error
            // says it once.
            let why = match why {
                Error::Corrupt(m) => m,
                other => other.to_string(),
            };
            return Error::corrupt(format!(
                "merge input {} ({}): {why}",
                p.meta.part_id,
                part_dir_name(p.meta.part_id)
            ));
        }
    }
    e
}

impl Inner {
    /// Replaces the live parts `remove`, of either kind, by the span parts
    /// `add` and the annotation parts `add_ann`, which are already written to
    /// `parts/` (each with its metadata and an open reader, ids reserved from
    /// the manifest). Every id in `remove` is live in exactly one of the two
    /// lists. In this order:
    ///
    /// 1. one manifest commit that removes and adds them;
    /// 2. the new span list, a split map that matches it, the new annotation
    ///    list and its window index are published in one step under the core
    ///    lock, so a snapshot never sees a list and a map that disagree, nor a
    ///    removed part and its replacement both present or both absent, nor
    ///    one kind changed without the other. The span list and the map are
    ///    built before the gate is taken, from the list seen then; under the
    ///    gate only the parts a flush published since are added to them. The
    ///    annotation list is small and is rebuilt under the gate from the list
    ///    as it is then, so annotation parts a flush published since stay;
    /// 3. the directories of the removed parts are deleted and `parts/` is
    ///    synced.
    ///
    /// Steps 1 and 2 hold the publish gate, which a flush also holds from its
    /// commit to its publish; nothing is held while directories are deleted.
    /// Only one replacement runs at a time (they are made by a maintenance
    /// pass), which is what lets the list be built before the gate.
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
    pub(crate) fn replace_parts(
        &self,
        remove: &[u64],
        add: Vec<LivePart>,
        add_ann: Vec<LiveAnnotationPart>,
    ) -> Result<Replaced> {
        self.replace_parts_expiring(remove, add, add_ann, None)
    }

    /// [`replace_parts`](Self::replace_parts) for retention: `expire_before`
    /// is the cut-off it decided the expired parts with (a window is expired
    /// when `window_start_ns + window_ns` is at or below it). A flush may
    /// publish a span part or an annotation part into an expiring window
    /// between that decision and the gate; those parts are removed in the
    /// same commit, so the window goes whole.
    pub(crate) fn replace_parts_expiring(
        &self,
        remove: &[u64],
        add: Vec<LivePart>,
        add_ann: Vec<LiveAnnotationPart>,
        expire_before: Option<i128>,
    ) -> Result<Replaced> {
        let expired = |m: &nwdb_core::PartMeta| {
            expire_before
                .is_some_and(|c| i128::from(m.window_start_ns) + i128::from(m.window_ns) <= c)
        };
        let mut remove: Vec<u64> = remove.to_vec();
        let mut dirs: Vec<PathBuf> = remove
            .iter()
            .map(|&id| self.parts_dir.join(part_dir_name(id)))
            .collect();
        let add: Vec<Arc<LivePart>> = add.into_iter().map(Arc::new).collect();
        let add_ann: Vec<Arc<LiveAnnotationPart>> = add_ann.into_iter().map(Arc::new).collect();

        // The long work, before the gate: the list and the split map that the
        // list seen now will have after the replacement. Costs what
        // `change_parts` says, with no lock held. A flush may publish parts
        // meanwhile; nothing else changes the list, because replacements run
        // only in a maintenance pass and passes run one at a time.
        let (seen, seen_split) = {
            let c = lock(&self.core);
            (c.vis.parts.clone(), c.vis.split.clone())
        };
        let (built, built_split) = change_parts(&seen, &seen_split, remove, add.clone());
        self.hook_before_commit(Stage::ReplaceMapBuilt)?;

        {
            let gate = lock(&self.publish_gate);
            let (old_parts, old_ann) = {
                let c = lock(&self.core);
                if let Some(f) = &c.fatal {
                    return Err(Error::fatal(f.clone()));
                }
                (c.vis.parts.clone(), c.vis.ann_parts.clone())
            };
            let live: HashSet<u64> = old_parts
                .iter()
                .map(|p| p.meta.part_id)
                .chain(old_ann.iter().map(|p| p.meta.part_id))
                .collect();
            let mut seen_twice = HashSet::new();
            for id in &remove {
                if !live.contains(id) || !seen_twice.insert(*id) {
                    return Err(Error::invalid_argument(format!(
                        "cannot remove part {id}: it is not live or is listed twice"
                    )));
                }
            }
            if remove.is_empty() && add.is_empty() && add_ann.is_empty() {
                return Ok(Replaced::default());
            }
            // The parts published since the list was seen. Everything that
            // was seen must still be live, or the map built above is for a
            // list that no longer exists; that is refused before the commit.
            let known: HashSet<u64> = seen.iter().map(|p| p.meta.part_id).collect();
            let later: Vec<Arc<LivePart>> = old_parts
                .iter()
                .filter(|p| !known.contains(&p.meta.part_id))
                .cloned()
                .collect();
            let removing: HashSet<u64> = remove.iter().copied().collect();
            if old_parts.len() != seen.len() + later.len()
                || later.iter().any(|p| removing.contains(&p.meta.part_id))
            {
                return Err(Error::other(
                    "the part list changed while a replacement was being prepared",
                ));
            }
            // Parts published since the cut-off was decided that are in an
            // expired window go with it; they are in `later` and in no map
            // yet, so they are only left out of what is patched in.
            let (swept, later): (Vec<_>, Vec<_>) = later.into_iter().partition(|p| expired(&p.meta));
            let swept_ann: Vec<u64> = old_ann
                .iter()
                .filter(|p| expired(&p.meta) && !removing.contains(&p.meta.part_id))
                .map(|p| p.meta.part_id)
                .collect();
            for id in swept.iter().map(|p| p.meta.part_id).chain(swept_ann) {
                dirs.push(self.parts_dir.join(part_dir_name(id)));
                remove.push(id);
            }
            let removing: HashSet<u64> = remove.iter().copied().collect();
            // The last point where nothing has changed yet.
            self.hook_before_commit(Stage::BeforeReplaceCommit)?;

            // 1. The commit.
            let edit = Edit {
                add: add
                    .iter()
                    .map(|p| p.meta.clone())
                    .chain(add_ann.iter().map(|p| p.meta.clone()))
                    .collect(),
                remove: remove.clone(),
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

            // 2. The publish. Only the parts published since cost anything
            // here, as in a flush.
            let (parts, split) = if later.is_empty() {
                (built, built_split)
            } else {
                change_parts(&built, &built_split, &[], later)
            };
            // The annotation list is small: rebuilt from the list as it is
            // now, so parts a flush added meanwhile stay.
            let mut ann: Vec<Arc<LiveAnnotationPart>> = old_ann
                .iter()
                .filter(|p| !removing.contains(&p.meta.part_id))
                .cloned()
                .chain(add_ann)
                .collect();
            ann.sort_by_key(|p| p.meta.part_id);
            let ann_windows = Arc::new(annotation_windows(&ann));
            {
                let mut c = lock(&self.core);
                c.vis.parts = Arc::new(parts);
                c.vis.split = Arc::new(split);
                c.vis.ann_parts = Arc::new(ann);
                c.vis.ann_windows = ann_windows;
            }
            drop(gate);
        }
        self.stage_after_commit(Stage::ReplacePublished);

        // 3. The deletion.
        Ok(self.delete_dirs(dirs))
    }

    /// Calls the stage hook at a point before the commit of a replacement. An
    /// error it returns is the replacement's; `Fatal` also stops the engine.
    fn hook_before_commit(&self, stage: Stage) -> Result<()> {
        if let Err(e) = run_hook(&self.opts.stage_hook, stage) {
            if matches!(e, Error::Fatal(_)) {
                let mut c = lock(&self.core);
                self.set_fatal_locked(&mut c, e.to_string());
            }
            return Err(e);
        }
        Ok(())
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
                // `None`: too far away to represent, so only a flush wakes it.
                let deadline = Instant::now().checked_add(self.opts.maintenance_interval);
                loop {
                    if c.stop_maintenance {
                        return;
                    }
                    if c.maintenance_due {
                        c.maintenance_due = false;
                        break;
                    }
                    c = match deadline {
                        Some(deadline) => {
                            let now = Instant::now();
                            if now >= deadline {
                                break;
                            }
                            self.maintenance_cv
                                .wait_timeout(c, deadline - now)
                                .unwrap_or_else(|e| e.into_inner())
                                .0
                        }
                        None => self
                            .maintenance_cv
                            .wait(c)
                            .unwrap_or_else(|e| e.into_inner()),
                    };
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
        let mut last_retention = Instant::now();
        let mut result = self.retention_step();
        if merge && !self.maintenance_stopping() {
            let merged = self.choose_and_merge_parts(&mut last_retention);
            if result.is_ok() {
                result = merged;
            }
        }
        result
    }

    /// Retries the deletions that failed and applies retention.
    fn retention_step(&self) -> Result<()> {
        let mut result = self.retry_deletions();
        if !self.maintenance_stopping() {
            let applied = self.apply_retention();
            if result.is_ok() {
                result = applied;
            }
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

    /// Removes every part, of either kind, whose whole window is older than
    /// the retention, in one replacement.
    fn apply_retention(&self) -> Result<()> {
        let Some(retention) = self.opts.retention else {
            return Ok(());
        };
        // Wide arithmetic: a window near the end of the range, a huge
        // retention or an odd clock must not overflow.
        let keep = i128::try_from(retention.as_nanos()).unwrap_or(i128::MAX);
        let cutoff = i128::from(self.now_ns()).saturating_sub(keep);
        let (parts, ann) = {
            let c = lock(&self.core);
            (c.vis.parts.clone(), c.vis.ann_parts.clone())
        };
        let expired = |m: &nwdb_core::PartMeta| {
            i128::from(m.window_start_ns) + i128::from(m.window_ns) <= cutoff
        };
        let expired: Vec<u64> = parts
            .iter()
            .map(|p| &p.meta)
            .chain(ann.iter().map(|p| &p.meta))
            .filter(|m| expired(m))
            .map(|m| m.part_id)
            .collect();
        if expired.is_empty() {
            return Ok(());
        }
        let replaced =
            self.replace_parts_expiring(&expired, Vec::new(), Vec::new(), Some(cutoff))?;
        lock(&self.core).retention_removed += expired.len() as u64;
        replaced.into_result()
    }

    /// Merges parts while the policy finds something to merge, one merge at a
    /// time, and stops between merges when shutdown asks. A merge that is
    /// running when shutdown asks ends at its next chunk (`merge_parts` checks
    /// the stop flag) and leaves nothing behind; the README has the bound this
    /// puts on shutdown.
    ///
    /// A merge that fails is recorded and its window is left alone for a
    /// while (until a restart if the cause is damage in an input); the other
    /// windows go on. The first error is the step's result.
    ///
    /// A pass can keep finding merges for as long as flushes keep coming, so
    /// before each merge, once `maintenance_interval` has passed since
    /// retention (and the deletions) last ran, they run again.
    /// `last_retention` is when that was.
    fn choose_and_merge_parts(&self, last_retention: &mut Instant) -> Result<()> {
        if !self.opts.compaction.enabled {
            return Ok(());
        }
        let mut first_error = None;
        // A window that failed is left alone for the rest of the pass, so a
        // failure that persists ends the pass instead of repeating in it,
        // whatever the backoff.
        let mut failed_now: HashSet<MergeKey> = HashSet::new();
        while !self.merge_stop.load(Ordering::SeqCst) && !self.maintenance_stopping() {
            if last_retention.elapsed() >= self.opts.maintenance_interval {
                *last_retention = Instant::now();
                if let Err(e) = self.retention_step() {
                    first_error.get_or_insert(e);
                }
                if self.maintenance_stopping() {
                    break;
                }
            }
            let (parts, ann) = {
                let c = lock(&self.core);
                (c.vis.parts.clone(), c.vis.ann_parts.clone())
            };
            let Some(plan) = self.next_merge(&parts, &ann, &failed_now) else {
                break;
            };
            match self.merge_one(&parts, &ann, &plan) {
                Ok(replaced) => {
                    if let Err(e) = replaced.into_result() {
                        first_error.get_or_insert(e);
                    }
                }
                // Shutdown asked the merge to stop: not a failure of the
                // window, unless the engine is lost.
                Err(e)
                    if !matches!(e, Error::Fatal(_)) && self.merge_stop.load(Ordering::SeqCst) =>
                {
                    break;
                }
                Err(e) => {
                    failed_now.insert((plan.window_start_ns, plan.window_ns, plan.kind));
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

    /// The policy's choice for the parts of both kinds that are live now,
    /// leaving out the (window, kind) pairs that are backing off or damaged.
    fn next_merge(
        &self,
        parts: &[Arc<LivePart>],
        ann: &[Arc<LiveAnnotationPart>],
        failed_now: &HashSet<MergeKey>,
    ) -> Option<MergePlan> {
        let infos: Vec<PartInfo> = parts
            .iter()
            .map(|p| PartInfo {
                id: p.meta.part_id,
                kind: PartKind::Spans,
                window_start_ns: p.meta.window_start_ns,
                window_ns: p.meta.window_ns,
                size_bytes: p.meta.size_bytes(),
                pieces: p.meta.payload.pieces,
                level: p.meta.level,
                live_for: p.live_since.elapsed(),
            })
            .chain(ann.iter().map(|p| PartInfo {
                id: p.meta.part_id,
                kind: PartKind::Annotations,
                window_start_ns: p.meta.window_start_ns,
                window_ns: p.meta.window_ns,
                size_bytes: p.meta.size_bytes(),
                pieces: 0,
                level: p.meta.level,
                live_for: p.live_since.elapsed(),
            }))
            .collect();
        let now = Instant::now();
        let (damaged, waiting) = {
            let mut c = lock(&self.core);
            let live: HashSet<MergeKey> = infos
                .iter()
                .map(|p| (p.window_start_ns, p.window_ns, p.kind))
                .collect();
            // A window that is gone (expired) has nothing to remember.
            c.compaction.failing.retain(|k, _| live.contains(k));
            let waiting: HashSet<MergeKey> = c
                .compaction
                .failing
                .iter()
                .filter(|(_, (_, retry_at))| retry_at.is_none_or(|t| t > now))
                .map(|(k, _)| *k)
                .collect();
            (c.compaction.damaged.clone(), waiting)
        };
        let max_pieces = self.writer_options().max_pieces as u64;
        choose_merge(&self.opts.compaction, &infos, max_pieces, &|key| {
            damaged.contains(&key) || waiting.contains(&key) || failed_now.contains(&key)
        })
    }

    /// Merges the parts of `plan` into one and replaces them by it. The kind
    /// of the plan decides which list the inputs are in and which merge writes
    /// the output; everything around it (the id, the level, the stages, the
    /// abandoning of the output, the replacement, the statistics) is the same.
    /// On any error before the commit the database is as it was and the new
    /// directory is gone (except after `Fatal`, where whether the commit
    /// happened is not known and a restart decides).
    fn merge_one(
        &self,
        live: &[Arc<LivePart>],
        live_ann: &[Arc<LiveAnnotationPart>],
        plan: &MergePlan,
    ) -> Result<Replaced> {
        // Oldest first: ids grow with time. Which copy of a span a merge keeps
        // does not depend on this order; it matters only for equal arrivals.
        let mut inputs: Vec<&Arc<LivePart>> = Vec::new();
        let mut ann_inputs: Vec<&Arc<LiveAnnotationPart>> = Vec::new();
        match plan.kind {
            PartKind::Spans => {
                inputs.extend(live.iter().filter(|p| plan.parts.contains(&p.meta.part_id)))
            }
            PartKind::Annotations => {
                ann_inputs.extend(
                    live_ann
                        .iter()
                        .filter(|p| plan.parts.contains(&p.meta.part_id)),
                );
            }
        }
        inputs.sort_by_key(|p| p.meta.part_id);
        ann_inputs.sort_by_key(|p| p.meta.part_id);
        // What the steps below need of an input, whatever its kind.
        let metas: Vec<&nwdb_core::PartMeta> = inputs
            .iter()
            .map(|p| &p.meta)
            .chain(ann_inputs.iter().map(|p| &p.meta))
            .collect();
        if metas.len() != plan.parts.len() {
            return Err(Error::other("a part chosen for a merge is not live"));
        }
        let _busy = Merging::new(self);
        let id = self.manifest.reserve_part_ids(1)?.start;
        let dir = self.parts_dir.join(part_dir_name(id));
        let level = metas
            .iter()
            .map(|m| m.level)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        // The same writer settings a flush uses. `stop` is set at shutdown:
        // the merge then ends at its next chunk, and the pass treats the
        // error that arrives as no failure.
        run_hook(&self.opts.stage_hook, Stage::MergeStarting)?;
        let (meta, duplicates, superseded) = match plan.kind {
            PartKind::Spans => {
                let readers: Vec<&PartReader> = inputs.iter().map(|p| &p.reader).collect();
                let options = MergeOptions {
                    writer: self.writer_options(),
                    level,
                    stop: Some(self.merge_stop.clone()),
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
                (meta, stats.duplicates_dropped, 0)
            }
            PartKind::Annotations => {
                let readers: Vec<&AnnotationReader> =
                    ann_inputs.iter().map(|p| &p.reader).collect();
                let (meta, stats) = merge_annotation_parts(
                    &readers,
                    &self.parts_dir,
                    id,
                    plan.window_start_ns,
                    plan.window_ns,
                    level,
                    Some(&self.merge_stop),
                )
                .map_err(|e| name_damaged_input(&ann_inputs, &self.merge_stop, e))?;
                (meta, 0, stats.superseded)
            }
        };

        // From here the output exists on disk. Until the commit an error
        // removes it. Only the merge of the inputs reports damage of an input
        // (`Corrupt`); an error from opening or checking the output, or from
        // the replacement, is an ordinary failure of the merge: the window
        // backs off and is tried again.
        let abandon = |e: Error| {
            if !matches!(e, Error::Fatal(_)) {
                let _ = std::fs::remove_dir_all(&dir);
            }
            match e {
                Error::Corrupt(m) => {
                    Error::other(format!("the output of the merge could not be used: {m}"))
                }
                e => e,
            }
        };
        let output_bytes = meta.size_bytes();
        // The inputs were all quiet when the final rule chose them and
        // nothing new is in the output, so it is as quiet as they were: the
        // window can go on to its next merge at once, not after another quiet
        // period.
        let quiet_since = if plan.rule == Rule::Final {
            inputs
                .iter()
                .map(|p| p.live_since)
                .chain(ann_inputs.iter().map(|p| p.live_since))
                .max()
        } else {
            None
        };
        let (add, add_ann) = match plan.kind {
            PartKind::Spans => {
                let reader = PartReader::open(&dir).map_err(abandon)?;
                let mut part = LivePart::new(meta, reader);
                if let Some(since) = quiet_since {
                    part.live_since = since;
                }
                (vec![part], Vec::new())
            }
            PartKind::Annotations => {
                let reader = AnnotationReader::open(&dir, &meta).map_err(abandon)?;
                let mut part = LiveAnnotationPart::new(meta, reader);
                if let Some(since) = quiet_since {
                    part.live_since = since;
                }
                (Vec::new(), vec![part])
            }
        };
        run_hook(&self.opts.stage_hook, Stage::MergeWritten).map_err(abandon)?;
        let remove: Vec<u64> = metas.iter().map(|m| m.part_id).collect();
        let replaced = self.replace_parts(&remove, add, add_ann).map_err(abandon)?;

        let input_bytes: u64 = metas.iter().map(|m| m.size_bytes()).sum();
        let mut c = lock(&self.core);
        c.compaction.merges_done += 1;
        c.compaction.parts_merged_away += remove.len() as u64;
        c.compaction.duplicates_dropped += duplicates;
        c.compaction.annotations_superseded += superseded;
        c.compaction.input_bytes += input_bytes;
        c.compaction.output_bytes += output_bytes;
        c.compaction
            .failing
            .remove(&(plan.window_start_ns, plan.window_ns, plan.kind));
        Ok(replaced)
    }

    /// Records a failed merge in the statistics and decides when its window
    /// is tried again: not before a restart for damage, otherwise after a
    /// backoff that doubles with every failure in a row. A `Fatal` error also
    /// puts the engine in the fatal state.
    fn note_merge_failure(&self, plan: &MergePlan, e: &Error) {
        let key = (plan.window_start_ns, plan.window_ns, plan.kind);
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
            // A time that cannot be represented is "not by the clock".
            let retry_at = self
                .opts
                .compaction
                .retry_backoff
                .checked_mul(factor)
                .and_then(|delay| Instant::now().checked_add(delay));
            c.compaction.failing.insert(key, (n, retry_at));
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
