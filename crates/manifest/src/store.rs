use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use nwdb_core::{Error, Result};

use crate::file::{TEMP_PREFIX, file_name, parse_file_name};
use crate::state::{Edit, MAX_NEXT_PART_ID, ManifestState, PART_ID_LEFTOVER_WINDOW};

/// Options for [`Manifest::open`].
#[derive(Debug, Clone)]
pub struct ManifestOptions {
    /// How many of the newest manifest files to keep after a commit. Values
    /// below 1 are treated as 1: the newest file is never deleted.
    pub keep_versions: usize,
}

impl Default for ManifestOptions {
    fn default() -> Self {
        Self { keep_versions: 10 }
    }
}

/// What [`Manifest::open`] does when the manifest directory has no manifest
/// file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// First start of a new database: create the directory if it is missing.
    /// An existing directory without manifest files is version 0.
    Create,
    /// Restart of an existing database: the directory must exist and hold at
    /// least one manifest file, otherwise [`Error::NotFound`]. A wrong path or
    /// an unmounted volume is an error, never an empty database.
    MustExist,
}

/// Test hook: runs inside `commit`, after the temporary file is written and
/// before the link, with the writer lock held.
type CommitHook = Arc<dyn Fn() + Send + Sync>;

/// A handle on a manifest directory.
///
/// One writer per data directory: the engine takes an exclusive lock on the
/// data directory and holds exactly one `Manifest`. Other handles would still
/// be detected (the loser of a publish race gets [`Error::Conflict`]), but
/// that is a second line of defence, not a supported way to run concurrent
/// writers.
///
/// Readers ([`current`](Self::current)) take only a short read lock on the
/// published pointer and never wait for a commit in progress.
pub struct Manifest {
    dir: PathBuf,
    keep_versions: u64,
    /// The last state known to be durable. Held only to clone or swap the
    /// pointer, never across I/O.
    published: RwLock<Arc<ManifestState>>,
    /// Serialises `commit` and `reload`; held across file writes and syncs.
    writer: Mutex<()>,
    /// First id not yet handed out by `reserve_part_ids` in this process.
    next_reserved: Mutex<u64>,
    /// Set when a failure after the link left the outcome unknown.
    poisoned: AtomicBool,
    commit_hook: Mutex<Option<CommitHook>>,
    fail_dir_sync: AtomicBool,
}

impl Manifest {
    /// Opens the manifest in `dir`.
    ///
    /// With [`OpenMode::Create`] the directory is created if missing (and its
    /// parent synced); with [`OpenMode::MustExist`] a missing directory, or
    /// one with no manifest file, is [`Error::NotFound`].
    ///
    /// Leftover temporary files from a crashed commit are removed. The
    /// highest-numbered manifest file is loaded, and the directory is synced
    /// before returning, so the version reported is durable: a previous
    /// process may have linked it and died before syncing.
    ///
    /// Returns [`Error::Corrupt`] if the highest-numbered file is damaged. It
    /// does not fall back to an older file: an older version could bring
    /// removed parts back to life or forget data already acknowledged.
    pub fn open(dir: impl AsRef<Path>, mode: OpenMode, options: ManifestOptions) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        match mode {
            OpenMode::Create => {
                if !dir.is_dir() {
                    fs::create_dir_all(&dir)?;
                    // Make the new directory's name durable.
                    if let Some(parent) = dir.parent() {
                        let parent = if parent.as_os_str().is_empty() {
                            Path::new(".")
                        } else {
                            parent
                        };
                        sync_dir(parent)?;
                    }
                }
            }
            OpenMode::MustExist => {
                if !dir.is_dir() {
                    return Err(Error::not_found(format!(
                        "manifest directory {} does not exist",
                        dir.display()
                    )));
                }
            }
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with(TEMP_PREFIX) {
                match fs::remove_file(entry.path()) {
                    Err(e) if e.kind() != ErrorKind::NotFound => return Err(e.into()),
                    _ => {}
                }
            }
        }
        let state = match load_highest(&dir)? {
            Some(s) => s,
            None if mode == OpenMode::MustExist => {
                return Err(Error::not_found(format!(
                    "manifest directory {} holds no manifest file",
                    dir.display()
                )));
            }
            None => ManifestState::empty(),
        };
        // The file we loaded may only be in the page cache.
        sync_dir(&dir)?;
        let state = Arc::new(state);
        Ok(Self {
            dir,
            keep_versions: options.keep_versions.max(1) as u64,
            next_reserved: Mutex::new(state.next_part_id),
            published: RwLock::new(state),
            writer: Mutex::new(()),
            poisoned: AtomicBool::new(false),
            commit_hook: Mutex::new(None),
            fail_dir_sync: AtomicBool::new(false),
        })
    }

    /// The writer lock. It guards `()`, so a panic in another thread cannot
    /// leave anything half updated.
    fn lock_writer(&self) -> MutexGuard<'_, ()> {
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check_poisoned(&self) -> Result<()> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(Error::fatal(
                "manifest handle is poisoned by an earlier failure after publishing; \
                 restart the process and recover",
            ));
        }
        Ok(())
    }

    fn set_published(&self, state: Arc<ManifestState>) {
        // The pointer is only ever replaced whole, so a poisoned lock still
        // holds valid data.
        *self.published.write().unwrap_or_else(|e| e.into_inner()) = state;
    }

    /// The snapshot readers hold. It never changes, whatever is committed
    /// later, and taking it never waits for a commit in progress. After an
    /// [`Error::Fatal`] it is the last state known to be durable.
    pub fn current(&self) -> Arc<ManifestState> {
        self.published
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Re-reads the highest version from disk, syncs the directory so that
    /// version is durable, and makes it current.
    ///
    /// Returns [`Error::Fatal`] on a poisoned handle and [`Error::Corrupt`]
    /// if the highest file is damaged.
    pub fn reload(&self) -> Result<Arc<ManifestState>> {
        let _w = self.lock_writer();
        self.check_poisoned()?;
        let state = load_highest(&self.dir)?.unwrap_or_else(ManifestState::empty);
        sync_dir(&self.dir)?;
        let state = Arc::new(state);
        self.set_published(state.clone());
        Ok(state)
    }

    /// Applies `edit` to the current state and publishes it as the next
    /// version.
    ///
    /// What the errors mean:
    ///
    /// * [`Error::InvalidArgument`]: the edit is invalid against this
    ///   handle's current state (removing a part that is not live, adding an
    ///   id that is live, going backwards, an implausible `next_part_id`).
    ///   After a conflict and [`reload`](Self::reload) the caller must
    ///   re-decide its edit against the reloaded state; an edit that is then
    ///   still invalid is the caller's bug, not a race. A handle that has not
    ///   reloaded learns of other writers only through `Conflict`.
    /// * [`Error::Conflict`]: someone else published this version (or a
    ///   newer one) first. Nothing changed; reload, re-decide and retry.
    /// * Any other error before the link, such as [`Error::Io`]: nothing
    ///   changed.
    /// * [`Error::Fatal`]: the link succeeded but the directory sync failed,
    ///   so the new version is visible on disk but may not be durable. The
    ///   handle is poisoned: every later `commit` and `reload` returns
    ///   `Fatal`, and `current()` stays at the last state known durable. The
    ///   process must restart; `open` syncs the directory and reports
    ///   whichever version survived.
    pub fn commit(&self, edit: Edit) -> Result<Arc<ManifestState>> {
        let _w = self.lock_writer();
        self.check_poisoned()?;
        let next = self.current().apply(&edit, now_unix_ms())?;
        let bytes = next.encode()?;

        self.publish(&bytes, next.version)?;

        // From here the version exists on disk: any failure is Fatal.
        let synced = if self.fail_dir_sync.load(Ordering::SeqCst) {
            Err(Error::other("injected directory sync failure"))
        } else {
            sync_dir(&self.dir)
        };
        if let Err(e) = synced {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(Error::fatal(format!(
                "manifest version {} was published but syncing the directory failed \
                 ({e}); it may or may not survive a crash, restart and recover",
                next.version
            )));
        }
        let next = Arc::new(next);
        self.set_published(next.clone());
        self.prune(next.version);
        Ok(next)
    }

    /// Test only: runs `hook` inside every commit, after the temporary file
    /// is written and before the link, with the writer lock held.
    #[doc(hidden)]
    pub fn set_commit_hook(&self, hook: Option<CommitHook>) {
        *self.commit_hook.lock().unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// Test only: makes the directory sync after the link fail.
    #[doc(hidden)]
    pub fn set_fail_dir_sync(&self, fail: bool) {
        self.fail_dir_sync.store(fail, Ordering::SeqCst);
    }

    /// Writes `bytes` to a temporary file and links it to the name of
    /// `version`, removing the temporary name afterwards in every case.
    fn publish(&self, bytes: &[u8], version: u64) -> Result<()> {
        // Defence only: with one writer per data directory nothing else
        // removes our temporary file. A second handle's `open` would remove
        // every temporary file it sees, including ours between the write and
        // the link; the link then reports NotFound with nothing published, so
        // writing again is safe. Bounded, so a persistent NotFound (for
        // example a deleted directory) still surfaces.
        let mut attempt = 0;
        loop {
            let temp = self.dir.join(format!(
                "{TEMP_PREFIX}{}-{}-{}",
                std::process::id(),
                TEMP_COUNTER.fetch_add(1, Ordering::Relaxed),
                now_unix_ms()
            ));
            let result = self.write_and_link(&temp, bytes, version);
            let _ = fs::remove_file(&temp);
            attempt += 1;
            match result {
                Err(Error::Io(e)) if e.kind() == ErrorKind::NotFound && attempt < 5 => {}
                other => return other,
            }
        }
    }

    fn write_and_link(&self, temp: &Path, bytes: &[u8], version: u64) -> Result<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);

        let hook = self
            .commit_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(h) = hook {
            h();
        }

        // Kept even with a single writer: a handle that is far behind could
        // otherwise publish a version that old-file cleanup has already
        // removed, and no reader would ever load it. Refuse if this version
        // or anything newer exists.
        if highest_version(&self.dir)?.is_some_and(|h| h >= version) {
            return Err(conflict(version));
        }
        match fs::hard_link(temp, self.dir.join(file_name(version))) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Err(conflict(version)),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes files older than the newest `keep_versions`. Failures are
    /// ignored: a leftover old file costs nothing, and the next commit tries
    /// again.
    fn prune(&self, newest: u64) {
        let Ok(rd) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in rd.flatten() {
            let name = entry.file_name();
            let Some(v) = name.to_str().and_then(parse_file_name) else {
                continue;
            };
            if v < newest && newest - v >= self.keep_versions {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    /// Hands out `n` part ids for parts the caller is about to write.
    ///
    /// This is purely in memory: ids start at the current `next_part_id` (or
    /// later) and are never handed out twice by one handle, even if the
    /// parts are never committed. A later [`commit`](Self::commit) that adds
    /// the parts raises `next_part_id` past them.
    ///
    /// Ids are not recorded anywhere until a commit, so after a crash the
    /// parts directory may hold directories whose ids the manifest does not
    /// know, and a restarted process would hand the same ids out again. See
    /// [`highest_part_dir_id`] for the startup rule that prevents this.
    ///
    /// Returns [`Error::InvalidArgument`] if the ids would pass
    /// [`MAX_NEXT_PART_ID`]; nothing is reserved then.
    pub fn reserve_part_ids(&self, n: u64) -> Result<Range<u64>> {
        let mut reserved = self.next_reserved.lock().unwrap_or_else(|e| e.into_inner());
        let start = (*reserved).max(self.current().next_part_id);
        let end = start
            .checked_add(n)
            .filter(|&e| e <= MAX_NEXT_PART_ID)
            .ok_or_else(|| Error::invalid_argument("part id space exhausted"))?;
        *reserved = end;
        Ok(start..end)
    }
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn conflict(version: u64) -> Error {
    Error::conflict(format!("manifest version {version} already exists"))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Makes a link, creation or removal in `dir` durable.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Directories cannot be opened for syncing on this platform.
#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

/// The highest manifest version in `dir`; other files are ignored.
fn highest_version(dir: &Path) -> Result<Option<u64>> {
    let mut best = None;
    for entry in fs::read_dir(dir)? {
        if let Some(v) = entry?.file_name().to_str().and_then(parse_file_name) {
            best = best.max(Some(v));
        }
    }
    Ok(best)
}

/// Loads the highest-numbered manifest file, or `None` if there is none.
fn load_highest(dir: &Path) -> Result<Option<ManifestState>> {
    // The file can disappear between listing and reading when another handle
    // commits and removes old files; listing again finds the newer one.
    for _ in 0..8 {
        let Some(v) = highest_version(dir)? else {
            return Ok(None);
        };
        let name = file_name(v);
        let bytes = match fs::read(dir.join(&name)) {
            Ok(b) => b,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let state = ManifestState::decode(&name, &bytes)?;
        if state.version != v {
            return Err(Error::corrupt(format!(
                "manifest file {name} holds version {}",
                state.version
            )));
        }
        return Ok(Some(state));
    }
    Err(Error::other(
        "manifest files kept disappearing while loading",
    ))
}

/// The part id a directory name stands for: 20 digits (a final part) or 20
/// digits plus `.tmp` (a part being written). Returns the id and whether the
/// directory is final.
fn part_dir_id(name: &str) -> Option<(u64, bool)> {
    let (digits, is_final) = match name.strip_suffix(".tmp") {
        Some(d) => (d, false),
        None => (name, true),
    };
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().map(|id| (id, is_final))
}

/// Part directories under `parts_dir` as `(path, id, is_final)`; a missing
/// `parts_dir` is empty.
fn part_dirs(parts_dir: &Path) -> Result<Vec<(PathBuf, u64, bool)>> {
    let rd = match fs::read_dir(parts_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut out = Vec::new();
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some((id, is_final)) = part_dir_id(&name) {
            out.push((entry.path(), id, is_final));
        }
    }
    Ok(out)
}

/// The largest part id among final and temporary part directories under
/// `parts_dir`, or `None` if there are none (or no `parts_dir`).
///
/// `next_part_id` is the manifest's current value. Ids at or above
/// `next_part_id + PART_ID_LEFTOVER_WINDOW` (and any id from
/// `MAX_NEXT_PART_ID` up) are ignored: no flush of this database could have
/// written them, so such a directory is a stray, and raising `next_part_id`
/// for it would use up the id space. [`unreferenced_part_dirs`] still lists
/// the stray directories.
///
/// Used at startup so ids are never reused after a crash; see the README.
pub fn highest_part_dir_id(parts_dir: &Path, next_part_id: u64) -> Result<Option<u64>> {
    let limit = next_part_id
        .checked_add(PART_ID_LEFTOVER_WINDOW)
        .map_or(MAX_NEXT_PART_ID, |l| l.min(MAX_NEXT_PART_ID));
    Ok(part_dirs(parts_dir)?
        .into_iter()
        .map(|(_, id, _)| id)
        .filter(|&id| id < limit)
        .max())
}

/// Whether `parts_dir` holds any part directory, final or temporary,
/// whatever its id.
pub fn has_part_dirs(parts_dir: &Path) -> Result<bool> {
    Ok(!part_dirs(parts_dir)?.is_empty())
}

/// Lists directories under `parts_dir` that `state` does not reference.
///
/// A directory counts if its name is a 20-digit part id (a final part) or
/// that followed by `.tmp` (a part still being written). A final directory is
/// referenced when `state` holds a live part with that id; a `.tmp` directory
/// never is. Other names and plain files are left alone. A missing
/// `parts_dir` gives an empty list. The result is sorted by name.
///
/// Refuses with [`Error::Corrupt`] when `state` is version 0 (nothing has
/// ever been committed) and `parts_dir` holds any final part directory: an
/// empty manifest next to existing parts means a lost or wrong manifest,
/// never a reason to delete them.
///
/// Call this at startup, before any writer runs: while a flush is in flight
/// its directory is unreferenced too. After a crash the listed directories
/// are leftovers and must be deleted, after the first commit described in
/// the README, so their ids are never reused.
pub fn unreferenced_part_dirs(parts_dir: &Path, state: &ManifestState) -> Result<Vec<PathBuf>> {
    let dirs = part_dirs(parts_dir)?;
    if state.version == 0 && dirs.iter().any(|(_, _, is_final)| *is_final) {
        return Err(Error::corrupt(format!(
            "manifest is at version 0 but {} contains part directories; \
             refusing to treat them as unreferenced",
            parts_dir.display()
        )));
    }
    let live: HashSet<u64> = state.parts.iter().map(|p| p.part_id).collect();
    let mut out: Vec<PathBuf> = dirs
        .into_iter()
        .filter(|(_, id, is_final)| !*is_final || !live.contains(id))
        .map(|(path, _, _)| path)
        .collect();
    out.sort();
    Ok(out)
}
