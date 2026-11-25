use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use nwdb_core::{Error, Result, part_dir_name};

use crate::file::{TEMP_PREFIX, file_name, parse_file_name};
use crate::state::{Edit, ManifestState};

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

struct Inner {
    state: Arc<ManifestState>,
    /// First id not yet handed out by `reserve_part_ids` in this process.
    next_reserved: u64,
}

/// A handle on a manifest directory.
///
/// Several handles, in this or other processes, may share one directory; they
/// coordinate only through the create-if-absent publish of the next version.
pub struct Manifest {
    dir: PathBuf,
    keep_versions: u64,
    inner: Mutex<Inner>,
}

impl Manifest {
    /// Opens the manifest in `dir`, creating the directory if it is missing.
    ///
    /// Leftover temporary files from a crashed commit are removed (this also
    /// removes the temporary file of a commit running right now in another
    /// handle, which then writes it again). The
    /// highest-numbered manifest file is loaded; an empty directory gives the
    /// empty state at version 0.
    ///
    /// Returns [`Error::Corrupt`] if the highest-numbered file is damaged. It
    /// does not fall back to an older file: an older version could bring
    /// removed parts back to life or forget data already acknowledged.
    pub fn open(dir: impl AsRef<Path>, options: ManifestOptions) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with(TEMP_PREFIX) {
                match fs::remove_file(entry.path()) {
                    Err(e) if e.kind() != ErrorKind::NotFound => return Err(e.into()),
                    _ => {}
                }
            }
        }
        let state = Arc::new(load_highest(&dir)?);
        Ok(Self {
            dir,
            keep_versions: options.keep_versions.max(1) as u64,
            inner: Mutex::new(Inner {
                next_reserved: state.next_part_id,
                state,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // The guarded data is only ever replaced whole, so a panic in another
        // thread never leaves it half updated.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The snapshot readers hold. It never changes, whatever is committed
    /// later.
    pub fn current(&self) -> Arc<ManifestState> {
        self.lock().state.clone()
    }

    /// Re-reads the highest version from disk and makes it current.
    pub fn reload(&self) -> Result<Arc<ManifestState>> {
        let mut inner = self.lock();
        let state = Arc::new(load_highest(&self.dir)?);
        inner.state = state.clone();
        Ok(state)
    }

    /// Applies `edit` to the current state and publishes it as the next
    /// version.
    ///
    /// Returns [`Error::InvalidArgument`] if the edit is not valid for the
    /// current state, and [`Error::Conflict`] if another writer already
    /// published the next version. In both cases nothing changed; after a
    /// conflict the caller should [`reload`](Self::reload), re-evaluate its
    /// edit against the new state and retry.
    ///
    /// An [`Error::Io`] before the file is published leaves nothing changed.
    /// An I/O error from the final directory sync means the new version is
    /// published and current but may not yet be durable; call
    /// [`reload`](Self::reload) to see the outcome instead of repeating the
    /// edit.
    pub fn commit(&self, edit: Edit) -> Result<Arc<ManifestState>> {
        let mut inner = self.lock();
        let next = inner.state.apply(&edit, now_unix_ms())?;
        let bytes = next.encode()?;

        self.publish(&bytes, next.version)?;

        let next = Arc::new(next);
        inner.state = next.clone();
        let synced = sync_dir(&self.dir);
        self.prune(next.version);
        synced?;
        Ok(next)
    }

    /// Writes `bytes` to a temporary file and links it to the name of
    /// `version`, removing the temporary name afterwards in every case.
    fn publish(&self, bytes: &[u8], version: u64) -> Result<()> {
        // `open` in another handle removes every temporary file it sees, which
        // can include ours between the write and the link. The link then
        // reports NotFound and nothing has been published, so writing again
        // is safe.
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

        // A handle that is far behind could otherwise publish a version that
        // old-file cleanup has already removed, and no reader would ever load
        // it. Refuse if this version or anything newer exists.
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
    /// Consequence for the engine: ids are not recorded anywhere until a
    /// commit, so after a crash the parts directory may hold directories
    /// whose ids the manifest does not know, and a restarted process hands
    /// the same ids out again. Delete every directory listed by
    /// [`unreferenced_part_dirs`] at startup, before any id is reserved. Two
    /// processes reserving from one directory can collide; that is caught
    /// when the second one commits, because the id is then already live.
    ///
    /// # Panics
    ///
    /// If the ids would pass `u64::MAX`, which no real workload reaches.
    pub fn reserve_part_ids(&self, n: u64) -> Range<u64> {
        let mut inner = self.lock();
        let start = inner.next_reserved.max(inner.state.next_part_id);
        let end = start.checked_add(n).expect("part id space exhausted");
        inner.next_reserved = end;
        start..end
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

/// Makes a link or removal in `dir` durable.
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

/// Loads the highest-numbered manifest file, or the empty state.
fn load_highest(dir: &Path) -> Result<ManifestState> {
    // The file can disappear between listing and reading when another handle
    // commits and removes old files; listing again finds the newer one.
    for _ in 0..8 {
        let Some(v) = highest_version(dir)? else {
            return Ok(ManifestState::empty());
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
        return Ok(state);
    }
    Err(Error::other(
        "manifest files kept disappearing while loading",
    ))
}

/// Lists directories under `parts_dir` that `state` does not reference.
///
/// A directory counts if its name is a 20-digit part id (a final part) or
/// ends in `.tmp` (a part still being written). A final directory is
/// referenced when `state` holds a live part with that id; a `.tmp` directory
/// never is. Other names and plain files are left alone. A missing
/// `parts_dir` gives an empty list. The result is sorted by name.
///
/// Call this at startup, before any writer runs: while a flush is in flight
/// its directory is unreferenced too. After a crash the listed directories
/// are leftovers and must be deleted before their ids can be reused.
pub fn unreferenced_part_dirs(parts_dir: &Path, state: &ManifestState) -> Result<Vec<PathBuf>> {
    let rd = match fs::read_dir(parts_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let live: HashSet<String> = state
        .parts
        .iter()
        .map(|p| part_dir_name(p.part_id))
        .collect();
    let mut out = Vec::new();
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_final = name.len() == 20 && name.bytes().all(|b| b.is_ascii_digit());
        if (is_final && !live.contains(&name)) || name.ends_with(".tmp") {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}
