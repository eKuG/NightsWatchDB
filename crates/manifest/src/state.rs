use std::collections::BTreeSet;

use nwdb_core::{Error, PartMeta, Result};
use serde::{Deserialize, Serialize};

/// The only on-disk format version this crate reads and writes.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// Largest `next_part_id` accepted on load or in an edit. A real database
/// never gets near 2^62 parts; a larger value is a damaged or hostile file,
/// and refusing it keeps id arithmetic far from overflow.
pub const MAX_NEXT_PART_ID: u64 = 1 << 62;

/// One immutable version of the manifest.
///
/// Unknown fields are ignored when reading, so an older reader still loads a
/// file written by a newer writer that only added fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestState {
    /// Version of the file format, currently 1.
    pub format_version: u32,
    /// Manifest version. 0 is the empty state that exists before the first
    /// commit and is never written to disk.
    pub version: u64,
    /// Smallest part id that has never been used. Every live part id is below
    /// it.
    pub next_part_id: u64,
    /// Every write-ahead-log record up to and including this position is
    /// inside a live part.
    pub wal_flushed_lsn: u64,
    /// The live parts, sorted by `part_id`, ids unique.
    pub parts: Vec<PartMeta>,
    /// When this version was created, Unix milliseconds.
    pub created_unix_ms: u64,
}

/// A change to the set of parts, applied atomically as one new version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Edit {
    /// Parts to make live.
    pub add: Vec<PartMeta>,
    /// Ids of live parts to drop.
    pub remove: Vec<u64>,
    /// New log position; must not be lower than the current one.
    pub wal_flushed_lsn: Option<u64>,
    /// New `next_part_id`; must not be lower than the current one. When
    /// absent it becomes the larger of the current value and the largest added
    /// id plus one.
    pub next_part_id: Option<u64>,
}

impl ManifestState {
    /// The empty state, version 0, from which the first commit starts.
    pub fn empty() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            version: 0,
            next_part_id: 1,
            wal_flushed_lsn: 0,
            parts: Vec::new(),
            created_unix_ms: 0,
        }
    }

    /// Returns the live part with this id.
    pub fn part(&self, part_id: u64) -> Option<&PartMeta> {
        self.parts
            .binary_search_by_key(&part_id, |p| p.part_id)
            .ok()
            .map(|i| &self.parts[i])
    }

    /// Checks the invariants a loaded or computed state must hold.
    pub(crate) fn validate(&self) -> std::result::Result<(), String> {
        if self.parts.windows(2).any(|w| w[0].part_id >= w[1].part_id) {
            return Err("parts are not sorted by unique part id".to_string());
        }
        if self.next_part_id > MAX_NEXT_PART_ID {
            return Err(format!(
                "next_part_id {} is implausibly large (limit {MAX_NEXT_PART_ID})",
                self.next_part_id
            ));
        }
        if let Some(last) = self.parts.last()
            && last.part_id >= self.next_part_id
        {
            return Err(format!(
                "live part {} is not below next_part_id {}",
                last.part_id, self.next_part_id
            ));
        }
        Ok(())
    }

    /// Computes the state that results from `edit`, as version + 1.
    ///
    /// Nothing is modified. Every error is [`Error::InvalidArgument`]: the
    /// edit is invalid against `self`, the state the caller built it from.
    pub(crate) fn apply(&self, edit: &Edit, now_unix_ms: u64) -> Result<Self> {
        let mut removed = BTreeSet::new();
        for &id in &edit.remove {
            if self.part(id).is_none() {
                return Err(Error::invalid_argument(format!(
                    "cannot remove part {id}: it is not live"
                )));
            }
            if !removed.insert(id) {
                return Err(Error::invalid_argument(format!(
                    "part {id} is removed twice"
                )));
            }
        }
        let mut added = BTreeSet::new();
        for p in &edit.add {
            if self.part(p.part_id).is_some() {
                return Err(Error::invalid_argument(format!(
                    "cannot add part {}: it is already live",
                    p.part_id
                )));
            }
            if !added.insert(p.part_id) {
                return Err(Error::invalid_argument(format!(
                    "part {} is added twice",
                    p.part_id
                )));
            }
        }
        let wal_flushed_lsn = edit.wal_flushed_lsn.unwrap_or(self.wal_flushed_lsn);
        if wal_flushed_lsn < self.wal_flushed_lsn {
            return Err(Error::invalid_argument(format!(
                "wal_flushed_lsn {wal_flushed_lsn} is below the current {}",
                self.wal_flushed_lsn
            )));
        }
        let next_part_id = match edit.next_part_id {
            Some(n) if n < self.next_part_id => {
                return Err(Error::invalid_argument(format!(
                    "next_part_id {n} is below the current {}",
                    self.next_part_id
                )));
            }
            Some(n) => n,
            None => {
                let above_added = match added.last() {
                    Some(&max) => max.saturating_add(1),
                    None => 0,
                };
                self.next_part_id.max(above_added)
            }
        };
        if next_part_id > MAX_NEXT_PART_ID {
            return Err(Error::invalid_argument(format!(
                "next_part_id {next_part_id} is implausibly large (limit {MAX_NEXT_PART_ID})"
            )));
        }
        let version = self
            .version
            .checked_add(1)
            .ok_or_else(|| Error::invalid_argument("manifest version overflow"))?;

        let mut parts: Vec<PartMeta> = self
            .parts
            .iter()
            .filter(|p| !removed.contains(&p.part_id))
            .chain(edit.add.iter())
            .cloned()
            .collect();
        parts.sort_by_key(|p| p.part_id);
        if let Some(last) = parts.last()
            && last.part_id >= next_part_id
        {
            return Err(Error::invalid_argument(format!(
                "part {} is not below next_part_id {next_part_id}",
                last.part_id
            )));
        }
        Ok(Self {
            format_version: self.format_version,
            version,
            next_part_id,
            wal_flushed_lsn,
            parts,
            created_unix_ms: now_unix_ms,
        })
    }
}
