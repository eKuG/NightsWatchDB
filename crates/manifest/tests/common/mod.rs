//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::collections::BTreeMap;

use nwdb_core::PartMeta;
use nwdb_manifest::Edit;

/// A part with the given id and the plainest other values.
pub fn base_part(id: u64) -> PartMeta {
    PartMeta {
        part_id: id,
        level: 0,
        window_start_ns: 0,
        window_ns: 900_000_000_000,
        rows: 1,
        traces: 1,
        min_start_ns: 0,
        max_end_ns: 1,
        files: BTreeMap::new(),
        created_unix_ms: 0,
        format_version: 1,
        payload: Default::default(),
    }
}

/// An edit that adds one `make(id)` part per id.
pub fn add_with(ids: &[u64], make: fn(u64) -> PartMeta) -> Edit {
    Edit {
        add: ids.iter().map(|&i| make(i)).collect(),
        ..Edit::default()
    }
}

/// An edit that adds one [`base_part`] per id.
pub fn add(ids: &[u64]) -> Edit {
    add_with(ids, base_part)
}

/// The name of manifest version `v`.
pub fn vname(v: u64) -> String {
    format!("{v:020}.json")
}
