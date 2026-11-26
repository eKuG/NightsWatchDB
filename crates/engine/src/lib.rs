//! The storage engine of NightsWatchDB.
//!
//! The engine wires the lower crates together: a request is logged
//! ([`nwdb_wal`]), acknowledged once durable, made visible to queries in a
//! memory buffer, later written into immutable parts ([`nwdb_segment`]) whose
//! set is published by the manifest ([`nwdb_manifest`]), and it survives a
//! crash at any point. The README of this crate describes the life of a
//! request, start-up, the threads, the flush procedure and what a crash at
//! each step leaves behind.
//!
//! # Duplicates
//!
//! A sender that retries after an error whose outcome was unknown can deliver
//! the same span twice. Within one flush, and in [`Snapshot::get_trace`],
//! duplicate `(trace id, span id)` pairs are removed. Duplicates that landed
//! in different parts are not removed from [`Snapshot::list_traces`] counts or
//! [`Snapshot::aggregate_tokens`] in this checkpoint; removing them is the
//! compactor's job.

mod engine;
mod flush;
mod maintenance;
mod query;
mod rows;
mod split;

#[cfg(test)]
extern crate self as nwdb_engine;
#[cfg(test)]
mod replace_tests;

pub use engine::{Clock, Engine, EngineOptions, EngineStats, Stage, StageHook};
pub use query::{AggQuery, ListQuery, ListStats, Snapshot, TokenAgg};

use std::sync::{Mutex, MutexGuard};

/// Locks, ignoring poisoning: no code holding the engine's locks leaves its
/// data half updated, and a panic in a caller's callback must not wedge the
/// engine.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
