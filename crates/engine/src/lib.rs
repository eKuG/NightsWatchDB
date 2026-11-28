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
//! the same span twice. Every row records the log record it arrived in (its
//! `arrival`), and of a span stored more than once the copy with the smallest
//! `arrival` is the one shown: in a flush, in a merge, in
//! [`Snapshot::get_trace`] and in [`Snapshot::search`], before and after any
//! merge. Copies that landed in different parts are still counted by
//! [`Snapshot::list_traces`] and [`Snapshot::aggregate_tokens`] until the parts
//! are merged (see [`CompactionOptions`]); a merge keeps one copy.

mod compaction;
mod engine;
mod flush;
mod maintenance;
mod query;
mod rows;
mod search;
mod split;

#[cfg(test)]
extern crate self as nwdb_engine;
#[cfg(test)]
mod replace_tests;
#[cfg(test)]
mod search_tests;

pub use compaction::CompactionOptions;
pub use engine::{
    ANNOTATION_RECORD_TAG, Annotated, Clock, Engine, EngineOptions, EngineStats, Stage, StageHook,
};
pub use nwdb_segment::Annotation;
pub use query::{AggQuery, AnnotationFilter, ListQuery, ListStats, Snapshot, TokenAgg};
pub use search::{SearchHit, SearchQuery, SearchResults, SearchStats};

/// Limits of a search. `max_confirm_bytes` is one budget for all the parts of
/// one engine search.
pub use nwdb_segment::SearchLimits;
/// The full-text query model: [`textindex::Query`] and [`textindex::parse`].
pub use nwdb_textindex as textindex;

// No code holding the engine's locks leaves its data half updated, and a
// panic in a caller's callback must not wedge the engine.
pub(crate) use nwdb_core::lock;
