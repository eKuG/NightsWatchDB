//! Shared types for NightsWatchDB.
//!
//! This is layer 0 of the workspace: every other crate may depend on it and it
//! depends on no other workspace crate.
//!
//! # What belongs here
//!
//! - Identifiers ([`TraceId`], [`SpanId`]) and the one error type
//!   ([`Error`]) that every crate returns.
//! - Pure arithmetic and constants shared by writers and readers: time
//!   windows ([`window_start`]) and buffer limits ([`BatchLimits`]).
//! - The definition of the span table ([`span_schema`], [`SpanRecord`]), the
//!   way to build a batch of it ([`SpanBatchBuilder`], which stores each
//!   distinct origin value once per batch: [`Origin`], [`OriginKeys`]) and the
//!   way to read one back ([`SpanColumns`]).
//! - The per-trace summary ([`TraceSummary`], [`SummaryBuilder`],
//!   [`summary_schema`]) and its batch form ([`SummaryBatchBuilder`],
//!   [`SummaryColumns`]).
//! - The metadata record that describes one immutable part ([`PartMeta`]).
//! - The interface between ingest doors and the engine for replaying the
//!   write-ahead log ([`RecordDecoder`]).
//!
//! # What does not belong here
//!
//! - File or network I/O, `async`, threads. Crates that touch the disk or the
//!   network build on these types.
//! - Protobuf or OTLP decoding. The raw columns of the span table hold OTLP
//!   messages as bytes; core never parses them.
//! - Parquet, the write-ahead log, the manifest, compaction policy, query
//!   execution: each has its own crate.
//! - Anything with only one user. A type moves here when a second crate needs
//!   it.

mod builder;
mod columns;
mod decoder;
mod error;
mod hex;
mod id;
mod limits;
mod origin;
mod part;
mod record;
mod schema;
mod summary;
mod summary_batch;
mod window;

pub use builder::SpanBatchBuilder;
pub use columns::SpanColumns;
pub use decoder::RecordDecoder;
pub use error::{Error, Result};
pub use id::{SpanId, TraceId};
pub use limits::BatchLimits;
pub use origin::{Origin, OriginKeys};
pub use part::{PartMeta, part_dir_name};
pub use record::SpanRecord;
pub use schema::{col, span_schema};
pub use summary::{SummaryBuilder, TraceSummary, summary_schema};
pub use summary_batch::{SummaryBatchBuilder, SummaryColumns};
pub use window::{DEFAULT_WINDOW_NS, window_start};
