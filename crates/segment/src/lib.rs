//! Writes and reads one immutable part of span data.
//!
//! A part is a directory holding the spans of the traces that started in one
//! time window, in three files:
//!
//! - `spans.parquet`: the span table ([`nwdb_core::span_schema`]), with the
//!   spans of each trace stored next to each other.
//! - `traces.idx`: for every trace, the row range of its spans, so opening a
//!   trace is one lookup and one contiguous read.
//! - `summary.parquet`: one row per trace ([`nwdb_core::summary_schema`]),
//!   ordered by start time, for listing and filtering traces.
//!
//! [`PartWriter`] builds a part in a temporary directory and renames it into
//! place when it is complete; [`PartReader`] reads one. The file formats are
//! described in the crate's `README.md`.

mod index;
mod reader;
mod writer;

pub use index::{FileChecksum, INDEX_FILE, TraceEntry};
pub use reader::{PartReader, Scan, ScanOptions};
pub use writer::{FORMAT_VERSION, PartWriter, WriterOptions};

/// File name of the span rows inside a part directory.
pub const SPANS_FILE: &str = "spans.parquet";
/// File name of the per-trace summary inside a part directory.
pub const SUMMARY_FILE: &str = "summary.parquet";
