//! Writes and reads one immutable part of span data.
//!
//! A part is a directory holding the spans of the traces that started in one
//! time window, in seven files:
//!
//! - `spans.parquet`: the span table ([`nwdb_core::span_schema`]), with the
//!   spans of each trace stored next to each other.
//! - `traces.idx`: for every trace, the row range of its spans, so opening a
//!   trace is one lookup and one contiguous read.
//! - `summary.parquet`: one row per trace ([`nwdb_core::summary_schema`]),
//!   ordered by start time, for listing and filtering traces.
//! - `pieces.blk` and `pieces.idx`: the piece store ([`nwdb_payload`]). Large
//!   text cut out of the raw columns of `spans.parquet` is stored here, each
//!   distinct piece once; the rows hold ropes that refer to it, and reads
//!   rebuild the original bytes.
//! - `text.idx` and `text.rows`: a full-text index with one document per piece
//!   and, for every piece, the rows that refer to it ([`PartReader::search`]).
//!
//! [`PartWriter`] builds a part in a temporary directory and renames it into
//! place when it is complete; [`PartReader`] reads one; [`merge_parts`] writes
//! one part from several without rebuilding their text. The file formats are
//! described in the crate's `README.md`.

mod annotation;
mod index;
mod merge;
mod reader;
mod search;
mod text;
mod writer;

pub use annotation::{
    ANNOTATION_FILE, ANNOTATION_FORMAT_VERSION, Annotation, MAX_BATCH_ITEMS, MAX_COMMENT_BYTES,
    MAX_LABEL_BYTES, MAX_NAME_BYTES, MAX_SOURCE_BYTES, check_annotation_format_version,
    decode_batch, encode_batch,
};
pub use index::{FileChecksum, INDEX_FILE, TraceEntry};
pub use merge::{DEFAULT_MERGE_CHUNK_BYTES, MergeOptions, MergeStats, merge_parts};
pub use nwdb_payload::{PieceObserver, PieceReader};
/// The text-index crate: [`textindex::Query`] and [`textindex::parse`] build the
/// queries [`PartReader::search`] takes.
pub use nwdb_textindex as textindex;
pub use reader::{PartReader, Scan, ScanOptions, SummaryScanRev};
pub use search::{SearchLimits, TextStats};
pub use text::{DEFAULT_ROW_LIST_BUDGET_BYTES, DEFAULT_TEXT_INDEX_BUDGET_BYTES};
pub use writer::{
    DEFAULT_DEDUP_WINDOW_BYTES, DEFAULT_MAX_PIECES, DEFAULT_MIN_PIECE_BYTES,
    DEFAULT_PIECE_BLOCK_BYTES, FORMAT_VERSION, PartWriter, WriterOptions, check_format_version,
};

/// File name of the span rows inside a part directory.
pub const SPANS_FILE: &str = "spans.parquet";
/// File name of the per-trace summary inside a part directory.
pub const SUMMARY_FILE: &str = "summary.parquet";
/// File name of the compressed blocks of the piece store inside a part
/// directory.
pub const PIECES_BLK: &str = nwdb_payload::BLOCKS_FILE;
/// File name of the piece store's block and piece tables inside a part
/// directory.
pub const PIECES_IDX: &str = nwdb_payload::INDEX_FILE;
/// File name of the text index (one document per piece) inside a part
/// directory.
pub const TEXT_INDEX_FILE: &str = "text.idx";
/// File name of the lists of rows of each piece inside a part directory.
pub const TEXT_ROWS_FILE: &str = "text.rows";
