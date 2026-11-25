use serde::{Deserialize, Serialize};
use std::ops::Range;

/// The raw columns of the span table whose values can be stored in pieces:
/// each is a concatenation of length-prefixed OTLP messages (see
/// [`span_schema`](crate::span_schema)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RawColumn {
    /// The span's attributes, one `KeyValue` after the other.
    Attrs,
    /// The span's events, one `Span.Event` after the other.
    Events,
    /// The span's links, one `Span.Link` after the other.
    Links,
}

/// Decides which bytes of a raw column value are stored out of line, once per
/// distinct content, instead of inside the span row.
///
/// The part writer cuts the ranges out of the original bytes and keeps
/// everything between them as it is, so a value is rebuilt byte for byte
/// whatever the splitter decides; a splitter can only affect how small the
/// part is. It lives next to [`RecordDecoder`](crate::RecordDecoder) because
/// the storage layer must not know the OTLP format and the OTLP crate must not
/// know the part format.
///
/// The contract: the ranges pushed to `cuts` are in ascending order, do not
/// overlap, are not empty and lie inside `value`; the function never panics
/// on any bytes; and it pushes nothing when in doubt. The part writer checks
/// the ranges and stores a value whole, inline, if they break the contract.
pub trait PayloadSplitter: Send + Sync {
    /// Appends to `cuts` the byte ranges of `value` to store out of line.
    fn cuts(&self, column: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>);
}

/// What a part's piece store holds, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadTotals {
    /// Number of distinct pieces stored.
    pub pieces: u64,
    /// What all references to pieces add up to: the size the pieces would
    /// have if each use stored its own copy.
    pub logical_bytes: u64,
    /// The sum of the lengths of the distinct pieces.
    pub unique_bytes: u64,
    /// Bytes the pieces take on disk, compressed (`pieces.blk`).
    pub stored_bytes: u64,
}
