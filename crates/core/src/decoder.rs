use arrow_array::RecordBatch;

use crate::Result;

/// Turns one write-ahead log record back into span batches.
///
/// An ingest door logs each request in its own wire format, so that logging
/// costs no re-encoding. After a crash the engine replays the log and needs to
/// decode those records again without knowing any protocol. Each door
/// registers a decoder under a one-byte tag; the engine stores the tag in
/// front of every record and uses it to pick the decoder on replay.
///
/// A decoder must be deterministic: the same bytes always produce the same
/// rows in the same order. It must return batches with [`crate::span_schema`]
/// and must not panic on any input; damaged input is [`crate::Error::Corrupt`].
pub trait RecordDecoder: Send + Sync {
    /// Decodes one logged record into span batches.
    fn decode(&self, record: &[u8]) -> Result<Vec<RecordBatch>>;
}
