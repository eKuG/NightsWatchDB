//! The `traces.idx` file: trace id to row range.

use nwdb_core::{Error, Result, TraceId};

/// File name of the trace index inside a part directory.
pub const INDEX_FILE: &str = "traces.idx";

const MAGIC: &[u8; 4] = b"NWTI";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 4 + 4 + 8;
const RECORD_LEN: usize = 44;
const CRC_LEN: usize = 4;

/// Where one trace's spans are stored in `spans.parquet`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceEntry {
    /// The trace.
    pub trace_id: TraceId,
    /// Row number of the trace's first span.
    pub first_row: u64,
    /// Number of spans; they occupy `first_row..first_row + row_count`.
    pub row_count: u32,
    /// Earliest span start in the trace, Unix nanoseconds.
    pub start_ns: i64,
    /// Latest span end in the trace, Unix nanoseconds.
    pub end_ns: i64,
}

/// Encodes entries, which must already be sorted by trace id.
pub(crate) fn encode(entries: &[TraceEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + entries.len() * RECORD_LEN + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.trace_id.0);
        out.extend_from_slice(&e.first_row.to_le_bytes());
        out.extend_from_slice(&e.row_count.to_le_bytes());
        out.extend_from_slice(&e.start_ns.to_le_bytes());
        out.extend_from_slice(&e.end_ns.to_le_bytes());
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{INDEX_FILE}: {msg}"))
}

/// Decodes and checks the whole file: magic, version, length, checksum and
/// strictly increasing trace ids.
pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<TraceEntry>> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("bad magic"));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
    if version != VERSION {
        return Err(corrupt(format!("unsupported version {version}")));
    }
    let count = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let expected = count
        .checked_mul(RECORD_LEN as u64)
        .and_then(|n| n.checked_add((HEADER_LEN + CRC_LEN) as u64));
    if expected != Some(bytes.len() as u64) {
        return Err(corrupt(format!(
            "length {} does not match {count} traces",
            bytes.len()
        )));
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    let stored = u32::from_le_bytes(crc.try_into().expect("4 bytes"));
    if crc32c::crc32c(body) != stored {
        return Err(corrupt("checksum mismatch"));
    }

    let le_u64 = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8 bytes"));
    let le_i64 = |b: &[u8]| i64::from_le_bytes(b.try_into().expect("8 bytes"));
    let mut entries: Vec<TraceEntry> = Vec::with_capacity(count as usize);
    for r in body[HEADER_LEN..].as_chunks::<RECORD_LEN>().0 {
        let entry = TraceEntry {
            trace_id: TraceId::from_slice(&r[..16]).expect("16 bytes"),
            first_row: le_u64(&r[16..24]),
            row_count: u32::from_le_bytes(r[24..28].try_into().expect("4 bytes")),
            start_ns: le_i64(&r[28..36]),
            end_ns: le_i64(&r[36..44]),
        };
        if entries.last().is_some_and(|p| p.trace_id >= entry.trace_id) {
            return Err(corrupt("trace ids are not strictly increasing"));
        }
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(n: u8) -> TraceEntry {
        TraceEntry {
            trace_id: TraceId([n; 16]),
            first_row: n as u64 * 10,
            row_count: n as u32 + 1,
            start_ns: -(n as i64),
            end_ns: 100 + n as i64,
        }
    }

    #[test]
    fn round_trip() {
        let all: Vec<_> = (1..50).map(entry).collect();
        let bytes = encode(&all);
        assert_eq!(bytes.len(), 16 + 49 * 44 + 4);
        assert_eq!(decode(&bytes).unwrap(), all);
    }

    #[test]
    fn empty_index_is_valid() {
        assert!(decode(&encode(&[])).unwrap().is_empty());
    }

    #[test]
    fn rejects_unsorted_and_duplicate_ids() {
        let bytes = encode(&[entry(2), entry(1)]);
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
        let bytes = encode(&[entry(1), entry(1)]);
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
    }

    #[test]
    fn huge_count_does_not_overflow() {
        let mut bytes = encode(&[entry(1)]);
        bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
    }
}
