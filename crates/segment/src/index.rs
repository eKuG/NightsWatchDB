//! The `traces.idx` file: trace id to row range, and the length and checksum
//! of the part's files other than itself.

use nwdb_core::{Error, Result, TraceId};

/// File name of the trace index inside a part directory.
pub const INDEX_FILE: &str = "traces.idx";

const MAGIC: &[u8; 4] = b"NWTI";
/// The version written, always the part format version: the index is the one
/// file of a part that is read before any other, so it is where an older part
/// is recognised.
const VERSION: u32 = crate::FORMAT_VERSION;
const HEADER_LEN: usize = 4 + 4 + 8 + 6 * (8 + 4) + 4 + 4;
const RECORD_LEN: usize = 44;
const CRC_LEN: usize = 4;

/// Length and CRC-32C (Castagnoli) of one file of a part, computed while the
/// file was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileChecksum {
    /// Length of the file in bytes.
    pub len: u64,
    /// CRC-32C of every byte of the file, except for `pieces.idx`, where it
    /// is of every byte but the last four (the file's own checksum: over the
    /// whole file the value would be the same for every part).
    pub crc32c: u32,
}

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

/// The checksums of the other files of the part, in the order of the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileChecksums {
    pub spans: FileChecksum,
    pub summary: FileChecksum,
    pub pieces_blk: FileChecksum,
    pub pieces_idx: FileChecksum,
    pub text_index: FileChecksum,
    pub text_rows: FileChecksum,
    /// The checksum in the footer of `text.idx`, which covers its directory
    /// and so the checksum of every segment.
    pub text_index_footer: u32,
    /// The checksum in the footer of `text.rows`, which covers its group
    /// index and so the checksum of every group.
    pub text_rows_footer: u32,
}

/// What `traces.idx` holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Decoded {
    pub entries: Vec<TraceEntry>,
    pub checksums: FileChecksums,
}

/// Encodes entries, which must already be sorted by trace id.
pub(crate) fn encode(entries: &[TraceEntry], checksums: FileChecksums) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + entries.len() * RECORD_LEN + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for c in [
        checksums.spans,
        checksums.summary,
        checksums.pieces_blk,
        checksums.pieces_idx,
        checksums.text_index,
        checksums.text_rows,
    ] {
        out.extend_from_slice(&c.len.to_le_bytes());
        out.extend_from_slice(&c.crc32c.to_le_bytes());
    }
    out.extend_from_slice(&checksums.text_index_footer.to_le_bytes());
    out.extend_from_slice(&checksums.text_rows_footer.to_le_bytes());
    push_records(&mut out, entries);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn push_records(out: &mut Vec<u8>, entries: &[TraceEntry]) {
    for e in entries {
        out.extend_from_slice(&e.trace_id.0);
        out.extend_from_slice(&e.first_row.to_le_bytes());
        out.extend_from_slice(&e.row_count.to_le_bytes());
        out.extend_from_slice(&e.start_ns.to_le_bytes());
        out.extend_from_slice(&e.end_ns.to_le_bytes());
    }
}

fn corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{INDEX_FILE}: {msg}"))
}

/// Decodes and checks the whole file: magic, version, length, checksum and
/// strictly increasing trace ids.
pub(crate) fn decode(bytes: &[u8]) -> Result<Decoded> {
    // The version comes before any length or checksum check: an older part
    // has a different header, and the operator must be told that, not that
    // the file is damaged.
    if bytes.len() < 8 {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("bad magic"));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
    if let Err(Error::Corrupt(msg)) = crate::check_format_version(version) {
        return Err(corrupt(msg));
    }
    let header_len = HEADER_LEN;
    if bytes.len() < header_len + CRC_LEN {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    let count = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let expected = count
        .checked_mul(RECORD_LEN as u64)
        .and_then(|n| n.checked_add((header_len + CRC_LEN) as u64));
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
    let le_u32 = |b: &[u8]| u32::from_le_bytes(b.try_into().expect("4 bytes"));
    let sum_at = |at: usize| FileChecksum {
        len: le_u64(&body[at..at + 8]),
        crc32c: le_u32(&body[at + 8..at + 12]),
    };
    let checksums = FileChecksums {
        spans: sum_at(16),
        summary: sum_at(28),
        pieces_blk: sum_at(40),
        pieces_idx: sum_at(52),
        text_index: sum_at(64),
        text_rows: sum_at(76),
        text_index_footer: le_u32(&body[88..92]),
        text_rows_footer: le_u32(&body[92..96]),
    };
    let mut entries: Vec<TraceEntry> = Vec::with_capacity(count as usize);
    for r in body[header_len..].as_chunks::<RECORD_LEN>().0 {
        let entry = TraceEntry {
            trace_id: TraceId::from_slice(&r[..16]).expect("16 bytes"),
            first_row: le_u64(&r[16..24]),
            row_count: le_u32(&r[24..28]),
            start_ns: le_i64(&r[28..36]),
            end_ns: le_i64(&r[36..44]),
        };
        if entries.last().is_some_and(|p| p.trace_id >= entry.trace_id) {
            return Err(corrupt("trace ids are not strictly increasing"));
        }
        entries.push(entry);
    }
    Ok(Decoded { entries, checksums })
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

    const SUMS: FileChecksums = FileChecksums {
        spans: FileChecksum {
            len: 1234,
            crc32c: 0xDEAD_BEEF,
        },
        summary: FileChecksum {
            len: u64::MAX,
            crc32c: 7,
        },
        pieces_blk: FileChecksum { len: 5, crc32c: 6 },
        pieces_idx: FileChecksum { len: 8, crc32c: 9 },
        text_index: FileChecksum {
            len: 10,
            crc32c: 11,
        },
        text_rows: FileChecksum {
            len: 12,
            crc32c: 13,
        },
        text_index_footer: 14,
        text_rows_footer: 15,
    };

    #[test]
    fn round_trip() {
        let all: Vec<_> = (1..50).map(entry).collect();
        let bytes = encode(&all, SUMS);
        assert_eq!(bytes.len(), 96 + 49 * 44 + 4);
        let got = decode(&bytes).unwrap();
        assert_eq!(got.entries, all);
        assert_eq!(got.checksums, SUMS);
    }

    #[test]
    fn an_older_version_is_refused_before_anything_else_is_checked() {
        let mut bytes = encode(&[entry(1)], SUMS);
        for old in [1u32, 2, 3, 4, 5, 6] {
            bytes[4..8].copy_from_slice(&old.to_le_bytes());
            // Truncated and with a wrong checksum: the version still wins.
            for b in [&bytes[..], &bytes[..12]] {
                let msg = decode(b).unwrap_err().to_string();
                assert!(
                    msg.contains(&format!(
                        "format version {old} but this build reads version {VERSION}"
                    )) && msg.contains("cannot be read"),
                    "{msg}"
                );
            }
        }
    }

    #[test]
    fn empty_index_is_valid() {
        assert!(decode(&encode(&[], SUMS)).unwrap().entries.is_empty());
    }

    #[test]
    fn rejects_unsorted_and_duplicate_ids() {
        let bytes = encode(&[entry(2), entry(1)], SUMS);
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
        let bytes = encode(&[entry(1), entry(1)], SUMS);
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
    }

    #[test]
    fn huge_count_does_not_overflow() {
        let mut bytes = encode(&[entry(1)], SUMS);
        bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
    }
}
