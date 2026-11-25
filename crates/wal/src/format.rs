//! The on-disk format: file names, headers, record frames, and the scanner
//! shared by recovery and replay. See the crate README for the layout.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use nwdb_core::{Error, Result};

use crate::{Lsn, MAX_RECORD_BYTES};

pub(crate) const MAGIC: &[u8; 4] = b"NWAL";
pub(crate) const VERSION: u32 = 1;
pub(crate) const HEADER_LEN: u64 = 20;
pub(crate) const FRAME_LEN: u64 = 8;

pub(crate) fn file_name(first: Lsn) -> String {
    format!("wal-{first:020}.log")
}

pub(crate) fn file_path(dir: &Path, first: Lsn) -> PathBuf {
    dir.join(file_name(first))
}

/// Parses `wal-<20 digits>.log`; any other name is not a log file.
pub(crate) fn parse_file_name(name: &str) -> Option<Lsn> {
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

pub(crate) fn encode_header(first: Lsn) -> [u8; HEADER_LEN as usize] {
    let mut h = [0u8; HEADER_LEN as usize];
    h[0..4].copy_from_slice(MAGIC);
    h[4..8].copy_from_slice(&VERSION.to_le_bytes());
    h[8..16].copy_from_slice(&first.to_le_bytes());
    let crc = crc32c::crc32c(&h[0..16]);
    h[16..20].copy_from_slice(&crc.to_le_bytes());
    h
}

/// Checks a complete header and returns the first sequence number in it.
fn decode_header(h: &[u8; HEADER_LEN as usize]) -> std::result::Result<Lsn, String> {
    if &h[0..4] != MAGIC {
        return Err("bad magic".into());
    }
    let stored = u32::from_le_bytes(h[16..20].try_into().unwrap());
    if crc32c::crc32c(&h[0..16]) != stored {
        return Err("header checksum mismatch".into());
    }
    let version = u32::from_le_bytes(h[4..8].try_into().unwrap());
    if version != VERSION {
        return Err(format!("unsupported format version {version}"));
    }
    Ok(u64::from_le_bytes(h[8..16].try_into().unwrap()))
}

/// Appends one framed record to `out`.
pub(crate) fn encode_record(payload: &[u8], out: &mut Vec<u8>) {
    let len = (payload.len() as u32).to_le_bytes();
    let crc = crc32c::crc32c_append(crc32c::crc32c(&len), payload);
    out.extend_from_slice(&len);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// What the scanner found in a file.
pub(crate) struct Scan {
    pub records: u64,
    /// Offset just past the last good record (or the header).
    pub good_end: u64,
    pub file_len: u64,
}

/// How the scanner treats a bad record.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Mode {
    /// Any damage is `Error::Corrupt`.
    Strict,
    /// A short or failing record ends the scan; `Scan` says where.
    Lenient,
}

/// Reads a log file front to back, verifying every checksum. `visit` is called
/// with each record's sequence number and payload. Scanning stops early, with
/// no error, once the record numbered `limit` has been visited.
///
/// A bad header is always an error: a header cannot be told apart from damage
/// to the whole file, so even the last file is not repaired past it.
pub(crate) fn scan_file(
    path: &Path,
    expected_first: Lsn,
    mode: Mode,
    limit: Lsn,
    mut visit: impl FnMut(Lsn, &[u8]) -> Result<()>,
) -> Result<Scan> {
    let name = path.display();
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut r = BufReader::new(file);

    if file_len < HEADER_LEN {
        return Err(Error::corrupt(format!("{name}: incomplete header")));
    }
    let mut h = [0u8; HEADER_LEN as usize];
    r.read_exact(&mut h)?;
    let first = decode_header(&h).map_err(|e| Error::corrupt(format!("{name}: {e}")))?;
    if first != expected_first {
        return Err(Error::corrupt(format!(
            "{name}: header says first sequence number {first}, file name says {expected_first}"
        )));
    }

    let mut scan = Scan {
        records: 0,
        good_end: HEADER_LEN,
        file_len,
    };
    while first + scan.records <= limit {
        let remaining = file_len - scan.good_end;
        if remaining == 0 {
            break;
        }
        match read_record(&mut r, remaining)? {
            Ok(payload) => {
                visit(first + scan.records, &payload)?;
                scan.records += 1;
                scan.good_end += FRAME_LEN + payload.len() as u64;
            }
            Err(damage) => {
                if mode == Mode::Strict {
                    return Err(Error::corrupt(format!(
                        "{name}: record {} at offset {}: {damage}",
                        first + scan.records,
                        scan.good_end
                    )));
                }
                break;
            }
        }
    }
    Ok(scan)
}

/// Reads one record. The outer `Result` is for I/O failures; the inner one
/// reports a record that is short or fails its checksum.
fn read_record(r: &mut impl Read, remaining: u64) -> Result<std::result::Result<Vec<u8>, String>> {
    if remaining < FRAME_LEN {
        return Ok(Err("short record header".into()));
    }
    let mut f = [0u8; FRAME_LEN as usize];
    r.read_exact(&mut f)?;
    let len = u32::from_le_bytes(f[0..4].try_into().unwrap());
    let stored = u32::from_le_bytes(f[4..8].try_into().unwrap());
    // A length beyond the rest of the file is damage; checking before the
    // allocation keeps a garbage length from asking for gigabytes.
    if len as usize > MAX_RECORD_BYTES || FRAME_LEN + u64::from(len) > remaining {
        return Ok(Err(format!(
            "record length {len} runs past the end of the file"
        )));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;
    let crc = crc32c::crc32c_append(crc32c::crc32c(&f[0..4]), &payload);
    if crc != stored {
        return Ok(Err("checksum mismatch".into()));
    }
    Ok(Ok(payload))
}
