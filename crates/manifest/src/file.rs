//! Encoding of one manifest file and the naming of files in the directory.

use nwdb_core::{Error, Result};

use crate::state::{FORMAT_VERSION, ManifestState, READABLE_VERSIONS};

const CRC_PREFIX: &str = "crc32c:";
const SUFFIX: &str = ".json";
/// Temporary files start with this and are never mistaken for a version.
pub(crate) const TEMP_PREFIX: &str = ".tmp-";

/// File name of a version: 20 decimal digits, the width of `u64::MAX`, so
/// names sort in version order.
pub(crate) fn file_name(version: u64) -> String {
    format!("{version:020}{SUFFIX}")
}

/// The version a directory entry names, or `None` for any other file.
pub(crate) fn parse_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(SUFFIX)?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // 20 digits can exceed u64::MAX; such a name is not ours.
    digits.parse().ok()
}

impl ManifestState {
    /// Serialises the state as one JSON line followed by a line
    /// `crc32c:<8 lowercase hex digits>` over the bytes of the JSON line.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(self).map_err(Error::other)?;
        let crc = crc32c::crc32c(json.as_bytes());
        Ok(format!("{json}\n{CRC_PREFIX}{crc:08x}\n").into_bytes())
    }

    /// Parses and verifies a file written by [`ManifestState::encode`].
    ///
    /// `name` is only used in error messages. Any defect is
    /// [`Error::Corrupt`].
    pub fn decode(name: &str, bytes: &[u8]) -> Result<Self> {
        let bad = |why: &str| Error::corrupt(format!("manifest file {name}: {why}"));
        let text = std::str::from_utf8(bytes).map_err(|_| bad("not valid UTF-8"))?;
        let (json, rest) = text.split_once('\n').ok_or_else(|| bad("truncated"))?;
        let hex = rest
            .strip_prefix(CRC_PREFIX)
            .and_then(|r| r.strip_suffix('\n'))
            .ok_or_else(|| bad("missing or truncated checksum line"))?;
        let stored = (hex.len() == 8 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
            .then(|| u32::from_str_radix(hex, 16).ok())
            .flatten()
            .ok_or_else(|| bad("malformed checksum"))?;
        if stored != crc32c::crc32c(json.as_bytes()) {
            return Err(bad("checksum mismatch"));
        }
        let state: Self =
            serde_json::from_str(json).map_err(|e| bad(&format!("invalid JSON: {e}")))?;
        if !READABLE_VERSIONS.contains(&state.format_version) {
            // Guessing at a newer layout could resurrect deleted parts.
            return Err(bad(&format!(
                "unsupported format_version {}; this build reads versions 1 and {FORMAT_VERSION}",
                state.format_version
            )));
        }
        if state.version == 0 {
            return Err(bad("version 0 is never written"));
        }
        state.validate().map_err(|why| bad(&why))?;
        Ok(state)
    }
}
