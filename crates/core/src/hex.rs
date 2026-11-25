//! Lowercase hex encoding, enough for identifiers.

use crate::{Error, Result};
use std::fmt;

/// Writes `bytes` as lowercase hex.
pub(crate) fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for b in bytes {
        write!(f, "{b:02x}")?;
    }
    Ok(())
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decodes exactly `N` bytes from `2 * N` hex digits, either case.
pub(crate) fn decode_array<const N: usize>(s: &str, what: &str) -> Result<[u8; N]> {
    let s = s.as_bytes();
    if s.len() != 2 * N {
        return Err(Error::invalid_argument(format!(
            "{what} must be {} hex digits, got {} bytes",
            2 * N,
            s.len()
        )));
    }
    let mut out = [0u8; N];
    let (pairs, _) = s.as_chunks::<2>();
    for (i, pair) in pairs.iter().enumerate() {
        match (nibble(pair[0]), nibble(pair[1])) {
            (Some(hi), Some(lo)) => out[i] = hi << 4 | lo,
            _ => return Err(Error::invalid_argument(format!("{what} is not hex"))),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_both_cases() {
        assert_eq!(decode_array::<2>("0aFf", "x").unwrap(), [0x0a, 0xff]);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(decode_array::<2>("0aF", "x").is_err());
        assert!(decode_array::<2>("0aFg", "x").is_err());
        assert!(decode_array::<2>("0aé", "x").is_err());
    }
}
