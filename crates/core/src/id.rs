use crate::{Error, Result, hex};
use std::fmt;
use std::str::FromStr;

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident, $len:literal, $what:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            /// Number of bytes in the identifier.
            pub const LEN: usize = $len;

            /// Builds an identifier from a slice, which must be exactly
            /// [`Self::LEN`] bytes.
            pub fn from_slice(bytes: &[u8]) -> Result<Self> {
                let arr: [u8; $len] = bytes.try_into().map_err(|_| {
                    Error::invalid_argument(format!(
                        concat!($what, " must be {} bytes, got {}"),
                        $len,
                        bytes.len()
                    ))
                })?;
                Ok(Self(arr))
            }

            /// The raw bytes.
            pub fn as_bytes(&self) -> &[u8; $len] {
                &self.0
            }

            /// True for the all-zero identifier, which OTLP defines as invalid.
            pub fn is_zero(&self) -> bool {
                self.0 == [0u8; $len]
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                hex::write_hex(f, &self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                hex::write_hex(f, &self.0)
            }
        }

        impl FromStr for $name {
            type Err = Error;

            /// Parses exactly `2 * LEN` hex digits, in either case.
            fn from_str(s: &str) -> Result<Self> {
                hex::decode_array::<$len>(s, $what).map(Self)
            }
        }
    };
}

id_type!(
    /// A 16-byte trace identifier. Displayed and parsed as 32 lowercase hex
    /// digits.
    TraceId,
    16,
    "trace id"
);
id_type!(
    /// An 8-byte span identifier. Displayed and parsed as 16 lowercase hex
    /// digits.
    SpanId,
    8,
    "span id"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_debug_round_trip() {
        let t = TraceId([0xab, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0xff]);
        let s = "ab000102030405060708090a0b0c0dff";
        assert_eq!(t.to_string(), s);
        assert_eq!(format!("{t:?}"), s);
        assert_eq!(s.parse::<TraceId>().unwrap(), t);
        assert_eq!(s.to_uppercase().parse::<TraceId>().unwrap(), t);

        let sp = SpanId([1, 2, 3, 4, 5, 6, 7, 0xa0]);
        assert_eq!(sp.to_string(), "01020304050607a0");
        assert_eq!("01020304050607a0".parse::<SpanId>().unwrap(), sp);
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!("".parse::<TraceId>().is_err());
        assert!("ab".parse::<TraceId>().is_err());
        assert!("0".repeat(31).parse::<TraceId>().is_err());
        assert!("0".repeat(33).parse::<TraceId>().is_err());
        assert!(format!("{}g", "0".repeat(31)).parse::<TraceId>().is_err());
        assert!(format!("0x{}", "0".repeat(30)).parse::<TraceId>().is_err());
        assert!(format!(" {}", "0".repeat(15)).parse::<SpanId>().is_err());
        // Wrong id type length.
        assert!("0".repeat(32).parse::<SpanId>().is_err());
    }

    #[test]
    fn from_slice_checks_length() {
        assert!(TraceId::from_slice(&[0; 16]).is_ok());
        assert!(TraceId::from_slice(&[0; 15]).is_err());
        assert!(SpanId::from_slice(&[0; 9]).is_err());
        let e = SpanId::from_slice(&[]).unwrap_err();
        assert!(matches!(e, Error::InvalidArgument(_)));
    }

    #[test]
    fn zero_and_order() {
        assert!(TraceId::default().is_zero());
        assert!(!SpanId([0, 0, 0, 0, 0, 0, 0, 1]).is_zero());
        assert!(SpanId([0, 0, 0, 0, 0, 0, 0, 1]) < SpanId([0, 0, 0, 0, 0, 0, 1, 0]));
    }
}
