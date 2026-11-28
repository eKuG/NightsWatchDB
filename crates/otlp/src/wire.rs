//! A bounds-checked reader for the protobuf wire format.
//!
//! Every read goes through a slice method that returns `None` when the input
//! is too short, so no length taken from the input is ever used to index or
//! allocate before it has been compared with the bytes that remain.

use nwdb_core::Error;

/// Longest string the reader accepts. The span table's `Utf8` columns use
/// 32-bit offsets and a builder only reports itself full between appends, so
/// one value must never be able to overflow a column on its own.
pub(crate) const MAX_STRING: usize = 1 << 30;

/// A decoding failure: what went wrong and the chain of field names from the
/// innermost field outward.
#[derive(Debug)]
pub(crate) struct Fault {
    what: &'static str,
    path: Vec<&'static str>,
}

pub(crate) type R<T> = std::result::Result<T, Fault>;

impl Fault {
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            what,
            path: Vec::new(),
        }
    }
}

/// Adds a field name to a failure as it propagates outward, so the final
/// message reads like `resource_spans.scope_spans.spans.name: invalid UTF-8`.
pub(crate) trait At<T> {
    fn at(self, field: &'static str) -> R<T>;
}

impl<T> At<T> for R<T> {
    fn at(self, field: &'static str) -> R<T> {
        self.map_err(|mut f| {
            f.path.push(field);
            f
        })
    }
}

impl From<Fault> for Error {
    fn from(f: Fault) -> Self {
        let path: Vec<&str> = f.path.iter().rev().copied().collect();
        if path.is_empty() {
            Error::corrupt(format!("otlp request: {}", f.what))
        } else {
            Error::corrupt(format!("otlp request: {}: {}", path.join("."), f.what))
        }
    }
}

/// The wire types a field may have. Groups (3 and 4) and the unassigned
/// values 6 and 7 are rejected when the tag is read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Wire {
    Varint,
    Fixed64,
    Len,
    Fixed32,
}

/// A cursor over the bytes of one message.
#[derive(Clone, Copy)]
pub(crate) struct Cur<'a> {
    rest: &'a [u8],
}

impl<'a> Cur<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { rest: buf }
    }

    pub(crate) fn varint(&mut self) -> R<u64> {
        let mut v = 0u64;
        for i in 0..10u32 {
            let Some((&b, tail)) = self.rest.split_first() else {
                return Err(Fault::new("truncated varint"));
            };
            self.rest = tail;
            // The tenth byte may only carry the top bit of a u64.
            if i == 9 && b > 1 {
                return Err(Fault::new("varint longer than 64 bits"));
            }
            v |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err(Fault::new("varint longer than 10 bytes"))
    }

    /// Reads the next tag, or `None` at the end of the message.
    pub(crate) fn next_field(&mut self) -> R<Option<(u32, Wire)>> {
        if self.rest.is_empty() {
            return Ok(None);
        }
        let tag = self.varint()?;
        let number = tag >> 3;
        if number == 0 || number > 0x1fff_ffff {
            return Err(Fault::new("invalid field number"));
        }
        let wire = match tag & 7 {
            0 => Wire::Varint,
            1 => Wire::Fixed64,
            2 => Wire::Len,
            5 => Wire::Fixed32,
            3 | 4 => return Err(Fault::new("group wire type is not supported")),
            _ => return Err(Fault::new("invalid wire type")),
        };
        // `number` fits in 29 bits, checked above.
        Ok(Some((number as u32, wire)))
    }

    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        let Some((head, tail)) = self.rest.split_at_checked(n) else {
            return Err(Fault::new("length runs past the end of the message"));
        };
        self.rest = tail;
        Ok(head)
    }

    /// The payload of a length-delimited field.
    pub(crate) fn len_delimited(&mut self) -> R<&'a [u8]> {
        let n = self.varint()?;
        // A length that does not fit in usize cannot fit in the input either.
        let n = usize::try_from(n)
            .map_err(|_| Fault::new("length runs past the end of the message"))?;
        self.take(n)
    }

    pub(crate) fn fixed64(&mut self) -> R<u64> {
        let b: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| Fault::new("truncated fixed64"))?;
        Ok(u64::from_le_bytes(b))
    }

    pub(crate) fn fixed32(&mut self) -> R<u32> {
        let b: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| Fault::new("truncated fixed32"))?;
        Ok(u32::from_le_bytes(b))
    }

    /// Skips the value of a field whose tag was just read.
    pub(crate) fn skip(&mut self, wire: Wire) -> R<()> {
        match wire {
            Wire::Varint => self.varint().map(drop),
            Wire::Fixed64 => self.take(8).map(drop),
            Wire::Len => self.len_delimited().map(drop),
            Wire::Fixed32 => self.take(4).map(drop),
        }
    }

    /// Length-delimited payload of a field that must have that wire type.
    pub(crate) fn expect_len(&mut self, wire: Wire) -> R<&'a [u8]> {
        if wire != Wire::Len {
            return Err(Fault::new("wrong wire type"));
        }
        self.len_delimited()
    }

    pub(crate) fn expect_varint(&mut self, wire: Wire) -> R<u64> {
        if wire != Wire::Varint {
            return Err(Fault::new("wrong wire type"));
        }
        self.varint()
    }

    pub(crate) fn expect_fixed64(&mut self, wire: Wire) -> R<u64> {
        if wire != Wire::Fixed64 {
            return Err(Fault::new("wrong wire type"));
        }
        self.fixed64()
    }

    pub(crate) fn expect_fixed32(&mut self, wire: Wire) -> R<u32> {
        if wire != Wire::Fixed32 {
            return Err(Fault::new("wrong wire type"));
        }
        self.fixed32()
    }
}

/// Validates bytes as UTF-8 and bounds their length.
pub(crate) fn utf8(b: &[u8]) -> R<&str> {
    if b.len() > MAX_STRING {
        return Err(Fault::new("string longer than 1 GiB"));
    }
    std::str::from_utf8(b).map_err(|_| Fault::new("invalid UTF-8"))
}

pub(crate) use nwdb_core::put_varint;
