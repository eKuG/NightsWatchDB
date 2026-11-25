//! A forgiving reader for the protobuf wire format, shared by the streaming
//! JSON writers and the repair of the protobuf view.
//!
//! Unlike the decoder's reader it never builds an error: a field it cannot
//! read is reported as [`Next::Bad`] and the reader remembers where that field
//! started, so a caller can keep the unread rest of the message as it is. It
//! never allocates.

/// The value of one field, by wire type.
#[derive(Clone, Copy)]
pub(crate) enum Val<'a> {
    Varint(u64),
    Fixed64(u64),
    Fixed32(u32),
    Len(&'a [u8]),
}

/// What the reader found next.
pub(crate) enum Next<T> {
    End,
    Bad,
    Item(T),
}

pub(crate) struct Rd<'a> {
    buf: &'a [u8],
    pos: usize,
    mark: usize,
}

impl<'a> Rd<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            mark: 0,
        }
    }

    /// Everything from the start of the field or element last asked for.
    pub(crate) fn unread(&self) -> &'a [u8] {
        &self.buf[self.mark..]
    }

    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for i in 0..10u32 {
            let b = *self.buf.get(self.pos)?;
            self.pos += 1;
            // The tenth byte may only carry the top bit of a u64.
            if i == 9 && b > 1 {
                return None;
            }
            v |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                return Some(v);
            }
        }
        None
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let head = self.buf.get(self.pos..)?.get(..n)?;
        self.pos += n;
        Some(head)
    }

    fn len_delimited(&mut self) -> Option<&'a [u8]> {
        let n = usize::try_from(self.varint()?).ok()?;
        self.take(n)
    }

    /// The next element of a column of length-prefixed messages.
    pub(crate) fn prefixed(&mut self) -> Next<&'a [u8]> {
        if self.pos == self.buf.len() {
            return Next::End;
        }
        self.mark = self.pos;
        match self.len_delimited() {
            Some(m) => Next::Item(m),
            None => Next::Bad,
        }
    }

    /// The next field of a message: its number and value.
    pub(crate) fn field(&mut self) -> Next<(u32, Val<'a>)> {
        if self.pos == self.buf.len() {
            return Next::End;
        }
        self.mark = self.pos;
        match self.read_field() {
            Some(f) => Next::Item(f),
            None => Next::Bad,
        }
    }

    fn read_field(&mut self) -> Option<(u32, Val<'a>)> {
        let tag = self.varint()?;
        let number = tag >> 3;
        if number == 0 || number > 0x1fff_ffff {
            return None;
        }
        let val = match tag & 7 {
            0 => Val::Varint(self.varint()?),
            1 => Val::Fixed64(u64::from_le_bytes(self.take(8)?.try_into().ok()?)),
            2 => Val::Len(self.len_delimited()?),
            5 => Val::Fixed32(u32::from_le_bytes(self.take(4)?.try_into().ok()?)),
            // Groups and the unassigned wire types.
            _ => return None,
        };
        // `number` fits in 29 bits, checked above.
        Some((number as u32, val))
    }
}
