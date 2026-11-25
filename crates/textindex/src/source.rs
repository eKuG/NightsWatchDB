//! Positioned reads, and the buffered counting writer used by the writers.

use std::io::{self, Write};
use std::sync::Arc;

/// Something that can be read at any position and knows its length.
///
/// Reads take `&self` and carry their own offset, so one value serves many
/// threads at once with no shared cursor.
pub trait ReadAt: Send + Sync {
    /// Total length in bytes.
    fn size(&self) -> io::Result<u64>;

    /// Fills `buf` from `offset`; a read that would pass the end is
    /// `UnexpectedEof`.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

fn eof() -> io::Error {
    io::Error::from(io::ErrorKind::UnexpectedEof)
}

impl ReadAt for [u8] {
    fn size(&self) -> io::Result<u64> {
        Ok(<[u8]>::len(self) as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(|_| eof())?;
        let end = start.checked_add(buf.len()).ok_or_else(eof)?;
        buf.copy_from_slice(self.get(start..end).ok_or_else(eof)?);
        Ok(())
    }
}

impl ReadAt for Vec<u8> {
    fn size(&self) -> io::Result<u64> {
        Ok(self.as_slice().len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.as_slice().read_exact_at(buf, offset)
    }
}

impl ReadAt for std::fs::File {
    fn size(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = std::os::windows::fs::FileExt::seek_read(self, buf, offset)?;
            if n == 0 {
                return Err(eof());
            }
            buf = &mut buf[n..];
            offset += n as u64;
        }
        Ok(())
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for Arc<T> {
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

/// Reads `len` bytes at `offset` after checking that they lie inside a source
/// of `total` bytes. The check comes before the allocation, so a length taken
/// from a damaged file can never size a buffer larger than the file.
pub(crate) fn read_checked<S: ReadAt + ?Sized>(
    src: &S,
    total: u64,
    offset: u64,
    len: u64,
    what: &str,
) -> nwdb_core::Result<Vec<u8>> {
    let end = offset.checked_add(len);
    if end.is_none_or(|e| e > total) {
        return Err(nwdb_core::Error::corrupt(format!(
            "{what} lies outside the file"
        )));
    }
    let len = usize::try_from(len)
        .map_err(|_| nwdb_core::Error::corrupt(format!("{what} is too large")))?;
    let mut buf = vec![0u8; len];
    src.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

/// A buffered writer that counts the bytes it has accepted.
pub(crate) struct Sink<W: Write> {
    inner: W,
    buf: Vec<u8>,
    pos: u64,
}

const SINK_BUF: usize = 64 * 1024;

impl<W: Write> Sink<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(SINK_BUF),
            pos: 0,
        }
    }

    /// Bytes accepted so far.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.pos += bytes.len() as u64;
        if self.buf.len() + bytes.len() > SINK_BUF {
            self.inner.write_all(&self.buf)?;
            self.buf.clear();
        }
        if bytes.len() >= SINK_BUF {
            self.inner.write_all(bytes)
        } else {
            self.buf.extend_from_slice(bytes);
            Ok(())
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.write_all(&self.buf)?;
        self.buf.clear();
        self.inner.flush()
    }
}
