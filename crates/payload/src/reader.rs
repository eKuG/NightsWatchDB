//! Reading a piece store.

use crate::format::{self, BLOCKS_FILE, INDEX_FILE, Index};
use crate::writer::FileSum;
use nwdb_core::{Error, PayloadTotals, Result};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// A [`PieceSession`] keeps decompressed blocks up to this many bytes; past
/// it, it drops what it has before keeping another.
const SESSION_CACHE_BYTES: usize = 32 << 20;

fn blk_corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{BLOCKS_FILE}: {msg}"))
}

/// The blocks one read call has decompressed so far, kept for the duration of
/// the call so that many pieces of one block cost one decompression. Create
/// one per call with [`PieceReader::session`].
#[derive(Default)]
pub struct PieceSession {
    blocks: HashMap<u32, Vec<u8>>,
    bytes: usize,
}

/// Read access to a piece store. Holds both files open and the tables in
/// memory; reads are positioned, so one reader serves many threads at once.
pub struct PieceReader {
    blk: File,
    idx: File,
    idx_len: u64,
    index: Index,
    totals: PayloadTotals,
    loads: AtomicU64,
}

impl PieceReader {
    /// Opens the store in `dir` and checks `pieces.idx` completely and the
    /// length of `pieces.blk`. The blocks are not read.
    ///
    /// Any damage is [`Error::Corrupt`] naming the file.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let missing = |name: &str, e: std::io::Error| match e.kind() {
            std::io::ErrorKind::NotFound => {
                Error::corrupt(format!("{name}: file is missing from the part"))
            }
            _ => Error::Io(e),
        };
        let idx = File::open(dir.join(INDEX_FILE)).map_err(|e| missing(INDEX_FILE, e))?;
        let idx_len = idx.metadata()?.len();
        let mut idx_bytes = vec![0u8; usize::try_from(idx_len).map_err(Error::other)?];
        idx.read_exact_at(&mut idx_bytes, 0)?;
        let index = format::decode(&idx_bytes)?;
        let blk = File::open(dir.join(BLOCKS_FILE)).map_err(|e| missing(BLOCKS_FILE, e))?;
        let len = blk.metadata()?.len();
        if len != index.blk_len() {
            return Err(blk_corrupt(format!(
                "file has {len} bytes but {INDEX_FILE} describes {}",
                index.blk_len()
            )));
        }
        let totals = PayloadTotals {
            pieces: index.pieces.len() as u64,
            logical_bytes: index.logical_bytes,
            unique_bytes: index.unique_bytes(),
            stored_bytes: len,
        };
        Ok(Self {
            blk,
            idx,
            idx_len,
            index,
            totals,
            loads: AtomicU64::new(0),
        })
    }

    /// The store's totals.
    pub fn totals(&self) -> PayloadTotals {
        self.totals
    }

    /// Length of `pieces.blk`.
    pub fn blocks_file_len(&self) -> u64 {
        self.totals.stored_bytes
    }

    /// Length of `pieces.idx`.
    pub fn index_file_len(&self) -> u64 {
        self.idx_len
    }

    /// Number of blocks.
    pub fn block_count(&self) -> usize {
        self.index.blocks.len()
    }

    /// How many times a block has been read and decompressed since the store
    /// was opened (a test and monitoring aid).
    pub fn blocks_loaded(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    /// An empty session for one read call.
    pub fn session(&self) -> PieceSession {
        PieceSession::default()
    }

    /// The length of piece `id`, without reading it.
    pub fn piece_len(&self, id: u32) -> Result<u64> {
        self.index
            .pieces
            .get(id as usize)
            .map(|p| u64::from(p.len))
            .ok_or_else(|| self.bad_id(id))
    }

    fn bad_id(&self, id: u32) -> Error {
        blk_corrupt(format!(
            "a stored value refers to piece {id} but the store has {}",
            self.index.pieces.len()
        ))
    }

    /// Reads, checks and decompresses one block.
    fn load(&self, block: u32) -> Result<Vec<u8>> {
        let loc = self
            .index
            .blocks
            .get(block as usize)
            .ok_or_else(|| blk_corrupt(format!("block {block} does not exist")))?;
        let mut packed = vec![0u8; loc.clen as usize];
        self.blk
            .read_exact_at(&mut packed, loc.offset)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    blk_corrupt(format!("block {block} ends past the end of the file"))
                }
                _ => Error::Io(e),
            })?;
        if crc32c::crc32c(&packed) != loc.crc {
            return Err(blk_corrupt(format!("block {block}: checksum mismatch")));
        }
        let data = zstd::bulk::decompress(&packed, loc.ulen as usize)
            .map_err(|e| blk_corrupt(format!("block {block} does not decompress ({e})")))?;
        if data.len() != loc.ulen as usize {
            return Err(blk_corrupt(format!(
                "block {block} decompresses to {} bytes, not {}",
                data.len(),
                loc.ulen
            )));
        }
        self.loads.fetch_add(1, Ordering::Relaxed);
        Ok(data)
    }

    /// Appends the bytes of piece `id` to `out`.
    ///
    /// The block is checked against its checksum every time it is
    /// decompressed, so what is appended is what was written or the result is
    /// [`Error::Corrupt`].
    pub fn read_piece(&self, session: &mut PieceSession, id: u32, out: &mut Vec<u8>) -> Result<()> {
        let p = *self
            .index
            .pieces
            .get(id as usize)
            .ok_or_else(|| self.bad_id(id))?;
        let (from, to) = (p.offset as usize, p.offset as usize + p.len as usize);
        if let Some(data) = session.blocks.get(&p.block) {
            out.extend_from_slice(&data[from..to]);
            return Ok(());
        }
        let data = self.load(p.block)?;
        out.extend_from_slice(&data[from..to]);
        if data.len() <= SESSION_CACHE_BYTES {
            if session.bytes + data.len() > SESSION_CACHE_BYTES {
                session.blocks.clear();
                session.bytes = 0;
            }
            session.bytes += data.len();
            session.blocks.insert(p.block, data);
        }
        Ok(())
    }

    /// Length and CRC-32C of `pieces.blk` and of `pieces.idx` as they are on
    /// disk now, read in full, for comparison with the values recorded when
    /// the part was written. Fails with [`Error::Corrupt`] if a file's length
    /// is not the one it had when the store was opened.
    pub fn file_sums(&self) -> Result<(FileSum, FileSum)> {
        Ok((
            sum_of(&self.blk, BLOCKS_FILE, self.totals.stored_bytes)?,
            sum_of(&self.idx, INDEX_FILE, self.idx_len)?,
        ))
    }

    /// Reads every block and checks its checksum and length. A sequential
    /// read of the whole store.
    pub fn verify(&self) -> Result<()> {
        for block in 0..self.index.blocks.len() {
            self.load(block as u32)?;
        }
        Ok(())
    }
}

/// Length and CRC-32C of a whole file, which must still be `opened_len` bytes.
fn sum_of(file: &File, name: &str, opened_len: u64) -> Result<FileSum> {
    let now = file.metadata()?.len();
    if now != opened_len {
        return Err(Error::corrupt(format!(
            "{name}: file has {now} bytes but had {opened_len} when it was opened"
        )));
    }
    let mut crc = 0u32;
    let mut buf = vec![0u8; 1 << 20];
    let mut at = 0u64;
    while at < opened_len {
        let n = (opened_len - at).min(buf.len() as u64) as usize;
        file.read_exact_at(&mut buf[..n], at)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    Error::corrupt(format!("{name}: file ended before its length"))
                }
                _ => Error::Io(e),
            })?;
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        at += n as u64;
    }
    Ok(FileSum {
        len: opened_len,
        crc32c: crc,
    })
}
