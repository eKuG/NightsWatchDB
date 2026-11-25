//! Reading a piece store.

use crate::format::{self, BLOCKS_FILE, INDEX_FILE, Index, PieceLoc};
use crate::writer::FileSum;
use nwdb_core::{Error, PayloadTotals, Result};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

fn blk_corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{BLOCKS_FILE}: {msg}"))
}

fn missing(name: &str, e: std::io::Error) -> Error {
    match e.kind() {
        std::io::ErrorKind::NotFound => {
            Error::corrupt(format!("{name}: file is missing from the part"))
        }
        _ => Error::Io(e),
    }
}

/// The block one read call decompressed last, kept so that consecutive
/// pieces of one block cost one decompression. Create one per call with
/// [`PieceReader::session`]; it holds at most one block.
#[derive(Default)]
pub struct PieceSession {
    block: Option<(u32, Vec<u8>)>,
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
        Self::open_with(dir.as_ref(), None)
    }

    /// Like [`open`](Self::open), for a store whose two files were recorded
    /// when it was written (by the part that holds it): the length of each file
    /// is compared with the record before anything is read, so a file that grew
    /// is not read at all, and the CRC-32C of `pieces.idx` (see
    /// [`FileSum::crc32c`]) is compared with the record after it is read. That checksum binds the store to its part,
    /// since `pieces.idx` holds the checksum of every block; a store taken from
    /// another part is refused here even when its files have the same lengths.
    pub fn open_recorded(dir: impl AsRef<Path>, blocks: FileSum, index: FileSum) -> Result<Self> {
        Self::open_with(dir.as_ref(), Some((blocks, index)))
    }

    fn open_with(dir: &Path, recorded: Option<(FileSum, FileSum)>) -> Result<Self> {
        let idx = File::open(dir.join(INDEX_FILE)).map_err(|e| missing(INDEX_FILE, e))?;
        let blk = File::open(dir.join(BLOCKS_FILE)).map_err(|e| missing(BLOCKS_FILE, e))?;
        let idx_len = idx.metadata()?.len();
        let blk_len = blk.metadata()?.len();
        if let Some((blocks, index)) = recorded {
            for (name, found, want) in [
                (INDEX_FILE, idx_len, index.len),
                (BLOCKS_FILE, blk_len, blocks.len),
            ] {
                if found != want {
                    return Err(Error::corrupt(format!(
                        "{name}: file has {found} bytes but the part's index recorded {want}"
                    )));
                }
            }
        }
        let mut idx_bytes = Vec::new();
        idx_bytes
            .try_reserve_exact(usize::try_from(idx_len).map_err(Error::other)?)
            .map_err(|_| format::corrupt(format!("{idx_len} bytes is too long")))?;
        idx_bytes.resize(idx_len as usize, 0);
        idx.read_exact_at(&mut idx_bytes, 0)?;
        if let Some((_, index)) = recorded {
            let crc = crc32c::crc32c(&idx_bytes[..idx_bytes.len().saturating_sub(4)]);
            if crc != index.crc32c {
                return Err(format::corrupt(format!(
                    "checksum mismatch: CRC-32C is {crc:#010x} but the part's index recorded \
                     {:#010x}; the file belongs to another part or is damaged",
                    index.crc32c
                )));
            }
        }
        let index = format::decode(&idx_bytes)?;
        drop(idx_bytes);
        if blk_len != index.blk_len() {
            return Err(blk_corrupt(format!(
                "file has {blk_len} bytes but {INDEX_FILE} describes {}",
                index.blk_len()
            )));
        }
        let totals = PayloadTotals {
            pieces: index.piece_count,
            logical_bytes: index.logical_bytes,
            unique_bytes: index.unique_bytes,
            stored_bytes: blk_len,
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

    /// Heap bytes this reader holds for the block and piece tables, which is
    /// what an open store costs in memory besides its file handles: about
    /// one to three bytes per piece.
    pub fn memory_bytes(&self) -> usize {
        self.index.memory_bytes()
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

    fn locate(&self, id: u32) -> Result<PieceLoc> {
        self.index.locate(id).ok_or_else(|| {
            blk_corrupt(format!(
                "a stored value refers to piece {id} but the store has {}",
                self.index.piece_count
            ))
        })
    }

    /// The length of piece `id`, without reading it.
    pub fn piece_len(&self, id: u32) -> Result<u64> {
        self.locate(id).map(|p| u64::from(p.len))
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
    /// [`Error::Corrupt`]. The session keeps the block, so reading the pieces
    /// of one block one after the other costs one decompression.
    pub fn read_piece(&self, session: &mut PieceSession, id: u32, out: &mut Vec<u8>) -> Result<()> {
        let p = self.locate(id)?;
        let (from, to) = (p.offset as usize, p.offset as usize + p.len as usize);
        if let Some((block, data)) = &session.block
            && *block == p.block
        {
            out.extend_from_slice(&data[from..to]);
            return Ok(());
        }
        session.block = None;
        let data = self.load(p.block)?;
        out.extend_from_slice(&data[from..to]);
        session.block = Some((p.block, data));
        Ok(())
    }

    /// Calls `f(n, bytes)` with the bytes of the piece named by the `n`th id of
    /// `ids`, which must not decrease. Each block is read and decompressed
    /// once however many of its pieces are named, and only one decompressed
    /// block is held at a time: this is how many pieces are rebuilt together
    /// at the cost of the blocks they lie in, not of the references to them.
    ///
    /// Fails with [`Error::Corrupt`] for a piece that does not exist or a
    /// block that is damaged, and with [`Error::InvalidArgument`] if the ids
    /// decrease.
    pub fn read_sorted(
        &self,
        ids: impl IntoIterator<Item = u32>,
        mut f: impl FnMut(usize, &[u8]),
    ) -> Result<()> {
        let mut current: Option<(u32, Vec<u8>)> = None;
        let mut last = 0u32;
        for (n, id) in ids.into_iter().enumerate() {
            if id < last {
                return Err(Error::invalid_argument(
                    "read_sorted needs ids that do not decrease",
                ));
            }
            last = id;
            let p = self.locate(id)?;
            if current.as_ref().is_none_or(|(block, _)| *block != p.block) {
                // Free the old block before the next one is read.
                drop(current.take());
                current = Some((p.block, self.load(p.block)?));
            }
            if let Some((_, data)) = &current {
                f(n, &data[p.offset as usize..(p.offset + p.len) as usize]);
            }
        }
        Ok(())
    }

    /// Length and CRC-32C of `pieces.blk` and of `pieces.idx` as they are on
    /// disk now, read in full, for comparison with the values recorded when
    /// the part was written. Fails with [`Error::Corrupt`] if a file's length
    /// is not the one it had when the store was opened.
    pub fn file_sums(&self) -> Result<(FileSum, FileSum)> {
        Ok((
            sum_of(&self.blk, BLOCKS_FILE, self.totals.stored_bytes, 0)?,
            sum_of(&self.idx, INDEX_FILE, self.idx_len, IDX_TRAILER)?,
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

/// Length of the checksum `pieces.idx` ends with, which [`FileSum`] leaves out.
const IDX_TRAILER: u64 = 4;

/// Length and CRC-32C of a file, which must still be `opened_len` bytes, of
/// every byte but the last `tail`.
fn sum_of(file: &File, name: &str, opened_len: u64, tail: u64) -> Result<FileSum> {
    let now = file.metadata()?.len();
    if now != opened_len {
        return Err(Error::corrupt(format!(
            "{name}: file has {now} bytes but had {opened_len} when it was opened"
        )));
    }
    let mut crc = 0u32;
    let mut buf = vec![0u8; 1 << 20];
    let mut at = 0u64;
    let end = opened_len.saturating_sub(tail);
    while at < end {
        let n = (end - at).min(buf.len() as u64) as usize;
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
