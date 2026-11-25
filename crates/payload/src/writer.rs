//! Writing a piece store.

use crate::format::{self, BLOCKS_FILE, BlockLoc, INDEX_FILE, Index, MAX_PIECE_BYTES, PieceLoc};
use ahash::RandomState;
use nwdb_core::{Error, PayloadTotals, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

const ZSTD_LEVEL: i32 = 3;

/// What the de-duplication window charges for each piece on top of its bytes:
/// the map entry and the table row it keeps in memory. Without it a window
/// full of tiny pieces would cost far more than the window is meant to.
const PIECE_OVERHEAD: usize = 64;

/// Settings of a [`PieceWriter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceWriterOptions {
    /// Uncompressed bytes at which a block is closed. A piece longer than
    /// this gets a block of its own. Default 1 MiB.
    pub block_bytes: usize,
    /// How many uncompressed bytes of closed blocks the writer keeps in
    /// memory to compare new pieces against. Default 64 MiB. A piece that
    /// repeats one from a block that was dropped is stored again.
    pub dedup_window_bytes: usize,
}

impl Default for PieceWriterOptions {
    fn default() -> Self {
        Self {
            block_bytes: 1 << 20,
            dedup_window_bytes: 64 << 20,
        }
    }
}

/// Length and CRC-32C of one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSum {
    /// Length in bytes.
    pub len: u64,
    /// CRC-32C of every byte.
    pub crc32c: u32,
}

/// What [`PieceWriter::finish`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreSummary {
    /// The store's totals.
    pub totals: PayloadTotals,
    /// `pieces.blk`.
    pub blocks: FileSum,
    /// `pieces.idx`.
    pub index: FileSum,
}

/// Marks the end of a chain of pieces that share a hash value.
const NONE: u32 = u32::MAX;

/// A closed block kept in memory for comparison.
struct Kept {
    data: Vec<u8>,
    /// Pieces `first..first + count` are in this block.
    first: u32,
    count: u32,
    /// When a piece of this block last matched, or when it was closed.
    used: u64,
}

impl Kept {
    fn cost(&self) -> usize {
        self.data.len() + self.count as usize * PIECE_OVERHEAD
    }
}

/// Appends pieces to a store, storing each distinct piece once.
///
/// Pieces are given ids 0, 1, 2 and so on in the order they are first seen.
/// They are gathered into blocks of about [`PieceWriterOptions::block_bytes`],
/// each compressed on its own with zstd level 3 and appended to `pieces.blk`;
/// [`finish`](Self::finish) writes `pieces.idx`.
///
/// De-duplication is exact. A keyed hash of the bytes, seeded randomly for
/// each writer and never stored, finds candidates; a candidate is used only
/// after its bytes have been compared with the new piece, so a sender cannot
/// make its text stand in for someone else's by producing a hash collision.
/// To compare, the writer needs the candidate's bytes, so it keeps the
/// uncompressed bytes of the open block and of recent closed blocks, up to
/// the window; when the window is full it drops the closed block whose last
/// match is oldest and forgets its pieces, so a later repeat of one of them is
/// stored again (a missed saving, never an error).
///
/// Memory: the open block, the window, one table row (12 bytes) and one map
/// entry per piece still in the window, and one table row per piece stored.
pub struct PieceWriter {
    blk: File,
    idx: File,
    opts: PieceWriterOptions,
    hasher: RandomState,

    /// The block being filled, and the first piece in it.
    open: Vec<u8>,
    open_first: u32,
    pieces: Vec<PieceLoc>,
    blocks: Vec<BlockLoc>,

    /// Hash value to the newest piece with it that is still in the window;
    /// `chain[id]` is the next older one, or `NONE`.
    map: HashMap<u64, u32>,
    chain: Vec<u32>,
    kept: HashMap<u32, Kept>,
    kept_cost: usize,
    tick: u64,

    written: u64,
    blk_crc: u32,
    logical: u64,
    unique: u64,

    #[cfg(test)]
    constant_hash: bool,
}

fn io_other(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::other(e)
}

impl PieceWriter {
    /// Creates `pieces.blk` and `pieces.idx` in `dir`, which must exist. An
    /// existing file is an error.
    pub fn create(dir: impl AsRef<Path>, opts: PieceWriterOptions) -> Result<Self> {
        if opts.block_bytes == 0 || opts.block_bytes > MAX_PIECE_BYTES {
            return Err(Error::invalid_argument(format!(
                "block_bytes must be between 1 and {MAX_PIECE_BYTES}"
            )));
        }
        let dir = dir.as_ref();
        Ok(Self {
            blk: File::create_new(dir.join(BLOCKS_FILE))?,
            idx: File::create_new(dir.join(INDEX_FILE))?,
            opts,
            hasher: RandomState::new(),
            open: Vec::new(),
            open_first: 0,
            pieces: Vec::new(),
            blocks: Vec::new(),
            map: HashMap::new(),
            chain: Vec::new(),
            kept: HashMap::new(),
            kept_cost: 0,
            tick: 0,
            written: 0,
            blk_crc: 0,
            logical: 0,
            unique: 0,
            #[cfg(test)]
            constant_hash: false,
        })
    }

    fn hash(&self, bytes: &[u8]) -> u64 {
        #[cfg(test)]
        if self.constant_hash {
            return 0;
        }
        self.hasher.hash_one(bytes)
    }

    /// The bytes of piece `id`, if the open block or the window still holds
    /// them.
    fn bytes_of(&self, id: u32) -> Option<&[u8]> {
        let p = self.pieces[id as usize];
        let (from, to) = (p.offset as usize, p.offset as usize + p.len as usize);
        if p.block as usize == self.blocks.len() {
            self.open.get(from..to)
        } else {
            self.kept.get(&p.block)?.data.get(from..to)
        }
    }

    /// Stores `bytes` unless an equal piece is already there, and returns the
    /// id of the piece that holds them. `bytes` must not be empty and must
    /// not be longer than [`MAX_PIECE_BYTES`].
    pub fn add(&mut self, bytes: &[u8]) -> Result<u32> {
        if bytes.is_empty() || bytes.len() > MAX_PIECE_BYTES {
            return Err(Error::invalid_argument(format!(
                "a piece must have 1 to {MAX_PIECE_BYTES} bytes, got {}",
                bytes.len()
            )));
        }
        self.logical += bytes.len() as u64;
        let h = self.hash(bytes);
        let mut at = self.map.get(&h).copied().unwrap_or(NONE);
        while at != NONE {
            if self.bytes_of(at) == Some(bytes) {
                let block = self.pieces[at as usize].block;
                self.tick += 1;
                if let Some(k) = self.kept.get_mut(&block) {
                    k.used = self.tick;
                }
                return Ok(at);
            }
            at = self.chain[at as usize];
        }

        if !self.open.is_empty() && self.open.len() + bytes.len() > self.opts.block_bytes {
            self.close_block()?;
        }
        let id = u32::try_from(self.pieces.len())
            .ok()
            .filter(|&id| id != NONE)
            .ok_or_else(|| Error::invalid_argument("a part can hold fewer than 2^32 pieces"))?;
        self.pieces.push(PieceLoc {
            block: self.blocks.len() as u32,
            offset: self.open.len() as u32,
            len: bytes.len() as u32,
        });
        self.open.extend_from_slice(bytes);
        self.unique += bytes.len() as u64;
        self.chain.push(self.map.insert(h, id).unwrap_or(NONE));
        if self.open.len() >= self.opts.block_bytes {
            self.close_block()?;
        }
        Ok(id)
    }

    /// Compresses the open block, appends it to the file and moves its bytes
    /// into the window.
    fn close_block(&mut self) -> Result<()> {
        if self.open.is_empty() {
            return Ok(());
        }
        let packed = zstd::bulk::compress(&self.open, ZSTD_LEVEL).map_err(io_other)?;
        self.blk.write_all(&packed)?;
        self.blk_crc = crc32c::crc32c_append(self.blk_crc, &packed);
        let block = self.blocks.len() as u32;
        self.blocks.push(BlockLoc {
            offset: self.written,
            clen: u32::try_from(packed.len()).map_err(|_| Error::other("block too large"))?,
            ulen: self.open.len() as u32,
            crc: crc32c::crc32c(&packed),
        });
        self.written += packed.len() as u64;

        self.tick += 1;
        let first = self.open_first;
        let end = self.pieces.len() as u32;
        let kept = Kept {
            data: std::mem::take(&mut self.open),
            first,
            count: end - first,
            used: self.tick,
        };
        self.kept_cost += kept.cost();
        self.kept.insert(block, kept);
        self.open_first = end;
        self.shrink_window();
        Ok(())
    }

    /// Drops closed blocks, least recently matched first, until the window
    /// fits.
    fn shrink_window(&mut self) {
        while self.kept_cost > self.opts.dedup_window_bytes {
            let Some(oldest) = self
                .kept
                .iter()
                .min_by_key(|(_, k)| k.used)
                .map(|(b, _)| *b)
            else {
                return;
            };
            let kept = self.kept.remove(&oldest).expect("just found");
            self.kept_cost -= kept.cost();
            for id in kept.first..kept.first + kept.count {
                let p = self.pieces[id as usize];
                let bytes = &kept.data[p.offset as usize..(p.offset + p.len) as usize];
                let h = self.hash(bytes);
                self.forget(h, id);
            }
        }
    }

    /// Removes piece `id`, which has hash `h`, from its chain.
    fn forget(&mut self, h: u64, id: u32) {
        let Some(&head) = self.map.get(&h) else {
            return;
        };
        if head == id {
            match self.chain[id as usize] {
                NONE => self.map.remove(&h),
                next => self.map.insert(h, next),
            };
            return;
        }
        let mut at = head;
        while at != NONE {
            let next = self.chain[at as usize];
            if next == id {
                self.chain[at as usize] = self.chain[id as usize];
                return;
            }
            at = next;
        }
    }

    /// Closes the last block, writes `pieces.idx`, syncs both files and
    /// reports what was written.
    pub fn finish(mut self) -> Result<StoreSummary> {
        self.close_block()?;
        self.blk.sync_all()?;
        let index = Index {
            blocks: std::mem::take(&mut self.blocks),
            pieces: std::mem::take(&mut self.pieces),
            logical_bytes: self.logical,
        };
        let bytes = format::encode(&index);
        self.idx.write_all(&bytes)?;
        self.idx.sync_all()?;
        Ok(StoreSummary {
            totals: PayloadTotals {
                pieces: index.pieces.len() as u64,
                logical_bytes: self.logical,
                unique_bytes: self.unique,
                stored_bytes: self.written,
            },
            blocks: FileSum {
                len: self.written,
                crc32c: self.blk_crc,
            },
            index: FileSum {
                len: bytes.len() as u64,
                crc32c: crc32c::crc32c(&bytes),
            },
        })
    }

    /// Bytes held in memory for comparison: the open block and the window.
    pub fn memory_bytes(&self) -> usize {
        self.open.len() + self.kept_cost
    }

    /// Makes every hash equal, so that all pieces share one bucket: the
    /// comparison, not the hash, must then keep different pieces apart.
    #[cfg(test)]
    pub(crate) fn force_constant_hash(&mut self) {
        self.constant_hash = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::PieceReader;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};

    fn small(block: usize, window: usize) -> PieceWriterOptions {
        PieceWriterOptions {
            block_bytes: block,
            dedup_window_bytes: window,
        }
    }

    fn read_all(dir: &Path, ids: &[u32]) -> Vec<Vec<u8>> {
        let r = PieceReader::open(dir).unwrap();
        let mut s = r.session();
        ids.iter()
            .map(|&id| {
                let mut out = Vec::new();
                r.read_piece(&mut s, id, &mut out).unwrap();
                out
            })
            .collect()
    }

    #[test]
    fn equal_pieces_share_an_id_and_different_ones_do_not() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), PieceWriterOptions::default()).unwrap();
        let a = w.add(b"alpha").unwrap();
        let b = w.add(b"beta").unwrap();
        assert_eq!((a, b), (0, 1));
        assert_eq!(w.add(b"alpha").unwrap(), 0);
        assert_eq!(w.add(b"beta").unwrap(), 1);
        assert_eq!(w.add(b"alpha2").unwrap(), 2);
        let sum = w.finish().unwrap();
        assert_eq!(sum.totals.pieces, 3);
        assert_eq!(sum.totals.logical_bytes, 5 + 4 + 5 + 4 + 6);
        assert_eq!(sum.totals.unique_bytes, 15);
        assert_eq!(
            read_all(tmp.path(), &[0, 1, 2]),
            [b"alpha".to_vec(), b"beta".to_vec(), b"alpha2".to_vec()]
        );
    }

    #[test]
    fn a_forced_hash_collision_keeps_different_pieces_apart() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(64, 1 << 20)).unwrap();
        w.force_constant_hash();
        let texts: Vec<Vec<u8>> = (0..40u8)
            .map(|i| vec![i; 5 + i as usize])
            .chain([b"same length a".to_vec(), b"same length b".to_vec()])
            .collect();
        let ids: Vec<u32> = texts.iter().map(|t| w.add(t).unwrap()).collect();
        // All of them in one bucket, none confused with another.
        assert_eq!(w.map.len(), 1);
        let distinct: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(distinct.len(), texts.len());
        // A repeat finds its own piece among the colliding ones, in the open
        // block and in closed ones.
        for (t, &id) in texts.iter().zip(&ids).rev() {
            assert_eq!(w.add(t).unwrap(), id);
        }
        w.finish().unwrap();
        assert_eq!(read_all(tmp.path(), &ids), texts);
    }

    #[test]
    fn a_collision_after_the_window_dropped_a_block_is_still_exact() {
        let tmp = tempfile::tempdir().unwrap();
        // A window of one block's worth: older blocks are forgotten.
        let mut w = PieceWriter::create(tmp.path(), small(100, 150)).unwrap();
        w.force_constant_hash();
        let texts: Vec<Vec<u8>> = (0..60u8).map(|i| vec![i; 50]).collect();
        let ids: Vec<u32> = texts.iter().map(|t| w.add(t).unwrap()).collect();
        for t in &texts {
            w.add(t).unwrap();
        }
        w.finish().unwrap();
        // Repeats of dropped pieces were stored again, as new pieces, and
        // everything still reads back as itself.
        let r = PieceReader::open(tmp.path()).unwrap();
        assert!(r.totals().pieces > 60);
        assert_eq!(read_all(tmp.path(), &ids), texts);
    }

    #[test]
    fn the_window_bounds_memory_and_the_data_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(4096, 16 * 1024)).unwrap();
        let mut rng = StdRng::seed_from_u64(5);
        let mut texts = Vec::new();
        let mut peak = 0;
        for _ in 0..400 {
            let mut t = vec![0u8; rng.random_range(1..3000usize)];
            rng.fill_bytes(&mut t);
            texts.push(t);
            w.add(texts.last().unwrap()).unwrap();
            peak = peak.max(w.memory_bytes());
        }
        // The open block plus the window, and a little slack for the piece
        // that closed a block.
        assert!(peak <= 16 * 1024 + 4096 + 3000, "{peak}");
        let ids: Vec<u32> = (0..400).collect();
        let sum = w.finish().unwrap();
        assert_eq!(sum.totals.pieces, 400);
        assert_eq!(read_all(tmp.path(), &ids), texts);
    }

    #[test]
    fn a_window_of_zero_still_finds_repeats_in_the_open_block() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(1 << 20, 0)).unwrap();
        let a = w.add(b"x").unwrap();
        assert_eq!(w.add(b"x").unwrap(), a);
        w.finish().unwrap();
    }

    #[test]
    fn a_piece_larger_than_a_block_has_a_block_of_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(100, 1 << 20)).unwrap();
        let big = vec![7u8; 1000];
        let ids = [
            w.add(b"head").unwrap(),
            w.add(&big).unwrap(),
            w.add(b"tail").unwrap(),
            w.add(&big).unwrap(),
        ];
        assert_eq!(ids, [0, 1, 2, 1]);
        w.finish().unwrap();
        let r = PieceReader::open(tmp.path()).unwrap();
        assert_eq!(r.block_count(), 3);
        assert_eq!(
            read_all(tmp.path(), &[0, 1, 2]),
            [b"head".to_vec(), big, b"tail".to_vec()]
        );
    }

    #[test]
    fn many_threads_read_one_store_at_once() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(2_000, 1 << 20)).unwrap();
        let mut rng = StdRng::seed_from_u64(21);
        let texts: Vec<Vec<u8>> = (0..300)
            .map(|_| {
                let mut t = vec![0u8; rng.random_range(1..700usize)];
                rng.fill_bytes(&mut t);
                t
            })
            .collect();
        for t in &texts {
            w.add(t).unwrap();
        }
        w.finish().unwrap();
        let r = PieceReader::open(tmp.path()).unwrap();
        std::thread::scope(|scope| {
            for seed in 0..8u64 {
                let (r, texts) = (&r, &texts);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    let mut session = r.session();
                    for _ in 0..500 {
                        let id = rng.random_range(0..texts.len());
                        let mut out = Vec::new();
                        r.read_piece(&mut session, id as u32, &mut out).unwrap();
                        assert_eq!(out, texts[id]);
                    }
                });
            }
        });
    }

    #[test]
    fn zero_pieces_make_a_valid_store() {
        let tmp = tempfile::tempdir().unwrap();
        let w = PieceWriter::create(tmp.path(), PieceWriterOptions::default()).unwrap();
        let sum = w.finish().unwrap();
        assert_eq!(sum.totals, PayloadTotals::default());
        assert_eq!(sum.blocks.len, 0);
        let r = PieceReader::open(tmp.path()).unwrap();
        assert_eq!(r.totals(), PayloadTotals::default());
        r.verify().unwrap();
    }

    #[test]
    fn bad_pieces_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), PieceWriterOptions::default()).unwrap();
        assert!(matches!(w.add(b""), Err(Error::InvalidArgument(_))));
    }
}
