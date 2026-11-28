//! Writing a piece store.

use crate::format::{self, BLOCKS_FILE, BlockLoc, INDEX_FILE, MAX_PIECE_BYTES};
use ahash::RandomState;
use nwdb_core::{Error, PayloadTotals, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

const ZSTD_LEVEL: i32 = 3;

/// What the de-duplication window charges for each piece on top of its bytes:
/// the offset and chain entries (4 bytes each) and the map entry (a 12-byte
/// slot with its control byte, at a load factor that can leave half the table
/// empty). More than is held, never less, so that a window full of tiny pieces
/// is bounded too.
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
    /// CRC-32C of every byte, except for `pieces.idx`, where it is the CRC-32C
    /// of every byte but the last four. That file ends with its own checksum,
    /// and the CRC-32C of a message with its CRC-32C appended is the same
    /// constant for every message: over the whole file it would tell one
    /// `pieces.idx` from another not at all. Over the rest it is that checksum
    /// itself, which depends on every block's checksum and so on the blocks.
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

/// What [`PieceWriter::add_piece`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Added {
    /// The id of the piece that holds the bytes.
    pub id: u32,
    /// True if the bytes were stored now, false if an equal piece was already
    /// there.
    pub new: bool,
}

/// Told about every piece the first time it is stored, in the order of their
/// ids, for example to index its text. The part writer calls it as it stores
/// the pieces of a batch, before the batch is written. A repeat of a piece
/// that is still known is not announced; a repeat of one the writer has
/// forgotten (see [`PieceWriterOptions::dedup_window_bytes`]) is stored again
/// under a new id and announced again.
pub trait PieceObserver: Send + Sync {
    /// Piece `id` was stored and holds `bytes`.
    fn piece_stored(&self, id: u32, bytes: &[u8]);
}

/// Marks the end of a chain of pieces that share a hash value.
const NONE: u32 = u32::MAX;

/// A block that is being filled or is kept in memory for comparison, with
/// what the writer needs to find and compare its pieces.
struct Block {
    data: Vec<u8>,
    /// The id of the first piece in this block.
    first: u32,
    /// Where each piece starts in `data`; a piece ends where the next one
    /// starts, the last one at the end of `data`.
    offsets: Vec<u32>,
    /// For each piece, the next older piece with the same hash value that is
    /// still known, or `NONE`.
    chain: Vec<u32>,
    /// When a piece of this block last matched, or when it was closed.
    used: u64,
}

impl Block {
    fn new(first: u32) -> Self {
        Self {
            data: Vec::new(),
            first,
            offsets: Vec::new(),
            chain: Vec::new(),
            used: 0,
        }
    }

    fn count(&self) -> usize {
        self.offsets.len()
    }

    fn piece(&self, id: u32) -> &[u8] {
        let i = (id - self.first) as usize;
        let from = self.offsets[i] as usize;
        let to = self
            .offsets
            .get(i + 1)
            .map_or(self.data.len(), |&o| o as usize);
        &self.data[from..to]
    }

    /// What the block holds in memory, as the window is charged for it.
    fn cost(&self) -> usize {
        self.data.capacity() + self.count() * PIECE_OVERHEAD
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
/// Memory: the open block, the window (each block of it trimmed to its size
/// and charged 64 bytes per piece for its offset, chain and map entries), and
/// the length of every piece stored, in the compact form it has on disk
/// (one byte up to 127 bytes, two up to 16 KiB, three up to 2 MiB). That last
/// part is the only one that grows with the number of pieces of the part.
pub struct PieceWriter {
    blk: File,
    idx: File,
    opts: PieceWriterOptions,
    hasher: RandomState,

    /// The block being filled.
    open: Block,
    /// The id of the first piece of each closed block.
    block_first: Vec<u32>,
    blocks: Vec<BlockLoc>,
    /// The lengths of all pieces, as they are written to `pieces.idx`.
    lens: Vec<u8>,
    count: u64,

    /// Hash value to the newest piece with it that is still known.
    map: HashMap<u64, u32>,
    kept: HashMap<u32, Block>,
    kept_cost: usize,
    tick: u64,

    written: u64,
    blk_crc: u32,
    logical: u64,
    unique: u64,

    #[cfg(test)]
    constant_hash: bool,
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
            open: Block::new(0),
            block_first: Vec::new(),
            blocks: Vec::new(),
            lens: Vec::new(),
            count: 0,
            map: HashMap::new(),
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

    /// The closed block that holds piece `id`, or `None` for the open one.
    fn block_no(&self, id: u32) -> Option<u32> {
        (id < self.open.first).then(|| self.block_first.partition_point(|&f| f <= id) as u32 - 1)
    }

    /// The block holding piece `id`, if the open block or the window still
    /// holds it.
    fn block(&self, id: u32) -> Option<&Block> {
        match self.block_no(id) {
            None => Some(&self.open),
            Some(b) => self.kept.get(&b),
        }
    }

    fn block_mut(&mut self, id: u32) -> Option<&mut Block> {
        match self.block_no(id) {
            None => Some(&mut self.open),
            Some(b) => self.kept.get_mut(&b),
        }
    }

    fn chain_of(&self, id: u32) -> u32 {
        self.block(id)
            .map_or(NONE, |b| b.chain[(id - b.first) as usize])
    }

    /// The piece with hash `h` and these bytes, if it is still known.
    fn lookup(&mut self, h: u64, bytes: &[u8]) -> Option<u32> {
        let mut at = self.map.get(&h).copied().unwrap_or(NONE);
        while at != NONE {
            let b = self.block(at)?;
            if b.piece(at) == bytes {
                self.tick += 1;
                let tick = self.tick;
                if let Some(b) = self.block_mut(at) {
                    b.used = tick;
                }
                return Some(at);
            }
            at = b.chain[(at - b.first) as usize];
        }
        None
    }

    /// The id of the piece that holds `bytes`, if the writer still knows one
    /// (the open block and the window are searched; a piece it has forgotten is
    /// not found). Counts as a reference to it, like [`add`](Self::add). Never
    /// stores anything, so it works when no new piece may be created.
    pub fn find(&mut self, bytes: &[u8]) -> Option<u32> {
        let id = self.find_unreferenced(bytes)?;
        self.logical += bytes.len() as u64;
        Some(id)
    }

    /// Like [`find`](Self::find), but the lookup is not counted as a
    /// reference. For a caller that counts references itself with
    /// [`add_references`](Self::add_references), such as one that carries the
    /// references of ropes written elsewhere over to this store.
    pub fn find_unreferenced(&mut self, bytes: &[u8]) -> Option<u32> {
        if bytes.is_empty() || bytes.len() > MAX_PIECE_BYTES {
            return None;
        }
        let h = self.hash(bytes);
        self.lookup(h, bytes)
    }

    /// Counts `bytes` more bytes of references to pieces of this store, for a
    /// caller that used [`add_piece_unreferenced`](Self::add_piece_unreferenced)
    /// or [`find_unreferenced`](Self::find_unreferenced). The total of all
    /// references is recorded in `pieces.idx` and checked by the part's
    /// `verify`, so it must equal the length of every reference the ropes
    /// written against this store hold.
    pub fn add_references(&mut self, bytes: u64) {
        self.logical = self.logical.saturating_add(bytes);
    }

    /// Stores `bytes` unless an equal piece is already there, and returns the
    /// id of the piece that holds them. `bytes` must not be empty and must
    /// not be longer than [`MAX_PIECE_BYTES`].
    pub fn add(&mut self, bytes: &[u8]) -> Result<u32> {
        self.add_piece(bytes).map(|a| a.id)
    }

    /// Like [`add`](Self::add), and says whether the piece is new.
    pub fn add_piece(&mut self, bytes: &[u8]) -> Result<Added> {
        let added = self.add_piece_unreferenced(bytes)?;
        self.logical += bytes.len() as u64;
        Ok(added)
    }

    /// Like [`add_piece`](Self::add_piece), but not counted as a reference;
    /// see [`add_references`](Self::add_references).
    pub fn add_piece_unreferenced(&mut self, bytes: &[u8]) -> Result<Added> {
        if bytes.is_empty() || bytes.len() > MAX_PIECE_BYTES {
            return Err(Error::invalid_argument(format!(
                "a piece must have 1 to {MAX_PIECE_BYTES} bytes, got {}",
                bytes.len()
            )));
        }
        let h = self.hash(bytes);
        if let Some(id) = self.lookup(h, bytes) {
            return Ok(Added { id, new: false });
        }

        if !self.open.data.is_empty() && self.open.data.len() + bytes.len() > self.opts.block_bytes
        {
            self.close_block()?;
        }
        let id = u32::try_from(self.count)
            .ok()
            .filter(|&id| id != NONE)
            .ok_or_else(|| Error::invalid_argument("a part can hold fewer than 2^32 pieces"))?;
        if self.lens.len() >= u32::MAX as usize - 8 {
            return Err(Error::invalid_argument("the piece table is too long"));
        }
        self.open.offsets.push(self.open.data.len() as u32);
        self.open.data.extend_from_slice(bytes);
        let older = self.map.insert(h, id).unwrap_or(NONE);
        self.open.chain.push(older);
        nwdb_core::put_varint(&mut self.lens, u64::from(bytes.len() as u32));
        self.count += 1;
        self.unique += bytes.len() as u64;
        if self.open.data.len() >= self.opts.block_bytes {
            self.close_block()?;
        }
        Ok(Added { id, new: true })
    }

    /// The number of pieces stored so far.
    pub fn piece_count(&self) -> u64 {
        self.count
    }

    /// Compresses the open block, appends it to the file and moves its bytes
    /// into the window, trimmed to the size they have.
    fn close_block(&mut self) -> Result<()> {
        if self.open.data.is_empty() {
            return Ok(());
        }
        let packed = zstd::bulk::compress(&self.open.data, ZSTD_LEVEL).map_err(Error::other)?;
        self.blk.write_all(&packed)?;
        self.blk_crc = crc32c::crc32c_append(self.blk_crc, &packed);
        let block = self.blocks.len() as u32;
        self.blocks.push(BlockLoc {
            offset: self.written,
            clen: u32::try_from(packed.len()).map_err(|_| Error::other("block too large"))?,
            ulen: self.open.data.len() as u32,
            crc: crc32c::crc32c(&packed),
        });
        self.block_first.push(self.open.first);
        self.written += packed.len() as u64;

        self.tick += 1;
        let mut closed = std::mem::replace(&mut self.open, Block::new(self.count as u32));
        closed.used = self.tick;
        // The buffers grew by doubling; the window must be charged for what is
        // held, so give back the unused part.
        closed.data.shrink_to_fit();
        closed.offsets.shrink_to_fit();
        closed.chain.shrink_to_fit();
        self.kept_cost += closed.cost();
        self.kept.insert(block, closed);
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
            // Unlink its pieces while the block is still there to be searched,
            // then drop it.
            let doomed: Vec<(u64, u32)> = {
                let b = &self.kept[&oldest];
                (0..b.count() as u32)
                    .map(|i| (self.hash(b.piece(b.first + i)), b.first + i))
                    .collect()
            };
            for (h, id) in doomed {
                self.forget(h, id);
            }
            let kept = self.kept.remove(&oldest).expect("just found");
            self.kept_cost -= kept.cost();
        }
    }

    /// Removes piece `id`, which has hash `h`, from its chain.
    fn forget(&mut self, h: u64, id: u32) {
        let Some(&head) = self.map.get(&h) else {
            return;
        };
        let next = self.chain_of(id);
        if head == id {
            match next {
                NONE => self.map.remove(&h),
                next => self.map.insert(h, next),
            };
            return;
        }
        let mut at = head;
        while at != NONE {
            let after = self.chain_of(at);
            if after == id {
                if let Some(b) = self.block_mut(at) {
                    let i = (at - b.first) as usize;
                    b.chain[i] = next;
                }
                return;
            }
            at = after;
        }
    }

    /// Closes the last block, writes `pieces.idx`, syncs both files and
    /// reports what was written.
    pub fn finish(mut self) -> Result<StoreSummary> {
        self.close_block()?;
        self.blk.sync_all()?;
        // The lengths are written from the table the writer holds, not copied
        // into a second buffer: that copy cost a byte for every piece at the
        // moment the writer's memory is highest.
        let head = format::encode_head(&self.blocks, self.count, self.logical);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&head), &self.lens);
        self.idx.write_all(&head)?;
        self.idx.write_all(&self.lens)?;
        self.idx.write_all(&crc.to_le_bytes())?;
        self.idx.sync_all()?;
        let idx_len = (head.len() + self.lens.len() + 4) as u64;
        Ok(StoreSummary {
            totals: PayloadTotals {
                pieces: self.count,
                logical_bytes: self.logical,
                unique_bytes: self.unique,
                stored_bytes: self.written,
            },
            blocks: FileSum {
                len: self.written,
                crc32c: self.blk_crc,
            },
            index: FileSum {
                len: idx_len,
                crc32c: crc,
            },
        })
    }

    /// Bytes this writer holds in memory, as it charges them: the open block,
    /// the window, and the tables that grow with the part. A measure of the
    /// writer's own accounting; the tests check it against the process's
    /// resident memory.
    pub fn memory_bytes(&self) -> usize {
        self.open.cost()
            + self.kept_cost
            + self.lens.capacity()
            + self.block_first.capacity() * size_of::<u32>()
            + self.blocks.capacity() * size_of::<BlockLoc>()
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
    fn add_piece_says_whether_the_piece_is_new_and_find_never_stores() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), PieceWriterOptions::default()).unwrap();
        assert_eq!(w.find(b"x"), None);
        assert_eq!(w.piece_count(), 0);
        assert_eq!(w.add_piece(b"x").unwrap(), Added { id: 0, new: true });
        assert_eq!(w.add_piece(b"x").unwrap(), Added { id: 0, new: false });
        assert_eq!(w.find(b"x"), Some(0));
        assert_eq!(w.find(b"y"), None);
        assert_eq!(w.find(b""), None);
        assert_eq!(w.piece_count(), 1);
        let sum = w.finish().unwrap();
        // Every add and every successful find is one reference.
        assert_eq!(sum.totals.logical_bytes, 3);
        assert_eq!(sum.totals.pieces, 1);
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
    fn the_window_keeps_its_chains_whole_while_blocks_are_dropped() {
        // Equal hashes across many blocks, with a window that drops blocks all
        // the time: every id the map or a chain can reach must be a piece that
        // is still held, so that lookups never meet a dropped block.
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(200, 700)).unwrap();
        w.force_constant_hash();
        let mut rng = StdRng::seed_from_u64(77);
        let mut texts = Vec::new();
        for _ in 0..600 {
            // Few distinct texts, so most adds are repeats of something that
            // may or may not have been dropped.
            let n = rng.random_range(0..25u8);
            texts.push(vec![n; 30 + n as usize]);
            w.add(texts.last().unwrap()).unwrap();
            let mut at = w.map.get(&0).copied().unwrap_or(NONE);
            while at != NONE {
                let b = w.block(at).expect("a chain reaches a dropped block");
                at = b.chain[(at - b.first) as usize];
            }
        }
        let n = w.piece_count() as u32;
        w.finish().unwrap();
        // Every id up to the count reads back as some text that was added.
        for got in read_all(tmp.path(), &(0..n).collect::<Vec<_>>()) {
            assert!(texts.contains(&got));
        }
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
    fn many_short_pieces_are_found_by_id_and_cost_a_few_bytes_each() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(4_096, 1 << 20)).unwrap();
        let mut rng = StdRng::seed_from_u64(31);
        let texts: Vec<Vec<u8>> = (0..20_000u32)
            .map(|i| {
                // Mostly short, some of a few hundred bytes, all distinct.
                let mut t = vec![
                    0u8;
                    if i % 9 == 0 {
                        300
                    } else {
                        rng.random_range(8..40)
                    }
                ];
                rng.fill_bytes(&mut t);
                t.extend_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        for t in &texts {
            w.add(t).unwrap();
        }
        // The writer's table is the lengths as they are on disk.
        assert!(w.lens.len() < 2 * texts.len(), "{}", w.lens.len());
        let sum = w.finish().unwrap();
        assert_eq!(sum.totals.pieces, texts.len() as u64);

        let r = PieceReader::open(tmp.path()).unwrap();
        let mut session = r.session();
        for id in (0..texts.len()).rev().step_by(7) {
            let mut out = Vec::new();
            r.read_piece(&mut session, id as u32, &mut out).unwrap();
            assert_eq!(out, texts[id], "piece {id}");
        }
        // All in id order, by block.
        let mut seen = 0;
        r.read_sorted(0..texts.len() as u32, |n, bytes| {
            assert_eq!(bytes, texts[n], "piece {n}");
            seen += 1;
        })
        .unwrap();
        assert_eq!(seen, texts.len());
        assert!(
            r.memory_bytes() < 3 * texts.len(),
            "{} bytes for {} pieces",
            r.memory_bytes(),
            texts.len()
        );
    }

    #[test]
    fn read_sorted_reads_each_block_once_and_refuses_decreasing_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(100, 1 << 20)).unwrap();
        let texts: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; 30]).collect();
        for t in &texts {
            w.add(t).unwrap();
        }
        w.finish().unwrap();
        let r = PieceReader::open(tmp.path()).unwrap();
        // Every piece named three times, and in no particular grouping other
        // than sorted: each block is decompressed once.
        let ids: Vec<u32> = (0..50u32).flat_map(|i| [i, i, i]).collect();
        let mut n_seen = 0;
        r.read_sorted(ids.iter().copied(), |n, bytes| {
            assert_eq!(bytes, texts[ids[n] as usize]);
            n_seen += 1;
        })
        .unwrap();
        assert_eq!(n_seen, 150);
        assert_eq!(r.blocks_loaded() as usize, r.block_count());
        assert!(matches!(
            r.read_sorted([5u32, 4], |_, _| {}),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            r.read_sorted([50u32], |_, _| {}),
            Err(Error::Corrupt(_))
        ));
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

    #[test]
    fn the_recorded_sums_bind_the_files_to_the_summary() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(tmp.path(), small(64, 1 << 20)).unwrap();
        for i in 0..20u8 {
            w.add(&[i; 30]).unwrap();
        }
        let sum = w.finish().unwrap();
        PieceReader::open_recorded(tmp.path(), sum.blocks, sum.index)
            .unwrap()
            .verify()
            .unwrap();
        let wrong_crc = FileSum {
            crc32c: sum.index.crc32c ^ 1,
            ..sum.index
        };
        let wrong_len = FileSum {
            len: sum.index.len + 1,
            ..sum.index
        };
        for (blocks, index) in [
            (sum.blocks, wrong_crc),
            (sum.blocks, wrong_len),
            (
                FileSum {
                    len: sum.blocks.len + 1,
                    ..sum.blocks
                },
                sum.index,
            ),
        ] {
            let err = PieceReader::open_recorded(tmp.path(), blocks, index)
                .err()
                .unwrap();
            assert!(matches!(err, Error::Corrupt(_)), "{err}");
        }

        // The checksum of `pieces.idx` tells two stores apart. (Over the whole
        // file it would not: a file that ends with its own CRC-32C has the same
        // CRC-32C as every other such file.)
        let other = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(other.path(), small(64, 1 << 20)).unwrap();
        for i in 0..20u8 {
            w.add(&[i ^ 0x55; 30]).unwrap();
        }
        let theirs = w.finish().unwrap();
        assert_eq!(theirs.index.len, sum.index.len, "the lengths must be equal");
        assert_ne!(theirs.index.crc32c, sum.index.crc32c);
        let whole = |dir: &Path| crc32c::crc32c(&std::fs::read(dir.join(INDEX_FILE)).unwrap());
        assert_eq!(whole(tmp.path()), whole(other.path()));
        let err = PieceReader::open_recorded(other.path(), theirs.blocks, sum.index)
            .err()
            .unwrap();
        assert!(
            matches!(&err, Error::Corrupt(m) if m.contains("checksum") && m.contains(INDEX_FILE)),
            "{err}"
        );
    }
}
