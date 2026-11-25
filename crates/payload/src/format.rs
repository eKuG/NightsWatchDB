//! The layout of `pieces.idx`, the block table and the piece table of a piece
//! store.
//!
//! ```text
//! offset        size  field
//! 0             4     magic "NWPI"
//! 4             4     version, u32 = 1
//! 8             8     block count B, u64
//! 16            8     piece count P, u64
//! 24            8     logical bytes, u64: what all references add up to
//! 32            20*B  blocks: file offset u64, compressed length u32,
//!                     uncompressed length u32, CRC-32C of the compressed
//!                     bytes u32
//! 32+20B        ...   P piece lengths, each a varint, in piece id order
//! end-4         4     CRC-32C of every byte before it
//! ```
//!
//! The blocks lie one after the other in `pieces.blk` with no gaps. The
//! pieces fill the blocks in order of their ids: a piece starts where the
//! previous one ended, and a block is full exactly when its pieces add up to
//! its uncompressed length, after which the next piece starts the next block.
//! So a piece's block and offset follow from the lengths and the block
//! table, and the table costs one to three bytes per piece. The decoder
//! refuses a table whose lengths do not fill the blocks exactly.
//!
//! A reader keeps the table as it is on disk and finds a piece with a small
//! index over it: the first piece of each block, and a checkpoint (position in
//! the table and offset in the block) for every 64th piece of a block.

use nwdb_core::{Error, Result};

/// File name of the blocks inside a part directory.
pub const BLOCKS_FILE: &str = "pieces.blk";
/// File name of the block and piece tables inside a part directory.
pub const INDEX_FILE: &str = "pieces.idx";

/// The longest piece, and so the longest block, in bytes. Lengths and
/// offsets are `u32` on disk, and a decoder never allocates more than this
/// for one block.
pub const MAX_PIECE_BYTES: usize = 1 << 30;

const MAGIC: &[u8; 4] = b"NWPI";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 32;
const BLOCK_LEN: usize = 20;
const CRC_LEN: usize = 4;

/// A piece is found by scanning at most this many lengths from a checkpoint,
/// so the checkpoints cost 8 bytes for every 64 pieces.
const CHECKPOINT_EVERY: usize = 64;

/// Where one compressed block lies in `pieces.blk`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockLoc {
    pub offset: u64,
    pub clen: u32,
    pub ulen: u32,
    pub crc: u32,
}

/// Where one piece lies in the uncompressed bytes of a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PieceLoc {
    pub block: u32,
    pub offset: u32,
    pub len: u32,
}

/// What a reader keeps about the pieces of one block: the id of the first,
/// where its length starts in the table, and where the block's checkpoints
/// start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockPieces {
    first: u32,
    pos: u32,
    checkpoints: u32,
}

/// A piece whose length starts at byte `pos` of the table and which starts
/// `offset` bytes into its block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Checkpoint {
    pos: u32,
    offset: u32,
}

/// What `pieces.idx` holds, in the form a reader keeps it: the block table
/// and the piece table as it is on disk, with a small index over it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Index {
    pub blocks: Vec<BlockLoc>,
    pub logical_bytes: u64,
    pub unique_bytes: u64,
    pub piece_count: u64,
    table: Vec<u8>,
    by_block: Vec<BlockPieces>,
    checkpoints: Vec<Checkpoint>,
}

impl Index {
    /// The length `pieces.blk` must have.
    pub fn blk_len(&self) -> u64 {
        self.blocks
            .last()
            .map_or(0, |b| b.offset + u64::from(b.clen))
    }

    /// Heap bytes the reader holds for the tables.
    pub fn memory_bytes(&self) -> usize {
        self.blocks.capacity() * size_of::<BlockLoc>()
            + self.table.capacity()
            + self.by_block.capacity() * size_of::<BlockPieces>()
            + self.checkpoints.capacity() * size_of::<Checkpoint>()
    }

    /// Finds piece `id`. Its block is a binary search over the first piece
    /// of each block; its offset is found by adding up at most 63 lengths
    /// from the nearest checkpoint. `None` if there is no such piece.
    pub fn locate(&self, id: u32) -> Option<PieceLoc> {
        if u64::from(id) >= self.piece_count {
            return None;
        }
        let block = self.by_block.partition_point(|b| b.first <= id) - 1;
        let b = self.by_block[block];
        let k = (id - b.first) as usize;
        let (pos, mut offset) = match k / CHECKPOINT_EVERY {
            0 => (b.pos, 0u32),
            n => {
                let c = self.checkpoints[b.checkpoints as usize + n - 1];
                (c.pos, c.offset)
            }
        };
        let mut rest = self.table.get(pos as usize..)?;
        for _ in 0..k % CHECKPOINT_EVERY {
            offset = offset.checked_add(get_len(&mut rest)?)?;
        }
        Some(PieceLoc {
            block: block as u32,
            offset,
            len: get_len(&mut rest)?,
        })
    }
}

/// Appends `v` as a little-endian base-128 varint.
pub(crate) fn put_len(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Reads one length from the front of `rest`. The table was walked at open,
/// so this fails only for a table that was not.
fn get_len(rest: &mut &[u8]) -> Option<u32> {
    let mut len = 0u32;
    for shift in (0..35).step_by(7) {
        let (&b, tail) = rest.split_first()?;
        *rest = tail;
        len |= u32::from(b & 0x7f).checked_shl(shift)?;
        if b < 0x80 {
            return Some(len);
        }
    }
    None
}

pub(crate) fn corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{INDEX_FILE}: {msg}"))
}

/// Encodes the file from the blocks and the piece lengths, which are already
/// varints in id order. The pieces must fill the blocks, as the writer's do.
pub(crate) fn encode(blocks: &[BlockLoc], lens: &[u8], pieces: u64, logical_bytes: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + blocks.len() * BLOCK_LEN + lens.len() + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(blocks.len() as u64).to_le_bytes());
    out.extend_from_slice(&pieces.to_le_bytes());
    out.extend_from_slice(&logical_bytes.to_le_bytes());
    for b in blocks {
        out.extend_from_slice(&b.offset.to_le_bytes());
        out.extend_from_slice(&b.clen.to_le_bytes());
        out.extend_from_slice(&b.ulen.to_le_bytes());
        out.extend_from_slice(&b.crc.to_le_bytes());
    }
    out.extend_from_slice(lens);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Decodes and checks the whole file.
pub(crate) fn decode(bytes: &[u8]) -> Result<Index> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("bad magic"));
    }
    let le_u32 = |b: &[u8]| u32::from_le_bytes(b.try_into().expect("4 bytes"));
    let le_u64 = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8 bytes"));
    let version = le_u32(&bytes[4..8]);
    if version != VERSION {
        return Err(corrupt(format!(
            "version {version}, but this build reads version {VERSION}"
        )));
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32c::crc32c(body) != le_u32(crc) {
        return Err(corrupt("checksum mismatch"));
    }
    let (blocks, pieces) = (le_u64(&body[8..16]), le_u64(&body[16..24]));
    let logical_bytes = le_u64(&body[24..32]);
    // Every piece length takes at least one byte, which bounds both counts
    // by the file's size before anything is allocated.
    let blocks_end = blocks
        .checked_mul(BLOCK_LEN as u64)
        .and_then(|n| n.checked_add(HEADER_LEN as u64))
        .filter(|&end| end <= body.len() as u64)
        .ok_or_else(|| corrupt(format!("{blocks} blocks do not fit in the file")))?
        as usize;
    if pieces > (body.len() - blocks_end) as u64 {
        return Err(corrupt(format!("{pieces} pieces do not fit in the file")));
    }
    // Positions in the table are `u32`.
    if body.len() - blocks_end > u32::MAX as usize {
        return Err(corrupt("the piece table is too long"));
    }
    let blocks: Vec<BlockLoc> = body[HEADER_LEN..blocks_end]
        .as_chunks::<BLOCK_LEN>()
        .0
        .iter()
        .map(|r| BlockLoc {
            offset: le_u64(&r[0..8]),
            clen: le_u32(&r[8..12]),
            ulen: le_u32(&r[12..16]),
            crc: le_u32(&r[16..20]),
        })
        .collect();
    check_blocks(&blocks)?;
    let table = body[blocks_end..].to_vec();
    let walked = walk_pieces(&table, pieces as usize, &blocks)?;
    Ok(Index {
        blocks,
        logical_bytes,
        unique_bytes: walked.unique,
        piece_count: pieces,
        table,
        by_block: walked.by_block,
        checkpoints: walked.checkpoints,
    })
}

/// Blocks follow each other in the file.
fn check_blocks(blocks: &[BlockLoc]) -> Result<()> {
    let mut file_at = 0u64;
    for (i, b) in blocks.iter().enumerate() {
        if b.offset != file_at {
            return Err(corrupt(format!(
                "block {i} does not start where the previous block ends"
            )));
        }
        if b.clen == 0 || b.ulen == 0 || b.ulen as usize > MAX_PIECE_BYTES {
            return Err(corrupt(format!(
                "block {i} has an impossible length ({} compressed, {} uncompressed)",
                b.clen, b.ulen
            )));
        }
        file_at += u64::from(b.clen);
    }
    Ok(())
}

struct Walked {
    by_block: Vec<BlockPieces>,
    checkpoints: Vec<Checkpoint>,
    unique: u64,
}

/// Reads `count` varint lengths from `table` and places each piece in the
/// blocks, which they must fill exactly; builds the index over them. A table
/// that has more than `u32::MAX` pieces is refused: ids are `u32`.
fn walk_pieces(table: &[u8], count: usize, blocks: &[BlockLoc]) -> Result<Walked> {
    if count as u64 > u64::from(u32::MAX) {
        return Err(corrupt("more pieces than a part can hold"));
    }
    let mut rest = table;
    let mut walked = Walked {
        by_block: Vec::with_capacity(blocks.len()),
        checkpoints: Vec::new(),
        unique: 0,
    };
    let (mut block, mut used, mut in_block) = (0usize, 0u32, 0usize);
    for i in 0..count {
        let pos = (table.len() - rest.len()) as u32;
        let mut len = 0u64;
        let mut shift = 0;
        loop {
            let Some((&b, tail)) = rest.split_first() else {
                return Err(corrupt(format!("piece {i}: the table ends early")));
            };
            rest = tail;
            len |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
            if shift > 28 {
                return Err(corrupt(format!("piece {i}: length is too long a number")));
            }
        }
        let len = u32::try_from(len)
            .ok()
            .filter(|&l| l > 0 && l as usize <= MAX_PIECE_BYTES)
            .ok_or_else(|| corrupt(format!("piece {i} has an impossible length")))?;
        if blocks.get(block).is_some_and(|b| b.ulen == used) {
            block += 1;
            used = 0;
            in_block = 0;
        }
        let Some(b) = blocks.get(block) else {
            return Err(corrupt(format!("piece {i} is past the last block")));
        };
        if used.checked_add(len).is_none_or(|end| end > b.ulen) {
            return Err(corrupt(format!("piece {i} does not fit in its block")));
        }
        if in_block == 0 {
            walked.by_block.push(BlockPieces {
                first: i as u32,
                pos,
                checkpoints: walked.checkpoints.len() as u32,
            });
        } else if in_block % CHECKPOINT_EVERY == 0 {
            walked.checkpoints.push(Checkpoint { pos, offset: used });
        }
        in_block += 1;
        used += len;
        walked.unique += u64::from(len);
    }
    if !rest.is_empty() {
        return Err(corrupt("bytes left after the last piece"));
    }
    let filled = if count == 0 {
        blocks.is_empty()
    } else {
        block + 1 == blocks.len() && blocks[block].ulen == used
    };
    if !filled {
        return Err(corrupt("the pieces do not fill the blocks exactly"));
    }
    walked.checkpoints.shrink_to_fit();
    Ok(walked)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(offset: u64, clen: u32, ulen: u32) -> BlockLoc {
        BlockLoc {
            offset,
            clen,
            ulen,
            crc: 1,
        }
    }

    fn lens(of: &[u32]) -> Vec<u8> {
        let mut out = Vec::new();
        for &l in of {
            put_len(&mut out, l);
        }
        out
    }

    /// Two blocks of 30 and 7 bytes holding pieces of 10, 20 and 7 bytes.
    fn sample() -> (Vec<BlockLoc>, Vec<u8>) {
        (vec![block(0, 10, 30), block(10, 5, 7)], lens(&[10, 20, 7]))
    }

    #[test]
    fn round_trip_and_sizes() {
        let (blocks, table) = sample();
        let bytes = encode(&blocks, &table, 3, 99);
        assert_eq!(bytes.len(), 32 + 2 * 20 + 3 + 4);
        let idx = decode(&bytes).unwrap();
        assert_eq!(idx.blocks, blocks);
        assert_eq!(idx.blk_len(), 15);
        assert_eq!(idx.unique_bytes, 37);
        assert_eq!(idx.logical_bytes, 99);
        let at = |id| {
            let p = idx.locate(id).unwrap();
            (p.block, p.offset, p.len)
        };
        assert_eq!((at(0), at(1), at(2)), ((0, 0, 10), (0, 10, 20), (1, 0, 7)));
        assert!(idx.locate(3).is_none());
        let empty = decode(&encode(&[], &[], 0, 0)).unwrap();
        assert!(empty.locate(0).is_none());
        assert_eq!(empty.piece_count, 0);
    }

    #[test]
    fn every_truncation_and_bit_flip_is_refused() {
        let (blocks, table) = sample();
        let bytes = encode(&blocks, &table, 3, 99);
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n]).is_err(), "{n}");
        }
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut b = bytes.clone();
                b[i] ^= 1 << bit;
                assert!(decode(&b).is_err(), "byte {i} bit {bit}");
            }
        }
    }

    #[test]
    fn tables_that_do_not_tile_are_refused_even_with_a_good_checksum() {
        let (blocks, table) = sample();
        let with = |blocks: Vec<BlockLoc>, table: &[u8], pieces: u64| {
            decode(&encode(&blocks, table, pieces, 0)).is_err()
        };
        let mut hole = blocks.clone();
        hole[1].offset = 11;
        let mut short = blocks.clone();
        short[1].ulen = 8;
        let mut long = blocks.clone();
        long[0].ulen = 29;
        assert!(with(hole, &table, 3));
        assert!(with(short, &table, 3));
        assert!(with(long, &table, 3));
        // A piece that spills out of its block, an empty piece, a missing
        // piece, an extra one, and a count that disagrees with the table.
        assert!(with(blocks.clone(), &lens(&[10, 21, 7]), 3));
        assert!(with(blocks.clone(), &lens(&[0, 30, 7]), 3));
        assert!(with(blocks.clone(), &lens(&[10, 20]), 2));
        assert!(with(blocks.clone(), &lens(&[10, 20, 7, 1]), 4));
        assert!(with(blocks.clone(), &table, 2));
        assert!(with(blocks, &table, 4));
    }

    #[test]
    fn long_lengths_use_few_bytes() {
        let blocks = vec![block(0, 9, 305)];
        let table = lens(&[300, 5]);
        assert_eq!(encode(&blocks, &table, 2, 305).len(), 32 + 20 + 3 + 4);
        let idx = decode(&encode(&blocks, &table, 2, 305)).unwrap();
        let p = idx.locate(1).unwrap();
        assert_eq!((p.offset, p.len), (300, 5));
    }

    #[test]
    fn checkpoints_find_every_piece_among_many() {
        // 5,000 pieces of varying length in blocks of varying size: every
        // piece must be found at the place the lengths put it.
        let mut blocks = Vec::new();
        let mut all = Vec::new();
        let mut file_at = 0u64;
        let mut id = 0u32;
        for b in 0..40u32 {
            let n = 1 + (b * 37) % 300;
            let mut used = 0u32;
            for _ in 0..n {
                let len = 1 + (id * 7919) % 400;
                all.push((blocks.len() as u32, used, len));
                used += len;
                id += 1;
            }
            blocks.push(block(file_at, 10, used));
            file_at += 10;
        }
        let table = lens(&all.iter().map(|p| p.2).collect::<Vec<_>>());
        let idx = decode(&encode(&blocks, &table, all.len() as u64, 0)).unwrap();
        assert!(all.len() > 3_000);
        for (id, want) in all.iter().enumerate() {
            let p = idx.locate(id as u32).unwrap();
            assert_eq!((p.block, p.offset, p.len), *want, "piece {id}");
        }
        assert!(idx.locate(all.len() as u32).is_none());
        // Two bytes a piece for the table, and a few for the index.
        assert!(
            idx.memory_bytes() < all.len() * 3 + 40 * 40,
            "{}",
            idx.memory_bytes()
        );
    }
}
