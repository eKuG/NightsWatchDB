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
//! table, and the table costs one or two bytes per piece. The decoder
//! rebuilds them and refuses a table whose lengths do not fill the blocks
//! exactly.

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

/// What `pieces.idx` holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Index {
    pub blocks: Vec<BlockLoc>,
    pub pieces: Vec<PieceLoc>,
    pub logical_bytes: u64,
}

impl Index {
    /// The length `pieces.blk` must have.
    pub fn blk_len(&self) -> u64 {
        self.blocks
            .last()
            .map_or(0, |b| b.offset + u64::from(b.clen))
    }

    pub fn unique_bytes(&self) -> u64 {
        self.pieces.iter().map(|p| u64::from(p.len)).sum()
    }
}

pub(crate) fn corrupt(msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{INDEX_FILE}: {msg}"))
}

/// Encodes an index whose pieces fill its blocks in order, as the writer's
/// do.
pub(crate) fn encode(index: &Index) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        HEADER_LEN + index.blocks.len() * BLOCK_LEN + index.pieces.len() * 2 + CRC_LEN,
    );
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(index.blocks.len() as u64).to_le_bytes());
    out.extend_from_slice(&(index.pieces.len() as u64).to_le_bytes());
    out.extend_from_slice(&index.logical_bytes.to_le_bytes());
    for b in &index.blocks {
        out.extend_from_slice(&b.offset.to_le_bytes());
        out.extend_from_slice(&b.clen.to_le_bytes());
        out.extend_from_slice(&b.ulen.to_le_bytes());
        out.extend_from_slice(&b.crc.to_le_bytes());
    }
    for p in &index.pieces {
        let mut v = p.len;
        while v >= 0x80 {
            out.push((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
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
    let pieces = decode_pieces(&body[blocks_end..], pieces as usize, &blocks)?;
    Ok(Index {
        blocks,
        pieces,
        logical_bytes,
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

/// Reads `count` varint lengths and places each piece in the blocks, which
/// they must fill exactly.
fn decode_pieces(mut rest: &[u8], count: usize, blocks: &[BlockLoc]) -> Result<Vec<PieceLoc>> {
    let mut pieces = Vec::with_capacity(count);
    let (mut block, mut used) = (0usize, 0u32);
    for i in 0..count {
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
        }
        let Some(b) = blocks.get(block) else {
            return Err(corrupt(format!("piece {i} is past the last block")));
        };
        if used.checked_add(len).is_none_or(|end| end > b.ulen) {
            return Err(corrupt(format!("piece {i} does not fit in its block")));
        }
        pieces.push(PieceLoc {
            block: block as u32,
            offset: used,
            len,
        });
        used += len;
    }
    if !rest.is_empty() {
        return Err(corrupt("bytes left after the last piece"));
    }
    let filled = if pieces.is_empty() {
        blocks.is_empty()
    } else {
        block + 1 == blocks.len() && blocks[block].ulen == used
    };
    if !filled {
        return Err(corrupt("the pieces do not fill the blocks exactly"));
    }
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn piece(block: u32, offset: u32, len: u32) -> PieceLoc {
        PieceLoc { block, offset, len }
    }

    fn block(offset: u64, clen: u32, ulen: u32) -> BlockLoc {
        BlockLoc {
            offset,
            clen,
            ulen,
            crc: 1,
        }
    }

    fn sample() -> Index {
        Index {
            blocks: vec![block(0, 10, 30), block(10, 5, 7)],
            pieces: vec![piece(0, 0, 10), piece(0, 10, 20), piece(1, 0, 7)],
            logical_bytes: 99,
        }
    }

    #[test]
    fn round_trip_and_sizes() {
        let idx = sample();
        let bytes = encode(&idx);
        assert_eq!(bytes.len(), 32 + 2 * 20 + 3 + 4);
        assert_eq!(decode(&bytes).unwrap(), idx);
        assert_eq!(idx.blk_len(), 15);
        assert_eq!(idx.unique_bytes(), 37);
        let empty = Index {
            blocks: vec![],
            pieces: vec![],
            logical_bytes: 0,
        };
        assert_eq!(decode(&encode(&empty)).unwrap(), empty);
    }

    #[test]
    fn every_truncation_and_bit_flip_is_refused() {
        let bytes = encode(&sample());
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
        let mut hole = sample();
        hole.blocks[1].offset = 11;
        let mut short = sample();
        short.blocks[1].ulen = 8;
        let mut long = sample();
        long.blocks[0].ulen = 29;
        let mut spill = sample();
        spill.pieces[1].len = 21;
        let mut empty_piece = sample();
        empty_piece.pieces[0].len = 0;
        let mut missing = sample();
        missing.pieces.pop();
        let mut extra = sample();
        extra.pieces.push(piece(1, 7, 1));
        for bad in [hole, short, long, spill, empty_piece, missing, extra] {
            assert!(decode(&encode(&bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn long_lengths_use_few_bytes() {
        let idx = Index {
            blocks: vec![block(0, 9, 305)],
            pieces: vec![piece(0, 0, 300), piece(0, 300, 5)],
            logical_bytes: 305,
        };
        assert_eq!(encode(&idx).len(), 32 + 20 + 3 + 4);
        assert_eq!(decode(&encode(&idx)).unwrap(), idx);
    }
}
