//! Varints, bit packing and posting-list blocks, shared by the index and the
//! id-list store.
//!
//! A list of ascending ids is stored as gaps: `gap = id - previous id - 1`,
//! with "previous id" taken as -1 before the first id. The first gap is
//! therefore the first id itself, and a run of consecutive ids has gap 0 and
//! packs to width 0. The ids are cut into blocks of [`BLOCK`]; every block
//! has one bit width for its gaps and can be decoded knowing only the last id
//! of the block before it.
//!
//! Everything that reads bytes from a file is bounds checked and returns
//! [`Error::Corrupt`]; nothing here indexes a slice with a number it has not
//! checked.

use nwdb_core::{Error, Result};

/// Ids per packed block.
pub const BLOCK: usize = 128;

/// Bytes of one skip entry: last id of the block, then the block's start
/// relative to the end of the skip table, both `u32`.
pub const SKIP_ENTRY: usize = 8;

pub use nwdb_core::put_varint;

/// Appends the ids of an inline list, strictly ascending, as varint gaps:
/// each id minus one past the one before it.
pub fn put_inline(out: &mut Vec<u8>, ids: &[u32]) {
    let mut next_min = 0u64;
    for &id in ids {
        put_varint(out, u64::from(id) - next_min);
        next_min = u64::from(id) + 1;
    }
}

/// Reads a varint at `*pos`, advancing it. A varint longer than 10 bytes, one
/// that overflows 64 bits, or one cut off by the end of `b` is corrupt.
pub fn get_varint(b: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    for i in 0..10 {
        let Some(&x) = b.get(*pos) else {
            return Err(Error::corrupt("varint runs past the end of its data"));
        };
        *pos += 1;
        let bits = u64::from(x & 0x7f);
        if i == 9 && bits > 1 {
            return Err(Error::corrupt("varint overflows 64 bits"));
        }
        v |= bits << (7 * i);
        if x < 0x80 {
            return Ok(v);
        }
    }
    Err(Error::corrupt("varint longer than 10 bytes"))
}

/// Reads a varint that must fit in `u32`.
pub fn get_varint_u32(b: &[u8], pos: &mut usize) -> Result<u32> {
    u32::try_from(get_varint(b, pos)?).map_err(|_| Error::corrupt("varint does not fit in 32 bits"))
}

/// Smallest bit width that holds every value (0 if all are zero).
pub fn width_of(vals: &[u32]) -> u8 {
    (32 - vals.iter().fold(0, |a, &v| a | v).leading_zeros()) as u8
}

/// Bytes needed for `n` values of `w` bits each.
pub fn packed_len(n: usize, w: u8) -> usize {
    (n * w as usize).div_ceil(8)
}

/// Appends `vals` as `w`-bit fields, least significant bit first.
pub fn pack(vals: &[u32], w: u8, out: &mut Vec<u8>) {
    let (mut acc, mut nb) = (0u64, 0u32);
    for &v in vals {
        acc |= u64::from(v) << nb;
        nb += u32::from(w);
        while nb >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            nb -= 8;
        }
    }
    if nb > 0 {
        out.push(acc as u8);
    }
}

/// Reads `out.len()` fields of `w` bits (at most 32) from `bytes`. The caller
/// has checked that `bytes` holds at least `packed_len(out.len(), w)` bytes;
/// a shorter slice reads as zeros rather than panicking.
pub fn unpack(bytes: &[u8], w: u8, out: &mut [u32]) {
    if w == 0 {
        out.fill(0);
        return;
    }
    let (w, mask) = (u32::from(w), (1u64 << w) - 1);
    let (mut acc, mut nb, mut p) = (0u64, 0u32, 0usize);
    for o in out.iter_mut() {
        while nb < w {
            acc |= u64::from(bytes.get(p).copied().unwrap_or(0)) << nb;
            p += 1;
            nb += 8;
        }
        *o = (acc & mask) as u32;
        acc >>= w;
        nb -= w;
    }
}

/// Appends the blocks of `ids` (strictly ascending): a width byte, then the
/// packed gaps. `on_block(last_id, start)` is called for each block with the
/// block's last id and its start offset relative to where this call began.
pub fn encode_blocks(ids: &[u32], out: &mut Vec<u8>, mut on_block: impl FnMut(u32, usize)) {
    let base = out.len();
    let (mut next_min, mut gaps) = (0u64, [0u32; BLOCK]);
    for chunk in ids.chunks(BLOCK) {
        let start = out.len() - base;
        for (g, &id) in gaps.iter_mut().zip(chunk) {
            *g = (u64::from(id) - next_min) as u32;
            next_min = u64::from(id) + 1;
        }
        let w = width_of(&gaps[..chunk.len()]);
        out.push(w);
        pack(&gaps[..chunk.len()], w, out);
        on_block(chunk[chunk.len() - 1], start);
    }
}

/// Appends a posting list: the skip table (one entry per block), then the
/// blocks. Returns the number of bytes appended.
pub fn encode_list(ids: &[u32], out: &mut Vec<u8>) -> Result<usize> {
    let begin = out.len();
    let mut skips = Vec::with_capacity(ids.len().div_ceil(BLOCK));
    let mut blocks = Vec::new();
    encode_blocks(ids, &mut blocks, |last, start| skips.push((last, start)));
    for (last, start) in skips {
        let start = u32::try_from(start)
            .map_err(|_| Error::invalid_argument("posting list is larger than 4 GiB"))?;
        out.extend_from_slice(&last.to_le_bytes());
        out.extend_from_slice(&start.to_le_bytes());
    }
    out.extend_from_slice(&blocks);
    Ok(out.len() - begin)
}

/// Decodes one block of `n` ids (1 to [`BLOCK`]) from the start of `bytes`,
/// appending them to `out`. `next_min` is the smallest id the block may start
/// with (the previous block's last id plus one, or 0). Returns the bytes the
/// block occupies. Ids that would pass `u32::MAX` are corrupt.
pub fn decode_block(bytes: &[u8], n: usize, next_min: u64, out: &mut Vec<u32>) -> Result<usize> {
    let Some(&w) = bytes.first() else {
        return Err(Error::corrupt("posting block is empty"));
    };
    if w > 32 {
        return Err(Error::corrupt("posting block width is more than 32 bits"));
    }
    if n == 0 || n > BLOCK {
        return Err(Error::corrupt("posting block has an impossible length"));
    }
    let used = 1 + packed_len(n, w);
    let Some(packed) = bytes.get(1..used) else {
        return Err(Error::corrupt(
            "posting block runs past the end of its data",
        ));
    };
    let mut gaps = [0u32; BLOCK];
    unpack(packed, w, &mut gaps[..n]);
    let mut next = next_min;
    for &g in &gaps[..n] {
        let id = next + u64::from(g);
        if id > u64::from(u32::MAX) {
            return Err(Error::corrupt("posting list passes the largest id"));
        }
        out.push(id as u32);
        next = id + 1;
    }
    Ok(used)
}

/// Largest size of a block of `n` ids.
pub fn block_max(n: usize) -> usize {
    1 + 4 * n
}
