//! Varints, bit packing and posting-list blocks.
//!
//! A posting list of ascending ids is stored as gaps: gap = id - previous id - 1, where the previous
//! id before the first one is u32::MAX (wrapping), so the first gap is the first id itself and a run of
//! consecutive documents has gap 0.

pub const BLOCK: usize = 128;
/// Bytes of one skip entry: last id of the block (u32), block start relative to the list data (u32).
pub const SKIP: usize = 8;

pub fn put_varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push(v as u8 | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

pub fn get_varint(b: &[u8], p: &mut usize) -> u64 {
    let (mut v, mut s) = (0u64, 0);
    loop {
        let x = b[*p];
        *p += 1;
        v |= ((x & 0x7f) as u64) << s;
        if x < 0x80 {
            return v;
        }
        s += 7;
    }
}

/// Smallest bit width that holds every value (0 if all are zero).
pub fn width_of(vals: &[u32]) -> u8 {
    (32 - vals.iter().fold(0, |a, &v| a | v).leading_zeros()) as u8
}

/// Appends `vals` as `w`-bit little-endian fields.
pub fn pack(vals: &[u32], w: u8, out: &mut Vec<u8>) {
    let (mut acc, mut nb) = (0u64, 0u32);
    for &v in vals {
        acc |= (v as u64) << nb;
        nb += w as u32;
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

pub fn unpack(b: &[u8], w: u8, out: &mut [u32]) {
    if w == 0 {
        out.fill(0);
        return;
    }
    let (w, mask) = (w as u32, (1u64 << w) - 1);
    let (mut acc, mut nb, mut p) = (0u64, 0u32, 0usize);
    for o in out.iter_mut() {
        while nb < w {
            acc |= (b[p] as u64) << nb;
            p += 1;
            nb += 8;
        }
        *o = (acc & mask) as u32;
        acc >>= w;
        nb -= w;
    }
}

pub fn gap(id: u32, prev: u32) -> u32 {
    id.wrapping_sub(prev).wrapping_sub(1)
}

pub fn ungap(g: u32, prev: u32) -> u32 {
    prev.wrapping_add(g).wrapping_add(1)
}

/// Appends a posting list as: skip table (one entry per block of 128 ids), then the blocks, each
/// `width byte, packed gaps`.
pub fn encode_list(ids: &[u32], out: &mut Vec<u8>) {
    let nb = ids.len().div_ceil(BLOCK);
    let skip_at = out.len();
    out.resize(skip_at + SKIP * nb, 0);
    let data_at = out.len();
    let (mut prev, mut gaps) = (u32::MAX, [0u32; BLOCK]);
    for (bi, chunk) in ids.chunks(BLOCK).enumerate() {
        let start = out.len() - data_at;
        for (g, &id) in gaps.iter_mut().zip(chunk) {
            *g = gap(id, prev);
            prev = id;
        }
        let w = width_of(&gaps[..chunk.len()]);
        out.push(w);
        pack(&gaps[..chunk.len()], w, out);
        out[skip_at + SKIP * bi..][..4].copy_from_slice(&prev.to_le_bytes());
        out[skip_at + SKIP * bi + 4..][..4].copy_from_slice(&(start as u32).to_le_bytes());
    }
}

/// Decodes one block of `n` ids from `b` (starting at its width byte) given the last id of the
/// previous block (u32::MAX for the first block); appends to `out`.
pub fn decode_block(b: &[u8], n: usize, prev: u32, out: &mut Vec<u32>) {
    let mut gaps = [0u32; BLOCK];
    unpack(&b[1..], b[0], &mut gaps[..n]);
    let mut p = prev;
    for &g in &gaps[..n] {
        p = ungap(g, p);
        out.push(p);
    }
}

/// Upper bound of the bytes of a block of `n` ids.
pub fn block_max(n: usize) -> usize {
    1 + 4 * n
}
