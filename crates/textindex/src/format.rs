//! Byte layout of an index file: the directory of segments and the footer.
//! The README gives the full layout.

use nwdb_core::{Error, Result};

pub const MAGIC: &[u8; 4] = b"NWTX";
pub const VERSION: u32 = 1;

/// Terms per dictionary block.
pub const TERMS_PER_BLOCK: u32 = 32;

/// Posting lists with at most this many ids live inside the dictionary entry.
pub const INLINE_MAX: usize = 8;

/// Bytes of a directory entry.
pub const DIR_ENTRY: usize = 72;

/// Bytes of the footer.
pub const FOOTER: usize = 64;

/// The most a dictionary block can occupy: 32 entries of at most 64 suffix
/// bytes, two length varints, a count varint and eight 5-byte gaps or a
/// 10-byte offset. Larger than this is corrupt, so a damaged sparse index can
/// never make the reader allocate more.
pub const MAX_DICT_BLOCK: u64 = 32 * (64 + 10 + 10 + 10 + 40);

/// What the footer says about the whole file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FooterFields {
    pub segments: u32,
    pub docs: u64,
    pub text_bytes: u64,
    pub terms: u64,
    pub postings: u64,
    pub directory_offset: u64,
}

/// What the directory says about one segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirEntry {
    pub offset: u64,
    pub postings_len: u64,
    pub dict_len: u64,
    pub sparse_len: u64,
    pub terms: u64,
    pub postings: u64,
    pub docs: u32,
    pub doc_lo: u32,
    /// The largest document id in the segment (inclusive).
    pub doc_hi: u32,
    pub dict_blocks: u32,
    pub body_crc: u32,
    pub sparse_crc: u32,
}

impl DirEntry {
    pub fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        for v in [
            self.postings_len,
            self.dict_len,
            self.sparse_len,
            self.terms,
            self.postings,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in [
            self.docs,
            self.doc_lo,
            self.doc_hi,
            self.dict_blocks,
            self.body_crc,
            self.sparse_crc,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&self.offset.to_le_bytes());
        debug_assert_eq!(out.len() - start, DIR_ENTRY);
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let b: &[u8; DIR_ENTRY] = b
            .try_into()
            .map_err(|_| Error::corrupt("directory entry has the wrong size"))?;
        Ok(Self {
            postings_len: le64(b, 0),
            dict_len: le64(b, 8),
            sparse_len: le64(b, 16),
            terms: le64(b, 24),
            postings: le64(b, 32),
            docs: le32(b, 40),
            doc_lo: le32(b, 44),
            doc_hi: le32(b, 48),
            dict_blocks: le32(b, 52),
            body_crc: le32(b, 56),
            sparse_crc: le32(b, 60),
            offset: le64(b, 64),
        })
    }

    /// Where the segment ends, or `None` if the lengths overflow.
    pub fn end(&self) -> Option<u64> {
        self.offset
            .checked_add(self.postings_len)?
            .checked_add(self.dict_len)?
            .checked_add(self.sparse_len)
    }
}

/// Encodes the footer. `crc` covers the directory bytes then the footer bytes
/// before the checksum.
pub fn encode_footer(f: &FooterFields, directory: &[u8]) -> [u8; FOOTER] {
    let mut b = [0u8; FOOTER];
    b[0..4].copy_from_slice(&VERSION.to_le_bytes());
    b[4..8].copy_from_slice(&f.segments.to_le_bytes());
    b[8..12].copy_from_slice(&TERMS_PER_BLOCK.to_le_bytes());
    // bytes 12..16 are reserved and zero
    b[16..24].copy_from_slice(&f.docs.to_le_bytes());
    b[24..32].copy_from_slice(&f.text_bytes.to_le_bytes());
    b[32..40].copy_from_slice(&f.terms.to_le_bytes());
    b[40..48].copy_from_slice(&f.postings.to_le_bytes());
    b[48..56].copy_from_slice(&f.directory_offset.to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(directory), &b[..56]);
    b[56..60].copy_from_slice(&crc.to_le_bytes());
    b[60..64].copy_from_slice(MAGIC);
    b
}

/// Checks magic and version and returns the fields and the stored checksum.
/// The caller checks the checksum once it has read the directory.
pub fn decode_footer(b: &[u8; FOOTER]) -> Result<(FooterFields, u32)> {
    if &b[60..64] != MAGIC {
        return Err(Error::corrupt("not an index file: bad magic"));
    }
    if le32(b, 0) != VERSION {
        return Err(Error::corrupt(format!(
            "unknown index format version {}",
            le32(b, 0)
        )));
    }
    if le32(b, 8) != TERMS_PER_BLOCK {
        return Err(Error::corrupt("unexpected terms per dictionary block"));
    }
    if le32(b, 12) != 0 {
        return Err(Error::corrupt("reserved footer bytes are not zero"));
    }
    Ok((
        FooterFields {
            segments: le32(b, 4),
            docs: le64(b, 16),
            text_bytes: le64(b, 24),
            terms: le64(b, 32),
            postings: le64(b, 40),
            directory_offset: le64(b, 48),
        },
        le32(b, 56),
    ))
}

/// Checksum the footer should carry for this directory and these footer bytes.
pub fn footer_crc(directory: &[u8], footer: &[u8; FOOTER]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(directory), &footer[..56])
}

pub fn le32<const N: usize>(b: &[u8; N], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

pub fn le64<const N: usize>(b: &[u8; N], at: usize) -> u64 {
    let mut x = [0u8; 8];
    x.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(x)
}
