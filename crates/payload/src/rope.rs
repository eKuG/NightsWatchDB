//! The rope: how a value is stored in a part when some of its bytes live in
//! the piece store.
//!
//! A rope is a sequence of segments, each either inline bytes or a reference
//! to a piece. Reading concatenates them. The cuts are made in the value's
//! original bytes and nothing is re-encoded, so the value is rebuilt byte for
//! byte by construction.
//!
//! ```text
//! empty value        ->  (no bytes)
//! value with no cuts ->  0x00, the value
//! value with cuts    ->  0x03, CRC-32C of the value (u32, little endian),
//!                        then segments until the end:
//!                          varint(len << 1),     len bytes of inline data
//!                          varint(id << 1 | 1)   a reference to piece `id`
//! ```
//!
//! A value without cuts therefore costs one byte more than the value, and an
//! empty value costs nothing. A value with cuts carries the checksum of the
//! bytes it rebuilds to, so that a damaged reference, which Parquet would not
//! notice, cannot splice in another value's text unseen: the reader checks it
//! after rebuilding. A reference costs one to five bytes. Varints are the
//! usual little-endian base-128 form. Inline segments are never empty, and
//! two segments of one kind may follow each other (two adjacent cuts are two
//! references).

use nwdb_core::Error;
use std::ops::Range;

const TAG_INLINE: u8 = 0;
/// Differs from `TAG_INLINE` in two bits, so that one flipped bit in the tag can
/// never turn a rope with references into a value that is taken as its own
/// bytes or the other way round.
const TAG_SEGMENTS: u8 = 3;
const CRC_LEN: usize = 4;

/// Why a rope could not be read.
#[derive(Debug)]
pub enum RopeError {
    /// The rope's own bytes are wrong.
    Malformed(&'static str),
    /// The rebuilt value does not have the checksum the rope carries.
    Checksum,
    /// Fetching a piece failed.
    Piece(Error),
}

/// One segment of a rope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Segment<'a> {
    /// Bytes stored in the rope itself.
    Inline(&'a [u8]),
    /// The piece with this id.
    Piece(u32),
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &mut &[u8]) -> Result<u64, RopeError> {
    let mut v = 0u64;
    for i in 0..10u32 {
        let Some((&b, tail)) = buf.split_first() else {
            return Err(RopeError::Malformed("truncated number"));
        };
        *buf = tail;
        if i == 9 && b > 1 {
            return Err(RopeError::Malformed("number longer than 64 bits"));
        }
        v |= u64::from(b & 0x7f) << (7 * i);
        if b < 0x80 {
            return Ok(v);
        }
    }
    Err(RopeError::Malformed("number longer than 10 bytes"))
}

/// Whether `cuts` satisfy the [`PayloadSplitter`](nwdb_core::PayloadSplitter)
/// contract for a value of `len` bytes: ascending, not overlapping, not
/// empty, inside the value.
pub fn cuts_are_valid(len: usize, cuts: &[Range<usize>]) -> bool {
    let mut at = 0;
    for c in cuts {
        if c.start < at || c.end <= c.start || c.end > len {
            return false;
        }
        at = c.end;
    }
    true
}

/// Appends the rope of a value that has no cuts.
pub fn encode_inline(value: &[u8], out: &mut Vec<u8>) {
    if value.is_empty() {
        return;
    }
    out.push(TAG_INLINE);
    out.extend_from_slice(value);
}

/// Appends the rope of `value` with `cuts` replaced by references: `ids[i]`
/// is the piece holding `value[cuts[i]]`. The cuts must be valid
/// ([`cuts_are_valid`]) and as many as the ids; with none, the result is the
/// same as [`encode_inline`].
pub fn encode_cut(value: &[u8], cuts: &[Range<usize>], ids: &[u32], out: &mut Vec<u8>) {
    debug_assert!(cuts_are_valid(value.len(), cuts) && cuts.len() == ids.len());
    if cuts.is_empty() {
        return encode_inline(value, out);
    }
    out.push(TAG_SEGMENTS);
    out.extend_from_slice(&crc32c::crc32c(value).to_le_bytes());
    let mut at = 0;
    for (cut, &id) in cuts.iter().zip(ids) {
        if cut.start > at {
            put_varint(out, ((cut.start - at) as u64) << 1);
            out.extend_from_slice(&value[at..cut.start]);
        }
        put_varint(out, (u64::from(id) << 1) | 1);
        at = cut.end;
    }
    if at < value.len() {
        put_varint(out, ((value.len() - at) as u64) << 1);
        out.extend_from_slice(&value[at..]);
    }
}

/// The segments of a rope, in order. Yields an error (once) at the first
/// malformed byte and then ends.
pub fn segments(rope: &[u8]) -> Segments<'_> {
    let (rest, failed) = match rope.split_first() {
        None => (&rope[..0], None),
        Some((&TAG_INLINE, rest)) => {
            return Segments {
                rest: &[],
                whole: Some(rest),
                failed: None,
            };
        }
        Some((&TAG_SEGMENTS, rest)) => match rest.split_at_checked(CRC_LEN) {
            Some((_, segments)) => (segments, None),
            None => (&rope[..0], Some(RopeError::Malformed("truncated checksum"))),
        },
        Some(_) => (&rope[..0], Some(RopeError::Malformed("unknown rope tag"))),
    };
    Segments {
        rest,
        whole: None,
        failed,
    }
}

/// The checksum a rope with cuts carries: the CRC-32C of the value it
/// rebuilds to. `None` for a rope without cuts, an empty one, or one too
/// short to hold it.
pub fn checksum(rope: &[u8]) -> Option<u32> {
    match rope.split_first() {
        Some((&TAG_SEGMENTS, rest)) => rest
            .first_chunk::<CRC_LEN>()
            .map(|c| u32::from_le_bytes(*c)),
        _ => None,
    }
}

/// The ids of the pieces a rope refers to, in order of appearance (an id that
/// appears twice is yielded twice). Yields an error (once) at the first
/// malformed byte and then ends.
pub fn piece_ids(rope: &[u8]) -> impl Iterator<Item = Result<u32, RopeError>> + '_ {
    segments(rope).filter_map(|s| match s {
        Ok(Segment::Piece(id)) => Some(Ok(id)),
        Ok(Segment::Inline(_)) => None,
        Err(e) => Some(Err(e)),
    })
}

/// Iterator returned by [`segments`].
pub struct Segments<'a> {
    rest: &'a [u8],
    /// The value of a rope without cuts, until it has been yielded.
    whole: Option<&'a [u8]>,
    failed: Option<RopeError>,
}

impl<'a> Iterator for Segments<'a> {
    type Item = Result<Segment<'a>, RopeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.failed.take() {
            return Some(Err(e));
        }
        if let Some(whole) = self.whole.take() {
            return (!whole.is_empty()).then_some(Ok(Segment::Inline(whole)));
        }
        if self.rest.is_empty() {
            return None;
        }
        let item = get_varint(&mut self.rest).and_then(|n| {
            if n & 1 == 1 {
                return u32::try_from(n >> 1)
                    .map(Segment::Piece)
                    .map_err(|_| RopeError::Malformed("piece id out of range"));
            }
            let len = usize::try_from(n >> 1)
                .map_err(|_| RopeError::Malformed("inline run longer than the rope"))?;
            if len == 0 {
                return Err(RopeError::Malformed("empty inline run"));
            }
            let Some((head, tail)) = self.rest.split_at_checked(len) else {
                return Err(RopeError::Malformed("inline run longer than the rope"));
            };
            self.rest = tail;
            Ok(Segment::Inline(head))
        });
        if item.is_err() {
            self.rest = &[];
        }
        Some(item)
    }
}

/// Appends the value a rope stands for to `out`. `piece` appends the bytes of
/// the piece with the given id to the vector it is given. If the rope carries
/// a checksum and the rebuilt value does not match it, the result is
/// [`RopeError::Checksum`] and `out` holds the bytes that failed: the caller
/// must not use them.
pub fn decode(
    rope: &[u8],
    mut piece: impl FnMut(u32, &mut Vec<u8>) -> Result<(), Error>,
    out: &mut Vec<u8>,
) -> Result<(), RopeError> {
    let start = out.len();
    for seg in segments(rope) {
        match seg? {
            Segment::Inline(b) => out.extend_from_slice(b),
            Segment::Piece(id) => piece(id, out).map_err(RopeError::Piece)?,
        }
    }
    match checksum(rope) {
        Some(want) if crc32c::crc32c(&out[start..]) != want => Err(RopeError::Checksum),
        _ => Ok(()),
    }
}

/// The length of the value a rope stands for, without reading any piece:
/// `piece_len` gives the length of one piece.
pub fn logical_len(
    rope: &[u8],
    mut piece_len: impl FnMut(u32) -> Result<u64, Error>,
) -> Result<u64, RopeError> {
    let mut total = 0u64;
    for seg in segments(rope) {
        total = total.saturating_add(match seg? {
            Segment::Inline(b) => b.len() as u64,
            Segment::Piece(id) => piece_len(id).map_err(RopeError::Piece)?,
        });
    }
    Ok(total)
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)] // the tests list one-range cut sets on purpose
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn pieces_of(value: &[u8], cuts: &[Range<usize>]) -> (Vec<Vec<u8>>, Vec<u32>) {
        let store: Vec<Vec<u8>> = cuts.iter().map(|c| value[c.clone()].to_vec()).collect();
        let ids = (0..cuts.len() as u32).collect();
        (store, ids)
    }

    fn round_trip(value: &[u8], cuts: &[Range<usize>]) -> Vec<u8> {
        let (store, ids) = pieces_of(value, cuts);
        let mut rope = Vec::new();
        encode_cut(value, cuts, &ids, &mut rope);
        let mut out = Vec::new();
        decode(
            &rope,
            |id, out| {
                out.extend_from_slice(&store[id as usize]);
                Ok(())
            },
            &mut out,
        )
        .unwrap();
        let len = logical_len(&rope, |id| Ok(store[id as usize].len() as u64)).unwrap();
        assert_eq!(len, value.len() as u64);
        out
    }

    #[test]
    fn fixed_shapes_round_trip() {
        let v = b"0123456789abcdef";
        let cases: Vec<(&[u8], Vec<Range<usize>>)> = vec![
            (b"", vec![]),
            (v, vec![]),
            (v, vec![0..16]),
            (v, vec![0..4, 4..8, 8..16]),
            (v, vec![3..4]),
            (v, vec![0..1, 15..16]),
            (v, vec![2..5, 6..9]),
            (b"x", vec![0..1]),
        ];
        for (value, cuts) in cases {
            assert_eq!(round_trip(value, &cuts), value, "{cuts:?}");
        }
    }

    #[test]
    fn overhead_is_small() {
        let mut out = Vec::new();
        encode_inline(b"", &mut out);
        assert!(out.is_empty());
        encode_inline(&[7u8; 1000], &mut out);
        assert_eq!(out.len(), 1001);
        out.clear();
        encode_cut(&[7u8; 1000], &[0..1000], &[5], &mut out);
        // The tag, the checksum of the value, one reference.
        assert_eq!(out.len(), 1 + 4 + 1);
        assert_eq!(out[0], TAG_SEGMENTS);
        assert_eq!(checksum(&out), Some(crc32c::crc32c(&[7u8; 1000])));
        assert_eq!(out[5], 11);
        assert_eq!(checksum(b""), None);
        assert_eq!(checksum(&[TAG_INLINE, 1, 2, 3, 4, 5]), None);
        assert_eq!(piece_ids(&out).collect::<Vec<_>>().len(), 1);
    }

    /// Six pieces: four of 8 bytes, two of 5.
    fn store() -> Vec<Vec<u8>> {
        let mut s: Vec<Vec<u8>> = (0..4u8).map(|i| vec![b'a' + i; 8]).collect();
        s.extend((4..6u8).map(|i| vec![b'a' + i; 5]));
        s
    }

    fn rebuild(rope: &[u8], store: &[Vec<u8>]) -> Result<Vec<u8>, RopeError> {
        let mut out = Vec::new();
        decode(
            rope,
            |id, out| match store.get(id as usize) {
                Some(p) => {
                    out.extend_from_slice(p);
                    Ok(())
                }
                None => Err(Error::corrupt("no such piece")),
            },
            &mut out,
        )
        .map(|()| out)
    }

    #[test]
    fn no_damaged_rope_rebuilds_to_other_bytes() {
        let store = store();
        let mut value = b"xy".to_vec();
        value.extend_from_slice(&store[0]);
        value.extend_from_slice(&store[1]);
        value.push(b'z');
        value.extend_from_slice(&store[2]);
        let cuts = [2..10, 10..18, 19..27];
        let mut good = Vec::new();
        encode_cut(&value, &cuts, &[0, 1, 2], &mut good);
        assert_eq!(rebuild(&good, &store).unwrap(), value);

        // Every bit of the rope, flipped: an error, or the same value.
        for i in 0..good.len() {
            for bit in 0..8 {
                let mut bad = good.clone();
                bad[i] ^= 1 << bit;
                if let Ok(got) = rebuild(&bad, &store) {
                    assert_eq!(got, value, "byte {i} bit {bit}");
                }
            }
        }

        // A reference to another piece of the same length, to one of another
        // length, the segments in another order, a repeated piece, one
        // missing, a piece that does not exist: each keeps the original
        // checksum and so each must fail.
        let forged = |ids: &[u32], cuts: &[Range<usize>], v: &[u8]| {
            let mut r = Vec::new();
            encode_cut(v, cuts, ids, &mut r);
            r[1..5].copy_from_slice(&good[1..5]);
            r
        };
        let mut v = value.clone();
        for (ids, what) in [
            (&[3u32, 1, 2][..], "same length"),
            (&[4, 1, 2], "another length"),
            (&[1, 0, 2], "reordered"),
            (&[0, 0, 2], "repeated"),
            (&[0, 1, 99], "missing piece"),
        ] {
            let r = forged(ids, &cuts, &v);
            assert!(
                matches!(
                    rebuild(&r, &store),
                    Err(RopeError::Checksum | RopeError::Piece(_))
                ),
                "{what}"
            );
        }
        // Fewer references than the value had, the rest dropped.
        v.truncate(18);
        let r = forged(&[0, 1], &cuts[..2], &v);
        assert!(matches!(rebuild(&r, &store), Err(RopeError::Checksum)));
        // A rope with no checksum bytes at all is malformed, not trusted.
        assert!(matches!(
            rebuild(&[TAG_SEGMENTS, 1, 2], &store),
            Err(RopeError::Malformed(_))
        ));
    }

    #[test]
    fn random_values_and_cuts_round_trip() {
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..2_000 {
            let len = rng.random_range(0..200usize);
            let value: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            let mut cuts = Vec::new();
            let mut at = 0;
            while at < len {
                let start = rng.random_range(at..=len);
                if start == len {
                    break;
                }
                let end = rng.random_range(start + 1..=len);
                cuts.push(start..end);
                at = end;
                if rng.random_bool(0.2) {
                    break;
                }
            }
            assert!(cuts_are_valid(len, &cuts));
            assert_eq!(round_trip(&value, &cuts), value);
        }
    }

    #[test]
    fn malformed_ropes_are_errors_not_panics() {
        let mut rng = StdRng::seed_from_u64(9);
        let none = |_: u32, _: &mut Vec<u8>| Ok(());
        for _ in 0..5_000 {
            let len = rng.random_range(0..40usize);
            let rope: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            let _ = decode(&rope, none, &mut Vec::new());
            let _ = logical_len(&rope, |_| Ok(1));
        }
        for bad in [
            &[2u8][..],
            &[3],
            &[3, 0, 0, 0],
            &[3, 0, 0, 0, 0, 0x80],
            &[3, 0, 0, 0, 0, 20, 1],
            &[3, 0, 0, 0, 0, 0],
            &[3, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f],
        ] {
            assert!(matches!(
                decode(bad, none, &mut Vec::new()),
                Err(RopeError::Malformed(_))
            ));
        }
    }

    #[test]
    fn contract_check() {
        assert!(cuts_are_valid(10, &[]));
        assert!(cuts_are_valid(10, &[0..10]));
        assert!(cuts_are_valid(10, &[0..5, 5..10]));
        assert!(!cuts_are_valid(10, &[5..5]));
        assert!(!cuts_are_valid(10, &[0..11]));
        assert!(!cuts_are_valid(10, &[4..6, 5..7]));
        assert!(!cuts_are_valid(10, &[6..8, 0..2]));
    }
}
