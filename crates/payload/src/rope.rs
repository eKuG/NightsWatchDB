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
//! value with cuts    ->  0x01, then segments until the end:
//!                          varint(len << 1),     len bytes of inline data
//!                          varint(id << 1 | 1)   a reference to piece `id`
//! ```
//!
//! A value without cuts therefore costs one byte more than the value, and an
//! empty value costs nothing. A reference costs one to five bytes. Varints
//! are the usual little-endian base-128 form. Inline segments are never
//! empty, and two segments of one kind may follow each other (two adjacent
//! cuts are two references).

use nwdb_core::Error;
use std::ops::Range;

const TAG_INLINE: u8 = 0;
const TAG_SEGMENTS: u8 = 1;

/// Why a rope could not be read.
#[derive(Debug)]
pub enum RopeError {
    /// The rope's own bytes are wrong.
    Malformed(&'static str),
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
        Some((&TAG_SEGMENTS, rest)) => (rest, None),
        Some(_) => (&rope[..0], Some(RopeError::Malformed("unknown rope tag"))),
    };
    Segments {
        rest,
        whole: None,
        failed,
    }
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
/// the piece with the given id to the vector it is given.
pub fn decode(
    rope: &[u8],
    mut piece: impl FnMut(u32, &mut Vec<u8>) -> Result<(), Error>,
    out: &mut Vec<u8>,
) -> Result<(), RopeError> {
    for seg in segments(rope) {
        match seg? {
            Segment::Inline(b) => out.extend_from_slice(b),
            Segment::Piece(id) => piece(id, out).map_err(RopeError::Piece)?,
        }
    }
    Ok(())
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
        assert_eq!(out, [TAG_SEGMENTS, 11]);
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
            &[1, 0x80],
            &[1, 20, 1],
            &[1, 0],
            &[1, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f],
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
