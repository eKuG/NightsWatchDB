//! `rope::translate` promises that a wrong map is caught, because the
//! checksum and the length are carried or checked, not recomputed. When every
//! reference of a rope becomes inline bytes, the rope is rewritten as a value
//! without cuts, which carries neither, so they are checked before that.

use nwdb_payload::rope::{self, Mapped, RopeError};

const VALUE: &[u8] = b"head 0123456789abcdef tail";

fn stored() -> Vec<u8> {
    let mut stored = Vec::new();
    let cut = 5..21;
    rope::encode_cut(VALUE, std::slice::from_ref(&cut), &[7], &mut stored);
    stored
}

#[test]
fn a_wrong_translation_to_inline_bytes_is_caught_when_read() {
    // Wrong bytes of another length, and wrong bytes of the right length.
    for wrong in [&b"other bytes"[..], b"0123456789abcdeX"] {
        let mut moved = Vec::new();
        let r = rope::translate(&stored(), |_| Ok(Mapped::Inline(wrong)), &mut moved);
        assert!(
            matches!(r, Err(RopeError::Length | RopeError::Checksum)),
            "{wrong:?}: {r:?}"
        );
    }
}

#[test]
fn the_right_bytes_translated_to_inline_read_back_as_the_value() {
    let mut moved = Vec::new();
    rope::translate(&stored(), |_| Ok(Mapped::Inline(&VALUE[5..21])), &mut moved).unwrap();
    let mut back = Vec::new();
    rope::decode(
        &moved,
        |_, _| Err(nwdb_core::Error::corrupt("no piece store")),
        &mut back,
    )
    .unwrap();
    assert_eq!(back, VALUE);
}
