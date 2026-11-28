//! Builders for hand-written protobuf messages.

/// `v` as a protobuf varint.
pub fn varint(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    nwdb_core::put_varint(&mut out, v);
    out
}

/// A length-delimited field: the tag, the length of `payload`, `payload`.
pub fn ld(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = varint(u64::from(field) << 3 | 2);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}
