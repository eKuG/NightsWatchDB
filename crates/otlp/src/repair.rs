//! Makes stored OTLP message bytes safe to hand to a standard OTLP parser.
//!
//! The stored bytes are what the sender sent, and the decoder does not look
//! into most of them. A standard parser refuses a string that is not UTF-8, a
//! truncated field, a wrong wire type on a known field, and messages nested
//! more deeply than its recursion limit. [`is_clean`] tells, without
//! allocating, whether none of those occurs; [`repair`] writes a copy without
//! them.

use crate::walk::{Next, Rd, Val};

/// Deepest nesting of messages accepted. Counted in messages, so a value in an
/// array in an array is four levels, which keeps a whole request well inside
/// the recursion limit of common parsers (100 in `prost`).
pub(crate) const MAX_LEVELS: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    KeyValue,
    AnyValue,
    Array,
    KvList,
    Resource,
    EntityRef,
    Scope,
    Event,
    Link,
}

#[derive(Clone, Copy)]
enum F {
    Varint,
    Fixed64,
    Fixed32,
    /// A `string` field: must be UTF-8.
    Str,
    /// A `bytes` field.
    Opaque,
    Msg(Kind),
    /// A repeated `KeyValue` that may be left out and counted as dropped.
    Attr,
}

fn schema(kind: Kind, n: u32) -> Option<F> {
    use F::*;
    Some(match (kind, n) {
        (Kind::KeyValue, 1) => Str,
        (Kind::KeyValue, 2) => Msg(Kind::AnyValue),
        (Kind::KeyValue, 3) => Varint,
        (Kind::AnyValue, 1) => Str,
        (Kind::AnyValue, 2 | 3 | 8) => Varint,
        (Kind::AnyValue, 4) => Fixed64,
        (Kind::AnyValue, 5) => Msg(Kind::Array),
        (Kind::AnyValue, 6) => Msg(Kind::KvList),
        (Kind::AnyValue, 7) => Opaque,
        (Kind::Array, 1) => Msg(Kind::AnyValue),
        (Kind::KvList, 1) => Msg(Kind::KeyValue),
        (Kind::Resource, 1) => Attr,
        (Kind::Resource, 2) => Varint,
        (Kind::Resource, 3) => Msg(Kind::EntityRef),
        (Kind::EntityRef, 1..=4) => Str,
        (Kind::Scope, 1 | 2) => Str,
        (Kind::Scope, 3) => Attr,
        (Kind::Scope, 4) => Varint,
        (Kind::Event, 1) => Fixed64,
        (Kind::Event, 2) => Str,
        (Kind::Event, 3) => Attr,
        (Kind::Event, 4) => Varint,
        (Kind::Link, 1 | 2) => Opaque,
        (Kind::Link, 3) => Str,
        (Kind::Link, 4) => Attr,
        (Kind::Link, 5) => Varint,
        (Kind::Link, 6) => Fixed32,
        _ => return None,
    })
}

/// The field that counts the attributes a message dropped.
fn dropped_field(kind: Kind) -> Option<u32> {
    match kind {
        Kind::Resource => Some(2),
        Kind::Scope | Kind::Event => Some(4),
        Kind::Link => Some(5),
        _ => None,
    }
}

fn fits(f: F, v: Val<'_>) -> bool {
    matches!(
        (f, v),
        (F::Varint, Val::Varint(_))
            | (F::Fixed64, Val::Fixed64(_))
            | (F::Fixed32, Val::Fixed32(_))
            | (F::Str | F::Opaque | F::Msg(_) | F::Attr, Val::Len(_))
    )
}

/// Whether a standard parser reads `msg` as a message of this kind.
pub(crate) fn is_clean(msg: &[u8], kind: Kind) -> bool {
    clean(msg, kind, 0)
}

fn clean(msg: &[u8], kind: Kind, depth: usize) -> bool {
    if depth >= MAX_LEVELS {
        return false;
    }
    let mut r = Rd::new(msg);
    loop {
        let (n, v) = match r.field() {
            Next::End => return true,
            Next::Bad => return false,
            Next::Item(f) => f,
        };
        let Some(f) = schema(kind, n) else { continue };
        let ok = fits(f, v)
            && match (f, v) {
                (F::Str, Val::Len(b)) => std::str::from_utf8(b).is_ok(),
                (F::Msg(k), Val::Len(b)) => clean(b, k, depth + 1),
                (F::Attr, Val::Len(b)) => clean(b, Kind::KeyValue, depth + 1),
                _ => true,
            };
        if !ok {
            return false;
        }
    }
}

/// A copy of `msg` that a standard parser reads: strings that are not UTF-8
/// get U+FFFD for each invalid sequence, and a `KeyValue` that cannot be read
/// is left out and added to the message's dropped-attributes count. `None`
/// when the message itself cannot be read.
pub(crate) fn repair(msg: &[u8], kind: Kind) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(msg.len());
    fix(&mut out, msg, kind, 0).then_some(out)
}

fn fix(out: &mut Vec<u8>, msg: &[u8], kind: Kind, depth: usize) -> bool {
    if depth >= MAX_LEVELS {
        return false;
    }
    let (mut old_dropped, mut newly_dropped) = (0u64, 0u64);
    let mut r = Rd::new(msg);
    loop {
        let (n, v) = match r.field() {
            Next::End => break,
            Next::Bad => return false,
            Next::Item(f) => f,
        };
        if dropped_field(kind) == Some(n) {
            if let Val::Varint(x) = v {
                old_dropped = x;
                continue;
            }
            return false;
        }
        let Some(f) = schema(kind, n) else {
            put_val(out, n, v);
            continue;
        };
        if !fits(f, v) {
            return false;
        }
        match (f, v) {
            (F::Str, Val::Len(b)) => put_len(out, n, String::from_utf8_lossy(b).as_bytes()),
            (F::Msg(k), Val::Len(b)) => {
                let mut inner = Vec::with_capacity(b.len());
                if !fix(&mut inner, b, k, depth + 1) {
                    return false;
                }
                put_len(out, n, &inner);
            }
            (F::Attr, Val::Len(b)) => {
                let mut inner = Vec::with_capacity(b.len());
                if fix(&mut inner, b, Kind::KeyValue, depth + 1) {
                    put_len(out, n, &inner);
                } else {
                    newly_dropped += 1;
                }
            }
            _ => put_val(out, n, v),
        }
    }
    if let Some(field) = dropped_field(kind) {
        let total = old_dropped
            .saturating_add(newly_dropped)
            .min(u64::from(u32::MAX));
        if total != 0 {
            put_val(out, field, Val::Varint(total));
        }
    }
    true
}

use nwdb_core::put_varint;

fn put_len(out: &mut Vec<u8>, n: u32, payload: &[u8]) {
    put_val(out, n, Val::Len(payload));
}

fn put_val(out: &mut Vec<u8>, n: u32, v: Val<'_>) {
    let wire = match v {
        Val::Varint(_) => 0,
        Val::Fixed64(_) => 1,
        Val::Len(_) => 2,
        Val::Fixed32(_) => 5,
    };
    put_varint(out, u64::from(n) << 3 | wire);
    match v {
        Val::Varint(x) => put_varint(out, x),
        Val::Fixed64(x) => out.extend_from_slice(&x.to_le_bytes()),
        Val::Fixed32(x) => out.extend_from_slice(&x.to_le_bytes()),
        Val::Len(b) => {
            put_varint(out, b.len() as u64);
            out.extend_from_slice(b);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList,
        any_value::Value,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    use prost::Message;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const KINDS: [Kind; 9] = [
        Kind::KeyValue,
        Kind::AnyValue,
        Kind::Array,
        Kind::KvList,
        Kind::Resource,
        Kind::EntityRef,
        Kind::Scope,
        Kind::Event,
        Kind::Link,
    ];

    fn parses(msg: &[u8], kind: Kind) -> bool {
        match kind {
            Kind::KeyValue => KeyValue::decode(msg).is_ok(),
            Kind::AnyValue => AnyValue::decode(msg).is_ok(),
            Kind::Array => ArrayValue::decode(msg).is_ok(),
            Kind::KvList => KeyValueList::decode(msg).is_ok(),
            Kind::Resource => Resource::decode(msg).is_ok(),
            Kind::EntityRef => EntityRef::decode(msg).is_ok(),
            Kind::Scope => InstrumentationScope::decode(msg).is_ok(),
            Kind::Event => Event::decode(msg).is_ok(),
            Kind::Link => Link::decode(msg).is_ok(),
        }
    }

    /// Clean means the reference parser reads it, and so does a repair of it.
    /// The converse is not required: this check also refuses groups, which a
    /// parser may skip, and deeper nesting than it.
    fn check(msg: &[u8]) {
        for kind in KINDS {
            if is_clean(msg, kind) {
                assert!(parses(msg, kind), "clean but refused: {msg:02x?}");
            }
            if let Some(fixed) = repair(msg, kind) {
                assert!(is_clean(&fixed, kind), "repair is not clean: {fixed:02x?}");
                assert!(parses(&fixed, kind));
            }
        }
    }

    fn samples(r: &mut StdRng) -> Vec<Vec<u8>> {
        let kvs: Vec<KeyValue> = (0..3)
            .map(|_| KeyValue {
                key: format!("k{}", r.random_range(0..3)),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("val".into())),
                }),
                ..Default::default()
            })
            .collect();
        vec![
            kvs[0].encode_to_vec(),
            Resource {
                attributes: kvs.clone(),
                dropped_attributes_count: 1,
                ..Default::default()
            }
            .encode_to_vec(),
            InstrumentationScope {
                name: "n".into(),
                version: "v".into(),
                attributes: kvs.clone(),
                dropped_attributes_count: 1,
            }
            .encode_to_vec(),
            Event {
                time_unix_nano: 4,
                name: "e".into(),
                attributes: kvs.clone(),
                dropped_attributes_count: 1,
            }
            .encode_to_vec(),
            Link {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                trace_state: "a=b".into(),
                attributes: kvs,
                dropped_attributes_count: 1,
                flags: 1,
            }
            .encode_to_vec(),
        ]
    }

    #[test]
    fn clean_and_repaired_agree_with_the_reference_parser() {
        let mut r = StdRng::seed_from_u64(5);
        for _ in 0..30 {
            for s in samples(&mut r) {
                check(&s);
                for n in 0..s.len() {
                    check(&s[..n]);
                }
                for _ in 0..60 {
                    let mut m = s.clone();
                    for _ in 0..r.random_range(1..4) {
                        let i = r.random_range(0..m.len());
                        m[i] = if r.random() { r.random() } else { m[i] ^ 0x80 };
                    }
                    check(&m);
                }
            }
        }
        for _ in 0..20_000 {
            let b: Vec<u8> = (0..r.random_range(0..40)).map(|_| r.random()).collect();
            check(&b);
        }
    }
}
