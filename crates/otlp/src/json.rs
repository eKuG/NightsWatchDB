//! Renders the raw columns of the span table (`resource`, `attrs`, `events`,
//! `links`) as JSON values, for the query API.
//!
//! The functions read the stored OTLP bytes with the crate's own wire reader
//! and need no protobuf library. Bytes that are not valid give
//! [`nwdb_core::Error::Corrupt`], never a panic.
//!
//! # Mapping of `AnyValue`
//!
//! | OTLP | JSON |
//! |---|---|
//! | string | string |
//! | bool | bool |
//! | int | number |
//! | double | number; NaN, infinity and negative infinity as the strings `"NaN"`, `"Infinity"`, `"-Infinity"` |
//! | array | array |
//! | key-value list | object |
//! | bytes | string, standard base64 with padding |
//! | not set | null |
//!
//! When a key occurs twice in one list the last occurrence wins, the same
//! rule the decoder uses for the promoted columns. Object keys come out
//! sorted. Values nested deeper than [`MAX_DEPTH`] are rejected so that
//! hostile input cannot exhaust the stack.
//!
//! The functions in this module build the whole value in memory and fail on the
//! first thing they cannot read, so one bad span can make a trace unreadable
//! and a large column needs hundreds of times its size in memory. They are
//! superseded by the streaming writers in this crate ([`write_attrs`] and its
//! siblings) and will be removed once their callers have moved.
//!
//! [`write_attrs`]: crate::write_attrs

use crate::wire::{At, Cur, Fault, R, utf8};
use nwdb_core::Result;
use serde_json::{Map, Number, Value};

/// Deepest nesting of arrays and key-value lists accepted.
pub const MAX_DEPTH: usize = 64;

/// The `attrs` column (length-prefixed `KeyValue` messages) as an object.
///
/// Superseded by [`crate::write_attrs`].
pub fn attrs_to_json(column: &[u8]) -> Result<Value> {
    let mut map = Map::new();
    each_prefixed(column, |msg| put_key_value(&mut map, msg, 0)).at("attrs")?;
    Ok(Value::Object(map))
}

/// The `resource` column (a `Resource` message) as an object of its
/// attributes.
///
/// Superseded by [`crate::write_resource`].
pub fn resource_to_json(resource: &[u8]) -> Result<Value> {
    message_attributes(resource, 1, "resource")
        .map(Value::Object)
        .map_err(Into::into)
}

/// The `scope` column (an `InstrumentationScope` message) as an object of
/// its attributes.
///
/// Superseded by [`crate::write_scope_attrs`].
pub fn scope_to_json(scope: &[u8]) -> Result<Value> {
    message_attributes(scope, 3, "scope")
        .map(Value::Object)
        .map_err(Into::into)
}

/// The `events` column as an array of `{time_ns, name, attributes}`.
///
/// Superseded by [`crate::write_events`].
pub fn events_to_json(column: &[u8]) -> Result<Value> {
    let mut out = Vec::new();
    each_prefixed(column, |msg| {
        out.push(event(msg)?);
        Ok(())
    })
    .at("events")?;
    Ok(Value::Array(out))
}

/// The `links` column as an array of `{trace_id, span_id, attributes}`, the
/// ids as lower-case hex.
pub fn links_to_json(column: &[u8]) -> Result<Value> {
    let mut out = Vec::new();
    each_prefixed(column, |msg| {
        out.push(link(msg)?);
        Ok(())
    })
    .at("links")?;
    Ok(Value::Array(out))
}

/// The attributes of a message that keeps them as repeated `KeyValue` in
/// field number `field`: 1 for `Resource`, 3 for `InstrumentationScope`.
fn message_attributes(msg: &[u8], field: u32, what: &'static str) -> R<Map<String, Value>> {
    let mut map = Map::new();
    let mut c = Cur::new(msg);
    while let Some((n, w)) = c.next_field().at(what)? {
        if n == field {
            let kv = c.expect_len(w).at("attributes").at(what)?;
            put_key_value(&mut map, kv, 0).at("attributes").at(what)?;
        } else {
            c.skip(w).at(what)?;
        }
    }
    Ok(map)
}

fn each_prefixed(column: &[u8], mut f: impl FnMut(&[u8]) -> R<()>) -> R<()> {
    let mut c = Cur::new(column);
    while !c.is_empty() {
        f(c.len_delimited()?)?;
    }
    Ok(())
}

fn event(msg: &[u8]) -> R<Value> {
    let mut c = Cur::new(msg);
    let (mut time, mut name) = (0u64, String::new());
    let mut attributes = Map::new();
    while let Some((n, w)) = c.next_field()? {
        match n {
            1 => time = c.expect_fixed64(w).at("time_unix_nano")?,
            2 => name = utf8(c.expect_len(w).at("name")?).at("name")?.to_owned(),
            3 => {
                let kv = c.expect_len(w).at("attributes")?;
                put_key_value(&mut attributes, kv, 0).at("attributes")?;
            }
            _ => c.skip(w)?,
        }
    }
    let mut o = Map::new();
    o.insert("time_ns".into(), Value::from(time));
    o.insert("name".into(), Value::String(name));
    o.insert("attributes".into(), Value::Object(attributes));
    Ok(Value::Object(o))
}

fn link(msg: &[u8]) -> R<Value> {
    let mut c = Cur::new(msg);
    let (mut trace_id, mut span_id): (&[u8], &[u8]) = (&[], &[]);
    let mut attributes = Map::new();
    while let Some((n, w)) = c.next_field()? {
        match n {
            1 => trace_id = c.expect_len(w).at("trace_id")?,
            2 => span_id = c.expect_len(w).at("span_id")?,
            4 => {
                let kv = c.expect_len(w).at("attributes")?;
                put_key_value(&mut attributes, kv, 0).at("attributes")?;
            }
            _ => c.skip(w)?,
        }
    }
    let mut o = Map::new();
    o.insert("trace_id".into(), Value::String(hex(trace_id)));
    o.insert("span_id".into(), Value::String(hex(span_id)));
    o.insert("attributes".into(), Value::Object(attributes));
    Ok(Value::Object(o))
}

/// Parses one `KeyValue` and adds it to `map`, replacing an earlier value of
/// the same key.
fn put_key_value(map: &mut Map<String, Value>, msg: &[u8], depth: usize) -> R<()> {
    let mut c = Cur::new(msg);
    let mut key = "";
    let mut value = Value::Null;
    while let Some((n, w)) = c.next_field()? {
        match n {
            1 => key = utf8(c.expect_len(w).at("key")?).at("key")?,
            2 => value = any_value(c.expect_len(w).at("value")?, depth).at("value")?,
            _ => c.skip(w)?,
        }
    }
    map.insert(key.to_owned(), value);
    Ok(())
}

fn any_value(msg: &[u8], depth: usize) -> R<Value> {
    if depth >= MAX_DEPTH {
        return Err(Fault::new("value nested too deeply"));
    }
    let mut c = Cur::new(msg);
    let mut out = Value::Null;
    // The fields form a oneof: the last one on the wire wins.
    while let Some((n, w)) = c.next_field()? {
        out = match n {
            1 => Value::String(
                utf8(c.expect_len(w).at("string_value")?)
                    .at("string_value")?
                    .to_owned(),
            ),
            2 => Value::Bool(c.expect_varint(w).at("bool_value")? != 0),
            3 => Value::from(c.expect_varint(w).at("int_value")? as i64),
            4 => double(f64::from_bits(c.expect_fixed64(w).at("double_value")?)),
            5 => array(c.expect_len(w).at("array_value")?, depth).at("array_value")?,
            6 => kvlist(c.expect_len(w).at("kvlist_value")?, depth).at("kvlist_value")?,
            7 => Value::String(base64(c.expect_len(w).at("bytes_value")?)),
            _ => {
                c.skip(w)?;
                continue;
            }
        };
    }
    Ok(out)
}

fn array(msg: &[u8], depth: usize) -> R<Value> {
    let mut c = Cur::new(msg);
    let mut items = Vec::new();
    while let Some((n, w)) = c.next_field()? {
        if n == 1 {
            items.push(any_value(c.expect_len(w)?, depth + 1)?);
        } else {
            c.skip(w)?;
        }
    }
    Ok(Value::Array(items))
}

fn kvlist(msg: &[u8], depth: usize) -> R<Value> {
    let mut c = Cur::new(msg);
    let mut map = Map::new();
    while let Some((n, w)) = c.next_field()? {
        if n == 1 {
            put_key_value(&mut map, c.expect_len(w)?, depth + 1)?;
        } else {
            c.skip(w)?;
        }
    }
    Ok(Value::Object(map))
}

fn double(d: f64) -> Value {
    match Number::from_f64(d) {
        Some(n) => Value::Number(n),
        None if d.is_nan() => Value::String("NaN".into()),
        None if d > 0.0 => Value::String("Infinity".into()),
        None => Value::String("-Infinity".into()),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Standard base64 with padding. Written out because it is the only place the
/// workspace needs it.
fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, b| n << 8 | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value as V,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    use prost::Message;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::json;

    fn av(v: V) -> Option<AnyValue> {
        Some(AnyValue { value: Some(v) })
    }

    fn kv(key: &str, value: Option<AnyValue>) -> KeyValue {
        KeyValue {
            key: key.into(),
            value,
            ..Default::default()
        }
    }

    fn prefixed<M: Message>(items: &[M]) -> Vec<u8> {
        let mut out = Vec::new();
        for m in items {
            out.extend(m.encode_length_delimited_to_vec());
        }
        out
    }

    fn random_string(r: &mut StdRng) -> String {
        const POOL: [&str; 6] = ["a", "gen_ai", "\u{e9}", "\u{1f600}", "\"q\"\n", ""];
        (0..r.random_range(0..4))
            .map(|_| POOL[r.random_range(0..POOL.len())])
            .collect()
    }

    fn random_value(r: &mut StdRng, depth: u32) -> Option<AnyValue> {
        let top = if depth >= 4 { 6 } else { 9 };
        match r.random_range(0..top) {
            0 => av(V::StringValue(random_string(r))),
            1 => av(V::BoolValue(r.random())),
            2 => av(V::IntValue(r.random())),
            3 => av(V::DoubleValue(match r.random_range(0..6) {
                0 => f64::NAN,
                1 => f64::INFINITY,
                2 => f64::NEG_INFINITY,
                3 => -0.0,
                _ => f64::from_bits(r.random()),
            })),
            4 => av(V::BytesValue(
                (0..r.random_range(0..9)).map(|_| r.random()).collect(),
            )),
            5 => None,
            6 | 7 => av(V::ArrayValue(ArrayValue {
                values: (0..r.random_range(0..4))
                    .map(|_| random_value(r, depth + 1).unwrap_or_default())
                    .collect(),
            })),
            _ => av(V::KvlistValue(KeyValueList {
                values: random_kvs(r, depth + 1),
            })),
        }
    }

    fn random_kvs(r: &mut StdRng, depth: u32) -> Vec<KeyValue> {
        // Few distinct keys so that duplicates happen.
        (0..r.random_range(0..5))
            .map(|_| {
                kv(
                    &format!("k{}", r.random_range(0..4)),
                    random_value(r, depth),
                )
            })
            .collect()
    }

    /// The expected rendering, written from the reference messages.
    fn reference(v: &Option<AnyValue>) -> Value {
        match v.as_ref().and_then(|a| a.value.as_ref()) {
            None => json!(null),
            Some(V::StringValue(s)) => json!(s),
            Some(V::BoolValue(b)) => json!(b),
            Some(V::IntValue(i)) => json!(i),
            Some(V::DoubleValue(d)) if d.is_nan() => json!("NaN"),
            Some(V::DoubleValue(d)) if d.is_infinite() => {
                json!(if *d > 0.0 { "Infinity" } else { "-Infinity" })
            }
            Some(V::DoubleValue(d)) => json!(d),
            Some(V::ArrayValue(a)) => Value::Array(
                a.values
                    .iter()
                    .map(|x| reference(&Some(x.clone())))
                    .collect(),
            ),
            Some(V::KvlistValue(l)) => reference_object(&l.values),
            Some(V::BytesValue(b)) => json!(reference_base64(b)),
            Some(V::StringValueStrindex(_)) => unreachable!("never generated"),
        }
    }

    fn reference_object(kvs: &[KeyValue]) -> Value {
        let mut m = Map::new();
        for x in kvs {
            m.insert(x.key.clone(), reference(&x.value));
        }
        Value::Object(m)
    }

    /// An independent base64, bit by bit, to check the table-driven one.
    fn reference_base64(b: &[u8]) -> String {
        let alphabet: Vec<char> = ('A'..='Z')
            .chain('a'..='z')
            .chain('0'..='9')
            .chain(['+', '/'])
            .collect();
        let bits: String = b.iter().map(|x| format!("{x:08b}")).collect();
        let mut s: String = bits
            .as_bytes()
            .chunks(6)
            .map(|c| {
                let mut v = String::from_utf8(c.to_vec()).unwrap();
                while v.len() < 6 {
                    v.push('0');
                }
                alphabet[usize::from_str_radix(&v, 2).unwrap()]
            })
            .collect();
        while !s.len().is_multiple_of(4) {
            s.push('=');
        }
        s
    }

    #[test]
    fn base64_known_values() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
        ] {
            assert_eq!(base64(i.as_bytes()), o);
        }
    }

    #[test]
    fn matches_reference_on_random_values() {
        let mut r = StdRng::seed_from_u64(7);
        for _ in 0..400 {
            let kvs = random_kvs(&mut r, 0);
            assert_eq!(
                attrs_to_json(&prefixed(&kvs)).unwrap(),
                reference_object(&kvs)
            );
            let res = Resource {
                attributes: kvs.clone(),
                dropped_attributes_count: 3,
                ..Default::default()
            };
            assert_eq!(
                resource_to_json(&res.encode_to_vec()).unwrap(),
                reference_object(&kvs)
            );
            let scope = InstrumentationScope {
                name: "lib".into(),
                version: "1".into(),
                attributes: kvs.clone(),
                dropped_attributes_count: 2,
            };
            assert_eq!(
                scope_to_json(&scope.encode_to_vec()).unwrap(),
                reference_object(&kvs)
            );

            let events: Vec<Event> = (0..r.random_range(0..3))
                .map(|_| Event {
                    time_unix_nano: r.random(),
                    name: random_string(&mut r),
                    attributes: random_kvs(&mut r, 0),
                    dropped_attributes_count: 1,
                })
                .collect();
            let want: Vec<_> = events
                .iter()
                .map(|e| {
                    json!({
                        "time_ns": e.time_unix_nano,
                        "name": e.name,
                        "attributes": reference_object(&e.attributes),
                    })
                })
                .collect();
            assert_eq!(
                events_to_json(&prefixed(&events)).unwrap(),
                Value::Array(want)
            );

            let links: Vec<Link> = (0..r.random_range(0..3))
                .map(|_| Link {
                    trace_id: (0..16).map(|_| r.random()).collect(),
                    span_id: (0..8).map(|_| r.random()).collect(),
                    trace_state: "x=y".into(),
                    attributes: random_kvs(&mut r, 0),
                    ..Default::default()
                })
                .collect();
            let want: Vec<_> = links
                .iter()
                .map(|l| {
                    json!({
                        "trace_id": hex(&l.trace_id),
                        "span_id": hex(&l.span_id),
                        "attributes": reference_object(&l.attributes),
                    })
                })
                .collect();
            assert_eq!(
                links_to_json(&prefixed(&links)).unwrap(),
                Value::Array(want)
            );
        }
    }

    #[test]
    fn last_duplicate_wins_and_empty_is_empty() {
        let kvs = [kv("a", av(V::IntValue(1))), kv("a", av(V::IntValue(2)))];
        assert_eq!(attrs_to_json(&prefixed(&kvs)).unwrap(), json!({"a": 2}));
        assert_eq!(attrs_to_json(&[]).unwrap(), json!({}));
        assert_eq!(resource_to_json(&[]).unwrap(), json!({}));
        assert_eq!(scope_to_json(&[]).unwrap(), json!({}));
        assert_eq!(events_to_json(&[]).unwrap(), json!([]));
        assert_eq!(links_to_json(&[]).unwrap(), json!([]));
    }

    #[test]
    fn special_doubles_and_unset() {
        let kvs = [
            kv("nan", av(V::DoubleValue(f64::NAN))),
            kv("inf", av(V::DoubleValue(f64::INFINITY))),
            kv("ninf", av(V::DoubleValue(f64::NEG_INFINITY))),
            kv("unset", Some(AnyValue { value: None })),
            kv("absent", None),
            kv("big", av(V::IntValue(i64::MIN))),
        ];
        assert_eq!(
            attrs_to_json(&prefixed(&kvs)).unwrap(),
            json!({
                "nan": "NaN", "inf": "Infinity", "ninf": "-Infinity",
                "unset": null, "absent": null, "big": i64::MIN,
            })
        );
    }

    #[test]
    fn deep_nesting_is_an_error_not_a_crash() {
        // Built as bytes: a tree of reference messages this deep would
        // overflow the stack in the test itself.
        fn varint(mut n: usize) -> Vec<u8> {
            let mut out = Vec::new();
            while n >= 0x80 {
                out.push(n as u8 & 0x7f | 0x80);
                n >>= 7;
            }
            out.push(n as u8);
            out
        }
        fn wrap(tag: u8, inner: &[u8]) -> Vec<u8> {
            let mut out = vec![tag];
            out.extend(varint(inner.len()));
            out.extend_from_slice(inner);
            out
        }
        let mut value = vec![0x18, 1];
        for _ in 0..2_000 {
            // AnyValue.array_value (5) holding ArrayValue.values (1).
            value = wrap(0x2a, &wrap(0x0a, &value));
        }
        let mut kv = vec![0x0a, 1, b'a'];
        kv.extend(wrap(0x12, &value));
        let mut column = varint(kv.len());
        column.extend(kv);
        assert!(attrs_to_json(&column).is_err());
    }

    /// Whatever the input, the answer is a value or `Corrupt`, never a panic.
    fn check(bytes: &[u8]) {
        for f in [
            attrs_to_json,
            resource_to_json,
            events_to_json,
            links_to_json,
        ] {
            if let Err(e) = f(bytes) {
                assert!(matches!(e, nwdb_core::Error::Corrupt(_)), "{e}");
            }
        }
    }

    #[test]
    fn every_truncation_is_ok_or_corrupt() {
        let mut r = StdRng::seed_from_u64(11);
        for _ in 0..30 {
            let kvs = random_kvs(&mut r, 0);
            let mut all = prefixed(&kvs);
            all.extend(prefixed(&[Event {
                time_unix_nano: 5,
                name: "e".into(),
                attributes: kvs.clone(),
                dropped_attributes_count: 0,
            }]));
            all.extend(
                Resource {
                    attributes: kvs,
                    dropped_attributes_count: 0,
                    ..Default::default()
                }
                .encode_to_vec(),
            );
            for n in 0..=all.len() {
                check(&all[..n]);
            }
        }
    }

    #[test]
    fn random_bytes_are_ok_or_corrupt() {
        let mut r = StdRng::seed_from_u64(13);
        for _ in 0..20_000 {
            let bytes: Vec<u8> = (0..r.random_range(0..64)).map(|_| r.random()).collect();
            check(&bytes);
        }
    }
}
