//! The streaming JSON writers: agreement with the reference types on valid
//! input, and valid JSON out for any bytes in.

mod common;

use common::*;
use nwdb_otlp::{
    MAX_DEPTH, resource_dropped_attributes, scope_dropped_attributes, write_attrs, write_events,
    write_links, write_resource, write_scope_attrs,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value as V,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use prost::Message;
use rand::Rng;
use serde_json::{Map, Value, json};
use std::io::{self, Write};

type W = fn(&mut dyn Write, &[u8]) -> io::Result<()>;

const WRITERS: [(&str, W); 5] = [
    ("attrs", write_attrs),
    ("resource", write_resource),
    ("scope", write_scope_attrs),
    ("events", write_events),
    ("links", write_links),
];

fn text(f: W, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    f(&mut out, bytes).expect("a Vec does not fail");
    out
}

fn render(f: W, bytes: &[u8]) -> Value {
    let out = text(f, bytes);
    serde_json::from_slice(&out)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&out)))
}

fn prefixed<M: Message>(items: &[M]) -> Vec<u8> {
    items
        .iter()
        .flat_map(|m| m.encode_length_delimited_to_vec())
        .collect()
}

/// Equal, except that floats may differ in the last digit: serde_json's
/// default parser does not read every float back exactly, and the writers'
/// text is parsed here.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) if x.is_f64() && y.is_f64() => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            x == y || (x - y).abs() <= x.abs().max(y.abs()) * 1e-14
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn assert_same(got: Value, want: Value) {
    assert!(same(&got, &want), "left: {got}\nright: {want}");
}

fn column(kvs: &[Vec<u8>]) -> Vec<u8> {
    kvs.iter()
        .flat_map(|m| [varint(m.len() as u64), m.clone()].concat())
        .collect()
}

fn unb64(s: &str) -> Vec<u8> {
    let val = |c: u8| match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => 0,
    };
    let mut out = Vec::new();
    for g in s.as_bytes().chunks(4) {
        let n = g.iter().fold(0u32, |n, &c| n << 6 | u32::from(val(c)));
        let pad = g.iter().filter(|&&c| c == b'=').count();
        out.extend(&n.to_be_bytes()[1..4 - pad]);
    }
    out
}

fn unparsed_of(v: &Value) -> Vec<u8> {
    unb64(v["$unparsed"].as_str().expect("an $unparsed object"))
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
fn valid_input_matches_the_reference() {
    let mut r = rng(21);
    for _ in 0..300 {
        let kvs = rand_attrs(&mut r);
        let want = reference_object(&kvs);
        assert_same(render(write_attrs, &prefixed(&kvs)), want.clone());

        let res = Resource {
            attributes: kvs.clone(),
            dropped_attributes_count: 3,
            ..Default::default()
        };
        assert_same(render(write_resource, &res.encode_to_vec()), want.clone());
        assert_eq!(resource_dropped_attributes(&res.encode_to_vec()), 3);
        let scope = InstrumentationScope {
            name: "n".into(),
            version: "v".into(),
            attributes: kvs.clone(),
            dropped_attributes_count: 2,
        };
        assert_same(render(write_scope_attrs, &scope.encode_to_vec()), want);
        assert_eq!(scope_dropped_attributes(&scope.encode_to_vec()), 2);

        let events: Vec<Event> = (0..r.random_range(0..4))
            .map(|_| Event {
                time_unix_nano: r.random(),
                name: rand_string(&mut r),
                attributes: rand_attrs(&mut r),
                dropped_attributes_count: r.random_range(0..2),
            })
            .collect();
        let want: Vec<Value> = events
            .iter()
            .map(|e| {
                let mut o = json!({
                    "time_ns": e.time_unix_nano,
                    "name": e.name,
                    "attributes": reference_object(&e.attributes),
                });
                if e.dropped_attributes_count != 0 {
                    o["dropped_attributes_count"] = e.dropped_attributes_count.into();
                }
                o
            })
            .collect();
        assert_same(render(write_events, &prefixed(&events)), Value::Array(want));

        let links: Vec<Link> = (0..r.random_range(0..4))
            .map(|_| Link {
                trace_id: (0..16).map(|_| r.random()).collect(),
                span_id: (0..8).map(|_| r.random()).collect(),
                trace_state: rand_string(&mut r),
                attributes: rand_attrs(&mut r),
                dropped_attributes_count: r.random_range(0..2),
                flags: r.random_range(0..3),
            })
            .collect();
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        let want: Vec<Value> = links
            .iter()
            .map(|l| {
                let mut o = json!({"trace_id": hex(&l.trace_id), "span_id": hex(&l.span_id)});
                if !l.trace_state.is_empty() {
                    o["trace_state"] = l.trace_state.clone().into();
                }
                if l.flags != 0 {
                    o["flags"] = l.flags.into();
                }
                o["attributes"] = reference_object(&l.attributes);
                if l.dropped_attributes_count != 0 {
                    o["dropped_attributes_count"] = l.dropped_attributes_count.into();
                }
                o
            })
            .collect();
        assert_same(render(write_links, &prefixed(&links)), Value::Array(want));
    }
}

#[test]
fn empty_columns_are_empty_containers() {
    for (name, f) in WRITERS {
        let want = if name == "events" || name == "links" {
            "[]"
        } else {
            "{}"
        };
        assert_eq!(text(f, &[]), want.as_bytes(), "{name}");
    }
}

/// A composite of valid bytes that every writer can be pointed at.
fn samples(r: &mut rand::rngs::StdRng) -> Vec<Vec<u8>> {
    let kvs = rand_attrs(r);
    vec![
        prefixed(&kvs),
        Resource {
            attributes: kvs.clone(),
            dropped_attributes_count: 1,
            ..Default::default()
        }
        .encode_to_vec(),
        InstrumentationScope {
            name: "n".into(),
            attributes: kvs.clone(),
            ..Default::default()
        }
        .encode_to_vec(),
        prefixed(&[Event {
            time_unix_nano: 5,
            name: "e".into(),
            attributes: kvs.clone(),
            dropped_attributes_count: 1,
        }]),
        prefixed(&[Link {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            trace_state: "a=b".into(),
            attributes: kvs,
            dropped_attributes_count: 1,
            flags: 1,
        }]),
    ]
}

fn all_render(bytes: &[u8]) {
    for (_, f) in WRITERS {
        render(f, bytes);
    }
    let _ = (
        resource_dropped_attributes(bytes),
        scope_dropped_attributes(bytes),
    );
}

#[test]
fn every_truncation_gives_json() {
    let mut r = rng(22);
    for _ in 0..20 {
        for s in samples(&mut r) {
            for n in 0..=s.len() {
                all_render(&s[..n]);
            }
        }
    }
}

#[test]
fn bit_flips_give_json() {
    let mut r = rng(23);
    for _ in 0..40 {
        for s in samples(&mut r) {
            if s.is_empty() {
                continue;
            }
            for _ in 0..30 {
                let mut m = s.clone();
                for _ in 0..r.random_range(1..4) {
                    let i = r.random_range(0..m.len());
                    m[i] ^= 1 << r.random_range(0..8);
                }
                all_render(&m);
            }
        }
    }
}

#[test]
fn random_bytes_give_json() {
    let mut r = rng(24);
    for _ in 0..5_000 {
        let b: Vec<u8> = (0..r.random_range(0..80)).map(|_| r.random()).collect();
        all_render(&b);
    }
}

#[test]
fn invalid_utf8_becomes_the_replacement_character() {
    // A lone surrogate as CESU-8 writes it, a cut emoji, and a stray byte.
    let column = column(&[
        kv_bytes(b"a", &ld(1, &[b'x', 0xed, 0xa0, 0xbd, b'y'])),
        kv_bytes(&[b'k', 0xf0, 0x9f], &ld(1, b"v")),
        kv_bytes(b"c", &ld(1, &[0xff, b'"', b'\n'])),
    ]);
    assert_eq!(
        render(write_attrs, &column),
        json!({
            "a": "x\u{fffd}\u{fffd}\u{fffd}y",
            "k\u{fffd}": "v",
            "c": "\u{fffd}\"\n",
        })
    );
}

#[test]
fn repeated_keys_are_written_as_sent() {
    let column = column(&[kv_bytes(b"a", &vf(3, 1)), kv_bytes(b"a", &vf(3, 2))]);
    let out = String::from_utf8(text(write_attrs, &column)).unwrap();
    assert_eq!(out, r#"{"a":1,"a":2}"#);
    assert_eq!(render(write_attrs, &column), json!({"a": 2}));
}

#[test]
fn what_cannot_be_read_is_kept_as_base64_in_its_place() {
    // A key-value whose key is cut short, between two good ones.
    let bad = vec![0x0a, 5, b'a'];
    let col = column(&[
        kv_bytes(b"x", &vf(3, 1)),
        bad.clone(),
        kv_bytes(b"y", &vf(3, 2)),
    ]);
    let got = render(write_attrs, &col);
    assert_eq!((got["x"].clone(), got["y"].clone()), (json!(1), json!(2)));
    assert_eq!(unparsed_of(&got), bad);

    // A value that is not an AnyValue keeps its key.
    let col = column(&[kv_bytes(b"v", &[0x0a, 9])]);
    let got = render(write_attrs, &col);
    assert_eq!(unparsed_of(&got["v"]), [0x0a, 9]);

    // The rest of a column whose framing breaks.
    let mut col = column(&[kv_bytes(b"x", &vf(3, 1))]);
    let tail = [0xff, 0xff, 0x01, 0x02];
    col.extend(tail);
    let got = render(write_attrs, &col);
    assert_eq!(got["x"], json!(1));
    assert_eq!(unparsed_of(&got), tail);

    // An event that is not a message does not hide the next one.
    let good = Event {
        time_unix_nano: 1,
        name: "ok".into(),
        ..Default::default()
    };
    let mut col = vec![1, 0xff];
    col.extend(good.encode_length_delimited_to_vec());
    let got = render(write_events, &col);
    assert_eq!(unparsed_of(&got[0]), [0xff]);
    assert_eq!(got[1]["name"], json!("ok"));

    // A link with a field of the wrong wire type.
    let mut col = vec![2, 0x08, 0x01];
    col.extend(
        Link {
            trace_id: vec![1; 16],
            ..Default::default()
        }
        .encode_length_delimited_to_vec(),
    );
    let got = render(write_links, &col);
    assert_eq!(unparsed_of(&got[0]), [0x08, 0x01]);
    assert_eq!(got[1]["trace_id"], json!("01".repeat(16)));
}

#[test]
fn nesting_deeper_than_the_limit_is_kept_as_base64() {
    // MAX_DEPTH arrays inside each other still render.
    let ok = column(&[kv_bytes(b"d", &nested(MAX_DEPTH - 1))]);
    let mut v = render(write_attrs, &ok)["d"].clone();
    for _ in 0..MAX_DEPTH - 1 {
        v = v[0].clone();
    }
    assert_eq!(v, json!(1));

    // One level more and the innermost value is kept as bytes.
    let deep = column(&[kv_bytes(b"d", &nested(MAX_DEPTH))]);
    let mut v = render(write_attrs, &deep)["d"].clone();
    for _ in 0..MAX_DEPTH {
        v = v[0].clone();
    }
    assert_eq!(unparsed_of(&v), vf(3, 1));

    // Far deeper than the stack could follow.
    let hostile = column(&[kv_bytes(b"d", &nested(50_000))]);
    render(write_attrs, &hostile);
}

#[test]
fn a_float_that_is_not_a_number_is_a_string() {
    let col = column(&[
        kv_bytes(b"nan", &f64f(4, f64::NAN.to_bits())),
        kv_bytes(b"inf", &f64f(4, f64::INFINITY.to_bits())),
        kv_bytes(b"ninf", &f64f(4, f64::NEG_INFINITY.to_bits())),
        kv_bytes(b"x", &f64f(4, 1.5f64.to_bits())),
    ]);
    assert_eq!(
        render(write_attrs, &col),
        json!({"nan": "NaN", "inf": "Infinity", "ninf": "-Infinity", "x": 1.5})
    );
}

#[test]
fn dropped_counts_of_resource_and_scope() {
    assert_eq!(resource_dropped_attributes(&vf(2, 7)), 7);
    assert_eq!(resource_dropped_attributes(&[]), 0);
    // Unreadable: nothing is reported.
    assert_eq!(
        resource_dropped_attributes(&concat(&[&vf(2, 7), &[0xff]])),
        0
    );
    assert_eq!(scope_dropped_attributes(&vf(4, 9)), 9);
    assert_eq!(scope_dropped_attributes(&vf(2, 9)), 0);
}

/// Fails after a set number of bytes, like a closed connection.
struct Failing(usize);

impl Write for Failing {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.len() > self.0 {
            return Err(io::Error::other("closed"));
        }
        self.0 -= b.len();
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn the_writers_own_error_is_the_only_error() {
    let big = column(&[kv_bytes(b"a", &ld(1, &vec![b'x'; 100_000]))]);
    for budget in [0, 10, 50_000] {
        let err = write_attrs(&mut Failing(budget), &big).unwrap_err();
        assert_eq!(err.to_string(), "closed");
    }
    assert!(write_attrs(&mut Failing(usize::MAX), &big).is_ok());
}
