//! Regressions for the JSON view of the raw columns: the one corner where it
//! deliberately differs from a protobuf parser, and the fields of a link.

mod common;

use common::*;
use nwdb_otlp::{write_attrs, write_links};
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value::Value};
use opentelemetry_proto::tonic::trace::v1::span::Link;
use prost::Message;
use serde_json::json;

fn render(
    f: fn(&mut dyn std::io::Write, &[u8]) -> std::io::Result<()>,
    column: &[u8],
) -> serde_json::Value {
    let mut out = Vec::new();
    f(&mut out, column).unwrap();
    serde_json::from_slice(&out).expect("the output is JSON")
}

/// When `KeyValue.value` occurs twice, protobuf merges the second into the
/// first; the JSON view shows the last occurrence alone. This is a documented
/// deviation (see the README): merging needs the whole message in memory. An
/// empty second value therefore shows as null, and of two key-value lists
/// only the second is shown.
#[test]
fn the_json_view_shows_the_last_of_a_repeated_value() {
    let entry = |k: &[u8], n: u64| ld(1, &concat(&[&ld(1, k), &ld(2, &vf(3, n))]));
    let list = concat(&[
        &ld(1, b"list"),
        &ld(2, &ld(6, &entry(b"a", 1))),
        &ld(2, &ld(6, &entry(b"b", 2))),
    ]);
    let text = concat(&[&ld(1, b"text"), &ld(2, &ld(1, b"hello")), &ld(2, b"")]);
    let mut column = Vec::new();
    for kv in [&list, &text] {
        column.extend(varint(kv.len() as u64));
        column.extend_from_slice(kv);
    }

    // What protobuf makes of the same bytes: the merged values.
    let list_ref = KeyValue::decode(list.as_slice()).unwrap();
    let Some(Value::KvlistValue(l)) = list_ref.value.unwrap().value else {
        panic!("not a list")
    };
    assert_eq!(l.values.len(), 2);
    let text_ref = KeyValue::decode(text.as_slice()).unwrap();
    assert_eq!(
        text_ref.value.unwrap().value,
        Some(Value::StringValue("hello".into()))
    );

    assert_eq!(
        render(write_attrs, &column),
        json!({"list": {"b": 2}, "text": null})
    );
}

/// A link carries its trace state, flags and dropped count, and the JSON view
/// shows them.
#[test]
fn the_json_view_keeps_what_a_link_carries() {
    let link = Link {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        trace_state: "vendor=abc".into(),
        flags: 0x301,
        dropped_attributes_count: 3,
        attributes: vec![],
    };
    let got = render(write_links, &link.encode_length_delimited_to_vec());
    assert_eq!(
        got,
        json!([{
            "trace_id": "01".repeat(16),
            "span_id": "02".repeat(8),
            "trace_state": "vendor=abc",
            "flags": 0x301,
            "attributes": {},
            "dropped_attributes_count": 3,
        }])
    );
}
