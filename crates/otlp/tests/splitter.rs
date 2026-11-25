//! `OtlpSplitter` against requests built with the reference types: which
//! bytes it cuts, that its cuts always satisfy the contract, and that it is
//! total on damaged input.

mod common;

use common::*;
use nwdb_core::{PayloadSplitter, RawColumn};
use nwdb_otlp::{CONTENT_KEYS, CONTENT_PREFIXES, OtlpSplitter};
use nwdb_payload::rope;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value,
};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use prost::Message;
use rand::Rng;
use std::ops::Range;

fn attrs_col(attrs: &[KeyValue]) -> Vec<u8> {
    attrs
        .iter()
        .flat_map(|a| a.encode_length_delimited_to_vec())
        .collect()
}

fn events_col(attrs: Vec<KeyValue>) -> Vec<u8> {
    Event {
        name: "e".into(),
        attributes: attrs,
        ..Default::default()
    }
    .encode_length_delimited_to_vec()
}

fn links_col(attrs: Vec<KeyValue>) -> Vec<u8> {
    Link {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        attributes: attrs,
        ..Default::default()
    }
    .encode_length_delimited_to_vec()
}

fn cuts(s: &OtlpSplitter, column: RawColumn, value: &[u8]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    s.cuts(column, value, &mut out);
    check_contract(value, &out);
    out
}

fn check_contract(value: &[u8], cuts: &[Range<usize>]) {
    assert!(rope::cuts_are_valid(value.len(), cuts), "{cuts:?}");
}

/// The cut bytes, as strings for readable failures.
fn cut_texts(value: &[u8], cuts: &[Range<usize>]) -> Vec<String> {
    cuts.iter()
        .map(|c| String::from_utf8_lossy(&value[c.clone()]).into_owned())
        .collect()
}

fn s(key: &str, v: &str) -> KeyValue {
    kv(key, str_val(v))
}

/// Whatever the cuts are, applying them and rebuilding gives the value back.
fn assert_rope_round_trip(value: &[u8], cuts: &[Range<usize>]) {
    let ids: Vec<u32> = (0..cuts.len() as u32).collect();
    let mut r = Vec::new();
    rope::encode_cut(value, cuts, &ids, &mut r);
    let mut out = Vec::new();
    rope::decode(
        &r,
        |id, out| {
            out.extend_from_slice(&value[cuts[id as usize].clone()]);
            Ok(())
        },
        &mut out,
    )
    .unwrap();
    assert_eq!(out, value);
}

#[test]
fn every_content_key_cuts_even_a_short_value_in_attributes_events_and_links() {
    let splitter = OtlpSplitter::default();
    let mut keys: Vec<String> = CONTENT_KEYS.iter().map(|k| k.to_string()).collect();
    for p in CONTENT_PREFIXES {
        keys.push(format!("{p}0.content"));
        keys.push(p.to_string());
    }
    assert!(keys.len() >= 10 + 8);
    for key in &keys {
        let attrs = [s("other", "xx"), s(key, "short"), s("another", "yy")];
        let a = attrs_col(&attrs);
        let got = cuts(&splitter, RawColumn::Attrs, &a);
        assert_eq!(cut_texts(&a, &got), ["short"], "{key}");
        assert_rope_round_trip(&a, &got);

        let e = events_col(vec![s("x", "no"), s(key, "short")]);
        let got = cuts(&splitter, RawColumn::Events, &e);
        assert_eq!(cut_texts(&e, &got), ["short"], "event {key}");

        let l = links_col(vec![s(key, "short")]);
        let got = cuts(&splitter, RawColumn::Links, &l);
        assert_eq!(cut_texts(&l, &got), ["short"], "link {key}");
    }
    // The same keys are not matched in the wrong places or as substrings.
    for key in [
        "gen_ai.input.message",
        "x.gen_ai.input.messages",
        "gen_ai.prompts",
        "input.values",
        "llm.input_messages",
        "gen_ai.promptx",
        "",
    ] {
        let a = attrs_col(&[s(key, "short")]);
        assert!(cuts(&splitter, RawColumn::Attrs, &a).is_empty(), "{key:?}");
    }
    // Event and link attributes are not cut as if they were span attributes
    // of the same column.
    let e = events_col(vec![s("plain", "short")]);
    assert!(cuts(&splitter, RawColumn::Events, &e).is_empty());
}

#[test]
fn the_length_threshold_is_exact() {
    let splitter = OtlpSplitter::default();
    assert_eq!(splitter.min_bytes(), 256);
    for (len, cut) in [
        (255, false),
        (256, true),
        (257, true),
        (1, false),
        (0, false),
    ] {
        let v = "a".repeat(len);
        let a = attrs_col(&[s("plain", &v)]);
        let got = cuts(&splitter, RawColumn::Attrs, &a);
        assert_eq!(!got.is_empty(), cut, "{len}");
        if cut {
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].len(), len);
            // Only the content of the string is cut: its tag and length stay
            // inline.
            assert!(got[0].start >= 2 && got[0].end == a.len());
        }
    }
    let small = OtlpSplitter::new(4);
    let a = attrs_col(&[s("plain", "abc"), s("plain", "abcd")]);
    assert_eq!(cut_texts(&a, &cuts(&small, RawColumn::Attrs, &a)), ["abcd"]);
    // Zero is one: an empty value is never cut, even under a content key.
    let zero = OtlpSplitter::new(0);
    let a = attrs_col(&[s("plain", ""), s("gen_ai.prompt", ""), s("plain", "x")]);
    assert_eq!(cut_texts(&a, &cuts(&zero, RawColumn::Attrs, &a)), ["x"]);
}

#[test]
fn bytes_values_follow_the_same_rules() {
    let splitter = OtlpSplitter::default();
    let a = attrs_col(&[
        kv("plain", Value::BytesValue(vec![7; 255])),
        kv("plain", Value::BytesValue(vec![8; 256])),
        kv("gen_ai.tool.call.result", Value::BytesValue(vec![9; 3])),
        kv("gen_ai.tool.call.result", Value::BytesValue(vec![])),
    ]);
    let got = cuts(&splitter, RawColumn::Attrs, &a);
    let lens: Vec<usize> = got.iter().map(|c| c.len()).collect();
    assert_eq!(lens, [256, 3]);
    assert_eq!(&a[got[0].clone()], &[8u8; 256][..]);
    assert_eq!(&a[got[1].clone()], &[9u8; 3][..]);
    // Not numbers, booleans or doubles.
    let a = attrs_col(&[
        kv("gen_ai.prompt", Value::IntValue(5)),
        kv("gen_ai.prompt", Value::BoolValue(true)),
        kv("gen_ai.prompt", Value::DoubleValue(1.5)),
    ]);
    assert!(cuts(&splitter, RawColumn::Attrs, &a).is_empty());
}

fn array_of(items: Vec<AnyValue>) -> Value {
    Value::ArrayValue(ArrayValue { values: items })
}

fn list_of(items: Vec<KeyValue>) -> Value {
    Value::KvlistValue(KeyValueList { values: items })
}

#[test]
fn structured_message_values_are_under_their_content_key() {
    let splitter = OtlpSplitter::default();
    let message = |role: &str, text: &str| any(list_of(vec![s("role", role), s("content", text)]));
    let a = attrs_col(&[
        kv(
            "gen_ai.input.messages",
            array_of(vec![message("system", "be brief"), message("user", "hi")]),
        ),
        // The same shape under another key is short text and stays inline.
        kv("messages", array_of(vec![message("user", "hi")])),
        // A long text is cut wherever it is.
        kv(
            "messages",
            array_of(vec![message("user", &"z".repeat(300))]),
        ),
    ]);
    let got = cuts(&splitter, RawColumn::Attrs, &a);
    let texts = cut_texts(&a, &got);
    assert_eq!(
        texts[..4],
        ["system", "be brief", "user", "hi"],
        "role and content values under the content key"
    );
    assert_eq!(texts.len(), 5);
    assert_eq!(texts[4], "z".repeat(300));
    assert_rope_round_trip(&a, &got);

    // Nested keys are not themselves content keys: a nested attribute named
    // like one, under a plain key, is plain.
    let a = attrs_col(&[kv(
        "plain",
        list_of(vec![s("gen_ai.prompt", "short"), s("x", "y")]),
    )]);
    assert!(cuts(&splitter, RawColumn::Attrs, &a).is_empty());
}

#[test]
fn json_arrays_are_cut_into_their_elements() {
    let splitter = OtlpSplitter::default();
    let json = r#" [ {"role":"user","content":"a, [b]  and more text to be long"} ,
        {"role":"assistant","content":"say \"hi\" and then some more words\\"}, "a string of the right kind" ] "#;
    let a = attrs_col(&[s("gen_ai.input.messages", json)]);
    let got = cuts(&splitter, RawColumn::Attrs, &a);
    assert_eq!(
        cut_texts(&a, &got),
        [
            r#"{"role":"user","content":"a, [b]  and more text to be long"}"#,
            r#"{"role":"assistant","content":"say \"hi\" and then some more words\\"}"#,
            r#""a string of the right kind""#,
        ]
    );
    assert_rope_round_trip(&a, &got);

    // Not an array, or not a complete one: the whole content is one cut.
    for text in ["{\"a\":1}", "[1, 2", "[1] x", "plain words", "[1,,2]"] {
        let a = attrs_col(&[s("gen_ai.completion", text)]);
        let got = cuts(&splitter, RawColumn::Attrs, &a);
        assert_eq!(cut_texts(&a, &got), [text]);
    }
    // An empty array has no elements.
    let a = attrs_col(&[s("gen_ai.completion", "[]")]);
    assert!(cuts(&splitter, RawColumn::Attrs, &a).is_empty());
    // A long array under a plain key is cut into elements too.
    let long = format!(
        "[{}]",
        vec![format!("\"{}\"", "0123456789".repeat(4)); 40].join(",")
    );
    let a = attrs_col(&[s("plain", &long)]);
    assert_eq!(cuts(&splitter, RawColumn::Attrs, &a).len(), 40);
}

#[test]
fn an_array_of_short_elements_is_one_piece() {
    let splitter = OtlpSplitter::default();
    // Numbers, short strings, nothing under 32 bytes on average: the whole
    // text of the array, whitespace and brackets included, is the one cut.
    let one = |text: &str, key: &str| {
        let a = attrs_col(&[s(key, text)]);
        let got = cuts(&splitter, RawColumn::Attrs, &a);
        assert_eq!(cut_texts(&a, &got), [text], "{text}");
        assert_rope_round_trip(&a, &got);
    };
    one(
        " [ 1 ,\n2\t, true,null , -3.5e+2 ] ",
        "gen_ai.input.messages",
    );
    one(r#"["a","b","c"]"#, "gen_ai.prompt");
    let numbers = format!(
        "[{}]",
        (0..5_000)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    one(&numbers, "input.value");
    // A long array of short elements under a plain key is cut too, as one.
    let plain = format!("[{}]", vec!["\"0123456789\""; 40].join(","));
    one(&plain, "plain");
    // A single short element is an array of short elements.
    one("[7]", "gen_ai.completion");

    // The bound is on the average, and exact: 32 bytes an element is cut into
    // elements, 31 is not.
    let elements = |len: usize| {
        let e = format!("\"{}\"", "x".repeat(len - 2));
        format!("[{}]", vec![e; 6].join(","))
    };
    let at = |len: usize| {
        let a = attrs_col(&[s("gen_ai.input.messages", &elements(len))]);
        cuts(&splitter, RawColumn::Attrs, &a).len()
    };
    assert_eq!(at(31), 1);
    assert_eq!(at(32), 6);
    // One large element among small ones raises the average.
    let mixed = format!("[1,2,3,\"{}\"]", "y".repeat(200));
    let a = attrs_col(&[s("gen_ai.input.messages", &mixed)]);
    assert_eq!(cuts(&splitter, RawColumn::Attrs, &a).len(), 4);
}

#[test]
fn nesting_beyond_the_depth_bound_is_left_inline() {
    let splitter = OtlpSplitter::default();
    let nest = |depth: usize| {
        let mut v = any(str_val("deep text"));
        for _ in 0..depth {
            v = any(array_of(vec![v]));
        }
        attrs_col(&[KeyValue {
            key: "gen_ai.prompt".into(),
            value: Some(v),
            ..Default::default()
        }])
    };
    assert_eq!(cuts(&splitter, RawColumn::Attrs, &nest(10)).len(), 1);
    assert_eq!(cuts(&splitter, RawColumn::Attrs, &nest(64)).len(), 1);
    assert!(cuts(&splitter, RawColumn::Attrs, &nest(65)).is_empty());
    assert!(cuts(&splitter, RawColumn::Attrs, &nest(1_000)).is_empty());
}

#[test]
fn key_after_value_and_unknown_fields_are_handled() {
    // The value is on the wire before the key.
    let splitter = OtlpSplitter::default();
    let value = ld(1, b"short");
    let kv_bytes = concat(&[
        &ld(2, &value),
        &unknowns(),
        &ld(1, b"gen_ai.prompt"),
        &unknowns(),
    ]);
    let col = concat(&[&varint(kv_bytes.len() as u64), &kv_bytes]);
    let got = cuts(&splitter, RawColumn::Attrs, &col);
    assert_eq!(cut_texts(&col, &got), ["short"]);
}

#[test]
fn damaged_input_never_panics_and_cuts_always_satisfy_the_contract() {
    let splitter = OtlpSplitter::new(8);
    let mut rng = rng(3);
    let mut columns: Vec<(RawColumn, Vec<u8>)> = Vec::new();
    for _ in 0..40 {
        let attrs: Vec<KeyValue> = rand_attrs(&mut rng);
        let mut with_content = attrs.clone();
        with_content.push(s(
            "gen_ai.input.messages",
            "[\"a\", {\"b\": [1, 2]}, \"c\"]",
        ));
        with_content.push(kv(
            "gen_ai.prompt.0",
            array_of(vec![any(str_val("x")), any(Value::BytesValue(vec![1; 40]))]),
        ));
        columns.push((RawColumn::Attrs, attrs_col(&with_content)));
        columns.push((RawColumn::Events, events_col(with_content.clone())));
        columns.push((RawColumn::Links, links_col(with_content)));
    }
    for (column, original) in &columns {
        // The intact column, each truncation, and bit flips.
        let got = cuts(&splitter, *column, original);
        assert_rope_round_trip(original, &got);
        for n in (0..original.len()).step_by(1 + original.len() / 300) {
            cuts(&splitter, *column, &original[..n]);
        }
        for _ in 0..30 {
            let mut v = original.clone();
            for _ in 0..rng.random_range(1..4) {
                let at = rng.random_range(0..v.len());
                v[at] ^= 1 << rng.random_range(0..8);
            }
            let got = cuts(&splitter, *column, &v);
            assert_rope_round_trip(&v, &got);
        }
        // The wrong column for the bytes.
        for other in [RawColumn::Attrs, RawColumn::Events, RawColumn::Links] {
            cuts(&splitter, other, original);
        }
    }
    // Pure noise.
    for _ in 0..3_000 {
        let n = rng.random_range(0..200);
        let v: Vec<u8> = (0..n).map(|_| rng.random()).collect();
        for column in [RawColumn::Attrs, RawColumn::Events, RawColumn::Links] {
            let got = cuts(&splitter, column, &v);
            assert_rope_round_trip(&v, &got);
        }
    }
    // Lengths that point far past the end, and nothing at all.
    for v in [
        &[][..],
        &[0xff; 20][..],
        &[0x80][..],
        &[5, 1, 2][..],
        &[0xff, 0xff, 0xff, 0xff, 0x0f, 1][..],
    ] {
        for column in [RawColumn::Attrs, RawColumn::Events, RawColumn::Links] {
            assert!(cuts(&splitter, column, v).is_empty());
        }
    }
}

#[test]
fn random_requests_give_valid_cuts_that_rebuild_every_value() {
    let splitter = OtlpSplitter::new(4);
    let mut rng = rng(8);
    for _ in 0..200 {
        let attrs = rand_attrs(&mut rng);
        let col = attrs_col(&attrs);
        let got = cuts(&splitter, RawColumn::Attrs, &col);
        assert_rope_round_trip(&col, &got);
    }
}
