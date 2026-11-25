//! Hand-built inputs: promotion rules, field order, unknown fields, empty and
//! malformed requests.

mod common;

use common::*;
use nwdb_core::{BatchLimits, Error, SpanRecord};
use nwdb_otlp::{RECORD_TAG_OTLP_TRACES, TraceDecoder};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use rand::Rng;

fn decoder() -> TraceDecoder {
    TraceDecoder::new(BatchLimits::DEFAULT)
}

fn request_with_attrs(attrs: Vec<KeyValue>) -> Vec<u8> {
    let span = Span {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        attributes: attrs,
        ..Default::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// All promoted columns of the only row, as text.
fn promoted(attrs: Vec<KeyValue>) -> Vec<Option<String>> {
    let batches = decoder().decode(&request_with_attrs(attrs)).unwrap();
    let rows = all_rows(&batches);
    assert_eq!(rows.len(), 1);
    columns(&rows[0])
}

fn columns(r: &SpanRecord<'_>) -> Vec<Option<String>> {
    let s = |v: Option<&str>| v.map(str::to_string);
    vec![
        s(r.operation),
        s(r.provider),
        s(r.request_model),
        s(r.response_model),
        r.input_tokens.map(|n| n.to_string()),
        r.output_tokens.map(|n| n.to_string()),
        s(r.conversation_id),
        s(r.session_id),
        s(r.user_id),
        s(r.agent_name),
        s(r.tool_name),
        s(r.error_type),
    ]
}

/// Column index, is-token-column, newest name and older names.
const TABLE: [(usize, bool, &str, &[&str]); 12] = [
    (0, false, "gen_ai.operation.name", &[]),
    (1, false, "gen_ai.provider.name", &["gen_ai.system"]),
    (2, false, "gen_ai.request.model", &[]),
    (3, false, "gen_ai.response.model", &[]),
    (
        4,
        true,
        "gen_ai.usage.input_tokens",
        &["gen_ai.usage.prompt_tokens"],
    ),
    (
        5,
        true,
        "gen_ai.usage.output_tokens",
        &["gen_ai.usage.completion_tokens"],
    ),
    (6, false, "gen_ai.conversation.id", &[]),
    (7, false, "session.id", &[]),
    (8, false, "user.id", &[]),
    (9, false, "gen_ai.agent.name", &[]),
    (10, false, "gen_ai.tool.name", &[]),
    (11, false, "error.type", &[]),
];

fn only(col: usize, v: &str) -> Vec<Option<String>> {
    let mut want = vec![None; 12];
    want[col] = Some(v.to_string());
    want
}

#[test]
fn new_names_and_older_names_are_promoted() {
    for (col, token, new, old) in TABLE {
        let value = |s: &str| {
            if token {
                Value::IntValue(s.parse().unwrap())
            } else {
                str_val(s)
            }
        };
        let sample = if token { "41" } else { "v" };
        // The attribute stays in attrs, and nothing else is promoted.
        assert_eq!(
            promoted(vec![kv(new, value(sample))]),
            only(col, sample),
            "{new}"
        );
        for old in old {
            assert_eq!(
                promoted(vec![kv(old, value(sample))]),
                only(col, sample),
                "{old}"
            );
            // The newer name wins whichever comes first.
            let n = value("2");
            let o = value("1");
            let sample_new = "2";
            assert_eq!(
                promoted(vec![kv(new, n.clone()), kv(old, o.clone())]),
                only(col, sample_new)
            );
            assert_eq!(
                promoted(vec![kv(old, o), kv(new, n)]),
                only(col, sample_new)
            );
        }
    }
}

#[test]
fn promotion_keeps_the_original_attribute() {
    let bytes = request_with_attrs(vec![kv("gen_ai.system", str_val("openai"))]);
    let batches = decoder().decode(&bytes).unwrap();
    let rows = all_rows(&batches);
    let parts = split_prefixed(rows[0].attrs);
    assert_eq!(parts.len(), 1);
    assert_eq!(
        KeyValue::decode(parts[0]).unwrap(),
        kv("gen_ai.system", str_val("openai"))
    );
}

fn wrong_types() -> Vec<Option<Value>> {
    vec![
        Some(Value::BoolValue(true)),
        Some(Value::DoubleValue(1.5)),
        Some(Value::BytesValue(vec![1, 2])),
        Some(Value::ArrayValue(ArrayValue {
            values: vec![AnyValue {
                value: Some(str_val("a")),
            }],
        })),
        None,
    ]
}

#[test]
fn wrong_value_types_leave_the_column_null() {
    for (col, token, new, _) in TABLE {
        for v in wrong_types() {
            let attr = KeyValue {
                key: new.to_string(),
                value: Some(AnyValue { value: v }),
                ..Default::default()
            };
            assert_eq!(promoted(vec![attr])[col], None, "{new}");
        }
        // An int is not text, and a missing value is not anything.
        if !token {
            assert_eq!(promoted(vec![kv(new, Value::IntValue(5))])[col], None);
        }
        let no_value = KeyValue {
            key: new.to_string(),
            value: None,
            ..Default::default()
        };
        assert_eq!(promoted(vec![no_value])[col], None);
    }
}

#[test]
fn token_columns_take_ints_and_numeric_strings() {
    let cases = [
        (str_val("123"), Some("123")),
        (str_val("-7"), None),
        (str_val("0"), Some("0")),
        (str_val("12x"), None),
        (str_val(""), None),
        (str_val(" 5"), None),
        (str_val("1.5"), None),
        (str_val("99999999999999999999"), None),
        (Value::IntValue(i64::MIN), None),
        (Value::IntValue(-1), None),
        (Value::IntValue(i64::MAX), Some("9223372036854775807")),
        (Value::IntValue(0), Some("0")),
    ];
    for (v, want) in cases {
        let got = promoted(vec![kv("gen_ai.usage.input_tokens", v.clone())]);
        assert_eq!(got[4].as_deref(), want, "{v:?}");
        let got = promoted(vec![kv("gen_ai.usage.completion_tokens", v)]);
        assert_eq!(got[5].as_deref(), want);
    }
}

/// A token count cannot be below zero: a negative value is not usable, so
/// the older name is tried, or the column stays null. The attribute stays in
/// `attrs` untouched.
#[test]
fn a_negative_token_count_is_not_usable() {
    for (col, new, old) in [
        (4, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"),
        (
            5,
            "gen_ai.usage.output_tokens",
            "gen_ai.usage.completion_tokens",
        ),
    ] {
        // Only a negative value: the column is null.
        assert_eq!(promoted(vec![kv(new, Value::IntValue(-3))])[col], None);
        assert_eq!(promoted(vec![kv(old, str_val("-3"))])[col], None);
        // A negative newer name falls through to a usable older one, in
        // either order on the wire.
        assert_eq!(
            promoted(vec![
                kv(new, Value::IntValue(-3)),
                kv(old, Value::IntValue(8))
            ])[col]
                .as_deref(),
            Some("8")
        );
        assert_eq!(
            promoted(vec![kv(old, str_val("8")), kv(new, str_val("-3"))])[col].as_deref(),
            Some("8")
        );
        // Two negatives: null. Zero is a usable value and wins over an older one.
        assert_eq!(
            promoted(vec![
                kv(new, Value::IntValue(-3)),
                kv(old, Value::IntValue(-4))
            ])[col],
            None
        );
        assert_eq!(
            promoted(vec![
                kv(new, Value::IntValue(0)),
                kv(old, Value::IntValue(8))
            ])[col]
                .as_deref(),
            Some("0")
        );
        // The attribute itself is kept as received.
        let batches = decoder()
            .decode(&request_with_attrs(vec![kv(new, Value::IntValue(-3))]))
            .unwrap();
        let rows = all_rows(&batches);
        assert!(
            rows[0]
                .attrs
                .windows(new.len())
                .any(|w| w == new.as_bytes())
        );
    }
}

#[test]
fn a_duplicated_key_takes_its_last_value() {
    let got = promoted(vec![
        kv("session.id", str_val("a")),
        kv("session.id", str_val("b")),
    ]);
    assert_eq!(got, only(7, "b"));
    // The last occurrence wins even when it is the wrong type.
    let got = promoted(vec![
        kv("session.id", str_val("a")),
        kv("session.id", Value::IntValue(3)),
    ]);
    assert_eq!(got[7], None);
    let got = promoted(vec![
        kv("gen_ai.usage.input_tokens", Value::IntValue(1)),
        kv("gen_ai.usage.input_tokens", str_val("9")),
    ]);
    assert_eq!(got[4].as_deref(), Some("9"));
    // Last wins per name: a newer name stays ahead of the older one.
    let got = promoted(vec![
        kv("gen_ai.system", str_val("old1")),
        kv("gen_ai.provider.name", str_val("new")),
        kv("gen_ai.system", str_val("old2")),
    ]);
    assert_eq!(got[1].as_deref(), Some("new"));
}

/// A newer name whose value cannot be used does not hide an older name.
#[test]
fn the_first_name_with_a_usable_value_wins() {
    for (col, token, new, old) in TABLE {
        let Some(old) = old.first() else { continue };
        let good = |s: &str| {
            if token {
                Value::IntValue(s.parse().unwrap())
            } else {
                str_val(s)
            }
        };
        let bad = || {
            if token {
                str_val("not a number")
            } else {
                Value::IntValue(7)
            }
        };
        // Newer name unusable, older usable: the older one is taken, in
        // either order on the wire.
        assert_eq!(
            promoted(vec![kv(new, bad()), kv(old, good("1"))]),
            only(col, "1"),
            "{new}"
        );
        assert_eq!(
            promoted(vec![kv(old, good("1")), kv(new, bad())]),
            only(col, "1"),
            "{new}"
        );
        // Both usable: the newer name wins.
        assert_eq!(
            promoted(vec![kv(old, good("1")), kv(new, good("2"))]),
            only(col, "2")
        );
        // Only the older name.
        assert_eq!(promoted(vec![kv(old, good("1"))]), only(col, "1"));
        // Neither is usable.
        assert_eq!(promoted(vec![kv(new, bad()), kv(old, bad())])[col], None);
        // A newer name with no value at all is skipped too.
        let empty = KeyValue {
            key: new.to_string(),
            value: None,
            ..Default::default()
        };
        assert_eq!(promoted(vec![empty, kv(old, good("1"))]), only(col, "1"));
    }
}

#[test]
fn the_last_member_of_a_value_wins() {
    // AnyValue with string_value then int_value on the wire is an int.
    let any = concat(&[&ld(1, b"5"), &vf(3, 8)]);
    let kvb = concat(&[&ld(1, b"gen_ai.usage.input_tokens"), &ld(2, &any)]);
    let span = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8]), &ld(9, &kvb)]);
    let req = ld(1, &ld(2, &ld(2, &span)));
    let batches = decoder().decode(&req).unwrap();
    assert_eq!(all_rows(&batches)[0].input_tokens, Some(8));
}

// ---- field order and unknown fields ----

fn full_span(name: &[u8]) -> Vec<u8> {
    let status = concat(&[&unknowns(), &ld(2, b"boom"), &vf(3, 2)]);
    let kv1 = concat(&[
        &unknowns(),
        &ld(1, b"session.id"),
        &ld(2, &concat(&[&unknowns(), &ld(1, b"s-1")])),
    ]);
    concat(&[
        &unknowns(),
        &ld(5, name),
        &ld(1, &[7; 16]),
        &f64f(8, 200),
        &ld(2, &[9; 8]),
        &vf(6, 3),
        &f64f(7, 100),
        &ld(4, &[3; 8]),
        &ld(9, &kv1),
        &ld(15, &status),
        &f32f(16, 0x101),
        &ld(3, b"k=v"),
        &vf(10, 4),
    ])
}

#[test]
fn fields_in_any_order_with_unknown_fields_at_every_level() {
    let resource = concat(&[
        &unknowns(),
        &ld(
            1,
            &concat(&[&ld(1, b"service.name"), &ld(2, &ld(1, b"svc"))]),
        ),
    ]);
    let scope = concat(&[&unknowns(), &ld(1, b"lib"), &ld(2, b"1.2")]);
    // Names repeated: the last wins. The scope follows its spans and the
    // resource follows the scope spans.
    let span_bytes = full_span(b"first");
    let span2 = concat(&[&ld(5, b"first"), &full_span(b"second")]);
    let scope_spans = concat(&[
        &unknowns(),
        &ld(2, &span_bytes),
        &ld(2, &span2),
        &ld(1, &scope),
        &ld(3, b"url"),
    ]);
    let resource_spans = concat(&[
        &unknowns(),
        &ld(2, &scope_spans),
        &ld(1, &resource),
        &ld(3, b"url"),
    ]);
    let req = concat(&[&unknowns(), &ld(1, &resource_spans), &unknowns()]);

    let batches = decoder().decode(&req).unwrap();
    let rows = all_rows(&batches);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "first");
    assert_eq!(rows[1].name, "second");
    for r in &rows {
        assert_eq!(r.service_name, Some("svc"));
        assert_eq!(r.scope_name, Some("lib"));
        assert_eq!(r.scope_version, Some("1.2"));
        assert_eq!(r.scope, Some(scope.as_slice()));
        assert_eq!(r.resource_schema_url, Some("url"));
        assert_eq!(r.scope_schema_url, Some("url"));
        assert_eq!(r.resource, Some(resource.as_slice()));
        assert_eq!(r.trace_id.0, [7; 16]);
        assert_eq!(r.span_id.0, [9; 8]);
        assert_eq!(r.parent_span_id.unwrap().0, [3; 8]);
        assert_eq!((r.kind, r.start_ns, r.end_ns), (3, 100, 200));
        assert_eq!((r.status_code, r.status_message), (2, Some("boom")));
        assert_eq!((r.flags, r.trace_state), (0x101, Some("k=v")));
        assert_eq!(r.dropped_attributes_count, 4);
        assert_eq!(r.session_id, Some("s-1"));
        assert_eq!(split_prefixed(r.attrs).len(), 1);
    }
}

#[test]
fn a_repeated_scalar_takes_the_last_value_as_prost_does() {
    let span = concat(&[
        &ld(1, &[1; 16]),
        &ld(2, &[2; 8]),
        &vf(6, 1),
        &vf(6, 2),
        &f64f(7, 5),
        &f64f(7, 6),
        &ld(2, &[4; 8]),
    ]);
    let req = ld(1, &ld(2, &ld(2, &span)));
    let rows_batches = decoder().decode(&req).unwrap();
    let rows = all_rows(&rows_batches);
    let reference = ExportTraceServiceRequest::decode(req.as_slice()).unwrap();
    let s = &reference.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(
        (rows[0].kind, rows[0].start_ns),
        (s.kind, s.start_time_unix_nano as i64)
    );
    assert_eq!(rows[0].span_id.0.as_slice(), s.span_id.as_slice());
    assert_eq!(rows[0].span_id.0, [4; 8]);
}

#[test]
fn a_resource_that_is_absent_is_null_and_an_empty_one_is_empty() {
    let span = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8])]);
    let none = decoder().decode(&ld(1, &ld(2, &ld(2, &span)))).unwrap();
    assert!(all_rows(&none)[0].resource.is_none());
    let empty = decoder()
        .decode(&ld(1, &concat(&[&ld(1, b""), &ld(2, &ld(2, &span))])))
        .unwrap();
    assert_eq!(all_rows(&empty)[0].resource, Some(&[][..]));
}

// ---- empty and edge requests ----

#[test]
fn empty_requests_give_no_batches() {
    assert!(decoder().decode(&[]).unwrap().is_empty());
    assert!(decoder().decode(&ld(1, b"")).unwrap().is_empty());
    assert!(decoder().decode(&ld(1, &ld(2, b""))).unwrap().is_empty());
    let resource_only = ld(1, &ld(1, &ld(1, &concat(&[&ld(1, b"k")]))));
    assert!(decoder().decode(&resource_only).unwrap().is_empty());
    let (b, stats) = decoder().decode_with_stats(&ld(1, &ld(2, b""))).unwrap();
    assert!(b.is_empty());
    assert_eq!((stats.spans, stats.resources, stats.scopes), (0, 1, 1));
}

#[test]
fn an_empty_or_zero_parent_is_a_root() {
    let ids = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8])]);
    let cases = [
        (concat(&[&ids, &ld(4, b"")]), true),
        (concat(&[&ids, &ld(4, &[0; 8])]), true),
        (ids.clone(), true),
        (concat(&[&ids, &ld(4, &[0, 0, 0, 0, 0, 0, 0, 1])]), false),
    ];
    for (span, root) in cases {
        let batches = decoder().decode(&ld(1, &ld(2, &ld(2, &span)))).unwrap();
        assert_eq!(all_rows(&batches)[0].parent_span_id.is_none(), root);
    }
}

#[test]
fn the_decoder_is_a_record_decoder() {
    use nwdb_core::RecordDecoder;
    assert_eq!(RECORD_TAG_OTLP_TRACES, 1);
    let d: Box<dyn RecordDecoder> = Box::new(decoder());
    let ids = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8])]);
    let batches = d.decode(&ld(1, &ld(2, &ld(2, &ids)))).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

// ---- malformed input ----

fn corrupt(bytes: &[u8]) -> String {
    match decoder().decode(bytes) {
        Err(Error::Corrupt(m)) => m,
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

fn ok_or_corrupt(bytes: &[u8]) {
    match decoder().decode(bytes) {
        Ok(_) | Err(Error::Corrupt(_)) => {}
        Err(e) => panic!("unexpected error {e:?}"),
    }
}

fn wrap_span(span: &[u8]) -> Vec<u8> {
    ld(1, &ld(2, &ld(2, span)))
}

#[test]
fn targeted_malformed_inputs_are_corrupt() {
    // A varint of 11 bytes where a tag is expected.
    let mut overlong = vec![0x80; 10];
    overlong.push(0x01);
    assert!(corrupt(&overlong).contains("varint"));
    // A tenth byte that overflows 64 bits.
    let mut overflow = vec![0xff; 9];
    overflow.push(0x7f);
    assert!(corrupt(&overflow).contains("varint"));
    // A length that runs past the end.
    assert!(corrupt(&[0x0a, 0xff, 0x01]).contains("length"));
    assert!(corrupt(&[0x0a, 0x05, 1, 2]).contains("length"));
    // A length that does not fit in 64 bits.
    let mut huge = vec![0x0a];
    huge.extend([0xff; 9]);
    huge.push(0x01);
    corrupt(&huge);
    // Group wire types, at the top and inside a span.
    assert!(corrupt(&[0x0b]).contains("group"));
    assert!(corrupt(&[0x0c]).contains("group"));
    assert!(corrupt(&wrap_span(&[0x0b, 0x0c])).contains("group"));
    // Reserved wire types and field number zero.
    corrupt(&[0x0e]);
    corrupt(&[0x00, 0x00]);
    // A truncated fixed field.
    corrupt(&wrap_span(&concat(&[
        &ld(1, &[1; 16]),
        &ld(2, &[2; 8]),
        &[0x39, 1, 2, 3],
    ])));
    // A known field with the wrong wire type.
    let m = corrupt(&wrap_span(&concat(&[
        &ld(1, &[1; 16]),
        &ld(2, &[2; 8]),
        &vf(5, 1),
    ])));
    assert!(m.contains("spans.name"), "{m}");
}

#[test]
fn a_parent_id_of_the_wrong_length_is_corrupt() {
    let span = |parent: &[u8]| {
        wrap_span(&concat(&[
            &ld(1, &[1; 16]),
            &ld(2, &[2; 8]),
            &ld(4, parent),
        ]))
    };
    let m = corrupt(&span(&[1; 9]));
    assert!(
        m.contains("resource_spans.scope_spans.spans.parent_span_id"),
        "{m}"
    );
    assert!(corrupt(&span(&[1; 7])).contains("parent_span_id"));
}

#[test]
fn invalid_utf8_is_corrupt() {
    let ids = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8])]);
    let bad = [0xff, 0xfe];
    assert!(
        corrupt(&wrap_span(&concat(&[&ids, &ld(5, &bad)]))).contains("spans.name: invalid UTF-8")
    );
    assert!(corrupt(&wrap_span(&concat(&[&ids, &ld(3, &bad)]))).contains("trace_state"));
    let status = ld(2, &bad);
    assert!(corrupt(&wrap_span(&concat(&[&ids, &ld(15, &status)]))).contains("status.message"));
    // An attribute key, and the string value of a promoted attribute.
    let key = ld(1, &bad);
    assert!(corrupt(&wrap_span(&concat(&[&ids, &ld(9, &key)]))).contains("key"));
    let val = concat(&[&ld(1, b"session.id"), &ld(2, &ld(1, &bad))]);
    assert!(corrupt(&wrap_span(&concat(&[&ids, &ld(9, &val)]))).contains("value"));
    // The scope and the service name.
    let scope = ld(1, &ld(1, &bad));
    assert!(corrupt(&ld(1, &ld(2, &concat(&[&scope, &ld(2, &ids)])))).contains("scope.name"));
    let res = ld(
        1,
        &ld(1, &concat(&[&ld(1, b"service.name"), &ld(2, &ld(1, &bad))])),
    );
    corrupt(&ld(1, &concat(&[&res, &ld(2, &ld(2, &ids))])));
    // Both schema URLs.
    let scope_spans = ld(2, &concat(&[&ld(3, &bad), &ld(2, &ids)]));
    assert!(corrupt(&ld(1, &scope_spans)).contains("scope_spans.schema_url"));
    assert!(corrupt(&ld(1, &ld(3, &bad))).contains("resource_spans.schema_url"));
    // Bytes that are not UTF-8 in a value that is only copied are not read.
    let blob = concat(&[&ld(1, b"blob"), &ld(2, &ld(1, &bad))]);
    decoder()
        .decode(&wrap_span(&concat(&[&ids, &ld(9, &blob)])))
        .unwrap();
}

fn small_request() -> Vec<u8> {
    let mut r = rng(11);
    let mut req = rand_request(&mut r, false);
    // A request with every layer present and some bytes in each.
    let mut span = rand_span(&mut r);
    span.attributes = vec![
        kv("gen_ai.usage.input_tokens", str_val("12")),
        kv("session.id", str_val("s")),
        kv("blob", Value::IntValue(-3)),
    ];
    req.resource_spans = vec![ResourceSpans {
        resource: Some(rand_resource(&mut r)),
        scope_spans: vec![ScopeSpans {
            scope: Some(
                opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope".into(),
                    version: "1".into(),
                    attributes: vec![kv("k", str_val("v"))],
                    dropped_attributes_count: 2,
                },
            ),
            spans: vec![span, rand_span(&mut r)],
            schema_url: "https://example.test/s".into(),
        }],
        schema_url: "https://example.test/r".into(),
    }];
    req.encode_to_vec()
}

#[test]
fn every_truncation_is_ok_or_corrupt() {
    let bytes = small_request();
    for n in 0..=bytes.len() {
        ok_or_corrupt(&bytes[..n]);
    }
    let mut r = rng(21);
    for seed in 0..40 {
        let bytes = rand_request(&mut rng(seed), false).encode_to_vec();
        for _ in 0..100.min(bytes.len()) {
            ok_or_corrupt(&bytes[..r.random_range(0..=bytes.len())]);
        }
    }
}

#[test]
fn every_flipped_byte_is_ok_or_corrupt() {
    let bytes = small_request();
    for i in 0..bytes.len() {
        for mask in [0x01, 0x80, 0xff, 0x7f] {
            let mut b = bytes.clone();
            b[i] ^= mask;
            ok_or_corrupt(&b);
        }
    }
}

#[test]
fn random_bytes_are_ok_or_corrupt() {
    let mut r = rng(31);
    for _ in 0..5000 {
        let len = r.random_range(0..120);
        let bytes: Vec<u8> = (0..len).map(|_| r.random()).collect();
        ok_or_corrupt(&bytes);
        // Biased towards tags and short lengths, which gets deeper.
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                *[
                    0x0a, 0x12, 0x1a, 0x22, 0x2a, 0x4a, 0x7a, 0x01, 0x02, 0x08, 0x10,
                ]
                .get(r.random_range(0..14))
                .unwrap_or(&r.random())
            })
            .collect();
        ok_or_corrupt(&bytes);
    }
}

#[test]
fn encode_rejects_a_batch_with_the_wrong_schema() {
    use arrow_array::{Int32Array, RecordBatch};
    use std::sync::Arc;
    let b = RecordBatch::try_from_iter([("x", Arc::new(Int32Array::from(vec![1])) as _)]).unwrap();
    assert!(matches!(
        nwdb_otlp::encode_spans(&[b]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(nwdb_otlp::encode_spans(&[]).unwrap().is_empty());
}
