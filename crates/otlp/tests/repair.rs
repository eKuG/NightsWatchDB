//! The protobuf view of stored spans is valid OTLP whatever the stored bytes
//! hold: strings that are not UTF-8, elements that are not messages, values
//! nested too deeply, and random damage. Stored pieces that are fine come
//! back byte for byte.

mod common;

use common::*;
use nwdb_core::BatchLimits;
use nwdb_otlp::{TraceDecoder, encode_spans};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use rand::Rng;

/// A lone UTF-16 surrogate as some encoders write it.
const SURROGATE: [u8; 3] = [0xed, 0xa0, 0xbd];
const FFFD3: &str = "\u{fffd}\u{fffd}\u{fffd}";

fn kv_bytes(key: &str, value: &[u8]) -> Vec<u8> {
    concat(&[&ld(1, key.as_bytes()), &ld(2, value)])
}

fn nested(levels: usize) -> Vec<u8> {
    let mut v = vf(3, 1);
    for _ in 0..levels {
        v = ld(5, &ld(1, &v));
    }
    v
}

fn prefixed(items: &[Vec<u8>]) -> Vec<u8> {
    items
        .iter()
        .flat_map(|m| [varint(m.len() as u64), m.clone()].concat())
        .collect()
}

/// A request with one span; `extra` is spliced into the span, `resource` and
/// `scope` are message bytes.
fn request(extra: &[u8], resource: &[u8], scope: &[u8]) -> Vec<u8> {
    let span = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8]), &ld(5, b"s"), extra]);
    let spans = concat(&[&ld(1, scope), &ld(2, &span)]);
    ld(1, &concat(&[&ld(1, resource), &ld(2, &spans)]))
}

fn through(req: &[u8]) -> (Vec<u8>, ExportTraceServiceRequest) {
    let batches = TraceDecoder::new(BatchLimits::DEFAULT)
        .decode(req)
        .expect("the decoder accepts the request");
    let out = encode_spans(&batches).unwrap();
    let parsed = ExportTraceServiceRequest::decode(out.as_slice())
        .unwrap_or_else(|e| panic!("a standard parser refuses the output: {e}"));
    (out, parsed)
}

fn only_span(r: &ExportTraceServiceRequest) -> &Span {
    &r.resource_spans[0].scope_spans[0].spans[0]
}

fn string_of(v: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> &str {
    match v.as_ref().and_then(|a| a.value.as_ref()) {
        Some(Value::StringValue(s)) => s,
        other => panic!("not a string: {other:?}"),
    }
}

#[test]
fn a_string_that_is_not_utf8_is_made_lossy_and_nothing_is_lost() {
    let extra = concat(&[
        &ld(9, &kv_bytes("a", &ld(1, &SURROGATE))),
        &ld(9, &kv_bytes("b", &ld(1, b"fine"))),
        &vf(10, 4),
    ]);
    let (_, out) = through(&request(&extra, &[], &[]));
    let span = only_span(&out);
    assert_eq!(span.attributes.len(), 2);
    assert_eq!(string_of(&span.attributes[0].value), FFFD3);
    assert_eq!(string_of(&span.attributes[1].value), "fine");
    assert_eq!(span.dropped_attributes_count, 4);
}

#[test]
fn a_value_nested_too_deeply_drops_its_attribute_and_counts_it() {
    let extra = concat(&[
        &ld(9, &kv_bytes("deep", &nested(70))),
        &ld(9, &kv_bytes("ok", &ld(1, b"v"))),
        &ld(9, &kv_bytes("deep2", &nested(40_000))),
        &vf(10, 5),
    ]);
    let (_, out) = through(&request(&extra, &[], &[]));
    let span = only_span(&out);
    let keys: Vec<&str> = span.attributes.iter().map(|k| k.key.as_str()).collect();
    assert_eq!(keys, ["ok"]);
    assert_eq!(span.dropped_attributes_count, 7);
}

#[test]
fn events_and_links_are_repaired_or_dropped_and_counted() {
    let good_event = concat(&[&ld(2, b"good"), &f64f(1, 9)]);
    let events = prefixed(&[
        ld(2, &SURROGATE),
        vec![0xff],
        good_event.clone(),
        // An attribute of an event that is not a message.
        ld(3, &[0xff]),
    ]);
    let links = prefixed(&[
        concat(&[&ld(1, &[1; 16]), &ld(3, &SURROGATE)]),
        vec![0x0a, 0x7f],
        ld(2, &[3; 8]),
    ]);
    // The decoder keeps these columns as sent; only their framing is shared.
    let extra = concat(&[
        &split_prefixed(&events)
            .iter()
            .flat_map(|m| ld(11, m))
            .collect::<Vec<u8>>(),
        &vf(12, 1),
        &split_prefixed(&links)
            .iter()
            .flat_map(|m| ld(13, m))
            .collect::<Vec<u8>>(),
        &vf(14, 10),
    ]);
    let (_, out) = through(&request(&extra, &[], &[]));
    let span = only_span(&out);

    let names: Vec<&str> = span.events.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, [FFFD3, "good", ""]);
    // One event could not be read, and the last event lost its attribute.
    assert_eq!(span.dropped_events_count, 2);
    assert_eq!(span.events[2].dropped_attributes_count, 1);
    assert!(span.events[2].attributes.is_empty());

    assert_eq!(span.links.len(), 2);
    assert_eq!(span.links[0].trace_state, FFFD3);
    assert_eq!(span.links[0].trace_id, [1; 16]);
    assert_eq!(span.links[1].span_id, [3; 8]);
    assert_eq!(span.dropped_links_count, 11);
}

#[test]
fn resource_and_scope_are_repaired() {
    let resource = concat(&[
        &ld(1, &kv_bytes("host.name", &ld(1, &SURROGATE))),
        &ld(1, &kv_bytes("deep", &nested(70))),
        &ld(1, &kv_bytes("ok", &ld(1, b"v"))),
        &vf(2, 2),
    ]);
    let scope = concat(&[
        &ld(1, b"lib"),
        &ld(3, &kv_bytes("a", &ld(1, &SURROGATE))),
        &ld(3, &kv_bytes("deep", &nested(70))),
    ]);
    let (_, out) = through(&request(&[], &resource, &scope));
    let rs = &out.resource_spans[0];
    let res = rs.resource.as_ref().unwrap();
    assert_eq!(res.attributes.len(), 2);
    assert_eq!(string_of(&res.attributes[0].value), FFFD3);
    assert_eq!(res.attributes[1].key, "ok");
    assert_eq!(res.dropped_attributes_count, 3);
    let sc = rs.scope_spans[0].scope.as_ref().unwrap();
    assert_eq!(sc.name, "lib");
    assert_eq!(sc.attributes.len(), 1);
    assert_eq!(sc.dropped_attributes_count, 1);
}

#[test]
fn what_is_stored_and_fine_comes_back_byte_for_byte() {
    // Valid but odd: unknown fields, a repeated value, a long varint.
    let kv = concat(&[
        &ld(1, b"k"),
        &ld(2, &ld(1, b"first")),
        &ld(2, &ld(1, b"second")),
        &vf(77, 5),
    ]);
    let event = concat(&[&ld(2, b"e"), &ld(3, &kv), &ld(99, b"x")]);
    let link = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8]), &ld(4, &kv)]);
    let resource = concat(&[&ld(1, &kv), &vf(2, 1), &ld(50, b"unknown")]);
    let scope = concat(&[&ld(1, b"lib"), &ld(3, &kv), &f64f(60, 1)]);
    let extra = concat(&[&ld(9, &kv), &ld(11, &event), &ld(13, &link)]);
    let (out, parsed) = through(&request(&extra, &resource, &scope));
    for piece in [&kv, &event, &link, &resource, &scope] {
        assert!(
            out.windows(piece.len()).any(|w| w == piece.as_slice()),
            "a stored piece was changed"
        );
    }
    let span = only_span(&parsed);
    assert_eq!(span.dropped_attributes_count, 0);
    assert_eq!(span.dropped_events_count, 0);
    assert_eq!(span.dropped_links_count, 0);
}

/// Random damage to valid requests: whatever the decoder accepts, the
/// protobuf view of it parses.
#[test]
fn damaged_requests_still_give_a_valid_protobuf_view() {
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let mut r = rng(31);
    let mut accepted = 0;
    for _ in 0..400 {
        let req = rand_request(&mut r, false).encode_to_vec();
        if req.is_empty() {
            continue;
        }
        for _ in 0..25 {
            let mut m = req.clone();
            for _ in 0..r.random_range(1..4) {
                let i = r.random_range(0..m.len());
                match r.random_range(0..3) {
                    0 => m[i] ^= 1 << r.random_range(0..8),
                    1 => m[i] = r.random(),
                    _ => m[i] = 0xff,
                }
            }
            let Ok(batches) = decoder.decode(&m) else {
                continue;
            };
            accepted += 1;
            let out = encode_spans(&batches).unwrap();
            let parsed = ExportTraceServiceRequest::decode(out.as_slice())
                .unwrap_or_else(|e| panic!("refused ({e}) for stored bytes of {m:02x?}"));
            // A parsable element is never dropped: the output holds as many
            // spans as the decoder stored.
            let spans: usize = parsed
                .resource_spans
                .iter()
                .flat_map(|x| &x.scope_spans)
                .map(|x| x.spans.len())
                .sum();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(spans, rows);
        }
    }
    assert!(
        accepted > 200,
        "only {accepted} damaged requests were accepted"
    );
}
