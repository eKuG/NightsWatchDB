//! Shared helpers: a random request generator built on the reference message
//! types, row access, and hand-built wire bytes.
#![allow(dead_code)]

use arrow_array::RecordBatch;
use nwdb_core::{SpanColumns, SpanRecord};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status,
    span::{Event, Link},
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub fn all_rows(batches: &[RecordBatch]) -> Vec<SpanRecord<'_>> {
    let mut rows = Vec::new();
    for b in batches {
        let cols = SpanColumns::try_new(b).expect("span schema");
        rows.extend((0..cols.len()).map(|i| cols.record(i)));
    }
    rows
}

/// Bytes of variable-length values in a batch, as the batch builder counts
/// them: the strings and raw columns of every row, except that each distinct
/// origin value counts once, plus a 4-byte key per origin column per row.
pub fn value_bytes(batch: &RecordBatch) -> usize {
    use std::collections::HashSet;
    let cols = SpanColumns::try_new(batch).expect("span schema");
    let mut origin: [HashSet<Vec<u8>>; 7] = Default::default();
    let mut total = 0;
    for row in 0..cols.len() {
        let r = cols.record(row);
        let strings = [
            Some(r.name),
            r.status_message,
            r.trace_state,
            r.operation,
            r.provider,
            r.request_model,
            r.response_model,
            r.conversation_id,
            r.session_id,
            r.user_id,
            r.agent_name,
            r.tool_name,
            r.error_type,
        ];
        let raws = [Some(r.attrs), r.events, r.links];
        total += strings.iter().flatten().map(|s| s.len()).sum::<usize>()
            + raws.iter().flatten().map(|b| b.len()).sum::<usize>()
            + 7 * 4;
        let o = cols.origin(row);
        let values: [Option<&[u8]>; 7] = [
            o.resource,
            o.service_name.map(str::as_bytes),
            o.resource_schema_url.map(str::as_bytes),
            o.scope,
            o.scope_name.map(str::as_bytes),
            o.scope_version.map(str::as_bytes),
            o.scope_schema_url.map(str::as_bytes),
        ];
        for (set, v) in origin.iter_mut().zip(values) {
            if let Some(v) = v {
                set.insert(v.to_vec());
            }
        }
    }
    total + origin.iter().flatten().map(Vec::len).sum::<usize>()
}

/// Splits a length-prefixed column into its messages.
pub fn split_prefixed(mut b: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    while !b.is_empty() {
        let (mut n, mut shift, mut i) = (0usize, 0, 0);
        loop {
            let byte = b[i];
            n |= usize::from(byte & 0x7f) << shift;
            i += 1;
            if byte < 0x80 {
                break;
            }
            shift += 7;
        }
        out.push(&b[i..i + n]);
        b = &b[i + n..];
    }
    out
}

// ---- random requests ----

const WORDS: [&str; 8] = [
    "plain",
    "h\u{e9}llo w\u{f6}rld",
    "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30b9}\u{30c8}",
    "emoji \u{1f642}\u{1f680}",
    "\u{0}nul and \n newline",
    "gpt-4o",
    "claude",
    "\u{10ffff}",
];

pub fn rand_string(rng: &mut StdRng) -> String {
    match rng.random_range(0..5) {
        0 => String::new(),
        1 | 2 => WORDS[rng.random_range(0..WORDS.len())].to_string(),
        3 => (0..rng.random_range(1..40))
            .map(|_| char::from(rng.random_range(b'a'..=b'z')))
            .collect(),
        _ => {
            let a = WORDS[rng.random_range(0..WORDS.len())];
            let b = WORDS[rng.random_range(0..WORDS.len())];
            format!("{a}-{b}")
        }
    }
}

pub fn any(v: Value) -> AnyValue {
    AnyValue { value: Some(v) }
}

pub fn kv(key: &str, v: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(any(v)),
        ..Default::default()
    }
}

pub fn str_val(s: &str) -> Value {
    Value::StringValue(s.to_string())
}

pub fn rand_any(rng: &mut StdRng, depth: u32) -> AnyValue {
    let kinds = if depth < 3 { 9 } else { 6 };
    let value = match rng.random_range(0..kinds) {
        0 => Some(Value::StringValue(rand_string(rng))),
        1 => Some(Value::BoolValue(rng.random())),
        2 => Some(Value::IntValue(match rng.random_range(0..4) {
            0 => i64::MIN,
            1 => i64::MAX,
            _ => rng.random(),
        })),
        3 => Some(Value::DoubleValue(rng.random_range(-1e12..1e12))),
        4 => Some(Value::BytesValue(
            (0..rng.random_range(0..30)).map(|_| rng.random()).collect(),
        )),
        5 => None,
        6 | 7 => Some(Value::ArrayValue(ArrayValue {
            values: (0..rng.random_range(0..4))
                .map(|_| rand_any(rng, depth + 1))
                .collect(),
        })),
        _ => Some(Value::KvlistValue(KeyValueList {
            values: (0..rng.random_range(0..4))
                .map(|_| rand_kv(rng, depth + 1))
                .collect(),
        })),
    };
    AnyValue { value }
}

pub fn rand_kv(rng: &mut StdRng, depth: u32) -> KeyValue {
    KeyValue {
        key: rand_string(rng),
        value: if rng.random_bool(0.05) {
            None
        } else {
            Some(rand_any(rng, depth))
        },
        ..Default::default()
    }
}

/// Every attribute key that is promoted, new and old names.
pub const PROMOTED_KEYS: [&str; 18] = [
    "gen_ai.operation.name",
    "gen_ai.provider.name",
    "gen_ai.system",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.prompt_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.completion_tokens",
    "gen_ai.conversation.id",
    "session.id",
    "user.id",
    "gen_ai.agent.name",
    "gen_ai.tool.name",
    "error.type",
    "service.name",
    "other.key",
    "gen_ai.input.messages",
];

fn promoted_kv(rng: &mut StdRng) -> KeyValue {
    let key = PROMOTED_KEYS[rng.random_range(0..PROMOTED_KEYS.len())];
    let value = match rng.random_range(0..6) {
        0 | 1 => Value::StringValue(rand_string(rng)),
        2 => Value::StringValue(rng.random_range(-1000i64..1000).to_string()),
        3 | 4 => Value::IntValue(rng.random_range(-5i64..5000)),
        _ => return rand_kv_with_key(rng, key),
    };
    kv(key, value)
}

fn rand_kv_with_key(rng: &mut StdRng, key: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(rand_any(rng, 0)),
        ..Default::default()
    }
}

pub fn rand_attrs(rng: &mut StdRng) -> Vec<KeyValue> {
    (0..rng.random_range(0..8))
        .map(|_| {
            if rng.random_bool(0.6) {
                promoted_kv(rng)
            } else {
                rand_kv(rng, 0)
            }
        })
        .collect()
}

fn rand_bytes(rng: &mut StdRng, n: usize) -> Vec<u8> {
    (0..n).map(|_| rng.random()).collect()
}

fn rand_event(rng: &mut StdRng) -> Event {
    Event {
        time_unix_nano: rng.random(),
        name: rand_string(rng),
        attributes: rand_attrs(rng),
        dropped_attributes_count: rng.random_range(0..3),
    }
}

fn rand_link(rng: &mut StdRng) -> Link {
    Link {
        trace_id: rand_bytes(rng, 16),
        span_id: rand_bytes(rng, 8),
        trace_state: rand_string(rng),
        attributes: rand_attrs(rng),
        dropped_attributes_count: rng.random_range(0..3),
        flags: rng.random_range(0..3),
    }
}

/// A span with valid ids; every other field present or absent at random.
pub fn rand_span(rng: &mut StdRng) -> Span {
    let mut s = Span {
        trace_id: rand_bytes(rng, 16),
        span_id: rand_bytes(rng, 8),
        ..Default::default()
    };
    if rng.random_bool(0.5) {
        s.trace_state = rand_string(rng);
    }
    s.parent_span_id = match rng.random_range(0..4) {
        0 => Vec::new(),
        1 => vec![0; 8],
        _ => rand_bytes(rng, 8),
    };
    if rng.random_bool(0.9) {
        s.name = rand_string(rng);
    }
    if rng.random_bool(0.7) {
        s.kind = rng.random_range(0..6);
    }
    if rng.random_bool(0.8) {
        s.start_time_unix_nano = rng.random();
    }
    if rng.random_bool(0.8) {
        s.end_time_unix_nano = rng.random();
    }
    s.attributes = rand_attrs(rng);
    if rng.random_bool(0.3) {
        s.dropped_attributes_count = rng.random();
    }
    s.events = (0..rng.random_range(0..3))
        .map(|_| rand_event(rng))
        .collect();
    if rng.random_bool(0.3) {
        s.dropped_events_count = rng.random();
    }
    s.links = (0..rng.random_range(0..3))
        .map(|_| rand_link(rng))
        .collect();
    if rng.random_bool(0.3) {
        s.dropped_links_count = rng.random();
    }
    if rng.random_bool(0.6) {
        s.status = Some(Status {
            message: rand_string(rng),
            code: rng.random_range(0..3),
        });
    }
    if rng.random_bool(0.4) {
        s.flags = rng.random();
    }
    s
}

pub fn rand_resource(rng: &mut StdRng) -> Resource {
    let mut attributes = rand_attrs(rng);
    if rng.random_bool(0.8) {
        let v = if rng.random_bool(0.8) {
            str_val(&rand_string(rng))
        } else {
            Value::IntValue(7)
        };
        attributes.insert(
            rng.random_range(0..=attributes.len()),
            kv("service.name", v),
        );
    }
    Resource {
        attributes,
        dropped_attributes_count: rng.random_range(0..3),
        ..Default::default()
    }
}

/// A random request. With `big`, one span carries a 1 MiB string attribute.
pub fn rand_request(rng: &mut StdRng, big: bool) -> ExportTraceServiceRequest {
    let mut resource_spans: Vec<ResourceSpans> = (0..rng.random_range(0..4))
        .map(|_| {
            let resource = match rng.random_range(0..5) {
                0 => None,
                1 => Some(Resource::default()),
                _ => Some(rand_resource(rng)),
            };
            let scope_spans = (0..rng.random_range(0..4))
                .map(|_| {
                    let scope = match rng.random_range(0..4) {
                        0 => None,
                        1 => Some(InstrumentationScope::default()),
                        _ => Some(InstrumentationScope {
                            name: rand_string(rng),
                            version: rand_string(rng),
                            attributes: rand_attrs(rng),
                            dropped_attributes_count: rng.random_range(0..2),
                        }),
                    };
                    ScopeSpans {
                        scope,
                        spans: (0..rng.random_range(0..6))
                            .map(|_| rand_span(rng))
                            .collect(),
                        schema_url: rand_string(rng),
                    }
                })
                .collect();
            ResourceSpans {
                resource,
                scope_spans,
                schema_url: rand_string(rng),
            }
        })
        .collect();
    if big {
        let text = "x\u{e9}".repeat(1 << 19);
        let mut span = rand_span(rng);
        span.attributes
            .push(kv("gen_ai.input.messages", Value::StringValue(text)));
        resource_spans.push(ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    ExportTraceServiceRequest { resource_spans }
}

// ---- hand-built wire bytes ----

pub fn varint(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    nwdb_core::put_varint(&mut out, v);
    out
}

/// An `AnyValue` holding `levels` arrays inside each other, around an int.
pub fn nested(levels: usize) -> Vec<u8> {
    let mut v = vf(3, 1);
    for _ in 0..levels {
        v = ld(5, &ld(1, &v));
    }
    v
}

/// The items, each preceded by its length.
pub fn prefixed(items: &[Vec<u8>]) -> Vec<u8> {
    items
        .iter()
        .flat_map(|m| [varint(m.len() as u64), m.clone()].concat())
        .collect()
}

/// A `KeyValue` message.
pub fn kv_bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
    concat(&[&ld(1, key), &ld(2, value)])
}

fn tag(field: u32, wire: u8) -> Vec<u8> {
    varint(u64::from(field) << 3 | u64::from(wire))
}

pub fn ld(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = tag(field, 2);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}

pub fn vf(field: u32, v: u64) -> Vec<u8> {
    let mut out = tag(field, 0);
    out.extend(varint(v));
    out
}

pub fn f64f(field: u32, v: u64) -> Vec<u8> {
    let mut out = tag(field, 1);
    out.extend(v.to_le_bytes());
    out
}

pub fn f32f(field: u32, v: u32) -> Vec<u8> {
    let mut out = tag(field, 5);
    out.extend(v.to_le_bytes());
    out
}

/// One unknown field of each wire type, to splice into any message.
pub fn unknowns() -> Vec<u8> {
    [vf(90, 12345), f64f(91, 7), ld(92, b"unknown"), f32f(93, 9)].concat()
}

pub fn concat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}
