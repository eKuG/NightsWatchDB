//! Compares the decoder with the reference protobuf implementation on random
//! requests, and checks the properties that follow: round trip, limits,
//! determinism and statistics.

mod common;

use common::*;
use nwdb_core::{BatchLimits, SpanRecord};
use nwdb_otlp::{TraceDecoder, encode_spans};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, span};
use prost::Message;

const CASES: u64 = 120;

fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

/// The promoted columns, independently of the decoder: for each column, its
/// attribute names newest first and whether it is a token count.
const REFERENCE: [(&[&str], bool); 11] = [
    (&["gen_ai.operation.name"], false),
    (&["gen_ai.provider.name", "gen_ai.system"], false),
    (&["gen_ai.request.model"], false),
    (&["gen_ai.response.model"], false),
    (&["gen_ai.conversation.id"], false),
    (&["session.id"], false),
    (&["user.id"], false),
    (&["gen_ai.agent.name"], false),
    (&["gen_ai.tool.name"], false),
    (&["error.type"], false),
    (
        &["gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"],
        true,
    ),
];

fn reference_value(attrs: &[KeyValue], names: &[&str], int: bool) -> Option<String> {
    // The first name whose last occurrence has a usable value.
    names.iter().find_map(|name| {
        let last = attrs.iter().rev().find(|kv| kv.key == *name)?;
        let v = last.value.as_ref().and_then(|v| v.value.as_ref());
        match v {
            Some(Value::StringValue(s)) if !int => Some(s.clone()),
            // A token count below zero is not usable.
            Some(Value::StringValue(s)) => s
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .map(|n| n.to_string()),
            Some(Value::IntValue(n)) if int && *n >= 0 => Some(n.to_string()),
            _ => None,
        }
    })
}

fn check_raw<M: Message + Default + PartialEq + std::fmt::Debug>(col: Option<&[u8]>, want: &[M]) {
    let got: Vec<M> = split_prefixed(col.unwrap_or(&[]))
        .into_iter()
        .map(|b| M::decode(b).expect("raw element decodes"))
        .collect();
    assert_eq!(got, want);
    if want.is_empty() {
        assert!(col.is_none_or(<[u8]>::is_empty));
    }
}

fn check_row(
    r: &SpanRecord<'_>,
    resource: &Option<Resource>,
    scope: &Option<InstrumentationScope>,
    schema_urls: (&str, &str),
    s: &Span,
) {
    assert_eq!(r.trace_id.0.as_slice(), s.trace_id.as_slice());
    assert_eq!(r.span_id.0.as_slice(), s.span_id.as_slice());
    let parent_is_root = s.parent_span_id.iter().all(|b| *b == 0);
    assert_eq!(r.parent_span_id.is_none(), parent_is_root);
    if let Some(p) = r.parent_span_id {
        assert_eq!(p.0.as_slice(), s.parent_span_id.as_slice());
    }
    assert_eq!(r.name, s.name);
    assert_eq!(r.kind, s.kind);
    assert_eq!(r.start_ns, s.start_time_unix_nano as i64);
    assert_eq!(r.end_ns, s.end_time_unix_nano as i64);
    let (code, message) = s
        .status
        .as_ref()
        .map_or((0, ""), |st| (st.code, st.message.as_str()));
    assert_eq!(r.status_code, code);
    assert_eq!(r.status_message, non_empty(message));
    assert_eq!(r.trace_state, non_empty(&s.trace_state));
    assert_eq!(r.flags, s.flags);
    assert_eq!(r.dropped_attributes_count, s.dropped_attributes_count);
    assert_eq!(r.dropped_events_count, s.dropped_events_count);
    assert_eq!(r.dropped_links_count, s.dropped_links_count);

    let service = resource.as_ref().and_then(|res| {
        res.attributes
            .iter()
            .rev()
            .find(|kv| kv.key == "service.name")
            .and_then(
                |kv| match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                },
            )
    });
    assert_eq!(r.service_name, service);
    let scope = scope.as_ref();
    assert_eq!(r.scope_name, scope.and_then(|sc| non_empty(&sc.name)));
    assert_eq!(r.scope_version, scope.and_then(|sc| non_empty(&sc.version)));
    assert_eq!(r.resource_schema_url, non_empty(schema_urls.0));
    assert_eq!(r.scope_schema_url, non_empty(schema_urls.1));
    match scope {
        Some(sc) => assert_eq!(r.scope, Some(sc.encode_to_vec().as_slice())),
        None => assert!(r.scope.is_none()),
    }

    let got = [
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
    for (i, (names, int)) in REFERENCE[..10].iter().enumerate() {
        let want = reference_value(&s.attributes, names, *int);
        assert_eq!(got[i].map(str::to_string), want, "column {names:?}");
    }
    let (names, int) = REFERENCE[10];
    assert_eq!(
        r.input_tokens.map(|n| n.to_string()),
        reference_value(&s.attributes, names, int)
    );
    assert_eq!(
        r.output_tokens.map(|n| n.to_string()),
        reference_value(
            &s.attributes,
            &[
                "gen_ai.usage.output_tokens",
                "gen_ai.usage.completion_tokens"
            ],
            true
        )
    );

    match resource {
        Some(res) => assert_eq!(r.resource, Some(res.encode_to_vec().as_slice())),
        None => assert!(r.resource.is_none()),
    }
    check_raw(Some(r.attrs), &s.attributes);
    check_raw::<span::Event>(r.events, &s.events);
    check_raw::<span::Link>(r.links, &s.links);
}

type Expected<'a> = (
    &'a Option<Resource>,
    &'a Option<InstrumentationScope>,
    (&'a str, &'a str),
    &'a Span,
);

fn expected_spans(req: &ExportTraceServiceRequest) -> Vec<Expected<'_>> {
    let mut out = Vec::new();
    for rs in &req.resource_spans {
        for ss in &rs.scope_spans {
            for s in &ss.spans {
                let urls = (rs.schema_url.as_str(), ss.schema_url.as_str());
                out.push((&rs.resource, &ss.scope, urls, s));
            }
        }
    }
    out
}

fn requests() -> impl Iterator<Item = ExportTraceServiceRequest> {
    (0..CASES).map(|seed| rand_request(&mut rng(seed), seed == 3))
}

#[test]
fn every_column_matches_the_reference_decoder() {
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let mut spans = 0;
    for req in requests() {
        let bytes = req.encode_to_vec();
        let batches = decoder.decode(&bytes).unwrap();
        let rows = all_rows(&batches);
        let want = expected_spans(&req);
        assert_eq!(rows.len(), want.len());
        for (row, (res, scope, urls, s)) in rows.iter().zip(want) {
            check_row(row, res, scope, urls, s);
        }
        spans += rows.len();
    }
    assert!(spans > 500, "generator produced only {spans} spans");
}

#[test]
fn encode_round_trip_gives_equal_rows() {
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    for req in requests() {
        let first = decoder.decode(&req.encode_to_vec()).unwrap();
        let encoded = encode_spans(&first).unwrap();
        // The output is a valid request for an independent decoder.
        ExportTraceServiceRequest::decode(encoded.as_slice()).expect("prost decodes the output");
        let second = decoder.decode(&encoded).unwrap();
        assert_eq!(all_rows(&first), all_rows(&second));
    }
}

#[test]
fn encode_groups_consecutive_rows() {
    let mut r = rng(77);
    let res = |name: &str| Resource {
        attributes: vec![kv("service.name", str_val(name))],
        ..Default::default()
    };
    let scope = |name: &str| {
        Some(InstrumentationScope {
            name: name.into(),
            ..Default::default()
        })
    };
    let mut group = |resource: Resource, scopes: &[&str]| {
        opentelemetry_proto::tonic::trace::v1::ResourceSpans {
            resource: Some(resource),
            scope_spans: scopes
                .iter()
                .map(|n| opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: scope(n),
                    spans: vec![rand_span(&mut r), rand_span(&mut r)],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    };
    // Two resource-spans with the same resource come back as one: the table
    // keeps no boundary between them.
    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            group(res("a"), &["s1", "s2"]),
            group(res("a"), &["s2"]),
            group(res("b"), &["s1"]),
        ],
    };
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let batches = decoder.decode(&req.encode_to_vec()).unwrap();
    let out =
        ExportTraceServiceRequest::decode(encode_spans(&batches).unwrap().as_slice()).unwrap();
    let shape: Vec<Vec<usize>> = out
        .resource_spans
        .iter()
        .map(|rs| rs.scope_spans.iter().map(|ss| ss.spans.len()).collect())
        .collect();
    assert_eq!(shape, vec![vec![2, 4], vec![2]]);
}

type Shape = Vec<(
    Option<Resource>,
    String,
    Vec<(Option<InstrumentationScope>, String, usize)>,
)>;

/// Resources, scopes and schema URLs of a request, with the span counts.
fn shape(req: &ExportTraceServiceRequest) -> Shape {
    req.resource_spans
        .iter()
        .map(|rs| {
            let scopes = rs
                .scope_spans
                .iter()
                .map(|ss| (ss.scope.clone(), ss.schema_url.clone(), ss.spans.len()))
                .collect();
            (rs.resource.clone(), rs.schema_url.clone(), scopes)
        })
        .collect()
}

#[test]
fn scope_attributes_and_schema_urls_survive_the_round_trip() {
    let mut r = rng(5);
    let lib = |attr: &str| InstrumentationScope {
        name: "lib".into(),
        version: "1".into(),
        attributes: vec![kv("k", str_val(attr))],
        dropped_attributes_count: 3,
    };
    let res = Resource {
        attributes: vec![kv("service.name", str_val("svc"))],
        ..Default::default()
    };
    let mut scope_spans = |scope: Option<InstrumentationScope>, url: &str| ScopeSpans {
        scope,
        spans: vec![rand_span(&mut r)],
        schema_url: url.into(),
    };
    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(res.clone()),
                schema_url: "https://example.test/r/1".into(),
                scope_spans: vec![
                    scope_spans(Some(lib("a")), "https://example.test/s/1"),
                    // Differs from the one before only in an attribute value.
                    scope_spans(Some(lib("b")), "https://example.test/s/1"),
                    // Same scope as the one before, a different schema URL.
                    scope_spans(Some(lib("b")), "https://example.test/s/2"),
                    // Only a name.
                    scope_spans(
                        Some(InstrumentationScope {
                            name: "only".into(),
                            ..Default::default()
                        }),
                        "",
                    ),
                    // An empty scope, then none at all.
                    scope_spans(Some(InstrumentationScope::default()), ""),
                    scope_spans(None, ""),
                    scope_spans(None, "https://example.test/s/1"),
                ],
            },
            // The same resource bytes, a different schema URL.
            ResourceSpans {
                resource: Some(res.clone()),
                schema_url: "https://example.test/r/2".into(),
                scope_spans: vec![scope_spans(None, "")],
            },
            ResourceSpans {
                resource: Some(res),
                schema_url: String::new(),
                scope_spans: vec![scope_spans(None, "")],
            },
        ],
    };
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let first = decoder.decode(&req.encode_to_vec()).unwrap();
    let encoded = encode_spans(&first).unwrap();
    let out = ExportTraceServiceRequest::decode(encoded.as_slice()).unwrap();
    // Nothing is merged and nothing is lost: every boundary above differs in
    // the resource, the scope bytes or a schema URL.
    assert_eq!(shape(&out), shape(&req));
    let second = decoder.decode(&encoded).unwrap();
    assert_eq!(all_rows(&first), all_rows(&second));

    let rows = all_rows(&second);
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0].scope_schema_url, Some("https://example.test/s/1"));
    assert_eq!(
        rows[0].resource_schema_url,
        Some("https://example.test/r/1")
    );
    let scope = |i: usize| InstrumentationScope::decode(rows[i].scope.unwrap()).unwrap();
    assert_eq!(scope(0), lib("a"));
    assert_eq!(scope(1), lib("b"));
    assert_eq!(scope(3).name, "only");
    assert_eq!(
        (rows[3].scope_name, rows[3].scope_version),
        (Some("only"), None)
    );
    assert_eq!(rows[4].scope, Some(&[][..]));
    assert!(rows[5].scope.is_none() && rows[5].scope_name.is_none());
    assert!(rows[5].scope_schema_url.is_none());
    assert_eq!(
        rows[7].resource_schema_url,
        Some("https://example.test/r/2")
    );
    assert!(rows[8].resource_schema_url.is_none());
}

#[test]
fn rows_without_scope_bytes_get_a_scope_from_name_and_version() {
    use nwdb_core::{SpanBatchBuilder, SpanRecord};
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    b.append(&SpanRecord {
        scope_name: Some("lib"),
        scope_version: Some("2"),
        ..SpanRecord::default()
    });
    let out =
        ExportTraceServiceRequest::decode(encode_spans(&[b.finish()]).unwrap().as_slice()).unwrap();
    let scope = out.resource_spans[0].scope_spans[0].scope.clone().unwrap();
    assert_eq!((scope.name.as_str(), scope.version.as_str()), ("lib", "2"));
}

#[test]
fn decoding_is_deterministic() {
    let decoder = TraceDecoder::new(BatchLimits {
        max_rows: 7,
        max_bytes: 4000,
    });
    for req in requests().take(20) {
        let bytes = req.encode_to_vec();
        assert_eq!(
            decoder.decode(&bytes).unwrap(),
            decoder.decode(&bytes).unwrap()
        );
    }
}

#[test]
fn stats_count_what_was_copied() {
    let decoder = TraceDecoder::new(BatchLimits {
        max_rows: 5,
        max_bytes: 10_000,
    });
    for req in requests().take(30) {
        let (batches, stats) = decoder.decode_with_stats(&req.encode_to_vec()).unwrap();
        let rows = all_rows(&batches);
        assert_eq!(stats.spans as usize, rows.len());
        assert_eq!(
            stats.bytes_copied as usize,
            batches.iter().map(value_bytes).sum::<usize>()
        );
        assert_eq!((stats.rejected_spans, stats.rejected_reason), (0, None));
        assert_eq!(stats.resources as usize, req.resource_spans.len());
        let scopes: usize = req.resource_spans.iter().map(|r| r.scope_spans.len()).sum();
        assert_eq!(stats.scopes as usize, scopes);
    }
}

fn simple_request(n: usize, attr_len: usize) -> Vec<u8> {
    let mut r = rng(5);
    let spans = (0..n)
        .map(|i| {
            let mut s = rand_span(&mut r);
            s.name = format!("span-{i}");
            s.trace_state.clear();
            s.status = None;
            s.events.clear();
            s.links.clear();
            s.attributes = vec![kv("blob", Value::StringValue("z".repeat(attr_len)))];
            s
        })
        .collect();
    let rs = opentelemetry_proto::tonic::trace::v1::ResourceSpans {
        scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
            spans,
            ..Default::default()
        }],
        ..Default::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![rs],
    }
    .encode_to_vec()
}

fn names(batches: &[arrow_array::RecordBatch]) -> Vec<String> {
    all_rows(batches)
        .iter()
        .map(|r| r.name.to_string())
        .collect()
}

#[test]
fn row_bound_splits_batches_in_order() {
    let bytes = simple_request(100, 10);
    let all = TraceDecoder::new(BatchLimits::DEFAULT)
        .decode(&bytes)
        .unwrap();
    assert_eq!(all.len(), 1);
    let limits = BatchLimits {
        max_rows: 32,
        max_bytes: usize::MAX,
    };
    let split = TraceDecoder::new(limits).decode(&bytes).unwrap();
    let sizes: Vec<usize> = split.iter().map(|b| b.num_rows()).collect();
    assert_eq!(sizes, vec![32, 32, 32, 4]);
    assert_eq!(names(&split), names(&all));
    assert_eq!(all_rows(&split), all_rows(&all));
}

#[test]
fn byte_bound_splits_batches_in_order() {
    let bytes = simple_request(100, 1000);
    let all = TraceDecoder::new(BatchLimits::DEFAULT)
        .decode(&bytes)
        .unwrap();
    let max_bytes = 10_000;
    let split = TraceDecoder::new(BatchLimits {
        max_rows: usize::MAX,
        max_bytes,
    })
    .decode(&bytes)
    .unwrap();
    assert!(split.len() > 5);
    assert_eq!(all_rows(&split), all_rows(&all));
    // Every batch but the last closed on the span that reached the bound.
    for b in &split[..split.len() - 1] {
        let total = value_bytes(b);
        let before_last = value_bytes(&b.slice(0, b.num_rows() - 1));
        assert!(total >= max_bytes && before_last < max_bytes);
    }
}

#[test]
fn a_span_over_the_byte_bound_is_alone() {
    let bytes = simple_request(3, 5000);
    let split = TraceDecoder::new(BatchLimits {
        max_rows: 100,
        max_bytes: 100,
    })
    .decode(&bytes)
    .unwrap();
    assert_eq!(
        split.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}

#[test]
fn the_large_attribute_is_kept_whole() {
    let req = rand_request(&mut rng(3), true);
    let batches = TraceDecoder::new(BatchLimits::DEFAULT)
        .decode(&req.encode_to_vec())
        .unwrap();
    let last = *all_rows(&batches).last().unwrap();
    let kvs = split_prefixed(last.attrs);
    let kv = KeyValue::decode(*kvs.last().unwrap()).unwrap();
    let AnyValue {
        value: Some(Value::StringValue(s)),
    } = kv.value.unwrap()
    else {
        panic!("not a string");
    };
    assert_eq!(s.len(), 3 << 19);
}
