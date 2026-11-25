//! Regression tests for the decoder: what a request costs once decoded, spans
//! that are skipped instead of refusing the request, and a documented
//! deviation from protobuf.

mod common;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use common::*;
use nwdb_core::{BatchLimits, SpanColumns};
use nwdb_otlp::{DEFAULT_MAX_ORIGIN_BYTES, DecodeStats, TraceDecoder};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message;
use rand::Rng;

fn decoder() -> TraceDecoder {
    TraceDecoder::new(BatchLimits::DEFAULT)
}

// ---- wire builders ----

/// A span with only its two ids, 28 bytes.
fn bare_span(trace: &[u8], span: &[u8]) -> Vec<u8> {
    concat(&[&ld(1, trace), &ld(2, span)])
}

/// A valid span whose span id is made from `n`.
fn nth_span(n: u32) -> Vec<u8> {
    bare_span(&[1; 16], &[n.to_be_bytes(), [9; 4]].concat())
}

fn named_span(n: u32, name: &str) -> Vec<u8> {
    concat(&[&nth_span(n), &ld(5, name.as_bytes())])
}

/// `ScopeSpans` holding the spans, with an optional scope and schema URL.
fn scope_spans(scope: Option<&[u8]>, schema_url: &str, spans: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(s) = scope {
        out.extend(ld(1, s));
    }
    for s in spans {
        out.extend(ld(2, s));
    }
    if !schema_url.is_empty() {
        out.extend(ld(3, schema_url.as_bytes()));
    }
    out
}

/// `ResourceSpans` holding the scope spans.
fn resource_spans(resource: Option<&[u8]>, schema_url: &str, scopes: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(r) = resource {
        out.extend(ld(1, r));
    }
    for s in scopes {
        out.extend(ld(2, s));
    }
    if !schema_url.is_empty() {
        out.extend(ld(3, schema_url.as_bytes()));
    }
    out
}

fn request(resources: &[Vec<u8>]) -> Vec<u8> {
    resources.iter().flat_map(|r| ld(1, r)).collect()
}

/// A `Resource` with a `service.name` attribute padded to `extra` bytes by a
/// second attribute.
fn resource_of(service: &str, extra: usize) -> Vec<u8> {
    let attr = |k: &str, v: &str| {
        ld(
            1,
            &concat(&[&ld(1, k.as_bytes()), &ld(2, &ld(1, v.as_bytes()))]),
        )
    };
    let mut r = attr("service.name", service);
    if extra > 0 {
        r.extend(attr("pad", &"p".repeat(extra)));
    }
    r
}

fn scope_of(name: &str) -> Vec<u8> {
    ld(1, name.as_bytes())
}

// ---- what a request costs ----

fn memory(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.get_array_memory_size()).sum()
}

/// Memory a batch may hold beyond what its rows need: the pre-sized buffers
/// of an idle builder and the arrays' own structures (the builder's own test
/// pins the idle size below 64 KiB).
const PER_BATCH: usize = 64 * 1024;

/// The most the decoded batches may weigh, as a multiple of the request.
const FACTOR: usize = 8;

/// What minimal 30-byte spans can reach: one row of the table costs about 200
/// bytes whatever the span holds, and Arrow's buffers grow by doubling, so a
/// batch with few more rows than a power of two holds nearly twice that.
const MINIMAL_SPAN_FACTOR: f64 = 11.0;

/// Decodes `req` and checks the bound. Returns the measured factor and the
/// stats.
fn check_bound(name: &str, req: &[u8]) -> (f64, DecodeStats) {
    let (batches, stats) = decoder().decode_with_stats(req).unwrap();
    let used = memory(&batches);
    let allowed = FACTOR * req.len() + PER_BATCH * batches.len();
    let factor = used as f64 / req.len() as f64;
    eprintln!(
        "{name}: request {} bytes, {} rows in {} batches, {used} bytes in memory ({factor:.2}x)",
        req.len(),
        stats.spans,
        batches.len()
    );
    assert!(
        used <= allowed,
        "{name}: a request of {} bytes decoded into {used} bytes of memory ({factor:.2}x), \
         allowed {allowed}",
        req.len()
    );
    (factor, stats)
}

/// The decoded size of a request is bounded by a small multiple of its size,
/// whatever its shape. The server's in-flight budget counts request bytes and
/// the decoder runs before the engine's buffer limit is checked, so this is
/// what keeps one request from asking for gigabytes.
#[test]
fn decoded_memory_is_bounded_by_the_request_size() {
    // One large resource and many spans that carry only their ids.
    let spans: Vec<Vec<u8>> = (0..5_000).map(nth_span).collect();
    let big = resource_of("svc", 60_000);
    let req = request(&[resource_spans(
        Some(&big),
        "",
        &[scope_spans(None, "", &spans)],
    )]);
    let (_, stats) = check_bound("one huge resource, many minimal spans", &req);
    assert_eq!(stats.spans, 5_000);

    // A 4 KiB service name and 1,000 minimal spans.
    let spans: Vec<Vec<u8>> = (0..1_000).map(nth_span).collect();
    let req = request(&[resource_spans(
        Some(&resource_of(&"s".repeat(4096), 0)),
        "",
        &[scope_spans(None, "", &spans)],
    )]);
    check_bound("4 KiB service name", &req);

    // Many tiny ResourceSpans, each with its own resource.
    let resources: Vec<Vec<u8>> = (0..3_000)
        .map(|i| {
            resource_spans(
                Some(&resource_of(&format!("svc-{i}"), 0)),
                "",
                &[scope_spans(None, "", &[nth_span(i)])],
            )
        })
        .collect();
    check_bound("many tiny ResourceSpans", &request(&resources));

    // Many tiny ScopeSpans under one large resource, each with its own scope.
    let scopes: Vec<Vec<u8>> = (0..3_000)
        .map(|i| {
            scope_spans(
                Some(&scope_of(&format!("scope-{i}"))),
                "https://example.test/schema",
                &[nth_span(i)],
            )
        })
        .collect();
    let req = request(&[resource_spans(Some(&big), "", &scopes)]);
    check_bound("many tiny ScopeSpans under one resource", &req);

    // Only minimal spans, no origin at all.
    let spans: Vec<Vec<u8>> = (0..10_000).map(nth_span).collect();
    let req = request(&[resource_spans(None, "", &[scope_spans(None, "", &spans)])]);
    let (_, stats) = check_bound("minimal spans", &req);
    assert_eq!(stats.spans, 10_000);

    // Small ordinary spans (a name and two timestamps, about 60 bytes), in a
    // batch with a row count just past a power of two, where Arrow's buffers
    // have the most spare room.
    let spans: Vec<Vec<u8>> = (0..4_097u32)
        .map(|i| {
            concat(&[
                &named_span(i, &format!("span-{i}")),
                &f64f(7, 1_000 + u64::from(i)),
                &f64f(8, 2_000 + u64::from(i)),
            ])
        })
        .collect();
    let req = request(&[resource_spans(None, "", &[scope_spans(None, "", &spans)])]);
    check_bound("small ordinary spans", &req);

    // Tiny batches: the pre-sized buffers are charged per batch, not per row.
    let small = TraceDecoder::new(BatchLimits {
        max_rows: 100,
        max_bytes: usize::MAX,
    });
    let (batches, _) = small.decode_with_stats(&req).unwrap();
    assert!(memory(&batches) <= FACTOR * req.len() + PER_BATCH * batches.len());
}

/// Measures the worst case for minimal spans: a batch whose row count is just
/// past a power of two has buffers grown to nearly twice what they hold.
#[test]
fn minimal_spans_worst_case_factor() {
    let mut worst = 0f64;
    for n in [1u32, 2, 65, 129, 257, 513, 1025, 2049, 4097, 8192, 8193] {
        let spans: Vec<Vec<u8>> = (0..n).map(nth_span).collect();
        let req = request(&[resource_spans(None, "", &[scope_spans(None, "", &spans)])]);
        let batches = decoder().decode(&req).unwrap();
        // What the rows hold beyond the fixed part of each batch.
        let rows_part = memory(&batches).saturating_sub(PER_BATCH * batches.len());
        let factor = rows_part as f64 / req.len() as f64;
        eprintln!("{n} minimal spans: {} bytes, {factor:.2}x", req.len());
        worst = worst.max(factor);
    }
    eprintln!("worst factor for minimal spans: {worst:.2}x");
    assert!(worst <= MINIMAL_SPAN_FACTOR, "{worst}");
}

/// A random request is bounded too, with a large shared resource.
#[test]
fn random_requests_are_bounded() {
    for seed in 0..30 {
        let mut r = rng(seed);
        let mut req = rand_request(&mut r, false);
        for rs in &mut req.resource_spans {
            if r.random_bool(0.5) {
                rs.resource = Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: vec![kv("blob", str_val(&"b".repeat(r.random_range(0..20_000))))],
                    ..Default::default()
                });
            }
        }
        let bytes = req.encode_to_vec();
        if bytes.is_empty() {
            continue;
        }
        check_bound(&format!("random request {seed}"), &bytes);
    }
}

/// A value over the limit is never stored: the request costs about what it
/// weighs, not what it asks for.
#[test]
fn an_oversized_resource_is_not_stored() {
    let spans: Vec<Vec<u8>> = (0..1_000).map(nth_span).collect();
    let huge = resource_of("svc", 2 * DEFAULT_MAX_ORIGIN_BYTES);
    let req = request(&[resource_spans(
        Some(&huge),
        "",
        &[scope_spans(None, "", &spans)],
    )]);
    let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
    assert!(batches.is_empty());
    assert_eq!((stats.spans, stats.rejected_spans), (0, 1_000));
    let why = stats.rejected_reason.unwrap();
    assert!(why.contains("span 1 ") && why.contains("resource"), "{why}");
}

// ---- the limit on one origin value ----

fn rows_of(req: &[u8], d: TraceDecoder) -> (Vec<RecordBatch>, DecodeStats) {
    d.decode_with_stats(req).unwrap()
}

/// Each of the four origin values over the limit rejects the spans of its
/// own message and none of the others.
#[test]
fn only_the_spans_under_an_oversized_value_are_rejected() {
    let limit = 200;
    let d = decoder().with_max_origin_bytes(limit);
    let small_resource = resource_of("svc", 0);
    let big_resource = resource_of("svc", limit);
    let big_text = "u".repeat(limit + 1);
    let big_scope = scope_of(&"n".repeat(limit + 1));
    assert!(small_resource.len() <= limit && big_resource.len() > limit);

    let good = |n: u32| scope_spans(Some(&scope_of("ok")), "", &[named_span(n, "good")]);
    let cases: [(&str, Vec<u8>); 4] = [
        (
            "resource",
            resource_spans(Some(&big_resource), "", &[good(1), good(2)]),
        ),
        (
            "resource schema url",
            resource_spans(Some(&small_resource), &big_text, &[good(1), good(2)]),
        ),
        (
            "scope",
            resource_spans(
                Some(&small_resource),
                "",
                &[
                    scope_spans(Some(&big_scope), "", &[named_span(1, "bad")]),
                    good(2),
                ],
            ),
        ),
        (
            "scope schema url",
            resource_spans(
                Some(&small_resource),
                "",
                &[
                    scope_spans(None, &big_text, &[named_span(1, "bad")]),
                    good(2),
                ],
            ),
        ),
    ];
    for (what, bad) in cases {
        let fine = resource_spans(Some(&small_resource), "", &[good(3)]);
        let req = request(&[bad, fine]);
        let (batches, stats) = rows_of(&req, d);
        let rows = all_rows(&batches);
        let names: Vec<&str> = rows.iter().map(|r| r.name).collect();
        assert!(
            names.iter().all(|n| *n == "good"),
            "{what}: a rejected span was stored: {names:?}"
        );
        assert!(stats.rejected_spans >= 1, "{what}");
        assert_eq!(stats.spans as usize, rows.len(), "{what}");
        assert_eq!(
            rows.last().map(|r| r.service_name),
            Some(Some("svc")),
            "{what}: the message after the bad one was lost"
        );
        assert!(stats.rejected_reason.is_some(), "{what}");
        assert_eq!(
            decoder().with_max_origin_bytes(limit).decode(&req).unwrap(),
            batches
        );
    }
}

#[test]
fn a_value_exactly_at_the_limit_is_kept() {
    let r = resource_of("svc", 50);
    let req = request(&[resource_spans(
        Some(&r),
        "",
        &[scope_spans(None, "", &[nth_span(1)])],
    )]);
    let (b, stats) = rows_of(&req, decoder().with_max_origin_bytes(r.len()));
    assert_eq!((stats.spans, stats.rejected_spans), (1, 0));
    assert_eq!(all_rows(&b)[0].resource, Some(&r[..]));
    let (_, stats) = rows_of(&req, decoder().with_max_origin_bytes(r.len() - 1));
    assert_eq!((stats.spans, stats.rejected_spans), (0, 1));
}

#[test]
fn the_default_limit_is_64_kib() {
    assert_eq!(DEFAULT_MAX_ORIGIN_BYTES, 64 * 1024);
}

/// A batch that fills in the middle of a message starts the next one with
/// its own dictionary, holding the same origin.
#[test]
fn a_batch_closed_mid_message_carries_its_own_dictionary() {
    let d = TraceDecoder::new(BatchLimits {
        max_rows: 4,
        max_bytes: usize::MAX,
    });
    let res = resource_of("svc", 100);
    let spans: Vec<Vec<u8>> = (0..10).map(nth_span).collect();
    let req = request(&[resource_spans(
        Some(&res),
        "https://example.test/r",
        &[scope_spans(
            Some(&scope_of("lib")),
            "https://example.test/s",
            &spans,
        )],
    )]);
    let (batches, stats) = d.decode_with_stats(&req).unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(stats.spans, 10);
    for b in &batches {
        for col in ["resource", "service_name", "scope", "scope_name"] {
            let c = b.column_by_name(col).unwrap();
            assert_eq!(c.as_any_dictionary().values().len(), 1, "{col}");
        }
    }
    for r in all_rows(&batches) {
        assert_eq!(r.resource, Some(&res[..]));
        assert_eq!(r.service_name, Some("svc"));
        assert_eq!(r.scope_name, Some("lib"));
        assert_eq!(r.resource_schema_url, Some("https://example.test/r"));
        assert_eq!(r.scope_schema_url, Some("https://example.test/s"));
    }
}

/// A message that holds no valid span stores none of its origin.
#[test]
fn origin_values_of_a_message_without_stored_spans_cost_nothing() {
    let bad = bare_span(&[0; 16], &[2; 8]);
    let req = request(&[resource_spans(
        Some(&resource_of("svc", 10_000)),
        "",
        &[scope_spans(Some(&scope_of("s")), "", &[bad])],
    )]);
    let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
    assert!(batches.is_empty());
    assert_eq!(
        (stats.spans, stats.rejected_spans, stats.bytes_copied),
        (0, 1, 0)
    );
}

// ---- spans with invalid ids ----

fn zero_trace() -> Vec<u8> {
    vec![0; 16]
}

/// Requests holding one invalid span followed by one valid one, with the
/// text the reason must contain.
fn invalid_id_cases() -> Vec<(&'static str, Vec<u8>, &'static str)> {
    let t = [1u8; 16];
    let s = [2u8; 8];
    vec![
        (
            "all-zero trace id",
            bare_span(&zero_trace(), &s),
            "trace_id is all zero",
        ),
        (
            "15-byte trace id",
            bare_span(&[1; 15], &s),
            "trace_id is not 16 bytes",
        ),
        (
            "17-byte trace id",
            bare_span(&[1; 17], &s),
            "trace_id is not 16 bytes",
        ),
        (
            "empty trace id",
            bare_span(b"", &s),
            "trace_id is not 16 bytes",
        ),
        (
            "all-zero span id",
            bare_span(&t, &[0; 8]),
            "span_id is all zero",
        ),
        (
            "7-byte span id",
            bare_span(&t, &[2; 7]),
            "span_id is not 8 bytes",
        ),
        (
            "9-byte span id",
            bare_span(&t, &[2; 9]),
            "span_id is not 8 bytes",
        ),
        (
            "empty span id",
            bare_span(&t, b""),
            "span_id is not 8 bytes",
        ),
        ("no ids at all", Vec::new(), "is not"),
    ]
}

#[test]
fn an_invalid_id_skips_that_span_and_keeps_the_rest() {
    for (what, bad, reason) in invalid_id_cases() {
        let req = request(&[resource_spans(
            Some(&resource_of("svc", 0)),
            "",
            &[scope_spans(None, "", &[bad, named_span(1, "kept")])],
        )]);
        let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
        let rows = all_rows(&batches);
        assert_eq!(rows.len(), 1, "{what}");
        assert_eq!(rows[0].name, "kept", "{what}");
        assert_eq!(rows[0].service_name, Some("svc"), "{what}");
        assert_eq!((stats.spans, stats.rejected_spans), (1, 1), "{what}");
        let why = stats.rejected_reason.unwrap();
        assert!(why.starts_with("span 1 of the request: "), "{what}: {why}");
        assert!(why.contains(reason), "{what}: {why}");
    }
}

#[test]
fn valid_and_invalid_spans_mixed_across_messages() {
    let ok = |n: u32| named_span(n, &format!("ok-{n}"));
    let zero = bare_span(&zero_trace(), &[3; 8]);
    let short = bare_span(&[1; 16], &[3; 4]);
    let req = request(&[
        resource_spans(
            Some(&resource_of("a", 0)),
            "",
            &[
                scope_spans(None, "", &[ok(1), zero.clone(), ok(2)]),
                scope_spans(Some(&scope_of("s")), "", &[short.clone(), ok(3)]),
            ],
        ),
        resource_spans(
            Some(&resource_of("b", 0)),
            "",
            &[scope_spans(None, "", &[zero.clone(), zero.clone(), ok(4)])],
        ),
    ]);
    let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
    let rows = all_rows(&batches);
    let got: Vec<(&str, Option<&str>)> = rows.iter().map(|r| (r.name, r.service_name)).collect();
    assert_eq!(
        got,
        [
            ("ok-1", Some("a")),
            ("ok-2", Some("a")),
            ("ok-3", Some("a")),
            ("ok-4", Some("b"))
        ]
    );
    assert_eq!((stats.spans, stats.rejected_spans), (4, 4));
    assert_eq!((stats.resources, stats.scopes), (2, 3));
    // The reason is about the first rejection: the second span.
    let why = stats.rejected_reason.unwrap();
    assert!(why.starts_with("span 2 of the request: "), "{why}");
    assert!(why.contains("trace_id is all zero"), "{why}");
    assert_eq!(rows.iter().filter(|r| r.scope_name == Some("s")).count(), 1);
}

/// The reason for the incident this fixes: spans with the same zero span id
/// used to be stored, and then taken for duplicates of each other.
#[test]
fn spans_with_the_same_zero_id_are_not_stored() {
    let zero = bare_span(&[1; 16], &[0; 8]);
    let req = request(&[resource_spans(
        None,
        "",
        &[scope_spans(None, "", &[zero.clone(), zero.clone(), zero])],
    )]);
    let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
    assert!(batches.is_empty());
    assert_eq!(stats.rejected_spans, 3);
}

#[test]
fn a_request_of_only_invalid_spans_is_not_an_error() {
    let bad: Vec<Vec<u8>> = invalid_id_cases().into_iter().map(|(_, b, _)| b).collect();
    let n = bad.len() as u64;
    let req = request(&[resource_spans(None, "", &[scope_spans(None, "", &bad)])]);
    let (batches, stats) = decoder().decode_with_stats(&req).unwrap();
    assert!(batches.is_empty());
    assert_eq!(
        (stats.spans, stats.rejected_spans, stats.bytes_copied),
        (0, n, 0)
    );
    assert!(stats.rejected_reason.is_some());
    assert!(decoder().decode(&req).unwrap().is_empty());
}

#[test]
fn an_accepted_request_reports_no_rejection() {
    let req = request(&[resource_spans(
        None,
        "",
        &[scope_spans(None, "", &[nth_span(1)])],
    )]);
    let (_, stats) = decoder().decode_with_stats(&req).unwrap();
    assert_eq!((stats.rejected_spans, stats.rejected_reason), (0, None));
}

/// Damage to the structure of the request is still an error for all of it,
/// even when the other spans are fine.
#[test]
fn a_damaged_span_still_fails_the_whole_request() {
    let bad_parent = concat(&[&nth_span(2), &ld(4, &[1; 9])]);
    let req = request(&[resource_spans(
        None,
        "",
        &[scope_spans(None, "", &[nth_span(1), bad_parent])],
    )]);
    assert!(decoder().decode(&req).is_err());
    let mut truncated = nth_span(1);
    truncated.push(0x0a);
    let req = request(&[resource_spans(
        None,
        "",
        &[scope_spans(None, "", &[nth_span(3), truncated])],
    )]);
    assert!(decoder().decode_with_stats(&req).is_err());
}

/// `decode`, which replays the log, skips what `decode_with_stats` rejects,
/// and gives the same batches.
#[test]
fn decode_and_decode_with_stats_agree() {
    let d = TraceDecoder::new(BatchLimits {
        max_rows: 5,
        max_bytes: 4_000,
    })
    .with_max_origin_bytes(300);
    for seed in 0..60 {
        let mut r = rng(seed);
        let mut req = rand_request(&mut r, false);
        let mut total = 0u64;
        for rs in &mut req.resource_spans {
            if r.random_bool(0.2) {
                rs.resource = Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: vec![kv("blob", str_val(&"b".repeat(400)))],
                    ..Default::default()
                });
            }
            for ss in &mut rs.scope_spans {
                for sp in &mut ss.spans {
                    total += 1;
                    match r.random_range(0..8) {
                        0 => sp.trace_id = vec![0; 16],
                        1 => sp.span_id = vec![0; 8],
                        2 => sp.trace_id.truncate(5),
                        3 => sp.span_id.clear(),
                        _ => {}
                    }
                }
            }
        }
        let bytes = req.encode_to_vec();
        let (with_stats, stats) = d.decode_with_stats(&bytes).unwrap();
        assert_eq!(d.decode(&bytes).unwrap(), with_stats, "seed {seed}");
        assert_eq!(stats.spans + stats.rejected_spans, total, "seed {seed}");
        assert_eq!(
            all_rows(&with_stats).len() as u64,
            stats.spans,
            "seed {seed}"
        );
        assert_eq!(stats.rejected_reason.is_some(), stats.rejected_spans > 0);
        for r in all_rows(&with_stats) {
            assert!(!r.trace_id.is_zero() && !r.span_id.is_zero(), "seed {seed}");
        }
    }
}

/// Every prefix of a request that holds invalid spans still decodes or is
/// corrupt, and when it decodes `decode` and `decode_with_stats` agree.
#[test]
fn truncations_of_a_request_with_rejections_stay_deterministic() {
    let req = request(&[resource_spans(
        Some(&resource_of("svc", 20)),
        "u",
        &[scope_spans(
            Some(&scope_of("s")),
            "v",
            &[
                bare_span(&zero_trace(), &[1; 8]),
                nth_span(1),
                bare_span(&[1; 16], &[0; 8]),
                nth_span(2),
            ],
        )],
    )]);
    for n in 0..=req.len() {
        let a = decoder().decode(&req[..n]);
        let b = decoder().decode_with_stats(&req[..n]);
        match (a, b) {
            (Ok(x), Ok((y, _))) => assert_eq!(x, y, "prefix {n}"),
            (Err(_), Err(_)) => {}
            (a, b) => panic!("prefix {n}: {a:?} against {b:?}"),
        }
    }
}

// ---- documented deviation ----

/// `KeyValue.value` is an embedded message. When it occurs twice, protobuf
/// merges the second into the first, so an empty second value leaves the
/// first in place. The decoder does not merge: the last occurrence replaces
/// the earlier ones, as for `resource`, `scope` and `status`. No real sender
/// encodes a repeated value, and the README lists this with the other
/// deviations. This test pins the behaviour so a change is deliberate.
#[test]
fn a_repeated_attribute_value_takes_the_last_occurrence() {
    let kv = concat(&[
        &ld(1, b"gen_ai.request.model"),
        &ld(2, &ld(1, b"gpt-4o")),
        &ld(2, b""),
    ]);
    let span = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8]), &ld(9, &kv)]);
    let req = ld(1, &ld(2, &ld(2, &span)));

    // Protobuf merges.
    let reference = ExportTraceServiceRequest::decode(req.as_slice()).unwrap();
    let attr = &reference.resource_spans[0].scope_spans[0].spans[0].attributes[0];
    assert_eq!(
        attr.value.as_ref().unwrap().value,
        Some(Value::StringValue("gpt-4o".into()))
    );

    // The decoder takes the last (empty) value, so nothing is promoted. The
    // attribute is kept as received in `attrs`.
    let batches = decoder().decode(&req).unwrap();
    let rows = all_rows(&batches);
    assert_eq!(rows[0].request_model, None);
    let cols = SpanColumns::try_new(&batches[0]).unwrap();
    assert_eq!(split_prefixed(cols.record(0).attrs), [&kv[..]]);

    // With the occurrences the other way round the result is the same value
    // as protobuf's: the last occurrence is the non-empty one.
    let kv = concat(&[
        &ld(1, b"gen_ai.request.model"),
        &ld(2, b""),
        &ld(2, &ld(1, b"gpt-4o")),
    ]);
    let span = concat(&[&ld(1, &[1; 16]), &ld(2, &[2; 8]), &ld(9, &kv)]);
    let batches = decoder().decode(&ld(1, &ld(2, &ld(2, &span)))).unwrap();
    assert_eq!(all_rows(&batches)[0].request_model, Some("gpt-4o"));
}
