//! End-to-end tests: a real server on ephemeral ports, a gRPC client built
//! from tonic's own pieces and a hand-written HTTP/1.1 client.

use bytes::{Buf, BufMut, Bytes};
use common::{Resp, dechunk, now_ns, opts};
use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value as V,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
    span::{Event, Link},
};
use prost::Message;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tonic::client::Grpc;
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

mod common;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

// ---- gRPC client ----

struct BytesCodec;
struct BytesEnc;
struct BytesDec;

impl Encoder for BytesEnc {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesDec {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEnc;
    type Decoder = BytesDec;
    fn encoder(&mut self) -> BytesEnc {
        BytesEnc
    }
    fn decoder(&mut self) -> BytesDec {
        BytesDec
    }
}

async fn channel(addr: SocketAddr) -> Channel {
    Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn export(
    ch: &Channel,
    body: Vec<u8>,
    compression: Option<CompressionEncoding>,
) -> Result<Bytes, Status> {
    let mut grpc = Grpc::new(ch.clone());
    if let Some(c) = compression {
        grpc = grpc.send_compressed(c);
    }
    grpc.ready()
        .await
        .map_err(|e| Status::unknown(e.to_string()))?;
    let reply = grpc
        .unary(
            Request::new(Bytes::from(body)),
            PathAndQuery::from_static(EXPORT),
            BytesCodec,
        )
        .await?;
    Ok(reply.into_inner())
}

// ---- HTTP client ----

async fn http(addr: SocketAddr, target: &str) -> Resp {
    http_with(addr, "GET", target, None).await
}

async fn http_with(addr: SocketAddr, method: &str, target: &str, accept: Option<&str>) -> Resp {
    let accept = accept.map_or(String::new(), |a| format!("Accept: {a}\r\n"));
    common::http(
        addr,
        method,
        target,
        &format!("{accept}Content-Length: 0\r\n"),
    )
    .await
}

// ---- fixtures ----

fn av(v: V) -> Option<AnyValue> {
    Some(AnyValue { value: Some(v) })
}

fn kv(key: &str, v: V) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: av(v),
        ..Default::default()
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn request(service: &str, spans: Vec<Span>) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", V::StringValue(service.into()))],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn tid(n: u8) -> Vec<u8> {
    vec![n; 16]
}

/// A span with a model, token counts and an operation.
#[allow(clippy::too_many_arguments)]
fn llm_span(
    trace: u8,
    span: u8,
    parent: Option<u8>,
    start: u64,
    model: &str,
    operation: &str,
    tokens: (i64, i64),
    error: bool,
) -> Span {
    Span {
        trace_id: tid(trace),
        span_id: vec![span; 8],
        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
        name: format!("span {span}"),
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1000,
        attributes: vec![
            kv("gen_ai.request.model", V::StringValue(model.into())),
            kv("gen_ai.operation.name", V::StringValue(operation.into())),
            kv("gen_ai.usage.input_tokens", V::IntValue(tokens.0)),
            kv("gen_ai.usage.output_tokens", V::IntValue(tokens.1)),
        ],
        status: error.then(|| SpanStatus {
            message: "failed".into(),
            code: 2,
        }),
        ..Default::default()
    }
}

/// One span that has a value of every kind, events, links and every
/// optional field.
fn full_span(start: u64) -> Span {
    Span {
        trace_id: tid(0xab),
        span_id: vec![1; 8],
        parent_span_id: vec![2; 8],
        trace_state: "a=b".into(),
        flags: 257,
        name: "full".into(),
        kind: 3,
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1000,
        attributes: vec![
            kv("s", V::StringValue("text \u{e9}\u{1f600} \"q\"".into())),
            kv("b", V::BoolValue(true)),
            kv("i", V::IntValue(-7)),
            kv("d", V::DoubleValue(1.5)),
            kv("inf", V::DoubleValue(f64::INFINITY)),
            kv("ninf", V::DoubleValue(f64::NEG_INFINITY)),
            kv(
                "arr",
                V::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(V::IntValue(1)),
                        },
                        AnyValue {
                            value: Some(V::StringValue("x".into())),
                        },
                    ],
                }),
            ),
            kv(
                "kv",
                V::KvlistValue(KeyValueList {
                    values: vec![kv("a", V::IntValue(1))],
                }),
            ),
            kv("bytes", V::BytesValue(vec![1, 2, 3, 4])),
            KeyValue {
                key: "unset".into(),
                value: None,
                ..Default::default()
            },
            kv("gen_ai.operation.name", V::StringValue("chat".into())),
            kv("gen_ai.request.model", V::StringValue("m1".into())),
            kv("gen_ai.usage.input_tokens", V::IntValue(10)),
            kv("gen_ai.usage.output_tokens", V::IntValue(20)),
        ],
        dropped_attributes_count: 2,
        events: vec![Event {
            time_unix_nano: 5,
            name: "ev".into(),
            attributes: vec![kv("k", V::StringValue("v".into()))],
            dropped_attributes_count: 0,
        }],
        dropped_events_count: 3,
        links: vec![Link {
            trace_id: tid(0xcd),
            span_id: vec![9; 8],
            trace_state: String::new(),
            attributes: vec![kv("l", V::BoolValue(true))],
            dropped_attributes_count: 0,
            flags: 0,
        }],
        dropped_links_count: 4,
        status: Some(SpanStatus {
            message: "boom".into(),
            code: 2,
        }),
    }
}

fn full_request(start: u64) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", V::StringValue("svc".into())),
                    kv("host.port", V::IntValue(8080)),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "scope".into(),
                    version: "1.2".into(),
                    attributes: vec![kv("scope.flavour", V::StringValue("plain".into()))],
                    ..Default::default()
                }),
                spans: vec![full_span(start)],
                schema_url: "https://example.test/scope/1.2".into(),
            }],
            schema_url: "https://example.test/resource/1.2".into(),
        }],
    }
    .encode_to_vec()
}

fn full_span_json(start: u64) -> Value {
    json!({
        "span_id": hex(&[1; 8]),
        "parent_span_id": hex(&[2; 8]),
        "name": "full",
        "kind": 3,
        "start_ns": start,
        "end_ns": start + 1000,
        "status_code": 2,
        "status_message": "boom",
        "trace_state": "a=b",
        "service_name": "svc",
        "scope_name": "scope",
        "scope_version": "1.2",
        "resource_schema_url": "https://example.test/resource/1.2",
        "scope_schema_url": "https://example.test/scope/1.2",
        "scope_attributes": {"scope.flavour": "plain"},
        "operation": "chat",
        "request_model": "m1",
        "input_tokens": 10,
        "output_tokens": 20,
        "flags": 257,
        "dropped_attributes_count": 2,
        "dropped_events_count": 3,
        "dropped_links_count": 4,
        "resource": {"service.name": "svc", "host.port": 8080},
        "attributes": {
            "s": "text \u{e9}\u{1f600} \"q\"",
            "b": true,
            "i": -7,
            "d": 1.5,
            "inf": "Infinity",
            "ninf": "-Infinity",
            "arr": [1, "x"],
            "kv": {"a": 1},
            "bytes": "AQIDBA==",
            "unset": null,
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "m1",
            "gen_ai.usage.input_tokens": 10,
            "gen_ai.usage.output_tokens": 20,
        },
        "events": [{"time_ns": 5, "name": "ev", "attributes": {"k": "v"}}],
        "links": [{"trace_id": hex(&[0xcd; 16]), "span_id": hex(&[9; 8]), "attributes": {"l": true}}],
    })
}

async fn started(dir: &Path) -> Server {
    Server::start(opts(dir)).await.unwrap()
}

async fn trace_ids(api: SocketAddr, query: &str) -> Vec<String> {
    let r = http(api, &format!("/api/v1/traces{query}")).await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let mut ids: Vec<String> = r.json()["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["trace_id"].as_str().unwrap().to_owned())
        .collect();
    ids.sort();
    ids
}

// ---- tests ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_and_read_back_every_field() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let api = server.api_addr();
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 60_000_000_000;

    // Uncompressed, then the same trace again under another id with gzip and
    // zstd: all three must read back identically.
    for (n, compression) in [
        (0xab, None),
        (0xac, Some(CompressionEncoding::Gzip)),
        (0xad, Some(CompressionEncoding::Zstd)),
    ] {
        let mut req = ExportTraceServiceRequest::decode(full_request(start).as_slice()).unwrap();
        let span = &mut req.resource_spans[0].scope_spans[0].spans[0];
        span.trace_id = tid(n);
        let reply = export(&ch, req.encode_to_vec(), compression).await.unwrap();
        assert!(reply.is_empty());

        let r = http(api, &format!("/api/v1/traces/{}", hex(&tid(n)))).await;
        assert_eq!(r.status, 200);
        assert!(r.headers.contains("content-type: application/json"));
        let got = r.json();
        assert_eq!(got["trace_id"], hex(&tid(n)));
        assert_eq!(got["spans"], json!([full_span_json(start)]));

        // The protobuf form decodes to the span that was sent.
        let r = http_with(
            api,
            "GET",
            &format!("/api/v1/traces/{}", hex(&tid(n))),
            Some("application/x-protobuf"),
        )
        .await;
        assert_eq!(r.status, 200);
        assert!(r.headers.contains("content-type: application/x-protobuf"));
        let back = ExportTraceServiceRequest::decode(r.body.as_slice()).unwrap();
        let sent = req;
        assert_eq!(back.resource_spans.len(), 1);
        assert_eq!(
            back.resource_spans[0].resource,
            sent.resource_spans[0].resource
        );
        assert_eq!(
            back.resource_spans[0].scope_spans[0].spans,
            sent.resource_spans[0].scope_spans[0].spans
        );
        assert_eq!(
            back.resource_spans[0].scope_spans[0]
                .scope
                .as_ref()
                .unwrap()
                .name,
            "scope"
        );
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nan_double_and_multiple_spans_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 1_000_000_000;
    let mut a = llm_span(5, 1, None, start, "m", "chat", (1, 2), false);
    a.attributes.push(kv("nan", V::DoubleValue(f64::NAN)));
    let b = llm_span(5, 2, Some(1), start + 10, "m", "chat", (3, 4), false);
    export(&ch, request("s", vec![b, a]), None).await.unwrap();
    let r = http(
        server.api_addr(),
        &format!("/api/v1/traces/{}", hex(&tid(5))),
    )
    .await;
    let j = r.json();
    let spans = j["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 2);
    // The engine returns spans by start time.
    assert_eq!(spans[0]["span_id"], hex(&[1; 8]));
    assert_eq!(spans[0]["attributes"]["nan"], "NaN");
    assert!(spans[0].get("parent_span_id").is_none());
    assert_eq!(spans[1]["parent_span_id"], hex(&[1; 8]));
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_and_token_endpoints_with_filters() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let api = server.api_addr();
    let ch = channel(server.otlp_grpc_addr()).await;
    let base = now_ns() - 600_000_000_000;
    let s = 1_000_000_000;

    let t1 = vec![
        llm_span(1, 1, None, base, "m1", "chat", (10, 20), false),
        llm_span(1, 2, Some(1), base + s / 2, "m1", "chat", (1, 2), false),
    ];
    let t2 = vec![llm_span(2, 1, None, base + s, "m2", "chat", (5, 5), true)];
    let t3 = vec![llm_span(
        3,
        1,
        None,
        base + 2 * s,
        "m1",
        "chat",
        (7, 8),
        false,
    )];
    let t4 = vec![llm_span(
        4,
        1,
        None,
        base + 3 * s,
        "m2",
        "embeddings",
        (2, 0),
        false,
    )];
    export(&ch, request("a", t1), None).await.unwrap();
    export(&ch, request("a", t2), None).await.unwrap();
    export(&ch, request("b", t3), None).await.unwrap();
    export(&ch, request("b", t4), None).await.unwrap();
    let id = |n: u8| hex(&tid(n));
    let ids = |ns: &[u8]| {
        let mut v: Vec<String> = ns.iter().map(|n| id(*n)).collect();
        v.sort();
        v
    };

    assert_eq!(trace_ids(api, "").await, ids(&[1, 2, 3, 4]));
    assert_eq!(trace_ids(api, "?service=b").await, ids(&[3, 4]));
    assert_eq!(trace_ids(api, "?model=m2").await, ids(&[2, 4]));
    assert_eq!(trace_ids(api, "?error=true").await, ids(&[2]));
    assert_eq!(trace_ids(api, "?error=false").await, ids(&[1, 3, 4]));
    assert_eq!(trace_ids(api, "?service=a&model=m1").await, ids(&[1]));
    assert_eq!(trace_ids(api, "?limit=2").await.len(), 2);
    assert_eq!(trace_ids(api, "?limit=1&service=a").await.len(), 1);
    // `to` is exclusive and `from` inclusive.
    let q = format!("?from={}&to={}", base + 2 * s, base + 3 * s);
    assert_eq!(trace_ids(api, &q).await, ids(&[3]));
    assert!(trace_ids(api, "?from=0&to=1").await.is_empty());
    // Percent-encoding and `+` are decoded.
    assert!(
        trace_ids(api, "?service=no%20such+service")
            .await
            .is_empty()
    );

    // Every field of a summary.
    let r = http(api, "/api/v1/traces?service=a&error=false").await;
    let traces = r.json()["traces"].clone();
    assert_eq!(
        traces,
        json!([{
            "trace_id": id(1),
            "start_ns": base,
            "end_ns": base + s / 2 + 1000,
            "root_service": "a",
            "root_name": "span 1",
            "span_count": 2,
            "error": false,
            "input_tokens": 11,
            "output_tokens": 22,
            "models": ["m1"],
        }])
    );

    let models = |q: &str| {
        let q = q.to_owned();
        async move {
            let r = http(api, &format!("/api/v1/tokens{q}")).await;
            assert_eq!(r.status, 200);
            let mut rows = r.json()["models"].as_array().unwrap().clone();
            rows.sort_by_key(|m| m["model"].as_str().unwrap().to_owned());
            rows
        }
    };
    assert_eq!(
        models("").await,
        vec![
            json!({"model": "m1", "count": 3, "input_tokens": 18, "output_tokens": 30}),
            json!({"model": "m2", "count": 2, "input_tokens": 7, "output_tokens": 5}),
        ]
    );
    assert_eq!(
        models("?operation=embeddings").await,
        vec![json!({"model": "m2", "count": 1, "input_tokens": 2, "output_tokens": 0})]
    );
    assert!(models("?from=0&to=1").await.is_empty());
    let q = format!("?from={}&to={}", base + s, base + 3 * s);
    assert_eq!(models(&q).await.len(), 2);

    let stats = http(api, "/api/v1/stats").await.json();
    assert_eq!(stats["records_ingested"], 4);
    for k in [
        "bytes_ingested",
        "acked_lsn",
        "flushed_lsn",
        "buffered_bytes",
        "parts",
        "flushes",
        "flushing",
        "last_error",
        "payload_logical_bytes",
        "payload_unique_bytes",
        "payload_stored_bytes",
        "fatal",
        "maintenance_passes",
        "retention_parts_removed",
        "last_maintenance_error",
    ] {
        assert!(stats.get(k).is_some(), "stats lacks {k}");
    }
    // Nothing has been removed by retention and maintenance has had no error;
    // the count of passes is a number whether or not one has run yet.
    assert_eq!(stats["retention_parts_removed"], 0, "{stats}");
    assert!(stats["last_maintenance_error"].is_null(), "{stats}");
    assert!(stats["maintenance_passes"].is_u64(), "{stats}");
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_error_statuses() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let api = server.api_addr();

    let r = http(api, "/healthz").await;
    assert_eq!((r.status, r.body.as_slice()), (200, b"ok".as_slice()));

    for target in [
        "/api/v1/traces/xyz",
        "/api/v1/traces/0123",
        &format!("/api/v1/traces/{}", "g".repeat(32)),
        "/api/v1/traces?limit=0",
        "/api/v1/traces?limit=1001",
        "/api/v1/traces?limit=x",
        "/api/v1/traces?error=maybe",
        "/api/v1/traces?from=abc",
        "/api/v1/traces?to=1.5",
        "/api/v1/traces?servce=a",
        "/api/v1/traces?service=%zz",
        "/api/v1/tokens?from=abc",
        "/api/v1/tokens?model=x",
    ] {
        let r = http(api, target).await;
        assert_eq!(r.status, 400, "{target}");
        assert!(r.json()["error"].is_string(), "{target}");
    }
    for target in [
        &format!("/api/v1/traces/{}", "0".repeat(32)),
        "/nothing",
        "/api/v1",
    ] {
        let r = http(api, target).await;
        assert_eq!(r.status, 404, "{target}");
        assert!(r.json()["error"].is_string());
    }
    let r = http_with(api, "POST", "/api/v1/stats", None).await;
    assert_eq!(r.status, 405);
    assert!(r.json()["error"].is_string());
    // The maximum limit is accepted.
    assert_eq!(http(api, "/api/v1/traces?limit=1000").await.status, 200);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_and_empty_requests() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;

    let good = request(
        "s",
        vec![llm_span(1, 1, None, now_ns(), "m", "chat", (1, 1), false)],
    );
    for bad in [
        vec![0xff; 10],
        good[..good.len() - 3].to_vec(),
        vec![0x0a, 0xff, 0xff, 0xff, 0x7f],
    ] {
        let e = export(&ch, bad, None).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{e}");
    }
    assert_eq!(server.engine().stats().records_ingested, 0);
    assert!(trace_ids(server.api_addr(), "").await.is_empty());

    // No spans: success, and nothing reaches the log.
    for empty in [
        Vec::new(),
        ExportTraceServiceRequest::default().encode_to_vec(),
        request("s", vec![]),
    ] {
        assert!(export(&ch, empty, None).await.unwrap().is_empty());
    }
    assert_eq!(server.engine().stats().records_ingested, 0);

    // A wrong method is not served.
    let mut grpc = Grpc::new(ch.clone());
    grpc.ready().await.unwrap();
    let e = grpc
        .unary(
            Request::new(Bytes::new()),
            PathAndQuery::from_static("/opentelemetry.proto.collector.trace.v1.TraceService/Nope"),
            BytesCodec,
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn back_pressure_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.engine.max_buffered_bytes = 1;
    let server = Server::start(o).await.unwrap();
    let ch = channel(server.otlp_grpc_addr()).await;
    let req = |n| {
        request(
            "s",
            vec![llm_span(n, 1, None, now_ns(), "m", "chat", (1, 1), false)],
        )
    };

    // A request larger than the limit is admitted when nothing is buffered.
    export(&ch, req(1), None).await.unwrap();
    let e = export(&ch, req(2), None).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e}");
    assert_eq!(server.engine().stats().records_ingested, 1);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reply_waits_for_the_log_sync() {
    let dir = tempfile::tempdir().unwrap();
    let held = Arc::new(AtomicBool::new(true));
    let mut o = opts(dir.path());
    let hook = held.clone();
    o.engine.wal.sync_hook = Some(Arc::new(move || {
        while hook.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    }));
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();
    let ch = channel(server.otlp_grpc_addr()).await;
    let body = request(
        "s",
        vec![llm_span(1, 1, None, now_ns(), "m", "chat", (1, 1), false)],
    );
    let call = tokio::spawn(async move { export(&ch, body, None).await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!call.is_finished(), "replied before the sync finished");
    // Nothing is visible before it is durable either.
    assert_eq!(
        http(api, &format!("/api/v1/traces/{}", hex(&tid(1))))
            .await
            .status,
        404
    );

    held.store(false, Ordering::SeqCst);
    let reply = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(reply.is_empty());
    assert_eq!(
        http(api, &format!("/api/v1/traces/{}", hex(&tid(1))))
            .await
            .status,
        200
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fatal_engine_error_is_unavailable_and_reported() {
    let dir = tempfile::tempdir().unwrap();
    let broken = Arc::new(AtomicBool::new(false));
    let mut o = opts(dir.path());
    let hook = broken.clone();
    o.engine.wal.sync_hook = Some(Arc::new(move || {
        if hook.load(Ordering::SeqCst) {
            Err(std::io::Error::other("disk gone"))
        } else {
            Ok(())
        }
    }));
    let server = Server::start(o).await.unwrap();
    let ch = channel(server.otlp_grpc_addr()).await;
    let req = |n| {
        request(
            "s",
            vec![llm_span(n, 1, None, now_ns(), "m", "chat", (1, 1), false)],
        )
    };
    export(&ch, req(1), None).await.unwrap();

    broken.store(true, Ordering::SeqCst);
    let e = export(&ch, req(2), None).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e}");
    tokio::time::timeout(Duration::from_secs(10), server.fatal())
        .await
        .expect("the server was not told about the fatal error");
    // Later requests are refused too.
    let e = export(&ch, req(3), None).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable);
    // Shutting down after a fatal error still returns.
    let _ = server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_then_reopen_shows_the_same_data() {
    let dir = tempfile::tempdir().unwrap();
    let start = now_ns() - 60_000_000_000;
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    export(&ch, full_request(start), None).await.unwrap();
    export(
        &ch,
        request(
            "a",
            vec![llm_span(1, 1, None, start, "m1", "chat", (3, 4), false)],
        ),
        None,
    )
    .await
    .unwrap();
    let before = http(server.api_addr(), "/api/v1/traces").await.json();
    let trace = format!("/api/v1/traces/{}", hex(&tid(0xab)));
    let trace_before = http(server.api_addr(), &trace).await.json();
    drop(ch);
    server.shutdown().await.unwrap();

    let server = started(dir.path()).await;
    // The clean shutdown flushed: the data is in parts, not replayed.
    let stats = http(server.api_addr(), "/api/v1/stats").await.json();
    assert!(stats["parts"].as_u64().unwrap() > 0, "{stats}");
    assert_eq!(
        http(server.api_addr(), "/api/v1/traces").await.json(),
        before
    );
    assert_eq!(http(server.api_addr(), &trace).await.json(), trace_before);
    assert_eq!(trace_before["spans"], json!([full_span_json(start)]));
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_exporters() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // Small enough that requests wait for each other.
    o.max_inflight_bytes = 4096;
    let server = Server::start(o).await.unwrap();
    let ch = channel(server.otlp_grpc_addr()).await;
    let base = now_ns() - 600_000_000_000;

    let mut tasks = Vec::new();
    for client in 0..16u8 {
        let ch = ch.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..10u8 {
                let n = client * 10 + i + 1;
                let span = llm_span(n, 1, None, base + u64::from(n), "m", "chat", (1, 2), false);
                let compression = (n % 2 == 0).then_some(CompressionEncoding::Gzip);
                export(&ch, request("s", vec![span]), compression)
                    .await
                    .unwrap();
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(60), t)
            .await
            .unwrap()
            .unwrap();
    }
    let api = server.api_addr();
    let r = http(api, "/api/v1/tokens").await.json();
    assert_eq!(
        r["models"],
        json!([{"model": "m", "count": 160, "input_tokens": 160, "output_tokens": 320}])
    );
    assert_eq!(trace_ids(api, "?limit=1000").await.len(), 160);
    assert_eq!(server.engine().stats().records_ingested, 160);
    server.shutdown().await.unwrap();
}

// ---- host check, tokens, limits, shutdown ----

/// A GET with the given `Host` header.
fn http_host_blocking(addr: SocketAddr, host: &str, target: &str) -> Resp {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        s,
        "GET {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(raw[..split].to_vec()).unwrap();
    Resp {
        status: head.split(' ').nth(1).unwrap().parse().unwrap(),
        headers: head.to_lowercase(),
        body: if head.to_lowercase().contains("transfer-encoding: chunked") {
            dechunk(&raw[split + 4..])
        } else {
            raw[split + 4..].to_vec()
        },
    }
}

async fn http_host(addr: SocketAddr, host: &str, target: &str) -> Resp {
    let (host, target) = (host.to_owned(), target.to_owned());
    tokio::task::spawn_blocking(move || http_host_blocking(addr, &host, &target))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_host_header_is_checked_on_a_loopback_api() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.api_allowed_hosts = vec!["traces.internal".into()];
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();
    let port = api.port();

    for host in [
        "localhost".to_string(),
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
        // An address literal cannot be re-pointed: no configuration needed.
        format!("10.1.2.3:{port}"),
        format!("traces.internal:{port}"),
        "TRACES.INTERNAL".to_string(),
    ] {
        let r = http_host(api, &host, "/healthz").await;
        assert_eq!(r.status, 200, "{host}");
    }
    for host in [
        format!("evil.example:{port}"),
        "evil.example".to_string(),
        "localhost.evil.example".to_string(),
        String::new(),
    ] {
        let r = http_host(api, &host, "/healthz").await;
        assert_eq!(r.status, 421, "{host:?}");
        assert!(r.headers.contains("content-type: application/json"));
        assert!(r.json()["error"].is_string());
    }
    // The refusal covers every path, existing or not.
    assert_eq!(http_host(api, "evil.example", "/nothing").await.status, 421);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_host_check_is_on_for_a_non_loopback_bind_without_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.api_addr = "0.0.0.0:0".parse().unwrap();
    let server = Server::start(o).await.unwrap();
    let api = SocketAddr::from(([127, 0, 0, 1], server.api_addr().port()));
    assert_eq!(
        http_host(api, "anything.example", "/healthz").await.status,
        421
    );
    assert_eq!(http_host(api, "10.9.8.7", "/healthz").await.status, 200);
    server.shutdown().await.unwrap();
    // With names configured, those are answered too.
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.api_addr = "0.0.0.0:0".parse().unwrap();
    o.api_allowed_hosts = vec!["ok.example".into()];
    let server = Server::start(o).await.unwrap();
    let api = SocketAddr::from(([127, 0, 0, 1], server.api_addr().port()));
    assert_eq!(http_host(api, "ok.example", "/healthz").await.status, 200);
    assert_eq!(
        http_host(api, "anything.example", "/healthz").await.status,
        421
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens_are_ordered_limited_and_marked_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 10_000_000_000;
    // Spans per model: a 1, b 3, c 3, d 2.
    let mut spans = Vec::new();
    for (i, model) in ["a", "b", "b", "b", "c", "c", "c", "d", "d"]
        .iter()
        .enumerate()
    {
        let (id, at) = (i as u8 + 1, start + i as u64);
        spans.push(llm_span(7, id, None, at, model, "chat", (1, 1), false));
    }
    export(&ch, request("s", spans), None).await.unwrap();
    let api = server.api_addr();

    let r = http(api, "/api/v1/tokens").await.json();
    let names: Vec<&str> = r["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["b", "c", "d", "a"]);
    assert!(r.get("truncated").is_none());

    let r = http(api, "/api/v1/tokens?limit=2").await.json();
    assert_eq!(r["models"].as_array().unwrap().len(), 2);
    assert_eq!(r["models"][0]["model"], "b");
    assert_eq!(r["models"][1]["model"], "c");
    assert_eq!(r["truncated"], true);
    // A limit that is exactly the number of rows cuts nothing.
    let r = http(api, "/api/v1/tokens?limit=4").await.json();
    assert!(r.get("truncated").is_none());
    for bad in ["0", "10001", "x", "-1"] {
        let r = http(api, &format!("/api/v1/tokens?limit={bad}")).await;
        assert_eq!(r.status, 400, "{bad}");
    }
    assert_eq!(http(api, "/api/v1/tokens?limit=10000").await.status, 200);
    server.shutdown().await.unwrap();
}

/// With room for one connection, a second one is not served until the first
/// is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_over_the_limit_wait() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.api_max_connections = 1;
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();

    let first = TcpStream::connect(api).unwrap();
    let waiting = tokio::spawn(http(api, "/healthz"));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!waiting.is_finished(), "served over the connection limit");
    drop(first);
    let r = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("not served after the first connection closed")
        .unwrap();
    assert_eq!(r.status, 200);
    server.shutdown().await.unwrap();
}

/// A client that holds an API connection open in the middle of a request must
/// not delay shutdown beyond the drain deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_returns_with_an_idle_api_connection() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.drain_timeout = Duration::from_millis(500);
    let server = Server::start(o).await.unwrap();
    let mut idle = TcpStream::connect(server.api_addr()).unwrap();
    idle.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n")
        .unwrap();
    let began = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("shutdown did not return")
        .unwrap();
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "{:?}",
        began.elapsed()
    );
    drop(idle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_directory_that_is_created_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("fresh");
    let server = started(&dir).await;
    let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    server.shutdown().await.unwrap();
}

// ---- partial success, fatal state ----

fn response_of(reply: &Bytes) -> ExportTraceServiceResponse {
    ExportTraceServiceResponse::decode(reply.as_ref()).unwrap()
}

/// A span the decoder skips: the trace id has the wrong length.
fn invalid_span(id: u8) -> Span {
    Span {
        trace_id: vec![9; 15],
        span_id: vec![id; 8],
        name: "bad".into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_spans_are_rejected_and_counted_while_the_valid_ones_are_stored() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 1_000_000_000;

    // Valid, invalid, valid, invalid (all-zero span id).
    let mut zero_id = invalid_span(0);
    zero_id.trace_id = tid(0x71);
    zero_id.span_id = vec![0; 8];
    let spans = vec![
        llm_span(0x71, 1, None, start, "m", "chat", (1, 2), false),
        invalid_span(2),
        llm_span(0x71, 3, Some(1), start + 5, "m", "chat", (1, 2), false),
        zero_id,
    ];
    let reply = export(&ch, request("s", spans), None).await.unwrap();
    let resp = response_of(&reply);
    let partial = resp.partial_success.expect("a partial success");
    assert_eq!(partial.rejected_spans, 2);
    assert!(!partial.error_message.is_empty());

    let r = http(
        server.api_addr(),
        &format!("/api/v1/traces/{}", hex(&tid(0x71))),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["spans"].as_array().unwrap().len(), 2);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_of_only_invalid_spans_is_not_logged() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let reply = export(
        &ch,
        request("s", vec![invalid_span(1), invalid_span(2), invalid_span(3)]),
        None,
    )
    .await
    .unwrap();
    let partial = response_of(&reply).partial_success.unwrap();
    assert_eq!(partial.rejected_spans, 3);
    assert!(!partial.error_message.is_empty());
    assert_eq!(server.engine().stats().records_ingested, 0);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_with_no_rejections_gets_an_empty_reply_message() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let span = llm_span(0x72, 1, None, now_ns(), "m", "chat", (1, 1), false);
    let reply = export(&ch, request("s", vec![span]), None).await.unwrap();
    assert!(reply.is_empty());
    server.shutdown().await.unwrap();
}

/// A flush in the background fails fatally with no request in flight: the
/// health check turns 503 and `Server::fatal` resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fatal_error_in_a_background_flush_is_noticed() {
    use nwdb_engine::Stage;
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.engine.flush_interval = Duration::from_millis(200);
    let armed = Arc::new(AtomicBool::new(false));
    let flag = armed.clone();
    o.engine.stage_hook = Some(Arc::new(move |stage| {
        if stage == Stage::FlushFrozen && flag.load(Ordering::SeqCst) {
            return Err(nwdb_core::Error::fatal("injected"));
        }
        Ok(())
    }));
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();
    assert_eq!(http(api, "/healthz").await.status, 200);
    assert_eq!(http(api, "/api/v1/stats").await.json()["fatal"], false);

    let ch = channel(server.otlp_grpc_addr()).await;
    let span = llm_span(0x73, 1, None, now_ns(), "m", "chat", (1, 1), false);
    export(&ch, request("s", vec![span]), None).await.unwrap();
    armed.store(true, Ordering::SeqCst);

    tokio::time::timeout(Duration::from_secs(15), server.fatal())
        .await
        .expect("the fatal flag was not noticed");
    let r = http(api, "/healthz").await;
    assert_eq!(r.status, 503);
    assert!(r.json()["error"].is_string());
    assert_eq!(http(api, "/api/v1/stats").await.json()["fatal"], true);
    let _ = server.shutdown().await;
}

/// Text that looks like words and does not repeat by chance, from a small
/// generator so that the test needs no random-number crate.
fn words(seed: &mut u64, len: usize) -> String {
    let mut out = String::with_capacity(len + 8);
    while out.len() < len {
        for _ in 0..4 {
            *seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.push(char::from(b'a' + ((*seed >> 33) % 26) as u8));
        }
        out.push(' ');
    }
    out.truncate(len);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_growing_conversation_is_stored_once_and_reads_back_identical() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let api = server.api_addr();
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 60_000_000_000;

    // Every call re-sends the system prompt, the tool definitions and all
    // earlier messages, and adds a user message, an answer and a tool result.
    let mut seed = 42;
    let system = words(&mut seed, 3_000);
    let tools = format!(
        "[{}]",
        (0..5)
            .map(|i| format!(
                r#"{{"name":"tool_{i}","description":"{}"}}"#,
                words(&mut seed, 800)
            ))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut history: Vec<String> = Vec::new();
    for call in 0..24u8 {
        let msg = |role: &str, len: usize, seed: &mut u64| {
            format!(r#"{{"role":"{role}","content":"{}"}}"#, words(seed, len))
        };
        history.push(msg("user", 1_200, &mut seed));
        let mut span = llm_span(
            0x77,
            call + 1,
            None,
            start + u64::from(call),
            "m",
            "chat",
            (1, 1),
            false,
        );
        span.attributes.extend([
            kv("gen_ai.system_instructions", V::StringValue(system.clone())),
            kv("gen_ai.tool.definitions", V::StringValue(tools.clone())),
            kv(
                "gen_ai.input.messages",
                V::StringValue(format!("[{}]", history.join(","))),
            ),
        ]);
        history.push(msg("assistant", 1_500, &mut seed));
        history.push(msg("tool", 4_000, &mut seed));
        export(&ch, request("agent", vec![span]), None)
            .await
            .unwrap();
    }

    let path = format!("/api/v1/traces/{}", hex(&tid(0x77)));
    let before = http(api, &path).await;
    assert_eq!(before.status, 200);
    // Nothing is flushed yet: no part, so no stored payload.
    let stats = http(api, "/api/v1/stats").await.json();
    assert_eq!(stats["payload_logical_bytes"], 0);
    assert_eq!(stats["payload_stored_bytes"], 0);

    server.engine().flush().unwrap();
    let stats = http(api, "/api/v1/stats").await.json();
    let logical = stats["payload_logical_bytes"].as_u64().unwrap();
    let unique = stats["payload_unique_bytes"].as_u64().unwrap();
    let stored = stats["payload_stored_bytes"].as_u64().unwrap();
    eprintln!("payload logical {logical} unique {unique} stored {stored}");
    assert!(logical > 1_000_000, "{logical}");
    // The distinct text, before compression, is what proves that repeats are
    // shared: compression alone could make the stored size small.
    assert!(unique > 0 && unique * 3 < logical, "{unique} of {logical}");
    assert!(stored > 0 && stored <= unique, "{stored} of {unique}");
    let after = http(api, &path).await;
    assert_eq!(
        after.body, before.body,
        "the trace changed by being flushed"
    );
    server.shutdown().await.unwrap();

    // The same bytes after a restart, from the part on disk.
    let server = started(dir.path()).await;
    let api = server.api_addr();
    let restarted = http(api, &path).await;
    assert_eq!(
        restarted.body, before.body,
        "the trace changed by a restart"
    );
    let stats = http(api, "/api/v1/stats").await.json();
    assert_eq!(stats["payload_logical_bytes"].as_u64().unwrap(), logical);
    server.shutdown().await.unwrap();
}
