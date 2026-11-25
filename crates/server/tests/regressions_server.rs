//! Review tests for the server: shutdown with a stalled sender, requests the
//! door acknowledges but the API cannot show, status codes, and the edges of
//! the HTTP API.

use bytes::{Buf, BufMut, Bytes};
use nwdb_server::{MAX_MESSAGE, Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

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

async fn export(ch: &Channel, body: Vec<u8>) -> Result<Bytes, Status> {
    let mut grpc = Grpc::new(ch.clone());
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

/// The body of a chunked response without the chunk framing.
fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let eol = raw.windows(2).position(|w| w == b"\r\n").unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&raw[..eol]).unwrap().trim(), 16).unwrap();
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&raw[eol + 2..eol + 2 + size]);
        raw = &raw[eol + 2 + size + 2..];
    }
}

struct Resp {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

fn http_blocking(addr: SocketAddr, target: &str, accept: Option<&str>) -> Resp {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let accept = accept.map_or(String::new(), |a| format!("Accept: {a}\r\n"));
    write!(
        s,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\n{accept}Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(raw[..split].to_vec()).unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Resp {
        status,
        headers: head.to_lowercase(),
        body: if head.to_lowercase().contains("transfer-encoding: chunked") {
            dechunk(&raw[split + 4..])
        } else {
            raw[split + 4..].to_vec()
        },
    }
}

async fn http(addr: SocketAddr, target: &str, accept: Option<&str>) -> Resp {
    let target = target.to_owned();
    let accept = accept.map(str::to_owned);
    tokio::task::spawn_blocking(move || http_blocking(addr, &target, accept.as_deref()))
        .await
        .unwrap()
}

/// How many spans the reply says were rejected.
fn rejected(reply: &Bytes) -> i64 {
    ExportTraceServiceResponse::decode(reply.as_ref())
        .unwrap()
        .partial_success
        .map_or(0, |p| p.rejected_spans)
}

// ---- fixtures ----

fn opts(dir: &Path) -> ServerOptions {
    let mut o = ServerOptions::new(dir);
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o
}

async fn started(dir: &Path) -> Server {
    Server::start(opts(dir)).await.unwrap()
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn span(trace: u8, id: u8, start: u64) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![id; 8],
        name: format!("span {id}"),
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1000,
        attributes: vec![KeyValue {
            key: "k".into(),
            value: Some(AnyValue {
                value: Some(V::StringValue("v".into())),
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn request(spans: Vec<Span>) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource::default()),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

// ---- hand-built wire bytes ----

fn varint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
}

fn ld(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = varint(u64::from(field) << 3 | 2);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}

fn fixed64(field: u32, v: u64) -> Vec<u8> {
    let mut out = varint(u64::from(field) << 3 | 1);
    out.extend(v.to_le_bytes());
    out
}

/// A request with one span; `extra` is appended to the span's fields and
/// `resource`, if given, is the resource message.
fn raw_request(trace: u8, id: u8, start: u64, extra: &[u8], resource: Option<&[u8]>) -> Vec<u8> {
    let span = [
        ld(1, &[trace; 16]),
        ld(2, &[id; 8]),
        ld(5, b"odd"),
        fixed64(7, start),
        fixed64(8, start + 1000),
        extra.to_vec(),
    ]
    .concat();
    let mut rs = Vec::new();
    if let Some(r) = resource {
        rs.extend(ld(1, r));
    }
    rs.extend(ld(2, &ld(2, &span)));
    ld(1, &rs)
}

/// A span attribute `key` whose `AnyValue` is `value` (raw bytes).
fn attr(key: &str, value: &[u8]) -> Vec<u8> {
    ld(9, &[ld(1, key.as_bytes()), ld(2, value)].concat())
}

/// An `AnyValue` holding arrays nested `depth` deep around an int.
fn nested(depth: usize) -> Vec<u8> {
    let mut v = vec![0x18, 1];
    for _ in 0..depth {
        v = ld(5, &ld(1, &v));
    }
    v
}

// ---- tests ----

/// A sender that opens an export and stops sending (a crashed client behind a
/// half-open connection, or a hostile one) must not keep the server from
/// shutting down: the README promises that SIGTERM flushes and exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_returns_while_a_sender_stalls() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let mut grpc = Grpc::new(ch.clone());
    grpc.ready().await.unwrap();
    let stalled = tokio::spawn(async move {
        grpc.client_streaming(
            Request::new(tonic::codegen::tokio_stream::pending::<Bytes>()),
            PathAndQuery::from_static(EXPORT),
            BytesCodec,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let done = tokio::time::timeout(Duration::from_secs(20), server.shutdown()).await;
    stalled.abort();
    assert!(
        done.is_ok(),
        "Server::shutdown did not return within 20 s while one export stream was open and idle"
    );
}

/// `Export` is unary. A sender that puts two messages on one call must get an
/// error, or have both stored; it must not get OK while the second message is
/// read and thrown away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_message_of_an_acknowledged_export_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns();
    let messages = vec![
        Bytes::from(request(vec![span(0x60, 1, start)])),
        Bytes::from(request(vec![span(0x61, 1, start)])),
    ];
    let mut grpc = Grpc::new(ch.clone());
    grpc.ready().await.unwrap();
    let reply = grpc
        .client_streaming(
            Request::new(tonic::codegen::tokio_stream::iter(messages)),
            PathAndQuery::from_static(EXPORT),
            BytesCodec,
        )
        .await;
    if reply.is_ok() {
        for trace in [0x60u8, 0x61] {
            let r = http(
                server.api_addr(),
                &format!("/api/v1/traces/{}", hex(&[trace; 16])),
                None,
            )
            .await;
            assert_eq!(
                r.status,
                200,
                "the export of two messages was acknowledged, but trace {} is missing",
                hex(&[trace; 16])
            );
        }
    }
    server.shutdown().await.unwrap();
}

/// OTLP defines an all-zero span id as invalid, like an empty one. The door
/// refuses an empty id but accepts the all-zero one; two spans that carry it
/// in one trace are then one span to the engine, and the other is lost after
/// the request was acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spans_with_all_zero_ids_are_refused_or_all_kept() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns();
    let mut a = span(0x62, 0, start);
    let mut b = span(0x62, 0, start + 10);
    a.name = "first".into();
    b.name = "second".into();
    // Refused as an error, or by name in the reply's count of rejected spans,
    // or kept: all three are fine; acknowledging both and keeping one is not.
    if export(&ch, request(vec![a, b]))
        .await
        .is_ok_and(|r| rejected(&r) == 0)
    {
        let r = http(
            server.api_addr(),
            &format!("/api/v1/traces/{}", hex(&[0x62; 16])),
            None,
        )
        .await;
        let j: Value = serde_json::from_slice(&r.body).unwrap();
        let names: Vec<&str> = j["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["first", "second"], "both spans were acknowledged");
    }
    server.shutdown().await.unwrap();
}

/// What the door acknowledges, the API must be able to show. Each case puts a
/// normal span and one odd span into one trace. The odd span is either
/// refused at the door, or accepted and then shown; accepting it and then
/// failing the whole trace loses the normal span too, and lets any sender
/// hide any trace whose id it knows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_span_never_hides_its_trace() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let api = server.api_addr();
    let ch = channel(server.otlp_grpc_addr()).await;
    let start = now_ns() - 60_000_000_000;

    // A lone UTF-16 surrogate as some protobuf encoders write it (CESU-8):
    // what a JavaScript sender produces when a model's output is cut in the
    // middle of an emoji.
    let surrogate = [0xed, 0xa0, 0xbd];
    let invalid_resource = ld(
        1,
        &[ld(1, b"host.name"), ld(2, &ld(1, &surrogate))].concat(),
    );
    // What the case is, bytes added to the span, the resource message.
    type Case = (&'static str, Vec<u8>, Option<Vec<u8>>);
    let cases: Vec<Case> = vec![
        (
            "invalid UTF-8 in a span attribute value",
            attr("llm.output", &ld(1, &surrogate)),
            None,
        ),
        // Valid protobuf: Go's decoder (the Collector) allows 10,000 levels.
        (
            "a value nested 70 arrays deep",
            attr("deep", &nested(70)),
            None,
        ),
        (
            "an event whose name is not UTF-8",
            ld(11, &ld(2, &surrogate)),
            None,
        ),
        ("an event that is not a message", ld(11, &[0xff]), None),
        (
            "invalid UTF-8 in a resource attribute value",
            Vec::new(),
            Some(invalid_resource),
        ),
    ];

    let mut failures = Vec::new();
    for (i, (what, extra, resource)) in cases.into_iter().enumerate() {
        let trace = 0x40 + i as u8;
        export(&ch, request(vec![span(trace, 1, start)]))
            .await
            .unwrap();
        let odd = raw_request(trace, 2, start + 10, &extra, resource.as_deref());
        let accepted = match export(&ch, odd).await {
            Ok(reply) => rejected(&reply) == 0,
            Err(e) if e.code() == Code::InvalidArgument => false,
            Err(e) => panic!("{what}: unexpected status {e}"),
        };
        let r = http(api, &format!("/api/v1/traces/{}", hex(&[trace; 16])), None).await;
        let spans = serde_json::from_slice::<Value>(&r.body)
            .ok()
            .and_then(|j| j["spans"].as_array().map(Vec::len));
        let want = if accepted { 2 } else { 1 };
        if r.status != 200 || spans != Some(want) {
            failures.push(format!(
                "{what}: acknowledged={accepted}, GET -> {} {}",
                r.status,
                String::from_utf8_lossy(&r.body)
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    server.shutdown().await.unwrap();
}

/// The protobuf form of an acknowledged trace must be a valid OTLP message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_protobuf_view_of_an_accepted_trace_is_valid_otlp() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let odd = raw_request(
        0x50,
        1,
        now_ns(),
        &attr("llm.output", &ld(1, &[0xed, 0xa0, 0xbd])),
        None,
    );
    let reply = export(&ch, odd)
        .await
        .expect("the export of a string that is not UTF-8 must be accepted");
    assert_eq!(rejected(&reply), 0, "the span must be stored, not skipped");
    let r = http(
        server.api_addr(),
        &format!("/api/v1/traces/{}", hex(&[0x50; 16])),
        Some("application/x-protobuf"),
    )
    .await;
    assert_eq!(r.status, 200);
    if let Err(e) = ExportTraceServiceRequest::decode(r.body.as_slice()) {
        panic!("the server acknowledged the request, but its protobuf view does not decode: {e}");
    }
    server.shutdown().await.unwrap();
}

/// OTLP times are unsigned 64-bit. The JSON view must not turn a large one
/// negative (the protobuf view keeps it, and event times are shown unsigned).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn span_times_above_i64_max_are_shown_as_sent() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    let mut s = span(0x51, 1, 0);
    s.start_time_unix_nano = u64::MAX - 10;
    s.end_time_unix_nano = u64::MAX - 5;
    export(&ch, request(vec![s])).await.unwrap();
    let r = http(
        server.api_addr(),
        &format!("/api/v1/traces/{}", hex(&[0x51; 16])),
        None,
    )
    .await;
    let j: Value = serde_json::from_slice(&r.body).unwrap();
    let got = &j["spans"][0];
    assert_eq!(
        (&got["start_ns"], &got["end_ns"]),
        (&Value::from(u64::MAX - 10), &Value::from(u64::MAX - 5)),
        "{got}"
    );
    server.shutdown().await.unwrap();
}

/// An export larger than the limit can never succeed, so its status must not
/// be one that OTLP senders retry (CANCELLED, DEADLINE_EXCEEDED, ABORTED,
/// OUT_OF_RANGE, UNAVAILABLE, DATA_LOSS).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_export_is_not_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    // One unknown length-delimited field (15) that fills the message: valid
    // protobuf, one byte over the limit. The zeroed pages are not touched.
    let total = MAX_MESSAGE + 1;
    let header = [vec![0x7a], varint((total - 5) as u64)].concat();
    assert_eq!(header.len(), 5);
    let mut body = vec![0u8; total];
    body[..5].copy_from_slice(&header);
    let e = export(&ch, body).await.unwrap_err();
    let retried = [
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unavailable,
        Code::DataLoss,
    ];
    assert!(
        !retried.contains(&e.code()),
        "a request over the limit gets {:?} ({}), which OTLP senders retry",
        e.code(),
        e.message()
    );
    server.shutdown().await.unwrap();
}

/// Every error of the API is JSON, says the README, including a trace id
/// whose percent-encoding is not UTF-8.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trace_id_that_is_not_utf8_gets_a_json_error() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let r = http(server.api_addr(), "/api/v1/traces/%ff%fe", None).await;
    assert_eq!(r.status, 400);
    let body: Option<Value> = serde_json::from_slice(&r.body).ok();
    assert!(
        r.headers.contains("content-type: application/json")
            && body.is_some_and(|b| b["error"].is_string()),
        "not a JSON error: {}\n{}",
        r.headers,
        String::from_utf8_lossy(&r.body)
    );
    server.shutdown().await.unwrap();
}

/// `q=0` means "not acceptable". A client that says so for protobuf and asks
/// for JSON must get JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accept_with_q_zero_does_not_select_protobuf() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    export(&ch, request(vec![span(0x52, 1, now_ns())]))
        .await
        .unwrap();
    let r = http(
        server.api_addr(),
        &format!("/api/v1/traces/{}", hex(&[0x52; 16])),
        Some("application/x-protobuf;q=0, application/json"),
    )
    .await;
    assert_eq!(r.status, 200);
    assert!(
        r.headers.contains("content-type: application/json"),
        "{}",
        r.headers
    );
    server.shutdown().await.unwrap();
}

/// `%+1` is not percent-encoding; the API says it rejects invalid encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_percent_sign_followed_by_a_plus_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let r = http(server.api_addr(), "/api/v1/traces?service=%+1", None).await;
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    server.shutdown().await.unwrap();
}

/// The API binds to loopback because it has no authentication. A web page the
/// user visits can still reach it by rebinding its own host name to
/// 127.0.0.1 and reading the answers as same-origin. A request that names a
/// host other than the one the server listens on must be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_naming_a_foreign_host_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = started(dir.path()).await;
    let ch = channel(server.otlp_grpc_addr()).await;
    export(&ch, request(vec![span(0x53, 1, now_ns())]))
        .await
        .unwrap();
    let api = server.api_addr();
    let r = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let target = format!("/api/v1/traces/{}", hex(&[0x53; 16]));
        write!(
            s,
            "GET {target} HTTP/1.1\r\nHost: evil.example:{}\r\nConnection: close\r\n\r\n",
            api.port()
        )
        .unwrap();
        let mut raw = Vec::new();
        s.read_to_end(&mut raw).unwrap();
        String::from_utf8_lossy(&raw).into_owned()
    })
    .await
    .unwrap();
    assert!(
        !r.starts_with("HTTP/1.1 200"),
        "a page on another host name read a trace:\n{r}"
    );
    server.shutdown().await.unwrap();
}
