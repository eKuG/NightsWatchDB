//! A trace with a span of many megabytes is sent as a stream: the heap of the
//! whole process, counted by an allocator, does not grow by more than the
//! rows the engine hands over, and a client that goes away mid-response lets
//! them go again.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::{Server, ServerOptions};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOC: Counting = Counting;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const SIZE: usize = 16 << 20;

fn big_request() -> Vec<u8> {
    let span = Span {
        trace_id: vec![0x42; 16],
        span_id: vec![1; 8],
        name: "big".into(),
        start_time_unix_nano: 1_000,
        end_time_unix_nano: 2_000,
        attributes: vec![KeyValue {
            key: "blob".into(),
            value: Some(AnyValue {
                value: Some(V::StringValue("a".repeat(SIZE))),
            }),
            ..Default::default()
        }],
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

async fn ingest(addr: SocketAddr, payload: Vec<u8>) {
    let io = TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (mut send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut m = vec![0u8];
    m.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    m.extend_from_slice(&payload);
    drop(payload);
    let req = Request::post(format!("http://localhost{EXPORT}"))
        .header("content-type", "application/grpc")
        .body(Full::new(Bytes::from(m)))
        .unwrap();
    send.ready().await.unwrap();
    let resp = send.send_request(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    let trailers = body.collect().await.unwrap().trailers().cloned();
    let status = |h: &http::HeaderMap| h.get("grpc-status").map(|v| v.to_str().unwrap().to_owned());
    let got = status(&parts.headers).or_else(|| trailers.as_ref().and_then(status));
    assert_eq!(got.as_deref(), Some("0"));
}

fn get(api: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(api).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let id = "42".repeat(16);
    write!(
        s,
        "GET /api/v1/traces/{id} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_trace_is_streamed_and_a_client_that_leaves_frees_its_task() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();
    ingest(server.otlp_grpc_addr(), big_request()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // What the engine needs to hand over the rows of the trace: the cost no
    // response format can avoid.
    let id: nwdb_core::TraceId = "42".repeat(16).parse().unwrap();
    let base = live();
    reset_peak();
    let rows = server.engine().get_trace(id).unwrap().unwrap();
    let rows_cost = peak() - base;
    drop(rows);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The whole response, read in small pieces and thrown away.
    let base = live();
    reset_peak();
    let total = tokio::task::spawn_blocking(move || {
        let mut s = get(api);
        let (mut buf, mut total) = (vec![0u8; 64 * 1024], 0usize);
        let mut head = Vec::new();
        loop {
            let n = s.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            if head.len() < 64 {
                head.extend_from_slice(&buf[..n.min(64)]);
            }
            total += n;
        }
        assert!(
            head.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&head)
        );
        total
    })
    .await
    .unwrap();
    let grown = peak() - base;
    eprintln!(
        "response {total} bytes; peak heap growth {grown} bytes, of which the engine's rows \
         {rows_cost} (span of {SIZE})"
    );
    assert!(total > SIZE, "{total}");
    // Building the JSON as one value or one buffer would add at least one more
    // copy of the span.
    assert!(
        grown < rows_cost + (SIZE / 4) as isize,
        "streaming a {SIZE} byte span grew the heap by {grown} bytes; the rows alone need {rows_cost}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(live() < base + (1 << 20), "memory was not released");

    // A client that reads a little and leaves. While it is connected the
    // task holds the rows; when it is gone the task ends and drops them.
    let mut s = tokio::task::spawn_blocking(move || {
        let mut s = get(api);
        let mut buf = vec![0u8; 100 * 1024];
        s.read_exact(&mut buf).unwrap();
        s
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        live() > base + SIZE as isize / 2,
        "the task did not wait for the client"
    );
    let _ = s.flush();
    drop(s);
    let deadline = Instant::now() + Duration::from_secs(15);
    while live() > base + (1 << 20) {
        assert!(
            Instant::now() < deadline,
            "the task kept its rows after the client left: {} bytes above the base",
            live() - base
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server.shutdown().await.unwrap();
}
