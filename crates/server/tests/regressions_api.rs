//! The query API: what clients that do not read can do to the others, and
//! errors that the router never sees.

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::{Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::Duration;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

fn opts(dir: &Path) -> ServerOptions {
    let mut o = ServerOptions::new(dir);
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o
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

/// A trace of `spans` spans that share one resource of about 60 KB. The
/// resource is stored once, but the JSON of the trace repeats it in every
/// span, so the JSON is tens of megabytes.
fn wide_trace(spans: u16) -> Vec<u8> {
    let resource = Resource {
        attributes: vec![KeyValue {
            key: "pad".into(),
            value: Some(AnyValue {
                value: Some(V::StringValue("a".repeat(60_000))),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let spans: Vec<Span> = (0..spans)
        .map(|i| Span {
            trace_id: vec![0x42; 16],
            span_id: (u64::from(i) + 1).to_be_bytes().to_vec(),
            name: "s".into(),
            start_time_unix_nano: 1_000 + u64::from(i),
            end_time_unix_nano: 2_000,
            ..Default::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn send(api: SocketAddr, target: &str) -> TcpStream {
    let mut s = TcpStream::connect(api).unwrap();
    write!(
        s,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    s
}

/// The README: the API "runs at most 64 requests at once (more wait)", and a
/// streamed response keeps its slot while the client reads it. Clients that
/// ask for a trace and then never read keep their slots for as long as they
/// keep the connection open, and `/healthz` waits behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_that_do_not_read_cannot_stop_the_api() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let api = server.api_addr();
    ingest(server.otlp_grpc_addr(), wide_trace(250)).await;
    assert!(healthz(api).await.starts_with("HTTP/1.1 200"));

    let id = "42".repeat(16);
    let target = format!("/api/v1/traces/{id}");
    let readers = tokio::task::spawn_blocking(move || {
        (0..64).map(|_| send(api, &target)).collect::<Vec<_>>()
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let answer = healthz(api).await;
    // Control: with the readers gone the same request is answered, so it
    // was their slots it waited for.
    drop(readers);
    let after = healthz(api).await;
    assert!(after.starts_with("HTTP/1.1 200"), "{after:?}");
    assert!(
        answer.starts_with("HTTP/1.1 200"),
        "GET /healthz got no answer within 5 s while 64 clients that asked for a trace did \
         not read (got {answer:?})"
    );
    server.shutdown().await.unwrap();
}

/// `GET /healthz`, with 5 s to answer; what came back, empty if nothing.
async fn healthz(api: SocketAddr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut s = send(api, "/healthz");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut raw = Vec::new();
        let _ = s.read_to_end(&mut raw);
        String::from_utf8_lossy(&raw).into_owned()
    })
    .await
    .unwrap()
}

/// The README says that replies the HTTP library makes itself, for a request
/// it cannot parse, are the one exception to JSON errors. What they are is
/// pinned here: status 400, and no JSON body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_request_gets_the_http_librarys_own_400() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let api = server.api_addr();
    let raw = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nno colon here\r\n\r\n")
            .unwrap();
        let mut raw = Vec::new();
        let _ = s.read_to_end(&mut raw);
        String::from_utf8_lossy(&raw).into_owned()
    })
    .await
    .unwrap();
    assert!(raw.starts_with("HTTP/1.1 400"), "{raw:?}");
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("");
    assert!(
        !raw.to_ascii_lowercase().contains("application/json") && !body.contains("error"),
        "{raw:?}"
    );
    server.shutdown().await.unwrap();
}

/// A client that asks for a large trace and never reads it holds its slot
/// only until the write timeout: then its connection is closed, the task that
/// renders the trace ends, and the slot is free. Meanwhile `/healthz` and
/// `/api/v1/stats` do not wait for slots, and a request that needs one does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_does_not_read_is_cut_off_and_frees_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.api_write_timeout = Duration::from_secs(3);
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();
    // About 30 MB of JSON, to clients whose receive buffers are tiny: the
    // kernel buffers a few MB, not all of it.
    ingest(server.otlp_grpc_addr(), wide_trace(500)).await;

    let id = "42".repeat(16);
    let target = format!("/api/v1/traces/{id}");
    let mut readers = Vec::new();
    for _ in 0..64 {
        let sock = tokio::net::TcpSocket::new_v4().unwrap();
        sock.set_recv_buffer_size(8 * 1024).unwrap();
        let mut s = sock.connect(api).await.unwrap().into_std().unwrap();
        s.set_nonblocking(false).unwrap();
        write!(
            s,
            "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        readers.push(s);
    }
    tokio::time::sleep(Duration::from_millis(800)).await;

    // The counters never wait for a slot.
    for path in ["/healthz", "/api/v1/stats"] {
        let answer = tokio::task::spawn_blocking(move || {
            let mut s = send(api, path);
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut raw = Vec::new();
            let _ = s.read_to_end(&mut raw);
            String::from_utf8_lossy(&raw).into_owned()
        })
        .await
        .unwrap();
        assert!(answer.starts_with("HTTP/1.1 200"), "{path}: {answer:?}");
    }

    // A query waits for a slot, and gets one when the readers are cut off.
    let began = std::time::Instant::now();
    let waiter = tokio::task::spawn_blocking(move || {
        let mut s = send(api, "/api/v1/tokens?from=0&to=9000000000000000000");
        s.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut one = [0u8; 1];
        assert!(
            s.read(&mut one).is_err(),
            "answered while every slot was held"
        );
        s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        let mut raw = Vec::new();
        let _ = s.read_to_end(&mut raw);
        String::from_utf8_lossy(&raw).into_owned()
    })
    .await
    .unwrap();
    assert!(waiter.starts_with("HTTP/1.1 200"), "{waiter:?}");
    assert!(
        began.elapsed() > Duration::from_secs(1),
        "{:?}",
        began.elapsed()
    );

    // The readers' timers all started within moments of each other. Once
    // they have run out, the server has closed their connections: reading one
    // now ends, with less than the whole trace.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut first = readers.remove(0);
    let (got, tail) = tokio::task::spawn_blocking(move || {
        first
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut n = 0;
        let mut tail = Vec::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            match first.read(&mut buf) {
                Ok(0) | Err(_) => break (n, tail),
                Ok(k) => {
                    n += k;
                    tail.extend_from_slice(&buf[..k]);
                    tail.drain(..tail.len().saturating_sub(16));
                }
            }
        }
    })
    .await
    .unwrap();
    // A response that was sent whole ends with the last chunk of the chunked
    // encoding.
    assert!(
        !tail.ends_with(b"0\r\n\r\n"),
        "read the whole response ({got} bytes) of a client that did not read"
    );
    drop(readers);
    server.shutdown().await.unwrap();
}
