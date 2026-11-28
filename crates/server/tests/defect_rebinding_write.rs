//! A web page writes annotations through DNS rebinding when the API listens
//! on all interfaces with no allowed hosts, which is how the container image
//! runs it.

mod common;

use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TRACE: &str = "07070707070707070707070707070707";

fn one_trace() -> Vec<u8> {
    let start = common::now_ns() - 600_000_000_000;
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![7; 16],
                    span_id: vec![1; 8],
                    name: "op".into(),
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 1_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// Sends `raw` on a new connection and returns the status and the body.
fn exchange(addr: SocketAddr, raw: &[u8]) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.write_all(raw).unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text.split(' ').nth(1).unwrap().parse().unwrap();
    let body = text.split_once("\r\n\r\n").unwrap().1.to_owned();
    (status, body)
}

/// Defect: the container image starts the server with
/// `--api-addr 0.0.0.0:4320` and no `--api-allowed-hosts` (Dockerfile
/// ENTRYPOINT), and `HostCheck::new` (src/api.rs) switches the `Host` check
/// off for a bind address that is not loopback when no names are configured.
/// The README (Security, Docker) tells the user that publishing the port as
/// `-p 127.0.0.1:4320:4320` protects it. It does not protect it from the
/// user's own browser: a page at `http://rebind.example:4320` whose name is
/// then pointed at 127.0.0.1 sends a same-origin `fetch` with
/// `Content-Type: application/json`. A same-origin request needs no
/// preflight, its `Origin` equals its `Host`, its `Sec-Fetch-Site` is
/// `same-origin`, so `JsonBody` (src/api/body.rs) lets it through and the
/// annotation is written. (The same page can already read every trace; with
/// the write endpoint it can now also change and delete annotations.)
///
/// Trigger: bind the API to 0.0.0.0 with no allowed hosts and send the POST
/// the rebound page sends.
///
/// Expected: a write whose `Host` is neither `localhost`, a loopback address,
/// the bound address nor an allowed host is refused (421 like a read, or
/// 403), and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebound_page_cannot_write_when_the_api_listens_on_all_interfaces() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    // What the container image does.
    o.api_addr = "0.0.0.0:0".parse().unwrap();
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    // The published port, reached on the loopback address.
    let api = SocketAddr::from(([127, 0, 0, 1], server.api_addr().port()));
    let port = api.port();

    let body =
        format!(r#"{{"annotations":[{{"trace_id":"{TRACE}","name":"planted","value":1}}]}}"#);
    let raw = format!(
        "POST /api/v1/annotations HTTP/1.1\r\n\
         Host: rebind.example:{port}\r\n\
         Origin: http://rebind.example:{port}\r\n\
         Sec-Fetch-Site: same-origin\r\n\
         Sec-Fetch-Mode: cors\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let (status, reply) = tokio::task::spawn_blocking(move || exchange(api, raw.as_bytes()))
        .await
        .unwrap();
    let view = common::http(api, "GET", &format!("/api/v1/traces/{TRACE}"), "").await;
    let written = view.json().get("annotations").cloned();
    server.shutdown().await.unwrap();

    assert!(
        matches!(status, 403 | 421) && written.is_none(),
        "a page on rebind.example got {status} ({reply}) and the trace now shows {written:?}"
    );
}
