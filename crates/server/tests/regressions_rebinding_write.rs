//! A web page cannot read or write through DNS rebinding, whatever address the
//! API listens on. The container image listens on all interfaces and passes no
//! allowed hosts; the `Host` check is on there as everywhere.

mod common;

use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::net::SocketAddr;

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

/// A server on all interfaces (what the container image does) with one trace,
/// and the loopback address its published port is reached on.
async fn start(allowed: &[&str]) -> (Server, SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    o.api_addr = "0.0.0.0:0".parse().unwrap();
    o.api_allowed_hosts = allowed.iter().map(|s| s.to_string()).collect();
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    let api = SocketAddr::from(([127, 0, 0, 1], server.api_addr().port()));
    (server, api, dir)
}

/// The POST that a rebound page sends: same origin, so it needs no preflight.
async fn rebound_post(api: SocketAddr, host: &str) -> common::Resp {
    let body =
        format!(r#"{{"annotations":[{{"trace_id":"{TRACE}","name":"planted","value":1}}]}}"#);
    let headers = format!(
        "Host: {host}\r\nOrigin: http://{host}\r\nSec-Fetch-Site: same-origin\r\n\
         Sec-Fetch-Mode: cors\r\nContent-Type: application/json\r\n"
    );
    common::http_body(
        api,
        "POST",
        "/api/v1/annotations",
        &headers,
        Some(body.into_bytes()),
        None,
    )
    .await
}

async fn get(api: SocketAddr, target: &str, host: &str) -> common::Resp {
    common::http(api, "GET", target, &format!("Host: {host}\r\n")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebound_page_cannot_write_when_the_api_listens_on_all_interfaces() {
    let (server, api, _dir) = start(&[]).await;
    let host = format!("rebind.example:{}", api.port());

    let r = rebound_post(api, &host).await;
    assert_eq!(r.status, 421, "{}", String::from_utf8_lossy(&r.body));
    assert!(r.error().contains("--api-allowed-hosts"), "{}", r.error());
    // Nothing was written.
    let view = get(api, &format!("/api/v1/traces/{TRACE}"), "localhost").await;
    assert_eq!(view.status, 200);
    assert!(view.json().get("annotations").is_none(), "{}", view.json());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebound_page_cannot_read_when_the_api_listens_on_all_interfaces() {
    let (server, api, _dir) = start(&[]).await;
    let host = format!("rebind.example:{}", api.port());

    // Every route, the two that answer without a slot included.
    for target in [
        "/api/v1/traces".to_string(),
        format!("/api/v1/traces/{TRACE}"),
        "/api/v1/tokens".to_string(),
        "/api/v1/search?q=op".to_string(),
        "/api/v1/stats".to_string(),
        "/healthz".to_string(),
    ] {
        let r = get(api, &target, &host).await;
        assert_eq!(r.status, 421, "{target}");
        assert!(r.error().contains("--api-allowed-hosts"), "{target}");
    }
    // No Host at all (HTTP/1.0 style) is refused too.
    let r = common::http(api, "GET", "/api/v1/traces", "Host:\r\n").await;
    assert_eq!(r.status, 421);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn address_literals_and_localhost_are_answered_on_all_interfaces() {
    let (server, api, _dir) = start(&[]).await;
    let port = api.port();

    for host in [
        format!("127.0.0.1:{port}"),
        "127.0.0.1".to_string(),
        format!("localhost:{port}"),
        "LOCALHOST.".to_string(),
        // An address of this machine other than loopback: a literal cannot
        // be re-pointed, so it needs no configuration.
        format!("10.20.30.40:{port}"),
        format!("[::1]:{port}"),
        "[2001:db8::1]".to_string(),
    ] {
        for target in ["/api/v1/traces", "/healthz", "/api/v1/stats"] {
            let r = get(api, target, &host).await;
            assert_eq!(r.status, 200, "{host} {target}");
        }
        // The same-origin POST is accepted from such a host: it is the
        // user's own client.
        let r = rebound_post(api, &host).await;
        assert_eq!(
            r.status,
            200,
            "{host}: {}",
            String::from_utf8_lossy(&r.body)
        );
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listed_name_is_answered_and_others_are_not() {
    let (server, api, _dir) = start(&["Traces.Internal"]).await;
    let port = api.port();

    for host in [
        format!("traces.internal:{port}"),
        "TRACES.internal.".to_string(),
    ] {
        assert_eq!(
            get(api, "/api/v1/traces", &host).await.status,
            200,
            "{host}"
        );
    }
    for host in [
        format!("rebind.example:{port}"),
        format!("other.traces.internal:{port}"),
    ] {
        assert_eq!(
            get(api, "/api/v1/traces", &host).await.status,
            421,
            "{host}"
        );
    }
    server.shutdown().await.unwrap();
}
