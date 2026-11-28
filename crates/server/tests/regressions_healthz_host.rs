//! `/healthz` answers for any `Host` header, or none: it returns `ok` or the
//! fatal error text and nothing else, and a health check (HAProxy's default
//! one is HTTP/1.0 without `Host`; a proxy sends the name of its upstream) must
//! not get a 421. Every other path, `/api/v1/stats` included, keeps the check.

mod common;

use nwdb_server::Server;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// Sends `raw` and returns what the server sends until it closes.
fn exchange(api: SocketAddr, raw: &str) -> String {
    let mut s = TcpStream::connect(api).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

fn status(reply: &str) -> &str {
    reply.split(' ').nth(1).unwrap_or(reply)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthz_answers_for_any_host_or_none_and_the_rest_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    let replies = tokio::task::spawn_blocking(move || {
        let get = |path: &str, host: Option<&str>, version: &str| {
            let host = host.map(|h| format!("Host: {h}\r\n")).unwrap_or_default();
            exchange(
                api,
                &format!("GET {path} {version}\r\n{host}Connection: close\r\n\r\n"),
            )
        };
        [
            // The name of a proxy's upstream, an ingress host, a name that is
            // not listed, a malformed value.
            get("/healthz", Some("evil.example"), "HTTP/1.1"),
            get(
                "/healthz",
                Some("backend.svc.cluster.local:4320"),
                "HTTP/1.1",
            ),
            get("/healthz", Some("not a host"), "HTTP/1.1"),
            // No `Host` at all: HTTP/1.0 as HAProxy sends it, and HTTP/1.1.
            get("/healthz", None, "HTTP/1.0"),
            get("/healthz", None, "HTTP/1.1"),
            // The same requests to a path that stays behind the check.
            get("/api/v1/stats", Some("evil.example"), "HTTP/1.1"),
            get("/api/v1/stats", None, "HTTP/1.0"),
            get("/api/v1/traces", Some("evil.example"), "HTTP/1.1"),
            get("/nowhere", Some("evil.example"), "HTTP/1.1"),
            // And the allowed ones still work.
            get("/api/v1/stats", Some("localhost:4320"), "HTTP/1.1"),
        ]
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    for r in &replies[..5] {
        assert_eq!(status(r), "200", "{r}");
        assert!(r.ends_with("ok"), "{r}");
    }
    for r in &replies[5..9] {
        assert_eq!(status(r), "421", "{r}");
    }
    assert_eq!(status(&replies[9]), "200", "{}", replies[9]);
}
