//! A sender refused with 413 keeps its connection, against the README.

mod common;

use nwdb_server::Server;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Defect: the README (The HTTP API, Write requests) says "A sender that has
/// to be cut off for 408 or 413 has its connection closed after the
/// response." Nothing in `JsonBody` (src/api/body.rs) asks for that: no
/// `Connection: close` is set, and closing is left to hyper, which first
/// tries to read the rest of the dropped body and keeps the connection when
/// that rest is already in its buffer. A chunked body that passes the limit
/// in its last chunk leaves only the terminating chunk unread, so the
/// connection stays open and the next request on it is served. The framing
/// stays right (the refused body is never read as a request), so this is a
/// broken promise of the README, not a way in.
///
/// Trigger: with the default 1 MiB limit, a chunked POST of one 1 MiB chunk,
/// one 1-byte chunk and the end, followed on the same connection by
/// `GET /healthz`.
///
/// Expected (as documented): the 413 is the last response on the connection;
/// the pipelined request gets no answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_of_a_413_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    let out = tokio::task::spawn_blocking(move || {
        let mut raw = b"POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
            Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n100000\r\n"
            .to_vec();
        raw.extend(std::iter::repeat_n(b' ', 1 << 20));
        raw.extend_from_slice(b"\r\n1\r\n \r\n0\r\n\r\n");
        raw.extend_from_slice(
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let _ = s.write_all(&raw);
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    let statuses: Vec<&str> = out
        .match_indices("HTTP/1.1 ")
        .map(|(i, _)| &out[i + 9..i + 12])
        .collect();
    assert!(
        statuses.first().is_none_or(|s| *s == "413") && statuses.len() <= 1,
        "responses on the connection after a 413: {statuses:?}"
    );
}
