//! A sender refused with 413 or 408 has its connection closed after the
//! response, as the README says. A 403 or a 415 does not close it.

mod common;

use nwdb_server::Server;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const HEALTHZ: &[u8] = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

/// Sends `raw` on a new connection and returns everything the server sends
/// until it closes the connection. A server that does not close fails the
/// read timeout.
fn exchange(api: SocketAddr, raw: &[u8]) -> String {
    let mut s = TcpStream::connect(api).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let _ = s.write_all(raw);
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

fn statuses(out: &str) -> Vec<&str> {
    out.match_indices("HTTP/1.1 ")
        .map(|(i, _)| &out[i + 9..i + 12])
        .collect()
}

/// A chunked POST of one 1 MiB chunk, one 1-byte chunk and the end is over
/// the default 1 MiB limit only in its last chunk, so the rest of it that
/// hyper still has to read is just the terminating chunk, which used to be
/// enough for it to keep the connection. The 413 carries `Connection: close`
/// and is the last response on the connection: the request pipelined behind
/// it gets no answer.
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
        raw.extend_from_slice(HEALTHZ);
        exchange(api, &raw)
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    assert_eq!(statuses(&out), ["413"], "{out}");
    assert!(out.to_lowercase().contains("connection: close"), "{out}");
}

/// The same when the declared length is over the limit and the body is never
/// sent, and for a body that is not received in time (408).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_of_a_413_by_length_or_a_408_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    o.api_body_timeout = Duration::from_millis(300);
    let server = Server::start(o).await.unwrap();
    let api = server.api_addr();

    let (by_length, timed_out) = tokio::task::spawn_blocking(move || {
        let head = |len: usize| {
            format!(
                "POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
                 Content-Type: application/json\r\nContent-Length: {len}\r\n\r\n"
            )
        };
        let mut raw = head((1 << 20) + 1).into_bytes();
        raw.extend_from_slice(HEALTHZ);
        let by_length = exchange(api, &raw);
        // Ten bytes announced, three sent, then silence.
        let mut raw = head(10).into_bytes();
        raw.extend_from_slice(b"{\"a");
        (by_length, exchange(api, &raw))
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    assert_eq!(statuses(&by_length), ["413"], "{by_length}");
    assert!(by_length.to_lowercase().contains("connection: close"));
    assert_eq!(statuses(&timed_out), ["408"], "{timed_out}");
    // The HTTP library closes the connection of a 408 on its own whatever the
    // response says, so ending the read is no proof; the only thing here that
    // fails without the fix is this header assertion.
    assert!(
        timed_out.to_lowercase().contains("connection: close"),
        "{timed_out}"
    );
}

/// What the README says of 403 and 415: the connection is not closed by the
/// server, so a client whose body was sent whole goes on with the next
/// request on it. (The body is not read by the handler; hyper reads what is
/// already in its buffer and carries on. When the rest of a large body has
/// not arrived, hyper closes.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_403_or_a_415_with_a_small_body_keeps_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    let out = tokio::task::spawn_blocking(move || {
        let post = |headers: &str| {
            format!(
                "POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n{headers}\
                 Content-Length: 2\r\n\r\n{{}}"
            )
        };
        let mut raw = post("Content-Type: text/plain\r\n").into_bytes();
        raw.extend_from_slice(
            post("Content-Type: application/json\r\nOrigin: http://evil.example\r\n").as_bytes(),
        );
        raw.extend_from_slice(HEALTHZ);
        exchange(api, &raw)
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    assert_eq!(statuses(&out), ["415", "403", "200"], "{out}");
}

/// A 415 or a 403 whose body has not arrived: the connection ends after the
/// response (hyper stops reading a body the handler did not read, when what
/// is left is not already in its buffer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_403_or_a_415_whose_body_is_not_there_ends_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    let out = tokio::task::spawn_blocking(move || {
        let raw = b"POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
            Content-Type: text/plain\r\nContent-Length: 100000\r\n\r\n"
            .to_vec();
        let started = std::time::Instant::now();
        (exchange(api, &raw), started.elapsed())
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    let (out, took) = out;
    assert_eq!(statuses(&out), ["415"], "{out}");
    assert!(took < Duration::from_secs(5), "the connection stayed open");
}
