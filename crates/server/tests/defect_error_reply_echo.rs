//! The 400 answer to a malformed annotation request repeats the request's
//! text, escaped, and is several times larger than the request.

mod common;

use nwdb_server::Server;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Defect: `parse` (src/api/annotate.rs) puts serde_json's message into the
/// error: `format!("not a valid annotation request: {e}")`. For a value of
/// the wrong type serde_json reports `invalid type: string {:?}`, the whole
/// string in Rust's debug form, and the reply then escapes that again as
/// JSON. A JSON string of U+0085 (two bytes each in UTF-8, allowed unescaped
/// in JSON) becomes `\u{85}` (six bytes) in the message and `\\u{85}` (seven
/// bytes) in the reply. A 1 MiB request therefore gets a reply of about
/// 3.5 MiB, and building it holds the message, its formatted copy and the
/// serialized reply at once: around ten times the body per request, beyond
/// the "limit x 64 slots" the README gives as the memory bound for write
/// requests, and the reply carries the client's text back to it.
///
/// Trigger: POST a body that is one JSON string of U+0085 characters, just
/// under the 1 MiB limit.
///
/// Expected: a 400 whose message is short; at the least, not larger than the
/// request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_400_does_not_repeat_the_body() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    let mut body = String::from("\"");
    while body.len() + 3 <= 1024 * 1024 {
        body.push('\u{85}');
    }
    body.push('"');
    let sent = body.len();
    let (status, reply_len) = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        write!(
            s,
            "POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {sent}\r\n\
             Connection: close\r\n\r\n"
        )
        .unwrap();
        s.write_all(body.as_bytes()).unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        let split = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let status: u16 = String::from_utf8_lossy(&out[..split])
            .split(' ')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        (status, out.len() - split - 4)
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    assert_eq!(status, 400);
    assert!(
        reply_len <= sent,
        "a request of {sent} bytes got a 400 of {reply_len} bytes"
    );
}
