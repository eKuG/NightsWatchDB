//! What the server's integration tests repeat: server options, a hand-written
//! HTTP/1.1 client, an HTTP/2 client with a streamed request body, and the
//! raw HTTP/2 frame helpers. Each test file uses a part of it.
#![allow(dead_code)]

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::ServerOptions;
use serde_json::Value;
use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

/// Options for a server on ephemeral ports.
pub fn opts(dir: &Path) -> ServerOptions {
    let mut o = ServerOptions::new(dir);
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o
}

pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ---- HTTP/1.1 client ----

/// The body of a chunked response without the chunk framing.
pub fn dechunk(mut raw: &[u8]) -> Vec<u8> {
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

pub struct Resp {
    pub status: u16,
    /// The response head, in lower case.
    pub headers: String,
    pub body: Vec<u8>,
}

impl Resp {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }
}

impl Resp {
    /// The `error` text of a JSON error reply.
    pub fn error(&self) -> String {
        self.json()["error"].as_str().unwrap().to_owned()
    }
}

/// One request with `Connection: close`. `headers` are the lines sent after
/// `Host: localhost` (unless they name a `Host` themselves), each ending in
/// `\r\n`.
pub fn http_blocking(addr: SocketAddr, method: &str, target: &str, headers: &str) -> Resp {
    http_body_blocking(addr, method, target, headers, None, None)
}

/// [`http_blocking`] with a body. A body is sent with a `Content-Length` of
/// its own length, or of `declared` when that is given, so that a test can
/// announce more than it sends. A server that refuses the request may close
/// before it has read all of the body; that is not an error here.
pub fn http_body_blocking(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &str,
    body: Option<&[u8]>,
    declared: Option<usize>,
) -> Resp {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut head = format!("{method} {target} HTTP/1.1\r\n");
    if !headers.to_ascii_lowercase().contains("host:") {
        head.push_str("Host: localhost\r\n");
    }
    head.push_str(headers);
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Length: {}\r\n",
            declared.unwrap_or(body.len())
        ));
    }
    head.push_str("Connection: close\r\n\r\n");
    s.write_all(head.as_bytes()).unwrap();
    if let Some(body) = body {
        let _ = s.write_all(body);
    }
    let mut raw = Vec::new();
    let read = s.read_to_end(&mut raw);
    if body.is_none() {
        read.unwrap();
    }
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

pub async fn http(addr: SocketAddr, method: &str, target: &str, headers: &str) -> Resp {
    http_body(addr, method, target, headers, None, None).await
}

pub async fn http_body(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &str,
    body: Option<Vec<u8>>,
    declared: Option<usize>,
) -> Resp {
    let (method, target, headers) = (method.to_owned(), target.to_owned(), headers.to_owned());
    tokio::task::spawn_blocking(move || {
        http_body_blocking(addr, &method, &target, &headers, body.as_deref(), declared)
    })
    .await
    .unwrap()
}

// ---- HTTP/2 client ----

/// A request body fed from a channel: it ends when the sender is dropped.
pub struct Feed(pub mpsc::UnboundedReceiver<Bytes>);

impl Body for Feed {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx).map(|b| b.map(|b| Ok(Frame::data(b))))
    }
}

/// An HTTP/2 connection to `addr`; the request body type is the caller's.
pub async fn connect<B>(addr: SocketAddr) -> SendRequest<B>
where
    B: Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let io = TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    send
}

/// Sends `payload` as one gRPC message to the export method and checks that
/// the call ends with status 0. With `free_payload_early` the payload is
/// dropped as soon as it is framed, for a test that counts memory.
pub async fn ingest(addr: SocketAddr, payload: Vec<u8>, free_payload_early: bool) {
    let mut send = connect::<Full<Bytes>>(addr).await;
    let mut m = vec![0u8];
    m.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    m.extend_from_slice(&payload);
    if free_payload_early {
        drop(payload);
    }
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

// ---- raw HTTP/2 frames ----

pub fn frame(out: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
}

/// An HPACK string literal without Huffman coding.
pub fn hpack_str(out: &mut Vec<u8>, s: &str) {
    assert!(s.len() < 127);
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

/// Reads one frame and returns its type and flags.
pub fn read_frame(s: &mut TcpStream) -> (u8, u8) {
    let mut head = [0u8; 9];
    s.read_exact(&mut head).unwrap();
    let len = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload).unwrap();
    (head[3], head[4])
}
