//! The gRPC door at the protocol level: framing, compression claims, limits
//! and timeouts. The client is a plain HTTP/2 client so that a test controls
//! every byte of the call, which a gRPC library would not allow.

use bytes::Bytes;
use http::Request;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::{MAX_MESSAGE, Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::TcpStream as TokioTcp;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

const OK: u8 = 0;
const INVALID_ARGUMENT: u8 = 3;
const DEADLINE_EXCEEDED: u8 = 4;
const RESOURCE_EXHAUSTED: u8 = 8;
const UNIMPLEMENTED: u8 = 12;
const UNAVAILABLE: u8 = 14;

// ---- client ----

/// A request body fed from a channel: it ends when the sender is dropped.
struct Feed(mpsc::UnboundedReceiver<Bytes>);

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

async fn connect(addr: SocketAddr) -> SendRequest<Feed> {
    let io = TokioIo::new(TokioTcp::connect(addr).await.unwrap());
    let (send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    send
}

struct Reply {
    http: u16,
    code: u8,
    message: String,
    headers: http::HeaderMap,
    body: Bytes,
}

/// A call in progress. Dropping `tx` ends the request stream.
struct Call {
    tx: Option<mpsc::UnboundedSender<Bytes>>,
    reply: JoinHandle<Reply>,
}

impl Call {
    fn end(&mut self) {
        self.tx = None;
    }

    /// The reply, or a panic if there is none within `within`.
    async fn reply(self, within: Duration) -> Reply {
        tokio::time::timeout(within, self.reply)
            .await
            .expect("no reply in time")
            .unwrap()
    }
}

async fn call_with(
    send: &mut SendRequest<Feed>,
    path: &str,
    content_type: &str,
    encoding: Option<&str>,
    chunks: Vec<Bytes>,
) -> Call {
    let (tx, rx) = mpsc::unbounded_channel();
    for c in chunks {
        tx.send(c).unwrap();
    }
    let mut req = Request::post(format!("http://localhost{path}"))
        .header("content-type", content_type)
        .header("te", "trailers");
    if let Some(e) = encoding {
        req = req.header("grpc-encoding", e);
    }
    send.ready().await.unwrap();
    let fut = send.send_request(req.body(Feed(rx)).unwrap());
    let reply = tokio::spawn(async move {
        let resp = fut.await.unwrap();
        let (parts, mut body) = resp.into_parts();
        let mut data = Vec::new();
        let mut trailers = None;
        while let Some(frame) = body.frame().await {
            let frame = frame.unwrap();
            match frame.into_data() {
                Ok(d) => data.extend_from_slice(&d),
                Err(f) => trailers = f.into_trailers().ok(),
            }
        }
        let status = |h: &http::HeaderMap| {
            h.get("grpc-status")
                .map(|v| v.to_str().unwrap().parse::<u8>().unwrap())
        };
        let code = status(&parts.headers)
            .or_else(|| trailers.as_ref().and_then(status))
            .expect("no grpc-status");
        let message = parts
            .headers
            .get("grpc-message")
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default();
        Reply {
            http: parts.status.as_u16(),
            code,
            message,
            headers: parts.headers,
            body: Bytes::from(data),
        }
    });
    Call {
        tx: Some(tx),
        reply,
    }
}

/// An export call with the given encoding header and whole messages, ended.
async fn export(send: &mut SendRequest<Feed>, encoding: Option<&str>, chunks: Vec<Bytes>) -> Reply {
    let mut call = call_with(send, EXPORT, "application/grpc", encoding, chunks).await;
    call.end();
    call.reply(Duration::from_secs(20)).await
}

/// A gRPC message: flag, big-endian length, payload.
fn message(compressed: bool, payload: &[u8]) -> Bytes {
    let mut m = vec![u8::from(compressed)];
    m.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    m.extend_from_slice(payload);
    m.into()
}

// ---- payloads ----

fn varint(mut v: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
}

/// A valid request with no spans: one unknown field of `n` zero bytes.
fn padding(n: usize) -> Vec<u8> {
    let mut v = vec![0x7a];
    v.extend(varint(n));
    v.resize(v.len() + n, 0);
    v
}

fn one_span(trace: u8) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![trace; 16],
                    span_id: vec![1; 8],
                    name: "s".into(),
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 2_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

/// A zstd frame with no content size in its header: the encoder is not told
/// the size, and writes the header before it has seen the end of the input.
fn zstd_unsized(data: &[u8]) -> Vec<u8> {
    let mut e = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    for piece in data.chunks(1000) {
        e.write_all(piece).unwrap();
    }
    let frame = e.finish().unwrap();
    assert!(
        zstd::zstd_safe::get_frame_content_size(&frame)
            .unwrap()
            .is_none()
    );
    frame
}

fn set_gzip_claim(mut gz: Vec<u8>, claim: u32) -> Vec<u8> {
    let n = gz.len();
    gz[n - 4..].copy_from_slice(&claim.to_le_bytes());
    gz
}

/// A zstd frame whose header says the content is `claim` bytes, followed by
/// one run-length block of `run` bytes.
fn zstd_with_claim(claim: u32, run: usize) -> Vec<u8> {
    // Magic; single segment with a four byte content size.
    let mut f = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
    f.extend_from_slice(&claim.to_le_bytes());
    // Last block, run-length kind.
    let h = 1u32 | 1 << 1 | (run as u32) << 3;
    f.extend_from_slice(&h.to_le_bytes()[..3]);
    f.push(0);
    f
}

// ---- server fixtures ----

fn opts(dir: &Path) -> ServerOptions {
    let mut o = ServerOptions::new(dir);
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o
}

fn trace_status(api: SocketAddr, trace: u8) -> u16 {
    let mut s = TcpStream::connect(api).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let id: String = (0..16).map(|_| format!("{trace:02x}")).collect();
    write!(
        s,
        "GET /api/v1/traces/{id} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    String::from_utf8_lossy(&raw)
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

// ---- tests ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_good_export_is_http_200_with_an_empty_message_and_trailers() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let r = export(&mut c, None, vec![message(false, &one_span(0x11))]).await;
    assert_eq!((r.http, r.code), (200, OK), "{}", r.message);
    assert_eq!(r.headers["content-type"], "application/grpc");
    assert_eq!(r.headers["grpc-accept-encoding"], "identity,gzip,zstd");
    // One length-prefixed message of zero bytes.
    assert_eq!(r.body.as_ref(), [0, 0, 0, 0, 0]);
    assert_eq!(trace_status(server.api_addr(), 0x11), 200);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_message_is_refused_before_its_payload_is_sent() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    // Only the five header bytes are sent, and the stream stays open.
    let mut header = vec![0u8];
    header.extend_from_slice(&(MAX_MESSAGE as u32 + 1).to_be_bytes());
    let call = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![header.into()],
    )
    .await;
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!((r.http, r.code), (200, RESOURCE_EXHAUSTED), "{}", r.message);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_message_on_the_call_is_an_error_and_nothing_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;

    // In separate chunks, and in one.
    let (a, b) = (
        message(false, &one_span(0x21)),
        message(false, &one_span(0x22)),
    );
    let r = export(&mut c, None, vec![a.clone(), b.clone()]).await;
    assert_eq!(r.code, INVALID_ARGUMENT, "{}", r.message);
    assert!(r.message.contains("one%20message") || r.message.contains("one message"));
    let both = [a, b].concat();
    let r = export(&mut c, None, vec![both.into()]).await;
    assert_eq!(r.code, INVALID_ARGUMENT);
    // A single stray byte after the message is also refused.
    let mut stray = message(false, &one_span(0x23)).to_vec();
    stray.push(0);
    let r = export(&mut c, None, vec![stray.into()]).await;
    assert_eq!(r.code, INVALID_ARGUMENT);

    for t in [0x21, 0x22, 0x23] {
        assert_eq!(trace_status(server.api_addr(), t), 404, "{t:#x}");
    }
    assert_eq!(server.engine().stats().records_ingested, 0);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_that_stalls_is_timed_out() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.stall_timeout = Duration::from_millis(300);
    o.receive_timeout = Duration::from_millis(1_200);
    let server = Server::start(o).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;

    // Half of the header.
    let call = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![vec![0u8, 0].into()],
    )
    .await;
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, DEADLINE_EXCEEDED, "{}", r.message);

    // Part of the payload.
    let whole = message(false, &one_span(0x31));
    let call = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![whole.slice(..20)],
    )
    .await;
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, DEADLINE_EXCEEDED);

    // The whole message, but the stream is never ended.
    let call = call_with(&mut c, EXPORT, "application/grpc", None, vec![whole]).await;
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, DEADLINE_EXCEEDED);
    assert!(r.message.contains("stall_timeout"), "{}", r.message);
    assert_eq!(trace_status(server.api_addr(), 0x31), 404);

    // A sender that is never silent for the stall timeout but sends one byte
    // at a time runs into the deadline for the whole message.
    let mut header = vec![0u8];
    header.extend_from_slice(&1000u32.to_be_bytes());
    let call = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![header.into()],
    )
    .await;
    let tx = call.tx.clone().unwrap();
    let drip = tokio::spawn(async move {
        while tx.send(Bytes::from_static(b"\0")).is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let began = Instant::now();
    let r = call.reply(Duration::from_secs(10)).await;
    drip.abort();
    assert_eq!(r.code, DEADLINE_EXCEEDED, "{}", r.message);
    assert!(r.message.contains("receive_timeout"), "{}", r.message);
    assert!(
        began.elapsed() < Duration::from_secs(4),
        "{:?}",
        began.elapsed()
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_messages_that_do_not_match_their_claim_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;

    let raw = padding(1 << 20);
    let good_gz = gzip(&raw);
    let small = padding(1000);
    let small_gz = gzip(&small);
    let mut corrupt = good_gz.clone();
    let mid = corrupt.len() / 2;
    corrupt[mid] ^= 0xff;
    let mut trailing = small_gz.clone();
    trailing.extend_from_slice(&small_gz);

    let cases: Vec<(&str, &str, Vec<u8>, u8)> = vec![
        ("gzip", "good", good_gz.clone(), OK),
        (
            "gzip",
            "expands past its claim",
            set_gzip_claim(good_gz.clone(), 100),
            INVALID_ARGUMENT,
        ),
        (
            "gzip",
            "claims more than it holds",
            set_gzip_claim(small_gz.clone(), 5000),
            INVALID_ARGUMENT,
        ),
        (
            "gzip",
            "claim over the limit",
            set_gzip_claim(good_gz, 0xffff_ff00),
            RESOURCE_EXHAUSTED,
        ),
        ("gzip", "corrupt", corrupt, INVALID_ARGUMENT),
        ("gzip", "two members", trailing, INVALID_ARGUMENT),
        (
            "gzip",
            "too short for a trailer",
            vec![0x1f, 0x8b, 8],
            INVALID_ARGUMENT,
        ),
        ("zstd", "no content size", zstd_unsized(&raw), OK),
        (
            "zstd",
            "expands past its claim",
            zstd_with_claim(1000, 100_000),
            INVALID_ARGUMENT,
        ),
        (
            "zstd",
            "claim over the limit",
            {
                // Eight byte content size, 100 MiB.
                let mut f = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
                f.extend_from_slice(&(100u64 << 20).to_le_bytes());
                f
            },
            RESOURCE_EXHAUSTED,
        ),
        (
            "zstd",
            "garbage after the frame",
            {
                let mut f = zstd_unsized(&small);
                f.extend_from_slice(b"garbage");
                f
            },
            INVALID_ARGUMENT,
        ),
        (
            "zstd",
            "not a frame",
            b"definitely not zstd".to_vec(),
            INVALID_ARGUMENT,
        ),
    ];
    for (enc, what, payload, want) in cases {
        let r = export(&mut c, Some(enc), vec![message(true, &payload)]).await;
        assert_eq!(
            (r.http, r.code),
            (200, want),
            "{enc}: {what}: {}",
            r.message
        );
    }
    assert_eq!(server.engine().stats().records_ingested, 0);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encodings_paths_and_content_types_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let m = message(false, &one_span(0x41));

    let r = export(&mut c, Some("br"), vec![m.clone()]).await;
    assert_eq!(r.code, UNIMPLEMENTED);
    assert_eq!(r.headers["grpc-accept-encoding"], "identity,gzip,zstd");
    // `identity` is the same as no encoding; a compressed flag without an
    // encoding is not valid.
    let r = export(&mut c, Some("identity"), vec![m.clone()]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    let r = export(&mut c, None, vec![message(true, &one_span(0x42))]).await;
    assert_eq!(r.code, INVALID_ARGUMENT);
    // A flag byte that is neither 0 nor 1.
    let mut bad = m.to_vec();
    bad[0] = 2;
    assert_eq!(
        export(&mut c, None, vec![bad.into()]).await.code,
        INVALID_ARGUMENT
    );
    // No message at all, and a message shorter than its length.
    assert_eq!(export(&mut c, None, vec![]).await.code, INVALID_ARGUMENT);
    assert_eq!(
        export(&mut c, None, vec![m.slice(..m.len() - 1)])
            .await
            .code,
        INVALID_ARGUMENT
    );

    let mut call = call_with(
        &mut c,
        "/nothing/here",
        "application/grpc",
        None,
        vec![m.clone()],
    )
    .await;
    call.end();
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!((r.http, r.code), (200, UNIMPLEMENTED));
    let mut call = call_with(&mut c, EXPORT, "application/json", None, vec![m]).await;
    call.end();
    let r = call.reply(Duration::from_secs(5)).await;
    assert_eq!((r.http, r.code), (200, UNIMPLEMENTED));
    server.shutdown().await.unwrap();
}

/// More streams than the door allows on one connection do not run at once;
/// they wait, and are served when others end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_has_at_most_64_streams_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.stall_timeout = Duration::from_secs(60);
    let server = Server::start(o).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let m = message(false, &one_span(0x51));

    // 64 calls that are never ended hold every stream.
    let mut open = Vec::new();
    for _ in 0..64 {
        open.push(call_with(&mut c, EXPORT, "application/grpc", None, vec![m.clone()]).await);
    }
    let mut c2 = c.clone();
    let m2 = m.clone();
    let mut extra = tokio::spawn(async move { export(&mut c2, None, vec![m2]).await });
    let waited = tokio::time::timeout(Duration::from_millis(700), &mut extra).await;
    assert!(waited.is_err(), "the 65th stream was served at once");
    // Ending the others frees streams.
    for call in &mut open {
        call.end();
    }
    let r = tokio::time::timeout(Duration::from_secs(20), extra)
        .await
        .expect("the waiting stream was never served")
        .unwrap();
    assert_eq!(r.code, OK, "{}", r.message);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_returns_while_a_stalled_stream_is_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.drain_timeout = Duration::from_millis(500);
    o.stall_timeout = Duration::from_secs(600);
    let server = Server::start(o).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let whole = message(false, &one_span(0x61));
    let _stalled = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![whole.slice(..12)],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let began = Instant::now();
    tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("shutdown did not return")
        .unwrap();
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "{:?}",
        began.elapsed()
    );
}

/// A database whose log directory was deleted after data was flushed: the
/// binary refuses to start, says why and names the flag; with the flag it
/// starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_log_is_refused_unless_accepted() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let r = export(&mut c, None, vec![message(false, &one_span(0x81))]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    // A clean shutdown flushes the data into a part.
    server.shutdown().await.unwrap();
    let wal = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.file_name().unwrap().to_string_lossy().contains("wal"))
        .expect("a log directory");
    std::fs::remove_dir_all(&wal).unwrap();

    let bin = env!("CARGO_BIN_EXE_nightswatchdb");
    let args = [
        "--otlp-grpc-addr",
        "127.0.0.1:0",
        "--api-addr",
        "127.0.0.1:0",
        "--data-dir",
    ];
    let refused = Command::new(bin)
        .args(args)
        .arg(dir.path())
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(1));
    let err = String::from_utf8_lossy(&refused.stderr);
    assert!(err.contains("--accept-lost-log"), "{err}");
    assert!(err.contains("cannot start"), "{err}");

    let mut child = Command::new(bin)
        .args(args)
        .arg(dir.path())
        .arg("--accept-lost-log")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = std::io::BufReader::new(child.stderr.take().unwrap()).lines();
    loop {
        let line = lines.next().expect("exited before listening").unwrap();
        if line.starts_with("listening:") {
            break;
        }
    }
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    std::thread::spawn(move || lines.for_each(drop));
    assert_eq!(child.wait().unwrap().code(), Some(0));
}

// ---- waiting for budget and call slots ----

fn message_stating(len: usize) -> Bytes {
    let mut m = vec![0u8];
    m.extend_from_slice(&(len as u32).to_be_bytes());
    m.into()
}

/// A sender that holds the budget with a message it never finishes makes the
/// others wait for `budget_wait` and no longer; they are told to retry and
/// the reply names the limit. Once the holder is gone the budget is free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_that_cannot_get_budget_is_refused_as_unavailable() {
    const BUDGET: usize = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.max_inflight_bytes = BUDGET;
    o.stall_timeout = Duration::from_secs(60);
    o.receive_timeout = Duration::from_secs(60);
    o.budget_wait = Duration::from_millis(400);
    let server = Server::start(o).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;

    // States the whole of the bytes-read half and sends none of it.
    let mut holder = call_with(
        &mut c,
        EXPORT,
        "application/grpc",
        None,
        vec![message_stating(BUDGET / 2)],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let began = Instant::now();
    let r = export(&mut c, None, vec![message(false, &one_span(0x71))]).await;
    assert_eq!((r.http, r.code), (200, UNAVAILABLE), "{}", r.message);
    assert!(r.message.contains("bytes-read"), "{}", r.message);
    assert!(
        began.elapsed() < Duration::from_secs(3),
        "{:?}",
        began.elapsed()
    );

    // The holder's stream ends short of its length; its budget comes back.
    holder.end();
    let r = holder.reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, INVALID_ARGUMENT, "{}", r.message);
    let r = export(&mut c, None, vec![message(false, &one_span(0x72))]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    server.shutdown().await.unwrap();
}

/// A call takes a slot before it reads its body; one that cannot get a slot
/// within `budget_wait` is refused, and says which limit it hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_that_cannot_get_a_slot_is_refused_as_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.max_calls = 2;
    o.stall_timeout = Duration::from_secs(60);
    o.budget_wait = Duration::from_millis(400);
    let server = Server::start(o).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;

    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(call_with(&mut c, EXPORT, "application/grpc", None, vec![]).await);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = export(&mut c, None, vec![message(false, &one_span(0x73))]).await;
    assert_eq!((r.http, r.code), (200, UNAVAILABLE), "{}", r.message);
    assert!(r.message.contains("max_calls"), "{}", r.message);

    // A call that ends frees its slot.
    held[0].end();
    let r = held.remove(0).reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, INVALID_ARGUMENT, "{}", r.message);
    let r = export(&mut c, None, vec![message(false, &one_span(0x74))]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    held[0].end();
    let r = held.remove(0).reply(Duration::from_secs(5)).await;
    assert_eq!(r.code, INVALID_ARGUMENT, "{}", r.message);
    server.shutdown().await.unwrap();
}

// ---- zstd frames that do not state their size ----

/// A request of `n` spans with distinct ids and a long name each (the same
/// name): megabytes that compress to a few KB.
fn long_spans(n: u32) -> Vec<u8> {
    let spans = (0..n)
        .map(|i| Span {
            trace_id: vec![0x75; 16],
            span_id: [i.to_be_bytes(), [1; 4]].concat(),
            name: "x".repeat(2000),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 2_000,
            ..Default::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn span_count(api: SocketAddr) -> u64 {
    let mut s = TcpStream::connect(api).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        s,
        "GET /api/v1/traces?from=0&to=9000000000000000000 HTTP/1.1\r\nHost: localhost\r\n\
         Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw);
    let n = text.split("\"span_count\":").nth(1).expect(&text);
    n.split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsized_zstd_frame_within_the_assumed_size_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let frame = zstd_unsized(&one_span(0x76));
    let r = export(&mut c, Some("zstd"), vec![message(true, &frame)]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    assert_eq!(trace_status(server.api_addr(), 0x76), 200);
    server.shutdown().await.unwrap();
}

/// Output beyond the assumed size (1 MiB for a frame this small) is not an
/// error: the frame is expanded again under the limit, and stored whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsized_zstd_frame_beyond_the_assumed_size_is_still_accepted() {
    const SPANS: u32 = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let raw = long_spans(SPANS);
    let frame = zstd_unsized(&raw);
    // Beyond what is assumed for such a frame: 64 times its length, but at
    // least 1 MiB.
    assert!(raw.len() > 1 << 20, "{}", raw.len());
    assert!(frame.len() * 64 < raw.len(), "{}", frame.len());
    let r = export(&mut c, Some("zstd"), vec![message(true, &frame)]).await;
    assert_eq!(r.code, OK, "{}", r.message);
    assert_eq!(server.engine().stats().records_ingested, 1);
    assert_eq!(span_count(server.api_addr()), u64::from(SPANS));
    server.shutdown().await.unwrap();
}

/// A zstd frame with no content size made of run-length blocks: `n` bytes of
/// zero in a few KB.
fn zstd_zeros(n: usize) -> Vec<u8> {
    // Magic, a descriptor with no content size or checksum, a 1 MiB window.
    let mut f = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 10 << 3];
    let mut left = n;
    while left > 0 {
        let k = left.min(128 * 1024);
        left -= k;
        let h = u32::from(left == 0) | 1 << 1 | (k as u32) << 3;
        f.extend_from_slice(&h.to_le_bytes()[..3]);
        f.push(0);
    }
    f
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsized_zstd_frame_beyond_the_limit_is_resource_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let mut c = connect(server.otlp_grpc_addr()).await;
    let frame = zstd_zeros(MAX_MESSAGE + 1);
    assert!(frame.len() < 8192, "{}", frame.len());
    let r = export(&mut c, Some("zstd"), vec![message(true, &frame)]).await;
    assert_eq!((r.http, r.code), (200, RESOURCE_EXHAUSTED), "{}", r.message);
    // One byte less is within the limit: it is expanded, and refused only
    // because zeros are not a request.
    let frame = zstd_zeros(MAX_MESSAGE);
    let r = export(&mut c, Some("zstd"), vec![message(true, &frame)]).await;
    assert_eq!(r.code, INVALID_ARGUMENT, "{}", r.message);
    assert_eq!(server.engine().stats().records_ingested, 0);
    server.shutdown().await.unwrap();
}

// ---- the stream limit, from a client that does not obey it ----

fn h2_frame(out: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
}

fn hpack_str(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

/// The request headers of an export, in HPACK without Huffman coding or
/// indexing.
fn export_headers() -> Vec<u8> {
    let mut h = vec![0x83, 0x86, 0x04];
    hpack_str(&mut h, EXPORT);
    h.push(0x01);
    hpack_str(&mut h, "localhost");
    for (name, value) in [("content-type", "application/grpc"), ("te", "trailers")] {
        h.push(0x00);
        hpack_str(&mut h, name);
        hpack_str(&mut h, value);
    }
    h
}

/// One HTTP/2 frame: type, flags, stream, payload.
fn read_h2_frame(s: &mut TcpStream) -> std::io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut head = [0u8; 9];
    s.read_exact(&mut head)?;
    let len = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
    let stream = u32::from_be_bytes([head[5] & 0x7f, head[6], head[7], head[8]]);
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)?;
    Ok((head[3], head[4], stream, payload))
}

/// The server allows 64 streams at once and says so in its settings. A
/// client that opens a 65th regardless is refused with REFUSED_STREAM, and
/// the 64 that were allowed stay open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_65th_stream_opened_regardless_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.stall_timeout = Duration::from_secs(60);
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();
    let reset = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        h2_frame(&mut out, 4, 0, 0, &[]);
        s.write_all(&out).unwrap();
        // The server's settings, which must be acknowledged; they carry the
        // stream limit (setting 3).
        let limit = loop {
            let (kind, flags, _, payload) = read_h2_frame(&mut s).unwrap();
            if kind == 4 && flags & 1 == 0 {
                break payload
                    .chunks(6)
                    .find(|e| e[..2] == [0, 3])
                    .map(|e| u32::from_be_bytes([e[2], e[3], e[4], e[5]]));
            }
        };
        assert_eq!(limit, Some(64));
        let mut out = Vec::new();
        h2_frame(&mut out, 4, 1, 0, &[]);
        // 64 calls that are never ended, then a 65th.
        for i in 0..65u32 {
            h2_frame(&mut out, 1, 0x4, 1 + 2 * i, &export_headers());
        }
        s.write_all(&out).unwrap();
        loop {
            let (kind, _, stream, payload) = read_h2_frame(&mut s).unwrap();
            if kind == 3 {
                // RST_STREAM carries a four byte error code.
                break (stream, u32::from_be_bytes(payload[..4].try_into().unwrap()));
            }
        }
    })
    .await
    .unwrap();
    // The first reset is of the 65th stream, with REFUSED_STREAM (7).
    assert_eq!(reset, (129, 7));
    server.shutdown().await.unwrap();
}
