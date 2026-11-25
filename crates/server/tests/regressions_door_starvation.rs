//! A sender that holds the door's budget without ever stalling.
//!
//! A message takes its stated length from the budget before its payload is
//! read, and the call fails only after the sender has been silent for the
//! stall timeout. A sender that states a length as large as the budget and
//! then sends one byte at a time, each well inside the stall timeout, keeps
//! the whole budget for as long as it likes. Every other export waits for the
//! budget, and that wait has no limit.

use bytes::Bytes;
use http::Request;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::{Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

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
    let io = TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    send
}

/// Starts an export whose body is fed through the returned sender, and
/// returns the gRPC status of the reply once there is one.
async fn start(
    send: &mut SendRequest<Feed>,
) -> (
    mpsc::UnboundedSender<Bytes>,
    tokio::task::JoinHandle<String>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let req = Request::post(format!("http://localhost{EXPORT}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Feed(rx))
        .unwrap();
    send.ready().await.unwrap();
    let fut = send.send_request(req);
    let reply = tokio::spawn(async move {
        let resp = fut.await.unwrap();
        let (parts, body) = resp.into_parts();
        let trailers = body.collect().await.unwrap().trailers().cloned();
        let status =
            |h: &http::HeaderMap| h.get("grpc-status").map(|v| v.to_str().unwrap().to_owned());
        status(&parts.headers)
            .or_else(|| trailers.as_ref().and_then(status))
            .unwrap()
    });
    (tx, reply)
}

fn one_span() -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0x11; 16],
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_that_trickles_cannot_hold_the_budget_forever() {
    const BUDGET: usize = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    o.stall_timeout = Duration::from_secs(1);
    // The deadline for a whole message is what ends the trickler. The wait
    // for budget is left at its default, 5 s, which the waiting export
    // outlasts: it is served as soon as the trickler's budget comes back.
    o.receive_timeout = Duration::from_secs(2);
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    // The trickler states a message as large as the half of the budget that
    // counts bytes read, then sends one byte every 200 ms: never silent for
    // the 1 s stall timeout.
    let mut slow = connect(addr).await;
    let (trickle, slow_reply) = start(&mut slow).await;
    let mut header = vec![0u8];
    header.extend_from_slice(&((BUDGET / 2) as u32).to_be_bytes());
    trickle.send(header.into()).unwrap();
    let dripping = tokio::spawn(async move {
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if trickle.send(Bytes::from_static(b"\0")).is_err() {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // An ordinary export of one span on another connection.
    let mut c = connect(addr).await;
    let (tx, mut reply) = start(&mut c).await;
    let span = one_span();
    let mut m = vec![0u8];
    m.extend_from_slice(&(span.len() as u32).to_be_bytes());
    m.extend_from_slice(&span);
    tx.send(m.into()).unwrap();
    drop(tx);
    let status = tokio::time::timeout(Duration::from_secs(5), &mut reply)
        .await
        .expect(
            "an export of one span got no reply within 5 s, 5 times the stall timeout, while \
             another sender held the budget by sending a byte every 200 ms",
        )
        .unwrap();
    // It waited for the budget, and got it when the trickler's deadline
    // passed.
    assert_eq!(status, "0");
    // The trickler was ended by the deadline for its message.
    let trickler = tokio::time::timeout(Duration::from_secs(5), slow_reply)
        .await
        .expect("the trickler got no reply")
        .unwrap();
    assert_eq!(trickler, "4");
    dripping.abort();
    server.shutdown().await.unwrap();
}
