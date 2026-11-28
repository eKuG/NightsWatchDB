//! Streams that wait for the door's budget, each holding the first frame of
//! its message.
//!
//! The door reads one DATA frame to learn the length of a message, keeps the
//! rest of that frame, and only then waits for the budget, with no limit on
//! the wait. Every stream of every connection can do that at once: with the
//! defaults (1,024 connections, 64 streams, 16 KiB frames) that is about
//! 1 GiB held outside a budget of 256 MiB. Here 8 connections of 64 streams
//! do it against a budget of 1 MiB.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use bytes::Bytes;
use common::{Feed, connect};
use http::Request;
use hyper::client::conn::http2::SendRequest;
use nwdb_server::{Server, ServerOptions};
use nwdb_testkit::alloc::{Counting, live};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;

#[global_allocator]
static ALLOC: Counting = Counting;

mod common;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const BUDGET: usize = 1 << 20;
const CONNECTIONS: usize = 8;
const STREAMS: usize = 64;
/// The largest DATA frame a client may send without asking.
const FRAME: usize = 16 * 1024;

/// Starts an export, sends `first` and keeps the stream open.
async fn open(send: &mut SendRequest<Feed>, first: &Bytes) -> mpsc::UnboundedSender<Bytes> {
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(first.clone()).unwrap();
    let req = Request::post(format!("http://localhost{EXPORT}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Feed(rx))
        .unwrap();
    send.ready().await.unwrap();
    let fut = send.send_request(req);
    tokio::spawn(async move {
        let _ = fut.await;
    });
    tx
}

/// Opens `CONNECTIONS` x `STREAMS` exports that each send `first` and stay
/// open, and returns how much the heap grew at most while they were open.
async fn held_by_streams(addr: SocketAddr, first: &Bytes) -> isize {
    let mut conns = Vec::new();
    for _ in 0..CONNECTIONS {
        conns.push(connect(addr).await);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let base = live();
    let mut open_streams = Vec::new();
    for c in &mut conns {
        for _ in 0..STREAMS {
            open_streams.push(open(c, first).await);
        }
    }
    let mut grown = 0;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        grown = grown.max(live() - base);
    }
    drop(open_streams);
    drop(conns);
    tokio::time::sleep(Duration::from_secs(1)).await;
    grown
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_waiting_for_the_budget_hold_nothing_outside_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    o.stall_timeout = Duration::from_secs(60);
    // Each call that holds a slot holds the first frame of its message
    // outside the budget, so the slots bound that: 16 x 16 KiB is 256 KiB.
    // The streams that wait for a slot hold nothing but what HTTP/2 buffers,
    // at most one 128 KiB window per connection (8 x 128 KiB is 1 MiB).
    o.max_calls = 16;
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    // The cost of the streams themselves, client and server side: each
    // sends four bytes, not yet a message header, and waits.
    let streams_only = held_by_streams(addr, &Bytes::from_static(&[0, 0, 0, 0])).await;

    // One full frame each: a message header stating half the budget, then
    // the start of its payload. One stream is admitted and waits for the
    // rest of its message; all others wait for the budget.
    let mut first = vec![0u8];
    first.extend_from_slice(&((BUDGET / 2) as u32).to_be_bytes());
    first.resize(FRAME, 0);
    let with_frames = held_by_streams(addr, &Bytes::from(first)).await;

    let frames = with_frames - streams_only;
    eprintln!(
        "{} streams: {streams_only} bytes for the streams, {frames} more with a frame each; \
         budget {BUDGET}",
        CONNECTIONS * STREAMS
    );
    assert!(
        frames < 2 * BUDGET as isize,
        "{} export streams that each sent one {FRAME}-byte frame and wait for the budget hold \
         {frames} bytes more than streams that sent nothing; the budget is {BUDGET} bytes",
        CONNECTIONS * STREAMS
    );
    server.shutdown().await.unwrap();
}
