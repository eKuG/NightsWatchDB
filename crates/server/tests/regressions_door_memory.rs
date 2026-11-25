//! Memory held by export requests that the in-flight budget has not admitted.
//!
//! The README says each request takes its size from a budget of bytes in
//! flight "so many large concurrent requests cannot use unbounded memory".
//! This test counts heap bytes with a global allocator while a client opens
//! export streams whose messages are small zstd frames that expand to a few
//! MiB each, and never ends them. The client is a hand-written HTTP/2 writer,
//! so the only large allocations in the process are the server's.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use nwdb_server::{Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

static LIVE: AtomicIsize = AtomicIsize::new(0);

struct Counting;

#[allow(unsafe_code)]
mod alloc_impl {
    use super::{Counting, LIVE};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::Ordering::Relaxed;

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                LIVE.fetch_add(l.size() as isize, Relaxed);
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                LIVE.fetch_add(l.size() as isize, Relaxed);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) };
            LIVE.fetch_sub(l.size() as isize, Relaxed);
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let q = unsafe { System.realloc(p, l, n) };
            if !q.is_null() {
                LIVE.fetch_add(n as isize - l.size() as isize, Relaxed);
            }
            q
        }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.load(Ordering::SeqCst)
}

const PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

/// A valid request whose last bytes are `n` copies of `a` (the value of a
/// string attribute), split into the bytes before them and `n`.
fn request_prefix(trace: u8, n: usize) -> Vec<u8> {
    let span = Span {
        trace_id: vec![trace; 16],
        span_id: vec![1; 8],
        attributes: vec![KeyValue {
            key: "blob".into(),
            value: Some(AnyValue {
                value: Some(V::StringValue("a".repeat(n))),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let (prefix, tail) = req.split_at(req.len() - n);
    assert!(tail.iter().all(|b| *b == b'a'));
    prefix.to_vec()
}

/// A zstd frame of `prefix` followed by `n` copies of `byte`: one raw block,
/// then run-length blocks of 128 KiB. A few hundred bytes for megabytes.
fn zstd_frame(prefix: &[u8], byte: u8, n: usize) -> Vec<u8> {
    fn block(out: &mut Vec<u8>, kind: u32, size: usize, last: bool) {
        let h = u32::from(last) | kind << 1 | (size as u32) << 3;
        out.extend_from_slice(&h.to_le_bytes()[..3]);
    }
    // Magic, a descriptor with no content size or checksum, a 1 MiB window.
    let mut f = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 10 << 3];
    block(&mut f, 0, prefix.len(), false);
    f.extend_from_slice(prefix);
    let mut left = n;
    while left > 0 {
        let k = left.min(128 * 1024);
        left -= k;
        block(&mut f, 1, k, left == 0);
        f.push(byte);
    }
    f
}

fn frame(out: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
}

fn hpack_str(out: &mut Vec<u8>, s: &str) {
    assert!(s.len() < 127);
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

/// The request headers of an export with zstd compression, in HPACK without
/// Huffman coding or indexing.
fn headers() -> Vec<u8> {
    // :method POST, :scheme http (static table 3 and 6).
    let mut h = vec![0x83, 0x86];
    // :path and :authority, literal values with indexed names (4 and 1).
    h.push(0x04);
    hpack_str(&mut h, PATH);
    h.push(0x01);
    hpack_str(&mut h, "localhost");
    for (name, value) in [
        ("content-type", "application/grpc"),
        ("te", "trailers"),
        ("grpc-encoding", "zstd"),
    ] {
        h.push(0x00);
        hpack_str(&mut h, name);
        hpack_str(&mut h, value);
    }
    h
}

fn read_frame(s: &mut TcpStream) -> (u8, u8) {
    let mut head = [0u8; 9];
    s.read_exact(&mut head).unwrap();
    let len = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload).unwrap();
    (head[3], head[4])
}

/// Connects, exchanges settings, and sends one export per stream id with the
/// given message as a compressed gRPC message. With `end`, each stream is
/// ended; without, it stays open after the message.
fn h2_exports(addr: SocketAddr, message: &[u8], streams: u32, end: bool) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut out, 4, 0, 0, &[]);
    s.write_all(&out).unwrap();
    // The server's SETTINGS, which must be acknowledged.
    loop {
        let (kind, flags) = read_frame(&mut s);
        if kind == 4 && flags & 1 == 0 {
            break;
        }
    }
    let mut out = Vec::new();
    frame(&mut out, 4, 1, 0, &[]);
    let mut grpc = vec![1u8];
    grpc.extend_from_slice(&(message.len() as u32).to_be_bytes());
    grpc.extend_from_slice(message);
    for i in 0..streams {
        let id = 1 + 2 * i;
        frame(&mut out, 1, 0x4, id, &headers());
        frame(&mut out, 0, u8::from(end), id, &grpc);
    }
    s.write_all(&out).unwrap();
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_outside_the_budget_do_not_hold_memory() {
    const SIZE: usize = 6 << 20;
    const STREAMS: u32 = 4;
    const BUDGET: usize = 1 << 20;

    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    let prefix = request_prefix(1, SIZE);
    let message = zstd_frame(&prefix, b'a', SIZE);
    assert!(message.len() < 1024, "{}", message.len());

    // Control: the same framing with the stream ended is a normal export,
    // which shows the hand-written HTTP/2 and zstd are right.
    let control = {
        let m = message.clone();
        tokio::task::spawn_blocking(move || h2_exports(addr, &m, 1, true))
            .await
            .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.engine().stats().records_ingested < 1 {
        assert!(
            Instant::now() < deadline,
            "the control export was not stored: the test's framing is wrong"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(control);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let base = live();

    // Streams that send their whole message and then stop. None of them has
    // reached the door's budget.
    let stalled = {
        let m = message.clone();
        tokio::task::spawn_blocking(move || h2_exports(addr, &m, STREAMS, false))
            .await
            .unwrap()
    };
    let wire = STREAMS as usize * (message.len() + 200);
    let mut grown = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && grown < STREAMS as isize * SIZE as isize / 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        grown = grown.max(live() - base);
    }
    drop(stalled);

    assert!(
        grown < 4 * BUDGET as isize,
        "{STREAMS} export streams that sent about {wire} bytes and then stalled hold {grown} \
         bytes of server memory; the in-flight budget is {BUDGET} bytes"
    );
    server.shutdown().await.unwrap();
}
