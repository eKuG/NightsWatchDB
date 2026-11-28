//! A sender that resets its stream while the door decodes its message.
//!
//! Decompression and decoding run on a blocking thread that keeps going after
//! the stream is gone. The budget it was charged must stay taken until that
//! thread has finished, or resetting streams in a loop runs any number of
//! decodes at once. Here each message is a small zstd frame that expands to
//! about 690 KB of minimal spans, so its decoding takes the whole decoding
//! half of the budget and two of them cannot be admitted at once. The client
//! resets each stream as soon as the server has started expanding it, and
//! sends the next.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use common::{frame, hpack_str, read_frame};
use nwdb_server::{DECODE_FACTOR, DECODE_FIXED, Server, ServerOptions};
use nwdb_testkit::alloc::{live, peak, reset_peak};
use nwdb_testkit::proto::ld;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Allocations of at least [`BIG`] bytes: live now, the most live at once,
/// and how many were ever made. Only the door's buffer of an expanded
/// message reaches that size in this test.
static BIG_LIVE: AtomicIsize = AtomicIsize::new(0);
static BIG_MAX: AtomicIsize = AtomicIsize::new(0);
static BIG_MADE: AtomicUsize = AtomicUsize::new(0);
mod common;

const BIG: usize = 1 << 20;

fn big(old: usize, new: usize) {
    if old < BIG && new >= BIG {
        BIG_MADE.fetch_add(1, Ordering::SeqCst);
        let now = BIG_LIVE.fetch_add(1, Ordering::SeqCst) + 1;
        BIG_MAX.fetch_max(now, Ordering::SeqCst);
    } else if old >= BIG && new < BIG {
        BIG_LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The shared counting allocator, which also counts the big allocations.
struct Counting;

#[allow(unsafe_code)]
mod alloc_impl {
    use super::{Counting, big};
    use nwdb_testkit::alloc::Counting as Inner;
    use std::alloc::{GlobalAlloc, Layout};

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { Inner::<{ usize::MAX }>.alloc(l) };
            if !p.is_null() {
                big(0, l.size());
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { Inner::<{ usize::MAX }>.alloc_zeroed(l) };
            if !p.is_null() {
                big(0, l.size());
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { Inner::<{ usize::MAX }>.dealloc(p, l) };
            big(l.size(), 0);
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let q = unsafe { Inner::<{ usize::MAX }>.realloc(p, l, n) };
            if !q.is_null() {
                big(l.size(), n);
            }
            q
        }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const BUDGET: usize = 16 << 20;
/// Spans per message: 23,000 x 30 bytes is about 690 KB expanded, so the
/// door charges 16 x 690 KB, more than the 8 MiB decoding half: the whole of
/// it, so that two messages are never decoded at once.
const SPANS: usize = 23_000;
const MESSAGES: usize = 6;

/// A request of `SPANS` spans that carry only their two ids.
fn request() -> Vec<u8> {
    let span = [ld(1, &[7; 16]), ld(2, &[3; 8])].concat();
    let spans: Vec<u8> = ld(2, &span).repeat(SPANS);
    ld(1, &ld(2, &spans))
}

/// The request headers of a zstd export, in HPACK without Huffman coding or
/// indexing.
fn headers() -> Vec<u8> {
    let mut h = vec![0x83, 0x86];
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

fn connect(addr: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.set_nodelay(true).unwrap();
    let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    frame(&mut out, 4, 0, 0, &[]);
    s.write_all(&out).unwrap();
    loop {
        let (kind, flags) = read_frame(&mut s);
        if kind == 4 && flags & 1 == 0 {
            break;
        }
    }
    let mut ack = Vec::new();
    frame(&mut ack, 4, 1, 0, &[]);
    s.write_all(&ack).unwrap();
    s
}

/// Sends the messages one after the other; each stream is reset as soon as
/// the server has begun to expand its message.
fn reset_each_once_it_expands(addr: SocketAddr, message: &[u8]) -> usize {
    let mut s = connect(addr);
    let mut grpc = vec![1u8];
    grpc.extend_from_slice(&(message.len() as u32).to_be_bytes());
    grpc.extend_from_slice(message);
    let mut admitted = 0;
    for i in 0..MESSAGES {
        let id = 1 + 2 * i as u32;
        let mut out = Vec::new();
        frame(&mut out, 1, 0x4, id, &headers());
        frame(&mut out, 0, 0x1, id, &grpc);
        s.write_all(&out).unwrap();
        // Wait until the server is expanding this message. A door that
        // keeps the budget until the work is done admits it only after the
        // previous one has finished.
        let deadline = Instant::now() + Duration::from_secs(60);
        while BIG_MADE.load(Ordering::SeqCst) <= i {
            assert!(Instant::now() < deadline, "message {i} was never expanded");
            std::thread::yield_now();
        }
        admitted += 1;
        // RST_STREAM, CANCEL.
        let mut rst = Vec::new();
        frame(&mut rst, 3, 0, id, &8u32.to_be_bytes());
        s.write_all(&rst).unwrap();
    }
    admitted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_stream_keeps_its_budget_until_its_decoding_ends() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    let raw = request();
    assert!(DECODE_FACTOR * raw.len() + DECODE_FIXED > BUDGET / 2);
    let message = zstd::bulk::compress(&raw, 3).unwrap();
    assert!(message.len() < 4096, "{}", message.len());
    drop(raw);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let base = live();
    reset_peak();
    BIG_MAX.store(BIG_LIVE.load(Ordering::SeqCst), Ordering::SeqCst);

    let admitted = tokio::task::spawn_blocking(move || reset_each_once_it_expands(addr, &message))
        .await
        .unwrap();
    // Let every expansion that is still running finish.
    let deadline = Instant::now() + Duration::from_secs(60);
    while BIG_LIVE.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let grown = peak() - base;
    let at_once = BIG_MAX.load(Ordering::SeqCst);
    eprintln!(
        "{admitted} messages reset while expanding; at most {at_once} expanded at once; \
         peak heap growth {grown} bytes, budget {BUDGET}"
    );
    assert!(
        grown < BUDGET as isize,
        "{admitted} exports whose streams were reset during decoding grew the heap by {grown} \
         bytes, {at_once} of them decoding at once; the budget is {BUDGET} bytes and each \
         needs most of its decoding half"
    );
    server.shutdown().await.unwrap();
}
