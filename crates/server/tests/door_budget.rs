//! Many compressed exports at once against a small budget: the heap of the
//! whole process, measured by a counting allocator, must stay inside the
//! decoding half of the budget. Each message is tiny on the wire and expands
//! to a request full of spans, so decoding really allocates: a charge per
//! request that is too small lets too many decode at once and the test fails.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use nwdb_server::{Server, ServerOptions};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);

struct Counting;

#[allow(unsafe_code)]
mod alloc_impl {
    use super::{Counting, LIVE};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::Ordering::Relaxed;

    fn grew(by: isize) {
        LIVE.fetch_add(by, Relaxed);
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                grew(l.size() as isize);
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                grew(l.size() as isize);
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
                grew(n as isize - l.size() as isize);
            }
            q
        }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

fn ld(field: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![field << 3 | 2];
    let mut len = payload.len();
    while len >= 0x80 {
        v.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    v.push(len as u8);
    v.extend_from_slice(payload);
    v
}

/// A span with only its two ids; `n` makes the span id unique.
fn minimal_span(n: u32) -> Vec<u8> {
    [ld(1, &[1; 16]), ld(2, &[n.to_be_bytes(), [9; 4]].concat())].concat()
}

/// A short printable name that is different for every `n` below 95 * 95.
fn short_name(n: u32) -> Vec<u8> {
    vec![b' ' + (n % 95) as u8, b' ' + (n / 95 % 95) as u8]
}

/// `n` `ScopeSpans` under one `ResourceSpans`, each with a scope of its own
/// and one minimal span. A little past 1,000 spans, or 4,000, is the shape
/// that costs the decoder the most per byte of request.
fn spans(n: u32) -> Vec<u8> {
    let scopes: Vec<u8> = (0..n)
        .flat_map(|i| {
            let name = short_name(i);
            let scope = [ld(1, &name), ld(2, &name)].concat();
            let scope_spans = [ld(1, &scope), ld(2, &minimal_span(i)), ld(3, &name)].concat();
            ld(2, &scope_spans)
        })
        .collect();
    ld(1, &scopes)
}

async fn connect(addr: std::net::SocketAddr) -> SendRequest<Full<Bytes>> {
    let io = TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    send
}

async fn export(send: &mut SendRequest<Full<Bytes>>, encoding: &str, message: &Bytes) -> String {
    let req = Request::post(format!("http://localhost{EXPORT}"))
        .header("content-type", "application/grpc")
        .header("grpc-encoding", encoding)
        .body(Full::new(message.clone()))
        .unwrap();
    send.ready().await.unwrap();
    let resp = send.send_request(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    let trailers = body.collect().await.unwrap().trailers().cloned();
    let status = |h: &http::HeaderMap| h.get("grpc-status").map(|v| v.to_str().unwrap().to_owned());
    status(&parts.headers)
        .or_else(|| trailers.as_ref().and_then(status))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_large_compressed_exports_stay_inside_the_budget() {
    const BUDGET: usize = 24 << 20;
    const SPANS: u32 = 4_097;
    const PER_KIND: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    let raw = spans(SPANS);
    let size = raw.len();
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(&raw).unwrap();
    let gz = e.finish().unwrap();
    // The bulk encoder writes the content size into the frame.
    let zst = zstd::bulk::compress(&raw, 3).unwrap();
    assert!(gz.len() < 128 << 10 && zst.len() < 128 << 10);
    let frame = |compressed: &[u8]| {
        let mut m = vec![1u8];
        m.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        m.extend_from_slice(compressed);
        Bytes::from(m)
    };
    let (gz, zst) = (frame(&gz), frame(&zst));
    drop(raw);

    // Charged at 16 times their size, four of these decode at once.
    // Without a budget, 40 in flight would hold 40 times their decoded size.
    let mut conns = Vec::new();
    for _ in 0..4 {
        conns.push(connect(addr).await);
    }
    // Warm up, so that the connections' own buffers are in the baseline.
    for c in &mut conns {
        assert_eq!(export(c, "gzip", &gz).await, "0");
    }
    let base = LIVE.load(Ordering::SeqCst);
    // What the engine keeps of the requests it has accepted is not the
    // door's: it is counted by the engine and left out here. So the heap is
    // sampled, less the engine's buffered bytes at that moment.
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (done, engine) = (done.clone(), server.engine().clone());
        std::thread::spawn(move || {
            let mut worst = 0isize;
            while !done.load(Ordering::SeqCst) {
                let kept = engine.stats().buffered_bytes as isize;
                worst = worst.max(LIVE.load(Ordering::SeqCst) - base - kept);
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
            worst
        })
    };

    let mut tasks = Vec::new();
    for i in 0..PER_KIND * 2 {
        let mut c = conns[i % conns.len()].clone();
        let (enc, msg) = if i % 2 == 0 {
            ("gzip", gz.clone())
        } else {
            ("zstd", zst.clone())
        };
        tasks.push(tokio::spawn(async move { export(&mut c, enc, &msg).await }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), "0");
    }
    done.store(true, Ordering::SeqCst);
    let grown = sampler.join().unwrap();
    // The compressed messages are in the other half, and are a few KiB.
    let decoding_half = (BUDGET / 2) as isize;
    eprintln!("peak heap growth: {grown} bytes, decoding half {decoding_half}, request {size}");
    assert!(
        grown < decoding_half,
        "40 concurrent exports of {SPANS} spans ({size} bytes) each grew the heap by {grown} \
         bytes; the decoding half of the budget is {decoding_half}"
    );
    server.shutdown().await.unwrap();
}
