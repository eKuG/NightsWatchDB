//! Many large compressed exports at once against a small budget: the heap of
//! the whole process, measured by a counting allocator, must stay inside the
//! budget. Each message is tiny on the wire and expands to about 1 MiB.
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
use std::sync::atomic::{AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

struct Counting;

#[allow(unsafe_code)]
mod alloc_impl {
    use super::{Counting, LIVE, PEAK};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::Ordering::Relaxed;

    fn grew(by: isize) {
        let now = LIVE.fetch_add(by, Relaxed) + by;
        PEAK.fetch_max(now, Relaxed);
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

/// A valid request with no spans: one unknown field of `n` zero bytes.
fn padding(n: usize) -> Vec<u8> {
    let mut v = vec![0x7a];
    let mut len = n;
    while len >= 0x80 {
        v.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    v.push(len as u8);
    v.resize(v.len() + n, 0);
    v
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
    const BUDGET: usize = 8 << 20;
    const SIZE: usize = 1 << 20;
    const PER_KIND: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.max_inflight_bytes = BUDGET;
    let server = Server::start(o).await.unwrap();
    let addr = server.otlp_grpc_addr();

    let raw = padding(SIZE);
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(&raw).unwrap();
    let gz = e.finish().unwrap();
    let zst = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
    assert!(gz.len() < 4096 && zst.len() < 4096);
    let frame = |compressed: &[u8]| {
        let mut m = vec![1u8];
        m.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        m.extend_from_slice(compressed);
        Bytes::from(m)
    };
    let (gz, zst) = (frame(&gz), frame(&zst));
    drop(raw);

    // Without a budget, 40 of these in flight would hold 40 MiB or more.
    let mut conns = Vec::new();
    for _ in 0..4 {
        conns.push(connect(addr).await);
    }
    // Warm up, so that the connections' own buffers are in the baseline.
    for c in &mut conns {
        assert_eq!(export(c, "gzip", &gz).await, "0");
    }
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);

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
    let grown = PEAK.load(Ordering::SeqCst) - base;
    eprintln!("peak heap growth: {grown} bytes, budget {BUDGET}");
    assert!(
        grown < BUDGET as isize,
        "40 concurrent exports that expand to {SIZE} bytes each grew the heap by {grown} bytes; \
         the budget is {BUDGET}"
    );
    server.shutdown().await.unwrap();
}
