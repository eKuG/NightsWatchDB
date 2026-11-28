//! The rules for write requests, against a real listener with hand-written
//! HTTP/1.1, so that the headers are exactly what a test sends. The route
//! `/api/v1/test/body` exists only in test builds: it answers with the length
//! of the body it was handed and a checksum of it.
//!
//! Every response read here is checked for an `access-control-` header.

use super::super::MAX_CONCURRENT;
use crate::{Server, ServerOptions};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOC: Counting = Counting;

const PATH: &str = "/api/v1/test/body";
const JSON: &str = "Content-Type: application/json";

struct Resp {
    status: u16,
    body: String,
}

impl Resp {
    /// The `len` and `sum` the test route answered.
    fn counted(&self) -> (u64, u64) {
        assert_eq!(self.status, 200, "{}", self.body);
        let v: serde_json::Value = serde_json::from_str(&self.body).unwrap();
        (v["len"].as_u64().unwrap(), v["sum"].as_u64().unwrap())
    }
}

fn sum(b: &[u8]) -> u64 {
    b.iter()
        .fold(0u64, |s, &x| s.wrapping_mul(31).wrapping_add(x.into()))
}

/// A request. `Host` and `Connection: close` are added, and a
/// `Content-Length`, unless the lines have a `Transfer-Encoding` or their own
/// `Content-Length` or the request is a GET or OPTIONS without a body.
fn build(addr: SocketAddr, method: &str, path: &str, lines: &[&str], body: &[u8]) -> Vec<u8> {
    let has = |name: &str| {
        lines
            .iter()
            .any(|l| l.to_ascii_lowercase().starts_with(name))
    };
    let mut r = format!("{method} {path} HTTP/1.1\r\n");
    if !has("host:") {
        r += &format!("Host: {addr}\r\n");
    }
    for l in lines {
        r += &format!("{l}\r\n");
    }
    let bodyless = body.is_empty() && matches!(method, "GET" | "OPTIONS");
    if !bodyless && !has("content-length:") && !has("transfer-encoding:") {
        r += &format!("Content-Length: {}\r\n", body.len());
    }
    r += "Connection: close\r\n\r\n";
    let mut out = r.into_bytes();
    out.extend_from_slice(body);
    out
}

fn chunked(body: &[u8], size: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for c in body.chunks(size) {
        out.extend_from_slice(format!("{:x}\r\n", c.len()).as_bytes());
        out.extend_from_slice(c);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
}

/// Reads one response by its Content-Length (every response of this API has
/// one) and fails if it carries a CORS header.
fn read_resp(s: &mut TcpStream) -> Resp {
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = s.read(&mut b).expect("reading the response");
        assert_eq!(n, 1, "the connection ended inside the response head");
        head.push(b[0]);
    }
    let head = String::from_utf8(head).unwrap().to_ascii_lowercase();
    assert!(!head.contains("\r\naccess-control-"), "{head}");
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let len: usize = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length: "))
        .map_or(0, |v| v.trim().parse().unwrap());
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).unwrap();
    Resp {
        status,
        body: String::from_utf8(body).unwrap(),
    }
}

fn connect(addr: SocketAddr) -> TcpStream {
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    s
}

/// Sends `raw` and reads the response. A write that fails because the server
/// has already answered and closed is not an error.
fn exchange(addr: SocketAddr, raw: &[u8]) -> Resp {
    let mut s = connect(addr);
    let _ = s.write_all(raw);
    read_resp(&mut s)
}

/// Runs blocking client code off the runtime's threads.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

async fn send(
    addr: SocketAddr,
    method: &'static str,
    path: &'static str,
    lines: &[&str],
    body: &[u8],
) -> Resp {
    let raw = build(addr, method, path, lines, body);
    blocking(move || exchange(addr, &raw)).await
}

/// A POST to the test route with the given extra header lines.
async fn post(addr: SocketAddr, lines: &[&str], body: &[u8]) -> Resp {
    send(addr, "POST", PATH, lines, body).await
}

async fn start(max: usize, timeout: Duration) -> (Server, SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut o = ServerOptions::new(dir.path());
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    o.api_max_body_bytes = max;
    o.api_body_timeout = timeout;
    o.drain_timeout = Duration::from_secs(1);
    let server = Server::start(o).await.unwrap();
    let addr = server.api_addr();
    (server, addr, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_write_paths_take_a_post_and_no_preflight_is_granted() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    let ok = post(addr, &[JSON], b"{}").await;
    assert_eq!(ok.counted(), (2, sum(b"{}")));

    // A write path refuses the other methods, other paths refuse POST.
    assert_eq!(send(addr, "GET", PATH, &[], b"").await.status, 405);
    assert_eq!(send(addr, "PUT", PATH, &[JSON], b"{}").await.status, 405);
    for path in [
        "/api/v1/traces",
        "/api/v1/stats",
        "/healthz",
        "/api/v1/search",
    ] {
        let r = send(addr, "POST", path, &[JSON], b"{}").await;
        assert_eq!(r.status, 405, "{path}");
    }
    assert_eq!(send(addr, "DELETE", "/healthz", &[], b"").await.status, 405);
    assert_eq!(
        send(addr, "POST", "/nowhere", &[JSON], b"{}").await.status,
        404
    );

    // A preflight to any path is refused without any CORS header (read_resp
    // fails on one).
    let preflight = [
        "Origin: http://evil.example",
        "Access-Control-Request-Method: POST",
        "Access-Control-Request-Headers: content-type",
    ];
    for path in [PATH, "/api/v1/traces", "/healthz", "/nowhere"] {
        let r = send(addr, "OPTIONS", path, &preflight, b"").await;
        assert!(matches!(r.status, 404 | 405), "{path}: {}", r.status);
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_content_type_must_be_json_in_utf_8() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    let accepted = [
        "Content-Type: application/json",
        "Content-Type: APPLICATION/JSON",
        "Content-Type: application/json; charset=utf-8",
        "Content-Type: application/json;charset=UTF-8",
        "Content-Type: application/json; charset=\"utf-8\"",
        "Content-Type: application/json; profile=x; charset=utf-8",
    ];
    for h in accepted {
        assert_eq!(post(addr, &[h], b"{}").await.counted().0, 2, "{h}");
    }
    let refused = [
        "Content-Type: text/plain",
        "Content-Type: text/plain;charset=UTF-8",
        "Content-Type: application/x-www-form-urlencoded",
        "Content-Type: multipart/form-data; boundary=x",
        "Content-Type: application/jsonx",
        "Content-Type: application/json; charset=utf-16",
        "Content-Type: application/json; charset=latin1",
        "Content-Type: application/x-protobuf",
        "Content-Type: ",
    ];
    for h in refused {
        let r = post(addr, &[h], b"{}").await;
        assert_eq!(r.status, 415, "{h}");
        assert!(r.body.contains("\"error\""), "{}", r.body);
    }
    // Missing, or given twice.
    assert_eq!(post(addr, &[], b"{}").await.status, 415);
    assert_eq!(post(addr, &[JSON, JSON], b"{}").await.status, 415);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_site_origin_is_refused() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    let own = format!("Origin: http://{addr}");
    let upper = format!("Origin: http://{addr}").to_ascii_uppercase();
    let wrong_port = format!("Origin: http://127.0.0.1:{}", addr.port() + 1);
    let https = format!("Origin: https://{addr}");
    let userinfo = format!("Origin: http://evil.example@{addr}");
    let path = format!("Origin: http://{addr}/");
    for h in [&own, &upper] {
        assert_eq!(post(addr, &[JSON, h], b"{}").await.counted().0, 2, "{h}");
    }
    // Names that are not the Host of the request, and one that is not an
    // origin at all.
    let refused = [
        "Origin: http://evil.example",
        "Origin: http://evil.example:4320",
        "Origin: http://localhost:1",
        "Origin: null",
        "Origin: ",
        "Origin: http://",
        "Origin: *",
        wrong_port.as_str(),
        https.as_str(),
        userinfo.as_str(),
        path.as_str(),
    ];
    for h in refused {
        let r = post(addr, &[JSON, h], b"{}").await;
        assert_eq!(r.status, 403, "{h}");
        assert!(r.body.contains("\"error\""), "{}", r.body);
    }
    // Two Origin headers, one of them right.
    assert_eq!(
        post(addr, &[JSON, &own, "Origin: null"], b"{}")
            .await
            .status,
        403
    );
    // The origin of the Host header, not of the address it came to.
    let r = post(
        addr,
        &[
            JSON,
            "Host: localhost:4320",
            "Origin: http://localhost:4320",
        ],
        b"{}",
    )
    .await;
    assert_eq!(r.counted().0, 2);
    let r = post(addr, &[JSON, "Host: localhost:4320", &own], b"{}").await;
    assert_eq!(r.status, 403);
    // No Origin: curl and SDKs.
    assert_eq!(post(addr, &[JSON], b"{}").await.counted().0, 2);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sec_fetch_site_must_be_same_origin_or_none() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    for v in ["same-origin", "none"] {
        let h = format!("Sec-Fetch-Site: {v}");
        assert_eq!(post(addr, &[JSON, &h], b"{}").await.counted().0, 2, "{v}");
    }
    for v in ["cross-site", "same-site", "", "anything"] {
        let h = format!("Sec-Fetch-Site: {v}");
        let r = post(addr, &[JSON, &h], b"{}").await;
        assert_eq!(r.status, 403, "{v}");
    }
    let r = post(
        addr,
        &[JSON, "Sec-Fetch-Site: none", "Sec-Fetch-Site: cross-site"],
        b"{}",
    )
    .await;
    assert_eq!(r.status, 403);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_host_check_comes_before_everything_else() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    // Wrong Host, and also wrong in every other way: still 421.
    let r = post(
        addr,
        &[
            "Host: rebind.example",
            "Content-Type: text/plain",
            "Origin: null",
            "Content-Length: 999999",
        ],
        b"",
    )
    .await;
    assert_eq!(r.status, 421);
    let r = post(addr, &[JSON, "Host: rebind.example"], b"{}").await;
    assert_eq!(r.status, 421);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_body_is_not_decompressed() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    for v in ["gzip", "zstd", "br", "deflate", "identity, gzip"] {
        let h = format!("Content-Encoding: {v}");
        assert_eq!(post(addr, &[JSON, &h], b"{}").await.status, 415, "{v}");
    }
    assert_eq!(
        post(addr, &[JSON, "Content-Encoding: Identity"], b"{}")
            .await
            .counted()
            .0,
        2
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_length_over_the_limit_is_refused_before_the_body_is_sent() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    // Headers only: the client never sends the body it announced.
    let raw = build(addr, "POST", PATH, &[JSON, "Content-Length: 1001"], b"");
    let r = blocking(move || exchange(addr, &raw)).await;
    assert_eq!(r.status, 413, "{}", r.body);
    assert!(r.body.contains("\"error\""));
    let raw = build(
        addr,
        "POST",
        PATH,
        &[JSON, "Content-Length: 99999999999"],
        b"",
    );
    assert_eq!(blocking(move || exchange(addr, &raw)).await.status, 413);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_body_is_read_up_to_the_limit_and_refused_past_it() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    let te = "Transfer-Encoding: chunked";
    let body: Vec<u8> = (0..=255u8).cycle().take(1001).collect();
    let r = post(addr, &[JSON, te], &chunked(&body, 100)).await;
    assert_eq!(r.status, 413, "{}", r.body);
    // One chunk that is itself over the limit.
    let r = post(addr, &[JSON, te], &chunked(&body, 2000)).await;
    assert_eq!(r.status, 413, "{}", r.body);
    // Exactly at the limit, chunked and with a length.
    let at = &body[..1000];
    let r = post(addr, &[JSON, te], &chunked(at, 100)).await;
    assert_eq!(r.counted(), (1000, sum(at)));
    let r = post(addr, &[JSON], at).await;
    assert_eq!(r.counted(), (1000, sum(at)));
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_body_reaches_the_handler_as_it_was_sent() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(30)).await;
    // Not valid UTF-8, not valid JSON, bytes of every value: the handler
    // gets them untouched. Empty is handed over as empty.
    let odd: Vec<u8> = (0..=255u8).chain(*b" \r\n\r\n{\"a\":1}  ").collect();
    let r = post(addr, &[JSON], &odd).await;
    assert_eq!(r.counted(), (odd.len() as u64, sum(&odd)));
    assert_eq!(post(addr, &[JSON], b"").await.counted(), (0, 0));
    let r = post(
        addr,
        &[JSON, "Transfer-Encoding: chunked"],
        &chunked(b"", 1),
    )
    .await;
    assert_eq!(r.counted(), (0, 0));
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_body_frees_its_slot() {
    let (server, addr, _dir) = start(1000, Duration::from_secs(4)).await;
    // Every slot is taken by a sender that announced 100 bytes, sent one and
    // stopped.
    let mut stalled = Vec::new();
    for _ in 0..MAX_CONCURRENT {
        let raw = build(addr, "POST", PATH, &[JSON, "Content-Length: 100"], b"x");
        let mut s = connect(addr);
        s.write_all(&raw).unwrap();
        stalled.push(s);
    }
    // Once the server has taken the slots, a query has to wait for one until
    // the senders are cut off.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let raw = build(addr, "GET", "/api/v1/traces", &[], b"");
    let started = Instant::now();
    let r = blocking(move || {
        let mut s = connect(addr);
        s.set_read_timeout(Some(Duration::from_secs(40))).unwrap();
        s.write_all(&raw).unwrap();
        read_resp(&mut s)
    })
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        started.elapsed() > Duration::from_millis(1500),
        "the query did not wait for a slot: {:?}",
        started.elapsed()
    );
    // Each stalled sender was answered 408.
    let timed_out = blocking(move || {
        let mut n = 0;
        for s in &mut stalled {
            n += usize::from(read_resp(s).status == 408);
        }
        n
    })
    .await;
    assert_eq!(timed_out, MAX_CONCURRENT);
    server.shutdown().await.unwrap();
}

/// 64 bodies of the largest size, all waiting for their last byte at the
/// same time: the heap of the process grows by about 64 times the limit and
/// not by a copy more. The allocator counts the whole test binary, so other
/// tests running at the same time add to the figure; the allowance is large
/// enough for them and far below the one more copy this would catch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_maximum_size_bodies_stay_within_the_limit_times_the_slots() {
    const LIMIT: usize = 1 << 20;
    // What 64 open connections hold besides the bodies (hyper's read buffers
    // were about 360 KiB each when measured), and the other tests.
    const ALLOWANCE: isize = 40 << 20;
    let (server, addr, _dir) = start(LIMIT, Duration::from_secs(120)).await;
    for use_chunks in [false, true] {
        let payload: Arc<Vec<u8>> = Arc::new((0..LIMIT).map(|i| (i % 251) as u8).collect());
        let barrier = Arc::new(Barrier::new(MAX_CONCURRENT));
        let base = live();
        reset_peak();
        let clients: Vec<_> = (0..MAX_CONCURRENT)
            .map(|_| {
                let (payload, barrier) = (payload.clone(), barrier.clone());
                std::thread::spawn(move || {
                    let mut s = connect(addr);
                    let (head, rest) = if use_chunks {
                        let raw = build(
                            addr,
                            "POST",
                            PATH,
                            &[JSON, "Transfer-Encoding: chunked"],
                            b"",
                        );
                        (raw, 0)
                    } else {
                        let len = format!("Content-Length: {LIMIT}");
                        (build(addr, "POST", PATH, &[JSON, &len], b""), 1)
                    };
                    s.write_all(&head).unwrap();
                    let first = LIMIT - rest;
                    if use_chunks {
                        for c in payload[..first - 4096].chunks(64 * 1024) {
                            s.write_all(format!("{:x}\r\n", c.len()).as_bytes())
                                .unwrap();
                            s.write_all(c).unwrap();
                            s.write_all(b"\r\n").unwrap();
                        }
                        barrier.wait();
                        s.write_all(b"1000\r\n").unwrap();
                        s.write_all(&payload[first - 4096..]).unwrap();
                        s.write_all(b"\r\n0\r\n\r\n").unwrap();
                    } else {
                        s.write_all(&payload[..first]).unwrap();
                        barrier.wait();
                        s.write_all(&payload[first..]).unwrap();
                    }
                    read_resp(&mut s).counted()
                })
            })
            .collect();
        for c in clients {
            assert_eq!(c.join().unwrap(), (LIMIT as u64, sum(&payload)));
        }
        let grown = peak() - base;
        let bound = (MAX_CONCURRENT * LIMIT) as isize;
        eprintln!("chunked={use_chunks}: peak heap growth {grown} bytes, bound {bound}");
        assert!(grown < bound + ALLOWANCE, "grew by {grown}, bound {bound}");
        // The test did hold the bodies at the same time.
        assert!(grown > bound / 2, "grew by only {grown}");
    }
    server.shutdown().await.unwrap();
}
