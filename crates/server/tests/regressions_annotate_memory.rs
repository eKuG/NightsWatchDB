//! The memory of 64 concurrent `POST /api/v1/annotations` of the largest size
//! (the API's 64 slots), against the real endpoint: well-formed requests (the
//! body, the items parsed from it, and the record the engine encodes and
//! keeps) and malformed ones whose error message could quote what was sent.

mod common;

use nwdb_server::Server;
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[global_allocator]
static ALLOC: Counting = Counting;

/// The allocator counts the whole process, so the tests of this file run one
/// at a time.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SLOTS: usize = 64;
const LIMIT: usize = 1 << 20;
const ITEMS: usize = 1000;
const TRACE: &str = "07070707070707070707070707070707";
/// What a well-formed request may hold: 2.5 times the limit. Its body, the
/// items parsed from it (about the size of the body; the log's record is built
/// from them) and the annotations the engine keeps in its buffers until they
/// are flushed are three copies of the text, which is where the earlier
/// statement of 3 times came from; they are not all there at once. Measured:
/// a peak of about 1.8 times the limit per request (119 MB for 64), of which
/// 70 MB stays held by the engine's buffers. Freeing the body as soon as it
/// is parsed (which the handler does) lowers the peak by about 0.3 times
/// (137 MB without it); that difference is smaller than the variation between
/// runs, so the bound does not test it.
const BOUND: isize = (SLOTS * LIMIT * 5 / 2) as isize;
/// What 64 malformed requests may hold: 2 times the limit each. The body is
/// held (about 1.5 times the limit is measured for any body of that size: the
/// HTTP layer holds it in pieces while it is received). The parse of a request
/// runs on a thread of the runtime without waiting, so only as many parses
/// are in flight as there are threads, and with 64 bodies in memory they do
/// not show in the peak (measured at most 1.5 times); each case is therefore
/// also run alone, where its own peak shows: 2 times the limit for a string,
/// 4 times for a field name of a megabyte (measured 3.5), which serde copies
/// into its own message.
const MALFORMED_BOUND: isize = (SLOTS * 2 * LIMIT) as isize;

fn one_trace() -> Vec<u8> {
    let start = common::now_ns() - 600_000_000_000;
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![7; 16],
                    span_id: vec![1; 8],
                    name: "op".into(),
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 1_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// The largest request: 1000 items (the most there can be) of a score, a
/// label and a comment, as long as the 1 MiB body limit allows. Each request
/// has names of its own, so every item is a record of its own.
fn largest(request: usize) -> Vec<u8> {
    let build = |comment: usize| {
        let comment = "c".repeat(comment);
        let items: Vec<String> = (0..ITEMS)
            .map(|i| {
                format!(
                    r#"{{"trace_id":"{TRACE}","name":"r{request:02}-i{i:04}","source":"s","value":1.5,"label":"label","comment":"{comment}"}}"#
                )
            })
            .collect();
        format!(r#"{{"annotations":[{}]}}"#, items.join(",")).into_bytes()
    };
    let mut comment = 4096;
    loop {
        let body = build(comment);
        if body.len() <= LIMIT {
            return body;
        }
        comment -= 8;
    }
}

/// A body of `LIMIT - 1024` bytes whose giant part is `filler` characters
/// between `before` and `after`.
fn giant(before: &str, filler: char, after: &str) -> Vec<u8> {
    let n = (LIMIT - 1024 - before.len() - after.len()) / filler.len_utf8();
    let mut s = String::with_capacity(LIMIT);
    s.push_str(before);
    s.extend(std::iter::repeat_n(filler, n));
    s.push_str(after);
    s.into_bytes()
}

/// Sends one body per slot, each held one byte short until all 64 are in
/// memory, then completes them all. Returns the replies, the heap growth at
/// the peak and the heap still held after.
fn run(api: SocketAddr, bodies: &[Arc<Vec<u8>>]) -> (Vec<String>, isize, isize) {
    let barrier = Arc::new(Barrier::new(bodies.len()));
    let base = live();
    reset_peak();
    let clients: Vec<_> = bodies
        .iter()
        .map(|body| {
            let (body, barrier) = (body.clone(), barrier.clone());
            std::thread::spawn(move || {
                let mut s = TcpStream::connect(api).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
                write!(
                    s,
                    "POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
                     Content-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                let (first, last) = body.split_at(body.len() - 1);
                s.write_all(first).unwrap();
                // All the bodies are in memory, one byte short, at this moment.
                barrier.wait();
                s.write_all(last).unwrap();
                let mut reply = String::new();
                s.read_to_string(&mut reply).unwrap();
                reply
            })
        })
        .collect();
    let replies = clients.into_iter().map(|c| c.join().unwrap()).collect();
    (replies, peak() - base, live() - base)
}

async fn server(dir: &std::path::Path) -> Server {
    let mut o = common::opts(dir);
    // Nothing is flushed: the engine keeps every record in its buffers.
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    server
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_largest_annotate_requests_stay_within_the_stated_memory() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let server = server(dir.path()).await;
    let bodies: Vec<Arc<Vec<u8>>> = (0..SLOTS).map(|r| Arc::new(largest(r))).collect();
    let sent = bodies[0].len();
    assert!(sent > LIMIT - 8192, "the request is only {sent} bytes");
    let (replies, grown, kept) = run(server.api_addr(), &bodies);
    for reply in replies {
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        assert!(reply.contains(r#""accepted":1000"#), "{reply}");
    }
    eprintln!(
        "{SLOTS} requests of {sent} bytes: peak heap growth {grown} bytes, still held {kept}, bound {BOUND}"
    );
    server.shutdown().await.unwrap();
    assert!(
        grown < BOUND,
        "grew by {} MiB, bound {} MiB",
        grown >> 20,
        BOUND >> 20
    );
    // The test did hold the bodies at the same time.
    assert!(grown > (SLOTS * LIMIT) as isize, "grew by only {grown}");
}

/// Malformed requests of the largest size whose parse error serde builds by
/// quoting the text it found: a string where an object is expected (as the
/// body and as an item), a string where a number is expected, and a field name
/// of a megabyte. The reply is a short 400 and the memory stays within the
/// stated bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_malformed_giant_requests_stay_within_the_stated_memory() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let server = server(dir.path()).await;
    let item = format!(r#"{{"trace_id":"{TRACE}","name":"n","#);
    let cases: Vec<(&str, Vec<u8>, &str, isize)> = vec![
        (
            "string as the body",
            giant("\"", 'x', "\""),
            "a JSON object",
            2,
        ),
        (
            "string as an item",
            giant(r#"{"annotations":[""#, 'x', r#""]}"#),
            "a JSON object",
            2,
        ),
        (
            "string as a number",
            giant(
                &format!(r#"{{"annotations":[{item}"time_ns":""#),
                'x',
                r#""}]}"#,
            ),
            "an integer",
            2,
        ),
        (
            "string as a bool",
            giant(
                &format!(r#"{{"annotations":[{item}"delete":""#),
                'x',
                r#""}]}"#,
            ),
            "a boolean",
            2,
        ),
        (
            "characters that debug formatting writes as escapes",
            giant(
                &format!(r#"{{"annotations":[{item}"value":""#),
                '\u{85}',
                r#""}]}"#,
            ),
            "a number",
            2,
        ),
        (
            "a field name of a megabyte",
            giant(&format!(r#"{{"annotations":[{item}""#), 'k', r#"":1}]}"#),
            "unknown field",
            4,
        ),
        (
            "a top-level field name of a megabyte",
            giant(r#"{""#, 'k', r#"":1}"#),
            "unknown field",
            4,
        ),
    ];
    let mut worst = 0;
    for (what, body, says, times) in cases {
        // One request alone: its own peak, parse included. With 64 at once
        // the parses are spread over the few threads of the runtime and are
        // hidden by the 64 bodies, so this is where a long message would show.
        let alone = vec![Arc::new(body.clone())];
        let (replies, single, _) = run(server.api_addr(), &alone);
        eprintln!(
            "{what}: one request of {} bytes, peak heap growth {single} bytes ({:.2} x limit)",
            body.len(),
            single as f64 / LIMIT as f64
        );
        assert!(
            replies[0].starts_with("HTTP/1.1 400"),
            "{what}: {}",
            replies[0]
        );
        assert!(
            single < times * LIMIT as isize,
            "{what}: one request grew the heap by {} KiB, bound {} KiB",
            single >> 10,
            (times * LIMIT as isize) >> 10
        );
        let bodies: Vec<Arc<Vec<u8>>> = (0..SLOTS).map(|_| Arc::new(body.clone())).collect();
        let (replies, grown, kept) = run(server.api_addr(), &bodies);
        eprintln!(
            "{what}: {SLOTS} x {} bytes, peak heap growth {grown} bytes ({:.2} x limit x {SLOTS}), still held {kept}, bound {MALFORMED_BOUND}",
            body.len(),
            grown as f64 / (LIMIT * SLOTS) as f64
        );
        for reply in &replies {
            assert!(reply.starts_with("HTTP/1.1 400"), "{what}: {reply}");
            assert!(reply.contains(says), "{what}: {reply}");
            assert!(
                reply.len() < 1024,
                "{what}: a reply of {} bytes",
                reply.len()
            );
        }
        assert!(
            grown < MALFORMED_BOUND,
            "{what}: grew by {} MiB, bound {} MiB",
            grown >> 20,
            MALFORMED_BOUND >> 20
        );
        worst = worst.max(grown);
    }
    server.shutdown().await.unwrap();
    eprintln!("worst malformed peak: {worst} bytes");
}
