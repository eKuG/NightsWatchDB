//! The memory of 64 concurrent `POST /api/v1/annotations` of the largest size
//! (the API's 64 slots), against the real endpoint: the body, the items parsed
//! from it, and the record the engine encodes and keeps.

mod common;

use nwdb_server::Server;
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[global_allocator]
static ALLOC: Counting = Counting;

const SLOTS: usize = 64;
const LIMIT: usize = 1 << 20;
const ITEMS: usize = 1000;
const TRACE: &str = "07070707070707070707070707070707";
/// What a request may hold: its body, the items parsed from it (about the
/// size of the body, and the log's record is built from them), and the
/// annotations the engine keeps in its buffers until they are flushed (the
/// same text again, as the body is freed once it is parsed). Three times the
/// limit per request: 192 MiB for 64. Measured: a peak of about 112 MiB, of
/// which about 67 MiB stays held by the engine's buffers.
const BOUND: isize = (SLOTS * 3 * LIMIT) as isize;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_largest_annotate_requests_stay_within_the_stated_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    // Nothing is flushed: the engine keeps every record in its buffers.
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    let api = server.api_addr();

    let bodies: Vec<Arc<Vec<u8>>> = (0..SLOTS).map(|r| Arc::new(largest(r))).collect();
    let sent = bodies[0].len();
    assert!(sent > LIMIT - 8192, "the request is only {sent} bytes");
    let barrier = Arc::new(Barrier::new(SLOTS));
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
                // All 64 bodies are in memory, one byte short, at this moment.
                barrier.wait();
                s.write_all(last).unwrap();
                let mut reply = String::new();
                s.read_to_string(&mut reply).unwrap();
                reply
            })
        })
        .collect();
    for c in clients {
        let reply = c.join().unwrap();
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        assert!(reply.contains(r#""accepted":1000"#), "{reply}");
    }
    let grown = peak() - base;
    let kept = live() - base;
    let bound = BOUND;
    eprintln!(
        "{SLOTS} requests of {sent} bytes: peak heap growth {grown} bytes, still held {kept}, bound {bound}"
    );
    server.shutdown().await.unwrap();
    assert!(
        grown < bound,
        "grew by {} MiB, bound {} MiB",
        grown >> 20,
        bound >> 20
    );
    // The test did hold the bodies at the same time.
    assert!(grown > (SLOTS * LIMIT) as isize, "grew by only {grown}");
}
