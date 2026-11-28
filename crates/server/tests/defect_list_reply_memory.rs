//! One `GET /api/v1/traces?limit=1000` holds hundreds of MiB once the listed
//! traces carry annotations.

mod common;

use nwdb_core::TraceId;
use nwdb_engine::Annotation;
use nwdb_server::Server;
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;

#[global_allocator]
static ALLOC: Counting = Counting;

const TRACES: u32 = 1000;
/// What the list shows per trace.
const PER_TRACE: usize = 32;
/// The memory bound the README gives for write requests: 64 slots x 1 MiB.
const BOUND: isize = 64 << 20;

fn trace_id(i: u32) -> [u8; 16] {
    let mut id = [0x5a; 16];
    id[12..].copy_from_slice(&i.to_be_bytes());
    id
}

fn traces() -> Vec<u8> {
    let start = common::now_ns() - 600_000_000_000;
    let spans = (0..TRACES)
        .map(|i| Span {
            trace_id: trace_id(i).to_vec(),
            span_id: vec![1; 8],
            name: "op".into(),
            start_time_unix_nano: start + u64::from(i),
            end_time_unix_nano: start + u64::from(i) + 1_000,
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

/// The largest annotation the engine takes; `"` is legal everywhere and
/// doubles when written as JSON.
fn largest(trace: u32, k: usize) -> Annotation {
    let q = |n: usize| "\"".repeat(n);
    Annotation {
        trace_id: TraceId(trace_id(trace)),
        span_id: None,
        name: format!("{k:02}{}", q(126)),
        source: q(128),
        time_ns: 1,
        value: Some(0.5),
        label: Some(q(256)),
        comment: Some(q(4096)),
        delete: false,
        arrival: 0,
    }
}

/// Defect: `list_traces` (src/api.rs) reads up to 1000 summaries and, for
/// each, up to 32 annotations, then builds the whole answer in memory: the
/// rows (`traces`, which stays alive because the next `let traces` only
/// shadows it), a `serde_json::Value` per trace, a second copy of that tree
/// made by `json!({ "traces": traces })` (the macro serializes its argument
/// by reference), and the serialized text in `json_response`. An annotation
/// can carry 4608 bytes of text that become 9 KiB of JSON, so one request
/// for the largest list holds several hundred MiB, and 64 of them at once (the
/// API's slots) tens of GiB. The trace view streams its JSON for exactly this
/// reason ("a trace is never held as JSON"); before this checkpoint a summary
/// was a few hundred bytes. Anyone who can write annotations can set this up,
/// and any client can then ask for it.
///
/// Trigger: 1000 traces with 32 of the largest annotations each, then
/// `GET /api/v1/traces?limit=1000`.
///
/// Expected: the heap one list request needs stays bounded, here below the
/// 64 MiB the README gives for all write bodies together (by streaming the
/// list, or by bounding the annotation text a list may carry).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_list_request_stays_within_a_bounded_heap() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), traces(), false).await;

    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || {
        for k in 0..PER_TRACE {
            let items: Vec<Annotation> = (0..TRACES).map(|t| largest(t, k)).collect();
            let (tx, rx) = mpsc::channel();
            let done = engine
                .annotate(items, move |r| {
                    let _ = tx.send(r);
                })
                .unwrap();
            assert_eq!(
                done.accepted,
                TRACES as usize,
                "{:?}",
                done.rejected.first()
            );
            rx.recv().unwrap().unwrap();
        }
        engine.flush().unwrap();
    })
    .await
    .unwrap();

    let api = server.api_addr();
    let base = live();
    reset_peak();
    let (head, body_len) = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
        s.write_all(
            b"GET /api/v1/traces?limit=1000 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        // Counted, not kept, so that the client adds nothing to the heap.
        let mut buf = vec![0u8; 64 * 1024];
        let mut head = Vec::new();
        let mut total = 0usize;
        loop {
            let n = s.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            if head.len() < 512 {
                head.extend_from_slice(&buf[..n.min(512)]);
            }
            total += n;
        }
        (String::from_utf8_lossy(&head).into_owned(), total)
    })
    .await
    .unwrap();
    let grown = peak() - base;
    server.shutdown().await.unwrap();

    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    eprintln!("list reply {body_len} bytes, heap grew by {grown} bytes during the request");
    assert!(
        grown < BOUND,
        "one list request grew the heap by {} MiB (reply {} MiB), bound {} MiB",
        grown >> 20,
        body_len >> 20,
        BOUND >> 20
    );
}
