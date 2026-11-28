//! A trace list reads everything its reply shows before the first byte is
//! sent and then lets go of the database snapshot: a client that reads slowly
//! does not keep parts alive. The annotations come from the buffer and the
//! parts alike, an engine error is an error response, and no comments are
//! shown.

mod common;

use nwdb_core::TraceId;
use nwdb_engine::Annotation;
use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use serde_json::Value;
use std::sync::mpsc;
use std::time::Duration;

const TRACES: u32 = 1000;

fn trace_id(i: u32) -> [u8; 16] {
    let mut id = [0x5a; 16];
    id[12..].copy_from_slice(&i.to_be_bytes());
    id
}

/// One span per trace; trace `i` starts `i` ns after the one before, so the
/// highest numbers are the newest, which is what a list shows first.
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

fn annotation(trace: u32, name: String, label: &str, comment: &str) -> Annotation {
    Annotation {
        trace_id: TraceId(trace_id(trace)),
        span_id: None,
        name,
        source: "s".into(),
        time_ns: 1,
        value: Some(0.5),
        label: Some(label.into()),
        comment: Some(comment.into()),
        delete: false,
        arrival: 0,
    }
}

fn annotate(engine: &nwdb_engine::Engine, items: Vec<Annotation>) {
    let n = items.len();
    let (tx, rx) = mpsc::channel();
    let done = engine
        .annotate(items, move |r| {
            let _ = tx.send(r);
        })
        .unwrap();
    assert_eq!(done.accepted, n, "{:?}", done.rejected.first());
    rx.recv().unwrap().unwrap();
}

/// The annotations of listed traces that are still in the buffer are shown as
/// they are after a flush, with the buffer full of the annotations of other
/// traces (the list is of the newest 20; the other 980 carry 5 each, and the
/// listed ones carry 5, one of them 10 so that it is cut at 8). The engine
/// does not count how often the buffer is walked, so this pins the result; the
/// walk is one per batch of 100 traces, not one per trace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn annotations_in_the_buffer_are_listed_as_after_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), traces(), false).await;
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || {
        engine.flush().unwrap();
        for k in 0..5 {
            let items = (0..TRACES)
                .map(|t| annotation(t, format!("score{k}"), "lbl", "a comment"))
                .collect();
            annotate(&engine, items);
        }
        let items = (5..10)
            .map(|k| annotation(TRACES - 1, format!("score{k}"), "lbl", "a comment"))
            .collect();
        annotate(&engine, items);
        assert_eq!(engine.stats().annotation_parts, 0, "nothing flushed yet");
    })
    .await
    .unwrap();

    let api = server.api_addr();
    let buffered = common::http(api, "GET", "/api/v1/traces?limit=20", "").await;
    assert_eq!(buffered.status, 200);
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || engine.flush().unwrap())
        .await
        .unwrap();
    assert!(server.engine().stats().annotation_parts > 0);
    let flushed = common::http(api, "GET", "/api/v1/traces?limit=20", "").await;
    assert_eq!(flushed.status, 200);
    server.shutdown().await.unwrap();

    let (a, b) = (buffered.json(), flushed.json());
    assert_eq!(a, b, "the buffered list differs from the flushed one");
    let list = a["traces"].as_array().unwrap();
    assert_eq!(list.len(), 20);
    for t in list {
        let id = t["trace_id"].as_str().unwrap();
        let cut = id.ends_with(&format!("{:08x}", TRACES - 1));
        let shown = t["annotations"].as_array().unwrap();
        assert_eq!(shown.len(), if cut { 8 } else { 5 }, "{t}");
        assert_eq!(t.get("annotations_truncated").is_some(), cut, "{t}");
        for n in shown {
            assert!(n.get("comment").is_none(), "a list shows no comments: {n}");
            assert_eq!(n["label"], "lbl");
        }
    }
}

/// The part files are not kept by a client that has read the headers of a
/// list and nothing more. Retention removes the parts; the process must not
/// hold their files open (on Linux a removed file that is open shows in
/// `/proc/self/fd` as `(deleted)`; there is no counter in the engine for it).
/// The reply is large (about 9 MB, more than the pipeline and the socket
/// buffers take), so the task that writes it is still waiting for the client
/// when retention runs.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_has_read_only_the_headers_does_not_keep_removed_parts_open() {
    use std::io::Read;
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn deleted_files(dir: &std::path::Path) -> Vec<String> {
        let dir = dir.canonicalize().unwrap();
        let mut found = Vec::new();
        for e in std::fs::read_dir("/proc/self/fd").unwrap().flatten() {
            if let Ok(target) = std::fs::read_link(e.path()) {
                let t = target.to_string_lossy().into_owned();
                if t.ends_with(" (deleted)") && t.starts_with(dir.to_str().unwrap()) {
                    found.push(t);
                }
            }
        }
        found
    }

    let dir = tempfile::tempdir().unwrap();
    let skew = Arc::new(AtomicI64::new(0));
    let mut o = common::opts(dir.path());
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    o.engine.retention = Some(Duration::from_secs(3600));
    let clock = skew.clone();
    o.engine.clock = Some(Arc::new(move || {
        common::now_ns() as i64 + clock.load(Ordering::SeqCst)
    }));
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), traces(), false).await;
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || {
        let q = |n: usize| "\"".repeat(n);
        for k in 0..8 {
            let items = (0..TRACES)
                .map(|t| {
                    let mut a = annotation(t, format!("{k:02}{}", q(126)), &q(256), &q(4096));
                    a.source = q(128);
                    a
                })
                .collect();
            annotate(&engine, items);
        }
        engine.flush().unwrap();
        assert!(engine.stats().parts > 0);
    })
    .await
    .unwrap();
    let dirpath = dir.path().to_path_buf();
    assert!(deleted_files(&dirpath).is_empty());

    // A client that has the headers and reads nothing more.
    let api = server.api_addr();
    let mut client = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        std::io::Write::write_all(
            &mut s,
            b"GET /api/v1/traces?limit=1000 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        let mut head = Vec::new();
        let mut one = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut one).unwrap();
            head.push(one[0]);
        }
        assert!(head.starts_with(b"HTTP/1.1 200"));
        s
    })
    .await
    .unwrap();

    // Retention removes every part (the clock moves three hours on). The
    // maintenance pass removes the part directories; their files are closed
    // when the last snapshot that has them is dropped.
    skew.store(3 * 3_600_000_000_000, Ordering::SeqCst);
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || {
        engine.run_maintenance().unwrap();
        assert_eq!(engine.stats().parts, 0, "retention removed nothing");
    })
    .await
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let open = deleted_files(&dirpath);
        if open.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "removed files are still open while a client reads slowly: {open:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The reply is whole all the same.
    let body = tokio::task::spawn_blocking(move || {
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).unwrap();
        common::dechunk(&rest)
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["traces"].as_array().unwrap().len(), TRACES as usize);
}

/// An engine error while the annotations are read is an error response with
/// the engine's status, not a 200 that stops in the middle: the annotation
/// part of the listed traces is cut short on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_error_before_the_first_byte_is_an_error_response() {
    fn find(dir: &std::path::Path, name: &str, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                find(&p, name, out);
            } else if p.file_name().is_some_and(|n| n == name) {
                out.push(p);
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let mut o = common::opts(dir.path());
    o.engine.flush_interval = Duration::from_secs(3600);
    o.engine.compaction.enabled = false;
    let server = Server::start(o).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), traces(), false).await;
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || {
        let items = (0..TRACES)
            .map(|t| annotation(t, "score".into(), "lbl", "c"))
            .collect();
        annotate(&engine, items);
        engine.flush().unwrap();
    })
    .await
    .unwrap();
    let api = server.api_addr();
    let fine = common::http(api, "GET", "/api/v1/traces?limit=20", "").await;
    assert_eq!(fine.status, 200);

    let mut files = Vec::new();
    find(dir.path(), "annotations.nwa", &mut files);
    assert!(!files.is_empty());
    for f in files {
        let file = std::fs::OpenOptions::new().write(true).open(f).unwrap();
        file.set_len(64).unwrap();
    }
    let broken = common::http(api, "GET", "/api/v1/traces?limit=20", "").await;
    let _ = server.shutdown().await;
    assert!(
        (500..600).contains(&broken.status),
        "{} {}",
        broken.status,
        String::from_utf8_lossy(&broken.body)
    );
    assert!(broken.headers.contains("application/json"));
    assert!(!broken.error().is_empty());
}
