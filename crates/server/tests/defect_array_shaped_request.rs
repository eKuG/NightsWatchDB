//! A request body, or an item, written as a JSON array instead of an object
//! is accepted and written.

mod common;

use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TRACE: &str = "07070707070707070707070707070707";

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

/// POSTs `body` as JSON and returns the status and the reply.
fn post(addr: SocketAddr, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        s,
        "POST /api/v1/annotations HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text.split(' ').nth(1).unwrap().parse().unwrap();
    (status, text.split_once("\r\n\r\n").unwrap().1.to_owned())
}

/// Defect: `Req` and `Item` (src/api/annotate.rs) are derived `Deserialize`
/// structs, and serde_json builds a struct from a JSON array as well as from
/// an object, field by field in declaration order; `deny_unknown_fields`
/// only governs objects. So a body `[[{...}]]` is read as `{"annotations":
/// [{...}]}`, and an item `["<trace>", null, "q", null, null, 1, null, null,
/// null]` as `{"trace_id": "<trace>", "name": "q", "value": 1}`. The README
/// (What is a 400 and what is a rejected item) says that a body that is not
/// JSON of the documented shape, "a wrong type" among them, is a 400 and
/// nothing is written. The existing case "not an object" (`[]`) is a 400 only
/// because an empty array is too short.
///
/// Trigger: POST either array form for a trace that exists.
///
/// Expected: both are 400 and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_or_an_item_that_is_an_array_is_a_400() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    let api = server.api_addr();

    let whole = format!(r#"[[{{"trace_id":"{TRACE}","name":"whole","value":1}}]]"#);
    let item = format!(r#"{{"annotations":[["{TRACE}",null,"item",null,null,1,null,null,null]]}}"#);
    let (got_whole, got_item) =
        tokio::task::spawn_blocking(move || (post(api, &whole), post(api, &item)))
            .await
            .unwrap();
    let view = common::http(api, "GET", &format!("/api/v1/traces/{TRACE}"), "").await;
    let written = view.json().get("annotations").cloned();
    server.shutdown().await.unwrap();

    assert!(
        got_whole.0 == 400 && got_item.0 == 400 && written.is_none(),
        "array body: {got_whole:?}; array item: {got_item:?}; the trace now shows {written:?}"
    );
}
