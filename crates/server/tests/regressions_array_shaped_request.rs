//! A request body, and every item of it, must be a JSON object. A JSON array
//! in either place is a 400 and nothing is written, although serde can build
//! the same structs from an array, field by field.

mod common;

use nwdb_server::Server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::net::SocketAddr;

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

async fn post(api: SocketAddr, body: &str) -> common::Resp {
    common::http_body(
        api,
        "POST",
        "/api/v1/annotations",
        "Content-Type: application/json\r\n",
        Some(body.as_bytes().to_vec()),
        None,
    )
    .await
}

/// The body `[[{...}]]` was read as `{"annotations": [{...}]}`, and an item
/// `["<trace>", null, "q", null, null, 1, null, null, null]` as
/// `{"trace_id": "<trace>", "name": "q", "value": 1}`. Both are refused now,
/// as is every other array in the place of an object, and a request of
/// objects is still accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_or_an_item_that_is_an_array_is_a_400() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    common::ingest(server.otlp_grpc_addr(), one_trace(), false).await;
    let api = server.api_addr();

    let whole = format!(r#"[[{{"trace_id":"{TRACE}","name":"whole","value":1}}]]"#);
    let item = format!(r#"{{"annotations":[["{TRACE}",null,"item",null,null,1,null,null,null]]}}"#);
    // The array forms with fewer fields than the struct has, and an array
    // as the second item of a list whose first is fine.
    let short_item = format!(r#"{{"annotations":[["{TRACE}",null,"short"]]}}"#);
    let second = format!(
        r#"{{"annotations":[{{"trace_id":"{TRACE}","name":"first","value":1}},["{TRACE}",null,"second",null,null,1]]}}"#
    );
    for (what, body) in [
        ("array body", whole),
        ("array item", item),
        ("short array item", short_item),
        ("array as second item", second),
    ] {
        let r = post(api, &body).await;
        assert_eq!(
            r.status,
            400,
            "{what}: {}",
            String::from_utf8_lossy(&r.body)
        );
        assert!(r.error().contains("object"), "{what}: {}", r.error());
    }
    let view = common::http(api, "GET", &format!("/api/v1/traces/{TRACE}"), "").await;
    assert!(
        view.json().get("annotations").is_none(),
        "something was written: {}",
        view.json()
    );

    // Objects are as before.
    let ok = format!(r#"{{"annotations":[{{"trace_id":"{TRACE}","name":"fine","value":1}}]}}"#);
    let r = post(api, &ok).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["accepted"], 1);
    server.shutdown().await.unwrap();
}
