//! `GET /api/v1/search` against a real server: the response, every error
//! status, percent-encoded and non-ASCII queries, and what a flush and a
//! restart do.

use bytes::{Buf, BufMut, Bytes};
use common::{Resp, now_ns};
use nwdb_server::{Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use serde_json::Value;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

mod common;

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

struct BytesCodec;
struct BytesEnc;
struct BytesDec;

impl Encoder for BytesEnc {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesDec {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEnc;
    type Decoder = BytesDec;
    fn encoder(&mut self) -> BytesEnc {
        BytesEnc
    }
    fn decoder(&mut self) -> BytesDec {
        BytesDec
    }
}

async fn export(addr: SocketAddr, body: Vec<u8>) {
    let ch: Channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut grpc = Grpc::new(ch);
    grpc.ready().await.unwrap();
    grpc.unary(
        Request::new(Bytes::from(body)),
        PathAndQuery::from_static(EXPORT),
        BytesCodec,
    )
    .await
    .unwrap();
}

async fn http(addr: SocketAddr, target: &str) -> Resp {
    common::http(addr, "GET", target, "Content-Length: 0\r\n").await
}

fn opts(dir: &Path) -> ServerOptions {
    let mut o = common::opts(dir);
    // A flush soon after data arrives, so that it is searchable.
    o.engine.flush_interval = Duration::from_millis(100);
    o
}

fn kv(key: &str, v: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(V::StringValue(v.into())),
        }),
        ..Default::default()
    }
}

/// One span with a prompt (a content key, so the text is stored in pieces).
fn prompt_span(trace: u8, span: u8, start: u64, prompt: &str) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![span; 8],
        name: format!("chat {span}"),
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1_000,
        attributes: vec![
            kv("gen_ai.prompt", prompt),
            kv("gen_ai.request.model", "gpt-test"),
            kv("gen_ai.operation.name", "chat"),
        ],
        ..Default::default()
    }
}

fn request(spans: Vec<Span>) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "agent")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

/// Searches until the data sent so far is flushed, and returns the response.
async fn search_flushed(api: SocketAddr, target: &str, at_least: usize) -> Value {
    for _ in 0..200 {
        let r = http(api, target).await;
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
        let v = r.json();
        if v["spans"].as_array().unwrap().len() >= at_least {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{target}: not searchable after 10 seconds");
}

async fn get(api: SocketAddr, target: &str) -> Value {
    let r = http(api, target).await;
    assert_eq!(
        r.status,
        200,
        "{target}: {}",
        String::from_utf8_lossy(&r.body)
    );
    r.json()
}

async fn count(api: SocketAddr, text: &str) -> usize {
    get(api, &format!("/api/v1/search?q={text}")).await["spans"]
        .as_array()
        .unwrap()
        .len()
}

fn trace_hex(n: u8) -> String {
    format!("{n:02x}").repeat(16)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_returns_spans_newest_first_with_the_position_searched() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let api = server.api_addr();
    let start = now_ns() - 600_000_000_000;

    // Nothing flushed: no hits, and the position says nothing was searched.
    let r = http(api, "/api/v1/search?q=weather").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["spans"].as_array().unwrap().len(), 0);
    assert_eq!(r.json()["searched_through_lsn"], 0);

    export(
        server.otlp_grpc_addr(),
        request(vec![
            prompt_span(1, 1, start, "what is the weather in Paris today"),
            prompt_span(2, 1, start + 5_000, "the weather in London is rainy"),
            prompt_span(3, 1, start + 9_000, "nothing about the sky here at all"),
        ]),
    )
    .await;
    let v = search_flushed(api, "/api/v1/search?q=weather", 2).await;
    let spans = v["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0]["trace_id"], trace_hex(2));
    assert_eq!(spans[0]["span_id"], "01".repeat(8));
    assert_eq!(spans[0]["start_ns"], start + 5_000);
    assert_eq!(spans[0]["end_ns"], start + 6_000);
    assert_eq!(spans[0]["name"], "chat 1");
    assert_eq!(spans[0]["service_name"], "agent");
    assert_eq!(spans[0]["operation"], "chat");
    assert_eq!(spans[0]["request_model"], "gpt-test");
    assert_eq!(spans[1]["trace_id"], trace_hex(1));
    assert!(v["searched_through_lsn"].as_u64().unwrap() >= 1);

    // The grammar: AND, OR, a negation, a phrase, a prefix, parentheses.
    let ids = |v: &Value| -> Vec<String> {
        v["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["trace_id"].as_str().unwrap().to_owned())
            .collect()
    };
    let q = |text: &str| format!("/api/v1/search?q={text}");
    assert_eq!(ids(&get(api, &q("weather+paris")).await), [trace_hex(1)]);
    assert_eq!(
        ids(&get(api, &q("paris+OR+london")).await),
        [trace_hex(2), trace_hex(1)]
    );
    assert_eq!(ids(&get(api, &q("weather+-paris")).await), [trace_hex(2)]);
    assert_eq!(
        ids(&get(api, &q("%22weather+in+london%22")).await),
        [trace_hex(2)]
    );
    assert!(ids(&get(api, &q("%22london+in+weather%22")).await).is_empty());
    assert_eq!(ids(&get(api, &q("rain*")).await), [trace_hex(2)]);
    assert_eq!(
        ids(&get(api, &q("(paris+OR+london)+-rainy")).await),
        [trace_hex(1)]
    );
    // limit, from and to.
    let limited = get(api, "/api/v1/search?q=weather&limit=1").await;
    assert_eq!(ids(&limited), [trace_hex(2)]);
    let ranged = get(
        api,
        &format!(
            "/api/v1/search?q=weather&from={}&to={}",
            start,
            start + 5_000
        ),
    )
    .await;
    assert_eq!(ids(&ranged), [trace_hex(1)]);

    // After a restart the same answer.
    server.shutdown().await.unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let again = http(server.api_addr(), "/api/v1/search?q=weather")
        .await
        .json();
    assert_eq!(again["spans"], v["spans"]);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn percent_encoded_and_non_ascii_queries() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let api = server.api_addr();
    let start = now_ns() - 600_000_000_000;
    export(
        server.otlp_grpc_addr(),
        request(vec![
            prompt_span(1, 1, start, "un caf\u{e9} cr\u{e8}me, s'il vous pla\u{ee}t"),
            prompt_span(
                2,
                1,
                start + 1,
                "\u{65e5}\u{672c}\u{8a9e} text with Ünïcode letters",
            ),
            prompt_span(3, 1, start + 2, "plain ascii words only here"),
        ]),
    )
    .await;
    search_flushed(api, "/api/v1/search?q=plain", 1).await;
    // UTF-8 percent-encoded: café, 日本語, Ünïcode (case is folded for ASCII
    // only, so the capital U of the text and the query agree).
    assert_eq!(count(api, "caf%C3%A9").await, 1);
    assert_eq!(count(api, "%E6%97%A5%E6%9C%AC%E8%AA%9E").await, 1);
    assert_eq!(count(api, "%C3%9Cn%C3%AFcode").await, 1);
    // A space as %20 and as +, quotes as %22.
    assert_eq!(count(api, "%22un%20caf%C3%A9%22").await, 1);
    assert_eq!(count(api, "%22un+caf%C3%A9%22").await, 1);
    assert_eq!(count(api, "plain%20ascii").await, 1);
    assert_eq!(count(api, "PLAIN").await, 1);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_error_is_a_400_with_a_message() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(opts(dir.path())).await.unwrap();
    let api = server.api_addr();
    let start = now_ns() - 600_000_000_000;
    // One prompt of 1,100 distinct words with the same prefix: a prefix query
    // over it expands past the limit of 1,024 terms.
    let words: Vec<String> = (0..1_100).map(|i| format!("zzword{i:04}")).collect();
    export(
        server.otlp_grpc_addr(),
        request(vec![prompt_span(1, 1, start, &words.join(" "))]),
    )
    .await;
    search_flushed(api, "/api/v1/search?q=zzword0001", 1).await;

    for (target, what) in [
        ("/api/v1/search", "a missing q"),
        ("/api/v1/search?q=", "an empty q"),
        ("/api/v1/search?q=%20%20", "a q of spaces"),
        ("/api/v1/search?q=%22unterminated", "an unterminated phrase"),
        ("/api/v1/search?q=(a+b", "an unterminated group"),
        ("/api/v1/search?q=a+b)", "a stray parenthesis"),
        ("/api/v1/search?q=-a", "only a negation"),
        ("/api/v1/search?q=a+OR", "OR without an operand"),
        ("/api/v1/search?q=a*", "a prefix of one letter"),
        ("/api/v1/search?q=a+b&limit=0", "limit 0"),
        ("/api/v1/search?q=a+b&limit=1001", "limit 1001"),
        (
            "/api/v1/search?q=a+b&limit=x",
            "a limit that is not a number",
        ),
        ("/api/v1/search?q=a+b&from=abc", "a bad from"),
        ("/api/v1/search?q=a+b&to=1.5", "a bad to"),
        ("/api/v1/search?q=a+b&qq=1", "an unknown parameter"),
        ("/api/v1/search?q=%zz", "bad percent-encoding"),
        ("/api/v1/search?q=%FF%FE", "a q that is not UTF-8"),
    ] {
        let r = http(api, target).await;
        assert_eq!(
            r.status,
            400,
            "{what}: {}",
            String::from_utf8_lossy(&r.body)
        );
        assert!(
            r.json()["error"].as_str().is_some_and(|m| !m.is_empty()),
            "{what}"
        );
    }
    let long = "a".repeat(17_000);
    let r = http(api, &format!("/api/v1/search?q={long}")).await;
    assert_eq!(r.status, 400, "a query over 16 KiB");

    // A query over a limit of the search: the message says which.
    let r = http(api, "/api/v1/search?q=zzword*").await;
    assert_eq!(r.status, 400);
    let msg = r.json()["error"].as_str().unwrap().to_owned();
    assert!(msg.contains("prefix") && msg.contains("1024"), "{msg}");
    // The server is unharmed, and the maximum limit is accepted.
    assert_eq!(
        http(api, "/api/v1/search?q=zzword0007&limit=1000")
            .await
            .status,
        200
    );
    server.shutdown().await.unwrap();
}
