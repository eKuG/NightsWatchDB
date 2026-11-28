//! `POST /api/v1/annotations` against a real server, and how annotations show
//! in the trace view, the list and the stats.

use bytes::{Buf, BufMut, Bytes};
use nwdb_server::{Server, ServerOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const MAX_BODY: usize = 1024 * 1024;

// ---- clients ----

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

struct Resp {
    status: u16,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }
    fn error(&self) -> String {
        self.json()["error"].as_str().unwrap().to_owned()
    }
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let eol = raw.windows(2).position(|w| w == b"\r\n").unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&raw[..eol]).unwrap().trim(), 16).unwrap();
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&raw[eol + 2..eol + 2 + size]);
        raw = &raw[eol + 2 + size + 2..];
    }
}

/// One request on its own connection. `declared` replaces the body's length
/// in `Content-Length`.
fn send_blocking(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
    declared: Option<usize>,
) -> Resp {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut head = format!(
        "{method} {target} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n",
        declared.unwrap_or(body.len())
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    // The server may close with the rest of a refused body unread.
    let _ = s.read_to_end(&mut raw);
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(raw[..split].to_vec()).unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Resp {
        status,
        body: if head.to_lowercase().contains("transfer-encoding: chunked") {
            dechunk(&raw[split + 4..])
        } else {
            raw[split + 4..].to_vec()
        },
    }
}

async fn send(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
    declared: Option<usize>,
) -> Resp {
    let (method, target) = (method.to_owned(), target.to_owned());
    let headers: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        send_blocking(addr, &method, &target, &headers, &body, declared)
    })
    .await
    .unwrap()
}

const JSON: (&str, &str) = ("Content-Type", "application/json");

async fn get(api: SocketAddr, target: &str) -> Resp {
    send(api, "GET", target, &[], Vec::new(), None).await
}

async fn get_json(api: SocketAddr, target: &str) -> Value {
    let r = get(api, target).await;
    assert_eq!(
        r.status,
        200,
        "{target}: {}",
        String::from_utf8_lossy(&r.body)
    );
    r.json()
}

async fn post_bytes(api: SocketAddr, body: &[u8]) -> Resp {
    send(
        api,
        "POST",
        "/api/v1/annotations",
        &[JSON],
        body.to_vec(),
        None,
    )
    .await
}

async fn post(api: SocketAddr, body: &Value) -> Resp {
    post_bytes(api, body.to_string().as_bytes()).await
}

/// Posts items and expects a 200 with nothing rejected.
async fn post_ok(api: SocketAddr, items: Vec<Value>) {
    let n = items.len();
    let r = post(api, &json!({ "annotations": items })).await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json(), json!({ "accepted": n, "rejected": [] }));
}

// ---- data ----

fn opts(dir: &Path) -> ServerOptions {
    let mut o = ServerOptions::new(dir);
    o.otlp_grpc_addr = "127.0.0.1:0".parse().unwrap();
    o.api_addr = "127.0.0.1:0".parse().unwrap();
    // Nothing is flushed during a test, so everything stays in the buffers.
    o.engine.flush_interval = Duration::from_secs(3600);
    o
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
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

fn span(trace: u8, span: u8, start: u64) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![span; 8],
        name: format!("op {span}"),
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1_000,
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

fn trace_hex(n: u8) -> String {
    format!("{n:02x}").repeat(16)
}

fn span_hex(n: u8) -> String {
    format!("{n:02x}").repeat(8)
}

/// Starts a server and ingests the traces 1 to `traces`, two spans each
/// (span ids 1 and 2).
async fn server_with(dir: &Path, traces: u8) -> Server {
    server_with_options(opts(dir), traces).await
}

async fn server_with_options(o: ServerOptions, traces: u8) -> Server {
    let server = Server::start(o).await.unwrap();
    let start = now_ns() - 600_000_000_000;
    for t in 1..=traces {
        let spans = vec![
            span(t, 1, start + u64::from(t) * 10_000),
            span(t, 2, start + u64::from(t) * 10_000 + 500),
        ];
        export(server.otlp_grpc_addr(), request(spans)).await;
    }
    server
}

fn score(trace: u8, name: &str, value: f64) -> Value {
    json!({ "trace_id": trace_hex(trace), "name": name, "value": value })
}

async fn annotations_of(api: SocketAddr, trace: u8) -> Vec<Value> {
    let v = get_json(api, &format!("/api/v1/traces/{}", trace_hex(trace))).await;
    v.get("annotations")
        .map(|a| a.as_array().unwrap().clone())
        .unwrap_or_default()
}

fn trace_ids(v: &Value) -> BTreeSet<String> {
    v["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["trace_id"].as_str().unwrap().to_owned())
        .collect()
}

// ---- the happy path ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn annotate_read_back_overwrite_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();

    let before = now_ns() as i64;
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "judge",
                   "value": 0.9, "label": "good", "comment": "ok\nline two"}),
            json!({"trace_id": trace_hex(1), "span_id": span_hex(1), "name": "relevance",
                   "value": 3}),
        ],
    )
    .await;
    let after = now_ns() as i64;

    // A trace-level annotation comes before a span-level one.
    let got = annotations_of(api, 1).await;
    assert_eq!(got.len(), 2);
    let a = &got[0];
    assert_eq!(a["name"], "quality");
    assert_eq!(a["source"], "judge");
    assert_eq!(a["value"], 0.9);
    assert_eq!(a["label"], "good");
    assert_eq!(a["comment"], "ok\nline two");
    assert!(a.get("span_id").is_none());
    // The server filled in the time.
    let t = a["time_ns"].as_i64().unwrap();
    assert!(
        (before..=after).contains(&t),
        "{t} not in {before}..={after}"
    );
    let b = &got[1];
    assert_eq!(b["span_id"], span_hex(1));
    assert_eq!(b["name"], "relevance");
    assert_eq!(b["value"], 3.0);
    // Fields that are not there are left out.
    for k in ["source", "label", "comment"] {
        assert!(b.get(k).is_none(), "{k}");
    }
    assert!(
        get_json(api, &format!("/api/v1/traces/{}", trace_hex(1))).await["spans"]
            .as_array()
            .unwrap()
            .len()
            == 2
    );

    // The list shows them too.
    let list = get_json(api, "/api/v1/traces").await;
    assert_eq!(list["traces"][0]["annotations"], json!(got));
    assert!(list["traces"][0].get("annotations_truncated").is_none());

    // The latest write of one identity wins, and a given time is kept.
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "judge",
                    "value": 0.1, "time_ns": 12345}),
        ],
    )
    .await;
    let got = annotations_of(api, 1).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["value"], 0.1);
    assert_eq!(got[0]["time_ns"], 12345);
    // The overwrite carries no label: it replaced the whole record.
    assert!(got[0].get("label").is_none());

    // Another source is another annotation.
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "human",
                    "label": "fine"}),
        ],
    )
    .await;
    assert_eq!(annotations_of(api, 1).await.len(), 3);

    // Delete, one identity at a time; the last one takes the fields away.
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "judge", "delete": true}),
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "human", "delete": true}),
        ],
    )
    .await;
    let got = annotations_of(api, 1).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["name"], "relevance");
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "span_id": span_hex(1), "name": "relevance",
                    "delete": true}),
        ],
    )
    .await;
    let v = get_json(api, &format!("/api/v1/traces/{}", trace_hex(1))).await;
    assert!(v.get("annotations").is_none());
    assert!(v.get("annotations_truncated").is_none());
    let list = get_json(api, "/api/v1/traces").await;
    assert!(list["traces"][0].get("annotations").is_none());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stats_count_buffered_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    assert_eq!(
        get_json(api, "/api/v1/stats").await["annotations_buffered"],
        0
    );
    post_ok(
        api,
        vec![score(1, "a", 1.0), score(1, "b", 2.0), score(1, "c", 3.0)],
    )
    .await;
    assert_eq!(
        get_json(api, "/api/v1/stats").await["annotations_buffered"],
        3
    );
    server.shutdown().await.unwrap();
}

// ---- durability ----

/// A sync hook that waits while its gate is closed.
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn set(&self, open: bool) {
        *self.open.lock().unwrap() = open;
        self.cv.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reply_comes_only_after_the_log_sync() {
    let gate = Arc::new(Gate {
        open: Mutex::new(true),
        cv: Condvar::new(),
    });
    let g = gate.clone();
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.engine.wal.sync_hook = Some(Arc::new(move || {
        let mut open = g.open.lock().unwrap();
        while !*open {
            open = g.cv.wait(open).unwrap();
        }
        Ok(())
    }));
    let server = server_with_options(o, 1).await;
    let api = server.api_addr();

    gate.set(false);
    let (tx, rx) = mpsc::channel();
    let body = json!({"annotations": [score(1, "q", 1.0)]})
        .to_string()
        .into_bytes();
    std::thread::spawn(move || {
        let r = send_blocking(
            api,
            "POST",
            "/api/v1/annotations",
            &[("Content-Type".into(), "application/json".into())],
            &body,
            None,
        );
        let _ = tx.send(r.status);
    });
    // No answer while the sync is held, and the annotation is not visible.
    assert!(rx.recv_timeout(Duration::from_millis(1000)).is_err());
    assert!(annotations_of(api, 1).await.is_empty());
    gate.set(true);
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)).unwrap(), 200);
    assert_eq!(annotations_of(api, 1).await.len(), 1);
    server.shutdown().await.unwrap();
}

/// A 200 is durable: the process is killed at once and the annotation is
/// there after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_200_survives_kill_9() {
    const BIN: &str = env!("CARGO_BIN_EXE_nightswatchdb");
    let dir = tempfile::tempdir().unwrap();
    let start = |dir: &Path| {
        let mut child = Command::new(BIN)
            .args([
                "--otlp-grpc-addr",
                "127.0.0.1:0",
                "--api-addr",
                "127.0.0.1:0",
            ])
            .arg("--data-dir")
            .arg(dir)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let addrs: Vec<SocketAddr> = loop {
            let line = lines.next().expect("exited before listening").unwrap();
            if let Some(rest) = line.strip_prefix("listening: OTLP gRPC on ") {
                let (door, api) = rest.split_once(", HTTP API on ").unwrap();
                break vec![door.parse().unwrap(), api.parse().unwrap()];
            }
        };
        std::thread::spawn(move || lines.for_each(drop));
        (child, addrs[0], addrs[1])
    };

    let (mut child, door, api) = start(dir.path());
    export(door, request(vec![span(1, 1, now_ns() - 600_000_000_000)])).await;
    post_ok(api, vec![score(1, "q", 0.75)]).await;
    let status = Command::new("kill")
        .args(["-9", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    child.wait().unwrap();

    let (mut child, _, api) = start(dir.path());
    let got = annotations_of(api, 1).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["value"], 0.75);
    child.kill().unwrap();
    child.wait().unwrap();
}

// ---- items that are refused ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn items_are_refused_by_index_and_the_others_are_written() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let t = trace_hex(1);
    let items = vec![
        score(1, "good-0", 1.0),                                         // 0
        json!({"trace_id": trace_hex(0xee), "name": "n", "value": 1}),   // 1 unknown
        json!({"trace_id": "zz".repeat(16), "name": "n", "value": 1}),   // 2 not hex
        json!({"trace_id": "abcd", "name": "n", "value": 1}),            // 3 short
        json!({"trace_id": "0".repeat(32), "name": "n", "value": 1}),    // 4 zero
        json!({"trace_id": t, "name": "n".repeat(129), "value": 1}),     // 5 name
        json!({"trace_id": t, "name": "n", "value": 1, "delete": true}), // 6 both
        json!({"trace_id": t, "name": "n", "comment": "alone"}),         // 7 comment
        json!({"trace_id": t, "span_id": span_hex(2), "name": "good-8", "label": "x"}), // 8
        json!({"trace_id": t, "span_id": "0".repeat(16), "name": "n", "value": 1}), // 9 zero span
        json!({"trace_id": t, "span_id": "12", "name": "n", "value": 1}), // 10 short span
    ];
    let r = post(api, &json!({ "annotations": items })).await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let v = r.json();
    assert_eq!(v["accepted"], 2);
    let rejected = v["rejected"].as_array().unwrap();
    let indexes: Vec<u64> = rejected
        .iter()
        .map(|r| r["index"].as_u64().unwrap())
        .collect();
    assert_eq!(indexes, [1, 2, 3, 4, 5, 6, 7, 9, 10]);
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("unknown trace")
    );
    for r in rejected {
        assert!(!r["error"].as_str().unwrap().is_empty());
    }
    let names: Vec<String> = annotations_of(api, 1)
        .await
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["good-0", "good-8"]);

    // Nothing valid: answered at once, 200.
    let r = post(
        api,
        &json!({"annotations": [
            {"trace_id": trace_hex(0xee), "name": "n", "value": 1},
            {"trace_id": "xx", "name": "n", "value": 1}]}),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["accepted"], 0);
    assert_eq!(r.json()["rejected"].as_array().unwrap().len(), 2);
    server.shutdown().await.unwrap();
}

// ---- requests that are refused ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_that_is_not_of_the_shape_is_a_400() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let t = trace_hex(1);
    let item = format!(r#"{{"trace_id":"{t}","name":"n","value":1}}"#);
    let cases: Vec<(&str, String)> = vec![
        ("invalid JSON", "{".into()),
        ("not an object", "[]".into()),
        ("no list", "{}".into()),
        ("null list", r#"{"annotations":null}"#.into()),
        ("list is an object", r#"{"annotations":{}}"#.into()),
        ("empty list", r#"{"annotations":[]}"#.into()),
        ("trailing text", format!(r#"{{"annotations":[{item}]}} x"#)),
        (
            "unknown top field",
            format!(r#"{{"annotations":[{item}],"extra":1}}"#),
        ),
        (
            "unknown item field",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":1,"x":1}}]}}"#),
        ),
        (
            "top field twice",
            format!(r#"{{"annotations":[{item}],"annotations":[{item}]}}"#),
        ),
        (
            "item field twice",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","name":"m","value":1}}]}}"#),
        ),
        (
            "trace id not a string",
            r#"{"annotations":[{"trace_id":5,"name":"n","value":1}]}"#.into(),
        ),
        (
            "name missing",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","value":1}}]}}"#),
        ),
        (
            "name not a string",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":5,"value":1}}]}}"#),
        ),
        (
            "value not a number",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":"1"}}]}}"#),
        ),
        (
            "delete not a bool",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","delete":"yes"}}]}}"#),
        ),
        (
            "time not an integer",
            format!(
                r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":1,"time_ns":1.5}}]}}"#
            ),
        ),
        (
            "time past 64 bits",
            format!(
                r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":1,"time_ns":99999999999999999999}}]}}"#
            ),
        ),
        (
            "value out of range",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":1e400}}]}}"#),
        ),
        (
            "NaN",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":NaN}}]}}"#),
        ),
        (
            "Infinity",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","value":Infinity}}]}}"#),
        ),
        ("item is not an object", r#"{"annotations":[1]}"#.into()),
        (
            "lone surrogate",
            format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","label":"\ud800"}}]}}"#),
        ),
    ];
    for (what, body) in cases {
        let r = post_bytes(api, body.as_bytes()).await;
        assert_eq!(
            r.status,
            400,
            "{what}: {}",
            String::from_utf8_lossy(&r.body)
        );
        assert!(!r.error().is_empty(), "{what}");
    }
    let r = post_bytes(api, b"").await;
    assert_eq!(r.status, 400);
    assert!(r.error().contains("empty"));
    // Not UTF-8.
    let r = post_bytes(api, b"{\"annotations\":[{\"trace_id\":\"\xff\"}]}").await;
    assert_eq!(r.status, 400);
    // None of them wrote anything.
    assert!(annotations_of(api, 1).await.is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_1000_items() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let tiny = format!(
        r#"{{"trace_id":"{}","name":"n","value":1}}"#,
        trace_hex(0xee)
    );
    let body = |n: usize, tail: &str| {
        let mut items = vec![tiny.clone(); n].join(",");
        items.push_str(tail);
        format!(r#"{{"annotations":[{items}]}}"#)
    };

    // 1000 are fine (here all refused: the trace is unknown).
    let r = post_bytes(api, body(1000, "").as_bytes()).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["rejected"].as_array().unwrap().len(), 1000);

    // 1001 are not.
    let r = post_bytes(api, body(1001, "").as_bytes()).await;
    assert_eq!(r.status, 400);
    assert!(r.error().contains("at most 1000"), "{}", r.error());

    // The 1001st is not even read as an item: it is not one, and the answer
    // is still the limit, not a complaint about its type.
    let r = post_bytes(api, body(1000, ",12345").as_bytes()).await;
    assert_eq!(r.status, 400);
    assert!(r.error().contains("at most 1000"), "{}", r.error());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_write_request_rules_guard_the_route() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let body = json!({"annotations": [score(1, "q", 1.0)]})
        .to_string()
        .into_bytes();
    let path = "/api/v1/annotations";

    let r = send(
        api,
        "POST",
        path,
        &[("Content-Type", "text/plain")],
        body.clone(),
        None,
    )
    .await;
    assert_eq!(r.status, 415);
    let r = send(
        api,
        "POST",
        path,
        &[JSON, ("Origin", "http://evil.example")],
        body.clone(),
        None,
    )
    .await;
    assert_eq!(r.status, 403);
    // Declared over the limit: refused before the body is read.
    let r = send(api, "POST", path, &[JSON], Vec::new(), Some(MAX_BODY + 1)).await;
    assert_eq!(r.status, 413);
    // Other methods are still 405.
    assert_eq!(get(api, path).await.status, 405);
    // None of these wrote anything.
    assert!(annotations_of(api, 1).await.is_empty());
    server.shutdown().await.unwrap();
}

// ---- the engine says no ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_engine_is_a_503() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let engine = server.engine().clone();
    tokio::task::spawn_blocking(move || engine.shutdown())
        .await
        .unwrap()
        .unwrap();
    let r = post(api, &json!({"annotations": [score(1, "q", 1.0)]})).await;
    assert_eq!(r.status, 503, "{}", String::from_utf8_lossy(&r.body));
    assert!(!r.error().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_engine_refuses_what_it_would_log_and_answers_what_it_would_not() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    // The first request into an empty buffer is taken; after it the buffer
    // is over this.
    o.engine.max_buffered_bytes = 1;
    let server = server_with_options(o, 1).await;
    let api = server.api_addr();

    let r = post(api, &json!({"annotations": [score(1, "q", 1.0)]})).await;
    assert_eq!(r.status, 503, "{}", String::from_utf8_lossy(&r.body));

    // Nothing to log: no admission, so the rejections are answered.
    let r = post(
        api,
        &json!({"annotations": [
            {"trace_id": trace_hex(0xee), "name": "n", "value": 1},
            {"trace_id": "bad", "name": "n", "value": 1}]}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["accepted"], 0);
    assert_eq!(r.json()["rejected"].as_array().unwrap().len(), 2);
    server.shutdown().await.unwrap();
}

// ---- the trace view ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_trace_view_truncates_at_1001_and_protobuf_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 2).await;
    let api = server.api_addr();
    let target = format!("/api/v1/traces/{}", trace_hex(1));

    let pb = |api| async move {
        let r = send(
            api,
            "GET",
            &format!("/api/v1/traces/{}", trace_hex(1)),
            &[("Accept", "application/x-protobuf")],
            Vec::new(),
            None,
        )
        .await;
        assert_eq!(r.status, 200);
        r.body
    };
    let before = pb(api).await;
    // A trace without annotations has neither field.
    let v = get_json(api, &target).await;
    assert!(v.get("annotations").is_none() && v.get("annotations_truncated").is_none());

    let batch = |from: usize, to: usize| -> Vec<Value> {
        (from..to)
            .map(|i| score(1, &format!("n{i:04}"), i as f64))
            .collect()
    };
    post_ok(api, batch(0, 1000)).await;
    let v = get_json(api, &target).await;
    assert_eq!(v["annotations"].as_array().unwrap().len(), 1000);
    assert!(v.get("annotations_truncated").is_none());
    // The spans come first, then the annotations.
    let text = String::from_utf8(get(api, &target).await.body).unwrap();
    assert!(text.find("\"spans\"").unwrap() < text.find("\"annotations\"").unwrap());

    post_ok(api, batch(1000, 1001)).await;
    let v = get_json(api, &target).await;
    assert_eq!(v["annotations"].as_array().unwrap().len(), 1000);
    assert_eq!(v["annotations_truncated"], true);
    // Identity order: the 1001st name is the one that is cut.
    assert_eq!(v["annotations"][999]["name"], "n0999");
    assert_eq!(v["spans"].as_array().unwrap().len(), 2);

    // The protobuf form is the same bytes, and so is another trace's JSON.
    assert_eq!(pb(api).await, before);
    let other = get_json(api, &format!("/api/v1/traces/{}", trace_hex(2))).await;
    assert!(other.get("annotations").is_none());
    server.shutdown().await.unwrap();
}

// ---- the list ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_list_filters_by_annotation() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 4).await;
    let api = server.api_addr();
    post_ok(
        api,
        vec![
            json!({"trace_id": trace_hex(1), "name": "quality", "source": "judge",
                   "value": 0.9, "label": "good"}),
            json!({"trace_id": trace_hex(2), "name": "quality", "source": "human",
                   "value": 0.2, "label": "bad"}),
            json!({"trace_id": trace_hex(3), "name": "other", "value": 5}),
        ],
    )
    .await;
    let ids = |nums: &[u8]| -> BTreeSet<String> { nums.iter().map(|&n| trace_hex(n)).collect() };
    let list = |q: &'static str| async move {
        trace_ids(&get_json(api, &format!("/api/v1/traces?{q}")).await)
    };

    assert_eq!(list("").await, ids(&[1, 2, 3, 4]));
    assert_eq!(list("annotation=quality").await, ids(&[1, 2]));
    assert_eq!(list("annotation=other").await, ids(&[3]));
    assert_eq!(list("annotation=missing").await, ids(&[]));
    assert_eq!(
        list("annotation=quality&annotation_source=judge").await,
        ids(&[1])
    );
    assert_eq!(
        list("annotation=quality&annotation_min=0.5").await,
        ids(&[1])
    );
    assert_eq!(
        list("annotation=quality&annotation_max=0.5").await,
        ids(&[2])
    );
    assert_eq!(
        list("annotation=quality&annotation_min=0.1&annotation_max=0.95").await,
        ids(&[1, 2])
    );
    assert_eq!(
        list("annotation=quality&annotation_label=bad").await,
        ids(&[2])
    );
    assert_eq!(
        list("annotation=quality&annotation_label=bad&annotation_source=judge").await,
        ids(&[])
    );

    // A trace shows all its annotations, not only the one that matched.
    post_ok(api, vec![score(1, "second", 7.0)]).await;
    let v = get_json(api, "/api/v1/traces?annotation=quality&annotation_min=0.5").await;
    assert_eq!(v["traces"].as_array().unwrap().len(), 1);
    assert_eq!(v["traces"][0]["annotations"].as_array().unwrap().len(), 2);

    // The others without `annotation`.
    for q in [
        "annotation_source=judge",
        "annotation_min=1",
        "annotation_max=1",
        "annotation_label=good",
    ] {
        let r = get(api, &format!("/api/v1/traces?{q}")).await;
        assert_eq!(r.status, 400, "{q}");
        assert!(r.error().contains("`annotation`"), "{q}: {}", r.error());
    }
    // Numbers that are not finite numbers.
    for q in [
        "annotation_min=abc",
        "annotation_min=",
        "annotation_min=inf",
        "annotation_min=NaN",
        "annotation_min=-Infinity",
        "annotation_min=1e400",
        "annotation_max=abc",
        "annotation_max=inf",
        "annotation_max=1e999",
    ] {
        let r = get(api, &format!("/api/v1/traces?annotation=quality&{q}")).await;
        assert_eq!(r.status, 400, "{q}");
        assert!(r.error().contains("finite"), "{q}: {}", r.error());
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_list_shows_at_most_32_annotations_per_trace() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let batch = |from: usize, to: usize| -> Vec<Value> {
        (from..to)
            .map(|i| score(1, &format!("n{i:02}"), i as f64))
            .collect()
    };
    post_ok(api, batch(0, 32)).await;
    let v = get_json(api, "/api/v1/traces").await;
    assert_eq!(v["traces"][0]["annotations"].as_array().unwrap().len(), 32);
    assert!(v["traces"][0].get("annotations_truncated").is_none());

    post_ok(api, batch(32, 33)).await;
    let v = get_json(api, "/api/v1/traces").await;
    assert_eq!(v["traces"][0]["annotations"].as_array().unwrap().len(), 32);
    assert_eq!(v["traces"][0]["annotations_truncated"], true);
    assert_eq!(v["traces"][0]["annotations"][31]["name"], "n31");
    // The trace view still has all 33.
    assert_eq!(annotations_of(api, 1).await.len(), 33);
    server.shutdown().await.unwrap();
}

// ---- hostile input ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_json_gets_a_400_or_a_rejected_item() {
    let dir = tempfile::tempdir().unwrap();
    let server = server_with(dir.path(), 1).await;
    let api = server.api_addr();
    let t = trace_hex(1);

    // Deep nesting, in the list and inside a field.
    for body in [
        format!(r#"{{"annotations":{}"#, "[".repeat(100_000)),
        format!(
            r#"{{"annotations":[{{"trace_id":"{t}","name":{}}}]}}"#,
            "[".repeat(100_000)
        ),
        format!(r#"{{"annotations":[{}"#, "{\"a\":".repeat(50_000)),
        "[".repeat(MAX_BODY - 10),
    ] {
        let r = post_bytes(api, body.as_bytes()).await;
        assert_eq!(r.status, 400);
        assert!(!r.error().is_empty());
    }

    // A string of about 1 MiB is a label that is too long: a rejected item.
    let big = "x".repeat(MAX_BODY - 200);
    let body = format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"n","label":"{big}"}}]}}"#);
    assert!(body.len() <= MAX_BODY);
    let r = post_bytes(api, body.as_bytes()).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["accepted"], 0);
    assert_eq!(r.json()["rejected"][0]["index"], 0);

    // An escaped control character is valid JSON and a rejected item.
    let r = post_bytes(
        api,
        format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"a\u0000b","value":1}}]}}"#)
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["accepted"], 0);

    // A raw control character inside a string is not JSON.
    let r = post_bytes(
        api,
        format!("{{\"annotations\":[{{\"trace_id\":\"{t}\",\"name\":\"a\nb\",\"value\":1}}]}}")
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);

    // A well-formed pair of surrogates is a character.
    post_ok(
        api,
        vec![json!({"trace_id": t, "name": "n", "label": "\u{1F600}"})],
    )
    .await;
    let r = post_bytes(
        api,
        format!(r#"{{"annotations":[{{"trace_id":"{t}","name":"m","label":"😀"}}]}}"#).as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["accepted"], 1);

    // The server is still up and answers.
    assert_eq!(get(api, "/healthz").await.status, 200);
    server.shutdown().await.unwrap();
}
