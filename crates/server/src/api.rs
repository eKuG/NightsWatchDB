//! The HTTP query API. Every response is JSON except `/healthz` and the
//! protobuf form of a trace. Engine calls block and may read files, so each
//! runs on the blocking pool.

use axum::Router;
use axum::extract::{Path, RawQuery, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use nwdb_core::{Error, SpanColumns, SpanRecord, TraceId, TraceSummary};
use nwdb_engine::{AggQuery, Engine, ListQuery};
use nwdb_otlp::{attrs_to_json, encode_spans, events_to_json, links_to_json, resource_to_json};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

const HOUR_NS: i64 = 3_600_000_000_000;
const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 1000;
const PROTOBUF: &str = "application/x-protobuf";

#[derive(Clone)]
struct Api {
    engine: Engine,
    fatal: Arc<Notify>,
}

/// The routes of the API.
pub(crate) fn router(engine: Engine, fatal: Arc<Notify>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/traces", get(list_traces))
        .route("/api/v1/traces/{trace_id}", get(get_trace))
        .route("/api/v1/tokens", get(tokens))
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "no such path") })
        .method_not_allowed_fallback(|| async {
            ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "only GET is supported")
        })
        .with_state(Api { engine, fatal })
}

// ---- errors ----

struct ApiError(StatusCode, String);

impl ApiError {
    fn new(code: StatusCode, msg: impl Into<String>) -> Self {
        Self(code, msg.into())
    }

    fn bad(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        json_response(self.0, &json!({ "error": self.1 }))
    }
}

fn json_response(code: StatusCode, body: &Value) -> Response {
    json_bytes(code, body.to_string().into_bytes())
}

fn json_bytes(code: StatusCode, body: Vec<u8>) -> Response {
    (code, [(CONTENT_TYPE, "application/json")], body).into_response()
}

fn status_of(e: &Error) -> StatusCode {
    match e {
        Error::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        Error::NotFound(_) => StatusCode::NOT_FOUND,
        Error::Unavailable(_) | Error::Fatal(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl Api {
    fn error(&self, e: &Error) -> ApiError {
        if matches!(e, Error::Fatal(_)) {
            self.fatal.notify_one();
        }
        ApiError::new(status_of(e), e.to_string())
    }

    /// Runs `f` on the blocking pool.
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(Engine) -> nwdb_core::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let engine = self.engine.clone();
        match tokio::task::spawn_blocking(move || f(engine)).await {
            Ok(r) => r.map_err(|e| self.error(&e)),
            Err(_) => Err(ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the query failed",
            )),
        }
    }
}

// ---- query strings ----

/// The parameters of a request. A name that is not taken by the handler is an
/// error, so that a misspelt filter does not silently return everything.
struct Params(Vec<(String, String)>);

impl Params {
    fn parse(raw: Option<String>) -> Result<Self, ApiError> {
        let mut pairs = Vec::new();
        for part in raw
            .as_deref()
            .unwrap_or("")
            .split('&')
            .filter(|p| !p.is_empty())
        {
            let (k, v) = part.split_once('=').unwrap_or((part, ""));
            match (percent_decode(k), percent_decode(v)) {
                (Some(k), Some(v)) => pairs.push((k, v)),
                _ => {
                    return Err(ApiError::bad(
                        "the query string is not valid percent-encoding",
                    ));
                }
            }
        }
        Ok(Self(pairs))
    }

    /// Removes and returns the last value of `name`.
    fn take(&mut self, name: &str) -> Option<String> {
        let found = self.0.iter().rposition(|(k, _)| k == name)?;
        let value = self.0.remove(found).1;
        self.0.retain(|(k, _)| k != name);
        Some(value)
    }

    fn int(&mut self, name: &str) -> Result<Option<i64>, ApiError> {
        self.take(name)
            .map(|v| {
                v.parse()
                    .map_err(|_| ApiError::bad(format!("`{name}` must be an integer")))
            })
            .transpose()
    }

    /// `from` and `to` in Unix nanoseconds; `to` defaults to now and `from` to
    /// an hour before `to`.
    fn range(&mut self) -> Result<(i64, i64), ApiError> {
        let to = self.int("to")?.unwrap_or_else(now_ns);
        let from = self
            .int("from")?
            .unwrap_or_else(|| to.saturating_sub(HOUR_NS));
        Ok((from, to))
    }

    fn finish(self) -> Result<(), ApiError> {
        match self.0.first() {
            Some((k, _)) => Err(ApiError::bad(format!("unknown parameter `{k}`"))),
            None => Ok(()),
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(i64::MAX)
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

// ---- handlers ----

async fn stats(State(api): State<Api>) -> Response {
    let s = api.engine.stats();
    json_response(
        StatusCode::OK,
        &json!({
            "records_ingested": s.records_ingested,
            "bytes_ingested": s.bytes_ingested,
            "acked_lsn": s.acked_lsn,
            "flushed_lsn": s.flushed_lsn,
            "buffered_bytes": s.buffered_bytes,
            "parts": s.parts,
            "flushes": s.flushes,
            "flushing": s.flushing,
            "last_error": s.last_error,
        }),
    )
}

async fn get_trace(
    State(api): State<Api>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let trace_id: TraceId = id.parse().map_err(|e| api.error(&e))?;
    let protobuf = headers
        .get_all(ACCEPT)
        .iter()
        .any(|v| v.to_str().is_ok_and(|v| v.contains(PROTOBUF)));
    let body = api
        .run(move |engine| {
            let batches = engine
                .get_trace(trace_id)?
                .filter(|b| b.iter().any(|b| b.num_rows() > 0))
                .ok_or_else(|| Error::not_found(format!("trace {trace_id}")))?;
            if protobuf {
                encode_spans(&batches)
            } else {
                render_trace(trace_id, &batches)
            }
        })
        .await?;
    Ok(if protobuf {
        (StatusCode::OK, [(CONTENT_TYPE, PROTOBUF)], body).into_response()
    } else {
        json_bytes(StatusCode::OK, body)
    })
}

/// Writes the trace one span at a time, so the JSON of the whole trace is
/// never held as a tree of values.
fn render_trace(id: TraceId, batches: &[arrow_array::RecordBatch]) -> nwdb_core::Result<Vec<u8>> {
    let mut out = format!("{{\"trace_id\":\"{id}\",\"spans\":[").into_bytes();
    let mut first = true;
    for batch in batches {
        let cols = SpanColumns::try_new(batch)?;
        for row in 0..cols.len() {
            if !first {
                out.push(b',');
            }
            first = false;
            serde_json::to_writer(&mut out, &span_json(&cols.record(row))?)
                .map_err(Error::other)?;
        }
    }
    out.extend_from_slice(b"]}");
    Ok(out)
}

fn span_json(r: &SpanRecord<'_>) -> nwdb_core::Result<Value> {
    let mut m = Map::new();
    let mut put = |k: &str, v: Value| m.insert(k.to_owned(), v);
    put("span_id", r.span_id.to_string().into());
    if let Some(p) = r.parent_span_id {
        put("parent_span_id", p.to_string().into());
    }
    put("name", r.name.into());
    put("kind", r.kind.into());
    put("start_ns", r.start_ns.into());
    put("end_ns", r.end_ns.into());
    put("status_code", r.status_code.into());
    let strings = [
        ("status_message", r.status_message),
        ("trace_state", r.trace_state),
        ("service_name", r.service_name),
        ("scope_name", r.scope_name),
        ("scope_version", r.scope_version),
        ("operation", r.operation),
        ("provider", r.provider),
        ("request_model", r.request_model),
        ("response_model", r.response_model),
        ("conversation_id", r.conversation_id),
        ("session_id", r.session_id),
        ("user_id", r.user_id),
        ("agent_name", r.agent_name),
        ("tool_name", r.tool_name),
        ("error_type", r.error_type),
    ];
    for (k, v) in strings {
        if let Some(v) = v {
            put(k, v.into());
        }
    }
    for (k, v) in [
        ("input_tokens", r.input_tokens),
        ("output_tokens", r.output_tokens),
    ] {
        if let Some(v) = v {
            put(k, v.into());
        }
    }
    for (k, v) in [
        ("flags", r.flags),
        ("dropped_attributes_count", r.dropped_attributes_count),
        ("dropped_events_count", r.dropped_events_count),
        ("dropped_links_count", r.dropped_links_count),
    ] {
        if v != 0 {
            put(k, v.into());
        }
    }
    if let Some(res) = r.resource {
        put("resource", resource_to_json(res)?);
    }
    put("attributes", attrs_to_json(r.attrs)?);
    if let Some(e) = r.events {
        put("events", events_to_json(e)?);
    }
    if let Some(l) = r.links {
        put("links", links_to_json(l)?);
    }
    Ok(Value::Object(m))
}

async fn list_traces(
    State(api): State<Api>,
    RawQuery(raw): RawQuery,
) -> Result<Response, ApiError> {
    let mut p = Params::parse(raw)?;
    let (from_ns, to_ns) = p.range()?;
    let error = match p.take("error").as_deref() {
        None => None,
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(_) => return Err(ApiError::bad("`error` must be true or false")),
    };
    let limit = match p.take("limit") {
        None => DEFAULT_LIMIT,
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| ApiError::bad(format!("`limit` must be 1 to {MAX_LIMIT}")))?,
    };
    let query = ListQuery {
        from_ns,
        to_ns,
        service: p.take("service"),
        model: p.take("model"),
        error,
        limit,
    };
    p.finish()?;
    let traces = api.run(move |e| e.list_traces(&query)).await?;
    let traces: Vec<Value> = traces.iter().map(summary_json).collect();
    Ok(json_response(StatusCode::OK, &json!({ "traces": traces })))
}

fn summary_json(s: &TraceSummary) -> Value {
    let mut m = Map::new();
    m.insert("trace_id".into(), s.trace_id.to_string().into());
    m.insert("start_ns".into(), s.start_ns.into());
    m.insert("end_ns".into(), s.end_ns.into());
    for (k, v) in [
        ("root_service", &s.root_service),
        ("root_name", &s.root_name),
        ("session_id", &s.session_id),
        ("user_id", &s.user_id),
    ] {
        if let Some(v) = v {
            m.insert(k.into(), v.as_str().into());
        }
    }
    m.insert("span_count".into(), s.span_count.into());
    m.insert("error".into(), s.error.into());
    m.insert("input_tokens".into(), s.input_tokens.into());
    m.insert("output_tokens".into(), s.output_tokens.into());
    m.insert("models".into(), json!(s.models));
    Value::Object(m)
}

async fn tokens(State(api): State<Api>, RawQuery(raw): RawQuery) -> Result<Response, ApiError> {
    let mut p = Params::parse(raw)?;
    let (from_ns, to_ns) = p.range()?;
    let query = AggQuery {
        from_ns,
        to_ns,
        operation: p.take("operation"),
    };
    p.finish()?;
    let rows = api.run(move |e| e.aggregate_tokens(&query)).await?;
    let models: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mut m = Map::new();
            if let Some(model) = &r.model {
                m.insert("model".into(), model.as_str().into());
            }
            m.insert("count".into(), r.count.into());
            m.insert("input_tokens".into(), r.input_tokens.into());
            m.insert("output_tokens".into(), r.output_tokens.into());
            Value::Object(m)
        })
        .collect();
    Ok(json_response(StatusCode::OK, &json!({ "models": models })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_errors_map_to_statuses() {
        let cases = [
            (Error::invalid_argument("x"), 400),
            (Error::not_found("x"), 404),
            (Error::unavailable("x"), 503),
            (Error::fatal("x"), 503),
            (Error::corrupt("x"), 500),
            (Error::conflict("x"), 500),
            (Error::other("x"), 500),
            (Error::from(std::io::Error::other("x")), 500),
        ];
        for (e, code) in cases {
            assert_eq!(status_of(&e).as_u16(), code, "{e}");
        }
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(
            percent_decode("a%20b+c%C3%A9").as_deref(),
            Some("a b c\u{e9}")
        );
        assert_eq!(percent_decode("%"), None);
        assert_eq!(percent_decode("%zz"), None);
        assert_eq!(percent_decode("%ff"), None);
    }
}
