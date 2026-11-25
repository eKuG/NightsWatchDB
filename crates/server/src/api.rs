//! The HTTP query API. Every response is JSON except `/healthz` and the
//! protobuf form of a trace. Engine calls block and may read files, so each
//! runs on the blocking pool.

use axum::Router;
use axum::extract::{RawQuery, Request, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE, HOST};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use nwdb_core::{Error, SpanColumns, SpanRecord, TraceId, TraceSummary};
use nwdb_engine::{AggQuery, Engine, ListQuery};
use nwdb_otlp::{
    attrs_to_json, encode_spans, events_to_json, links_to_json, resource_to_json, scope_to_json,
};
use serde_json::{Map, Value, json};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, Semaphore};

const HOUR_NS: i64 = 3_600_000_000_000;
const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 1000;
const TOKENS_DEFAULT_LIMIT: usize = 1000;
const TOKENS_MAX_LIMIT: usize = 10_000;
/// API requests that run at once; the others wait.
const MAX_CONCURRENT: usize = 64;
const PROTOBUF: &str = "application/x-protobuf";
const TRACE_PREFIX: &str = "/api/v1/traces/";

#[derive(Clone)]
struct Api {
    engine: Engine,
    fatal: Arc<Notify>,
}

/// Which `Host` headers the API answers to. A web page can make a browser send
/// requests to the loopback address under its own host name (DNS rebinding)
/// and then read the answers; those requests carry the page's host name.
pub(crate) struct HostCheck {
    enabled: bool,
    bound: IpAddr,
    allowed: Vec<String>,
}

impl HostCheck {
    /// The check is on when the API listens on a loopback address, or when
    /// the operator named hosts. An API exposed on another address with no
    /// names configured was exposed on purpose, and we cannot guess its names.
    pub(crate) fn new(bound: IpAddr, allowed: &[String]) -> Self {
        let allowed: Vec<String> = allowed
            .iter()
            .map(|h| normalize(host_name(h.trim())))
            .filter(|h| !h.is_empty())
            .collect();
        Self {
            enabled: bound.is_loopback() || !allowed.is_empty(),
            bound,
            allowed,
        }
    }

    fn allows(&self, host: Option<&HeaderValue>) -> bool {
        if !self.enabled {
            return true;
        }
        let Some(host) = host.and_then(|h| h.to_str().ok()) else {
            return false;
        };
        let name = normalize(host_name(host));
        name == "localhost"
            || name
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback() || ip == self.bound)
            || self.allowed.contains(&name)
    }
}

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

/// The host of a `Host` header value without its port.
fn host_name(h: &str) -> &str {
    if let Some(inner) = h.strip_prefix('[') {
        return inner.split(']').next().unwrap_or(inner);
    }
    if h.parse::<IpAddr>().is_ok() {
        return h;
    }
    match h.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => h,
    }
}

struct Guard {
    hosts: HostCheck,
    slots: Semaphore,
}

async fn guard(State(g): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    if !g.hosts.allows(req.headers().get(HOST)) {
        return ApiError::new(
            StatusCode::MISDIRECTED_REQUEST,
            "the Host header is not one this server answers to; see --api-allowed-hosts",
        )
        .into_response();
    }
    // The semaphore is never closed.
    let _slot = g.slots.acquire().await;
    next.run(req).await
}

/// The routes of the API.
pub(crate) fn router(engine: Engine, fatal: Arc<Notify>, hosts: HostCheck) -> Router {
    let guard_state = Arc::new(Guard {
        hosts,
        slots: Semaphore::new(MAX_CONCURRENT),
    });
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
        .layer(from_fn_with_state(guard_state, guard))
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
            match (percent_decode(k, true), percent_decode(v, true)) {
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

/// Decodes percent-encoding: `%` must be followed by exactly two hex digits.
/// `plus_is_space` is for query strings. `None` when the encoding is wrong or
/// the result is not UTF-8.
fn percent_decode(s: &str, plus_is_space: bool) -> Option<String> {
    fn hex(b: u8) -> Option<u8> {
        char::from(b).to_digit(16).map(|d| d as u8)
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' if plus_is_space => out.push(b' '),
            b'%' => {
                let (hi, lo) = (hex(*b.get(i + 1)?)?, hex(*b.get(i + 2)?)?);
                out.push(hi << 4 | lo);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// Whether the client asked for protobuf: a media range `application/x-protobuf`
/// with a quality above zero.
fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|range| {
            let mut parts = range.split(';');
            if !parts
                .next()
                .is_some_and(|t| t.trim().eq_ignore_ascii_case(PROTOBUF))
            {
                return false;
            }
            let q = parts.find_map(|p| {
                let (k, v) = p.split_once('=')?;
                k.trim()
                    .eq_ignore_ascii_case("q")
                    .then(|| v.trim().parse::<f32>().ok())
            });
            // No q means 1; a q that is not a number does not select it.
            q.is_none_or(|q| q.is_some_and(|q| q > 0.0))
        })
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
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // The path is decoded here, not by the router, so that a bad encoding is
    // an error of ours (JSON) and not the router's.
    let id = uri
        .path()
        .strip_prefix(TRACE_PREFIX)
        .and_then(|raw| percent_decode(raw, false))
        .ok_or_else(|| ApiError::bad("the trace id is not valid percent-encoding"))?;
    let trace_id: TraceId = id.parse().map_err(|e| api.error(&e))?;
    let protobuf = wants_protobuf(&headers);
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
    // Times are unsigned on the wire and stored as the same 64 bits.
    put("start_ns", (r.start_ns as u64).into());
    put("end_ns", (r.end_ns as u64).into());
    put("status_code", r.status_code.into());
    let strings = [
        ("status_message", r.status_message),
        ("trace_state", r.trace_state),
        ("service_name", r.service_name),
        ("scope_name", r.scope_name),
        ("scope_version", r.scope_version),
        ("resource_schema_url", r.resource_schema_url),
        ("scope_schema_url", r.scope_schema_url),
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
    if let Some(scope) = r.scope {
        // Most scopes carry a name and a version only; those are already
        // fields of their own.
        let attributes = scope_to_json(scope)?;
        if attributes.as_object().is_some_and(|a| !a.is_empty()) {
            put("scope_attributes", attributes);
        }
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
    m.insert("start_ns".into(), (s.start_ns as u64).into());
    m.insert("end_ns".into(), (s.end_ns as u64).into());
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
    let limit = match p.take("limit") {
        None => TOKENS_DEFAULT_LIMIT,
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=TOKENS_MAX_LIMIT).contains(n))
            .ok_or_else(|| ApiError::bad(format!("`limit` must be 1 to {TOKENS_MAX_LIMIT}")))?,
    };
    p.finish()?;
    let mut rows = api.run(move |e| e.aggregate_tokens(&query)).await?;
    rows.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.model.cmp(&b.model)));
    let truncated = rows.len() > limit;
    rows.truncate(limit);
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
    let mut body = json!({ "models": models });
    if truncated {
        body["truncated"] = true.into();
    }
    Ok(json_response(StatusCode::OK, &body))
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
            percent_decode("a%20b+c%C3%A9", true).as_deref(),
            Some("a b c\u{e9}")
        );
        assert_eq!(percent_decode("a+b", false).as_deref(), Some("a+b"));
        assert_eq!(percent_decode("%", true), None);
        assert_eq!(percent_decode("%1", true), None);
        assert_eq!(percent_decode("%zz", true), None);
        assert_eq!(percent_decode("%+1", true), None);
        assert_eq!(percent_decode("%-1", true), None);
        assert_eq!(percent_decode("%ff", true), None);
    }

    #[test]
    fn accept_header() {
        let accept = |v: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(ACCEPT, HeaderValue::from_static(v));
            wants_protobuf(&h)
        };
        assert!(accept("application/x-protobuf"));
        assert!(accept("application/json, application/x-protobuf;q=0.5"));
        assert!(accept("Application/X-Protobuf ; Q=1"));
        assert!(!accept("application/x-protobuf;q=0, application/json"));
        assert!(!accept("application/x-protobuf;q=0.0"));
        assert!(!accept("application/json"));
        assert!(!accept("*/*"));
        assert!(!wants_protobuf(&HeaderMap::new()));
    }

    #[test]
    fn host_check() {
        let allows = |bound: &str, allowed: &[&str], host: &str| {
            let allowed: Vec<String> = allowed.iter().map(|s| s.to_string()).collect();
            HostCheck::new(bound.parse().unwrap(), &allowed)
                .allows(Some(&HeaderValue::from_str(host).unwrap()))
        };
        for host in [
            "localhost",
            "LOCALHOST:4320",
            "localhost.",
            "127.0.0.1:1",
            "127.9.9.9",
            "[::1]:4320",
        ] {
            assert!(allows("127.0.0.1", &[], host), "{host}");
        }
        for host in [
            "evil.example",
            "evil.example:4320",
            "10.0.0.5",
            "localhost.evil.example",
            "",
        ] {
            assert!(!allows("127.0.0.1", &[], host), "{host}");
        }
        assert!(allows(
            "127.0.0.1",
            &["Traces.Internal"],
            "traces.internal:80"
        ));
        // Bound to a LAN address: that literal address is accepted.
        assert!(allows("10.0.0.5", &["x"], "10.0.0.5:4320"));
        assert!(!allows("10.0.0.5", &["x"], "10.0.0.6"));
        // Off when exposed on purpose with nothing configured.
        assert!(allows("0.0.0.0", &[], "anything.example"));
        assert!(!allows("0.0.0.0", &["a.example"], "anything.example"));
        let check = HostCheck::new("127.0.0.1".parse().unwrap(), &[]);
        assert!(!check.allows(None));
    }
}
