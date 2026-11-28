//! The HTTP query API. Every response is JSON except `/healthz` and the
//! protobuf form of a trace. Engine calls block and may read files, so each
//! runs on the blocking pool.

use axum::Router;
use axum::extract::{Extension, RawQuery, Request, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE, HOST};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame};
use nwdb_core::{Error, SpanColumns, SpanRecord, TraceId, TraceSummary};
use nwdb_engine::{
    AggQuery, Annotation, AnnotationFilter, Engine, ListQuery, SearchQuery, Snapshot,
};
use nwdb_otlp::{
    encode_spans, resource_dropped_attributes, scope_dropped_attributes, write_attrs, write_events,
    write_links, write_resource, write_scope_attrs,
};
use serde_json::{Map, Value, json};
use std::io::{self, Write};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

const HOUR_NS: i64 = 3_600_000_000_000;
const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 1000;
const TOKENS_DEFAULT_LIMIT: usize = 1000;
const TOKENS_MAX_LIMIT: usize = 10_000;
/// API requests that run at once; the others wait.
const MAX_CONCURRENT: usize = 64;
/// A streamed response is cut into chunks of about this size.
const CHUNK: usize = 64 * 1024;
/// Chunks that may wait for a slow client before the writer blocks.
const CHUNKS_IN_FLIGHT: usize = 4;
const PROTOBUF: &str = "application/x-protobuf";
const TRACE_PREFIX: &str = "/api/v1/traces/";
/// Annotations a trace view shows; one more means `annotations_truncated`.
const TRACE_ANNOTATIONS: usize = 1000;
/// Annotations each trace of a list shows.
const LIST_ANNOTATIONS: usize = 32;

mod annotate;
mod body;
pub(crate) use body::BodyLimits;

use annotate::annotations_json;

#[derive(Clone)]
struct Api {
    engine: Engine,
    fatal: Arc<Notify>,
    body: BodyLimits,
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
    slots: Arc<Semaphore>,
}

/// A request's concurrency slot. Whoever works for the request holds a copy:
/// the response body, a query on the blocking pool, the task that renders a
/// trace. The slot is free only when all of them are done, so work that
/// outlives a client that went away still counts as running.
type Slot = Arc<OwnedSemaphorePermit>;

/// A response body that keeps its request's slot until the body is finished
/// or dropped.
struct Held {
    inner: axum::body::Body,
    _slot: Slot,
}

impl HttpBody for Held {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

async fn guard(State(g): State<Arc<Guard>>, mut req: Request, next: Next) -> Response {
    if !g.hosts.allows(req.headers().get(HOST)) {
        return ApiError::new(
            StatusCode::MISDIRECTED_REQUEST,
            "the Host header is not one this server answers to; see --api-allowed-hosts",
        )
        .into_response();
    }
    // These two answer from memory. They are what an operator looks at when
    // every slot is taken, so they never wait for one.
    if matches!(req.uri().path(), "/healthz" | "/api/v1/stats") {
        return next.run(req).await;
    }
    // The semaphore is never closed.
    let Ok(slot) = g.slots.clone().acquire_owned().await else {
        return ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response();
    };
    let slot = Arc::new(slot);
    req.extensions_mut().insert(slot.clone());
    let (parts, body) = next.run(req).await.into_parts();
    Response::from_parts(
        parts,
        axum::body::Body::new(Held {
            inner: body,
            _slot: slot,
        }),
    )
}

/// The routes of the API.
pub(crate) fn router(
    engine: Engine,
    fatal: Arc<Notify>,
    hosts: HostCheck,
    body: BodyLimits,
) -> Router {
    let guard_state = Arc::new(Guard {
        hosts,
        slots: Arc::new(Semaphore::new(MAX_CONCURRENT)),
    });
    let routes = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/traces", get(list_traces))
        .route("/api/v1/traces/{trace_id}", get(get_trace))
        .route("/api/v1/tokens", get(tokens))
        .route("/api/v1/search", get(search));
    write_routes(routes)
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "no such path") })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "the method is not supported on this path",
            )
        })
        .with_state(Api {
            engine,
            fatal,
            body,
        })
        .layer(from_fn_with_state(guard_state, guard))
}

/// The routes that take a POST. A handler of one takes [`body::JsonBody`] as its
/// last argument, which applies every rule for write requests. A path listed
/// here answers 405 to GET; every path not listed answers 405 to a POST.
fn write_routes(routes: Router<Api>) -> Router<Api> {
    #[cfg(test)]
    let routes = routes.route(
        "/api/v1/test/body",
        axum::routing::post(|body::JsonBody(b): body::JsonBody| async move {
            let sum = b
                .iter()
                .fold(0u64, |s, &x| s.wrapping_mul(31).wrapping_add(x.into()));
            json_response(StatusCode::OK, &json!({ "len": b.len(), "sum": sum }))
        }),
    );
    routes.route(
        "/api/v1/annotations",
        axum::routing::post(annotate::annotate),
    )
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

    /// Runs `f` on the blocking pool. The slot goes with it, so that a client
    /// that disconnects does not free the slot while its query still runs.
    async fn run<T: Send + 'static>(
        &self,
        slot: Slot,
        f: impl FnOnce(Engine) -> nwdb_core::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let engine = self.engine.clone();
        let query = move || {
            let _slot = slot;
            f(engine)
        };
        match tokio::task::spawn_blocking(query).await {
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

    /// A finite number.
    fn float(&mut self, name: &str) -> Result<Option<f64>, ApiError> {
        self.take(name)
            .map(|v| {
                v.parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .ok_or_else(|| ApiError::bad(format!("`{name}` must be a finite number")))
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

/// Healthy means the engine is not in its fatal state, where it accepts and
/// flushes nothing and only a restart recovers it.
async fn healthz(State(api): State<Api>) -> Response {
    if api.engine.stats().fatal {
        return ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the engine is in a fatal state",
        )
        .into_response();
    }
    "ok".into_response()
}

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
            "fatal": s.fatal,
            "last_error": s.last_error,
            "payload_logical_bytes": s.payload_logical_bytes,
            "payload_unique_bytes": s.payload_unique_bytes,
            "payload_stored_bytes": s.payload_stored_bytes,
            "maintenance_passes": s.maintenance_passes,
            "retention_parts_removed": s.retention_parts_removed,
            "last_maintenance_error": s.last_maintenance_error,
            "merges_done": s.merges_done,
            "parts_merged_away": s.parts_merged_away,
            "duplicate_spans_dropped": s.duplicate_spans_dropped,
            "merge_input_bytes": s.merge_input_bytes,
            "merge_output_bytes": s.merge_output_bytes,
            "annotations_superseded": s.annotations_superseded,
            "windows_with_many_parts": s.windows_with_many_parts,
            "merging": s.merging,
            "last_compaction_error": s.last_compaction_error,
            "annotations_buffered": s.annotations_buffered,
            "annotation_parts": s.annotation_parts,
            "annotation_bytes": s.annotation_bytes,
            "annotations_written": s.annotations_written,
            "annotations_dropped": s.annotations_dropped,
        }),
    )
}

async fn get_trace(
    State(api): State<Api>,
    Extension(slot): Extension<Slot>,
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
    if protobuf {
        let body = api
            .run(slot, move |engine| {
                encode_spans(&spans_of(&engine.snapshot(), trace_id)?)
            })
            .await?;
        return Ok((StatusCode::OK, [(CONTENT_TYPE, PROTOBUF)], body).into_response());
    }
    // Spans and annotations come from one snapshot.
    let (batches, annotations, more) = api
        .run(slot.clone(), move |engine| {
            let snapshot = engine.snapshot();
            let batches = spans_of(&snapshot, trace_id)?;
            let (annotations, more) = snapshot.annotations(trace_id, TRACE_ANNOTATIONS)?;
            Ok((batches, annotations, more))
        })
        .await?;
    // The JSON is written by a blocking task and sent in chunks through a
    // small channel, so a trace is never held as JSON. A client that is slow
    // holds the task back; one that is gone, or that makes no progress for
    // the write timeout (the connection is then closed), ends it. The task
    // holds the slot as long as it holds the rows.
    let (tx, rx) = mpsc::channel(CHUNKS_IN_FLIGHT);
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let mut out = ChunkWriter {
            tx,
            buf: Vec::with_capacity(CHUNK),
        };
        let result = render_trace(trace_id, &batches, &annotations, more, &mut out)
            .and_then(|()| out.flush());
        if let Err(e) = result {
            // A failure other than the client leaving ends the body with an
            // error, so that the client sees a cut-off response and not a
            // short one that looks complete.
            if e.kind() != io::ErrorKind::BrokenPipe {
                let _ = out.tx.blocking_send(Err(e));
            }
        }
    });
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        axum::body::Body::new(Chunks(rx)),
    )
        .into_response())
}

/// The spans of a trace in `snapshot`; not found when it has none.
fn spans_of(snapshot: &Snapshot, id: TraceId) -> nwdb_core::Result<Vec<arrow_array::RecordBatch>> {
    snapshot
        .get_trace(id)?
        .filter(|b| b.iter().any(|b| b.num_rows() > 0))
        .ok_or_else(|| Error::not_found(format!("trace {id}")))
}

/// The body of a streamed response.
struct Chunks(mpsc::Receiver<io::Result<Bytes>>);

impl HttpBody for Chunks {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        self.0
            .poll_recv(cx)
            .map(|chunk| chunk.map(|c| c.map(Frame::data)))
    }
}

/// Collects output into chunks of about [`CHUNK`] bytes and sends them. It
/// fails with `BrokenPipe` once the receiving side is gone, which stops the
/// writer that feeds it.
struct ChunkWriter {
    tx: mpsc::Sender<io::Result<Bytes>>,
    buf: Vec<u8>,
}

impl ChunkWriter {
    fn send(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK)));
        self.tx
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        // A big piece (a long string) is cut up, so that the chunk is never
        // more than CHUNK and a piece is not copied whole.
        for piece in b.chunks(CHUNK) {
            let room = CHUNK - self.buf.len();
            let (now, later) = piece.split_at(room.min(piece.len()));
            self.buf.extend_from_slice(now);
            if self.buf.len() >= CHUNK {
                self.send()?;
            }
            self.buf.extend_from_slice(later);
        }
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send()
    }
}

/// Writes the trace one span at a time, then its annotations.
fn render_trace(
    id: TraceId,
    batches: &[arrow_array::RecordBatch],
    annotations: &[Annotation],
    more: bool,
    out: &mut dyn Write,
) -> io::Result<()> {
    write!(out, "{{\"trace_id\":\"{id}\",\"spans\":[")?;
    let mut first = true;
    for batch in batches {
        let cols = SpanColumns::try_new(batch).map_err(io::Error::other)?;
        for row in 0..cols.len() {
            if !first {
                out.write_all(b",")?;
            }
            first = false;
            write_span(out, &cols.record(row))?;
        }
    }
    out.write_all(b"]")?;
    if !annotations.is_empty() {
        out.write_all(b",\"annotations\":")?;
        serde_json::to_writer(&mut *out, &annotations_json(annotations))
            .map_err(io::Error::from)?;
        if more {
            out.write_all(b",\"annotations_truncated\":true")?;
        }
    }
    out.write_all(b"}")
}

/// A writer that stops as soon as it is given more than an empty object.
struct EmptyObject(usize);

impl Write for EmptyObject {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 += b.len();
        if self.0 > 2 {
            return Err(io::Error::other("not empty"));
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_str(out: &mut dyn Write, v: &str) -> io::Result<()> {
    serde_json::to_writer(&mut *out, v).map_err(io::Error::from)
}

fn write_span(out: &mut dyn Write, r: &SpanRecord<'_>) -> io::Result<()> {
    write!(out, "{{\"span_id\":\"{}\"", r.span_id)?;
    if let Some(p) = r.parent_span_id {
        write!(out, ",\"parent_span_id\":\"{p}\"")?;
    }
    out.write_all(b",\"name\":")?;
    json_str(out, r.name)?;
    // Times are unsigned on the wire and stored as the same 64 bits.
    write!(
        out,
        ",\"kind\":{},\"start_ns\":{},\"end_ns\":{},\"status_code\":{}",
        r.kind, r.start_ns as u64, r.end_ns as u64, r.status_code
    )?;
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
            write!(out, ",\"{k}\":")?;
            json_str(out, v)?;
        }
    }
    for (k, v) in [
        ("input_tokens", r.input_tokens),
        ("output_tokens", r.output_tokens),
    ] {
        if let Some(v) = v {
            write!(out, ",\"{k}\":{v}")?;
        }
    }
    for (k, v) in [
        ("flags", r.flags),
        ("dropped_attributes_count", r.dropped_attributes_count),
        ("dropped_events_count", r.dropped_events_count),
        ("dropped_links_count", r.dropped_links_count),
    ] {
        if v != 0 {
            write!(out, ",\"{k}\":{v}")?;
        }
    }
    if let Some(res) = r.resource {
        out.write_all(b",\"resource\":")?;
        write_resource(out, res)?;
        let dropped = resource_dropped_attributes(res);
        if dropped != 0 {
            write!(out, ",\"resource_dropped_attributes_count\":{dropped}")?;
        }
    }
    if let Some(scope) = r.scope {
        // Most scopes carry a name and a version only; those are already
        // fields of their own.
        if write_scope_attrs(&mut EmptyObject(0), scope).is_err() {
            out.write_all(b",\"scope_attributes\":")?;
            write_scope_attrs(out, scope)?;
        }
        let dropped = scope_dropped_attributes(scope);
        if dropped != 0 {
            write!(out, ",\"scope_dropped_attributes_count\":{dropped}")?;
        }
    }
    out.write_all(b",\"attributes\":")?;
    write_attrs(out, r.attrs)?;
    if let Some(e) = r.events {
        out.write_all(b",\"events\":")?;
        write_events(out, e)?;
    }
    if let Some(l) = r.links {
        out.write_all(b",\"links\":")?;
        write_links(out, l)?;
    }
    out.write_all(b"}")
}

async fn list_traces(
    State(api): State<Api>,
    Extension(slot): Extension<Slot>,
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
    let name = p.take("annotation");
    let source = p.take("annotation_source");
    let min = p.float("annotation_min")?;
    let max = p.float("annotation_max")?;
    let label = p.take("annotation_label");
    let annotation = match name {
        Some(name) => Some(AnnotationFilter {
            name,
            source,
            min,
            max,
            label,
        }),
        None if source.is_some() || min.is_some() || max.is_some() || label.is_some() => {
            return Err(ApiError::bad(
                "`annotation_source`, `annotation_min`, `annotation_max` and `annotation_label` need `annotation`",
            ));
        }
        None => None,
    };
    let query = ListQuery {
        from_ns,
        to_ns,
        service: p.take("service"),
        model: p.take("model"),
        error,
        annotation,
        limit,
    };
    p.finish()?;
    // The annotations come from the snapshot the listing was read from.
    let traces = api
        .run(slot, move |e| {
            let snapshot = e.snapshot();
            snapshot
                .list_traces(&query)?
                .into_iter()
                .map(|t| {
                    let (a, more) = snapshot.annotations(t.trace_id, LIST_ANNOTATIONS)?;
                    Ok((t, a, more))
                })
                .collect::<nwdb_core::Result<Vec<_>>>()
        })
        .await?;
    let traces: Vec<Value> = traces
        .iter()
        .map(|(t, a, more)| summary_json(t, a, *more))
        .collect();
    Ok(json_response(StatusCode::OK, &json!({ "traces": traces })))
}

/// `GET /api/v1/search?q=...&from=&to=&limit=`: the newest spans whose text
/// matches `q`. Only flushed parts are searched; the answer says through which
/// log position.
async fn search(
    State(api): State<Api>,
    Extension(slot): Extension<Slot>,
    RawQuery(raw): RawQuery,
) -> Result<Response, ApiError> {
    let mut p = Params::parse(raw)?;
    let (from_ns, to_ns) = p.range()?;
    let limit = match p.take("limit") {
        None => DEFAULT_LIMIT,
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| ApiError::bad(format!("`limit` must be 1 to {MAX_LIMIT}")))?,
    };
    let text = p
        .take("q")
        .ok_or_else(|| ApiError::bad("`q` is required"))?;
    p.finish()?;
    let query = nwdb_engine::textindex::parse(&text).map_err(|e| api.error(&e))?;
    let query = SearchQuery {
        query,
        from_ns,
        to_ns,
        limit,
    };
    let found = api.run(slot, move |e| e.search(&query)).await?;
    let spans: Vec<Value> = found
        .hits
        .iter()
        .map(|h| {
            let mut m = Map::new();
            m.insert("trace_id".into(), h.trace_id.to_string().into());
            m.insert("span_id".into(), h.span_id.to_string().into());
            m.insert("start_ns".into(), (h.start_ns as u64).into());
            m.insert("end_ns".into(), (h.end_ns as u64).into());
            m.insert("name".into(), h.name.as_str().into());
            for (k, v) in [
                ("service_name", &h.service_name),
                ("operation", &h.operation),
                ("request_model", &h.request_model),
            ] {
                if let Some(v) = v {
                    m.insert(k.into(), v.as_str().into());
                }
            }
            Value::Object(m)
        })
        .collect();
    Ok(json_response(
        StatusCode::OK,
        &json!({ "spans": spans, "searched_through_lsn": found.searched_through_lsn }),
    ))
}

fn summary_json(s: &TraceSummary, annotations: &[Annotation], more: bool) -> Value {
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
    if !annotations.is_empty() {
        m.insert("annotations".into(), annotations_json(annotations));
        if more {
            m.insert("annotations_truncated".into(), true.into());
        }
    }
    Value::Object(m)
}

async fn tokens(
    State(api): State<Api>,
    Extension(slot): Extension<Slot>,
    RawQuery(raw): RawQuery,
) -> Result<Response, ApiError> {
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
    let mut rows = api.run(slot, move |e| e.aggregate_tokens(&query)).await?;
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

    /// A query that was started keeps its slot until it has finished, even if
    /// the request that asked for it is gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_query_keeps_its_slot_after_its_request_is_dropped() {
        use nwdb_engine::EngineOptions;
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineOptions::new(dir.path()), Vec::new()).unwrap();
        let api = Api {
            engine: engine.clone(),
            fatal: Arc::new(Notify::new()),
            body: BodyLimits {
                max: 0,
                timeout: Duration::ZERO,
            },
        };
        let slots = Arc::new(Semaphore::new(1));
        let slot: Slot = Arc::new(slots.clone().acquire_owned().await.unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let request = tokio::spawn(async move {
            api.run(slot, move |_| {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
                Ok(())
            })
            .await
        });
        started_rx.await.unwrap();

        // The client goes away: its request's future is dropped.
        request.abort();
        assert!(request.await.is_err_and(|e| e.is_cancelled()));
        assert_eq!(slots.available_permits(), 0, "the slot was given up");

        // The query ends, and then the slot is free.
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while slots.available_permits() == 0 {
            assert!(Instant::now() < deadline, "the slot was never freed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::task::spawn_blocking(move || engine.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}
