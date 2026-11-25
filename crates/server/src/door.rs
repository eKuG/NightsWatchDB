//! The OTLP gRPC door: `TraceService/Export`, handled by hand so that every
//! byte is counted against a budget before the door holds it. A gRPC library
//! would buffer and decompress a whole message before any of our code ran.

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Request, Response};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use nwdb_core::Error;
use nwdb_engine::Engine;
use nwdb_otlp::{DecodeStats, RECORD_TAG_OTLP_TRACES, TraceDecoder};
use std::convert::Infallible;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::Instant;

const EXPORT_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const ACCEPT_ENCODING: &str = "identity,gzip,zstd";

/// What decoding a request takes from the decoding pool: `DECODE_FACTOR`
/// times the request's decompressed size plus `DECODE_FIXED` bytes.
///
/// Derived from the heap the decoder holds at its peak, counted with an
/// allocator that sees every byte (the test `decoding_never_needs_more_than_the_door_charges`).
/// That is more than the finished batches: the interning tables and the
/// builders' buffers, which double as they grow, are alive at the same time.
/// The worst shape measured was 13.6 times the request (a scope of its own
/// for each span, a little past 1,000 spans; the other shapes tried were
/// below that). A compressed message also has its decompressed request in the
/// pool, which is one more times the request: 14.6 in all. 16 leaves a margin
/// over that. The fixed part covers the empty builders, which cost the same
/// however small the request is.
pub const DECODE_FACTOR: usize = 16;
/// The fixed part of the decoding charge, see [`DECODE_FACTOR`].
pub const DECODE_FIXED: usize = 64 * 1024;
/// A zstd frame that does not state its size is assumed to expand to this
/// many times its length, but not below [`UNSIZED_FLOOR`].
const UNSIZED_RATIO: usize = 64;
const UNSIZED_FLOOR: usize = 1 << 20;
/// Zstd frames that ask for a window above 2^23 bytes (8 MiB) are refused:
/// the decoder would allocate the window before we see any output.
const ZSTD_WINDOW_LOG_MAX: u32 = 23;

/// Largest request accepted: 64 MiB, or less if the log cannot hold such a
/// record (one byte is the format tag).
pub const MAX_MESSAGE: usize = if 64 << 20 < nwdb_wal::MAX_RECORD_BYTES - 1 {
    64 << 20
} else {
    nwdb_wal::MAX_RECORD_BYTES - 1
};

// gRPC status codes.
const INVALID_ARGUMENT: u8 = 3;
const DEADLINE_EXCEEDED: u8 = 4;
const RESOURCE_EXHAUSTED: u8 = 8;
const UNIMPLEMENTED: u8 = 12;
const INTERNAL: u8 = 13;
const UNAVAILABLE: u8 = 14;

/// State shared by every request.
pub(crate) struct Shared {
    pub engine: Engine,
    pub decoder: TraceDecoder,
    /// Bytes read from the network and not yet answered. A message takes its
    /// length from here before its payload is read.
    pub wire: Arc<Semaphore>,
    pub wire_cap: usize,
    /// Memory for expanding and decoding a message. It is a separate pool so
    /// that a request never waits for more of the pool it already holds a part
    /// of (two such requests could wait for each other forever).
    pub work: Arc<Semaphore>,
    pub work_cap: usize,
    /// Told when the engine reports a fatal error, so the process can stop.
    pub fatal: Arc<Notify>,
    /// How long a sender may be silent in the middle of a call.
    pub stall: Duration,
    /// How long a sender has to deliver a whole message once its budget is
    /// taken.
    pub receive: Duration,
    /// How long a call waits for a call slot or for budget.
    pub budget_wait: Duration,
    /// Export calls in progress, from before their first byte is read.
    pub calls: Arc<Semaphore>,
}

/// A gRPC error: status code and message.
#[derive(Debug)]
struct Fail(u8, String);

impl Fail {
    fn new(code: u8, msg: impl Into<String>) -> Self {
        Self(code, msg.into())
    }
}

impl Shared {
    fn fail(&self, e: &Error) -> Fail {
        match e {
            Error::Unavailable(_) => Fail::new(UNAVAILABLE, e.to_string()),
            Error::Fatal(_) => {
                self.fatal.notify_one();
                // The sender should retry, against the restarted process.
                Fail::new(UNAVAILABLE, e.to_string())
            }
            _ => Fail::new(INTERNAL, e.to_string()),
        }
    }
}

/// Takes `n` permits from `pool`, waiting at most `wait`. A sender that
/// holds the pool must not make everybody else wait for ever: the senders
/// that give up are told to retry, with the limit that was hit.
async fn take(
    pool: &Arc<Semaphore>,
    n: u32,
    wait: Duration,
    limit: &str,
) -> Result<OwnedSemaphorePermit, Fail> {
    match tokio::time::timeout(wait, pool.clone().acquire_many_owned(n)).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(Fail::new(UNAVAILABLE, "shutting down")),
        Err(_) => Err(Fail::new(
            UNAVAILABLE,
            format!("the server is busy: no room within {wait:?} in {limit}; retry with back-off"),
        )),
    }
}

/// Takes `want` bytes (at least 1, at most the whole pool) from `pool`. A
/// request bigger than the pool gets the whole pool and so runs alone.
async fn charge(
    pool: &Arc<Semaphore>,
    cap: usize,
    want: usize,
    wait: Duration,
    limit: &str,
) -> Result<OwnedSemaphorePermit, Fail> {
    let n = u32::try_from(want.clamp(1, cap)).unwrap_or(u32::MAX);
    take(pool, n, wait, limit).await
}

const WIRE_LIMIT: &str = "the bytes-read half of max_inflight_bytes";
const WORK_LIMIT: &str = "the decoding half of max_inflight_bytes";
const CALLS_LIMIT: &str = "the call slots (max_calls)";

/// The decoding charge for a request of `size` decompressed bytes.
fn decode_charge(size: usize) -> usize {
    size.saturating_mul(DECODE_FACTOR)
        .saturating_add(DECODE_FIXED)
}

// ---- reading the request ----

/// Request body chunks. Each wait is limited by the stall timeout, and once
/// `deadline` is set, all of them together by that.
struct Chunks {
    body: Incoming,
    stall: Duration,
    deadline: Option<Instant>,
}

impl Chunks {
    /// The next non-empty chunk, or `None` at the end of the stream.
    async fn next(&mut self) -> Result<Option<Bytes>, Fail> {
        loop {
            let wait = match self.deadline {
                Some(d) => self.stall.min(d.saturating_duration_since(Instant::now())),
                None => self.stall,
            };
            match tokio::time::timeout(wait, self.body.frame()).await {
                Err(_) if self.deadline.is_some_and(|d| Instant::now() >= d) => {
                    return Err(Fail::new(
                        DEADLINE_EXCEEDED,
                        "the whole message did not arrive within receive_timeout",
                    ));
                }
                Err(_) => {
                    return Err(Fail::new(
                        DEADLINE_EXCEEDED,
                        "the sender stopped sending (stall_timeout)",
                    ));
                }
                Ok(None) => return Ok(None),
                Ok(Some(Err(e))) => return Err(Fail::new(INTERNAL, format!("read failed: {e}"))),
                Ok(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        return Ok(Some(data));
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Encoding {
    Identity,
    Gzip,
    Zstd,
}

fn encoding_of(req: &Request<Incoming>) -> Result<Encoding, Fail> {
    match req
        .headers()
        .get("grpc-encoding")
        .map(HeaderValue::as_bytes)
    {
        None | Some(b"identity") => Ok(Encoding::Identity),
        Some(b"gzip") => Ok(Encoding::Gzip),
        Some(b"zstd") => Ok(Encoding::Zstd),
        Some(_) => Err(Fail::new(UNIMPLEMENTED, "unsupported grpc-encoding")),
    }
}

/// Reads the one message of the call into a buffer that has room for the
/// format tag. Returns its compressed flag and bytes.
async fn read_message(
    shared: &Shared,
    body: Incoming,
) -> Result<(bool, Vec<u8>, OwnedSemaphorePermit), Fail> {
    let mut chunks = Chunks {
        body,
        stall: shared.stall,
        deadline: None,
    };
    let mut head = [0u8; 5];
    let mut have = 0;
    let mut rest = Bytes::new();
    while have < head.len() {
        let Some(mut chunk) = chunks.next().await? else {
            return Err(Fail::new(
                INVALID_ARGUMENT,
                "the call has no complete message",
            ));
        };
        let n = chunk.len().min(head.len() - have);
        head[have..have + n].copy_from_slice(&chunk[..n]);
        have += n;
        rest = chunk.split_off(n);
    }
    let compressed = match head[0] {
        0 => false,
        1 => true,
        _ => return Err(Fail::new(INVALID_ARGUMENT, "bad compressed flag")),
    };
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    if len > MAX_MESSAGE {
        // Retrying cannot help, and gRPC senders do not retry this status.
        return Err(Fail::new(
            RESOURCE_EXHAUSTED,
            format!("message of {len} bytes is over the limit of {MAX_MESSAGE}"),
        ));
    }
    // The budget is taken before the payload is read, and from then on the
    // sender has a limited time to deliver the message.
    let permit = charge(
        &shared.wire,
        shared.wire_cap,
        len,
        shared.budget_wait,
        WIRE_LIMIT,
    )
    .await?;
    chunks.deadline = Some(Instant::now() + shared.receive);
    let mut data = Vec::with_capacity(len + 1);
    let mut piece = rest;
    loop {
        if data.len() + piece.len() > len {
            return Err(one_message());
        }
        data.extend_from_slice(&piece);
        if data.len() == len {
            break;
        }
        piece = chunks
            .next()
            .await?
            .ok_or_else(|| Fail::new(INVALID_ARGUMENT, "the message is shorter than its length"))?;
    }
    // Exactly one message: the stream must end here.
    if chunks.next().await?.is_some() {
        return Err(one_message());
    }
    Ok((compressed, data, permit))
}

fn one_message() -> Fail {
    Fail::new(INVALID_ARGUMENT, "one message per export")
}

// ---- decompression ----

/// The size `data` says it expands to, and whether that is a statement of the
/// sender (`true`) or only an assumption (`false`). Never above
/// [`MAX_MESSAGE`].
fn claimed_size(enc: Encoding, data: &[u8]) -> Result<(usize, bool), Fail> {
    let (size, exact) = match enc {
        Encoding::Gzip => {
            let n = data.len();
            if n < 18 {
                return Err(Fail::new(INVALID_ARGUMENT, "truncated gzip data"));
            }
            let trailer = [data[n - 4], data[n - 3], data[n - 2], data[n - 1]];
            (u64::from(u32::from_le_bytes(trailer)), true)
        }
        Encoding::Zstd => match zstd::zstd_safe::get_frame_content_size(data) {
            Ok(Some(n)) => (n, true),
            // Streaming encoders write no size. Charging such a frame the
            // whole pool would make them decode one at a time, so a size is
            // assumed; a frame that turns out larger is run again alone.
            Ok(None) => (assumed_size(data.len()) as u64, false),
            Err(_) => return Err(Fail::new(INVALID_ARGUMENT, "bad zstd frame header")),
        },
        Encoding::Identity => unreachable!("only compressed messages have a claimed size"),
    };
    match usize::try_from(size) {
        Ok(n) if n <= MAX_MESSAGE => Ok((n, exact)),
        _ => Err(Fail::new(
            RESOURCE_EXHAUSTED,
            format!("the message expands to {size} bytes, over the limit of {MAX_MESSAGE}"),
        )),
    }
}

/// The size a frame of `compressed` bytes that states none is assumed to
/// expand to.
fn assumed_size(compressed: usize) -> usize {
    MAX_MESSAGE.min(compressed.saturating_mul(UNSIZED_RATIO).max(UNSIZED_FLOOR))
}

/// Expands `data` to at most `claim` bytes. `None` when the output is longer
/// than `claim`. Output shorter than an exact claim, bytes after the
/// compressed stream and corrupt data are errors.
fn decompress(
    enc: Encoding,
    data: &[u8],
    claim: usize,
    exact: bool,
) -> Result<Option<Vec<u8>>, Fail> {
    let bad = |what: &str| Fail::new(INVALID_ARGUMENT, format!("bad {what} data"));
    let mut input = data;
    // The output grows as it is produced: a claim is checked only at the end,
    // so a false one must not be able to reserve memory.
    let mut out = Vec::new();
    // One byte past the claim is read, to tell "exactly the claim" from "more".
    let limit = claim as u64 + 1;
    match enc {
        Encoding::Gzip => {
            let mut d = flate2::bufread::GzDecoder::new(&mut input);
            (&mut d)
                .take(limit)
                .read_to_end(&mut out)
                .map_err(|_| bad("gzip"))?;
        }
        Encoding::Zstd => {
            let mut d = zstd::stream::read::Decoder::with_buffer(&mut input)
                .map_err(|_| bad("zstd"))?
                .single_frame();
            d.window_log_max(ZSTD_WINDOW_LOG_MAX)
                .map_err(|_| bad("zstd"))?;
            (&mut d)
                .take(limit)
                .read_to_end(&mut out)
                .map_err(|_| bad("zstd"))?;
        }
        Encoding::Identity => unreachable!(),
    }
    if out.len() > claim {
        return Ok(None);
    }
    if exact && out.len() != claim {
        return Err(Fail::new(
            INVALID_ARGUMENT,
            "the message does not expand to the size it claims",
        ));
    }
    if !input.is_empty() {
        return Err(Fail::new(
            INVALID_ARGUMENT,
            "bytes after the compressed data",
        ));
    }
    Ok(Some(out))
}

/// What one run of the blocking task found.
enum Step {
    /// The request and what the decoder made of it.
    Decoded(
        Vec<u8>,
        nwdb_core::Result<(Vec<arrow_array::RecordBatch>, DecodeStats)>,
    ),
    /// The message expanded to more than its cap. Nothing is kept.
    Over,
    Failed(Fail),
}

/// Expands (if compressed) and decodes a message. Runs on the blocking pool.
/// The compressed bytes come back, for another try with a larger cap.
fn expand_and_decode(
    decoder: TraceDecoder,
    enc: Encoding,
    compressed: bool,
    input: Vec<u8>,
    claim: usize,
    exact: bool,
) -> (Step, Vec<u8>) {
    if !compressed {
        let decoded = decoder.decode_with_stats(&input);
        return (Step::Decoded(input, decoded), Vec::new());
    }
    match decompress(enc, &input, claim, exact) {
        Ok(Some(out)) => {
            let decoded = decoder.decode_with_stats(&out);
            (Step::Decoded(out, decoded), input)
        }
        Ok(None) => (Step::Over, input),
        Err(e) => (Step::Failed(e), input),
    }
}

// ---- the reply ----

/// The `ExportTraceServiceResponse` message: empty when nothing was rejected,
/// otherwise `partial_success { rejected_spans, error_message }`.
fn export_response(rejected_spans: u64, reason: &str) -> Vec<u8> {
    if rejected_spans == 0 {
        return Vec::new();
    }
    fn varint(out: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            out.push(v as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    // ExportTracePartialSuccess: field 1 int64 (varint), field 2 string.
    let mut inner = vec![0x08];
    varint(&mut inner, rejected_spans);
    if !reason.is_empty() {
        inner.push(0x12);
        varint(&mut inner, reason.len() as u64);
        inner.extend_from_slice(reason.as_bytes());
    }
    let mut out = vec![0x0a];
    varint(&mut out, inner.len() as u64);
    out.extend_from_slice(&inner);
    out
}

type Reply = BoxBody<Bytes, Infallible>;

fn reply_headers() -> http::response::Builder {
    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/grpc")
        .header("grpc-accept-encoding", ACCEPT_ENCODING)
}

/// The text of `grpc-message`: printable ASCII as is, everything else (and
/// `%`) as `%XX`.
fn percent_encode(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    for b in msg.bytes() {
        if (0x20..0x7f).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn reply(result: Result<Vec<u8>, Fail>) -> Response<Reply> {
    match result {
        Ok(message) => {
            let mut framed = Vec::with_capacity(5 + message.len());
            framed.push(0);
            framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
            framed.extend_from_slice(&message);
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            let body = Full::new(Bytes::from(framed))
                .with_trailers(std::future::ready(Some(Ok::<_, Infallible>(trailers))))
                .boxed();
            reply_headers().body(body)
        }
        Err(Fail(code, msg)) => reply_headers()
            .header("grpc-status", code.to_string())
            .header("grpc-message", percent_encode(&msg))
            .body(Empty::new().boxed()),
    }
    .expect("static headers are valid")
}

// ---- the call ----

/// Handles one HTTP/2 request.
pub(crate) async fn handle(
    shared: Arc<Shared>,
    req: Request<Incoming>,
) -> Result<Response<Reply>, Infallible> {
    Ok(reply(export(&shared, req).await))
}

async fn export(shared: &Shared, req: Request<Incoming>) -> Result<Vec<u8>, Fail> {
    let grpc = req
        .headers()
        .get(CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"));
    if req.method() != Method::POST || req.uri().path() != EXPORT_PATH || !grpc {
        return Err(Fail::new(
            UNIMPLEMENTED,
            "only TraceService/Export over gRPC is served",
        ));
    }
    let enc = encoding_of(&req)?;
    // Before the first byte of the body is read, so that the calls that wait
    // for budget, each holding the first frame of its message, are limited.
    let _slot = take(&shared.calls, 1, shared.budget_wait, CALLS_LIMIT).await?;
    let (compressed, data, mut wire) = read_message(shared, req.into_body()).await?;
    if compressed && enc == Encoding::Identity {
        return Err(Fail::new(
            INVALID_ARGUMENT,
            "compressed message without grpc-encoding",
        ));
    }

    // Expanding and decoding take memory beyond the bytes that were read.
    let (mut claim, mut exact) = if compressed {
        claimed_size(enc, &data)?
    } else {
        (data.len(), true)
    };
    let mut work = charge(
        &shared.work,
        shared.work_cap,
        decode_charge(claim),
        shared.budget_wait,
        WORK_LIMIT,
    )
    .await?;

    // The permits go into the blocking task and come back with its result:
    // if the call's future is dropped (the stream was reset), they are
    // released when the task has finished and its memory is freed, not
    // before.
    let mut input = data;
    let (data, decoded) = loop {
        let decoder = shared.decoder;
        let ((step, back), w, k) = tokio::task::spawn_blocking(move || {
            let done = expand_and_decode(decoder, enc, compressed, input, claim, exact);
            (done, wire, work)
        })
        .await
        .map_err(|_| Fail::new(INTERNAL, "decoding failed"))?;
        (wire, work, input) = (w, k, back);
        match step {
            Step::Decoded(data, decoded) => break (data, decoded),
            Step::Failed(e) => return Err(e),
            Step::Over if exact => {
                return Err(Fail::new(
                    INVALID_ARGUMENT,
                    "the message does not expand to the size it claims",
                ));
            }
            Step::Over if claim >= MAX_MESSAGE => {
                return Err(Fail::new(
                    RESOURCE_EXHAUSTED,
                    format!("the message expands to more than the limit of {MAX_MESSAGE} bytes"),
                ));
            }
            Step::Over => {
                // Larger than assumed: give the charge back, take the whole
                // pool (so this runs alone) and expand again up to the limit.
                drop(work);
                claim = MAX_MESSAGE;
                exact = false;
                work = charge(
                    &shared.work,
                    shared.work_cap,
                    decode_charge(claim),
                    shared.budget_wait,
                    WORK_LIMIT,
                )
                .await?;
            }
        }
    };
    drop(input);
    // A request that cannot be decoded will never decode: the sender must not
    // retry it.
    let (batches, stats) = decoded.map_err(|e| Fail::new(INVALID_ARGUMENT, e.to_string()))?;
    // Spans the decoder skipped are reported to the sender, who must not
    // retry them: the rest of the request is stored.
    let response = export_response(
        stats.rejected_spans,
        stats.rejected_reason.as_deref().unwrap_or(""),
    );
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(response);
    }

    let (tx, rx) = oneshot::channel();
    // The engine holds the record and its batches until the log has them, so
    // the permits are given back then, even if this call is gone by that time.
    let held = (wire, work);
    // The engine puts the format tag in front of `data`.
    shared
        .engine
        .ingest(RECORD_TAG_OTLP_TRACES, data, batches, move |result| {
            drop(held);
            let _ = tx.send(result);
        })
        .map_err(|e| shared.fail(&e))?;
    // The reply goes out only after the record is durable.
    match rx.await {
        Ok(Ok(())) => Ok(response),
        Ok(Err(e)) => Err(shared.fail(&e)),
        Err(_) => Err(Fail::new(INTERNAL, "the engine dropped the request")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTracePartialSuccess, ExportTraceServiceResponse,
    };
    use prost::Message;

    #[test]
    fn export_response_matches_the_proto_types() {
        assert!(export_response(0, "ignored").is_empty());
        let cases = [
            (1u64, ""),
            (3, "bad span"),
            (127, "x"),
            (128, "boundary"),
            (300_000, &"long".repeat(100)),
            (i64::MAX as u64, "max"),
        ];
        for (rejected, reason) in cases {
            let want = ExportTraceServiceResponse {
                partial_success: Some(ExportTracePartialSuccess {
                    rejected_spans: rejected as i64,
                    error_message: reason.to_owned(),
                }),
            };
            assert_eq!(export_response(rejected, reason), want.encode_to_vec());
            let back =
                ExportTraceServiceResponse::decode(export_response(rejected, reason).as_slice())
                    .unwrap();
            assert_eq!(back, want);
        }
    }

    /// A zstd frame that states no size is charged an assumed size, not the
    /// pool: several of them are admitted at once.
    #[test]
    fn unsized_zstd_frames_are_charged_their_assumed_size() {
        // Magic, a descriptor with no content size, a window, then one last
        // run-length block of 100 bytes.
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 10 << 3];
        frame.extend_from_slice(&(1u32 | 1 << 1 | 100 << 3).to_le_bytes()[..3]);
        frame.push(7);
        let (claim, exact) = claimed_size(Encoding::Zstd, &frame).unwrap();
        assert!(!exact);
        assert_eq!(claim, UNSIZED_FLOOR);
        // 64 times the frame, once that is above the floor, and never above
        // the limit.
        assert_eq!(assumed_size(100), UNSIZED_FLOOR);
        assert_eq!(assumed_size(100_000), 64 * 100_000);
        assert_eq!(assumed_size(usize::MAX), MAX_MESSAGE);
        assert_eq!(assumed_size(MAX_MESSAGE), MAX_MESSAGE);

        // With the default pool (half of 512 MiB), 15 such frames fit at
        // once; each takes its own charge, not all of it.
        let cap = 256 << 20;
        let pool = Arc::new(Semaphore::new(cap));
        let charge = decode_charge(claim).min(cap) as u32;
        assert!((charge as usize) < cap / 10);
        let held: Vec<_> = (0..10)
            .map(|_| pool.clone().try_acquire_many_owned(charge).unwrap())
            .collect();
        assert_eq!(pool.available_permits(), cap - 10 * charge as usize);
        drop(held);
    }

    #[test]
    fn decoding_is_charged_by_the_factor_and_the_fixed_part() {
        assert_eq!(decode_charge(0), DECODE_FIXED);
        assert_eq!(decode_charge(1000), 16 * 1000 + 64 * 1024);
        assert_eq!(decode_charge(usize::MAX), usize::MAX);
    }

    #[test]
    fn grpc_message_is_percent_encoded() {
        assert_eq!(percent_encode("a b%c\n\u{e9}"), "a b%25c%0A%C3%A9");
    }
}
