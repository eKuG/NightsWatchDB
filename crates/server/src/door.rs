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
use nwdb_otlp::{RECORD_TAG_OTLP_TRACES, TraceDecoder};
use std::convert::Infallible;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

const EXPORT_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const ACCEPT_ENCODING: &str = "identity,gzip,zstd";

/// How much larger than its bytes a decoded request is assumed to be. The
/// worst case measured for the decoder is 10.1 times the request (requests
/// of spans that carry only their ids, see the README of the otlp crate), so
/// 11 leaves room for what each batch adds on top.
const DECODE_FACTOR: usize = 11;
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
}

/// A gRPC error: status code and message.
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

/// Takes `want` bytes (at least 1, at most the whole pool) from `pool`. A
/// request bigger than the pool gets the whole pool and so runs alone.
async fn charge(
    pool: &Arc<Semaphore>,
    cap: usize,
    want: usize,
) -> Result<OwnedSemaphorePermit, Fail> {
    let n = u32::try_from(want.clamp(1, cap)).unwrap_or(u32::MAX);
    pool.clone()
        .acquire_many_owned(n)
        .await
        .map_err(|_| Fail::new(UNAVAILABLE, "shutting down"))
}

// ---- reading the request ----

/// Request body chunks, each wait limited by the stall timeout.
struct Chunks {
    body: Incoming,
    stall: Duration,
}

impl Chunks {
    /// The next non-empty chunk, or `None` at the end of the stream.
    async fn next(&mut self) -> Result<Option<Bytes>, Fail> {
        loop {
            match tokio::time::timeout(self.stall, self.body.frame()).await {
                Err(_) => {
                    return Err(Fail::new(DEADLINE_EXCEEDED, "the sender stopped sending"));
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
    // The budget is taken before the payload is read.
    let permit = charge(&shared.wire, shared.wire_cap, len).await?;
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

/// The size `data` says it expands to, never above [`MAX_MESSAGE`].
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
            // The frame does not say; assume the worst.
            Ok(None) => (MAX_MESSAGE as u64, false),
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

/// Expands `data` to at most `claim` bytes. Output past the claim, a size that
/// differs from an exact claim, bytes after the compressed stream and
/// corrupt data are all errors.
fn decompress(enc: Encoding, data: &[u8], claim: usize, exact: bool) -> Result<Vec<u8>, Fail> {
    let bad = |what: &str| Fail::new(INVALID_ARGUMENT, format!("bad {what} data"));
    let mut input = data;
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
    if out.len() > claim || (exact && out.len() != claim) {
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
    Ok(out)
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
    let (compressed, data, _wire) = read_message(shared, req.into_body()).await?;
    if compressed && enc == Encoding::Identity {
        return Err(Fail::new(
            INVALID_ARGUMENT,
            "compressed message without grpc-encoding",
        ));
    }

    // Expanding and decoding take memory beyond the bytes that were read.
    let (claim, exact) = if compressed {
        claimed_size(enc, &data)?
    } else {
        (data.len(), true)
    };
    let _work = charge(
        &shared.work,
        shared.work_cap,
        claim.saturating_mul(DECODE_FACTOR),
    )
    .await?;

    let decoder = shared.decoder;
    let (data, decoded) = tokio::task::spawn_blocking(move || {
        let data = if compressed {
            decompress(enc, &data, claim, exact)?
        } else {
            data
        };
        let decoded = decoder.decode_with_stats(&data);
        Ok::<_, Fail>((data, decoded))
    })
    .await
    .map_err(|_| Fail::new(INTERNAL, "decoding failed"))??;
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
    // The engine puts the format tag in front of `data`.
    shared
        .engine
        .ingest(RECORD_TAG_OTLP_TRACES, data, batches, move |result| {
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

    #[test]
    fn grpc_message_is_percent_encoded() {
        assert_eq!(percent_encode("a b%c\n\u{e9}"), "a b%25c%0A%C3%A9");
    }
}
