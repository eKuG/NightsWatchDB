//! Wire messages and gRPC glue for the three OTel-Arrow streaming services, written by hand from
//! arrow_service.proto (no code generation, so no protoc).
use bytes::{Buf, Bytes};
use prost::Message;
use std::convert::Infallible;
use std::sync::Arc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder, Streaming};
use tonic::codegen::{http, BoxFuture, Context, Poll, Service};
use tonic::server::{Grpc, NamedService};
use tonic::{Request, Response, Status};

pub const MAX_MSG: usize = 64 << 20;

#[derive(Clone, PartialEq, Message)]
pub struct BatchArrowRecords {
    #[prost(int64, tag = "1")]
    pub batch_id: i64,
    #[prost(message, repeated, tag = "2")]
    pub arrow_payloads: Vec<ArrowPayload>,
    #[prost(bytes = "bytes", tag = "3")]
    pub headers: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct ArrowPayload {
    #[prost(string, tag = "1")]
    pub schema_id: String,
    // ArrowPayloadType as its integer value; unknown values must survive, so no enum type
    #[prost(int32, tag = "2")]
    pub r#type: i32,
    #[prost(bytes = "bytes", tag = "3")]
    pub record: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct BatchStatus {
    #[prost(int64, tag = "1")]
    pub batch_id: i64,
    #[prost(int32, tag = "2")]
    pub status_code: i32,
    #[prost(string, tag = "3")]
    pub status_message: String,
}

pub const STATUS_OK: i32 = 0;
pub const STATUS_UNAVAILABLE: i32 = 14;

// payload type numbers (ArrowPayloadType)
pub const RESOURCE_ATTRS: i32 = 1;
pub const NUMBER_DATA_POINTS: i32 = 11;
pub const SUMMARY_DATA_POINTS: i32 = 12;
pub const HISTOGRAM_DATA_POINTS: i32 = 13;
pub const EXP_HISTOGRAM_DATA_POINTS: i32 = 14;
pub const LOGS: i32 = 30;
pub const SPANS: i32 = 40;
pub const SPAN_ATTRS: i32 = 41;

/// One message as it arrived: the serialized bytes (after gRPC decompression, for the log and the byte count)
/// and the parsed form that borrows its payload buffers from them.
pub struct Inbound {
    pub wire: Bytes,
    pub msg: BatchArrowRecords,
}

#[derive(Default, Clone)]
struct DoorCodec;
struct Dec;
struct Enc;

impl Codec for DoorCodec {
    type Encode = BatchStatus;
    type Decode = Inbound;
    type Encoder = Enc;
    type Decoder = Dec;
    fn encoder(&mut self) -> Enc {
        Enc
    }
    fn decoder(&mut self) -> Dec {
        Dec
    }
}

impl Decoder for Dec {
    type Item = Inbound;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Inbound>, Status> {
        let wire = src.copy_to_bytes(src.remaining());
        let msg = BatchArrowRecords::decode(wire.clone()).map_err(|e| Status::invalid_argument(format!("bad BatchArrowRecords: {e}")))?;
        Ok(Some(Inbound { wire, msg }))
    }
}

impl Encoder for Enc {
    type Item = BatchStatus;
    type Error = Status;
    fn encode(&mut self, item: BatchStatus, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst).map_err(|e| Status::internal(e.to_string()))
    }
}

/// What the stream handler needs from the rest of the program.
pub trait StreamHandler: Send + Sync + 'static {
    /// Takes the inbound stream and returns the stream of acknowledgements.
    fn open(self: Arc<Self>, signal: u8, inbound: Streaming<Inbound>) -> tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, Status>>;
    /// Called with the value of the grpc-encoding request header, before it is rewritten.
    fn saw_encoding(&self, name: &str);
}

pub const TRACES: u8 = 0;
pub const LOGS_SIGNAL: u8 = 1;
pub const METRICS: u8 = 2;

/// One of the three services; S picks which.
pub struct ArrowService<const S: u8>(pub Arc<dyn StreamHandler>);

impl<const S: u8> Clone for ArrowService<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<const S: u8> NamedService for ArrowService<S> {
    const NAME: &'static str = match S {
        TRACES => "opentelemetry.proto.experimental.arrow.v1.ArrowTracesService",
        LOGS_SIGNAL => "opentelemetry.proto.experimental.arrow.v1.ArrowLogsService",
        _ => "opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService",
    };
}

struct Open<const S: u8>(Arc<dyn StreamHandler>);

impl<const S: u8> Service<Request<Streaming<Inbound>>> for Open<S> {
    type Response = Response<tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, Status>>>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Status>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Request<Streaming<Inbound>>) -> Self::Future {
        std::future::ready(Ok(Response::new(self.0.clone().open(S, req.into_inner()))))
    }
}

impl<const S: u8> Service<http::Request<tonic::body::Body>> for ArrowService<S> {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        let h = self.0.clone();
        // The Go exporter names its gRPC compressor "zstdarrow1".."zstdarrow10" (plain zstd, one registered name
        // per level). tonic only knows "zstd", so the name is rewritten before tonic looks at it.
        h.saw_encoding(req.headers().get("grpc-encoding").and_then(|v| v.to_str().ok()).unwrap_or("identity"));
        if let Some(v) = req.headers().get("grpc-encoding") {
            if v.as_bytes().starts_with(b"zstdarrow") {
                req.headers_mut().insert("grpc-encoding", http::HeaderValue::from_static("zstd"));
            }
        }
        Box::pin(async move {
            let mut grpc = Grpc::new(DoorCodec).accept_compressed(tonic::codec::CompressionEncoding::Zstd).max_decoding_message_size(MAX_MSG);
            Ok(grpc.streaming(Open::<S>(h), req).await)
        })
    }
}
