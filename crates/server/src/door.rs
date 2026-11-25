//! The OTLP gRPC door: `TraceService/Export` with a codec that hands the
//! request over as the bytes that were sent, so the log stores them as they
//! came and nothing is encoded again.

use bytes::{Buf, Bytes};
use nwdb_core::Error;
use nwdb_engine::Engine;
use nwdb_otlp::{RECORD_TAG_OTLP_TRACES, TraceDecoder};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{Notify, Semaphore, oneshot};
use tonic::Status;
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
use tonic::server::{Grpc, NamedService, UnaryService};

const SERVICE: &str = "opentelemetry.proto.collector.trace.v1.TraceService";

/// Largest request accepted: 64 MiB, or less if the log cannot hold such a
/// record (one byte is the format tag).
pub const MAX_MESSAGE: usize = if 64 << 20 < nwdb_wal::MAX_RECORD_BYTES - 1 {
    64 << 20
} else {
    nwdb_wal::MAX_RECORD_BYTES - 1
};

/// State shared by every request.
pub(crate) struct Shared {
    pub engine: Engine,
    pub decoder: TraceDecoder,
    /// Bytes of requests being decoded or waiting for the log. A request takes
    /// its own size before it is decoded, so many large requests at once
    /// cannot use unbounded memory.
    pub budget: Semaphore,
    pub budget_cap: usize,
    /// Told when the engine reports a fatal error, so the process can stop.
    pub fatal: Arc<Notify>,
}

impl Shared {
    fn status(&self, e: &Error) -> Status {
        match e {
            Error::Unavailable(_) => Status::unavailable(e.to_string()),
            Error::Fatal(_) => {
                self.fatal.notify_one();
                // The sender should retry, against the restarted process.
                Status::unavailable(e.to_string())
            }
            _ => Status::internal(e.to_string()),
        }
    }

    async fn export(&self, data: Bytes) -> Result<(), Status> {
        let want = data.len().clamp(1, self.budget_cap);
        let _permit = self
            .budget
            .acquire_many(u32::try_from(want).unwrap_or(u32::MAX))
            .await
            .map_err(|_| Status::unavailable("shutting down"))?;

        let decoder = self.decoder;
        let (data, decoded) = tokio::task::spawn_blocking(move || {
            let decoded = decoder.decode(&data);
            (data, decoded)
        })
        .await
        .map_err(|_| Status::internal("decoding failed"))?;
        // A request that cannot be decoded will never decode: the sender must
        // not retry it.
        let batches = decoded.map_err(|e| Status::invalid_argument(e.to_string()))?;
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }

        // One spare byte: the engine puts the format tag in front.
        let mut record = Vec::with_capacity(data.len() + 1);
        record.extend_from_slice(&data);
        drop(data);
        let (tx, rx) = oneshot::channel();
        self.engine
            .ingest(RECORD_TAG_OTLP_TRACES, record, batches, move |result| {
                let _ = tx.send(result);
            })
            .map_err(|e| self.status(&e))?;
        // The reply goes out only after the record is durable.
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(self.status(&e)),
            Err(_) => Err(Status::internal("the engine dropped the request")),
        }
    }
}

// ---- codec ----

/// The reply to every export: an empty `ExportTraceServiceResponse`, which is
/// zero bytes on the wire.
struct Empty;

struct RawDecoder;
struct EmptyEncoder;
struct RawCodec;

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

impl Encoder for EmptyEncoder {
    type Item = Empty;
    type Error = Status;
    fn encode(&mut self, _: Empty, _: &mut EncodeBuf<'_>) -> Result<(), Status> {
        Ok(())
    }
}

impl Codec for RawCodec {
    type Encode = Empty;
    type Decode = Bytes;
    type Encoder = EmptyEncoder;
    type Decoder = RawDecoder;
    fn encoder(&mut self) -> EmptyEncoder {
        EmptyEncoder
    }
    fn decoder(&mut self) -> RawDecoder {
        RawDecoder
    }
}

// ---- service ----

/// The tonic service for `TraceService`.
#[derive(Clone)]
pub(crate) struct Door(pub Arc<Shared>);

impl NamedService for Door {
    const NAME: &'static str = SERVICE;
}

struct Export(Arc<Shared>);

impl UnaryService<Bytes> for Export {
    type Response = Empty;
    type Future = BoxFuture<tonic::Response<Empty>, Status>;
    fn call(&mut self, req: tonic::Request<Bytes>) -> Self::Future {
        let shared = self.0.clone();
        Box::pin(async move {
            shared.export(req.into_inner()).await?;
            Ok(tonic::Response::new(Empty))
        })
    }
}

impl<B> Service<http::Request<B>> for Door
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let shared = self.0.clone();
        if req.uri().path() != format!("/{SERVICE}/Export") {
            return Box::pin(async {
                Ok(Status::unimplemented("unknown method").into_http::<tonic::body::Body>())
            });
        }
        Box::pin(async move {
            // Requests may be gzip or zstd compressed; replies never are.
            let mut grpc = Grpc::new(RawCodec)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .max_decoding_message_size(MAX_MESSAGE);
            Ok(grpc.unary(Export(shared), req).await)
        })
    }
}
