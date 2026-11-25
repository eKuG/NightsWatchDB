//! gRPC front: one raw-bytes codec and three Export services (traces, logs, metrics).
//! The request message is handed over as the bytes received (after gRPC decompression),
//! so the WAL stores them as they came and the lazy walker can read them in place.
use crate::wal;
use bytes::{Buf, Bytes};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::server::{Grpc, NamedService, UnaryService};
use tonic::Status;

pub const MAX_MESSAGE: usize = 64 << 20;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Signal {
    Traces,
    Logs,
    Metrics,
}

pub struct Work {
    pub signal: Signal,
    pub data: Bytes,
    pub reply: oneshot::Sender<Result<(), String>>,
}

/// Counters shared with main for the final report. Times are nanoseconds since `base`.
pub struct Counters {
    pub base: Instant,
    pub requests: AtomicU64,
    pub wire_bytes: AtomicU64,
    pub max_request: AtomicU64,
    pub rejected: AtomicU64,
    pub active: AtomicI64,
    pub first_ns: AtomicU64,
    pub last_ns: AtomicU64,
}

impl Counters {
    pub fn new() -> Self {
        Counters { base: Instant::now(), requests: 0.into(), wire_bytes: 0.into(), max_request: 0.into(), rejected: 0.into(), active: 0.into(), first_ns: u64::MAX.into(), last_ns: 0.into() }
    }
    pub fn now(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }
}

/// Everything a request handler needs. Dropping the last clone closes the channels, which
/// is how the WAL and converter threads learn that the server has finished.
pub struct Shared {
    pub wal: Option<mpsc::Sender<wal::Item>>,
    pub conv: Option<mpsc::Sender<Work>>,
    /// Bytes of requests in flight; a request waits here before it is logged or converted.
    pub inflight: Semaphore,
    pub inflight_cap: usize,
    pub counters: Arc<Counters>,
}

impl Shared {
    async fn export(&self, signal: Signal, data: Bytes) -> Result<(), Status> {
        let c = &self.counters;
        let n = data.len() as u64;
        c.first_ns.fetch_min(c.now(), Relaxed);
        c.active.fetch_add(1, Relaxed);
        c.requests.fetch_add(1, Relaxed);
        c.wire_bytes.fetch_add(n, Relaxed);
        c.max_request.fetch_max(n, Relaxed);
        let r = self.process(signal, data).await;
        if r.is_err() {
            c.rejected.fetch_add(1, Relaxed);
        }
        c.last_ns.fetch_max(c.now(), Relaxed);
        c.active.fetch_sub(1, Relaxed);
        r
    }

    async fn process(&self, signal: Signal, data: Bytes) -> Result<(), Status> {
        let _permit = self.inflight.acquire_many(data.len().clamp(1, self.inflight_cap) as u32).await.map_err(|_| Status::unavailable("shutting down"))?;
        let log = async {
            let Some(w) = &self.wal else { return Ok(()) };
            let (ack, rx) = oneshot::channel();
            w.send(wal::Item { data: data.clone(), ack }).await.map_err(|_| Status::unavailable("wal stopped"))?;
            rx.await.map_err(|_| Status::internal("wal stopped"))?.map_err(|e| Status::internal(format!("wal: {e}")))
        };
        let conv = async {
            let Some(w) = &self.conv else { return Ok(()) };
            let (reply, rx) = oneshot::channel();
            w.send(Work { signal, data: data.clone(), reply }).await.map_err(|_| Status::unavailable("converter stopped"))?;
            rx.await.map_err(|_| Status::internal("converter stopped"))?.map_err(|e| Status::invalid_argument(format!("decode: {e}")))
        };
        let (a, b) = tokio::join!(log, conv);
        a.and(b)
    }
}

// ---- codec ----

struct RawCodec<R>(PhantomData<R>);

struct RawDecoder;
struct ProstEncoder<R>(PhantomData<R>);

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

impl<R: prost::Message> Encoder for ProstEncoder<R> {
    type Item = R;
    type Error = Status;
    fn encode(&mut self, item: R, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst).map_err(|e| Status::internal(e.to_string()))
    }
}

impl<R: prost::Message + Default + Send + 'static> Codec for RawCodec<R> {
    type Encode = R;
    type Decode = Bytes;
    type Encoder = ProstEncoder<R>;
    type Decoder = RawDecoder;
    fn encoder(&mut self) -> Self::Encoder {
        ProstEncoder(PhantomData)
    }
    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

// ---- services ----

pub trait Kind: Send + Sync + 'static {
    const NAME: &'static str;
    const SIGNAL: Signal;
    type Resp: prost::Message + Default + Send + 'static;
}

macro_rules! kind {
    ($t:ident, $name:expr, $sig:expr, $resp:ty) => {
        pub struct $t;
        impl Kind for $t {
            const NAME: &'static str = $name;
            const SIGNAL: Signal = $sig;
            type Resp = $resp;
        }
    };
}

use opentelemetry_proto::tonic::collector::{logs::v1::ExportLogsServiceResponse, metrics::v1::ExportMetricsServiceResponse, trace::v1::ExportTraceServiceResponse};
kind!(TraceK, "opentelemetry.proto.collector.trace.v1.TraceService", Signal::Traces, ExportTraceServiceResponse);
kind!(LogsK, "opentelemetry.proto.collector.logs.v1.LogsService", Signal::Logs, ExportLogsServiceResponse);
kind!(MetricsK, "opentelemetry.proto.collector.metrics.v1.MetricsService", Signal::Metrics, ExportMetricsServiceResponse);

pub struct Door<K>(pub Arc<Shared>, PhantomData<K>);

impl<K> Door<K> {
    pub fn new(s: Arc<Shared>) -> Self {
        Door(s, PhantomData)
    }
}

impl<K> Clone for Door<K> {
    fn clone(&self) -> Self {
        Door(self.0.clone(), PhantomData)
    }
}

impl<K: Kind> NamedService for Door<K> {
    const NAME: &'static str = K::NAME;
}

struct Export<K>(Arc<Shared>, PhantomData<K>);

impl<K: Kind> UnaryService<Bytes> for Export<K> {
    type Response = K::Resp;
    type Future = BoxFuture<tonic::Response<K::Resp>, Status>;
    fn call(&mut self, req: tonic::Request<Bytes>) -> Self::Future {
        let s = self.0.clone();
        Box::pin(async move {
            s.export(K::SIGNAL, req.into_inner()).await?;
            Ok(tonic::Response::new(K::Resp::default()))
        })
    }
}

impl<K: Kind, B> Service<http::Request<B>> for Door<K>
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
        if req.uri().path().strip_prefix('/').and_then(|p| p.strip_prefix(K::NAME)) != Some("/Export") {
            return Box::pin(async { Ok(Status::unimplemented("unknown method").into_http::<tonic::body::Body>()) });
        }
        Box::pin(async move {
            // gzip and zstd request compression are accepted; responses are never compressed
            let mut grpc = Grpc::new(RawCodec::<K::Resp>(PhantomData))
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .max_decoding_message_size(MAX_MESSAGE);
            Ok(grpc.unary(Export::<K>(shared, PhantomData), req).await)
        })
    }
}
