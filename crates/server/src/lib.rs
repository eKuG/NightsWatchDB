//! The NightsWatchDB server: an OTLP gRPC door for traces and a small HTTP
//! query API in front of [`nwdb_engine::Engine`]. See the README of this crate.
//!
//! [`Server::start`] opens the database, binds both listeners and returns
//! at once; tests start it on port 0 and read the bound addresses back.

mod api;
pub mod config;
mod door;

pub use door::MAX_MESSAGE;

use nwdb_core::{Error, RecordDecoder, Result};
use nwdb_engine::{Engine, EngineOptions};
use nwdb_otlp::{RECORD_TAG_OTLP_TRACES, TraceDecoder};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinHandle;
use tonic::transport::Server as GrpcServer;
use tonic::transport::server::TcpIncoming;

/// Settings of [`Server::start`].
pub struct ServerOptions {
    /// The storage engine's settings, including the data directory.
    pub engine: EngineOptions,
    /// Listen address of the OTLP gRPC door.
    pub otlp_grpc_addr: SocketAddr,
    /// Listen address of the HTTP query API.
    pub api_addr: SocketAddr,
    /// Bytes of export requests that may be decoded or waiting for the log at
    /// one time. Default 256 MiB.
    pub max_inflight_bytes: usize,
}

impl ServerOptions {
    /// The defaults of the binary: loopback addresses, 256 MiB in flight.
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            engine: EngineOptions::new(data_dir),
            otlp_grpc_addr: SocketAddr::from(([127, 0, 0, 1], 4317)),
            api_addr: SocketAddr::from(([127, 0, 0, 1], 4320)),
            max_inflight_bytes: 256 * 1024 * 1024,
        }
    }
}

/// A running server.
pub struct Server {
    engine: Engine,
    otlp_grpc_addr: SocketAddr,
    api_addr: SocketAddr,
    stop: watch::Sender<bool>,
    fatal: Arc<Notify>,
    grpc: JoinHandle<Result<()>>,
    api: JoinHandle<Result<()>>,
}

impl Server {
    /// Opens (and if needed recovers) the database, then starts both
    /// listeners.
    pub async fn start(options: ServerOptions) -> Result<Server> {
        let limits = options.engine.batch_limits;
        let decoder = TraceDecoder::new(limits);
        let engine = tokio::task::spawn_blocking(move || {
            Engine::open(
                options.engine,
                vec![(
                    RECORD_TAG_OTLP_TRACES,
                    Arc::new(decoder) as Arc<dyn RecordDecoder>,
                )],
            )
        })
        .await
        .map_err(Error::other)??;

        let budget = options.max_inflight_bytes.clamp(1, u32::MAX as usize);
        let fatal = Arc::new(Notify::new());
        let (stop, stopped) = watch::channel(false);

        let incoming = TcpIncoming::bind(options.otlp_grpc_addr)?;
        let otlp_grpc_addr = incoming.local_addr()?;
        let door = door::Door(Arc::new(door::Shared {
            engine: engine.clone(),
            decoder,
            budget: Semaphore::new(budget),
            budget_cap: budget,
            fatal: fatal.clone(),
        }));
        let mut rx = stopped.clone();
        let grpc = tokio::spawn(async move {
            GrpcServer::builder()
                .add_service(door)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = rx.wait_for(|stop| *stop).await;
                })
                .await
                .map_err(Error::other)
        });

        let listener = tokio::net::TcpListener::bind(options.api_addr).await?;
        let api_addr = listener.local_addr()?;
        let router = api::router(engine.clone(), fatal.clone());
        let mut rx = stopped;
        let api = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.wait_for(|stop| *stop).await;
                })
                .await
                .map_err(Error::from)
        });

        Ok(Server {
            engine,
            otlp_grpc_addr,
            api_addr,
            stop,
            fatal,
            grpc,
            api,
        })
    }

    /// The bound address of the OTLP gRPC door.
    pub fn otlp_grpc_addr(&self) -> SocketAddr {
        self.otlp_grpc_addr
    }

    /// The bound address of the HTTP API.
    pub fn api_addr(&self) -> SocketAddr {
        self.api_addr
    }

    /// The engine, for statistics.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Completes when a request met a fatal engine error. The caller should
    /// then [`shutdown`](Self::shutdown) and exit with a failure so that a
    /// supervisor restarts the process and recovery runs.
    pub async fn fatal(&self) {
        self.fatal.notified().await;
    }

    /// Stops accepting, lets requests in progress finish, then flushes and
    /// closes the engine. Returns the first error met.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        let mut first = None;
        for task in [self.grpc, self.api] {
            let result = match task.await {
                Ok(r) => r,
                Err(e) => Err(Error::other(e)),
            };
            if let Err(e) = result {
                first.get_or_insert(e);
            }
        }
        let engine = self.engine;
        let closed = tokio::task::spawn_blocking(move || engine.shutdown())
            .await
            .map_err(Error::other)
            .and_then(|r| r);
        match (first, closed) {
            (Some(e), _) | (None, Err(e)) => Err(e),
            (None, Ok(())) => Ok(()),
        }
    }
}
