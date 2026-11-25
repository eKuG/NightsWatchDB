//! The NightsWatchDB server: an OTLP gRPC door for traces and a small HTTP
//! query API in front of [`nwdb_engine::Engine`]. See the README of this crate.
//!
//! [`Server::start`] opens the database, binds both listeners and returns
//! at once; tests start it on port 0 and read the bound addresses back.

mod api;
pub mod config;
mod door;
mod serve;

pub use door::{DECODE_FACTOR, DECODE_FIXED, MAX_MESSAGE};

use nwdb_core::{Error, RecordDecoder, Result};
use nwdb_engine::{Engine, EngineOptions};
use nwdb_otlp::{RECORD_TAG_OTLP_TRACES, TraceDecoder};
use std::net::SocketAddr;
use std::os::unix::fs::DirBuilderExt;

use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinHandle;

/// How often [`Server::fatal`] looks at the engine's fatal flag.
const FATAL_CHECK: Duration = Duration::from_secs(1);

/// Settings of [`Server::start`].
pub struct ServerOptions {
    /// The storage engine's settings, including the data directory.
    pub engine: EngineOptions,
    /// Listen address of the OTLP gRPC door.
    pub otlp_grpc_addr: SocketAddr,
    /// Listen address of the HTTP query API.
    pub api_addr: SocketAddr,
    /// Bytes of export requests that may be read, decoded or waiting for the
    /// log at one time. Default 512 MiB; the README says what it bounds.
    pub max_inflight_bytes: usize,
    /// Host names the API answers to besides `localhost`, the loopback
    /// addresses and the address it is bound to. Default none.
    pub api_allowed_hosts: Vec<String>,
    /// How long shutdown waits for open connections before it drops them.
    /// Default 5 s.
    pub drain_timeout: Duration,
    /// How long a sender may send nothing in the middle of an export before
    /// the call fails. Default 20 s.
    pub stall_timeout: Duration,
    /// How long a sender has to deliver a whole export message, counted from
    /// the moment its budget is taken. Default 30 s. The time after the
    /// request is handed to the engine is not counted.
    pub receive_timeout: Duration,
    /// How long an export waits for a call slot or for budget before it is
    /// refused with `UNAVAILABLE`. Default 5 s.
    pub budget_wait: Duration,
    /// Export calls in progress at once, from before their first byte is
    /// read until they end. Default 256.
    pub max_calls: usize,
    /// How long an API response may make no progress on the socket (the
    /// client does not read) before the connection is closed. Default 30 s.
    pub api_write_timeout: Duration,
    /// Open connections the OTLP door serves at once. Default 1024.
    pub otlp_max_connections: usize,
    /// Open connections the API serves at once. Default 256.
    pub api_max_connections: usize,
}

impl ServerOptions {
    /// The defaults of the binary: loopback addresses, 512 MiB in flight.
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            engine: EngineOptions::new(data_dir),
            otlp_grpc_addr: SocketAddr::from(([127, 0, 0, 1], 4317)),
            api_addr: SocketAddr::from(([127, 0, 0, 1], 4320)),
            max_inflight_bytes: 512 * 1024 * 1024,
            api_allowed_hosts: Vec::new(),
            drain_timeout: Duration::from_secs(5),
            stall_timeout: Duration::from_secs(20),
            receive_timeout: Duration::from_secs(30),
            budget_wait: Duration::from_secs(5),
            max_calls: 256,
            api_write_timeout: Duration::from_secs(30),
            otlp_max_connections: 1024,
            api_max_connections: 256,
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
    grpc: JoinHandle<()>,
    api: JoinHandle<()>,
}

impl Server {
    /// Opens (and if needed recovers) the database, then starts both
    /// listeners.
    pub async fn start(options: ServerOptions) -> Result<Server> {
        let limits = options.engine.batch_limits;
        let decoder = TraceDecoder::new(limits);
        let data_dir = options.engine.data_dir.clone();
        let engine = tokio::task::spawn_blocking(move || {
            // Trace content is private: a directory this process creates is
            // for its own user only. An existing one is left as it is.
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&data_dir)?;
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

        // Half of the budget counts bytes read from the network, half the
        // memory for expanding and decoding them (see the README).
        let budget = options.max_inflight_bytes.clamp(2, u32::MAX as usize);
        let (wire_cap, work_cap) = (budget / 2, budget - budget / 2);
        let fatal = Arc::new(Notify::new());
        let (stop, stopped) = watch::channel(false);
        let drain = options.drain_timeout;

        let listener = TcpListener::bind(options.otlp_grpc_addr).await?;
        let otlp_grpc_addr = listener.local_addr()?;
        let shared = Arc::new(door::Shared {
            engine: engine.clone(),
            decoder,
            wire: Arc::new(Semaphore::new(wire_cap)),
            wire_cap,
            work: Arc::new(Semaphore::new(work_cap)),
            work_cap,
            fatal: fatal.clone(),
            stall: options.stall_timeout,
            receive: options.receive_timeout,
            budget_wait: options.budget_wait,
            calls: Arc::new(Semaphore::new(options.max_calls.max(1))),
        });
        let service = service_fn(move |req| door::handle(shared.clone(), req));
        let h2 = serve::h2_builder();
        let grpc = tokio::spawn(serve::run(
            listener,
            options.otlp_max_connections,
            stopped.clone(),
            drain,
            move |sock, stop| {
                let conn = h2.serve_connection(TokioIo::new(sock), service.clone());
                serve::drive(conn, stop, |c| c.graceful_shutdown())
            },
        ));

        let listener = TcpListener::bind(options.api_addr).await?;
        let api_addr = listener.local_addr()?;
        let hosts = api::HostCheck::new(api_addr.ip(), &options.api_allowed_hosts);
        let service = TowerToHyperService::new(api::router(engine.clone(), fatal.clone(), hosts));
        let h1 = serve::h1_builder();
        let write_timeout = options.api_write_timeout;
        let api = tokio::spawn(serve::run(
            listener,
            options.api_max_connections,
            stopped,
            drain,
            move |sock, stop| {
                let io = serve::TimedWrite::new(TokioIo::new(sock), write_timeout);
                let conn = h1.serve_connection(io, service.clone());
                serve::drive(conn, stop, |c| c.graceful_shutdown())
            },
        ));

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
        // A request that meets the error says so at once. An error in the
        // background (a flush) has no request to tell, so the flag is looked
        // at once a second as well.
        let mut tick = tokio::time::interval(FATAL_CHECK);
        loop {
            tokio::select! {
                () = self.fatal.notified() => return,
                _ = tick.tick() => if self.engine.stats().fatal { return },
            }
        }
    }

    /// Stops accepting and asks open connections to finish. After
    /// [`ServerOptions::drain_timeout`] the ones still open are dropped, so
    /// this always returns. Then it flushes and closes the engine. Returns the
    /// first error met.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        let mut first = None;
        for task in [self.grpc, self.api] {
            if let Err(e) = task.await {
                first.get_or_insert(Error::other(e));
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
