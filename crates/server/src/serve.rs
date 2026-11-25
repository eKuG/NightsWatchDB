//! The accept loop both listeners share, and the connection settings of each
//! protocol. The gRPC door speaks HTTP/2 without TLS only; the API speaks
//! HTTP/1.1 only.

use hyper::server::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

/// Concurrent streams one door connection may have open.
const MAX_STREAMS: u32 = 64;
/// Largest header list a door request may carry.
const MAX_HEADER_LIST: u32 = 16 * 1024;
/// Per-stream and per-connection flow-control window. Bytes a sender has
/// pushed but the door has not read yet are bounded by this, so it is kept
/// small: the byte budget only counts what the door has chosen to read.
const WINDOW: u32 = 128 * 1024;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING_TIMEOUT: Duration = Duration::from_secs(10);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Settings of a door connection (HTTP/2, no TLS).
pub(crate) fn h2_builder() -> http2::Builder<TokioExecutor> {
    let mut b = http2::Builder::new(TokioExecutor::new());
    b.timer(TokioTimer::new())
        .max_concurrent_streams(MAX_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST)
        .initial_stream_window_size(WINDOW)
        .initial_connection_window_size(WINDOW)
        .keep_alive_interval(PING_INTERVAL)
        .keep_alive_timeout(PING_TIMEOUT);
    b
}

/// Settings of an API connection (HTTP/1.1).
pub(crate) fn h1_builder() -> http1::Builder {
    let mut b = http1::Builder::new();
    b.timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    b
}

/// Drives one connection. When `stop` fires it asks the connection to finish
/// what it is doing and keeps driving it; the accept loop aborts it if that
/// takes too long.
pub(crate) async fn drive<C>(
    conn: C,
    mut stop: watch::Receiver<bool>,
    graceful: impl FnOnce(Pin<&mut C>),
) where
    C: Future<Output = hyper::Result<()>>,
{
    let mut conn = pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => return,
        _ = stopped(&mut stop) => graceful(conn.as_mut()),
    }
    let _ = conn.await;
}

/// Accepts connections until `stop` fires, running `serve` for each. At most
/// `max_connections` are open at once; the others wait in the kernel's accept
/// queue. After `stop` it waits up to `drain` for the open connections to
/// finish, then aborts them, so it always returns.
pub(crate) async fn run<F, Fut>(
    listener: TcpListener,
    max_connections: usize,
    mut stop: watch::Receiver<bool>,
    drain: Duration,
    serve: F,
) where
    F: Fn(TcpStream, watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let slots = Arc::new(Semaphore::new(max_connections.max(1)));
    let mut conns = JoinSet::new();
    loop {
        // Wait for a free slot before accepting, so that connections over the
        // limit stay in the queue instead of being open and idle here.
        let slot = tokio::select! {
            s = slots.clone().acquire_owned() => match s { Ok(s) => s, Err(_) => break },
            _ = stopped(&mut stop) => break,
        };
        let sock = tokio::select! {
            r = listener.accept() => match r {
                Ok((sock, _)) => sock,
                Err(e) => {
                    // Typically out of file descriptors; do not spin.
                    eprintln!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = stopped(&mut stop) => break,
        };
        let _ = sock.set_nodelay(true);
        let conn = serve(sock, stop.clone());
        conns.spawn(async move {
            conn.await;
            drop(slot);
        });
        while conns.try_join_next().is_some() {}
    }
    drop(listener);
    let drained =
        tokio::time::timeout(drain, async { while conns.join_next().await.is_some() {} }).await;
    if drained.is_err() {
        conns.abort_all();
        while conns.join_next().await.is_some() {}
    }
}

/// Completes when `stop` fires or its sender is gone. A function of its own so
/// that the watch guard is not held across an await.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}
