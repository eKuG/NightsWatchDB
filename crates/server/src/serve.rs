//! The accept loop both listeners share, and the connection settings of each
//! protocol. The gRPC door speaks HTTP/2 without TLS only; the API speaks
//! HTTP/1.1 only.

use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::server::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Sleep;

/// Concurrent streams one door connection may have open.
const MAX_STREAMS: u32 = 64;
/// Largest header list a door request may carry.
const MAX_HEADER_LIST: u32 = 16 * 1024;
/// Per-stream and per-connection flow-control window. Bytes a sender has
/// pushed but the door has not read yet are bounded by this, so it is kept
/// small: the byte budget only counts what the door has chosen to read. The
/// connection's window is shared by its streams, so a connection holds at
/// most this much unread data, however many streams it has.
const WINDOW: u32 = 128 * 1024;
/// Largest HTTP/2 frame a client may send (the protocol's smallest).
const FRAME: u32 = 16 * 1024;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING_TIMEOUT: Duration = Duration::from_secs(10);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Settings of a door connection (HTTP/2, no TLS).
pub(crate) fn h2_builder() -> http2::Builder<TokioExecutor> {
    let mut b = http2::Builder::new(TokioExecutor::new());
    b.timer(TokioTimer::new())
        .max_concurrent_streams(MAX_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST)
        .max_frame_size(FRAME)
        .adaptive_window(false)
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

/// A connection whose writes stop making progress for `limit` fails with
/// `TimedOut`, which makes hyper drop the connection and everything its
/// response holds. A client that asks for a large response and never reads it
/// would otherwise keep it, and what produces it, for as long as it likes.
/// Reads are not limited here.
pub(crate) struct TimedWrite<T> {
    inner: T,
    limit: Duration,
    /// Running while a write or flush is waiting for the socket.
    timer: Option<Pin<Box<Sleep>>>,
}

impl<T> TimedWrite<T> {
    pub(crate) fn new(inner: T, limit: Duration) -> Self {
        Self {
            inner,
            limit,
            timer: None,
        }
    }

    /// Starts the timer when `r` is pending, stops it when not, and turns a
    /// pending result into an error once the timer has run out.
    fn watch<R>(&mut self, cx: &mut Context<'_>, r: Poll<io::Result<R>>) -> Poll<io::Result<R>> {
        if r.is_ready() {
            self.timer = None;
            return r;
        }
        let limit = self.limit;
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        if timer.as_mut().poll(cx).is_ready() {
            self.timer = None;
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::TimedOut)));
        }
        Poll::Pending
    }
}

impl<T: Read + Unpin> Read for TimedWrite<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<T: Write + Unpin> Write for TimedWrite<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.watch(cx, r)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.watch(cx, r)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_flush(cx);
        this.watch(cx, r)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.watch(cx, r)
    }
}
