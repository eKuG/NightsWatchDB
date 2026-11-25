//! The binary itself: usage errors and signals.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::ffi::OsStringExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_nightswatchdb");

fn not_utf8() -> OsString {
    OsString::from_vec(vec![b'x', 0xff, 0xfe])
}

#[test]
fn an_argument_that_is_not_utf8_is_a_usage_error() {
    let out = Command::new(BIN).arg(not_utf8()).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("UTF-8"));
}

#[test]
fn an_environment_variable_that_is_not_utf8_is_a_usage_error() {
    for var in ["NWDB_DATA_DIR", "NWDB_API_ADDR", "NWDB_API_ALLOWED_HOSTS"] {
        let out = Command::new(BIN).env(var, not_utf8()).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{var}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(var), "{var}: {err}");
    }
}

fn start(dir: &std::path::Path) -> (Child, std::net::SocketAddr) {
    let mut child = Command::new(BIN)
        .args([
            "--otlp-grpc-addr",
            "127.0.0.1:0",
            "--api-addr",
            "127.0.0.1:0",
        ])
        .arg("--data-dir")
        .arg(dir)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    let addr = loop {
        let line = lines
            .next()
            .expect("the server exited before listening")
            .unwrap();
        if let Some(rest) = line.strip_prefix("listening: OTLP gRPC on ") {
            break rest.split(',').next().unwrap().parse().unwrap();
        }
    };
    // Keep reading so the server never blocks on a full pipe.
    std::thread::spawn(move || lines.for_each(drop));
    (child, addr)
}

fn signal(child: &Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

fn hpack_str(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

/// The bytes of an HTTP/2 connection with one export call that has headers
/// and no body, so the server waits for the message.
fn open_call() -> Vec<u8> {
    let mut block = vec![0x83, 0x86, 0x04];
    hpack_str(
        &mut block,
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    );
    block.push(0x01);
    hpack_str(&mut block, "localhost");
    for (name, value) in [("content-type", "application/grpc"), ("te", "trailers")] {
        block.push(0x00);
        hpack_str(&mut block, name);
        hpack_str(&mut block, value);
    }
    let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    // An empty SETTINGS frame, then HEADERS (end of headers) on stream 1.
    out.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
    out.extend_from_slice(&(block.len() as u32).to_be_bytes()[1..]);
    out.extend_from_slice(&[1, 0x4, 0, 0, 0, 1]);
    out.extend_from_slice(&block);
    out
}

/// While the first signal's shutdown is held up by an open connection, a
/// second signal ends the process at once with code 130.
#[test]
fn a_second_signal_exits_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, door) = start(dir.path());
    // An export call that never sends its message.
    let mut held = TcpStream::connect(door).unwrap();
    held.write_all(&open_call()).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    signal(&child);
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        child.try_wait().unwrap().is_none(),
        "exited after one signal"
    );
    signal(&child);
    let began = Instant::now();
    let code = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s.code();
        }
        assert!(
            began.elapsed() < Duration::from_secs(3),
            "no exit after the second signal"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(code, Some(130));
    drop(held);
}

#[test]
fn one_signal_exits_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, _) = start(dir.path());
    signal(&child);
    let code = child.wait().unwrap().code();
    assert_eq!(code, Some(0));
}

// ---- a signal during start-up ----

const EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const SPANS_PER_EXPORT: usize = 40_000;
const EXPORTS: usize = 20;

fn ld(field: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![field << 3 | 2];
    let mut n = payload.len();
    while n >= 0x80 {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    out.extend_from_slice(payload);
    out
}

/// A gRPC message of `SPANS_PER_EXPORT` spans that carry only their ids.
fn big_message(trace: u8) -> Vec<u8> {
    let spans: Vec<u8> = (0..SPANS_PER_EXPORT as u32)
        .flat_map(|i| {
            let id = [i.to_be_bytes(), [1; 4]].concat();
            ld(2, &[ld(1, &[trace; 16]), ld(2, &id)].concat())
        })
        .collect();
    let request = ld(1, &ld(2, &spans));
    let mut m = vec![0u8];
    m.extend_from_slice(&(request.len() as u32).to_be_bytes());
    m.extend_from_slice(&request);
    m
}

/// Sends the export calls one by one and waits for each reply.
async fn load(door: std::net::SocketAddr) {
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let io = TokioIo::new(tokio::net::TcpStream::connect(door).await.unwrap());
    let (mut send, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(conn);
    for i in 0..EXPORTS {
        let req = http::Request::post(format!("http://localhost{EXPORT}"))
            .header("content-type", "application/grpc")
            .body(Full::new(bytes::Bytes::from(big_message(i as u8 + 1))))
            .unwrap();
        send.ready().await.unwrap();
        let resp = send.send_request(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let trailers = body.collect().await.unwrap().trailers().cloned();
        let ok = |h: &http::HeaderMap| h.get("grpc-status").is_some_and(|v| v == "0");
        assert!(ok(&parts.headers) || trailers.as_ref().is_some_and(ok));
    }
}

/// Starts the server on `dir` and returns it with a reader of its standard
/// error, stopped after the line that says the database is being opened.
fn start_opening(
    dir: &std::path::Path,
) -> (Child, std::io::Lines<BufReader<std::process::ChildStderr>>) {
    let mut child = Command::new(BIN)
        .args([
            "--otlp-grpc-addr",
            "127.0.0.1:0",
            "--api-addr",
            "127.0.0.1:0",
        ])
        .arg("--data-dir")
        .arg(dir)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    loop {
        let line = lines.next().expect("exited before opening").unwrap();
        if line.contains(": opening ") {
            break;
        }
    }
    (child, lines)
}

/// A SIGTERM that arrives while the log is being replayed ends the process at
/// once with code 130, before anything listens. The next start replays the
/// same log and loses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_during_start_up_exits_at_once() {
    let dir = tempfile::tempdir().unwrap();
    // Write a log: load the server and kill it before it flushes.
    let (mut writer, door) = start(dir.path());
    load(door).await;
    writer.kill().unwrap();
    writer.wait().unwrap();

    // Start again, and signal as soon as it begins to open the database.
    let (mut child, mut lines) = start_opening(dir.path());
    let signalled = Instant::now();
    signal(&child);
    let code = tokio::task::spawn_blocking(move || {
        let mut seen = String::new();
        for line in lines.by_ref() {
            seen.push_str(&line.unwrap());
            seen.push('\n');
        }
        let code = child.wait().unwrap().code();
        (code, seen)
    })
    .await
    .unwrap();
    let (code, seen) = code;
    assert_eq!(code, Some(130), "{seen}");
    assert!(seen.contains("while starting"), "{seen}");
    assert!(!seen.contains("listening"), "{seen}");
    assert!(signalled.elapsed() < Duration::from_secs(5));

    // The same start without a signal: the replay takes long enough that the
    // signal above could not have come after it, and the data is all there.
    let (mut child, mut lines) = start_opening(dir.path());
    let opened = Instant::now();
    let (door, api) = loop {
        let line = lines.next().expect("exited before listening").unwrap();
        if let Some(rest) = line.strip_prefix("listening: OTLP gRPC on ") {
            let (door, api) = rest.split_once(", HTTP API on ").unwrap();
            break (door.to_owned(), api.to_owned());
        }
    };
    let replay = opened.elapsed();
    let _ = door;
    std::thread::spawn(move || lines.for_each(drop));
    assert!(
        replay > Duration::from_millis(300),
        "the replay took only {replay:?}: too fast for a signal to be sure to land in it"
    );
    let counts = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(api).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        write!(
            s,
            "GET /api/v1/tokens?from=0&to=9000000000000000000 HTTP/1.1\r\nHost: localhost\r\n\
             Connection: close\r\n\r\n"
        )
        .unwrap();
        let mut raw = String::new();
        s.read_to_string(&mut raw).unwrap();
        raw.split("\"count\":")
            .skip(1)
            .map(|r| {
                r.split(|c: char| !c.is_ascii_digit())
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .sum::<usize>()
    })
    .await
    .unwrap();
    assert_eq!(counts, EXPORTS * SPANS_PER_EXPORT);
    signal(&child);
    assert_eq!(child.wait().unwrap().code(), Some(0));
}
