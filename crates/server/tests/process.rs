//! The binary itself: usage errors and signals.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
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
