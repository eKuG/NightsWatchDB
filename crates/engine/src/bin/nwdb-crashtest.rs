//! A test tool, not part of the database: the child process of the engine's
//! crash test (`tests/crash.rs`). It is built only so the test can start it
//! and kill it with SIGKILL.
//!
//! Usage: `nwdb-crashtest <data dir> <first id> [pause in microseconds]`
//!
//! It opens the engine with a small buffer, so it flushes all the time, then
//! ingests one single-span record per id, starting at `<first id>`, with a
//! bounded number in flight and, if given, a pause after each, so the test can
//! also kill a process that is mostly idle. Standard output is the protocol with the test:
//!
//! - `READY` once the engine is open and recovered;
//! - `A <id>` for each record, after the engine acknowledged it;
//! - `F 1` / `F 0` when a flush starts / ends, as the engine's statistics
//!   report it (sampled, so a very short flush may be missed).

#[path = "../../tests/common/mod.rs"]
mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::{TAG, TSpan, decoders, encode, opts, to_batches};
use nwdb_core::Error;
use nwdb_engine::Engine;
use nwdb_wal::{SyncMode, WalOptions};

/// How many records may wait for their acknowledgement at once.
const IN_FLIGHT: u64 = 32;

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("data directory");
    let first: u64 = args.next().expect("first id").parse().expect("first id");
    let pause = Duration::from_micros(args.next().map_or(0, |p| p.parse().expect("pause")));

    let mut o = opts(std::path::Path::new(&dir));
    // Real syncs: the point is what survives the process dying. A small
    // buffer and a short age limit make flushes, commits and trims frequent.
    o.wal = WalOptions {
        sync: SyncMode::Data,
        sync_interval: Duration::from_millis(1),
        max_file_bytes: 32 * 1024,
        ..WalOptions::default()
    };
    o.window_ns = 10_000;
    o.buffer_bytes = 48 * 1024;
    o.max_buffered_bytes = 1024 * 1024;
    // A paced child flushes by age with gaps in between, so some kills find the
    // engine idle.
    o.flush_interval = Duration::from_millis(if pause.is_zero() { 15 } else { 80 });
    let engine = Engine::open(o, decoders()).expect("open");

    let watcher = engine.clone();
    std::thread::spawn(move || {
        let mut last = false;
        loop {
            let now = watcher.stats().flushing;
            if now != last {
                say(if now { "F 1" } else { "F 0" });
                last = now;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    });

    say("READY");
    let acked = Arc::new(AtomicU64::new(0));
    let mut id = first;
    loop {
        if (id - first) - acked.load(Ordering::SeqCst) >= IN_FLIGHT {
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }
        let mut span = TSpan::new(u128::from(id) + 1, 1, (id as i64) * 100).root();
        span.attrs = vec![(id % 251) as u8; 200];
        let spans = [span];
        let counter = acked.clone();
        let sent = engine.ingest(TAG, encode(&spans), to_batches(&spans), move |r| {
            if r.is_ok() {
                say(&format!("A {id}"));
            }
            counter.fetch_add(1, Ordering::SeqCst);
        });
        match sent {
            Ok(()) => {
                id += 1;
                std::thread::sleep(pause);
            }
            Err(Error::Unavailable(_)) => std::thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("ingest failed: {e}"),
        }
    }
}
