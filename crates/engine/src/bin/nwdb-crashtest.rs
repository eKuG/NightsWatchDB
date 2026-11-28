//! A test tool, not part of the database: the child process of the engine's
//! crash tests (`tests/crash.rs`, `tests/crash_stages.rs`). It is built only
//! so the tests can start it and kill it, or let it abort itself.
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
//! - `ABORT <Stage>` immediately before the process aborts itself at a stage
//!   (see `NWDB_ABORT_AT`), so the test can tell that abort from any other
//!   way of dying;
//! - `F 1` / `F 0` when a flush starts (the buffer is frozen) and when it ends
//!   (the log is trimmed), from the engine's stage hook, so the interval
//!   covers the whole flush.
//!
//! Environment:
//!
//! - `NWDB_ABORT_AT=<Stage>:<n>` aborts the process (`std::process::abort`)
//!   the n-th time the engine reaches the named stage (`Stage`'s debug name).
//!   The log then keeps one record per file and flushes are triggered by size
//!   only, so the point at which it aborts does not depend on timing.
//! - `NWDB_BUFFER_BYTES=<n>` overrides the buffer size, so that a recovery
//!   flushes while it replays.
//! - `NWDB_RETENTION_NS=<n>` and `NWDB_CLOCK_NS=<t>` turn retention on with a
//!   clock fixed at `t` (Unix nanoseconds), so the maintenance pass that runs
//!   at start-up removes the parts that are expired at `t` by a replacement
//!   whose stages the abort variable can name.
//! - `NWDB_COMPACT=<n>` turns compaction on with `min_parts` n and no quiet
//!   period, so the maintenance thread merges the parts of a window as soon
//!   as it has n of them (the first pass runs at start-up), and the stages of
//!   a merge can be named by the abort variable.
//! - `NWDB_ANNOTATE=with` also annotates the trace of each acknowledged record
//!   (name `q`, source `c`, value the record's id) and prints `N <id> <value>`
//!   once the annotation is acknowledged. `NWDB_ANNOTATE=existing:<lo>:<hi>`
//!   ingests nothing and annotates the traces of the records `lo..=hi` in turn,
//!   with a growing value, printing `N <id> <value>` for each acknowledged
//!   one; the records must already be in the database.

#[path = "../../tests/common/mod.rs"]
mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::{TAG, TSpan, decoders, encode, opts, to_batches};
use nwdb_core::{Error, TraceId};
use nwdb_engine::{Annotation, Engine, Stage};
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
    let abort_at: Option<(String, u64)> = std::env::var("NWDB_ABORT_AT").ok().map(|v| {
        let (stage, n) = v.split_once(':').expect("NWDB_ABORT_AT=<Stage>:<n>");
        (stage.to_string(), n.parse().expect("occurrence"))
    });

    let mut o = opts(std::path::Path::new(&dir));
    // Real syncs: the point is what survives the process dying. A small
    // buffer and a short age limit make flushes, commits and trims frequent.
    o.wal = WalOptions {
        sync: SyncMode::Data,
        sync_interval: Duration::from_millis(1),
        max_file_bytes: if abort_at.is_some() { 1 } else { 32 * 1024 },
        ..WalOptions::default()
    };
    o.window_ns = 10_000;
    o.buffer_bytes = 48 * 1024;
    o.max_buffered_bytes = 1024 * 1024;
    // A paced child flushes by age with gaps in between, so some kills find the
    // engine idle. A child that aborts at a stage flushes by size only.
    o.flush_interval = Duration::from_millis(if abort_at.is_some() {
        3_600_000
    } else if pause.is_zero() {
        15
    } else {
        80
    });
    if let Ok(n) = std::env::var("NWDB_BUFFER_BYTES") {
        o.buffer_bytes = n.parse().expect("NWDB_BUFFER_BYTES");
    }
    if let Ok(n) = std::env::var("NWDB_RETENTION_NS") {
        o.retention = Some(Duration::from_nanos(n.parse().expect("NWDB_RETENTION_NS")));
        let now: i64 = std::env::var("NWDB_CLOCK_NS")
            .expect("NWDB_CLOCK_NS with NWDB_RETENTION_NS")
            .parse()
            .expect("NWDB_CLOCK_NS");
        o.clock = Some(Arc::new(move || now));
    }
    if let Ok(n) = std::env::var("NWDB_COMPACT") {
        o.compaction.enabled = true;
        o.compaction.min_parts = n.parse().expect("NWDB_COMPACT");
        o.compaction.max_parts = o.compaction.max_parts.max(o.compaction.min_parts);
        o.compaction.quiet = Duration::ZERO;
        o.compaction.retry_backoff = Duration::ZERO;
    }
    let seen = AtomicU64::new(0);
    o.stage_hook = Some(Arc::new(move |stage: Stage| {
        match stage {
            Stage::FlushFrozen => say("F 1"),
            Stage::Trimmed => say("F 0"),
            _ => {}
        }
        if let Some((name, n)) = &abort_at
            && format!("{stage:?}") == *name
            && seen.fetch_add(1, Ordering::SeqCst) + 1 == *n
        {
            say(&format!("ABORT {name}"));
            std::process::abort();
        }
        Ok(())
    }));
    let engine = Engine::open(o, decoders()).expect("open");

    say("READY");
    let mode = std::env::var("NWDB_ANNOTATE").unwrap_or_default();
    let annotated_in_flight = Arc::new(AtomicU64::new(0));
    let annotate = |engine: &Engine, id: u64, value: u64| -> bool {
        let counter = annotated_in_flight.clone();
        counter.fetch_add(1, Ordering::SeqCst);
        let c2 = counter.clone();
        let sent = engine.annotate(
            vec![Annotation {
                trace_id: TraceId((u128::from(id) + 1).to_be_bytes()),
                span_id: None,
                name: "q".into(),
                source: "c".into(),
                time_ns: 1,
                value: Some(value as f64),
                label: None,
                comment: None,
                delete: false,
                arrival: 0,
            }],
            move |r| {
                if r.is_ok() {
                    say(&format!("N {id} {value}"));
                }
                c2.fetch_sub(1, Ordering::SeqCst);
            },
        );
        match sent {
            Ok(r) if r.accepted == 1 => true,
            Ok(_) => {
                counter.fetch_sub(1, Ordering::SeqCst);
                false
            }
            Err(Error::Unavailable(_)) => {
                counter.fetch_sub(1, Ordering::SeqCst);
                false
            }
            Err(e) => panic!("annotate failed: {e}"),
        }
    };
    if let Some(range) = mode.strip_prefix("existing:") {
        let (lo, hi) = range.split_once(':').expect("existing:<lo>:<hi>");
        let (lo, hi): (u64, u64) = (lo.parse().expect("lo"), hi.parse().expect("hi"));
        let mut k = 0u64;
        loop {
            if annotated_in_flight.load(Ordering::SeqCst) >= IN_FLIGHT {
                std::thread::sleep(Duration::from_micros(100));
                continue;
            }
            if annotate(&engine, lo + k % (hi - lo + 1), k) {
                k += 1;
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    let to_annotate: Arc<std::sync::Mutex<Vec<u64>>> = Arc::default();
    let acked = Arc::new(AtomicU64::new(0));
    let mut id = first;
    loop {
        if mode == "with" {
            let ids = std::mem::take(&mut *to_annotate.lock().unwrap());
            for i in ids {
                while !annotate(&engine, i, i) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        if (id - first) - acked.load(Ordering::SeqCst) >= IN_FLIGHT {
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }
        let mut span = TSpan::new(u128::from(id) + 1, 1, (id as i64) * 100).root();
        span.attrs = vec![(id % 251) as u8; 200];
        let spans = [span];
        let counter = acked.clone();
        let pending = to_annotate.clone();
        let sent = engine.ingest(TAG, encode(&spans), to_batches(&spans), move |r| {
            if r.is_ok() {
                say(&format!("A {id}"));
                pending.lock().unwrap().push(id);
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
