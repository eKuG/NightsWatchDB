//! `flush()` and `ingest()` from several threads while `shutdown()` runs.

mod common;
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use common::*;
use nwdb_core::Error;
use nwdb_engine::Engine;
use support::{present_ids, tree};

const ROUNDS: u64 = 25;

fn record(id: u64) -> Vec<TSpan> {
    let mut s = TSpan::new(u128::from(id) + 1, 1, (id as i64) * 100).root();
    s.attrs = vec![7; 100];
    vec![s]
}

fn shut_down_error(e: &Error) -> bool {
    matches!(e, Error::Unavailable(m) if m.contains("shut"))
}

#[test]
fn nothing_acknowledged_is_lost_and_nothing_is_written_after_shutdown_returns() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().unwrap();
        let mut o = opts(dir.path());
        // Small buffer and files: the flusher, the callers and shutdown all
        // flush and trim while the threads run.
        o.buffer_bytes = 4096;
        o.wal.max_file_bytes = 2048;
        let e = open(o.clone());
        let acked = Arc::new(Mutex::new(BTreeSet::new()));

        let mut threads = Vec::new();
        for t in 0..4u64 {
            let (e, acked) = (e.clone(), acked.clone());
            threads.push(std::thread::spawn(move || {
                for i in 0.. {
                    if t % 2 == 0 {
                        let id = t * 1_000_000 + i;
                        match ingest(&e, &record(id)) {
                            Ok(()) => {
                                acked.lock().unwrap().insert(id);
                            }
                            Err(Error::Unavailable(m)) if !m.contains("shut") => {
                                std::thread::sleep(Duration::from_micros(200));
                            }
                            Err(err) if shut_down_error(&err) => return,
                            Err(err) => panic!("ingest: {err}"),
                        }
                    } else {
                        match e.flush() {
                            Ok(()) => std::thread::sleep(Duration::from_micros(300)),
                            Err(err) if shut_down_error(&err) => return,
                            Err(err) => panic!("flush: {err}"),
                        }
                    }
                }
            }));
        }
        std::thread::sleep(Duration::from_millis(5 + (round % 5) * 4));

        // Two shutdowns at once: neither may return before the directory
        // lock is released, so whichever is first, a second engine can open
        // the directory the moment it has returned.
        let (tx, rx) = mpsc::channel();
        let shutdowns: Vec<_> = (0..2)
            .map(|_| {
                let (e, tx) = (e.clone(), tx.clone());
                std::thread::spawn(move || {
                    let r = e.shutdown();
                    tx.send(()).unwrap();
                    r
                })
            })
            .collect();
        rx.recv_timeout(Duration::from_secs(60))
            .expect("shutdown hangs");
        let after = tree(dir.path());
        let second = Engine::open(opts(dir.path()), decoders())
            .unwrap_or_else(|e| panic!("round {round}: the directory is still locked: {e}"));
        second.shutdown().unwrap();
        for s in shutdowns {
            s.join().unwrap().unwrap();
        }
        for t in threads {
            t.join().unwrap();
        }
        // Nothing the racing callers did touched the files after shutdown had
        // returned (the second engine above only reads and syncs).
        let settled = tree(dir.path());
        let strip = |t: &std::collections::BTreeMap<String, (u64, u64)>| {
            t.iter()
                .filter(|(k, _)| !k.starts_with("wal/"))
                .map(|(k, v)| (k.clone(), *v))
                .collect::<Vec<_>>()
        };
        assert_eq!(strip(&settled), strip(&after), "round {round}");

        let acked = acked.lock().unwrap().clone();
        let e = open(opts(dir.path()));
        let present = present_ids(&e);
        assert!(
            acked.is_subset(&present),
            "round {round}: lost {:?}",
            acked.difference(&present).collect::<Vec<_>>()
        );
        e.shutdown().unwrap();
    }
}
