//! Real process death: a child process ingests and flushes continuously and
//! is killed with SIGKILL at a random moment; the directory is then reopened
//! here and everything the child reported as acknowledged must be there,
//! exactly once.

mod common;

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use nwdb_core::TraceId;
use nwdb_engine::{AggQuery, Engine, ListQuery};
use nwdb_manifest::{Manifest, ManifestOptions, OpenMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const ITERATIONS: u64 = 24;
const SEED: u64 = 0x4b49_4c4c;

struct Run {
    acked: Vec<u64>,
    /// The last flush marker the child printed: a flush was running when
    /// the process died.
    flushing_at_kill: bool,
}

fn run_child(dir: &Path, first_id: u64, pause_us: u64, kill_after: Duration) -> Run {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nwdb-crashtest"))
        .arg(dir)
        .arg(first_id.to_string())
        .arg(pause_us.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the child");
    let out = BufReader::new(child.stdout.take().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut acked = Vec::new();
        let mut flushing = false;
        for line in out.lines() {
            let Ok(line) = line else { break };
            match line.split_once(' ') {
                None if line == "READY" => tx.send(()).unwrap(),
                Some(("A", id)) => acked.push(id.parse::<u64>().unwrap()),
                Some(("F", f)) => flushing = f == "1",
                _ => {}
            }
        }
        (acked, flushing)
    });
    rx.recv_timeout(Duration::from_secs(120))
        .expect("the child never became ready");
    std::thread::sleep(kill_after);
    child.kill().expect("SIGKILL");
    child.wait().unwrap();
    let (acked, flushing_at_kill) = reader.join().unwrap();
    Run {
        acked,
        flushing_at_kill,
    }
}

/// Describes what the dead process left on disk, before recovery touches it.
fn leftovers(dir: &Path) -> (bool, bool) {
    let parts = dir.join("parts");
    let state = Manifest::open(
        dir.join("manifest"),
        OpenMode::MustExist,
        ManifestOptions::default(),
    )
    .unwrap()
    .current();
    let (mut tmp, mut unreferenced) = (false, false);
    for e in std::fs::read_dir(&parts).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.ends_with(".tmp") {
            tmp = true;
        } else if let Ok(id) = name.parse::<u64>()
            && state.part(id).is_none()
        {
            unreferenced = true;
        }
    }
    (tmp, unreferenced)
}

fn present_ids(e: &Engine) -> BTreeSet<u64> {
    let all = e
        .list_traces(&ListQuery {
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            service: None,
            model: None,
            error: None,
            limit: usize::MAX,
        })
        .unwrap();
    let mut ids = BTreeSet::new();
    for s in &all {
        // Each record is one span, so a trace with two was replayed twice.
        assert_eq!(
            s.span_count, 1,
            "trace {} appears more than once",
            s.trace_id
        );
        let id = u128::from_be_bytes(s.trace_id.0) as u64 - 1;
        assert!(ids.insert(id));
    }
    let agg = e
        .aggregate_tokens(&AggQuery {
            from_ns: i64::MIN,
            to_ns: i64::MAX,
            operation: None,
        })
        .unwrap();
    assert_eq!(agg.iter().map(|a| a.count).sum::<u64>(), ids.len() as u64);
    ids
}

#[test]
fn acknowledged_records_survive_sigkill() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let (mut during_flush, mut tmp_dirs, mut unreferenced_dirs, mut total_acked) = (0, 0, 0, 0);

    for iteration in 0..ITERATIONS {
        let kill_after = Duration::from_millis(rng.random_range(5..400));
        // Half the runs ingest flat out; the others leave gaps, so kills also
        // land when the engine is mostly idle or flushing by age.
        let pause_us = if rng.random_bool(0.5) {
            0
        } else {
            rng.random_range(1_000..8_000)
        };
        let run = run_child(dir.path(), iteration * 1_000_000, pause_us, kill_after);
        total_acked += run.acked.len();
        acked.extend(&run.acked);
        during_flush += usize::from(run.flushing_at_kill);

        if dir.path().join("manifest").exists() {
            let (tmp, unreferenced) = leftovers(dir.path());
            tmp_dirs += usize::from(tmp);
            unreferenced_dirs += usize::from(unreferenced);
        }

        // Recover in this process. Alternate between a clean shutdown and
        // simply dropping the engine.
        let mut o = opts(dir.path());
        o.wal.sync = nwdb_wal::SyncMode::Data;
        let e = open(o);
        let present = present_ids(&e);
        for id in &acked {
            assert!(
                present.contains(id),
                "iteration {iteration}: acknowledged record {id} is lost"
            );
            let spans = e
                .get_trace(TraceId((u128::from(*id) + 1).to_be_bytes()))
                .unwrap()
                .map(|b| from_batches(&b))
                .unwrap_or_default();
            assert_eq!(spans.len(), 1, "record {id}");
        }
        // The engine must work after recovery.
        let sentinel = TSpan::new(u128::MAX - u128::from(iteration), 1, 5).root();
        ingest(&e, std::slice::from_ref(&sentinel)).unwrap();
        e.flush().unwrap();
        assert!(e.get_trace(sentinel.trace_id()).unwrap().is_some());
        if iteration % 2 == 0 {
            e.shutdown().unwrap();
        }
        drop(e);
    }

    eprintln!(
        "{ITERATIONS} kills: {total_acked} acknowledged records checked; the child was flushing at \
         the kill in {during_flush} runs; {tmp_dirs} runs left a temporary part directory and \
         {unreferenced_dirs} an unreferenced finished part"
    );
    let e = open(opts(dir.path()));
    let present = present_ids(&e);
    assert!(acked.is_subset(&present));
    assert!(total_acked > 0);
    e.shutdown().unwrap();
}
