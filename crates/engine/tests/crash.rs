//! Real process death: a child process ingests and flushes continuously and
//! is killed with SIGKILL at a random moment; the directory is then reopened
//! here and everything the child reported as acknowledged must be there,
//! exactly once.

mod common;
mod support;

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use common::*;
use nwdb_core::TraceId;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use support::{disk_state, present_ids, run_child};

const ITERATIONS: u64 = 24;
const SEED: u64 = 0x4b49_4c4c;

/// Describes what the dead process left on disk, before recovery touches it.
/// The manifest is read from a copy: opening it in place would remove its
/// temporary files and sync it, and so change what recovery then finds.
fn leftovers(dir: &Path) -> (bool, bool) {
    let (_, unreferenced) = disk_state(dir);
    let tmp = unreferenced.iter().any(|n| n.ends_with(".tmp"));
    (tmp, unreferenced.iter().any(|n| !n.ends_with(".tmp")))
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
        let run = run_child(
            dir.path(),
            iteration * 1_000_000,
            pause_us,
            &[],
            Some(kill_after),
        );
        total_acked += run.acked.len();
        acked.extend(&run.acked);
        during_flush += usize::from(run.flushing_at_end);

        if dir.path().join("manifest").exists() {
            let (tmp, unreferenced) = leftovers(dir.path());
            tmp_dirs += usize::from(tmp);
            unreferenced_dirs += usize::from(unreferenced);
        }

        // Recover in this process. Half the recoveries use a small buffer, so
        // the flush that replay needs when the buffer fills runs too.
        // Alternate between a clean shutdown and simply dropping the engine.
        let mut o = opts(dir.path());
        o.wal.sync = nwdb_wal::SyncMode::Data;
        if iteration % 4 >= 2 {
            o.buffer_bytes = 2048;
        }
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
