//! A merge whose output cannot be opened marks its window as damaged until a
//! restart, although no input is damaged.

mod common;
mod support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::*;
use nwdb_core::PartKind;
use nwdb_engine::Stage;
use support::disk_state;

/// A comment of 4000 letters that differs from item to item, so the merge has
/// real work to do while its output file exists.
fn comment(seed: u64) -> String {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..4000)
        .map(|_| {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            char::from(b'a' + ((x >> 33) % 26) as u8)
        })
        .collect()
}

/// Waits for the temporary directory of a part being written and removes the
/// annotation file inside it while the writer still has it open: the writer
/// finishes and renames the directory, and the file is then missing.
fn unlink_output_once(parts: PathBuf) -> JoinHandle<bool> {
    std::thread::spawn(move || {
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(60) {
            for entry in std::fs::read_dir(&parts).into_iter().flatten().flatten() {
                if entry.file_name().to_string_lossy().ends_with(".tmp")
                    && std::fs::remove_file(entry.path().join("annotations.nwa")).is_ok()
                {
                    return true;
                }
            }
        }
        false
    })
}

/// Defect: `merge_one` opens the merge output with `AnnotationReader::open`
/// before the replacement (which is right: a part that cannot be opened must
/// not replace its inputs), but a failure there comes back as `Corrupt`
/// ("file is missing from the part") and `note_merge_failure` treats every
/// `Corrupt` as damage in an input: the (window, kind) goes into `damaged` and
/// is left alone until a restart. The README keeps "until the next start" for
/// damage found in an input; any other error before the commit backs off for
/// `retry_backoff`.
///
/// Sequence: one window, one span part, two annotation parts of 1000 records
/// with long comments; compaction with `retry_backoff` zero. While the merge
/// writes its output, the output file is unlinked (the writer keeps writing
/// to the open file and renames the directory into place); the open of the
/// output fails and the merge is abandoned.
///
/// Expected: the inputs stay live and the manifest never lists the output
/// (this part holds on this tree and is the check that the output is opened
/// before the replacement); the next pass, the backoff being zero, merges the
/// window. On this tree it does not: the window is "damaged" until a restart.
#[test]
fn a_merge_output_that_cannot_be_opened_is_retried_after_the_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let parts = dir.path().join("parts");
    let started = Arc::new(AtomicBool::new(false));
    let watcher: Arc<Mutex<Option<JoinHandle<bool>>>> = Arc::default();
    let mut o = compact_opts(dir.path());
    o.stage_hook = Some({
        let (started, watcher) = (started.clone(), watcher.clone());
        Arc::new(move |s| {
            if s == Stage::MergeStarting && !started.swap(true, Ordering::SeqCst) {
                *watcher.lock().unwrap() = Some(unlink_output_once(parts.clone()));
            }
            Ok(())
        })
    });
    let e = open(o);
    ingest(&e, &[TSpan::new(1, 1, 10).root()]).unwrap();
    e.flush().unwrap();
    for prefix in ["a", "b"] {
        let items = (0..1000u64)
            .map(|i| {
                let mut a = score(1, None, &format!("{prefix}{i}"), "s", i as f64);
                a.comment = Some(comment(i + if prefix == "a" { 0 } else { 5000 }));
                a
            })
            .collect();
        annotate(&e, items);
        e.flush().unwrap();
    }
    let inputs: Vec<u64> = e
        .snapshot()
        .annotation_part_info()
        .iter()
        .map(|x| x.0)
        .collect();
    assert_eq!(inputs.len(), 2);

    let first = e.run_maintenance();
    let unlinked = watcher.lock().unwrap().take().unwrap().join().unwrap();
    assert!(
        unlinked,
        "the output file was not caught while being written"
    );
    assert!(first.is_err(), "the merge cannot have succeeded: {first:?}");
    let s = e.stats();
    assert!(!s.fatal);
    // The output was opened before the replacement: nothing was replaced.
    assert_eq!(s.merges_done, 0, "{s:?}");
    let listed: Vec<u64> = disk_state(dir.path())
        .0
        .parts
        .iter()
        .filter(|p| p.kind == PartKind::Annotations)
        .map(|p| p.part_id)
        .collect();
    assert_eq!(listed, inputs);
    assert!(
        disk_state(dir.path()).1.is_empty(),
        "the output directory is left"
    );
    assert_eq!(shown(&e, 1).len(), 2000);

    // The inputs are healthy and the backoff is zero.
    e.run_maintenance().unwrap();
    let s = e.stats();
    assert_eq!(
        (s.merges_done, s.annotation_parts),
        (1, 1),
        "the window was not merged again although no input is damaged; \
         last_compaction_error: {:?}",
        s.last_compaction_error
    );
    e.shutdown().unwrap();
}
