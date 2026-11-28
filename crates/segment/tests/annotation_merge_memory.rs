//! The memory of a merge of annotation parts follows the number of inputs
//! and the block size, not the size of the inputs. Eight inputs, then eight
//! inputs four times as large: the peak must not grow.
//!
//! The peak is the kernel's high-water mark of the process's resident memory
//! (`VmHWM`, read from `/proc`; a counting allocator needs `unsafe`, which the
//! workspace forbids). The inputs are written by the test process; the merge
//! runs in a child process (this test binary, filtered to this one test) so
//! that the memory used to make the inputs is not in the measure.

#![cfg(target_os = "linux")]

mod support;

use nwdb_core::part_dir_name;
use nwdb_segment::{Annotation, AnnotationReader, merge_annotation_parts};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use support::ann::*;

const MIB: usize = 1 << 20;
const CHILD: &str = "NWDB_ANNOTATION_MERGE_MEMORY";
const NAME: &str = "the_merge_does_not_grow_with_the_inputs";
const PARTS: u64 = 8;
/// Records per input: about 4 KiB each, so 2 MiB and 8 MiB an input.
const RECORDS: [u128; 2] = [500, 2000];

fn resident() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    line.split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024
}

/// Inputs of two kinds of identity, so that three quarters of the records are superseded:
/// the even inputs hold the even traces, the odd inputs the odd ones, and
/// later inputs win.
fn write_inputs(dir: &Path, per_part: u128) -> u64 {
    let mut rng = StdRng::seed_from_u64(1);
    let mut bytes = 0;
    for p in 0..PARTS {
        let records: Vec<Annotation> = (0..per_part)
            .map(|i| {
                let mut a = score(i * 2 + (p as u128 % 2) + 1, p + 1);
                a.comment = Some(text(&mut rng, 4000));
                a
            })
            .collect();
        let meta = write_part(dir, p + 1, &records);
        bytes += meta.files.values().sum::<u64>();
    }
    bytes
}

fn child(dir: PathBuf) {
    let readers: Vec<AnnotationReader> = (1..=PARTS)
        .map(|p| AnnotationReader::open(dir.join(part_dir_name(p)), &meta_of(&dir, p)).unwrap())
        .collect();
    let refs: Vec<&AnnotationReader> = readers.iter().collect();
    let before = resident();
    let (meta, stats) =
        merge_annotation_parts(&refs, &dir, 99, WINDOW_START, WINDOW_NS, 1, None).unwrap();
    let peak = resident();
    eprintln!(
        "ANNOTATION-MERGE-MEMORY before={before} peak={peak} rows={} superseded={}",
        meta.rows, stats.superseded
    );
}

/// The metadata of part `p`, rebuilt from its footer (only the last 80 bytes
/// are read, so that the file is not in the child's memory).
fn meta_of(dir: &Path, p: u64) -> nwdb_core::PartMeta {
    let file = std::fs::File::open(dir.join(part_dir_name(p)).join("annotations.nwa")).unwrap();
    let len = file.metadata().unwrap().len();
    let mut f = [0u8; 80];
    file.read_exact_at(&mut f, len - 80).unwrap();
    let u = |at: usize| u64::from_le_bytes(f[at..at + 8].try_into().unwrap());
    nwdb_core::PartMeta {
        part_id: p,
        level: 0,
        window_start_ns: WINDOW_START,
        window_ns: WINDOW_NS,
        rows: u(32),
        traces: u(40),
        min_start_ns: u(56) as i64,
        max_end_ns: u(64) as i64,
        files: [("annotations.nwa".to_string(), len)].into(),
        created_unix_ms: 1,
        format_version: 1,
        payload: Default::default(),
        kind: nwdb_core::PartKind::Annotations,
    }
}

fn run_child(dir: &Path) -> usize {
    let out = Command::new(std::env::current_exe().unwrap())
        .args([NAME, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, dir)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "the merge failed:\n{stderr}");
    let line = stderr
        .lines()
        .find(|l| l.starts_with("ANNOTATION-MERGE-MEMORY"))
        .unwrap_or_else(|| panic!("no report:\n{stderr}"));
    eprintln!("{line}");
    let field = |name: &str| -> usize {
        line.split_whitespace()
            .find_map(|w| w.strip_prefix(name))
            .unwrap()
            .parse()
            .unwrap()
    };
    field("peak=") - field("before=")
}

#[test]
fn the_merge_does_not_grow_with_the_inputs() {
    if let Some(dir) = std::env::var_os(CHILD) {
        child(PathBuf::from(dir));
        return;
    }
    let small = tempfile::tempdir().unwrap();
    let large = tempfile::tempdir().unwrap();
    let small_bytes = write_inputs(small.path(), RECORDS[0]);
    let large_bytes = write_inputs(large.path(), RECORDS[1]);
    assert!(large_bytes > 3 * small_bytes);
    let grew_small = run_child(small.path());
    let grew_large = run_child(large.path());
    eprintln!(
        "{} MiB of inputs raised the resident peak by {} KiB, {} MiB of inputs by {} KiB",
        small_bytes as usize / MIB,
        grew_small / 1024,
        large_bytes as usize / MIB,
        grew_large / 1024
    );
    // Eight cursors of two blocks, the output block and table: a few MiB at
    // most, whatever the size of the inputs.
    assert!(grew_large < 8 * MIB, "{} KiB", grew_large / 1024);
    assert!(
        grew_large <= grew_small + 2 * MIB,
        "{} KiB for the smaller inputs, {} KiB for four times as much",
        grew_small / 1024,
        grew_large / 1024
    );
    // The merge wrote a part of a quarter of the records.
    let merged = AnnotationReader::open(
        large.path().join(part_dir_name(99)),
        &meta_of(large.path(), 99),
    )
    .unwrap();
    merged.verify().unwrap();
    assert_eq!(u128::from(merged.meta().rows), RECORDS[1] * 2);
}
