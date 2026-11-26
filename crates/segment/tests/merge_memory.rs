//! The memory of a merge follows the chunk and the writer's window, not the
//! size of the inputs.
//!
//! The peak is the kernel's high-water mark of a process's resident memory
//! (`VmHWM`, read from `/proc`; a counting allocator needs `unsafe`, which the
//! workspace forbids). The inputs are written by the test process; the merge
//! runs in a child process (this test binary, filtered to this one test) so
//! that the memory used to make the inputs is not in the measure.

#![cfg(target_os = "linux")]

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::{MergeOptions, PartReader, PartWriter, WriterOptions, merge_parts};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const MIB: usize = 1 << 20;
const CHILD: &str = "NWDB_MERGE_MEMORY_PARTS";
const CHILD_INPUTS: &str = "NWDB_MERGE_MEMORY_INPUTS";
const NAME: &str = "merge_memory_follows_the_chunk_not_the_inputs";
const WINDOW_NS: i64 = 900_000_000_000;

const PARTS: u64 = 8;
const ROWS_PER_PART: usize = 40;
/// Cut into pieces of 4 KiB: 192 KiB of text of its own, 4 KiB that every row
/// of every part shares.
const ATTRS: usize = 196 * 1024;
/// Not cut: stays in the row, so that rows are large as stored.
const EVENTS: usize = 96 * 1024;

const CHUNK: usize = 4 * MIB;
const WINDOW: usize = 4 * MIB;

/// Cuts only `attrs`, into 4 KiB pieces.
struct AttrsOnly;

impl PayloadSplitter for AttrsOnly {
    fn cuts(&self, column: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        if column != RawColumn::Attrs {
            return;
        }
        let mut at = 0;
        while at < value.len() {
            let end = (at + 4096).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

fn writer_options() -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(AttrsOnly)),
        dedup_window_bytes: WINDOW,
        row_group_bytes: 4 * MIB,
        // The text index and the row lists have budgets of their own; they are
        // kept small so that what these tests measure is not hidden by them.
        text_index_budget_bytes: 1 << 20,
        row_list_budget_bytes: 1 << 20,
        ..WriterOptions::default()
    }
}

fn resident() -> (usize, usize) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb = |key: &str| -> usize {
        let line = status.lines().find(|l| l.starts_with(key)).unwrap();
        line.split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<usize>()
            .unwrap()
            * 1024
    };
    (kb("VmHWM:"), kb("VmRSS:"))
}

/// Writes the inputs: every part holds rows of every trace, so every trace is
/// stitched from all the parts. With `one_trace` there is a single trace.
fn write_inputs(dir: &Path, one_trace: bool) -> u64 {
    let mut rng = StdRng::seed_from_u64(1);
    let mut shared = vec![0u8; 4096];
    rng.fill_bytes(&mut shared);
    let mut total = 0;
    for p in 0..PARTS {
        let mut w = PartWriter::create(dir, p + 1, 0, WINDOW_NS, writer_options()).unwrap();
        // The spans of a trace are adjacent: a part holds its rows trace by
        // trace.
        let traces = if one_trace { 1 } else { 8 };
        for t in 0..traces {
            let mut b = SpanBatchBuilder::new(BatchLimits {
                max_rows: 64,
                max_bytes: 32 * MIB,
            });
            for r in 0..ROWS_PER_PART / traces {
                let mut attrs = shared.clone();
                let mut own = vec![0u8; ATTRS - 4096];
                rng.fill_bytes(&mut own);
                attrs.extend_from_slice(&own);
                let mut events = vec![0u8; EVENTS];
                rng.fill_bytes(&mut events);
                let span = p * 1_000 + (t * 100 + r) as u64;
                b.append(&SpanRecord {
                    trace_id: TraceId([t as u8 + 1; 16]),
                    span_id: SpanId((span + 1).to_be_bytes()),
                    name: "s",
                    start_ns: (p * 1_000 + (t * 100 + r) as u64) as i64,
                    end_ns: 5_000_000,
                    attrs: &attrs,
                    events: Some(&events),
                    ..SpanRecord::default()
                });
                if b.is_full() {
                    w.write(&b.finish()).unwrap();
                }
            }
            if !b.is_empty() {
                w.write(&b.finish()).unwrap();
            }
        }
        w.finish().unwrap();
        total += dir_size(&dir.join(part_dir_name(p + 1)));
    }
    total
}

fn dir_size(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap().metadata().unwrap().len())
        .sum()
}

/// The child: opens the inputs, merges them, and reports the peak.
fn child(dir: PathBuf, inputs: u64) {
    let readers: Vec<PartReader> = (1..=inputs)
        .map(|p| PartReader::open(dir.join(part_dir_name(p))).unwrap())
        .collect();
    let refs: Vec<&PartReader> = readers.iter().collect();
    let (before, rss) = resident();
    let options = MergeOptions {
        writer: writer_options(),
        chunk_bytes: CHUNK,
        ..MergeOptions::default()
    };
    let (_, stats) = merge_parts(&refs, &dir, 99, 0, WINDOW_NS, options).unwrap();
    let (peak, _) = resident();
    eprintln!(
        "MERGE-MEMORY before={before} rss={rss} peak={peak} rows={} traces={} stitched={}",
        stats.rows_out, stats.traces, stats.traces_stitched
    );
}

fn run_child(dir: &Path, inputs: u64) -> (usize, usize) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args([NAME, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, dir)
        .env(CHILD_INPUTS, inputs.to_string())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "the merge failed:\n{stderr}");
    let line = stderr
        .lines()
        .find(|l| l.starts_with("MERGE-MEMORY"))
        .unwrap_or_else(|| panic!("no report:\n{stderr}"));
    eprintln!("{line}");
    let field = |name: &str| -> usize {
        line.split_whitespace()
            .find_map(|w| w.strip_prefix(name))
            .unwrap()
            .parse()
            .unwrap()
    };
    (field("before="), field("peak="))
}

#[test]
fn merge_memory_follows_the_chunk_not_the_inputs() {
    if let Some(dir) = std::env::var_os(CHILD) {
        let inputs = std::env::var(CHILD_INPUTS).unwrap().parse().unwrap();
        child(PathBuf::from(dir), inputs);
        return;
    }
    for one_trace in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let input_bytes = write_inputs(tmp.path(), one_trace);
        assert!(input_bytes as usize > 70 * MIB);
        // Half of the inputs, and all of them.
        let (before_half, peak_half) = run_child(tmp.path(), PARTS / 2);
        std::fs::remove_dir_all(tmp.path().join(part_dir_name(99))).unwrap();
        let (before, peak) = run_child(tmp.path(), PARTS);
        let (grew_half, grew) = (peak_half - before_half, peak - before);
        eprintln!(
            "one trace: {one_trace}; {} MiB of inputs on disk, chunk {} MiB, window {} MiB; the \
             merge raised the resident peak by {} MiB for half of them and {} MiB for all",
            input_bytes as usize / MIB,
            CHUNK / MIB,
            WINDOW / MIB,
            grew_half / MIB,
            grew / MIB
        );
        // The chunk, the output batch, a row group, the dedup window, a piece
        // block and the buffers of the Parquet readers and writer: a few
        // times the chunk, well under the inputs.
        assert!(
            grew < 40 * MIB,
            "the merge of {} MiB of inputs raised the peak by {} MiB",
            input_bytes as usize / MIB,
            grew / MIB
        );
        // Twice the inputs, the same memory.
        assert!(
            grew <= grew_half + 6 * MIB,
            "{} MiB for half the inputs, {} MiB for all",
            grew_half / MIB,
            grew / MIB
        );
        let merged = PartReader::open(tmp.path().join(part_dir_name(99))).unwrap();
        assert_eq!(merged.row_count(), PARTS * ROWS_PER_PART as u64);
        merged.verify().unwrap();
    }
}
