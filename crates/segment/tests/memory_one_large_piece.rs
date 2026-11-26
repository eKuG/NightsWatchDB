//! The writer's text index and one large piece of distinct
//! words.
//!
//! `WriterOptions::text_index_budget_bytes` is the memory the text index may
//! hold for its open segment, and the README says that at most the budget is
//! added to the peak of a part being written. The builder checks the budget
//! between documents, and a document is a whole piece, so one piece of many
//! distinct words is held in full however small the budget. The peak is the
//! kernel's high-water mark of resident memory (`VmHWM`); this file has one
//! test so nothing else runs at the same time.

#![cfg(target_os = "linux")]

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
};
use nwdb_segment::{PartWriter, WriterOptions};
use std::ops::Range;
use std::sync::Arc;

const MIB: usize = 1 << 20;

/// The whole value is one piece.
struct Whole;

impl PayloadSplitter for Whole {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.push(0..value.len());
    }
}

fn peak() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    line.split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024
}

/// Writes a part of one span whose `attrs` is `value`, one piece.
fn write(dir: &std::path::Path, id: u64, value: &[u8]) {
    let options = WriterOptions {
        splitter: Some(Arc::new(Whole)),
        dedup_window_bytes: 4 * MIB,
        text_index_budget_bytes: MIB,
        row_list_budget_bytes: MIB,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(dir, id, 0, 900_000_000_000, options).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 10,
        max_bytes: 1 << 30,
    });
    b.append(&SpanRecord {
        trace_id: TraceId([1; 16]),
        span_id: SpanId([1; 8]),
        name: "span",
        start_ns: 1,
        end_ns: 2,
        attrs: value,
        ..SpanRecord::default()
    });
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
}

#[test]
fn one_piece_of_distinct_words_stays_within_the_text_index_budget() {
    const LEN: usize = 3 * MIB;
    let mut repeated = "alpha beta gamma delta ".repeat(LEN / 23 + 1).into_bytes();
    repeated.truncate(LEN);
    let mut distinct = Vec::with_capacity(LEN + 16);
    let mut i = 0u32;
    while distinct.len() < LEN {
        distinct.extend_from_slice(format!("w{i:07} ").as_bytes());
        i += 1;
    }
    distinct.truncate(LEN);

    let tmp = tempfile::tempdir().unwrap();
    // The same size of text, with four words: what everything but the index
    // costs, twice so that one-time costs are in the first.
    write(tmp.path(), 1, &repeated);
    write(tmp.path(), 2, &repeated);
    let base = peak();
    write(tmp.path(), 3, &distinct);
    let growth = peak().saturating_sub(base);
    assert!(
        growth <= 4 * MIB,
        "a part with one piece of {i} distinct words raised the peak by {:.1} MiB over the same \
         size of repeated words, with a text index budget of 1 MiB",
        growth as f64 / MIB as f64
    );
}
