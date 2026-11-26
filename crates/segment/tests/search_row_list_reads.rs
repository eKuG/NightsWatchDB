//! What a search for a common word reads from `text.rows`.
//!
//! The rows of the matching pieces are read one group at a time, in ascending
//! order of piece id, so a word in every piece reads each group once and not
//! once per key in it. Bytes read are the kernel's count of this process's read
//! calls (`rchar`); this file has one test so nothing else reads at the same
//! time.

#![cfg(target_os = "linux")]

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::textindex::parse;
use nwdb_segment::{
    PartReader, PartWriter, SearchLimits, TEXT_INDEX_FILE, TEXT_ROWS_FILE, WriterOptions,
};
use std::ops::Range;
use std::sync::Arc;

/// The whole value is one piece.
struct Whole;

impl PayloadSplitter for Whole {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.push(0..value.len());
    }
}

fn bytes_read() -> u64 {
    let io = std::fs::read_to_string("/proc/self/io").unwrap();
    let line = io.lines().find(|l| l.starts_with("rchar:")).unwrap();
    line.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[test]
fn a_common_word_reads_the_row_lists_about_once() {
    const ROWS: u32 = 30_000;
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        splitter: Some(Arc::new(Whole)),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(tmp.path(), 1, 0, 900_000_000_000, options).unwrap();
    let texts: Vec<String> = (0..ROWS)
        .map(|i| format!("common line number {i}"))
        .collect();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 20,
        max_bytes: 1 << 30,
    });
    for (i, text) in texts.iter().enumerate() {
        let n = i as u64;
        b.append(&SpanRecord {
            trace_id: TraceId(u128::from(n / 4 + 1).to_be_bytes()),
            span_id: SpanId((n + 1).to_be_bytes()),
            name: "span",
            start_ns: n as i64,
            end_ns: n as i64 + 1,
            attrs: text.as_bytes(),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    let dir = tmp.path().join(part_dir_name(1));
    let size = |f: &str| std::fs::metadata(dir.join(f)).unwrap().len();
    let files = size(TEXT_INDEX_FILE) + size(TEXT_ROWS_FILE);

    let r = PartReader::open(&dir).unwrap();
    // The first use of the index reads it whole to check it; done here so
    // that it is not counted below.
    assert!(
        r.search(&parse("absent").unwrap(), &SearchLimits::default())
            .unwrap()
            .is_empty()
    );
    let before = bytes_read();
    let rows = r
        .search(&parse("common").unwrap(), &SearchLimits::default())
        .unwrap();
    let read = bytes_read() - before;
    assert_eq!(rows.len(), ROWS as usize);
    assert!(
        read <= 4 * files + (256 << 10),
        "a search of one word read {read} bytes; text.idx and text.rows hold {files}"
    );
}
