//! The merge's key pass reads the span and trace ids of every row. A row that
//! is not of the trace the input's index lists it under is damage in that
//! input and is reported as such (`Corrupt`), whether or not the caller asked
//! for the input files to be verified first: Parquet pages carry no checksum
//! of their own.

use nwdb_core::{BatchLimits, Error, SpanBatchBuilder, SpanId, SpanRecord, TraceId, part_dir_name};
use nwdb_segment::{MergeOptions, PartReader, PartWriter, SPANS_FILE, WriterOptions, merge_parts};
use std::path::Path;

const WINDOW_NS: i64 = 900_000_000_000;

/// One trace of two spans, ids built from `trace`.
fn write(dir: &Path, id: u64, trace: u8) -> PartReader {
    let mut w = PartWriter::create(dir, id, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for i in 0..2u8 {
        b.append(&SpanRecord {
            trace_id: TraceId([trace; 16]),
            span_id: SpanId([trace, i, 0, 0, 0, 0, 0, 1]),
            name: "s",
            start_ns: i64::from(i),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    PartReader::open(dir.join(part_dir_name(id))).unwrap()
}

#[test]
fn a_row_of_another_trace_than_the_index_lists_is_corrupt_in_the_key_pass() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), 1, 1);
    write(tmp.path(), 2, 2);
    let (a, b) = (
        tmp.path().join(part_dir_name(1)),
        tmp.path().join(part_dir_name(2)),
    );
    let (x, y) = (
        std::fs::read(a.join(SPANS_FILE)).unwrap(),
        std::fs::read(b.join(SPANS_FILE)).unwrap(),
    );
    assert_eq!(x.len(), y.len(), "the setup needs equal lengths");
    assert_ne!(x, y);
    // The rows of the second part under the first part's index.
    std::fs::copy(b.join(SPANS_FILE), a.join(SPANS_FILE)).unwrap();
    let damaged = PartReader::open(&a).unwrap();
    let other = PartReader::open(&b).unwrap();
    let r = merge_parts(
        &[&damaged, &other],
        tmp.path(),
        3,
        0,
        WINDOW_NS,
        MergeOptions {
            verify_inputs: false,
            ..MergeOptions::default()
        },
    );
    match r {
        Err(Error::Corrupt(m)) => assert!(m.contains("not of the trace"), "{m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}
