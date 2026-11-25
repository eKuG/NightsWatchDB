//! A trace read back must not hold the origin values of the other traces
//! stored beside it, whichever way `get_trace` produces its result.

mod common;

use arrow_array::RecordBatch;
use common::*;
use nwdb_core::TraceId;

/// Size of the service name of each of the other traces.
const BIG: usize = 60 * 1024;
const OTHERS: u128 = 15;
/// What a small trace may cost beyond its own rows.
const SLACK: usize = 64 * 1024;

fn big_traces() -> Vec<TSpan> {
    (2..2 + OTHERS)
        .map(|t| {
            let mut s = TSpan::new(t, 1, 100 + t as i64).root();
            s.service = Some(format!("{t:04}").repeat(BIG / 4));
            s
        })
        .collect()
}

fn small(span: u64, start: i64) -> TSpan {
    let mut s = TSpan::new(1, span, start);
    s.service = Some("small".into());
    s
}

fn held(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}

#[test]
fn a_small_trace_beside_large_origins_is_read_compactly() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut first = vec![small(1, 100)];
    first.extend(big_traces());
    ingest(&e, &first).unwrap();
    e.flush().unwrap();

    // A trace of one part, already in order: returned as read.
    let one = e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().unwrap();
    assert_eq!(from_batches(&one).len(), 1);
    assert!(held(&one) < SLACK, "{} bytes for one row", held(&one));

    // The same trace in a second part and in the buffer: merged from several
    // sources into new batches.
    ingest(&e, &[small(2, 90)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[small(3, 300)]).unwrap();
    let merged = e.get_trace(TraceId(1u128.to_be_bytes())).unwrap().unwrap();
    let spans = from_batches(&merged);
    assert_eq!(spans.iter().map(|s| s.span).collect::<Vec<_>>(), [2, 1, 3]);
    assert!(held(&merged) < SLACK, "{} bytes for 3 rows", held(&merged));

    // The others still read back whole.
    let big = e.get_trace(TraceId(2u128.to_be_bytes())).unwrap().unwrap();
    assert_eq!(from_batches(&big)[0].service.as_ref().unwrap().len(), BIG);
    e.shutdown().unwrap();
}
