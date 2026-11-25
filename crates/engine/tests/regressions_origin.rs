//! Origin values (resource and scope) are stored once per batch, so a query
//! that merges batches must carry each row's own origin across.

mod common;

use arrow_array::RecordBatch;
use common::*;
use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanColumns, SpanId, SpanRecord, TraceId};
use nwdb_engine::Engine;

/// One span of trace 7 with the given origin.
fn batch(spans: &[(u64, i64, &str, &[u8])]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for (span, start, service, resource) in spans {
        b.append(&SpanRecord {
            trace_id: TraceId(7u128.to_be_bytes()),
            span_id: SpanId(span.to_be_bytes()),
            parent_span_id: (*span != 1).then_some(SpanId(1u64.to_be_bytes())),
            name: "s",
            start_ns: *start,
            end_ns: *start + 1,
            service_name: Some(service),
            resource: Some(resource),
            scope_name: Some(service),
            ..SpanRecord::default()
        });
    }
    b.finish()
}

fn ingest_batch(e: &Engine, batch: RecordBatch) {
    let (tx, rx) = std::sync::mpsc::channel();
    // The record is only read back on replay, which this test never does.
    e.ingest(TAG, encode(&[]), vec![batch], move |r| {
        let _ = tx.send(r);
    })
    .unwrap();
    rx.recv().unwrap().unwrap();
}

/// Spans of one trace in a part and in two buffer batches, interleaved in
/// time and with the same resources under different keys, are merged into
/// batches in which every span still has its own service, scope and resource.
#[test]
fn a_merged_trace_keeps_the_origin_of_each_span() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    ingest_batch(&e, batch(&[(1, 10, "a", b"res-a"), (4, 40, "b", b"res-b")]));
    e.flush().unwrap();
    ingest_batch(&e, batch(&[(3, 30, "b", b"res-b"), (2, 20, "c", b"res-c")]));
    ingest_batch(&e, batch(&[(6, 60, "a", b"res-a"), (5, 50, "c", b"res-c")]));

    let got = e.get_trace(TraceId(7u128.to_be_bytes())).unwrap().unwrap();
    let mut seen = Vec::new();
    for b in &got {
        let cols = SpanColumns::try_new(b).unwrap();
        for row in 0..cols.len() {
            let r = cols.record(row);
            seen.push((
                u64::from_be_bytes(r.span_id.0),
                r.service_name.unwrap().to_owned(),
                r.scope_name.unwrap().to_owned(),
                r.resource.unwrap().to_vec(),
            ));
        }
    }
    let want: Vec<(u64, String, String, Vec<u8>)> =
        [(1, "a"), (2, "c"), (3, "b"), (4, "b"), (5, "c"), (6, "a")]
            .iter()
            .map(|(span, s)| {
                (
                    *span,
                    s.to_string(),
                    s.to_string(),
                    format!("res-{s}").into_bytes(),
                )
            })
            .collect();
    assert_eq!(seen, want);
    e.shutdown().unwrap();
}
