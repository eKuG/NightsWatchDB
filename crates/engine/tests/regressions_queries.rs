//! Regression tests for query results: duplicate spans in the buffer, and a
//! trace summary that must not depend on flush timing.

mod common;

use arrow_array::RecordBatch;
use common::*;
use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanId, SpanRecord, TraceId};
use nwdb_engine::{AggQuery, Engine, ListQuery};

fn list_all() -> ListQuery {
    ListQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        service: None,
        model: None,
        error: None,
        limit: usize::MAX,
    }
}

fn agg_all() -> AggQuery {
    AggQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        operation: None,
    }
}

/// A sender retries a request whose outcome it did not learn. Within one
/// flush duplicates are documented as removed, and `list_traces` and
/// `get_trace` do remove them from the buffer, but `aggregate_tokens` scans
/// the buffer row by row: the same snapshot counts the span once in one
/// query and twice in the other, and the totals drop when the buffer is
/// flushed.
#[test]
fn aggregate_tokens_counts_a_retried_span_once_while_it_is_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(opts(dir.path()));
    let mut s = TSpan::new(1, 1, 100).root();
    s.model = Some("m".into());
    s.input = Some(10);
    let request = vec![s];
    ingest(&e, &request).unwrap();
    ingest(&e, &request).unwrap();

    let snap = e.snapshot();
    let listed: u64 = snap
        .list_traces(&list_all())
        .unwrap()
        .iter()
        .map(|t| u64::from(t.span_count))
        .sum();
    let before = snap.aggregate_tokens(&agg_all()).unwrap();
    let counted: u64 = before.iter().map(|a| a.count).sum();
    e.flush().unwrap();
    let after = e.aggregate_tokens(&agg_all()).unwrap();
    e.shutdown().unwrap();

    assert_eq!(listed, 1);
    assert_eq!(
        counted, listed,
        "one snapshot: list_traces counts {listed} span, aggregate_tokens {counted}"
    );
    assert_eq!(
        before, after,
        "the aggregate changed when the buffer was flushed"
    );
}

fn span_batch(
    trace: u128,
    span: u64,
    start: i64,
    root: bool,
    session: Option<&str>,
) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    let name = format!("span-{span}");
    b.append(&SpanRecord {
        trace_id: TraceId(trace.to_be_bytes()),
        span_id: SpanId(span.to_be_bytes()),
        parent_span_id: (!root).then_some(SpanId(1u64.to_be_bytes())),
        name: &name,
        start_ns: start,
        end_ns: start + 5,
        service_name: Some("svc"),
        session_id: session,
        ..SpanRecord::default()
    });
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

/// The same three spans give a different `session_id` in `list_traces`
/// depending on whether a flush happened between them. Summarised together,
/// the first non-null session in span order wins; merged from a part and the
/// buffer, the partial with the earlier start wins, wherever its session
/// span lies.
#[test]
fn a_trace_summary_does_not_depend_on_when_its_spans_were_flushed() {
    let spans = [
        (1u64, 10i64, true, None),
        (3, 30, false, Some("from-span-at-30")),
        (2, 20, false, Some("from-span-at-20")),
    ];
    let mut sessions = Vec::new();
    for flush_after in [None, Some(2usize)] {
        let dir = tempfile::tempdir().unwrap();
        let e = open(opts(dir.path()));
        for (i, (span, start, root, session)) in spans.iter().enumerate() {
            if flush_after == Some(i) {
                e.flush().unwrap();
            }
            ingest_batch(&e, span_batch(9, *span, *start, *root, *session));
        }
        let listed = e.list_traces(&list_all()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].span_count, 3);
        sessions.push(listed[0].session_id.clone());
        e.shutdown().unwrap();
    }
    assert_eq!(
        sessions[0], sessions[1],
        "same spans, different session_id (all buffered vs. split by a flush)"
    );
}
