//! Grouping the rows of buffered batches by trace.
//!
//! One routine serves both the flush, which writes the groups into parts,
//! and the queries, which summarise the buffer. Using the same routine keeps
//! a trace's summary the same before and after it is flushed.

use nwdb_core::{SpanColumns, TraceId};

/// One row of a buffered batch, with the values the ordering needs.
#[derive(Clone, Copy)]
pub(crate) struct Entry {
    pub trace: [u8; 16],
    pub span: [u8; 8],
    pub start_ns: i64,
    /// The log record the row arrived in.
    pub arrival: u64,
    /// Index of the batch in the slice the rows came from.
    pub batch: u32,
    pub row: u32,
}

/// The rows of one trace: `entries[from..to]`, ordered by `(start_ns,
/// span_id)` with duplicate span ids removed.
pub(crate) struct TraceRange {
    pub trace_id: TraceId,
    /// Earliest span start of the trace in these batches.
    pub start_ns: i64,
    pub from: usize,
    pub to: usize,
}

/// All rows of a set of batches, grouped by trace.
pub(crate) struct Grouped {
    pub entries: Vec<Entry>,
    /// Ordered by `(start_ns, trace_id)`.
    pub traces: Vec<TraceRange>,
}

/// Groups the rows of `cols` by trace.
///
/// A span id seen twice in one trace is kept once: the occurrence with the
/// smallest `arrival`, and among equal ones the first in batch order. This is how a sender's retry after an unknown outcome
/// is made harmless within one flush.
pub(crate) fn group_traces(cols: &[SpanColumns<'_>]) -> Grouped {
    let total: usize = cols.iter().map(SpanColumns::len).sum();
    let mut entries = Vec::with_capacity(total);
    for (b, c) in cols.iter().enumerate() {
        for row in 0..c.len() {
            let r = c.record(row);
            entries.push(Entry {
                trace: r.trace_id.0,
                span: r.span_id.0,
                start_ns: r.start_ns,
                arrival: r.arrival,
                batch: b as u32,
                row: row as u32,
            });
        }
    }
    // First sort by identity so duplicates are adjacent and the earliest
    // arrival comes first, then drop all but that one.
    entries.sort_unstable_by_key(|e| (e.trace, e.span, e.arrival, e.batch, e.row));
    entries.dedup_by(|later, kept| later.trace == kept.trace && later.span == kept.span);
    entries.sort_unstable_by_key(|e| (e.trace, e.start_ns, e.span, e.batch, e.row));

    let mut traces = Vec::new();
    let mut from = 0;
    while from < entries.len() {
        let trace = entries[from].trace;
        let mut to = from + 1;
        while to < entries.len() && entries[to].trace == trace {
            to += 1;
        }
        traces.push(TraceRange {
            trace_id: TraceId(trace),
            start_ns: entries[from].start_ns,
            from,
            to,
        });
        from = to;
    }
    traces.sort_unstable_by_key(|t| (t.start_ns, t.trace_id));
    Grouped { entries, traces }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanId, SpanRecord};

    fn batch(span: u8, arrival: u64, start_ns: i64) -> arrow_array::RecordBatch {
        let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
        b.append(&SpanRecord {
            trace_id: TraceId([1; 16]),
            span_id: SpanId([span; 8]),
            start_ns,
            arrival,
            ..SpanRecord::default()
        });
        b.finish()
    }

    /// The copy with the smallest `arrival` is kept even when it comes later
    /// in batch order; batch order decides only among equal arrivals.
    #[test]
    fn the_smallest_arrival_is_kept_whatever_the_batch_order() {
        let batches = [batch(7, 9, 100), batch(7, 3, 200), batch(7, 3, 300)];
        let cols: Vec<_> = batches
            .iter()
            .map(|b| SpanColumns::try_new(b).unwrap())
            .collect();
        let g = group_traces(&cols);
        assert_eq!(g.entries.len(), 1);
        assert_eq!((g.entries[0].arrival, g.entries[0].start_ns), (3, 200));
    }
}
