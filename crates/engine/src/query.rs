//! Reads: a consistent view of parts and buffers, and the three queries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use nwdb_core::{
    BatchLimits, Error, Result, SpanBatchBuilder, SpanColumns, SpanId, SummaryBuilder,
    SummaryColumns, TraceId, TraceSummary, col,
};
use nwdb_segment::{PartReader, ScanOptions};

use crate::engine::Visible;
use crate::rows::group_traces;

/// Parameters of [`Snapshot::list_traces`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListQuery {
    /// Inclusive lower bound of the trace's start (its earliest span start).
    pub from_ns: i64,
    /// Exclusive upper bound of the trace's start.
    pub to_ns: i64,
    /// Keep only traces whose root span's service equals this.
    pub service: Option<String>,
    /// Keep only traces with at least one span whose `request_model` equals
    /// this.
    pub model: Option<String>,
    /// Keep only traces whose error flag equals this.
    pub error: Option<bool>,
    /// Return at most this many traces.
    pub limit: usize,
}

/// Parameters of [`Snapshot::aggregate_tokens`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggQuery {
    /// Inclusive lower bound of a span's `start_ns`.
    pub from_ns: i64,
    /// Exclusive upper bound of a span's `start_ns`.
    pub to_ns: i64,
    /// Keep only spans whose `operation` equals this.
    pub operation: Option<String>,
}

/// One row of [`Snapshot::aggregate_tokens`]: the spans of one model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenAgg {
    /// The `request_model`; `None` for spans without one.
    pub model: Option<String>,
    /// Number of spans.
    pub count: u64,
    /// Sum of `input_tokens`, null counted as 0, saturating.
    pub input_tokens: i64,
    /// Sum of `output_tokens`, null counted as 0, saturating.
    pub output_tokens: i64,
}

/// A consistent read view of the engine: the live parts, the buffer being
/// flushed if a flush was in progress, and the batches of the active buffer,
/// all as of one instant. Nothing that happens later (ingest, flush, even
/// a part being deleted) changes what its queries return.
///
/// A snapshot keeps the buffers it saw in memory until it is dropped, so it
/// should not be held longer than needed.
#[derive(Clone)]
pub struct Snapshot {
    vis: Visible,
    limits: BatchLimits,
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| Error::corrupt(format!("column `{name}` is missing or has the wrong type")))
}

fn matching_rows(batch: &RecordBatch, id: TraceId) -> Result<Vec<u32>> {
    let ids: &FixedSizeBinaryArray = column(batch, col::TRACE_ID)?;
    Ok((0..ids.len())
        .filter(|&r| ids.value(r) == id.0)
        .map(|r| r as u32)
        .collect())
}

/// Where a partial summary came from; the order makes merging deterministic.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Source {
    Part(usize),
    Buffer,
}

impl Snapshot {
    pub(crate) fn new(vis: Visible, limits: BatchLimits) -> Self {
        Self { vis, limits }
    }

    fn buffer(&self) -> impl Iterator<Item = &Arc<RecordBatch>> {
        self.vis
            .frozen
            .iter()
            .flat_map(|f| f.iter())
            .chain(self.vis.active.iter())
    }

    /// Every span of one trace, from the buffers and from every part that
    /// holds it, ordered by `(start_ns, span_id)`; a span id that appears
    /// twice is returned once, the first copy found (parts in id order, then
    /// the buffer being flushed, then the active buffer). `None` if the trace
    /// is unknown.
    ///
    /// Part rows are read through the part reader's byte-bounded batches, one
    /// trace at a time. The result holds the whole trace in memory, and while
    /// it is merged the sources and the result coexist, so a trace costs up to
    /// twice its size; a trace that comes from one part, already in order, is
    /// returned as read, without a copy. Finding the trace in the buffers
    /// reads every buffered batch's trace id column, which costs time
    /// proportional to the number of buffered rows.
    pub fn get_trace(&self, trace_id: TraceId) -> Result<Option<Vec<RecordBatch>>> {
        // Each source is a batch and, for buffer batches, the rows of the
        // trace in it; part batches hold only the trace.
        let mut sources: Vec<(RecordBatch, Option<Vec<u32>>)> = Vec::new();
        for p in self.vis.parts.iter() {
            if let Some(batches) = p.reader.read_trace(trace_id, None)? {
                sources.extend(batches.into_iter().map(|b| (b, None)));
            }
        }
        for b in self.buffer() {
            let rows = matching_rows(b, trace_id)?;
            if !rows.is_empty() {
                sources.push((b.as_ref().clone(), Some(rows)));
            }
        }
        let cols = sources
            .iter()
            .map(|(b, _)| SpanColumns::try_new(b))
            .collect::<Result<Vec<_>>>()?;

        struct Row {
            start_ns: i64,
            span: SpanId,
            source: u32,
            row: u32,
        }
        let mut seen: HashSet<SpanId> = HashSet::new();
        let mut rows: Vec<Row> = Vec::new();
        let mut dropped = false;
        for (i, (_, only)) in sources.iter().enumerate() {
            let all;
            let wanted: &[u32] = match only {
                Some(r) => r,
                None => {
                    all = (0..cols[i].len() as u32).collect::<Vec<_>>();
                    &all
                }
            };
            for &row in wanted {
                let r = cols[i].record(row as usize);
                if seen.insert(r.span_id) {
                    rows.push(Row {
                        start_ns: r.start_ns,
                        span: r.span_id,
                        source: i as u32,
                        row,
                    });
                } else {
                    dropped = true;
                }
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let in_order = rows
            .windows(2)
            .all(|w| (w[0].start_ns, w[0].span) < (w[1].start_ns, w[1].span));
        if in_order && !dropped && sources.iter().all(|(_, only)| only.is_none()) {
            return Ok(Some(sources.into_iter().map(|(b, _)| b).collect()));
        }
        rows.sort_unstable_by_key(|r| (r.start_ns, r.span));

        let mut out = Vec::new();
        let mut builder = SpanBatchBuilder::new(self.limits);
        for r in &rows {
            // `source` numbers the batch for the builder, so each distinct
            // origin of a batch is hashed once per output batch.
            builder.append_from(&cols[r.source as usize], r.source as usize, r.row as usize);
            if builder.is_full() {
                out.push(builder.finish());
            }
        }
        if !builder.is_empty() {
            out.push(builder.finish());
        }
        Ok(Some(out))
    }

    /// Partial summaries of the traces in the buffers, one per trace.
    fn buffer_summaries(&self) -> Result<HashMap<TraceId, TraceSummary>> {
        let batches: Vec<&Arc<RecordBatch>> = self.buffer().collect();
        let cols = batches
            .iter()
            .map(|b| SpanColumns::try_new(b))
            .collect::<Result<Vec<_>>>()?;
        let grouped = group_traces(&cols);
        let mut out = HashMap::with_capacity(grouped.traces.len());
        for t in &grouped.traces {
            let mut b = SummaryBuilder::new();
            for e in &grouped.entries[t.from..t.to] {
                b.push(&cols[e.batch as usize].record(e.row as usize))?;
            }
            if let Some(s) = b.finish_trace() {
                out.insert(t.trace_id, s);
            }
        }
        Ok(out)
    }

    /// The summary row a part holds for one trace, if it holds the trace.
    fn part_summary(reader: &PartReader, id: TraceId) -> Result<Option<TraceSummary>> {
        let Some(entry) = reader.find_trace(id) else {
            return Ok(None);
        };
        // The trace index gives the trace's start in this part, which is the
        // `start_ns` of its summary row, so only the row groups that can hold
        // it are read.
        for batch in reader.scan_summary(Some(entry.start_ns), Some(entry.start_ns))? {
            let cols = SummaryColumns::try_new(&batch)?;
            let ids: &FixedSizeBinaryArray = column(&batch, "trace_id")?;
            if let Some(row) = (0..cols.len()).find(|&r| ids.value(r) == id.0) {
                return Ok(Some(cols.summary(row)));
            }
        }
        Err(Error::corrupt(format!(
            "trace {id} is in a part's index but not in its summary"
        )))
    }

    /// Traces whose start (earliest span start) is in `[from_ns, to_ns)`,
    /// optionally filtered, newest first, ties by trace id ascending, at most
    /// `limit`. The summaries are merged across every part and the buffers
    /// that hold the trace, so a trace spread over several parts by late
    /// spans is one row with the right start, end, counts, models and error
    /// flag.
    ///
    /// # How a late span cannot hide or add a trace
    ///
    /// Each part holds a partial summary of the traces it contains, and the
    /// merged start of a trace is the smallest of its partial starts. So a
    /// trace's merged start is in the range exactly when the partial with the
    /// smallest start is in the range, which means every trace that belongs in
    /// the result has at least one partial summary whose own start is in
    /// range. The candidates are therefore the traces with a partial start in
    /// range, found by the part readers' range-pruned summary scans and in the
    /// buffers. For each candidate the summary rows of every other part that
    /// holds it (found through the trace index) and of the buffers are
    /// fetched and merged, and only then are the range and the filters applied
    /// to the merged summary. A candidate whose merged start falls before the
    /// range because another partial starts earlier is dropped; a trace with
    /// no partial inside the range cannot have a merged start inside it.
    ///
    /// The filters (service, model, error) depend on the merged summary, so
    /// they cannot be applied before merging, and neither can `limit`, since
    /// the merged start decides the order. Cost: the scans read the summary
    /// row groups that overlap the range; each candidate costs one lookup per
    /// part in the trace index (memory) and one summary row group read for
    /// every other part that holds it, which is zero for a trace that was
    /// never split. Summaries of all candidates are held in memory. The
    /// buffers are summarised in full on every call.
    ///
    /// A span delivered twice into different parts, or into a part and the
    /// buffers, is counted twice.
    pub fn list_traces(&self, query: &ListQuery) -> Result<Vec<TraceSummary>> {
        if query.limit == 0 || query.to_ns <= query.from_ns {
            return Ok(Vec::new());
        }
        let in_range = |s: i64| s >= query.from_ns && s < query.to_ns;

        let mut cands: HashMap<TraceId, Vec<(Source, TraceSummary)>> = HashMap::new();
        for (i, p) in self.vis.parts.iter().enumerate() {
            if p.meta.min_start_ns >= query.to_ns {
                continue;
            }
            // The scan's range is inclusive and prunes whole row groups.
            for batch in p
                .reader
                .scan_summary(Some(query.from_ns), Some(query.to_ns - 1))?
            {
                let cols = SummaryColumns::try_new(&batch)?;
                let starts: &Int64Array = column(&batch, "start_ns")?;
                for r in 0..cols.len() {
                    if in_range(starts.value(r)) {
                        let s = cols.summary(r);
                        cands
                            .entry(s.trace_id)
                            .or_default()
                            .push((Source::Part(i), s));
                    }
                }
            }
        }
        let buffer = self.buffer_summaries()?;
        for (id, s) in &buffer {
            if in_range(s.start_ns) {
                cands
                    .entry(*id)
                    .or_default()
                    .push((Source::Buffer, s.clone()));
            }
        }

        let mut out = Vec::new();
        for (id, mut partials) in cands {
            for (i, p) in self.vis.parts.iter().enumerate() {
                if partials.iter().all(|(s, _)| *s != Source::Part(i))
                    && let Some(s) = Self::part_summary(&p.reader, id)?
                {
                    partials.push((Source::Part(i), s));
                }
            }
            if partials.iter().all(|(s, _)| *s != Source::Buffer)
                && let Some(s) = buffer.get(&id)
            {
                partials.push((Source::Buffer, s.clone()));
            }
            partials.sort_by_key(|(s, _)| *s);
            let mut it = partials.into_iter().map(|(_, s)| s);
            let Some(mut merged) = it.next() else {
                continue;
            };
            for s in it {
                merged.merge(&s)?;
            }
            if !in_range(merged.start_ns) {
                continue;
            }
            if let Some(svc) = &query.service
                && merged.root_service.as_ref() != Some(svc)
            {
                continue;
            }
            if let Some(m) = &query.model
                && !merged.models.contains(m)
            {
                continue;
            }
            if query.error.is_some_and(|e| e != merged.error) {
                continue;
            }
            out.push(merged);
        }
        out.sort_by(|a, b| {
            b.start_ns
                .cmp(&a.start_ns)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
        out.truncate(query.limit);
        Ok(out)
    }

    /// Token totals per `request_model` over the spans whose `start_ns` is in
    /// `[from_ns, to_ns)` and, if given, whose `operation` equals the value,
    /// sorted by model with the null group first.
    ///
    /// Parts are read through the span scan, which skips row groups outside
    /// the range using their statistics and returns only the five columns
    /// needed. The buffers (the frozen one and the active one together) go
    /// through the same grouping as `list_traces`, so a span id that appears
    /// twice for one trace in the buffers is counted once, and the three
    /// queries agree inside one snapshot. A span delivered twice into
    /// different parts, or into a part and the buffers, is still counted
    /// twice until compaction exists.
    pub fn aggregate_tokens(&self, query: &AggQuery) -> Result<Vec<TokenAgg>> {
        let mut acc: BTreeMap<Option<String>, TokenAgg> = BTreeMap::new();
        if query.to_ns <= query.from_ns {
            return Ok(Vec::new());
        }
        let mut add = |start: i64,
                       op: Option<&str>,
                       model: Option<&str>,
                       input: Option<i64>,
                       output: Option<i64>| {
            if start < query.from_ns || start >= query.to_ns {
                return;
            }
            if let Some(want) = &query.operation
                && op != Some(want.as_str())
            {
                return;
            }
            let e = acc
                .entry(model.map(str::to_owned))
                .or_insert_with(|| TokenAgg {
                    model: model.map(str::to_owned),
                    count: 0,
                    input_tokens: 0,
                    output_tokens: 0,
                });
            e.count += 1;
            e.input_tokens = e.input_tokens.saturating_add(input.unwrap_or(0));
            e.output_tokens = e.output_tokens.saturating_add(output.unwrap_or(0));
        };
        let projection = [
            col::START_NS,
            col::OPERATION,
            col::REQUEST_MODEL,
            col::INPUT_TOKENS,
            col::OUTPUT_TOKENS,
        ];
        for p in self.vis.parts.iter() {
            if p.meta.min_start_ns >= query.to_ns {
                continue;
            }
            let scan = p.reader.scan(ScanOptions {
                projection: Some(projection.iter().map(|s| (*s).to_owned()).collect()),
                start_ns_min: Some(query.from_ns),
                start_ns_max: Some(query.to_ns - 1),
            })?;
            for batch in scan {
                let batch = batch?;
                let start: &Int64Array = column(&batch, col::START_NS)?;
                let op: &StringArray = column(&batch, col::OPERATION)?;
                let model: &StringArray = column(&batch, col::REQUEST_MODEL)?;
                let input: &Int64Array = column(&batch, col::INPUT_TOKENS)?;
                let output: &Int64Array = column(&batch, col::OUTPUT_TOKENS)?;
                for r in 0..batch.num_rows() {
                    add(
                        start.value(r),
                        op.is_valid(r).then(|| op.value(r)),
                        model.is_valid(r).then(|| model.value(r)),
                        input.is_valid(r).then(|| input.value(r)),
                        output.is_valid(r).then(|| output.value(r)),
                    );
                }
            }
        }
        let batches: Vec<&Arc<RecordBatch>> = self.buffer().collect();
        let cols = batches
            .iter()
            .map(|b| SpanColumns::try_new(b))
            .collect::<Result<Vec<_>>>()?;
        for e in &group_traces(&cols).entries {
            let r = cols[e.batch as usize].record(e.row as usize);
            add(
                r.start_ns,
                r.operation,
                r.request_model,
                r.input_tokens,
                r.output_tokens,
            );
        }
        Ok(acc.into_values().collect())
    }
}
