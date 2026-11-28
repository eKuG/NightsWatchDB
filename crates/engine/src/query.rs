//! Reads: a consistent view of parts and buffers, and the three queries.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use nwdb_core::{
    BatchLimits, Error, Result, SpanBatchBuilder, SpanColumns, SpanId, SummaryBuilder,
    SummaryColumns, TraceId, TraceSummary, col,
};
use nwdb_segment::{Annotation, Latest, PartReader, ScanOptions, SummaryScanRev};

use crate::engine::{LiveAnnotationPart, Visible, WindowKey};
use crate::rows::group_traces;
use crate::split::gallop;

/// Parameters of [`Snapshot::list_traces`].
#[derive(Clone, Debug, PartialEq)]
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
    /// Keep only traces with a matching annotation (see [`AnnotationFilter`]).
    pub annotation: Option<AnnotationFilter>,
    /// Return at most this many traces.
    pub limit: usize,
}

/// A test on the winning, not deleted annotations of a trace and of its
/// spans. A trace matches if some such annotation has this `name`, and the
/// `source` if one is given, and a value of at least `min` if given, and a
/// value of at most `max` if given (an annotation without a value matches
/// neither bound), and the `label` if given.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationFilter {
    /// The annotation's name; required.
    pub name: String,
    /// Its source, if given.
    pub source: Option<String>,
    /// Its value must be at least this, if given.
    pub min: Option<f64>,
    /// Its value must be at most this, if given.
    pub max: Option<f64>,
    /// Its label, if given.
    pub label: Option<String>,
}

impl AnnotationFilter {
    /// Whether one annotation passes; the caller has applied winners and
    /// deletes.
    pub(crate) fn matches(&self, a: &Annotation) -> bool {
        a.name == self.name
            && self.source.as_ref().is_none_or(|s| *s == a.source)
            && self.min.is_none_or(|m| a.value.is_some_and(|v| v >= m))
            && self.max.is_none_or(|m| a.value.is_some_and(|v| v <= m))
            && self
                .label
                .as_ref()
                .is_none_or(|l| a.label.as_ref() == Some(l))
    }
}

/// The most trace ids one window's set of matching traces may hold in a
/// list query with an annotation filter.
const ANNOTATION_SET_CAP: usize = 1_048_576;

/// What one [`Snapshot::annotations_with_stats`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AnnotationStats {
    /// Annotation parts whose records of the trace were looked up.
    pub parts_probed: u64,
    /// Blocks those parts read (a block is read from disk and decoded once
    /// per lookup; there is no cache).
    pub blocks_read: u64,
    /// Span part indexes searched to find which windows hold the trace (zero
    /// when the trace is in the split map, or when no annotation part exists
    /// in any window).
    pub span_index_probes: u64,
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
    /// Sum of `input_tokens`, null or negative counted as 0, saturating.
    pub input_tokens: i64,
    /// Sum of `output_tokens`, null or negative counted as 0, saturating.
    pub output_tokens: i64,
}

/// The work one [`Snapshot::list_traces_with_stats`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ListStats {
    /// Summary rows taken from the parts and the buffers, including rows
    /// skipped for starting at or after the range's upper bound.
    pub summary_rows_read: u64,
    /// Summary row groups read from parts, for the scan and for fetching a
    /// split trace's other partials.
    pub row_groups_read: u64,
    /// Lookups in the snapshot's split map: one per trace first seen.
    pub split_lookups: u64,
    /// Lookups in a part's trace index: one per partial fetched from another
    /// part, and, for a trace that came from the buffer and is not split, one
    /// per part searched until the trace is found.
    pub index_lookups: u64,
    /// Windows whose annotation parts were scanned once to build the set of
    /// traces that match the annotation filter.
    pub annotation_windows_scanned: u64,
    /// Candidates whose annotations were looked up one trace at a time
    /// (`Snapshot::annotations`): split traces, traces with buffered
    /// annotations and traces that are only in the buffer.
    pub annotation_exact_lookups: u64,
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
    pub(crate) vis: Visible,
    limits: BatchLimits,
    /// The log position the parts of `vis` cover.
    flushed_lsn: u64,
    /// See [`ANNOTATION_SET_CAP`]; tests lower it.
    set_cap: usize,
}

pub(crate) fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
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

/// A held result, ordered so that the worse one is the greater: older first,
/// then the larger trace id.
struct Ranked(TraceSummary);

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .start_ns
            .cmp(&self.0.start_ns)
            .then_with(|| self.0.trace_id.cmp(&other.0.trace_id))
    }
}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Ranked {}

fn start_of(batch: &RecordBatch, row: usize) -> Result<i64> {
    Ok(column::<Int64Array>(batch, "start_ns")?.value(row))
}

/// The summary rows of one part, newest first, one row group in memory at a
/// time.
struct Cursor {
    scan: SummaryScanRev,
    /// The group being read, in file order (oldest first).
    batches: Vec<RecordBatch>,
    /// The next row to give, as (batch, row); `None` before a group is read
    /// and when the group is used up.
    next: Option<(usize, usize)>,
    /// Exclusive upper bound of the range; newer rows are skipped.
    to: i64,
}

impl Cursor {
    /// The largest start this cursor can still give: the next row's start, or
    /// the bound of the next group when none is loaded; `None` at the end.
    fn key(&self) -> Result<Option<i64>> {
        match self.next {
            Some((b, r)) => start_of(&self.batches[b], r).map(Some),
            None => Ok(self.scan.next_upper_bound()),
        }
    }

    /// The last row of the batches before `before`.
    fn last_row(&self, before: usize) -> Option<(usize, usize)> {
        (0..before)
            .rev()
            .find(|&b| self.batches[b].num_rows() > 0)
            .map(|b| (b, self.batches[b].num_rows() - 1))
    }

    fn load(&mut self, stats: &mut ListStats) -> Result<()> {
        if let Some(group) = self.scan.next_group()? {
            stats.row_groups_read += 1;
            self.batches = group;
            self.next = self.last_row(self.batches.len());
            self.skip_newer(stats)?;
        }
        Ok(())
    }

    /// Steps back one row; frees the group when it is used up.
    fn retreat(&mut self) {
        self.next = match self.next {
            Some((b, r)) if r > 0 => Some((b, r - 1)),
            Some((b, _)) => self.last_row(b),
            None => None,
        };
        if self.next.is_none() {
            self.batches = Vec::new();
        }
    }

    /// Skips rows that start at or after the range's end.
    fn skip_newer(&mut self, stats: &mut ListStats) -> Result<()> {
        while let Some((b, r)) = self.next {
            if start_of(&self.batches[b], r)? < self.to {
                break;
            }
            stats.summary_rows_read += 1;
            self.retreat();
        }
        Ok(())
    }

    /// Takes the next row; one must be loaded.
    fn take(&mut self, stats: &mut ListStats) -> Result<TraceSummary> {
        let (b, r) = self.next.expect("a row is loaded");
        let summary = SummaryColumns::try_new(&self.batches[b])?.summary(r);
        stats.summary_rows_read += 1;
        self.retreat();
        self.skip_newer(stats)?;
        Ok(summary)
    }
}

/// The records of `trace` in a [`Snapshot::buffer_index`], in identity order,
/// several per identity possible.
/// Borrowed: a record is copied only if it wins (see `Latest`).
fn buffered_of<'a>(index: &[(TraceId, &'a Annotation)], trace: TraceId) -> Vec<&'a Annotation> {
    let lo = index.partition_point(|(t, _)| *t < trace);
    let hi = index.partition_point(|(t, _)| *t <= trace);
    let mut v: Vec<&Annotation> = index[lo..hi].iter().map(|(_, a)| *a).collect();
    v.sort_by(|a, b| a.identity_cmp(b));
    v
}

impl Snapshot {
    pub(crate) fn new(vis: Visible, limits: BatchLimits, flushed_lsn: u64) -> Self {
        Self {
            vis,
            limits,
            flushed_lsn,
            set_cap: ANNOTATION_SET_CAP,
        }
    }

    /// For tests: the most trace ids one window's annotation filter set may
    /// hold.
    #[doc(hidden)]
    pub fn with_annotation_set_cap(mut self, cap: usize) -> Self {
        self.set_cap = cap;
        self
    }

    /// The highest log position whose data is in the parts of this snapshot:
    /// what [`search`](Self::search) covers.
    pub fn searched_through_lsn(&self) -> u64 {
        self.flushed_lsn
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
    /// twice is returned once, the copy with the smallest `arrival` (the
    /// earliest arrival), and among equal ones the first found (parts in list
    /// order, then the buffer being flushed, then the active buffer). The
    /// answer is the same before and after any merge. `None` if the trace is
    /// unknown.
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
        // The split map names the parts of a trace that is in several. A
        // trace that is not in the map is in at most one part, so the search
        // ends at the first index that has it.
        match self.vis.split.holders(&trace_id) {
            Some(holders) => {
                for &h in holders {
                    if let Some(batches) = self.vis.parts[h as usize]
                        .reader
                        .read_trace(trace_id, None)?
                    {
                        sources.extend(batches.into_iter().map(|b| (b, None)));
                    }
                }
            }
            None => {
                for p in self.vis.parts.iter() {
                    if let Some(batches) = p.reader.read_trace(trace_id, None)? {
                        sources.extend(batches.into_iter().map(|b| (b, None)));
                        break;
                    }
                }
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
            arrival: u64,
            source: u32,
            row: u32,
        }
        // Where each span id's row is in `rows`.
        let mut at: HashMap<SpanId, usize> = HashMap::new();
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
                let this = Row {
                    start_ns: r.start_ns,
                    span: r.span_id,
                    arrival: r.arrival,
                    source: i as u32,
                    row,
                };
                match at.get(&r.span_id) {
                    None => {
                        at.insert(r.span_id, rows.len());
                        rows.push(this);
                    }
                    Some(&k) => {
                        dropped = true;
                        if this.arrival < rows[k].arrival {
                            rows[k] = this;
                        }
                    }
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

    /// For each of `ids`, in the order given, whether the trace has rows in
    /// a live part or in the buffers (the active one and the one being
    /// flushed). Duplicate ids are fine.
    ///
    /// Cost: the ids are sorted once (`n log n`); then each live part's
    /// in-memory trace index is walked forward once with the galloping search
    /// the split map uses, for the ids still unknown: at most `n` steps of
    /// the logarithm of the index over `n` per part, and no I/O. Only if some
    /// ids are still unknown after the parts, one pass over the buffered
    /// batches' trace id columns, which costs time proportional to the
    /// buffered rows, whatever `n`.
    pub fn known_traces(&self, ids: &[TraceId]) -> Result<Vec<bool>> {
        let mut known = vec![false; ids.len()];
        if ids.is_empty() {
            return Ok(known);
        }
        let mut order: Vec<usize> = (0..ids.len()).collect();
        order.sort_unstable_by_key(|&i| ids[i]);
        for p in self.vis.parts.iter() {
            let entries = p.reader.trace_entries();
            let mut at = 0;
            for &i in &order {
                if known[i] {
                    continue;
                }
                at = gallop(entries, at, ids[i]);
                if at == entries.len() {
                    break;
                }
                known[i] = entries[at].trace_id == ids[i];
            }
        }
        let mut unknown: HashSet<TraceId> = order
            .iter()
            .filter(|&&i| !known[i])
            .map(|&i| ids[i])
            .collect();
        let mut found: HashSet<TraceId> = HashSet::new();
        for b in self.buffer() {
            if unknown.is_empty() {
                break;
            }
            let col: &FixedSizeBinaryArray = column(b, col::TRACE_ID)?;
            for r in 0..col.len() {
                let id = TraceId(
                    col.value(r)
                        .try_into()
                        .map_err(|_| Error::corrupt("a trace id column is not 16 bytes wide"))?,
                );
                if unknown.remove(&id) {
                    found.insert(id);
                }
            }
        }
        for (k, id) in known.iter_mut().zip(ids) {
            *k = *k || found.contains(id);
        }
        Ok(known)
    }

    /// The records of the buffers for one trace, in identity order, several
    /// per identity possible. Time proportional to the buffered annotations.
    fn buffer_annotations_of(&self, trace: TraceId) -> Vec<&Annotation> {
        let mut v: Vec<&Annotation> = self
            .buffered_annotations()
            .filter(|a| a.trace_id == trace)
            .collect();
        v.sort_by(|a, b| a.identity_cmp(b));
        v
    }

    fn buffered_annotations(&self) -> impl Iterator<Item = &Annotation> {
        self.vis
            .ann_frozen
            .iter()
            .flat_map(|f| f.iter())
            .chain(self.vis.ann_active.iter())
            .flat_map(|b| b.iter())
    }

    /// One pass over the buffered annotations: references to all of them
    /// sorted by trace id (24 bytes each, nothing copied), for
    /// [`buffered_of`] to look traces up in.
    fn buffer_index(&self) -> Vec<(TraceId, &Annotation)> {
        let mut v: Vec<(TraceId, &Annotation)> = self
            .buffered_annotations()
            .map(|a| (a.trace_id, a))
            .collect();
        v.sort_by_key(|(t, _)| *t);
        v
    }

    /// The annotation parts that can hold annotations of `trace`: those of the
    /// windows of the span parts that hold it, found as `get_trace` finds the
    /// holders (the split map, or the first part whose index has it).
    ///
    /// Only span parts whose window has an annotation part are searched, so
    /// with no annotation part nothing is probed. Also returns how many span
    /// part indexes were searched.
    fn annotation_parts_of(&self, trace: TraceId) -> (Vec<&Arc<LiveAnnotationPart>>, u64) {
        if self.vis.ann_windows.is_empty() {
            return (Vec::new(), 0);
        }
        let mut probes = 0;
        let holders: Vec<usize> = match self.vis.split.holders(&trace) {
            Some(h) => h.iter().map(|&x| x as usize).collect(),
            // A trace in one part: the first part that has it is the only one.
            None => self
                .vis
                .parts
                .iter()
                .position(|p| {
                    let m = &p.meta;
                    self.vis
                        .ann_windows
                        .contains_key(&(m.window_start_ns, m.window_ns))
                        && {
                            probes += 1;
                            p.reader.find_trace(trace).is_some()
                        }
                })
                .into_iter()
                .collect(),
        };
        let windows: BTreeSet<WindowKey> = holders
            .iter()
            .map(|&h| {
                let m = &self.vis.parts[h].meta;
                (m.window_start_ns, m.window_ns)
            })
            .collect();
        let parts = windows
            .iter()
            .filter_map(|w| self.vis.ann_windows.get(w))
            .flatten()
            .map(|&i| &self.vis.ann_parts[i as usize])
            .collect();
        (parts, probes)
    }

    /// The winning annotations of a trace, on the trace and on its spans,
    /// with deletes applied (a deleted identity is absent), in identity order
    /// (span id with none first, name, source), at most `limit`; the flag says
    /// more exist. Of the records with one identity the one with the largest
    /// `arrival` wins, whether it is in a part or in the buffers.
    ///
    /// Cost: the trace's rows are looked up in the span parts' in-memory
    /// indexes, then the trace's records are read from the annotation parts of
    /// the windows that hold it (a lookup reads only the blocks that can hold
    /// the trace, typically one block per part), and one pass is made over the
    /// annotations in the buffers.
    pub fn annotations(&self, trace: TraceId, limit: usize) -> Result<(Vec<Annotation>, bool)> {
        self.annotations_with_stats(trace, limit)
            .map(|(a, more, _)| (a, more))
    }

    /// [`annotations`](Self::annotations) and a count of the work it did.
    pub fn annotations_with_stats(
        &self,
        trace: TraceId,
        limit: usize,
    ) -> Result<(Vec<Annotation>, bool, AnnotationStats)> {
        self.annotations_of(trace, limit, self.buffer_annotations_of(trace))
    }

    /// The winning annotations of each trace, as [`annotations`](Self::annotations)
    /// gives them, in the order of `traces`; one pass over the buffer for all
    /// of them.
    pub fn annotations_many(
        &self,
        traces: &[TraceId],
        per_trace_limit: usize,
    ) -> Result<Vec<(Vec<Annotation>, bool)>> {
        let index = self.buffer_index();
        traces
            .iter()
            .map(|&t| {
                self.annotations_of(t, per_trace_limit, buffered_of(&index, t))
                    .map(|(a, more, _)| (a, more))
            })
            .collect()
    }

    /// The winners of `trace` over its annotation parts and `buffered` (its
    /// buffered records). Stops reading once it holds `limit` winners and has
    /// seen one more.
    fn annotations_of(
        &self,
        trace: TraceId,
        limit: usize,
        buffered: Vec<&Annotation>,
    ) -> Result<(Vec<Annotation>, bool, AnnotationStats)> {
        let (parts, span_index_probes) = self.annotation_parts_of(trace);
        let blocks_before: u64 = parts.iter().map(|p| p.reader.blocks_read()).sum();
        let cursors = parts.iter().map(|p| p.reader.cursor(Some(trace))).collect();
        let mut latest = Latest::new(cursors, buffered);
        let mut out: Vec<Annotation> = Vec::new();
        let mut more = false;
        while let Some(a) = latest.next_ref()? {
            if a.delete {
                continue;
            }
            if out.len() == limit {
                more = true;
                break;
            }
            out.push(a.into_owned());
        }
        drop(latest);
        let blocks_read = parts
            .iter()
            .map(|p| p.reader.blocks_read())
            .sum::<u64>()
            .saturating_sub(blocks_before);
        let stats = AnnotationStats {
            parts_probed: parts.len() as u64,
            blocks_read,
            span_index_probes,
        };
        Ok((out, more, stats))
    }

    /// Whether some winning, not deleted annotation of `trace` passes `f`:
    /// the winners are streamed and the first match ends the read, so one
    /// record is held at a time besides `buffered`.
    fn trace_matches(
        &self,
        trace: TraceId,
        f: &AnnotationFilter,
        buffered: Vec<&Annotation>,
    ) -> Result<bool> {
        let cursors = self
            .annotation_parts_of(trace)
            .0
            .iter()
            .map(|p| p.reader.cursor(Some(trace)))
            .collect();
        let mut latest = Latest::new(cursors, buffered);
        while let Some(a) = latest.next_ref()? {
            if !a.delete && f.matches(&a) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The ids of the traces with a winning, not deleted annotation that
    /// `f` accepts, among the annotation parts of `window`: one scan of all of
    /// them. The caller guarantees these are the only parts the traces it
    /// asks about can have annotations in. `None` once more than the cap
    /// match: the set is dropped, and the caller answers by trace.
    fn annotation_matches_of_window(
        &self,
        window: WindowKey,
        f: &AnnotationFilter,
        stats: &mut ListStats,
    ) -> Result<Option<HashSet<TraceId>>> {
        let mut set = HashSet::new();
        let Some(idx) = self.vis.ann_windows.get(&window) else {
            return Ok(Some(set));
        };
        stats.annotation_windows_scanned += 1;
        let cursors = idx
            .iter()
            .map(|&i| self.vis.ann_parts[i as usize].reader.cursor(None))
            .collect();
        let mut latest = Latest::new(cursors, Vec::new());
        while let Some(a) = latest.next()? {
            if !a.delete && f.matches(&a) && set.insert(a.trace_id) && set.len() > self.set_cap {
                return Ok(None);
            }
        }
        Ok(Some(set))
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
    fn part_summary(
        reader: &PartReader,
        id: TraceId,
        stats: &mut ListStats,
    ) -> Result<Option<TraceSummary>> {
        stats.index_lookups += 1;
        let Some(entry) = reader.find_trace(id) else {
            return Ok(None);
        };
        // The trace index gives the trace's start in this part, which is the
        // `start_ns` of its summary row, so only the row groups that can hold
        // it are read.
        let mut scan = reader.scan_summary_rev(Some(entry.start_ns), Some(entry.start_ns))?;
        while let Some(group) = scan.next_group()? {
            stats.row_groups_read += 1;
            for batch in group {
                let cols = SummaryColumns::try_new(&batch)?;
                let ids: &FixedSizeBinaryArray = column(&batch, "trace_id")?;
                if let Some(row) = (0..cols.len()).find(|&r| ids.value(r) == id.0) {
                    return Ok(Some(cols.summary(row)));
                }
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
    /// range. Those partials are the candidates. A partial starting at or
    /// after `to_ns` is not a candidate (its trace is found through its
    /// in-range partial, if it has one), and one starting before `from_ns`
    /// is the end of the scan, since the merged start can only be older.
    ///
    /// # How the scan goes newest first and stops early
    ///
    /// The partials of the parts (each summary file is sorted by start, read
    /// from its newest row group backwards) and of the buffers are taken in
    /// descending order of start, merging the parts' streams with a heap.
    /// The first time a trace is seen, its other partials are fetched (the
    /// parts that hold it come from the snapshot's split map, and the buffer
    /// from a hash map), the partials are merged, the range and the filters
    /// are applied to the merged summary, and the best `limit` merged
    /// summaries are kept. The trace is then marked as seen, so its other
    /// partials are skipped when they come up.
    ///
    /// The scan stops when `limit` results are held and the next partial to
    /// come, which has the largest start of all that remain, starts below
    /// `T`, the start of the worst held result. Every trace not yet seen has
    /// all its partials among those that remain, so all of them start at or
    /// below that next start, and its merged start, the minimum of them, is
    /// below `T` too. A result sorts by start first, so that trace sorts
    /// after every held result and cannot enter the answer. A trace whose
    /// newest partial was taken earlier was merged completely when it was
    /// seen, including partials older than `T`, so nothing it has is missed.
    /// The test is strict: a remaining partial that starts exactly at `T` can
    /// belong to a trace whose merged start is `T` and whose id is smaller
    /// than the worst result's, so it must still be looked at, and so must
    /// all the partials that tie. A filter that rejects most traces keeps the
    /// results short of `limit`, and the scan then runs on until the range's
    /// lower bound, which is correct. An empty `limit` or range returns at
    /// once.
    ///
    /// Memory: the held results, the ids of the traces seen so far, one
    /// summary row group per part being read, and the buffer's summaries.
    /// Parts whose newest group cannot reach the front are never read: a
    /// part's place in the heap before it is read is the largest start its
    /// row group statistics allow.
    ///
    /// A span stored in two parts, or in a part and the buffers, is counted
    /// twice until the parts are merged.
    pub fn list_traces(&self, query: &ListQuery) -> Result<Vec<TraceSummary>> {
        self.list_traces_with_stats(query).map(|(rows, _)| rows)
    }

    /// [`list_traces`](Self::list_traces) and a count of the work it did.
    pub fn list_traces_with_stats(
        &self,
        query: &ListQuery,
    ) -> Result<(Vec<TraceSummary>, ListStats)> {
        let mut stats = ListStats::default();
        if query.limit == 0 || query.to_ns <= query.from_ns {
            return Ok((Vec::new(), stats));
        }
        let (from, to) = (query.from_ns, query.to_ns);
        let parts = &self.vis.parts;

        // One cursor per part that can hold a candidate, and the buffers as
        // one more source after them.
        let mut cursors: Vec<(usize, Cursor)> = Vec::new();
        for (i, p) in parts.iter().enumerate() {
            if p.meta.min_start_ns >= to {
                continue;
            }
            // The scan's range is inclusive.
            let scan = p.reader.scan_summary_rev(Some(from), Some(to - 1))?;
            cursors.push((
                i,
                Cursor {
                    scan,
                    batches: Vec::new(),
                    next: None,
                    to,
                },
            ));
        }
        let buffer = self.buffer_summaries()?;
        // Ascending, so the newest is popped from the end.
        let mut buffer_order: Vec<(i64, TraceId)> = buffer
            .iter()
            .filter(|(_, s)| s.start_ns < to)
            .map(|(id, s)| (s.start_ns, *id))
            .collect();
        buffer_order.sort_unstable();
        let buffer_source = cursors.len();

        // (largest start the source can still give, source). Each source has
        // exactly one entry.
        let mut heap: BinaryHeap<(i64, usize)> = BinaryHeap::new();
        for (k, (_, c)) in cursors.iter().enumerate() {
            if let Some(key) = c.key()? {
                heap.push((key, k));
            }
        }
        if let Some(&(start, _)) = buffer_order.last() {
            heap.push((start, buffer_source));
        }

        let mut seen: HashSet<TraceId> = HashSet::new();
        // The annotation filter's state: the sets of matching traces of the
        // two windows used last, the windows whose set passed the cap, and
        // the buffered annotations by trace (one pass, on first use).
        let mut ann_sets: Vec<(WindowKey, HashSet<TraceId>)> = Vec::new();
        let mut over_cap: HashSet<WindowKey> = HashSet::new();
        let mut buffered_ann: Option<Vec<(TraceId, &Annotation)>> = None;
        // The worst held result is on top.
        let mut best: BinaryHeap<Ranked> = BinaryHeap::new();
        while let Some((key, k)) = heap.pop() {
            // Stop: see the comment on this function.
            if key < from {
                break;
            }
            if best.len() >= query.limit && best.peek().is_some_and(|worst| key < worst.0.start_ns)
            {
                break;
            }
            let (source, partial) = if k == buffer_source {
                let (_, id) = buffer_order.pop().expect("the heap entry has a trace");
                if let Some(&(start, _)) = buffer_order.last() {
                    heap.push((start, k));
                }
                stats.summary_rows_read += 1;
                (Source::Buffer, buffer[&id].clone())
            } else {
                let (part, c) = &mut cursors[k];
                if c.next.is_none() {
                    // Only now is the group read.
                    c.load(&mut stats)?;
                    if let Some(key) = c.key()? {
                        heap.push((key, k));
                    }
                    continue;
                }
                let s = c.take(&mut stats)?;
                if let Some(key) = c.key()? {
                    heap.push((key, k));
                }
                (Source::Part(*part), s)
            };
            let id = partial.trace_id;
            if !seen.insert(id) {
                continue;
            }

            let mut partials = vec![(source, partial)];
            stats.split_lookups += 1;
            match self.vis.split.holders(&id) {
                Some(holders) => {
                    for &h in holders {
                        let h = h as usize;
                        if source == Source::Part(h) {
                            continue;
                        }
                        let s = Self::part_summary(&parts[h].reader, id, &mut stats)?.ok_or_else(
                            || {
                                Error::corrupt(format!(
                                    "trace {id} is in the split map but not in a part"
                                ))
                            },
                        )?;
                        partials.push((Source::Part(h), s));
                    }
                }
                // Not split: the trace is in at most one part. When it came
                // from a part that is the one; when it came from the buffer
                // the part, if any, is not known, so the indexes are asked
                // until one has it. Only buffered traces pay this, and the
                // buffer is small next to the parts.
                None if source == Source::Buffer => {
                    for p in parts.iter().enumerate() {
                        if let Some(s) = Self::part_summary(&p.1.reader, id, &mut stats)? {
                            partials.push((Source::Part(p.0), s));
                            break;
                        }
                    }
                }
                None => {}
            }
            if source != Source::Buffer
                && let Some(s) = buffer.get(&id)
            {
                partials.push((Source::Buffer, s.clone()));
            }
            let part_holders: Vec<usize> = partials
                .iter()
                .filter_map(|(s, _)| match s {
                    Source::Part(h) => Some(*h),
                    Source::Buffer => None,
                })
                .collect();
            partials.sort_by_key(|(s, _)| *s);
            let mut it = partials.into_iter().map(|(_, s)| s);
            let Some(mut merged) = it.next() else {
                continue;
            };
            for s in it {
                merged.merge(&s)?;
            }
            if merged.start_ns < from || merged.start_ns >= to {
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
            if let Some(f) = &query.annotation {
                // The exact path for a trace whose annotations may be in
                // several windows or in the buffer; every other candidate is
                // in one span part, and its annotations are all in the
                // annotation parts of that part's window (an annotation goes
                // to the window that ends last among its holders). A window
                // whose set of matches passed the cap is answered by the exact
                // path too.
                let index = buffered_ann.get_or_insert_with(|| self.buffer_index());
                let buffered = buffered_of(index, id);
                let by_set = part_holders.len() == 1
                    && self.vis.split.holders(&id).is_none()
                    && buffered.is_empty();
                let mut answer = None;
                if by_set {
                    let m = &parts[part_holders[0]].meta;
                    let w = (m.window_start_ns, m.window_ns);
                    let at = match ann_sets.iter().position(|(k, _)| *k == w) {
                        Some(at) => Some(at),
                        None if over_cap.contains(&w) => None,
                        None => match self.annotation_matches_of_window(w, f, &mut stats)? {
                            Some(set) => {
                                // Only the two most recently used sets stay.
                                if ann_sets.len() == 2 {
                                    ann_sets.remove(0);
                                }
                                ann_sets.push((w, set));
                                Some(ann_sets.len() - 1)
                            }
                            None => {
                                over_cap.insert(w);
                                None
                            }
                        },
                    };
                    if let Some(at) = at {
                        // The most recently used goes last.
                        let used = ann_sets.remove(at);
                        ann_sets.push(used);
                        answer = Some(ann_sets[ann_sets.len() - 1].1.contains(&id));
                    }
                }
                let pass = match answer {
                    Some(p) => p,
                    None => {
                        stats.annotation_exact_lookups += 1;
                        self.trace_matches(id, f, buffered)?
                    }
                };
                if !pass {
                    continue;
                }
            }
            best.push(Ranked(merged));
            if best.len() > query.limit {
                best.pop();
            }
        }
        // Ascending in `Ranked`'s order is best first.
        let out = best.into_sorted_vec().into_iter().map(|r| r.0).collect();
        Ok((out, stats))
    }

    /// For tests: each live part's id and the trace ids of its index.
    #[doc(hidden)]
    pub fn part_traces(&self) -> Vec<(u64, Vec<TraceId>)> {
        self.vis
            .parts
            .iter()
            .map(|p| {
                let ids = p.reader.trace_entries().iter().map(|e| e.trace_id);
                (p.meta.part_id, ids.collect())
            })
            .collect()
    }

    /// For tests: the annotations in the buffers, the frozen ones first, in
    /// the order they were applied.
    #[doc(hidden)]
    pub fn buffered_annotation_list(&self) -> Vec<Annotation> {
        self.buffered_annotations().cloned().collect()
    }

    /// For tests: each live annotation part's id, window start, window width
    /// and number of records.
    #[doc(hidden)]
    pub fn annotation_part_info(&self) -> Vec<(u64, i64, i64, u64)> {
        self.vis
            .ann_parts
            .iter()
            .map(|p| {
                let m = &p.meta;
                (m.part_id, m.window_start_ns, m.window_ns, m.rows)
            })
            .collect()
    }

    /// For tests: the traces the split map holds, with the ids of the parts
    /// it says hold them, sorted by trace id.
    #[doc(hidden)]
    pub fn split_traces(&self) -> Vec<(TraceId, Vec<u64>)> {
        let mut out: Vec<_> = self
            .vis
            .split
            .iter()
            .map(|(id, holders)| {
                let ids = holders
                    .iter()
                    .map(|&h| self.vis.parts[h as usize].meta.part_id);
                (*id, ids.collect())
            })
            .collect();
        out.sort();
        out
    }

    /// Token totals per `request_model` over the spans whose `start_ns` is in
    /// `[from_ns, to_ns)` and, if given, whose `operation` equals the value,
    /// sorted by model with the null group first.
    ///
    /// Parts are read through the span scan, which skips row groups outside
    /// the range using their statistics and returns only the five columns
    /// needed. The buffers (the frozen one and the active one together) go
    /// through the same grouping as `list_traces`, so a span id that appears
    /// twice for one trace in the buffers is counted once. A span stored in
    /// two parts, or in a part and the buffers, is counted twice until the
    /// parts are merged.
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
            // A negative count (written by another ingest path) counts as 0.
            // With only non-negative terms saturating addition is
            // associative, so the sums do not depend on flush timing.
            e.input_tokens = e.input_tokens.saturating_add(input.unwrap_or(0).max(0));
            e.output_tokens = e.output_tokens.saturating_add(output.unwrap_or(0).max(0));
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
