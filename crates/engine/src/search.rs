//! Full-text search over the parts of a snapshot.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayAccessor, DictionaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch,
    StringArray,
};
use nwdb_core::{Error, Result, SpanId, TraceId, col};
use nwdb_segment::{PartReader, SearchLimits};
use nwdb_textindex::Query;

use crate::query::{Snapshot, column};

/// Parameters of [`Snapshot::search`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchQuery {
    /// What the text must match; see [`nwdb_textindex::parse`].
    pub query: Query,
    /// Inclusive lower bound of the span's `start_ns`.
    pub from_ns: i64,
    /// Exclusive upper bound of the span's `start_ns`.
    pub to_ns: i64,
    /// Return at most this many spans.
    pub limit: usize,
}

/// One matching span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    /// The trace the span belongs to.
    pub trace_id: TraceId,
    /// The span.
    pub span_id: SpanId,
    /// Start of the span, Unix nanoseconds.
    pub start_ns: i64,
    /// End of the span, Unix nanoseconds.
    pub end_ns: i64,
    /// The span's name.
    pub name: String,
    /// The service that emitted it.
    pub service_name: Option<String>,
    /// The `operation` of a generative-AI span.
    pub operation: Option<String>,
    /// The `request_model` of a generative-AI span.
    pub request_model: Option<String>,
}

/// The answer of [`Engine::search`](crate::Engine::search): the hits and how
/// far the log was searched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchResults {
    /// The newest matching spans, newest first.
    pub hits: Vec<SearchHit>,
    /// Every span of the log up to this position that is in a part was
    /// searched; spans after it, in the memory buffer or not yet flushed, were
    /// not. A span that was acknowledged above this position becomes
    /// searchable at the next flush.
    pub searched_through_lsn: u64,
}

/// The work one [`Snapshot::search_with_stats`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Parts of the snapshot.
    pub parts: u64,
    /// Parts ruled out by their time bounds alone.
    pub parts_out_of_range: u64,
    /// Parts whose text was searched.
    pub parts_searched: u64,
    /// Parts left unsearched because they could not change the answer.
    pub parts_skipped_early: u64,
    /// Rows the searched parts matched, before the time range and the limit.
    pub rows_matched: u64,
    /// Rows whose columns were read.
    pub rows_read: u64,
    /// Calls that read a range of rows.
    pub row_ranges_read: u64,
}

/// The columns a hit needs. None of the raw columns, so no piece is read.
const PROJECTION: [&str; 8] = [
    col::TRACE_ID,
    col::SPAN_ID,
    col::START_NS,
    col::END_NS,
    col::NAME,
    col::SERVICE_NAME,
    col::OPERATION,
    col::REQUEST_MODEL,
];

/// Matching rows closer than this are read in one range, the rows between
/// them read and dropped, rather than in separate calls.
const GAP_ROWS: u32 = 64;

/// The longest range read in one call, so a part whose every row matches is
/// read in pieces.
const RANGE_ROWS: u32 = 16_384;

/// A result's sort key: newest first, then trace id and span id ascending.
type Key = (Reverse<i64>, TraceId, SpanId);

/// The best spans found so far.
struct Held {
    limit: usize,
    /// In key order, so the worst is last. The part is the index of the part
    /// the copy was read from.
    hits: BTreeMap<Key, (usize, SearchHit)>,
    by_id: HashMap<(TraceId, SpanId), Key>,
}

impl Held {
    fn is_full(&self) -> bool {
        self.hits.len() >= self.limit
    }

    /// The start of the worst held span once `limit` are held.
    fn worst_start(&self) -> Option<i64> {
        if !self.is_full() {
            return None;
        }
        self.hits.last_key_value().map(|(k, _)| k.0.0)
    }

    /// Whether a span with this key could still enter.
    fn admits(&self, key: &Key) -> bool {
        !self.is_full()
            || self
                .hits
                .last_key_value()
                .is_some_and(|(worst, _)| key < worst)
    }

    /// Adds a span read from part `part`. A span that is already held, seen in
    /// two parts, is kept once, from the older part (a merge keeps the row of
    /// the earliest input, so the answer does not change when the parts are
    /// merged).
    fn insert(&mut self, part: usize, hit: SearchHit) {
        let key: Key = (Reverse(hit.start_ns), hit.trace_id, hit.span_id);
        let id = (hit.trace_id, hit.span_id);
        if let Some(old_key) = self.by_id.get(&id).copied() {
            let old_part = self.hits[&old_key].0;
            if old_part <= part {
                return;
            }
            self.hits.remove(&old_key);
            self.by_id.remove(&id);
        }
        if !self.admits(&key) {
            return;
        }
        if self.is_full()
            && let Some((_, (_, gone))) = self.hits.pop_last()
        {
            self.by_id.remove(&(gone.trace_id, gone.span_id));
        }
        self.by_id.insert(id, key);
        self.hits.insert(key, (part, hit));
    }
}

fn optional(array: &StringArray, row: usize) -> Option<String> {
    array.is_valid(row).then(|| array.value(row).to_owned())
}

impl Snapshot {
    /// The newest `limit` spans whose text matches `query`, with `start_ns` in
    /// `[from_ns, to_ns)`, ordered by `(start_ns` descending, trace id, span
    /// id)`.
    ///
    /// # What is searched
    ///
    /// **Only flushed parts.** Spans still in the memory buffer are not
    /// searched; they become searchable at the next flush, at most the flush
    /// interval (30 seconds by default) after they arrive.
    /// [`searched_through_lsn`](Self::searched_through_lsn) is the log
    /// position the search covered, so a caller can tell whether a span it
    /// just sent could be found. A snapshot taken before a flush stays as it
    /// was: it never sees the part the flush wrote.
    ///
    /// Only text held in pieces is searched: the message text of the raw
    /// columns. Text that stays inline in a span's row is not: values under 8
    /// bytes, attribute values under 256 bytes that are not under a content
    /// key, and text beyond the part's piece ceiling. A span's text is all its
    /// pieces together: a word matches if any piece has it, a phrase must lie
    /// inside one piece, and `AND`, `OR` and `NOT` combine spans.
    ///
    /// # Duplicates
    ///
    /// The same `(trace id, span id)` can be in two parts (a retry that
    /// crossed a flush, until compaction merges the window). It is returned
    /// once, the copy of the older part, as a merge would keep it.
    ///
    /// # How the walk stops early
    ///
    /// Every span of a part has a start no larger than the part's `U`, the
    /// largest maximum in the statistics of its `start_ns` column. The parts
    /// are searched in descending order of `U`. A span enters the answer only
    /// if it sorts before the worst held span, and sorting starts with the
    /// start, descending: so once `limit` spans are held and the next part's
    /// `U` is below the start of the worst held span, no span of that part can
    /// enter, and neither can any later part, whose `U` is no larger. The test
    /// is strict: a span that starts exactly at the worst held start can still
    /// win on trace id, so a part with `U` equal to it is searched. The worst
    /// held span only ever gets better as spans are added, so a part ruled out
    /// stays ruled out. Within a part a row that starts before the worst held
    /// span is dropped as soon as it is read.
    ///
    /// # Cost
    ///
    /// A searched part costs its leaf lookups (see
    /// [`PartReader::search`]) and a read of the matching rows' eight columns,
    /// in ranges of at most 16,384 rows, never the raw columns or the piece
    /// files. Memory is the matching row numbers of one part, one range of
    /// rows and the `limit` best hits.
    ///
    /// A query over a limit of [`SearchLimits`] is
    /// [`Error::InvalidArgument`] saying which.
    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>> {
        self.search_with_stats(query).map(|(hits, _)| hits)
    }

    /// [`search`](Self::search) and a count of the work it did.
    pub fn search_with_stats(&self, query: &SearchQuery) -> Result<(Vec<SearchHit>, SearchStats)> {
        let parts = &self.vis.parts;
        let mut stats = SearchStats {
            parts: parts.len() as u64,
            ..SearchStats::default()
        };
        if query.limit == 0 || query.to_ns <= query.from_ns {
            return Ok((Vec::new(), stats));
        }
        // (the part's `U`, its index), newest bound first, older part first
        // among equal bounds.
        let mut order: Vec<(i64, usize)> = Vec::new();
        for (i, p) in parts.iter().enumerate() {
            if p.meta.min_start_ns >= query.to_ns {
                stats.parts_out_of_range += 1;
                continue;
            }
            let upper = p.reader.max_start_ns()?;
            if upper < query.from_ns {
                stats.parts_out_of_range += 1;
                continue;
            }
            order.push((upper, i));
        }
        order.sort_unstable_by_key(|&(upper, i)| (Reverse(upper), i));

        let limits = SearchLimits::default();
        let mut held = Held {
            limit: query.limit,
            hits: BTreeMap::new(),
            by_id: HashMap::new(),
        };
        for (n, &(upper, i)) in order.iter().enumerate() {
            // Stop: see the comment on `search`.
            if held.worst_start().is_some_and(|worst| upper < worst) {
                stats.parts_skipped_early = (order.len() - n) as u64;
                break;
            }
            let reader = &parts[i].reader;
            let rows = reader.search(&query.query, &limits)?;
            stats.parts_searched += 1;
            stats.rows_matched += rows.len() as u64;
            collect(reader, i, &rows, query, &mut held, &mut stats)?;
        }
        let hits = held.hits.into_values().map(|(_, hit)| hit).collect();
        Ok((hits, stats))
    }
}

/// Reads the rows of `reader` that matched and adds those in the time range
/// to `held`.
fn collect(
    reader: &PartReader,
    part: usize,
    rows: &[u32],
    query: &SearchQuery,
    held: &mut Held,
    stats: &mut SearchStats,
) -> Result<()> {
    let mut i = 0;
    while i < rows.len() {
        let first = rows[i];
        let mut last = first;
        let mut j = i + 1;
        while j < rows.len() && rows[j] - last <= GAP_ROWS && rows[j] - first < RANGE_ROWS {
            last = rows[j];
            j += 1;
        }
        let wanted = &rows[i..j];
        stats.row_ranges_read += 1;
        let batches = reader.read_rows(
            u64::from(first),
            u64::from(last - first) + 1,
            Some(&PROJECTION),
        )?;
        let mut row = u64::from(first);
        let mut next = 0;
        for batch in &batches {
            stats.rows_read += batch.num_rows() as u64;
            let cols = HitColumns::new(batch)?;
            for r in 0..batch.num_rows() {
                while next < wanted.len() && u64::from(wanted[next]) < row {
                    next += 1;
                }
                if next < wanted.len() && u64::from(wanted[next]) == row {
                    cols.consider(r, part, query, held)?;
                }
                row += 1;
            }
        }
        i = j;
    }
    Ok(())
}

/// The projected columns of one batch.
struct HitColumns<'a> {
    trace_id: &'a FixedSizeBinaryArray,
    span_id: &'a FixedSizeBinaryArray,
    start_ns: &'a Int64Array,
    end_ns: &'a Int64Array,
    name: &'a StringArray,
    service_name: &'a DictionaryArray<Int32Type>,
    operation: &'a StringArray,
    request_model: &'a StringArray,
}

impl<'a> HitColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            trace_id: column(batch, col::TRACE_ID)?,
            span_id: column(batch, col::SPAN_ID)?,
            start_ns: column(batch, col::START_NS)?,
            end_ns: column(batch, col::END_NS)?,
            name: column(batch, col::NAME)?,
            service_name: column(batch, col::SERVICE_NAME)?,
            operation: column(batch, col::OPERATION)?,
            request_model: column(batch, col::REQUEST_MODEL)?,
        })
    }

    fn consider(&self, r: usize, part: usize, query: &SearchQuery, held: &mut Held) -> Result<()> {
        let start = self.start_ns.value(r);
        if start < query.from_ns || start >= query.to_ns {
            return Ok(());
        }
        let trace_id = TraceId::from_slice(self.trace_id.value(r))?;
        let span_id = SpanId::from_slice(self.span_id.value(r))?;
        // Skipped before the strings are copied; `insert` decides again,
        // because it also has to look at duplicates.
        if !held.by_id.contains_key(&(trace_id, span_id))
            && !held.admits(&(Reverse(start), trace_id, span_id))
        {
            return Ok(());
        }
        let service_name = match self.service_name.downcast_dict::<StringArray>() {
            Some(d) if d.is_valid(r) => Some(d.value(r).to_owned()),
            Some(_) => None,
            None => {
                return Err(Error::corrupt(
                    "column `service_name` does not hold strings",
                ));
            }
        };
        held.insert(
            part,
            SearchHit {
                trace_id,
                span_id,
                start_ns: start,
                end_ns: self.end_ns.value(r),
                name: self.name.value(r).to_owned(),
                service_name,
                operation: optional(self.operation, r),
                request_model: optional(self.request_model, r),
            },
        );
        Ok(())
    }
}
