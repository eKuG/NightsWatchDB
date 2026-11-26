//! Full-text search over the parts of a snapshot.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayAccessor, DictionaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch,
    StringArray, UInt64Array,
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
    /// Lookups of a split trace in another part to see whether a copy of a
    /// matching span there arrived earlier: one per other part that holds
    /// the trace, for each split trace that has a matching span in the time
    /// range.
    pub winner_lookups: u64,
    /// Bytes of piece text read to confirm phrases and long words, all parts
    /// together; the one budget [`SearchLimits::max_confirm_bytes`] limits.
    pub confirm_bytes: u64,
}

/// The columns a hit needs. None of the raw columns, so no piece is read.
const PROJECTION: [&str; 9] = [
    col::TRACE_ID,
    col::SPAN_ID,
    col::START_NS,
    col::END_NS,
    col::NAME,
    col::SERVICE_NAME,
    col::OPERATION,
    col::REQUEST_MODEL,
    col::ARRIVAL,
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
    /// In key order, so the worst is last.
    hits: BTreeMap<Key, SearchHit>,
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

    /// Adds a span. Only the winning copy of a span is ever offered (see
    /// [`Rivals`]), so a span is never held twice.
    fn insert(&mut self, hit: SearchHit) {
        let key: Key = (Reverse(hit.start_ns), hit.trace_id, hit.span_id);
        if !self.admits(&key) {
            return;
        }
        if self.is_full() {
            self.hits.pop_last();
        }
        self.hits.insert(key, hit);
    }
}

/// For each span id, the smallest `(arrival, part)` among its copies.
type Earliest = HashMap<[u8; 8], (u64, usize)>;

/// Decides whether a row of one part is the copy of its span that is shown.
///
/// A span stored in several parts is shown from the row with the smallest
/// `arrival`; among equal values, from the part that is first in the list. A
/// trace that is not in the split map is in one part, which never holds a
/// span twice, so its rows win without a lookup. For a trace that is in the
/// map, the span ids and `arrival` of the trace's rows in every other holder
/// are read once, and kept until a row of another trace is asked about; the
/// rows of one part come out in trace order, so each split trace is looked up
/// once per part searched.
struct Rivals<'a> {
    snapshot: &'a Snapshot,
    /// The part being searched.
    part: usize,
    /// The last split trace looked up: for each span id in its other holders,
    /// the smallest `(arrival, part)`.
    last: Option<(TraceId, Earliest)>,
    lookups: u64,
}

impl Rivals<'_> {
    /// True if another part holds a copy of the span that wins over this one.
    /// The time range and the query play no part: a copy that does not match
    /// still hides the ones that arrived after it.
    fn beaten(&mut self, trace: TraceId, span: SpanId, arrival: u64) -> Result<bool> {
        let Some(holders) = self.snapshot.vis.split.holders(&trace) else {
            return Ok(false);
        };
        if self.last.as_ref().is_none_or(|(t, _)| *t != trace) {
            let mut best = Earliest::new();
            for &h in holders {
                let h = h as usize;
                if h == self.part {
                    continue;
                }
                self.lookups += 1;
                let reader = &self.snapshot.vis.parts[h].reader;
                let batches = reader.read_trace(trace, Some(&[col::SPAN_ID, col::ARRIVAL]))?;
                for batch in batches.into_iter().flatten() {
                    let spans: &FixedSizeBinaryArray = column(&batch, col::SPAN_ID)?;
                    let arrivals: &UInt64Array = column(&batch, col::ARRIVAL)?;
                    for r in 0..batch.num_rows() {
                        let id = SpanId::from_slice(spans.value(r))?.0;
                        let rank = (arrivals.value(r), h);
                        best.entry(id)
                            .and_modify(|b| *b = (*b).min(rank))
                            .or_insert(rank);
                    }
                }
            }
            self.last = Some((trace, best));
        }
        Ok(self
            .last
            .as_ref()
            .and_then(|(_, best)| best.get(&span.0))
            .is_some_and(|&rival| rival < (arrival, self.part)))
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
    /// crossed a flush, until compaction merges the parts). Only the copy with
    /// the smallest `arrival` is a hit, the one [`get_trace`](Self::get_trace)
    /// returns and a merge keeps; among equal values the copy of the part that
    /// is first in the list. This holds whatever the limit, the time range and
    /// the query: a copy that is not the earliest is never returned, even when
    /// the earliest does not match the query or starts outside the range.
    ///
    /// The check happens for each matching row in the time range before it is
    /// held, so it does not disturb the early stop below. It costs one lookup
    /// of the trace in each other part that holds it, reading only the span id
    /// and `arrival` of the trace's rows, and only for a hit whose trace is
    /// in the split map; a hit whose trace is in one part costs nothing.
    /// Spans in the memory buffer are not searched, and they always arrived
    /// later than the ones in parts, so they cannot hide a hit.
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
    /// [`Error::InvalidArgument`] saying which. The limit on the bytes read to
    /// confirm phrases and long words is one budget for the whole search, not
    /// one per part: each part gets what the earlier parts left.
    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>> {
        self.search_with_stats(query).map(|(hits, _)| hits)
    }

    /// [`search`](Self::search) and a count of the work it did.
    pub fn search_with_stats(&self, query: &SearchQuery) -> Result<(Vec<SearchHit>, SearchStats)> {
        self.search_limited(query, SearchLimits::default())
    }

    pub(crate) fn search_limited(
        &self,
        query: &SearchQuery,
        limits: SearchLimits,
    ) -> Result<(Vec<SearchHit>, SearchStats)> {
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

        let mut held = Held {
            limit: query.limit,
            hits: BTreeMap::new(),
        };
        for (n, &(upper, i)) in order.iter().enumerate() {
            // Stop: see the comment on `search`.
            if held.worst_start().is_some_and(|worst| upper < worst) {
                stats.parts_skipped_early = (order.len() - n) as u64;
                break;
            }
            let reader = &parts[i].reader;
            let left = SearchLimits {
                max_confirm_bytes: limits.max_confirm_bytes.saturating_sub(stats.confirm_bytes),
                ..limits
            };
            let (rows, spent) = reader.search_counted(&query.query, &left)?;
            stats.confirm_bytes += spent;
            stats.parts_searched += 1;
            stats.rows_matched += rows.len() as u64;
            let mut rivals = Rivals {
                snapshot: self,
                part: i,
                last: None,
                lookups: 0,
            };
            collect(reader, &rows, query, &mut held, &mut rivals, &mut stats)?;
            stats.winner_lookups += rivals.lookups;
        }
        let hits = held.hits.into_values().collect();
        Ok((hits, stats))
    }
}

/// Reads the rows of `reader` that matched and adds those in the time range
/// to `held`.
fn collect(
    reader: &PartReader,
    rows: &[u32],
    query: &SearchQuery,
    held: &mut Held,
    rivals: &mut Rivals<'_>,
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
                    cols.consider(r, query, held, rivals)?;
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
    arrival: &'a UInt64Array,
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
            arrival: column(batch, col::ARRIVAL)?,
        })
    }

    fn consider(
        &self,
        r: usize,
        query: &SearchQuery,
        held: &mut Held,
        rivals: &mut Rivals<'_>,
    ) -> Result<()> {
        let start = self.start_ns.value(r);
        if start < query.from_ns || start >= query.to_ns {
            return Ok(());
        }
        let trace_id = TraceId::from_slice(self.trace_id.value(r))?;
        let span_id = SpanId::from_slice(self.span_id.value(r))?;
        // A row that could not enter the answer is skipped before anything
        // more is done for it.
        if !held.admits(&(Reverse(start), trace_id, span_id)) {
            return Ok(());
        }
        if rivals.beaten(trace_id, span_id, self.arrival.value(r))? {
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
        held.insert(SearchHit {
            trace_id,
            span_id,
            start_ns: start,
            end_ns: self.end_ns.value(r),
            name: self.name.value(r).to_owned(),
            service_name,
            operation: optional(self.operation, r),
            request_model: optional(self.request_model, r),
        });
        Ok(())
    }
}
