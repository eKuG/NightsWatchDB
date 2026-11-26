//! Merging several parts into one.
//!
//! [`merge_parts`] builds the part a flush would have written had it received
//! all the rows of its inputs at once, without rebuilding any text: rows are
//! read in stored form, the references of their ropes are translated from the
//! numbering of the piece store they came from to that of the output, and
//! every piece is copied from an input store once.
//!
//! The work goes in chunks of the output order, bounded by bytes. For a chunk
//! the key columns (`span_id`, `start_ns`) of its traces are read first, the
//! duplicates are dropped and the rows are ordered exactly as a flush orders
//! them; then the rows themselves are read, in runs that are close to
//! sequential because each input is already nearly in output order, their
//! pieces are carried over, and the rows go to the part writer that a flush
//! uses, through the same row groups, dictionaries, summary and index.

use crate::reader::PartReader;
use crate::writer::{PartWriter, WriterOptions};
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use nwdb_core::{
    BatchLimits, Error, PartMeta, PayloadTotals, Result, SpanBatchBuilder, SpanColumns, SpanRecord,
    TraceId, col,
};
use nwdb_payload::rope::{self, Mapped, RopeError};
use std::collections::HashMap;
use std::path::Path;

/// The default for [`MergeOptions::chunk_bytes`]: 32 MiB.
pub const DEFAULT_MERGE_CHUNK_BYTES: usize = 32 << 20;

/// Translation table entry of an input piece not yet carried over.
const UNSET: u32 = u32::MAX;
/// Translation table entry of an input piece that went inline into the rows
/// that use it, because the output could not take one more piece.
const INLINE: u32 = u32::MAX - 1;
/// The most rows whose keys one chunk holds (32 bytes each), unless a single
/// trace has more.
const MAX_CHUNK_ROWS: usize = 1 << 19;
/// Rows between two wanted rows of one input that are read through rather than
/// skipped: a second read costs more than a few unwanted rows.
const READ_GAP_ROWS: u64 = 32;
/// Rows go to the writer in batches of at most this many rows, and at most the
/// chunk size in bytes (between 1 MiB and this many).
const OUTPUT_BATCH_ROWS: usize = 8_192;
const OUTPUT_BATCH_MAX_BYTES: usize = 16 << 20;

/// Settings of [`merge_parts`].
#[derive(Debug, Clone)]
pub struct MergeOptions {
    /// How the output part is written: row groups, piece store, piece ceiling
    /// and observer, exactly as for a flush. Its `splitter` is not used,
    /// because no text is cut again.
    pub writer: WriterOptions,
    /// About how many bytes of stored rows one chunk holds (default 32 MiB).
    /// The size of a row is estimated before it is read, from the average of
    /// its row group, so a chunk can be larger when the rows of a group differ
    /// a lot in size.
    pub chunk_bytes: usize,
    /// The compaction level recorded in the output's [`PartMeta`]. Default 1.
    pub level: u8,
    /// Check every input in full before the merge starts: the length and
    /// CRC-32C of its four data files against its index, and every block of
    /// its piece store (`PartReader::verify` without the walk over the
    /// ropes, which the checksum of the rows makes redundant). Default true.
    /// Without it, only what the merge reads is checked, and a damaged
    /// Parquet page that still decodes is carried over as data.
    pub verify_inputs: bool,
}

impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            writer: WriterOptions::default(),
            chunk_bytes: DEFAULT_MERGE_CHUNK_BYTES,
            level: 1,
            verify_inputs: true,
        }
    }
}

/// What a merge did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Number of input parts.
    pub inputs: usize,
    /// Rows of all inputs.
    pub rows_in: u64,
    /// Rows of the output.
    pub rows_out: u64,
    /// Rows dropped because their `(trace_id, span_id)` was already taken by
    /// an earlier input or an earlier row of the same input.
    pub duplicates_dropped: u64,
    /// Traces of the output.
    pub traces: u64,
    /// Traces that had rows in more than one input.
    pub traces_stitched: u64,
    /// The inputs' piece stores added up: pieces, logical, unique and stored
    /// bytes.
    pub payload_in: PayloadTotals,
    /// The output's piece store.
    pub payload_out: PayloadTotals,
}

/// The key a trace is ordered by: its earliest start, then its id.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OrderKey {
    start: i64,
    trace: [u8; 16],
}

/// The rows of one trace in one input.
struct Part {
    /// Position of the trace in its chunk.
    trace: u32,
    input: u16,
    first: u64,
    count: u32,
}

/// What the merge knows about a row before it has read it.
#[derive(Clone, Copy)]
struct KeyRow {
    trace: u32,
    input: u16,
    start: i64,
    span: [u8; 8],
    row: u64,
}

/// A batch read from an input and the row it starts at.
struct Loaded {
    input: u16,
    first_row: u64,
    batch: RecordBatch,
}

/// An error from reading input `i` with the input named. A library error that
/// is neither an I/O failure nor a misuse of the reader comes from a file that
/// does not decode, so it is damage too.
fn named(inputs: &[&PartReader], i: usize, e: Error) -> Error {
    let name = |m: String| {
        Error::corrupt(format!(
            "merge input {i} ({}): {m}",
            inputs[i].dir().display()
        ))
    };
    match e {
        Error::Corrupt(m) => name(m),
        Error::Other(e) => name(e.to_string()),
        other => other,
    }
}

fn damaged(inputs: &[&PartReader], i: usize, what: impl std::fmt::Display) -> Error {
    named(inputs, i, Error::corrupt(what))
}

fn rope_error(inputs: &[&PartReader], i: usize, e: RopeError) -> Error {
    match e {
        RopeError::Malformed(what) => damaged(
            inputs,
            i,
            format!("spans.parquet: a stored value is malformed ({what})"),
        ),
        RopeError::Checksum => damaged(inputs, i, "spans.parquet: a stored value is damaged"),
        RopeError::Length => damaged(
            inputs,
            i,
            "spans.parquet: a stored value does not add up to the length it carries",
        ),
        RopeError::Piece(e) => named(inputs, i, e),
    }
}

/// Merges `inputs`, oldest first, into one new part, `<parts_dir>/<part_id>`.
///
/// The output is the part a flush would write if it received every row of
/// every input at once:
///
/// - The spans of a trace are adjacent and ordered by `(start_ns, span_id)`;
///   traces are ordered by `(the earliest start of the trace in any input,
///   trace_id)`. A trace with rows in several inputs is one run.
/// - A `(trace_id, span_id)` that occurs more than once is kept once: the row
///   of the earliest input, and within one input its first row. The others are
///   counted in [`MergeStats::duplicates_dropped`].
/// - The trace index and the summary are those of any part: the summary of a
///   trace is computed from its merged rows.
/// - Every value of every kept row reads back byte for byte as the input
///   returns it. Text is not rebuilt: a rope keeps its checksum and has its
///   piece references translated, each piece is copied from an input's store at
///   most once, and text that several inputs hold is stored once. If the output
///   cannot take another piece (`max_pieces`), the piece's bytes go into the
///   rope; the value is unchanged.
///
/// `window_start_ns` and `window_ns` are recorded in the returned
/// [`PartMeta`] as for any part; the merge does not check that the rows lie
/// inside the window.
///
/// Memory, besides the chunk (see [`MergeOptions::chunk_bytes`]): 24 bytes per
/// distinct trace for the output order, 4 bytes per piece of every input for
/// the translation table, 32 bytes per row of the chunk for its keys, and what
/// the part writer and its piece writer hold. It does not grow with the size
/// of the inputs.
///
/// The inputs are never modified. Any error removes the temporary directory
/// and leaves nothing behind (after [`Error::Fatal`] the part may exist, as
/// for [`PartWriter::finish`]). Damage found in an input is
/// [`Error::Corrupt`] naming the input by its position and directory.
///
/// Fails with [`Error::InvalidArgument`] if `inputs` is empty or has more than
/// 65,535 parts, or `options.chunk_bytes` is 0, or the writer options or the
/// window are invalid.
pub fn merge_parts(
    inputs: &[&PartReader],
    parts_dir: &Path,
    part_id: u64,
    window_start_ns: i64,
    window_ns: i64,
    options: MergeOptions,
) -> Result<(PartMeta, MergeStats)> {
    if inputs.is_empty() {
        return Err(Error::invalid_argument("a merge needs at least one input"));
    }
    if inputs.len() > usize::from(u16::MAX) {
        return Err(Error::invalid_argument(
            "a merge takes at most 65535 inputs",
        ));
    }
    if options.chunk_bytes == 0 {
        return Err(Error::invalid_argument(
            "chunk_bytes must be greater than 0",
        ));
    }
    if options.verify_inputs {
        for (i, input) in inputs.iter().enumerate() {
            input.verify_files().map_err(|e| named(inputs, i, e))?;
        }
    }

    let mut writer = PartWriter::create(
        parts_dir,
        part_id,
        window_start_ns,
        window_ns,
        options.writer,
    )?;
    writer.set_level(options.level);

    let (order, stitched) = trace_order(inputs);
    let mut merger = Merger {
        inputs,
        writer,
        tables: inputs
            .iter()
            .map(|r| vec![UNSET; r.piece_store().totals().pieces as usize])
            .collect(),
        builder: SpanBatchBuilder::new(BatchLimits {
            max_rows: OUTPUT_BATCH_ROWS,
            max_bytes: options.chunk_bytes.clamp(1 << 20, OUTPUT_BATCH_MAX_BYTES),
        }),
        next_source: 0,
        chunk_bytes: options.chunk_bytes as u64,
        dropped: 0,
        inline: HashMap::new(),
        ropes: [Vec::new(), Vec::new(), Vec::new()],
    };

    let mut next = 0;
    while next < order.len() {
        let begin = next;
        let (mut parts, mut bytes, mut rows) = (Vec::new(), 0u64, 0usize);
        while next < order.len() {
            let trace = TraceId(order[next].trace);
            let before = parts.len();
            let (mut trace_bytes, mut trace_rows) = (0u64, 0usize);
            for (i, input) in inputs.iter().enumerate() {
                if let Some(e) = input.find_trace(trace) {
                    parts.push(Part {
                        trace: (next - begin) as u32,
                        input: i as u16,
                        first: e.first_row,
                        count: e.row_count,
                    });
                    trace_bytes = trace_bytes.saturating_add(
                        u64::from(e.row_count)
                            .saturating_mul(input.estimated_row_bytes(e.first_row)),
                    );
                    trace_rows += e.row_count as usize;
                }
            }
            // A chunk holds whole traces; one that is alone may be larger
            // than the bound.
            if next > begin
                && (bytes.saturating_add(trace_bytes) > merger.chunk_bytes
                    || rows + trace_rows > MAX_CHUNK_ROWS)
            {
                parts.truncate(before);
                break;
            }
            bytes = bytes.saturating_add(trace_bytes);
            rows += trace_rows;
            next += 1;
        }
        merger.merge_chunk(&parts)?;
    }
    if !merger.builder.is_empty() {
        let batch = merger.builder.finish();
        merger.writer.write_stored(&batch)?;
    }
    let meta = merger.writer.finish()?;

    let mut payload_in = PayloadTotals::default();
    for input in inputs {
        let t = input.payload_totals();
        payload_in.pieces += t.pieces;
        payload_in.logical_bytes += t.logical_bytes;
        payload_in.unique_bytes += t.unique_bytes;
        payload_in.stored_bytes += t.stored_bytes;
    }
    let rows_in: u64 = inputs.iter().map(|r| r.row_count()).sum();
    let stats = MergeStats {
        inputs: inputs.len(),
        rows_in,
        rows_out: meta.rows,
        duplicates_dropped: merger.dropped,
        traces: meta.traces,
        traces_stitched: stitched,
        payload_in,
        payload_out: meta.payload,
    };
    debug_assert_eq!(rows_in, stats.rows_out + stats.duplicates_dropped);
    Ok((meta, stats))
}

/// The distinct traces of all inputs in output order, and how many of them
/// have rows in more than one input. The inputs' indexes are sorted by trace
/// id, so this is a merge of sorted lists.
fn trace_order(inputs: &[&PartReader]) -> (Vec<OrderKey>, u64) {
    let mut cursor = vec![0usize; inputs.len()];
    let mut order = Vec::new();
    let mut stitched = 0;
    loop {
        let smallest = inputs
            .iter()
            .zip(&cursor)
            .filter_map(|(r, &c)| r.trace_entries().get(c).map(|e| e.trace_id))
            .min();
        let Some(trace) = smallest else { break };
        let (mut start, mut seen) = (i64::MAX, 0);
        for (r, c) in inputs.iter().zip(&mut cursor) {
            if let Some(e) = r.trace_entries().get(*c)
                && e.trace_id == trace
            {
                start = start.min(e.start_ns);
                *c += 1;
                seen += 1;
            }
        }
        stitched += u64::from(seen > 1);
        order.push(OrderKey {
            start,
            trace: trace.0,
        });
    }
    order.sort_unstable();
    (order, stitched)
}

struct Merger<'a> {
    inputs: &'a [&'a PartReader],
    writer: PartWriter,
    /// For each input, the id in the output of each of its pieces, `UNSET` or
    /// `INLINE`.
    tables: Vec<Vec<u32>>,
    builder: SpanBatchBuilder,
    /// Numbers the batches read from inputs, for the builder's origin cache.
    next_source: usize,
    chunk_bytes: u64,
    dropped: u64,
    /// Bytes of the pieces of the current unit that the output holds inline.
    inline: HashMap<(u16, u32), Vec<u8>>,
    /// The translated `attrs`, `events` and `links` of the row in hand.
    ropes: [Vec<u8>; 3],
}

impl Merger<'_> {
    /// Writes the rows of the traces of one chunk, in output order.
    fn merge_chunk(&mut self, parts: &[Part]) -> Result<()> {
        let mut keys = self.read_keys(parts)?;
        let before = keys.len();
        // The earliest arrival of a span id wins: inputs are oldest first, and
        // within an input the first row.
        keys.sort_unstable_by_key(|k| (k.trace, k.span, k.input, k.row));
        keys.dedup_by(|later, kept| later.trace == kept.trace && later.span == kept.span);
        self.dropped += (before - keys.len()) as u64;
        // The order of a flush: by start, then span id.
        keys.sort_unstable_by_key(|k| (k.trace, k.start, k.span, k.input, k.row));

        let mut from = 0;
        while from < keys.len() {
            let (mut to, mut bytes) = (from, 0u64);
            while to < keys.len() {
                let k = &keys[to];
                bytes += self.inputs[usize::from(k.input)].estimated_row_bytes(k.row);
                to += 1;
                if bytes >= self.chunk_bytes || to - from >= MAX_CHUNK_ROWS {
                    break;
                }
            }
            self.write_unit(&keys[from..to])?;
            from = to;
        }
        Ok(())
    }

    /// The keys of every row of `parts`, read without touching the raw
    /// columns or the piece stores.
    fn read_keys(&self, parts: &[Part]) -> Result<Vec<KeyRow>> {
        let inputs = self.inputs;
        let mut keys = Vec::with_capacity(parts.iter().map(|p| p.count as usize).sum());
        for (i, input) in inputs.iter().enumerate() {
            let mut mine: Vec<&Part> = parts.iter().filter(|p| usize::from(p.input) == i).collect();
            mine.sort_unstable_by_key(|p| p.first);
            let mut at = 0;
            while at < mine.len() {
                // Traces that follow each other in the input are one read.
                let mut end = at + 1;
                let mut run_end = mine[at].first + u64::from(mine[at].count);
                while end < mine.len() && mine[end].first == run_end {
                    run_end += u64::from(mine[end].count);
                    end += 1;
                }
                let run = &mine[at..end];
                let first = run[0].first;
                let batches = input
                    .read_rows_stored(first, run_end - first, Some(&[col::SPAN_ID, col::START_NS]))
                    .map_err(|e| named(inputs, i, e))?;
                let short = || damaged(inputs, i, "spans.parquet: fewer rows than the index lists");
                let (mut part, mut left, mut row) = (0, run[0].count, first);
                for batch in &batches {
                    let (Some(spans), Some(starts)) = (
                        batch.column(0).as_fixed_size_binary_opt(),
                        batch.column(1).as_primitive_opt::<Int64Type>(),
                    ) else {
                        return Err(damaged(inputs, i, "spans.parquet: unexpected column types"));
                    };
                    for r in 0..batch.num_rows() {
                        while left == 0 {
                            part += 1;
                            left = run.get(part).ok_or_else(short)?.count;
                        }
                        let span: [u8; 8] = spans
                            .value(r)
                            .try_into()
                            .map_err(|_| damaged(inputs, i, "spans.parquet: bad span id"))?;
                        keys.push(KeyRow {
                            trace: run[part].trace,
                            input: i as u16,
                            start: starts.value(r),
                            span,
                            row,
                        });
                        left -= 1;
                        row += 1;
                    }
                }
                if row != run_end {
                    return Err(short());
                }
                at = end;
            }
        }
        Ok(keys)
    }

    /// Reads the rows of `unit`, carries their pieces over and hands them to
    /// the writer in the order given.
    fn write_unit(&mut self, unit: &[KeyRow]) -> Result<()> {
        let inputs = self.inputs;
        // The rows in input order, so that they are read in runs.
        let mut by_row: Vec<u32> = (0..unit.len() as u32).collect();
        by_row.sort_unstable_by_key(|&u| (unit[u as usize].input, unit[u as usize].row));

        let mut loaded: Vec<Loaded> = Vec::new();
        // Where each row of the unit is: its batch in `loaded` and its row
        // in that batch.
        let mut loc = vec![(0u32, 0u32); unit.len()];
        let mut at = 0;
        while at < by_row.len() {
            let input = unit[by_row[at] as usize].input;
            let first = unit[by_row[at] as usize].row;
            let mut end = at + 1;
            let mut run_end = first + 1;
            while end < by_row.len() {
                let k = &unit[by_row[end] as usize];
                if k.input != input || k.row > run_end + READ_GAP_ROWS {
                    break;
                }
                run_end = k.row + 1;
                end += 1;
            }
            let i = usize::from(input);
            let batches = inputs[i]
                .read_rows_stored(first, run_end - first, None)
                .map_err(|e| named(inputs, i, e))?;
            let base = loaded.len();
            let mut row0 = first;
            for batch in batches {
                let rows = batch.num_rows() as u64;
                loaded.push(Loaded {
                    input,
                    first_row: row0,
                    batch,
                });
                row0 += rows;
            }
            if row0 != run_end {
                return Err(damaged(
                    inputs,
                    i,
                    "spans.parquet: fewer rows than the index lists",
                ));
            }
            let mut b = base;
            for &u in &by_row[at..end] {
                let row = unit[u as usize].row;
                while loaded[b].first_row + loaded[b].batch.num_rows() as u64 <= row {
                    b += 1;
                }
                loc[u as usize] = (b as u32, (row - loaded[b].first_row) as u32);
            }
            at = end;
        }
        drop(by_row);

        let columns = loaded
            .iter()
            .map(|l| {
                SpanColumns::try_new(&l.batch).map_err(|e| {
                    damaged(inputs, usize::from(l.input), format!("spans.parquet: {e}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sources: Vec<usize> = (0..columns.len()).map(|n| self.next_source + n).collect();
        self.next_source += columns.len();

        self.carry_pieces(unit, &loc, &columns)?;

        for (u, key) in unit.iter().enumerate() {
            let (b, row) = (loc[u].0 as usize, loc[u].1 as usize);
            let record = columns[b].record(row);
            self.translate_ropes(key.input, &record)?;
            self.builder.append_from_with_raw(
                &columns[b],
                sources[b],
                row,
                &self.ropes[0],
                record.events.map(|_| &self.ropes[1][..]),
                record.links.map(|_| &self.ropes[2][..]),
            );
            if self.builder.is_full() {
                let batch = self.builder.finish();
                self.writer.write_stored(&batch)?;
            }
        }
        Ok(())
    }

    /// Makes sure every piece the rows of `unit` refer to is in the output (or
    /// marked for inline) and, for those that go inline, has its bytes at
    /// hand. Each input's pieces are read in the order of their ids, which is
    /// the order of its blocks, so a block is decompressed once.
    fn carry_pieces(
        &mut self,
        unit: &[KeyRow],
        loc: &[(u32, u32)],
        columns: &[SpanColumns<'_>],
    ) -> Result<()> {
        let inputs = self.inputs;
        let mut need: Vec<(u16, u32)> = Vec::new();
        let mut compact_at = 1 << 16;
        for (u, key) in unit.iter().enumerate() {
            let i = usize::from(key.input);
            let record = columns[loc[u].0 as usize].record(loc[u].1 as usize);
            for value in [Some(record.attrs), record.events, record.links]
                .into_iter()
                .flatten()
            {
                for id in rope::piece_ids(value) {
                    let id = id.map_err(|e| rope_error(inputs, i, e))?;
                    match self.tables[i].get(id as usize) {
                        None => {
                            return Err(damaged(
                                inputs,
                                i,
                                format!(
                                    "spans.parquet: a stored value refers to piece {id} but the \
                                     store has {}",
                                    self.tables[i].len()
                                ),
                            ));
                        }
                        Some(&UNSET | &INLINE) => {
                            need.push((key.input, id));
                            // Many references name few pieces: keep the list
                            // as long as the distinct pieces, not the references.
                            if need.len() >= compact_at {
                                need.sort_unstable();
                                need.dedup();
                                compact_at = (need.len() * 2).max(1 << 16);
                            }
                        }
                        Some(_) => {}
                    }
                }
            }
        }
        need.sort_unstable();
        need.dedup();

        self.inline.clear();
        let mut at = 0;
        while at < need.len() {
            let input = need[at].0;
            let mut end = at;
            while end < need.len() && need[end].0 == input {
                end += 1;
            }
            let ids: Vec<u32> = need[at..end].iter().map(|n| n.1).collect();
            let i = usize::from(input);
            let table = &mut self.tables[i];
            let writer = &mut self.writer;
            let inline = &mut self.inline;
            let mut failure = None;
            inputs[i]
                .piece_store()
                .read_sorted(ids.iter().copied(), |n, bytes| {
                    if failure.is_some() {
                        return;
                    }
                    let id = ids[n];
                    let slot = &mut table[id as usize];
                    if *slot == UNSET {
                        match writer.intern_piece(bytes) {
                            Ok(Some(out)) => {
                                *slot = out;
                                return;
                            }
                            Ok(None) => *slot = INLINE,
                            Err(e) => {
                                failure = Some(e);
                                return;
                            }
                        }
                    }
                    inline.insert((input, id), bytes.to_vec());
                })
                .map_err(|e| named(inputs, i, e))?;
            if let Some(e) = failure {
                return Err(e);
            }
            at = end;
        }
        Ok(())
    }

    /// Fills `self.ropes` with the translated `attrs`, `events` and `links` of
    /// `record`, which came from input `input`, and counts the references.
    fn translate_ropes(&mut self, input: u16, record: &SpanRecord<'_>) -> Result<()> {
        let inputs = self.inputs;
        let i = usize::from(input);
        let values = [Some(record.attrs), record.events, record.links];
        for (slot, value) in values.into_iter().enumerate() {
            self.ropes[slot].clear();
            let Some(value) = value else { continue };
            let table = &self.tables[i];
            let writer = &mut self.writer;
            let inline = &self.inline;
            let unexpected = |id: u32| {
                Error::corrupt(format!("piece {id} was not carried over before its rows"))
            };
            rope::translate(
                value,
                |id| {
                    let len = inputs[i].piece_store().piece_len(id)?;
                    match table.get(id as usize).copied() {
                        Some(INLINE) => inline
                            .get(&(input, id))
                            .map(|b| Mapped::Inline(b))
                            .ok_or_else(|| unexpected(id)),
                        Some(UNSET) | None => Err(unexpected(id)),
                        Some(out) => {
                            writer.add_references(len);
                            Ok(Mapped::Piece(out))
                        }
                    }
                },
                &mut self.ropes[slot],
            )
            .map_err(|e| rope_error(inputs, i, e))?;
        }
        Ok(())
    }
}
