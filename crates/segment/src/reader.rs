//! Reading one part.
//!
//! Every file is opened once, by [`PartReader::open`], and read with
//! positioned reads that never touch the file offset, so one reader serves
//! many threads and keeps working after the part directory has been removed
//! (on Unix, an unlinked file stays readable through its open handle).
//!
//! Every call into the Parquet library runs inside `catch_unwind`: a damaged
//! file can make the library panic (assertions, arithmetic, unwraps), and a
//! database must report that as [`Error::Corrupt`], not crash. This relies on
//! the build not using `panic = "abort"`. The reader holds no mutable state,
//! so a caught panic cannot leave anything half-updated; an iterator that
//! panicked is finished and drops its Parquet reader.
//!
//! The raw columns are stored as ropes (see `nwdb_payload::rope`). A read
//! that returns any of them rebuilds the original bytes from the piece store;
//! a read that returns none never touches the piece files. Because a rebuilt
//! value can be thousands of times larger than its rope, each batch read from
//! Parquet is first measured in rebuilt bytes and divided so that every
//! returned batch respects [`BatchLimits`], and only then is one part of it
//! rebuilt at a time.
//!
//! A returned batch is rebuilt by block, not by reference: every reference of
//! its rows is collected and put in the order of the pieces, which is the order
//! of the blocks, so each block the batch needs is decompressed once and its
//! pieces are copied to their places in the output. What is held besides the
//! output is one decompressed block and 16 bytes per reference. Nothing is
//! allocated before the sizes have been checked against what the store says
//! the part's references add up to, and the allocation itself can fail with an
//! error.

use crate::index::{self, FileChecksum, FileChecksums, INDEX_FILE, TraceEntry};
use crate::search::TextFiles;
use crate::writer::row_value_bytes;
use crate::{PIECES_BLK, PIECES_IDX, SPANS_FILE, SUMMARY_FILE};
use arrow_array::cast::AsArray;
use arrow_array::{Array, LargeBinaryArray, RecordBatch};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::SchemaRef;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use bytes::Bytes;
use nwdb_core::{
    BatchLimits, Error, PayloadTotals, Result, TraceId, col, span_schema, summary_schema,
};
use nwdb_payload::rope::{self, RopeError, Segment};
use nwdb_payload::{FileSum, PieceReader};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Upper bound on rows per batch returned by the Parquet reader. The batch
/// size of a row group is lowered further so a batch stays near
/// [`BatchLimits::DEFAULT`]'s byte bound.
const MAX_BATCH_ROWS: usize = 8_192;
const VERIFY_CHUNK: usize = 1 << 20;
const READ_BUFFER: usize = 64 * 1024;

fn corrupt(file: &str, msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{file}: {msg}"))
}

/// Runs `f`, turning a panic into [`Error::Corrupt`] naming `file`.
fn guard<T>(file: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|payload| Err(panicked(file, payload)))
}

fn panicked(file: &str, payload: Box<dyn std::any::Any + Send>) -> Error {
    let msg = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into());
    corrupt(
        file,
        format!("the Parquet library panicked ({msg}): the file is damaged"),
    )
}

/// A file read with positioned reads only. Clones share the file but no
/// offset, so concurrent readers cannot disturb each other.
#[derive(Clone)]
struct PosFile {
    file: Arc<File>,
    len: u64,
}

struct PosReader {
    file: Arc<File>,
    pos: u64,
}

impl Read for PosReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.file.read_at(buf, self.pos) {
                Ok(n) => {
                    self.pos += n as u64;
                    return Ok(n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }
}

impl Length for PosFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for PosFile {
    type T = BufReader<PosReader>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(BufReader::with_capacity(
            READ_BUFFER,
            PosReader {
                file: self.file.clone(),
                pos: start,
            },
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        // A damaged footer can ask for any length: check it against the file
        // before allocating.
        let in_file = start
            .checked_add(length as u64)
            .is_some_and(|end| end <= self.len);
        if !in_file {
            return Err(ParquetError::EOF(format!(
                "range {start}+{length} is past the end of the file ({} bytes)",
                self.len
            )));
        }
        let mut buf = vec![0u8; length];
        self.file.read_exact_at(&mut buf, start)?;
        Ok(buf.into())
    }
}

/// Keeps only the dictionary values that the rows of `batch` use. The
/// Parquet reader hands back the dictionary of the whole page it decoded, so
/// without this a one-row read would hold every value of its row group's
/// page. The schema and the values of every row are unchanged.
fn compact_dictionaries(batch: RecordBatch) -> Result<RecordBatch> {
    if !batch
        .columns()
        .iter()
        .any(|c| c.as_any_dictionary_opt().is_some())
    {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .map(|c| match c.as_any_dictionary_opt() {
            Some(d) => garbage_collect_any_dictionary(d),
            None => Ok(Arc::clone(c)),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// The raw columns, which are stored as ropes.
const RAW_NAMES: [&str; 3] = [col::ATTRS, col::EVENTS, col::LINKS];

fn rope_error(e: RopeError) -> Error {
    match e {
        RopeError::Malformed(what) => corrupt(
            SPANS_FILE,
            format!("a stored value is malformed ({what}): the file is damaged"),
        ),
        RopeError::Checksum => value_checksum_error(),
        RopeError::Length => value_length_error(),
        RopeError::Piece(e) => e,
    }
}

fn value_length_error() -> Error {
    corrupt(
        SPANS_FILE,
        "a stored value does not add up to the length it carries: the file is damaged",
    )
}

fn value_checksum_error() -> Error {
    corrupt(
        SPANS_FILE,
        "a stored value does not rebuild to the checksum it carries: the file or the piece \
         store is damaged",
    )
}

/// What a rope needs to be rebuilt.
struct RopeSize {
    /// Bytes stored in the rope itself.
    inline: u64,
    /// Bytes of the pieces it refers to.
    refs: u64,
    /// The number of references.
    count: usize,
}

/// Measures a rope without reading any piece. A rope with references carries
/// the length of its value: the segments must add up to it, and the walk stops
/// at the segment that goes past it, so a damaged run length that makes text
/// parse as many references is refused after a few of them, not after the sum
/// of all. The bytes it refers to are also refused if they add up to more than
/// the store records for the whole part. A damaged or doctored rope must not
/// decide how much is allocated.
fn rope_size(store: &PieceReader, rope: &[u8]) -> Result<RopeSize> {
    let limit = store.totals().logical_bytes;
    let recorded = rope::recorded_len(rope).map_err(rope_error)?;
    let mut size = RopeSize {
        inline: 0,
        refs: 0,
        count: 0,
    };
    for segment in rope::segments(rope) {
        match segment.map_err(rope_error)? {
            Segment::Inline(b) => size.inline += b.len() as u64,
            Segment::Piece(id) => {
                size.refs = size
                    .refs
                    .checked_add(store.piece_len(id)?)
                    .filter(|&refs| refs <= limit)
                    .ok_or_else(|| too_many_references(limit))?;
                size.count += 1;
            }
        }
        if recorded.is_some_and(|n| size.inline + size.refs > n) {
            return Err(value_length_error());
        }
    }
    if recorded.is_some_and(|n| size.inline + size.refs != n) {
        return Err(value_length_error());
    }
    Ok(size)
}

fn too_many_references(limit: u64) -> Error {
    corrupt(
        SPANS_FILE,
        format!(
            "stored values refer to more bytes of pieces than the whole part does ({limit}): the \
             file is damaged"
        ),
    )
}

/// One reference to copy out of the store: piece `id` belongs at byte `dest`
/// of the values of the `col`th rebuilt column.
struct Copy {
    id: u32,
    col: u8,
    dest: u64,
}

/// One raw column while it is rebuilt.
struct Rebuilt {
    /// Position of the column in the batch.
    at: usize,
    /// Where each row's value starts in `values`, and where the last ends.
    offsets: Vec<i64>,
    values: Vec<u8>,
}

fn unrepresentable() -> Error {
    corrupt(
        SPANS_FILE,
        "the stored values rebuild to more bytes than a batch can hold: the file is damaged",
    )
}

/// `n` zeroed bytes, or an error if they cannot be had: an absurd size that
/// a damaged file asks for must not abort the process.
fn allocate(n: u64) -> Result<Vec<u8>> {
    let refused = || {
        corrupt(
            SPANS_FILE,
            format!(
                "a stored value of {n} bytes cannot be allocated: the file is damaged, or the \
                 machine is out of memory"
            ),
        )
    };
    let mut v = Vec::new();
    v.try_reserve_exact(usize::try_from(n).map_err(|_| refused())?)
        .map_err(|_| refused())?;
    v.resize(n as usize, 0);
    Ok(v)
}

/// `part` with the ropes of its raw columns replaced by the values they stand
/// for.
///
/// The sizes are worked out and checked first, then the output is allocated,
/// then the inline bytes are copied and every reference is noted. References
/// are put in the order of the pieces, which is the order of the blocks, and
/// copied block by block, so a block is decompressed once for the whole batch,
/// however many references to it there are and in whatever order the rows
/// hold them. Last, every value that has references is checked against the
/// checksum its rope carries: a reference damaged to name another piece
/// cannot return that piece's text.
fn rebuild(store: &PieceReader, part: &RecordBatch) -> Result<RecordBatch> {
    let schema = part.schema();
    let limit = store.totals().logical_bytes;

    let mut columns: Vec<Rebuilt> = Vec::new();
    let (mut refs, mut copy_count) = (0u64, 0usize);
    for name in RAW_NAMES {
        let Ok(at) = schema.index_of(name) else {
            continue;
        };
        let ropes = part.column(at).as_binary::<i64>();
        let mut offsets = Vec::with_capacity(ropes.len() + 1);
        offsets.push(0i64);
        let mut total = 0u64;
        for row in 0..ropes.len() {
            if !ropes.is_null(row) {
                let size = rope_size(store, ropes.value(row))?;
                refs = refs
                    .checked_add(size.refs)
                    .filter(|&r| r <= limit)
                    .ok_or_else(|| too_many_references(limit))?;
                copy_count += size.count;
                total = total
                    .checked_add(size.inline)
                    .and_then(|t| t.checked_add(size.refs))
                    .ok_or_else(unrepresentable)?;
            }
            offsets.push(i64::try_from(total).map_err(|_| unrepresentable())?);
        }
        columns.push(Rebuilt {
            at,
            offsets,
            values: Vec::new(),
        });
    }
    for column in &mut columns {
        let total = column.offsets.last().copied().unwrap_or(0);
        column.values = allocate(total as u64)?;
    }
    let mut copies: Vec<Copy> = Vec::new();
    copies
        .try_reserve_exact(copy_count)
        .map_err(|_| unrepresentable())?;

    for (c, column) in columns.iter_mut().enumerate() {
        let ropes = part.column(column.at).as_binary::<i64>();
        for row in (0..ropes.len()).filter(|&r| !ropes.is_null(r)) {
            let mut pos = column.offsets[row] as usize;
            for segment in rope::segments(ropes.value(row)) {
                match segment.map_err(rope_error)? {
                    Segment::Inline(b) => {
                        column.values[pos..pos + b.len()].copy_from_slice(b);
                        pos += b.len();
                    }
                    Segment::Piece(id) => {
                        copies.push(Copy {
                            id,
                            col: c as u8,
                            dest: pos as u64,
                        });
                        pos += store.piece_len(id)? as usize;
                    }
                }
            }
        }
    }
    copies.sort_unstable_by_key(|c| c.id);
    store.read_sorted(copies.iter().map(|c| c.id), |n, bytes| {
        let c = &copies[n];
        let at = c.dest as usize;
        columns[c.col as usize].values[at..at + bytes.len()].copy_from_slice(bytes);
    })?;
    drop(copies);

    let mut arrays = part.columns().to_vec();
    for column in columns {
        let ropes = part.column(column.at);
        let as_ropes = ropes.as_binary::<i64>();
        for row in (0..as_ropes.len()).filter(|&r| !as_ropes.is_null(r)) {
            if let Some(want) = rope::checksum(as_ropes.value(row)) {
                let value =
                    &column.values[column.offsets[row] as usize..column.offsets[row + 1] as usize];
                if crc32c::crc32c(value) != want {
                    return Err(value_checksum_error());
                }
            }
        }
        arrays[column.at] = Arc::new(LargeBinaryArray::try_new(
            OffsetBuffer::new(column.offsets.into()),
            Buffer::from_vec(column.values),
            ropes.nulls().cloned(),
        )?);
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// The columns a read returns and the order the caller asked for them in.
struct Projection {
    mask: ProjectionMask,
    /// Whether any of the raw columns is among them.
    rebuild: bool,
    /// For each requested column, its position among the columns the Parquet
    /// reader returns, which come back in file order. `None` when that is
    /// already the requested order.
    reorder: Option<Vec<usize>>,
}

impl Projection {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.reorder {
            Some(order) => Ok(batch.project(order)?),
            None => Ok(batch),
        }
    }
}

/// One Parquet file of a part, held open, with its footer and page index
/// loaded once.
struct Table {
    name: &'static str,
    file: PosFile,
    meta: ArrowReaderMetadata,
    /// Row number of the first row of each row group, and the total at the
    /// end.
    group_starts: Vec<u64>,
    /// Rows per batch for each row group.
    batch_rows: Vec<usize>,
    /// The piece store the rows' ropes refer to; `None` for a file without
    /// ropes (the summary).
    store: Option<Arc<PieceReader>>,
}

/// A batch as read from Parquet (raw columns still ropes) and how it is cut
/// into the batches to return.
struct Pending {
    stored: RecordBatch,
    ranges: VecDeque<Range<usize>>,
    rebuild: bool,
    /// Bytes of pieces the whole stored batch refers to, counting each
    /// reference.
    refs: u64,
}

impl Pending {
    /// The next batch to return, with its raw columns rebuilt.
    fn next(&mut self, table: &Table) -> Option<Result<RecordBatch>> {
        let range = self.ranges.pop_front()?;
        let part = self.stored.slice(range.start, range.len());
        Some(match &table.store {
            Some(store) if self.rebuild => rebuild(store, &part),
            _ => Ok(part),
        })
    }
}

impl Table {
    fn open(
        dir: &Path,
        name: &'static str,
        expected: &SchemaRef,
        recorded_len: u64,
    ) -> Result<Self> {
        let file = File::open(dir.join(name)).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => corrupt(name, "file is missing from the part"),
            _ => Error::Io(e),
        })?;
        let len = file.metadata()?.len();
        if recorded_len != len {
            return Err(corrupt(
                name,
                format!("file has {len} bytes but {INDEX_FILE} recorded {recorded_len}"),
            ));
        }
        let file = PosFile {
            file: Arc::new(file),
            len,
        };
        // The offset index is what lets a row selection skip pages. The
        // column index is not needed (no row filter is pushed down).
        let options = ArrowReaderOptions::new()
            .with_offset_index_policy(PageIndexPolicy::Required)
            .with_column_index_policy(PageIndexPolicy::Optional);
        let meta = guard(name, || {
            ArrowReaderMetadata::load(&file, options).map_err(|e| corrupt(name, e))
        })?;

        // The version was checked from the index before this, so a schema
        // that differs here is damage, not an older format.
        let have = meta.schema();
        let same = have.fields().len() == expected.fields().len()
            && have
                .fields()
                .iter()
                .zip(expected.fields())
                .all(|(h, w)| h.name() == w.name() && h.data_type() == w.data_type());
        if !same {
            return Err(corrupt(name, "columns do not match the expected schema"));
        }

        let mut group_starts = vec![0u64];
        let mut batch_rows = Vec::new();
        for group in meta.metadata().row_groups() {
            let rows = u64::try_from(group.num_rows()).map_err(|_| corrupt(name, "row count"))?;
            group_starts.push(group_starts.last().expect("never empty") + rows);
            // Rows per batch such that a batch of average rows stays near
            // the byte bound, from the group's uncompressed size.
            let avg = (u64::try_from(group.total_byte_size()).unwrap_or(0) / rows.max(1)).max(1);
            let fit = (BatchLimits::DEFAULT.max_bytes as u64 / avg).clamp(1, MAX_BATCH_ROWS as u64);
            batch_rows.push(fit as usize);
        }
        Ok(Self {
            name,
            file,
            meta,
            group_starts,
            batch_rows,
            store: None,
        })
    }

    /// Decides how a batch read from Parquet is cut into batches to return,
    /// from the sizes its values will have once rebuilt, without reading any
    /// piece. Every batch is within [`BatchLimits::DEFAULT`] in rebuilt value
    /// bytes, except that a single row larger than that is a batch alone.
    /// `rebuild` is false to keep the raw columns as stored.
    ///
    /// Fails, before anything is allocated for the values, if one value or the
    /// whole batch refers to more bytes of pieces than the store records for
    /// the part.
    fn plan(
        &self,
        stored: RecordBatch,
        projection: Option<&Projection>,
        rebuild: bool,
    ) -> Result<Pending> {
        let rows = stored.num_rows();
        let store = match &self.store {
            Some(store) if rebuild && projection.is_none_or(|p| p.rebuild) => store,
            _ => {
                return Ok(Pending {
                    ranges: if rows == 0 {
                        VecDeque::new()
                    } else {
                        std::iter::once(0..rows).collect()
                    },
                    stored,
                    rebuild: false,
                    refs: 0,
                });
            }
        };
        let limit = store.totals().logical_bytes;
        let mut batch_refs = 0u64;
        let mut weight = row_value_bytes(&stored);
        for name in RAW_NAMES {
            let Ok(at) = stored.schema().index_of(name) else {
                continue;
            };
            let column = stored.column(at).as_binary::<i64>();
            for (row, w) in weight.iter_mut().enumerate() {
                if column.is_null(row) {
                    continue;
                }
                let rope_bytes = column.value(row);
                let size = rope_size(store, rope_bytes)?;
                batch_refs = batch_refs
                    .checked_add(size.refs)
                    .filter(|&refs| refs <= limit)
                    .ok_or_else(|| too_many_references(limit))?;
                *w = w
                    .saturating_sub(rope_bytes.len() as u64)
                    .saturating_add(size.inline)
                    .saturating_add(size.refs);
            }
        }
        let limits = BatchLimits::DEFAULT;
        let mut ranges = VecDeque::new();
        let (mut start, mut sum) = (0usize, 0u64);
        for (row, &w) in weight.iter().enumerate() {
            if row > start && (sum + w > limits.max_bytes as u64 || row - start >= limits.max_rows)
            {
                ranges.push_back(start..row);
                (start, sum) = (row, 0);
            }
            sum += w;
        }
        if start < rows {
            ranges.push_back(start..rows);
        }
        Ok(Pending {
            stored,
            ranges,
            rebuild: true,
            refs: batch_refs,
        })
    }

    fn rows(&self) -> u64 {
        *self.group_starts.last().expect("never empty")
    }

    fn row_group_count(&self) -> usize {
        self.group_starts.len() - 1
    }

    /// Uncompressed bytes per row of the row group that holds `row`, at least
    /// 1; 1 for a row past the end.
    fn estimated_row_bytes(&self, row: u64) -> u64 {
        let group = self.group_starts.partition_point(|&s| s <= row) - 1;
        if group >= self.row_group_count() {
            return 1;
        }
        let g = self.meta.metadata().row_group(group);
        (u64::try_from(g.total_byte_size()).unwrap_or(0)
            / u64::try_from(g.num_rows()).unwrap_or(1).max(1))
        .max(1)
    }

    fn group_rows(&self, group: usize) -> usize {
        (self.group_starts[group + 1] - self.group_starts[group]) as usize
    }

    fn projection(&self, names: Option<&[&str]>) -> Result<Option<Projection>> {
        let Some(names) = names else {
            return Ok(None);
        };
        if names.is_empty() {
            return Err(Error::invalid_argument("projection names no columns"));
        }
        let schema = self.meta.schema();
        let mut wanted = Vec::with_capacity(names.len());
        for name in names {
            let idx = schema.index_of(name).map_err(|_| {
                Error::invalid_argument(format!("unknown column `{name}` in {}", self.name))
            })?;
            if wanted.contains(&idx) {
                return Err(Error::invalid_argument(format!(
                    "column `{name}` is named twice in the projection"
                )));
            }
            wanted.push(idx);
        }
        let mut file_order = wanted.clone();
        file_order.sort_unstable();
        let order: Vec<usize> = wanted
            .iter()
            .map(|w| {
                file_order
                    .binary_search(w)
                    .expect("came from the same list")
            })
            .collect();
        let reorder = (!order.iter().copied().eq(0..order.len())).then_some(order);
        let rebuild = names.iter().any(|n| RAW_NAMES.contains(n));
        let mask = guard(self.name, || {
            Ok(ProjectionMask::roots(
                self.meta.parquet_schema(),
                file_order,
            ))
        })?;
        Ok(Some(Projection {
            mask,
            rebuild,
            reorder,
        }))
    }

    /// A reader over `rows` (local to row group `group`), which selects rows
    /// so that pages holding none of them are not fetched. Its batches never
    /// span row groups and stay near the byte bound.
    fn group_reader(
        &self,
        group: usize,
        rows: Range<usize>,
        projection: Option<&Projection>,
    ) -> Result<ParquetRecordBatchReader> {
        guard(self.name, || {
            let group_rows = self.group_rows(group);
            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                self.file.clone(),
                self.meta.clone(),
            )
            .with_batch_size(self.batch_rows[group])
            .with_row_groups(vec![group]);
            if rows != (0..group_rows) {
                builder = builder.with_row_selection(RowSelection::from_consecutive_ranges(
                    std::iter::once(rows),
                    group_rows,
                ));
            }
            if let Some(p) = projection {
                builder = builder.with_projection(p.mask.clone());
            }
            builder.build().map_err(|e| corrupt(self.name, e))
        })
    }

    /// The next batch of `reader`, with the projection's column order applied.
    fn next_batch(
        &self,
        reader: &mut ParquetRecordBatchReader,
        projection: Option<&Projection>,
    ) -> Option<Result<RecordBatch>> {
        let batch = match catch_unwind(AssertUnwindSafe(|| reader.next())) {
            Ok(None) => return None,
            Ok(Some(batch)) => batch.map_err(|e| corrupt(self.name, e)),
            Err(payload) => Err(panicked(self.name, payload)),
        };
        Some(batch.and_then(|b| match projection {
            Some(p) => p.apply(b),
            None => Ok(b),
        }))
    }

    /// Reads `rows` (local to row group `group`).
    fn read_group(
        &self,
        group: usize,
        rows: Range<usize>,
        projection: Option<&Projection>,
        rebuild: bool,
    ) -> Result<Vec<RecordBatch>> {
        let mut reader = self.group_reader(group, rows, projection)?;
        let mut out = Vec::new();
        while let Some(batch) = self.next_batch(&mut reader, projection) {
            let mut pending = self.plan(batch?, projection, rebuild)?;
            while let Some(batch) = pending.next(self) {
                out.push(batch?);
            }
        }
        Ok(out)
    }

    /// Reads rows `first..first + count` of the file. With `rebuild` false
    /// the raw columns are returned as stored, as ropes.
    fn read_rows(
        &self,
        first: u64,
        count: u64,
        projection: Option<&Projection>,
        rebuild: bool,
    ) -> Result<Vec<RecordBatch>> {
        let end = first.checked_add(count);
        if end.is_none_or(|end| end > self.rows()) {
            return Err(Error::invalid_argument(format!(
                "rows {first}+{count} are outside {} ({} rows)",
                self.name,
                self.rows()
            )));
        }
        let end = first + count;
        let mut out = Vec::new();
        if count == 0 {
            return Ok(out);
        }
        // The first group that ends after `first`.
        let mut group = self.group_starts.partition_point(|&s| s <= first) - 1;
        while group < self.row_group_count() && self.group_starts[group] < end {
            let start = self.group_starts[group];
            let lo = first.max(start) - start;
            let hi = end.min(self.group_starts[group + 1]) - start;
            for batch in self.read_group(group, lo as usize..hi as usize, projection, rebuild)? {
                out.push(compact_dictionaries(batch)?);
            }
            group += 1;
        }
        Ok(out)
    }

    /// Row groups whose statistics for the root column `column` overlap the
    /// inclusive range.
    fn overlapping(&self, column: &str, min: Option<i64>, max: Option<i64>) -> Result<Vec<usize>> {
        guard(self.name, || {
            groups_overlapping(self.meta.metadata(), column, min, max)
                .ok_or_else(|| corrupt(self.name, format!("no column `{column}`")))
        })
    }

    /// The largest value in the statistics of root column `column` in row
    /// group `group`, or `i64::MAX` if the group has none.
    fn max_of(&self, column: &str, group: usize) -> Result<i64> {
        guard(self.name, || {
            let meta = self.meta.metadata();
            let idx = meta
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .position(|c| c.path().string() == column)
                .ok_or_else(|| corrupt(self.name, format!("no column `{column}`")))?;
            Ok(match meta.row_group(group).column(idx).statistics() {
                Some(Statistics::Int64(s)) => s.max_opt().copied().unwrap_or(i64::MAX),
                _ => i64::MAX,
            })
        })
    }

    fn read_groups(
        &self,
        groups: Vec<usize>,
        projection: Option<&Projection>,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        for group in groups {
            out.extend(self.read_group(group, 0..self.group_rows(group), projection, true)?);
        }
        Ok(out)
    }

    /// Reads every row, rebuilding every raw value, and discards what it
    /// reads: every rope must be well formed, name pieces that exist, rebuild
    /// to the checksum it carries, and together the ropes must refer to as many
    /// bytes of pieces as the store records for the part. A batch at a time.
    fn verify_ropes(&self) -> Result<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let mut refs = 0u64;
        for group in 0..self.row_group_count() {
            let mut reader = self.group_reader(group, 0..self.group_rows(group), None)?;
            while let Some(batch) = self.next_batch(&mut reader, None) {
                let mut pending = self.plan(batch?, None, true)?;
                refs = refs.saturating_add(pending.refs);
                while let Some(batch) = pending.next(self) {
                    batch?;
                }
            }
        }
        let recorded = store.totals().logical_bytes;
        if refs != recorded {
            return Err(corrupt(
                SPANS_FILE,
                format!(
                    "the stored values refer to {refs} bytes of pieces but {PIECES_IDX} records \
                     {recorded}"
                ),
            ));
        }
        Ok(())
    }

    /// Re-reads the whole file and compares its length and CRC-32C.
    fn verify(&self, want: FileChecksum) -> Result<()> {
        check_sum(self.name, self.file.len, self.file_sum()?, want)
    }

    /// Length and CRC-32C of the file as it is now.
    fn file_sum(&self) -> Result<FileChecksum> {
        let want = FileChecksum {
            len: self.file.len,
            crc32c: 0,
        };
        let now = self.file.file.metadata()?.len();
        if now != want.len {
            return Err(corrupt(
                self.name,
                format!(
                    "file has {now} bytes but had {} when it was opened",
                    want.len
                ),
            ));
        }
        let mut crc = 0u32;
        let mut buf = vec![0u8; VERIFY_CHUNK];
        let mut pos = 0u64;
        while pos < want.len {
            let n = (want.len - pos).min(VERIFY_CHUNK as u64) as usize;
            self.file
                .file
                .read_exact_at(&mut buf[..n], pos)
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        corrupt(self.name, "file ended before its recorded length")
                    }
                    _ => Error::Io(e),
                })?;
            crc = crc32c::crc32c_append(crc, &buf[..n]);
            pos += n as u64;
        }
        Ok(FileChecksum {
            len: want.len,
            crc32c: crc,
        })
    }
}

/// Compares a file's length and CRC-32C with the ones `traces.idx` recorded.
fn check_sum(name: &str, opened_len: u64, now: FileChecksum, want: FileChecksum) -> Result<()> {
    if now.len != want.len || opened_len != want.len {
        return Err(corrupt(
            name,
            format!(
                "file has {} bytes (opened with {opened_len}) but {INDEX_FILE} recorded {}",
                now.len, want.len
            ),
        ));
    }
    if now.crc32c != want.crc32c {
        return Err(corrupt(
            name,
            format!(
                "checksum mismatch: CRC-32C is {:#010x}, {INDEX_FILE} recorded {:#010x}",
                now.crc32c, want.crc32c
            ),
        ));
    }
    Ok(())
}

/// Row groups whose statistics for the root column named `column` overlap the
/// inclusive range; a group without usable statistics is kept. `None` if the
/// file has no such root column.
///
/// The Parquet column is found by name, not by the column's position in the
/// Arrow schema: a nested column before it has several Parquet leaves, so
/// the two numberings differ.
fn groups_overlapping(
    meta: &ParquetMetaData,
    column: &str,
    min: Option<i64>,
    max: Option<i64>,
) -> Option<Vec<usize>> {
    let idx = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.path().string() == column)?;
    Some(
        (0..meta.num_row_groups())
            .filter(|&g| {
                let stats = meta.row_group(g).column(idx).statistics();
                let Some(Statistics::Int64(s)) = stats else {
                    return true;
                };
                let (Some(lo), Some(hi)) = (s.min_opt(), s.max_opt()) else {
                    return true;
                };
                min.is_none_or(|m| *hi >= m) && max.is_none_or(|m| *lo <= m)
            })
            .collect(),
    )
}

/// Which rows [`PartReader::scan`] reads.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Columns to return, in the order they should appear; `None` for all.
    pub projection: Option<Vec<String>>,
    /// Keep row groups that may hold a span starting at or after this time.
    pub start_ns_min: Option<i64>,
    /// Keep row groups that may hold a span starting at or before this time.
    pub start_ns_max: Option<i64>,
}

/// The batches of a [`PartReader::scan`], read lazily one at a time.
///
/// Whole row groups are returned, one group after the other, and a batch
/// never spans two groups: the scan prunes by each group's `start_ns`
/// statistics and does not filter rows, so the caller must still test each
/// row against its range. After an error the scan ends.
pub struct Scan {
    table: Arc<Table>,
    projection: Option<Projection>,
    groups: std::vec::IntoIter<usize>,
    reader: Option<ParquetRecordBatchReader>,
    /// The batch read last, while its parts are being rebuilt and returned.
    pending: Option<Pending>,
    read: usize,
    skipped: usize,
}

impl Scan {
    /// Number of row groups this scan reads.
    pub fn row_groups_read(&self) -> usize {
        self.read
    }

    /// Number of row groups this scan skipped using statistics.
    pub fn row_groups_skipped(&self) -> usize {
        self.skipped
    }

    fn end(&mut self) {
        self.reader = None;
        self.pending = None;
        self.groups = Vec::new().into_iter();
    }
}

impl Iterator for Scan {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                match pending.next(&self.table) {
                    Some(Ok(batch)) => return Some(Ok(batch)),
                    Some(Err(e)) => {
                        self.end();
                        return Some(Err(e));
                    }
                    None => self.pending = None,
                }
            }
            let Some(reader) = self.reader.as_mut() else {
                let group = self.groups.next()?;
                let rows = 0..self.table.group_rows(group);
                match self
                    .table
                    .group_reader(group, rows, self.projection.as_ref())
                {
                    Ok(reader) => self.reader = Some(reader),
                    Err(e) => {
                        self.end();
                        return Some(Err(e));
                    }
                }
                continue;
            };
            match self.table.next_batch(reader, self.projection.as_ref()) {
                None => self.reader = None,
                Some(Ok(batch)) => match self.table.plan(batch, self.projection.as_ref(), true) {
                    Ok(pending) => self.pending = Some(pending),
                    Err(e) => {
                        self.end();
                        return Some(Err(e));
                    }
                },
                Some(Err(e)) => {
                    self.end();
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Read access to one part.
///
/// Opening loads and checks `traces.idx` in full and the Parquet metadata of
/// both files, including their page indexes, and keeps both Parquet files
/// open. Reads use positioned reads on those handles, so the reader is
/// `Send + Sync`, one value can serve many threads at once, and reads keep
/// working (on Unix) after the part directory has been deleted.
pub struct PartReader {
    dir: PathBuf,
    /// `traces.idx`, kept open so that `verify` reads the file the part was
    /// opened with, as the other files are, even after the directory is gone.
    index_file: File,
    index_len: u64,
    entries: Vec<TraceEntry>,
    checksums: FileChecksums,
    spans: Arc<Table>,
    summary: Arc<Table>,
    store: Arc<PieceReader>,
    pub(crate) text: TextFiles,
}

impl PartReader {
    /// Opens the part in `part_dir`.
    ///
    /// Returns [`Error::Corrupt`], naming the file, if a file is missing, the
    /// index fails its magic, version (an older or newer part format is refused
    /// with a message saying which version the part has), length or checksum check, a Parquet
    /// file's length differs from the one recorded in the index, the length of
    /// `pieces.blk` or `pieces.idx`, or the CRC-32C of `pieces.idx`, differs
    /// from the recorded one (the lengths are compared before the file is
    /// read), `pieces.idx` fails its own checks, a file's columns are not the
    /// expected ones, the Parquet library panics on the file, or the index's row
    /// ranges do not tile exactly the rows of `spans.parquet`. The contents of
    /// the Parquet files and the blocks of the piece store are not checksummed
    /// here; see [`verify`](Self::verify).
    pub fn open(part_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = part_dir.as_ref();
        let mut index_file = File::open(dir.join(INDEX_FILE)).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => corrupt(INDEX_FILE, "file is missing from the part"),
            _ => Error::Io(e),
        })?;
        let mut index_bytes = Vec::new();
        index_file.read_to_end(&mut index_bytes)?;
        let index_len = index_bytes.len() as u64;
        let decoded = index::decode(&index_bytes)?;
        let (entries, checksums) = (decoded.entries, decoded.checksums);
        let mut spans = Table::open(dir, SPANS_FILE, &span_schema(), checksums.spans.len)?;
        let summary = Table::open(dir, SUMMARY_FILE, &summary_schema(), checksums.summary.len)?;

        // The store checks the lengths before it reads anything, and the
        // checksum of `pieces.idx`, which holds the checksum of every block, so
        // that files of another part, whatever their lengths, are refused.
        let sum = |c: FileChecksum| FileSum {
            len: c.len,
            crc32c: c.crc32c,
        };
        let store = Arc::new(PieceReader::open_recorded(
            dir,
            sum(checksums.pieces_blk),
            sum(checksums.pieces_idx),
        )?);
        spans.store = Some(store.clone());
        let text = TextFiles::open(dir, &checksums, store.totals().pieces)?;

        check_ranges(&entries, spans.rows())?;
        if summary.rows() != entries.len() as u64 {
            return Err(corrupt(
                SUMMARY_FILE,
                format!(
                    "{} rows but the index lists {} traces",
                    summary.rows(),
                    entries.len()
                ),
            ));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            index_file,
            index_len,
            entries,
            checksums,
            spans: Arc::new(spans),
            summary: Arc::new(summary),
            store,
            text,
        })
    }

    /// What the part's piece store holds.
    pub fn payload_totals(&self) -> PayloadTotals {
        self.store.totals()
    }

    /// How many piece blocks this reader has read and decompressed since it
    /// was opened. A read that returns none of `attrs`, `events` and `links`
    /// leaves it unchanged.
    pub fn payload_blocks_loaded(&self) -> u64 {
        self.store.blocks_loaded()
    }

    /// Length and CRC-32C of `spans.parquet` recorded when the part was
    /// written.
    pub fn spans_checksum(&self) -> FileChecksum {
        self.checksums.spans
    }

    /// Length and CRC-32C of `summary.parquet` recorded when the part was
    /// written.
    pub fn summary_checksum(&self) -> FileChecksum {
        self.checksums.summary
    }

    /// Re-reads `traces.idx` and compares it with what was decoded at open,
    /// re-reads all four data files in full and compares their length and
    /// CRC-32C with the values recorded in `traces.idx`, reads every block of
    /// the piece store, and then walks every rope of every row against the
    /// store: each must be well formed, refer to pieces that exist, rebuild to
    /// the checksum it carries, and together the ropes must refer to exactly the
    /// bytes of pieces that `pieces.idx` records. Fails with [`Error::Corrupt`]
    /// naming the file on a mismatch. `verify` checks the files this reader
    /// holds open: a part that passes verify opens, as long as its files were
    /// not replaced after this reader opened them. Whatever `open` checks, this
    /// checks against those files as they are now, including the trailing
    /// checksum of `pieces.idx` and the footer checksums of the text files.
    ///
    /// This costs a sequential read of the part and a rebuild of every value,
    /// so [`open`](Self::open) does not do it; compaction and a scrubber
    /// should.
    pub fn verify(&self) -> Result<()> {
        self.verify_files()?;
        self.verify_text()?;
        self.spans.verify_ropes()
    }

    /// The part of [`verify`](Self::verify) that reads the files and no rope:
    /// the length and CRC-32C of all four data files against the index, and
    /// every block of the piece store. Because the checksum of `spans.parquet`
    /// covers every rope, a part that passes holds the ropes it was written
    /// with, which is all a caller that carries them over unchanged needs to
    /// know; it costs a sequential read and no rebuild of values.
    pub(crate) fn verify_files(&self) -> Result<()> {
        self.verify_index()?;
        self.spans.verify(self.checksums.spans)?;
        self.summary.verify(self.checksums.summary)?;
        let (blk, idx) = self.store.file_sums()?;
        for (name, now, recorded) in [
            (PIECES_BLK, blk, self.checksums.pieces_blk),
            (PIECES_IDX, idx, self.checksums.pieces_idx),
        ] {
            let now = FileChecksum {
                len: now.len,
                crc32c: now.crc32c,
            };
            check_sum(name, now.len, now, recorded)?;
        }
        self.store.verify()
    }

    /// Reads `traces.idx` again and requires it to decode to what was read at
    /// open. Everything `open` checks, `verify` checks again, so that a part
    /// that passes is a part that opens. It reads the handle kept from open,
    /// like every other file, and refuses a file whose length has changed
    /// before it reads it.
    fn verify_index(&self) -> Result<()> {
        let now = self.index_file.metadata()?.len();
        if now != self.index_len {
            return Err(corrupt(
                INDEX_FILE,
                format!(
                    "file has {now} bytes but had {} when it was opened",
                    self.index_len
                ),
            ));
        }
        let mut bytes = vec![0u8; now as usize];
        self.index_file.read_exact_at(&mut bytes, 0)?;
        let now = index::decode(&bytes)?;
        if now.entries != self.entries || now.checksums != self.checksums {
            return Err(corrupt(
                INDEX_FILE,
                "file differs from what it was when opened",
            ));
        }
        Ok(())
    }

    /// The directory the part was opened from, to name it in an error.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// About how many bytes row `row` takes as stored (uncompressed, ropes not
    /// rebuilt): the average of the row group that holds it. The merge sizes
    /// its chunks with it before it has read anything.
    pub(crate) fn estimated_row_bytes(&self, row: u64) -> u64 {
        self.spans.estimated_row_bytes(row)
    }

    /// Number of span rows.
    pub fn row_count(&self) -> u64 {
        self.spans.rows()
    }

    /// Number of traces.
    pub fn trace_count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Earliest span start in the part, Unix nanoseconds.
    pub fn min_start_ns(&self) -> i64 {
        self.entries.iter().map(|e| e.start_ns).min().unwrap_or(0)
    }

    /// Latest span end in the part, Unix nanoseconds.
    pub fn max_end_ns(&self) -> i64 {
        self.entries.iter().map(|e| e.end_ns).max().unwrap_or(0)
    }

    /// Number of row groups in `spans.parquet`.
    pub fn row_group_count(&self) -> usize {
        self.spans.row_group_count()
    }

    /// An upper bound on the `start_ns` of every span of the part: the largest
    /// maximum in the statistics of the row groups of `spans.parquet`
    /// (`i64::MAX` if a group records none). It reads no data page.
    pub fn max_start_ns(&self) -> Result<i64> {
        (0..self.spans.row_group_count()).try_fold(i64::MIN, |most, group| {
            Ok(most.max(self.spans.max_of(col::START_NS, group)?))
        })
    }

    /// Number of row groups in `summary.parquet`.
    pub fn summary_row_group_count(&self) -> usize {
        self.summary.row_group_count()
    }

    /// Every trace of the part, sorted by trace id.
    pub fn trace_entries(&self) -> &[TraceEntry] {
        &self.entries
    }

    /// Finds a trace by binary search.
    pub fn find_trace(&self, trace_id: TraceId) -> Option<TraceEntry> {
        self.entries
            .binary_search_by_key(&trace_id, |e| e.trace_id)
            .ok()
            .map(|i| self.entries[i])
    }

    /// Reads the spans of one trace, in the order they were written, or
    /// `None` if the part does not hold the trace.
    ///
    /// Only the trace's rows are fetched: pages that hold none of them are
    /// skipped. `projection` names the columns to return, in the order
    /// wanted; `None` returns all of them. No returned batch spans two row
    /// groups or holds much more than 32 MiB of values.
    pub fn read_trace(
        &self,
        trace_id: TraceId,
        projection: Option<&[&str]>,
    ) -> Result<Option<Vec<RecordBatch>>> {
        let projection = self.spans.projection(projection)?;
        let Some(entry) = self.find_trace(trace_id) else {
            return Ok(None);
        };
        self.spans
            .read_rows(
                entry.first_row,
                u64::from(entry.row_count),
                projection.as_ref(),
                true,
            )
            .map(Some)
    }

    /// Reads `row_count` rows starting at row `first_row` of `spans.parquet`.
    ///
    /// Fails with [`Error::InvalidArgument`] if the range is not inside the
    /// part.
    pub fn read_rows(
        &self,
        first_row: u64,
        row_count: u64,
        projection: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let projection = self.spans.projection(projection)?;
        self.spans
            .read_rows(first_row, row_count, projection.as_ref(), true)
    }

    /// Like [`read_rows`](Self::read_rows), but the raw columns `attrs`,
    /// `events` and `links` are returned as they are stored: ropes, not
    /// rebuilt values, and no piece is read. [`nwdb_payload::rope::piece_ids`]
    /// lists the pieces a rope refers to, and
    /// [`piece_store`](Self::piece_store) fetches a piece. Meant for building
    /// something on the piece store, such as an index of its text; a rope is
    /// not checked here, only when it is rebuilt by a read that returns it.
    pub fn read_rows_stored(
        &self,
        first_row: u64,
        row_count: u64,
        projection: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let projection = self.spans.projection(projection)?;
        self.spans
            .read_rows(first_row, row_count, projection.as_ref(), false)
    }

    /// The part's piece store: pieces by id
    /// ([`PieceReader::read_piece`], [`PieceReader::read_sorted`]) and its
    /// totals.
    pub fn piece_store(&self) -> &PieceReader {
        &self.store
    }

    /// Reads every row of the row groups whose `start_ns` statistics overlap
    /// the range in `options`, without filtering rows, one row group at a
    /// time. Errors in the files are reported by the returned iterator.
    pub fn scan(&self, options: ScanOptions) -> Result<Scan> {
        let names: Option<Vec<&str>> = options
            .projection
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        let projection = self.spans.projection(names.as_deref())?;
        let groups =
            self.spans
                .overlapping(col::START_NS, options.start_ns_min, options.start_ns_max)?;
        let read = groups.len();
        let skipped = self.spans.row_group_count() - read;
        Ok(Scan {
            table: self.spans.clone(),
            projection,
            groups: groups.into_iter(),
            reader: None,
            pending: None,
            read,
            skipped,
        })
    }

    /// Reads the trace summaries of the row groups whose `start_ns`
    /// statistics overlap the inclusive range. The result can include traces
    /// outside the range; the caller filters.
    pub fn scan_summary(
        &self,
        start_ns_min: Option<i64>,
        start_ns_max: Option<i64>,
    ) -> Result<Vec<RecordBatch>> {
        let groups = self
            .summary
            .overlapping("start_ns", start_ns_min, start_ns_max)?;
        self.summary.read_groups(groups, None)
    }

    /// Like [`scan_summary`](Self::scan_summary), but lazy and newest first:
    /// the row groups that overlap the inclusive range are read one at a time,
    /// starting with the group that holds the latest starts.
    pub fn scan_summary_rev(
        &self,
        start_ns_min: Option<i64>,
        start_ns_max: Option<i64>,
    ) -> Result<SummaryScanRev> {
        let groups = self
            .summary
            .overlapping("start_ns", start_ns_min, start_ns_max)?;
        let groups = groups
            .into_iter()
            .map(|g| Ok((g, self.summary.max_of("start_ns", g)?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(SummaryScanRev {
            table: self.summary.clone(),
            groups,
            read: 0,
        })
    }
}

/// The row groups of a [`PartReader::scan_summary_rev`], read lazily from the
/// newest to the oldest.
///
/// The summary file is sorted by `(start_ns, trace_id)`, so the groups come
/// in file order from the oldest to the newest, and a group's rows are in
/// ascending order: a caller that wants descending order reads each returned
/// group's batches and rows backwards. Like `scan_summary` it prunes by
/// statistics and does not filter rows.
pub struct SummaryScanRev {
    table: Arc<Table>,
    /// File order; each group with the largest `start_ns` its statistics
    /// allow. The next group to read is the last one.
    groups: Vec<(usize, i64)>,
    read: usize,
}

impl SummaryScanRev {
    /// An upper bound on the `start_ns` of every row not yet returned (the
    /// group's recorded maximum, or `i64::MAX` if it has none), or `None` when
    /// the scan is finished. It lets a caller that merges several parts decide
    /// whether this part can matter before paying to read a group.
    pub fn next_upper_bound(&self) -> Option<i64> {
        self.groups.last().map(|&(_, max)| max)
    }

    /// Reads the newest group not yet read, as batches in ascending row
    /// order; `None` when the scan is finished.
    pub fn next_group(&mut self) -> Result<Option<Vec<RecordBatch>>> {
        let Some(&(group, _)) = self.groups.last() else {
            return Ok(None);
        };
        let batches = self.table.read_groups(vec![group], None)?;
        // Dropped only after a successful read, so a failed read leaves the
        // scan where it was.
        self.groups.pop();
        self.read += 1;
        Ok(Some(batches))
    }

    /// Number of row groups returned so far.
    pub fn row_groups_read(&self) -> usize {
        self.read
    }
}

/// Checks that the traces' row ranges tile `0..rows` exactly, in some order.
fn check_ranges(entries: &[TraceEntry], rows: u64) -> Result<()> {
    let mut ranges: Vec<(u64, u64)> = entries
        .iter()
        .map(|e| {
            (
                e.first_row,
                e.first_row.saturating_add(u64::from(e.row_count)),
            )
        })
        .collect();
    ranges.sort_unstable();
    let mut next = 0;
    for (start, end) in ranges {
        if start != next || end <= start {
            return Err(corrupt(
                INDEX_FILE,
                format!("row range {start}..{end} does not follow the previous trace's rows"),
            ));
        }
        next = end;
    }
    if next != rows {
        return Err(corrupt(
            INDEX_FILE,
            format!("covers {next} rows but {SPANS_FILE} has {rows}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use parquet::arrow::ArrowWriter;

    /// A file whose first Arrow column is a struct with two Parquet leaves, so
    /// `start_ns` is Arrow column 1 but Parquet column 2. Row group 0 has
    /// `start_ns` 0..10 and struct leaf `b` 1000..1010; group 1 the reverse
    /// ranges, so statistics of the wrong column select the wrong group.
    #[test]
    fn statistics_are_looked_up_by_column_name() {
        let ints =
            |from: i64| -> ArrayRef { Arc::new(Int64Array::from_iter_values(from..from + 10)) };
        let inner = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("nest", DataType::Struct(inner.clone()), false),
            Field::new("start_ns", DataType::Int64, false),
        ]));
        let batch = |start: i64, b: i64| {
            let nest = StructArray::new(inner.clone(), vec![ints(0), ints(b)], None);
            RecordBatch::try_new(schema.clone(), vec![Arc::new(nest), ints(start)]).unwrap()
        };
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested.parquet");
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        w.write(&batch(0, 1000)).unwrap();
        w.flush().unwrap();
        w.write(&batch(100, 0)).unwrap();
        w.close().unwrap();

        let meta =
            ArrowReaderMetadata::load(&File::open(&path).unwrap(), ArrowReaderOptions::new())
                .unwrap();
        let md = meta.metadata();
        assert_eq!(md.num_row_groups(), 2);
        // Statistics of leaf `b`, which sits at the Arrow index of `start_ns`.
        let wrong = md.row_group(0).column(1).statistics().unwrap();
        assert!(matches!(wrong, Statistics::Int64(s) if s.min_opt() == Some(&1000)));

        assert_eq!(
            groups_overlapping(md, "start_ns", Some(100), None),
            Some(vec![1])
        );
        assert_eq!(
            groups_overlapping(md, "start_ns", None, Some(50)),
            Some(vec![0])
        );
        assert_eq!(
            groups_overlapping(md, "start_ns", Some(5), Some(105)),
            Some(vec![0, 1])
        );
        assert_eq!(groups_overlapping(md, "no_such_column", None, None), None);
        // `b` is nested, not a root column, so it cannot be addressed by
        // the bare name.
        assert_eq!(groups_overlapping(md, "b", None, None), None);
    }

    #[test]
    fn guard_turns_a_panic_into_corrupt() {
        let err = guard::<()>("x.parquet", || panic!("boom")).unwrap_err();
        assert!(matches!(&err, Error::Corrupt(m) if m.contains("x.parquet") && m.contains("boom")));
        let err = guard::<()>("x.parquet", || {
            let v: Vec<u8> = Vec::new();
            let i = v.len() + 1;
            let _ = v[i];
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(err, Error::Corrupt(_)));
        assert_eq!(guard("x", || Ok(5)).unwrap(), 5);
    }

    use arrow_schema::{DataType as Type, Field as Col, Schema as Cols};
    use nwdb_payload::{PieceWriter, PieceWriterOptions};
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};

    /// A store of `pieces`, each referenced `refs` times in total, so that the
    /// store records `refs` times their length as the part's references.
    fn store_of(dir: &Path, pieces: &[Vec<u8>], refs: usize) -> PieceReader {
        let mut w = PieceWriter::create(dir, PieceWriterOptions::default()).unwrap();
        for p in pieces {
            w.add(p).unwrap();
            for _ in 1..refs {
                w.find(p).unwrap();
            }
        }
        w.finish().unwrap();
        PieceReader::open(dir).unwrap()
    }

    fn random(seed: u64, len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        StdRng::seed_from_u64(seed).fill_bytes(&mut v);
        v
    }

    /// A batch with the one raw column `name`, holding `ropes`.
    fn batch_of(name: &str, ropes: &[Vec<u8>]) -> RecordBatch {
        let schema = Arc::new(Cols::new(vec![Col::new(name, Type::LargeBinary, true)]));
        let column = LargeBinaryArray::from_iter_values(ropes.iter());
        RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()
    }

    fn message(e: Error) -> String {
        match e {
            Error::Corrupt(m) => m,
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    /// One piece shared by many spans, so that the part records a total far
    /// larger than any value. A damaged length of an inline run of control
    /// bytes makes the text after it parse as references to the large piece:
    /// the references add up to less than the part's total, so only the length
    /// the rope records can refuse the value, and it must do so while the rope
    /// is measured, before the output is allocated.
    #[test]
    #[allow(clippy::single_range_in_vec_init)] // one cut, on purpose
    fn a_damaged_run_length_is_refused_when_the_rope_is_measured() {
        const PIECE: usize = 64 << 10;
        const SPANS: usize = 200;
        let tmp = tempfile::tempdir().unwrap();
        let store = store_of(tmp.path(), &[random(1, PIECE)], SPANS);
        assert_eq!(store.totals().logical_bytes, (SPANS * PIECE) as u64);

        // 300 bytes that parse as references to piece 0, then the piece.
        let mut value = vec![1u8; 300];
        value.extend_from_slice(&random(1, PIECE));
        let mut good = Vec::new();
        rope::encode_cut(&value, &[300..300 + PIECE], &[0], &mut good);
        let size = rope_size(&store, &good).unwrap();
        assert_eq!((size.inline, size.refs, size.count), (300, PIECE as u64, 1));

        let mut as_references = 0;
        for at in 0..good.len() {
            for bit in 0..8 {
                let mut bad = good.clone();
                bad[at] ^= 1 << bit;
                let parsed: Vec<_> = rope::segments(&bad).collect();
                let refs = parsed
                    .iter()
                    .filter(|s| matches!(s, Ok(Segment::Piece(_))))
                    .count();
                match rope_size(&store, &bad) {
                    Ok(size) => assert_eq!(
                        size.inline + size.refs,
                        value.len() as u64,
                        "byte {at} bit {bit}"
                    ),
                    Err(e) => {
                        if refs > 1 && parsed.iter().all(|s| s.is_ok()) {
                            // Text turned into references, every one of them
                            // valid, together under the part's total.
                            let m = message(e);
                            if m.contains("length it carries") {
                                as_references += 1;
                            } else {
                                // The other ways to refuse such a rope: more
                                // bytes than the whole part, or a piece that
                                // does not exist.
                                assert!(
                                    m.contains("more bytes") || m.contains("refers to piece"),
                                    "{m}"
                                );
                            }
                        }
                    }
                }
            }
        }
        assert!(as_references > 0, "no flip turned text into references");

        // The same in a batch of many spans: one damaged rope refuses the
        // batch, and the undamaged batch is rebuilt.
        let mut ropes = vec![good.clone(); SPANS - 1];
        let part = batch_of("attrs", &ropes);
        let rebuilt = rebuild(&store, &part).unwrap();
        assert_eq!(rebuilt.column(0).as_binary::<i64>().value(7), &value[..]);
        let mut bad = good.clone();
        bad[8] ^= 0x40;
        ropes[100] = bad;
        let m = message(rebuild(&store, &batch_of("attrs", &ropes)).unwrap_err());
        assert!(m.contains("length it carries"), "{m}");
    }

    /// Every single-bit flip of a rope with references, in each of the three
    /// raw columns, is refused or rebuilds to the original bytes, never to
    /// others.
    #[test]
    fn a_flipped_bit_in_a_rope_is_refused_or_harmless_in_every_raw_column() {
        let tmp = tempfile::tempdir().unwrap();
        let pieces: Vec<Vec<u8>> = (0..4).map(|i| random(10 + i, 40)).collect();
        let store = store_of(tmp.path(), &pieces, 3);
        let mut value = b"head ".to_vec();
        let mut cuts = Vec::new();
        for (i, p) in pieces.iter().enumerate() {
            if i % 2 == 0 {
                value.extend_from_slice(b"-gap-");
            }
            cuts.push(value.len()..value.len() + p.len());
            value.extend_from_slice(p);
        }
        value.extend_from_slice(b" tail");
        let mut good = Vec::new();
        rope::encode_cut(&value, &cuts, &[0, 1, 2, 3], &mut good);
        for name in RAW_NAMES {
            let ok = rebuild(&store, &batch_of(name, std::slice::from_ref(&good))).unwrap();
            assert_eq!(ok.column(0).as_binary::<i64>().value(0), &value[..]);
            let mut refused = 0;
            for at in 0..good.len() {
                for bit in 0..8 {
                    let mut bad = good.clone();
                    bad[at] ^= 1 << bit;
                    match rebuild(&store, &batch_of(name, &[bad])) {
                        Err(e) => {
                            message(e);
                            refused += 1;
                        }
                        Ok(got) => assert_eq!(
                            got.column(0).as_binary::<i64>().value(0),
                            &value[..],
                            "{name}: byte {at} bit {bit}"
                        ),
                    }
                }
            }
            assert!(refused > good.len(), "{name}: {refused}");
        }
    }
}
