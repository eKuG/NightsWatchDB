//! Reading one part.

use crate::index::{self, INDEX_FILE, TraceEntry};
use crate::{SPANS_FILE, SUMMARY_FILE};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use nwdb_core::{Error, Result, TraceId, col, span_schema, summary_schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use parquet::file::statistics::Statistics;
use std::fs::{self, File};
use std::ops::Range;
use std::path::{Path, PathBuf};

/// Rows per batch returned by the Parquet reader. A row group never holds
/// more value bytes than the writer's bound (plus one row), so a batch of this
/// many rows cannot exceed that bound either.
const READ_BATCH_ROWS: usize = 8_192;

fn corrupt(file: &str, msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{file}: {msg}"))
}

/// The columns a read returns and the order the caller asked for them in.
struct Projection {
    mask: ProjectionMask,
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

/// One Parquet file of a part, with its footer and page index loaded once.
///
/// It keeps no file handle. Every read opens its own, because handles that
/// are duplicated from one share a single file position, and two threads
/// reading through them corrupt each other's reads.
struct Table {
    name: &'static str,
    path: PathBuf,
    meta: ArrowReaderMetadata,
    /// Row number of the first row of each row group, and the total at the
    /// end.
    group_starts: Vec<u64>,
}

impl Table {
    fn open(dir: &Path, name: &'static str, expected: &SchemaRef) -> Result<Self> {
        let path = dir.join(name);
        let file = File::open(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => corrupt(name, "file is missing from the part"),
            _ => Error::Io(e),
        })?;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let meta = ArrowReaderMetadata::load(&file, options).map_err(|e| corrupt(name, e))?;

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
        for group in meta.metadata().row_groups() {
            let rows = u64::try_from(group.num_rows()).map_err(|_| corrupt(name, "row count"))?;
            group_starts.push(group_starts.last().expect("never empty") + rows);
        }
        Ok(Self {
            name,
            path,
            meta,
            group_starts,
        })
    }

    fn rows(&self) -> u64 {
        *self.group_starts.last().expect("never empty")
    }

    fn row_group_count(&self) -> usize {
        self.group_starts.len() - 1
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.meta.metadata()
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
        Ok(Some(Projection {
            mask: ProjectionMask::roots(self.meta.parquet_schema(), file_order),
            reorder,
        }))
    }

    fn builder(&self, file: File) -> ParquetRecordBatchReaderBuilder<File> {
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.meta.clone())
            .with_batch_size(READ_BATCH_ROWS)
    }

    /// Reads `rows` (local to row group `group`) with a row selection, so
    /// pages that hold none of them are not fetched.
    fn read_group(
        &self,
        group: usize,
        rows: Range<usize>,
        projection: Option<&Projection>,
    ) -> Result<Vec<RecordBatch>> {
        let group_rows = (self.group_starts[group + 1] - self.group_starts[group]) as usize;
        let mut builder = self
            .builder(File::open(&self.path)?)
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
        let reader = builder.build().map_err(|e| corrupt(self.name, e))?;
        reader
            .map(|batch| {
                let batch = batch.map_err(|e| corrupt(self.name, e))?;
                match projection {
                    Some(p) => p.apply(batch),
                    None => Ok(batch),
                }
            })
            .collect()
    }

    /// Reads rows `first..first + count` of the file.
    fn read_rows(
        &self,
        first: u64,
        count: u64,
        projection: Option<&Projection>,
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
            out.extend(self.read_group(group, lo as usize..hi as usize, projection)?);
            group += 1;
        }
        Ok(out)
    }

    /// Row groups whose `start_ns` statistics overlap the inclusive range. A
    /// group without usable statistics is kept.
    fn groups_overlapping(
        &self,
        column: &str,
        min: Option<i64>,
        max: Option<i64>,
    ) -> Result<Vec<usize>> {
        let idx = self
            .meta
            .schema()
            .index_of(column)
            .map_err(|_| corrupt(self.name, format!("no column `{column}`")))?;
        Ok((0..self.row_group_count())
            .filter(|&g| {
                let stats = self.metadata().row_group(g).column(idx).statistics();
                let Some(Statistics::Int64(s)) = stats else {
                    return true;
                };
                let (Some(lo), Some(hi)) = (s.min_opt(), s.max_opt()) else {
                    return true;
                };
                min.is_none_or(|m| *hi >= m) && max.is_none_or(|m| *lo <= m)
            })
            .collect())
    }

    fn read_groups(
        &self,
        groups: Vec<usize>,
        projection: Option<&Projection>,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        for group in groups {
            let rows = (self.group_starts[group + 1] - self.group_starts[group]) as usize;
            out.extend(self.read_group(group, 0..rows, projection)?);
        }
        Ok(out)
    }
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
/// Whole row groups are returned: the scan prunes by each group's `start_ns`
/// statistics and does not filter rows, so the caller must still test each
/// row against its range.
pub struct Scan {
    reader: Option<ParquetRecordBatchReader>,
    projection: Option<Projection>,
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
}

impl Iterator for Scan {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.reader.as_mut()?.next()?;
        Some(
            batch
                .map_err(|e| corrupt(SPANS_FILE, e))
                .and_then(|b| match &self.projection {
                    Some(p) => p.apply(b),
                    None => Ok(b),
                }),
        )
    }
}

/// Read access to one part.
///
/// Opening loads and checks `traces.idx` in full and the Parquet metadata of
/// both files, including their page indexes. The reader holds no file
/// handles or positions, so it is `Send + Sync` and one value can serve many
/// threads at once.
pub struct PartReader {
    entries: Vec<TraceEntry>,
    spans: Table,
    summary: Table,
}

impl PartReader {
    /// Opens the part in `part_dir`.
    ///
    /// Returns [`Error::Corrupt`], naming the file, if a file is missing, the
    /// index fails its magic, version, length or checksum check, a file's
    /// columns are not the expected ones, or the index's row ranges do not
    /// tile exactly the rows of `spans.parquet`.
    pub fn open(part_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = part_dir.as_ref();
        let index_bytes = fs::read(dir.join(INDEX_FILE)).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => corrupt(INDEX_FILE, "file is missing from the part"),
            _ => Error::Io(e),
        })?;
        let entries = index::decode(&index_bytes)?;
        let spans = Table::open(dir, SPANS_FILE, &span_schema())?;
        let summary = Table::open(dir, SUMMARY_FILE, &summary_schema())?;

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
            entries,
            spans,
            summary,
        })
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
    /// wanted; `None` returns all of them.
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
            .read_rows(first_row, row_count, projection.as_ref())
    }

    /// Reads every row of the row groups whose `start_ns` statistics overlap
    /// the range in `options`, without filtering rows.
    pub fn scan(&self, options: ScanOptions) -> Result<Scan> {
        let names: Option<Vec<&str>> = options
            .projection
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        let projection = self.spans.projection(names.as_deref())?;
        let groups = self.spans.groups_overlapping(
            col::START_NS,
            options.start_ns_min,
            options.start_ns_max,
        )?;
        let read = groups.len();
        let skipped = self.spans.row_group_count() - read;
        let reader = if groups.is_empty() {
            None
        } else {
            let mut builder = self
                .spans
                .builder(File::open(&self.spans.path)?)
                .with_row_groups(groups);
            if let Some(p) = &projection {
                builder = builder.with_projection(p.mask.clone());
            }
            Some(builder.build().map_err(|e| corrupt(SPANS_FILE, e))?)
        };
        Ok(Scan {
            reader,
            projection,
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
            .groups_overlapping("start_ns", start_ns_min, start_ns_max)?;
        self.summary.read_groups(groups, None)
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
