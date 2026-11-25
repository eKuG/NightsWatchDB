//! `parquet` and `parquet-tuned`: payloads.parquet with row-selection reads.
use crate::util::Fields;
use crate::util::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

/// Writer settings. `None` leaves the library default.
#[derive(Clone, Debug)]
pub struct PqOpts {
    pub level: i32,
    pub rg_rows: usize,
    pub rg_bytes: Option<usize>,
    pub page_bytes: Option<usize>,
    pub page_rows: Option<usize>,
    pub dict_limit: Option<usize>,
    pub dict: Option<bool>,
    pub write_batch: Option<usize>,
    /// Bytes of values handed to the writer per write call.
    pub feed: usize,
}

impl PqOpts {
    /// Exactly the bake-off settings: zstd 3, 2,048-row groups, page statistics, library defaults otherwise.
    pub fn plain() -> Self {
        Self { level: 3, rg_rows: 2048, rg_bytes: None, page_bytes: None, page_rows: None, dict_limit: None, dict: None, write_batch: None, feed: 64 << 20 }
    }
    /// Chosen from the smoke experiments (see the README).
    pub fn tuned() -> Self {
        Self { level: 9, rg_rows: 1 << 20, rg_bytes: Some(64 << 20), page_bytes: Some(32 << 20), page_rows: Some(1 << 20), dict_limit: None, dict: Some(true), write_batch: Some(1), feed: 64 << 20 }
    }
    /// `key=value,key=value` overrides, for the tuning experiments.
    pub fn apply(&mut self, s: &str) -> R<()> {
        for kv in s.split(',').filter(|x| !x.is_empty()) {
            let (k, v) = kv.split_once('=').ok_or_else(|| format!("bad tune item {kv}"))?;
            let n = || -> R<usize> { Ok(v.parse::<f64>()? as usize) };
            match k {
                "level" => self.level = v.parse()?,
                "rg_rows" => self.rg_rows = n()?,
                "rg_mb" => self.rg_bytes = Some(n()? << 20),
                "page_kb" => self.page_bytes = Some(n()? << 10),
                "page_rows" => self.page_rows = Some(n()?),
                "dict_kb" => self.dict_limit = Some(n()? << 10),
                "dict" => self.dict = Some(v == "1"),
                "write_batch" => self.write_batch = Some(n()?),
                "feed_mb" => self.feed = n()? << 20,
                _ => return Err(format!("unknown tune key {k}").into()),
            }
        }
        Ok(())
    }
    fn props(&self) -> R<WriterProperties> {
        let mut b = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(self.level)?))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_max_row_group_row_count(Some(self.rg_rows))
            .set_max_row_group_bytes(self.rg_bytes);
        if let Some(x) = self.page_bytes {
            b = b.set_data_page_size_limit(x);
        }
        if let Some(x) = self.page_rows {
            b = b.set_data_page_row_count_limit(x);
        }
        if let Some(x) = self.dict_limit {
            b = b.set_dictionary_page_size_limit(x);
        }
        if let Some(x) = self.dict {
            b = b.set_dictionary_enabled(x);
        }
        if let Some(x) = self.write_batch {
            b = b.set_write_batch_size(x);
        }
        Ok(b.build())
    }
}

pub fn schema() -> SchemaRef {
    let mut v = vec![Field::new("trace_id", DataType::FixedSizeBinary(16), false), Field::new("span_id", DataType::FixedSizeBinary(8), false)];
    v.extend(ATTRS.iter().map(|n| Field::new(*n, DataType::LargeBinary, true)));
    Arc::new(Schema::new(v))
}

pub struct PqBuilder {
    w: Option<ArrowWriter<BufWriter<File>>>,
    path: std::path::PathBuf,
    schema: SchemaRef,
    trace: FixedSizeBinaryBuilder,
    span: FixedSizeBinaryBuilder,
    pay: [LargeBinaryBuilder; 6],
    bytes: usize,
    feed: usize,
    desc: String,
}

impl PqBuilder {
    pub fn new(dir: &Path, o: &PqOpts) -> R<Self> {
        let path = dir.join("payloads.parquet");
        let schema = schema();
        let w = ArrowWriter::try_new(BufWriter::with_capacity(1 << 20, File::create(&path)?), schema.clone(), Some(o.props()?))?;
        Ok(Self {
            w: Some(w),
            path,
            schema,
            trace: FixedSizeBinaryBuilder::new(16),
            span: FixedSizeBinaryBuilder::new(8),
            pay: std::array::from_fn(|_| LargeBinaryBuilder::new()),
            bytes: 0,
            feed: o.feed,
            desc: format!("{o:?}"),
        })
    }
    fn flush(&mut self) -> R<()> {
        let mut cols: Vec<ArrayRef> = vec![Arc::new(self.trace.finish()), Arc::new(self.span.finish())];
        cols.extend(self.pay.iter_mut().map(|b| Arc::new(b.finish()) as ArrayRef));
        self.bytes = 0;
        let b = RecordBatch::try_new(self.schema.clone(), cols)?;
        if b.num_rows() > 0 {
            self.w.as_mut().ok_or("writer closed")?.write(&b)?;
        }
        Ok(())
    }
}

impl Builder for PqBuilder {
    fn begin_trace(&mut self, _: &[u8; 16], _: u64) -> R<()> {
        Ok(())
    }
    fn span(&mut self, s: &SpanRec) -> R<()> {
        self.trace.append_value(s.trace)?;
        self.span.append_value(s.span)?;
        for (b, v) in self.pay.iter_mut().zip(&s.vals) {
            b.append_option(*v);
            self.bytes += v.map_or(0, |x| x.len());
        }
        self.bytes += 24;
        if self.bytes >= self.feed {
            self.flush()?;
        }
        Ok(())
    }
    fn end_trace(&mut self) -> R<()> {
        Ok(())
    }
    fn finish(&mut self) -> R<(Vec<(String, u64)>, Fields)> {
        self.flush()?;
        self.w.take().ok_or("writer closed")?.close()?;
        Ok((vec![("payloads.parquet".into(), std::fs::metadata(&self.path)?.len())], vec![("settings", q(&self.desc))]))
    }
}

type Reader = ParquetRecordBatchReaderBuilder<File>;

pub struct PqStore {
    file: File,
    meta: ArrowReaderMetadata,
    rg_rows: Vec<usize>,
    traces: HashMap<[u8; 16], (u64, u64)>,
    rows: u64,
}

impl PqStore {
    /// Opens the file, reads the footer and page index, and scans the trace_id column once
    /// to learn where each trace's rows are.
    pub fn open(dir: &Path) -> R<Self> {
        let file = File::open(dir.join("payloads.parquet"))?;
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required))?;
        let md = meta.metadata().clone();
        let rg_rows: Vec<usize> = (0..md.num_row_groups()).map(|i| md.row_group(i).num_rows() as usize).collect();
        let mut s = Self { file, meta, rg_rows, traces: HashMap::new(), rows: md.file_metadata().num_rows() as u64 };
        let (mut cur, mut row): (Option<[u8; 16]>, u64) = (None, 0);
        for rg in 0..s.rg_rows.len() {
            for b in s.reader(rg, &[0], 8192)? {
                let b = b?;
                let ids = b.column(0).as_fixed_size_binary();
                for i in 0..b.num_rows() {
                    let id: [u8; 16] = ids.value(i).try_into()?;
                    if cur != Some(id) {
                        if s.traces.insert(id, (row, 0)).is_some() {
                            return Err("trace rows are not contiguous".into());
                        }
                        cur = Some(id);
                    }
                    s.traces.get_mut(&id).ok_or("trace")?.1 += 1;
                    row += 1;
                }
            }
        }
        Ok(s)
    }

    fn reader(&self, rg: usize, cols: &[usize], batch: usize) -> R<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
        let mask = ProjectionMask::leaves(self.meta.parquet_schema(), cols.iter().copied());
        Ok(Reader::new_with_metadata(self.file.try_clone()?, self.meta.clone()).with_row_groups(vec![rg]).with_projection(mask).with_batch_size(batch).build()?)
    }

    fn emit(b: &RecordBatch, first_col: usize, v: &mut dyn Visit) {
        for i in 0..b.num_rows() {
            for c in first_col..first_col + 6 {
                let a = b.column(c).as_binary::<i64>();
                if a.is_valid(i) {
                    v.value(a.value(i));
                }
            }
        }
    }

    /// Reads the global row range with the page index, skipping pages that hold no selected row.
    fn read_range(&self, from: u64, to: u64, v: &mut dyn Visit) -> R<()> {
        let mut start = 0u64;
        for (rg, &len) in self.rg_rows.iter().enumerate() {
            let (a, b) = (from.max(start), to.min(start + len as u64));
            if a < b {
                let sel = RowSelection::from_consecutive_ranges(std::iter::once((a - start) as usize..(b - start) as usize), len);
                let mask = ProjectionMask::leaves(self.meta.parquet_schema(), 2..8);
                let r = Reader::new_with_metadata(self.file.try_clone()?, self.meta.clone()).with_row_groups(vec![rg]).with_projection(mask).with_batch_size(16).with_row_selection(sel).build()?;
                for batch in r {
                    Self::emit(&batch?, 0, v);
                }
            }
            start += len as u64;
        }
        Ok(())
    }
}

impl Store for PqStore {
    fn rows(&self) -> u64 {
        self.rows
    }
    fn read_trace(&mut self, id: &[u8; 16], v: &mut dyn Visit) -> R<bool> {
        match self.traces.get(id) {
            None => Ok(false),
            Some(&(first, n)) => self.read_range(first, first + n, v).map(|_| true),
        }
    }
    fn read_span(&mut self, row: u64, v: &mut dyn Visit) -> R<()> {
        self.read_range(row, row + 1, v)
    }
    fn scan(&mut self, v: &mut dyn Visit) -> R<()> {
        let mut cur: Option<[u8; 16]> = None;
        for rg in 0..self.rg_rows.len() {
            for b in self.reader(rg, &[0, 2, 3, 4, 5, 6, 7], 16)? {
                let b = b?;
                let ids = b.column(0).as_fixed_size_binary();
                for i in 0..b.num_rows() {
                    let id: [u8; 16] = ids.value(i).try_into()?;
                    if cur != Some(id) {
                        v.trace(&id);
                        cur = Some(id);
                    }
                    for c in 1..7 {
                        let a = b.column(c).as_binary::<i64>();
                        if a.is_valid(i) {
                            v.value(a.value(i));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
