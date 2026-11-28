//! The annotation part: one file, `annotations.nwa`, of zstd blocks of whole
//! records, a block table and a footer. The format is described in the crate's
//! `README.md`.

use super::codec::{Cursor, decode_record, encode_record};
use super::{
    ANNOTATION_FILE, ANNOTATION_FORMAT_VERSION, Annotation, check_annotation_format_version,
};
use crate::writer::{create_dir_all_synced, parent_of, sync_after_rename, sync_dir};
use nwdb_core::{Error, PartKind, PartMeta, Result, TraceId, part_dir_name};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 4] = b"NWAN";
const FOOTER_BYTES: usize = 80;
const ENTRY_BYTES: usize = 36;
/// A block is closed once it holds this many uncompressed bytes.
const BLOCK_TARGET_BYTES: usize = 16 * 1024;
/// The most uncompressed bytes a block may declare.
pub(crate) const MAX_ULEN: u32 = 32 * 1024;
/// The most compressed bytes a block may declare: what zstd can make of
/// [`MAX_ULEN`] bytes, rounded up.
const MAX_CLEN: u32 = 2 * MAX_ULEN;
const ZSTD_LEVEL: i32 = 3;

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

fn i64_at(b: &[u8], at: usize) -> i64 {
    u64_at(b, at) as i64
}

/// Writes one annotation part. Records must come in strictly increasing
/// identity order.
pub struct AnnotationWriter {
    tmp_dir: PathBuf,
    final_dir: PathBuf,
    part_id: u64,
    window_start_ns: i64,
    window_ns: i64,
    file: File,
    /// Bytes of compressed blocks written so far.
    written: u64,
    /// The open block: whole records, uncompressed.
    block: Vec<u8>,
    block_first: Option<TraceId>,
    /// 36 bytes for each closed block.
    table: Vec<u8>,
    /// Identity of the last record appended, for the order check.
    last: Option<Annotation>,
    records: u64,
    traces: u64,
    min_time_ns: i64,
    max_time_ns: i64,
    committed: bool,
}

impl AnnotationWriter {
    /// Starts a part in `<parts_dir>/<part_dir_name(part_id)>.tmp`. Creates
    /// `parts_dir` if it does not exist. [`Error::InvalidArgument`] if the
    /// final or the temporary directory already exists or `window_ns` is not
    /// positive.
    pub fn create(
        parts_dir: impl AsRef<Path>,
        part_id: u64,
        window_start_ns: i64,
        window_ns: i64,
    ) -> Result<Self> {
        if window_ns <= 0 {
            return Err(Error::invalid_argument(format!(
                "window_ns must be positive, got {window_ns}"
            )));
        }
        let parts_dir = parts_dir.as_ref();
        let name = part_dir_name(part_id);
        let final_dir = parts_dir.join(&name);
        let tmp_dir = parts_dir.join(format!("{name}.tmp"));
        for dir in [&final_dir, &tmp_dir] {
            if dir.exists() {
                return Err(Error::invalid_argument(format!(
                    "part directory {} already exists",
                    dir.display()
                )));
            }
        }
        create_dir_all_synced(parts_dir)?;
        fs::create_dir(&tmp_dir)?;
        let file = match File::create_new(tmp_dir.join(ANNOTATION_FILE)) {
            Ok(f) => f,
            Err(e) => {
                let _ = fs::remove_dir_all(&tmp_dir);
                return Err(e.into());
            }
        };
        Ok(Self {
            tmp_dir,
            final_dir,
            part_id,
            window_start_ns,
            window_ns,
            file,
            written: 0,
            block: Vec::new(),
            block_first: None,
            table: Vec::new(),
            last: None,
            records: 0,
            traces: 0,
            min_time_ns: i64::MAX,
            max_time_ns: i64::MIN,
            committed: false,
        })
    }

    /// Adds a record. [`Error::InvalidArgument`] unless the record is valid
    /// and its identity is strictly greater than the last one's.
    pub fn append(&mut self, a: &Annotation) -> Result<()> {
        a.validate()
            .map_err(|why| Error::invalid_argument(format!("annotation: {why}")))?;
        if let Some(last) = &self.last
            && last.identity_cmp(a) != Ordering::Less
        {
            return Err(Error::invalid_argument(
                "annotation identity is not greater than the previous one",
            ));
        }
        if self.block.is_empty() {
            self.block_first = Some(a.trace_id);
        }
        encode_record(a, true, &mut self.block);
        if self.last.as_ref().is_none_or(|l| l.trace_id != a.trace_id) {
            self.traces += 1;
        }
        self.records += 1;
        self.min_time_ns = self.min_time_ns.min(a.time_ns);
        self.max_time_ns = self.max_time_ns.max(a.time_ns);
        self.last = Some(identity_of(a));
        if self.block.len() >= BLOCK_TARGET_BYTES {
            self.close_block()?;
        }
        Ok(())
    }

    /// Blocks closed so far.
    pub(crate) fn blocks_written(&self) -> u64 {
        (self.table.len() / ENTRY_BYTES) as u64
    }

    fn close_block(&mut self) -> Result<()> {
        let Some(first) = self.block_first.take() else {
            return Ok(());
        };
        let packed = zstd::bulk::compress(&self.block, ZSTD_LEVEL).map_err(Error::other)?;
        (&self.file).write_all(&packed)?;
        self.table.extend_from_slice(first.as_bytes());
        self.table.extend_from_slice(&self.written.to_le_bytes());
        self.table
            .extend_from_slice(&(packed.len() as u32).to_le_bytes());
        self.table
            .extend_from_slice(&(self.block.len() as u32).to_le_bytes());
        self.table
            .extend_from_slice(&crc32c::crc32c(&packed).to_le_bytes());
        self.written += packed.len() as u64;
        self.block.clear();
        Ok(())
    }

    /// Writes the table and the footer, syncs, and renames the part into
    /// place. [`Error::InvalidArgument`] if no record was appended. A writer
    /// dropped without `finish` removes its temporary directory. If a step
    /// after the rename fails the part may or may not be durable and the
    /// result is [`Error::Fatal`]: do not reuse the part id and restart.
    pub fn finish(mut self, level: u8) -> Result<PartMeta> {
        if self.records == 0 {
            return Err(Error::invalid_argument(
                "a part must contain at least one record",
            ));
        }
        self.close_block()?;
        let blocks = self.blocks_written();
        let mut footer = Vec::with_capacity(FOOTER_BYTES);
        footer.extend_from_slice(MAGIC);
        footer.extend_from_slice(&ANNOTATION_FORMAT_VERSION.to_le_bytes());
        footer.extend_from_slice(&self.part_id.to_le_bytes());
        footer.extend_from_slice(&self.window_start_ns.to_le_bytes());
        footer.extend_from_slice(&self.window_ns.to_le_bytes());
        footer.extend_from_slice(&self.records.to_le_bytes());
        footer.extend_from_slice(&self.traces.to_le_bytes());
        footer.extend_from_slice(&blocks.to_le_bytes());
        footer.extend_from_slice(&self.min_time_ns.to_le_bytes());
        footer.extend_from_slice(&self.max_time_ns.to_le_bytes());
        footer.extend_from_slice(&crc32c::crc32c(&self.table).to_le_bytes());
        let crc = crc32c::crc32c(&footer);
        footer.extend_from_slice(&crc.to_le_bytes());
        (&self.file).write_all(&self.table)?;
        (&self.file).write_all(&footer)?;
        self.file.sync_all()?;
        let len = self.written + self.table.len() as u64 + FOOTER_BYTES as u64;
        sync_dir(&self.tmp_dir)?;
        fs::rename(&self.tmp_dir, &self.final_dir)?;
        self.committed = true;
        // The rename has happened: from here on the part may exist, so a
        // failure must not look like one that left nothing behind.
        sync_after_rename(parent_of(&self.final_dir)).map_err(|e| {
            Error::fatal(format!(
                "part {} was renamed into place but syncing {} failed ({e}); it may or may not \
                 be durable: do not reuse the part id and restart",
                self.part_id,
                parent_of(&self.final_dir).display()
            ))
        })?;
        Ok(PartMeta {
            part_id: self.part_id,
            level,
            window_start_ns: self.window_start_ns,
            window_ns: self.window_ns,
            rows: self.records,
            traces: self.traces,
            min_start_ns: self.min_time_ns,
            max_end_ns: self.max_time_ns,
            files: BTreeMap::from([(ANNOTATION_FILE.to_string(), len)]),
            created_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            format_version: ANNOTATION_FORMAT_VERSION,
            payload: Default::default(),
            kind: PartKind::Annotations,
        })
    }
}

impl Drop for AnnotationWriter {
    fn drop(&mut self) {
        if !self.committed {
            // Best effort: the directory may already be gone.
            let _ = fs::remove_dir_all(&self.tmp_dir);
        }
    }
}

/// The identity fields of `a` and nothing else.
fn identity_of(a: &Annotation) -> Annotation {
    Annotation {
        trace_id: a.trace_id,
        span_id: a.span_id,
        name: a.name.clone(),
        source: a.source.clone(),
        time_ns: 0,
        value: None,
        label: None,
        comment: None,
        delete: false,
        arrival: 0,
    }
}

struct Entry {
    first: TraceId,
    offset: u64,
    clen: u32,
    ulen: u32,
    crc: u32,
}

/// Reads one annotation part. Holds the open file, the footer's values in the
/// [`PartMeta`] and the block table, 36 bytes per block; nothing per record
/// or per trace.
pub struct AnnotationReader {
    file: File,
    meta: PartMeta,
    table: Vec<u8>,
    blocks_read: AtomicU64,
}

fn bad(msg: impl std::fmt::Display) -> Error {
    Error::corrupt_in(ANNOTATION_FILE, msg)
}

impl AnnotationReader {
    /// Opens the part in `part_dir` and checks, in this order: the magic, the
    /// format version, the footer's checksum, that the part id and window
    /// are those of `meta`, the length of the file, the table's checksum,
    /// that blocks are contiguous from offset 0, that first keys do not
    /// decrease, that every block's length is within its ceiling, that there
    /// is at least one block, and that the footer's counts and time range
    /// are those of `meta`. A missing file is [`Error::Corrupt`].
    pub fn open(part_dir: impl AsRef<Path>, meta: &PartMeta) -> Result<Self> {
        let file = File::open(part_dir.as_ref().join(ANNOTATION_FILE))
            .map_err(|e| Error::part_file_open(ANNOTATION_FILE, e))?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(bad(format!(
                "file has {len} bytes, fewer than the {FOOTER_BYTES} of the footer"
            )));
        }
        let mut f = [0u8; FOOTER_BYTES];
        file.read_exact_at(&mut f, len - FOOTER_BYTES as u64)?;
        if &f[0..4] != MAGIC {
            return Err(bad("bad magic: not an annotation part"));
        }
        check_annotation_format_version(u32_at(&f, 4))?;
        if crc32c::crc32c(&f[..76]) != u32_at(&f, 76) {
            return Err(bad("footer checksum mismatch"));
        }
        if u64_at(&f, 8) != meta.part_id
            || i64_at(&f, 16) != meta.window_start_ns
            || i64_at(&f, 24) != meta.window_ns
        {
            return Err(bad(format!(
                "the footer is of part {} (window {} + {}), not of part {} (window {} + {})",
                u64_at(&f, 8),
                i64_at(&f, 16),
                i64_at(&f, 24),
                meta.part_id,
                meta.window_start_ns,
                meta.window_ns
            )));
        }
        let blocks = u64_at(&f, 48);
        let table_bytes = blocks
            .checked_mul(ENTRY_BYTES as u64)
            .filter(|t| t.saturating_add(FOOTER_BYTES as u64) <= len)
            .ok_or_else(|| bad(format!("{blocks} blocks do not fit in {len} bytes")))?;
        let mut table = vec![0u8; table_bytes as usize];
        file.read_exact_at(&mut table, len - FOOTER_BYTES as u64 - table_bytes)?;
        let r = Self {
            file,
            meta: meta.clone(),
            table,
            blocks_read: AtomicU64::new(0),
        };
        let clen_sum: u64 = (0..blocks as usize)
            .map(|i| u64::from(r.entry(i).clen))
            .sum();
        if clen_sum + table_bytes + FOOTER_BYTES as u64 != len {
            return Err(bad(format!(
                "file has {len} bytes but its blocks, table and footer add up to {}",
                clen_sum + table_bytes + FOOTER_BYTES as u64
            )));
        }
        if meta.files.get(ANNOTATION_FILE) != Some(&len) {
            return Err(bad(format!(
                "file has {len} bytes but the part's metadata says {:?}",
                meta.files.get(ANNOTATION_FILE)
            )));
        }
        if crc32c::crc32c(&r.table) != u32_at(&f, 72) {
            return Err(bad("block table checksum mismatch"));
        }
        let mut next_offset = 0u64;
        let mut prev_first: Option<TraceId> = None;
        for i in 0..blocks as usize {
            let e = r.entry(i);
            if e.offset != next_offset {
                return Err(bad(format!(
                    "block {i} starts at {} but the one before it ends at {next_offset}",
                    e.offset
                )));
            }
            next_offset += u64::from(e.clen);
            if prev_first.is_some_and(|p| p > e.first) {
                return Err(bad(format!(
                    "block {i} has a smaller first key than block {}",
                    i - 1
                )));
            }
            prev_first = Some(e.first);
        }
        for i in 0..blocks as usize {
            let e = r.entry(i);
            if e.ulen == 0 || e.ulen > MAX_ULEN || e.clen == 0 || e.clen > MAX_CLEN {
                return Err(bad(format!(
                    "block {i} declares {} compressed and {} uncompressed bytes",
                    e.clen, e.ulen
                )));
            }
        }
        if blocks == 0 {
            return Err(bad("the part has no blocks"));
        }
        if u64_at(&f, 32) != meta.rows
            || u64_at(&f, 40) != meta.traces
            || i64_at(&f, 56) != meta.min_start_ns
            || i64_at(&f, 64) != meta.max_end_ns
        {
            return Err(bad(
                "the footer's record count, trace count or time range differs from the part's metadata",
            ));
        }
        Ok(r)
    }

    fn blocks(&self) -> usize {
        self.table.len() / ENTRY_BYTES
    }

    fn entry(&self, i: usize) -> Entry {
        let b = &self.table[i * ENTRY_BYTES..(i + 1) * ENTRY_BYTES];
        Entry {
            first: TraceId(b[0..16].try_into().expect("16 bytes")),
            offset: u64_at(b, 16),
            clen: u32_at(b, 24),
            ulen: u32_at(b, 28),
            crc: u32_at(b, 32),
        }
    }

    fn first_key(&self, i: usize) -> TraceId {
        TraceId(
            self.table[i * ENTRY_BYTES..i * ENTRY_BYTES + 16]
                .try_into()
                .expect("16 bytes"),
        )
    }

    /// The metadata the part was opened with.
    pub fn meta(&self) -> &PartMeta {
        &self.meta
    }

    /// Blocks read since open.
    pub fn blocks_read(&self) -> u64 {
        self.blocks_read.load(AtomicOrdering::Relaxed)
    }

    /// Reads every block and checks what the open checks could not: the
    /// order of the records across the file, and the record count, trace
    /// count and time range against the metadata.
    pub fn verify(&self) -> Result<()> {
        let mut cursor = self.cursor(None);
        let (mut records, mut traces) = (0u64, 0u64);
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        let mut last_trace = None;
        while let Some(a) = cursor.next()? {
            records += 1;
            if last_trace != Some(a.trace_id) {
                traces += 1;
                last_trace = Some(a.trace_id);
            }
            min = min.min(a.time_ns);
            max = max.max(a.time_ns);
        }
        let m = &self.meta;
        if records != m.rows || traces != m.traces || min != m.min_start_ns || max != m.max_end_ns {
            return Err(bad(format!(
                "the blocks hold {records} records of {traces} traces with times {min} to {max}, \
                 the part's metadata says {} of {} with {} to {}",
                m.rows, m.traces, m.min_start_ns, m.max_end_ns
            )));
        }
        Ok(())
    }

    /// The records in identity order: all of them, or those of one trace. A
    /// lookup reads only the blocks that can hold the trace.
    pub fn cursor(&self, trace: Option<TraceId>) -> AnnotationCursor<'_> {
        let blocks = self.blocks();
        let (start, end) = match trace {
            None => (0, blocks),
            Some(t) => {
                // The first block whose successor starts at or after `t`:
                // the blocks before it end before `t`.
                let (mut lo, mut hi) = (0, blocks);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if mid + 1 == blocks || self.first_key(mid + 1) >= t {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                let mut end = lo;
                while end < blocks && self.first_key(end) <= t {
                    end += 1;
                }
                (lo, end)
            }
        };
        AnnotationCursor {
            reader: self,
            trace,
            next_block: start,
            end_block: end,
            comp: Vec::new(),
            raw: Vec::new(),
            pos: 0,
            prev: None,
            done: false,
        }
    }
}

/// Reads records in identity order; holds one compressed and one
/// decompressed block.
pub struct AnnotationCursor<'a> {
    reader: &'a AnnotationReader,
    trace: Option<TraceId>,
    next_block: usize,
    end_block: usize,
    comp: Vec<u8>,
    raw: Vec<u8>,
    pos: usize,
    prev: Option<Annotation>,
    done: bool,
}

impl AnnotationCursor<'_> {
    /// The next record, or `None` at the end. After an error the cursor is
    /// finished.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Annotation>> {
        if self.done {
            return Ok(None);
        }
        let r = self.advance();
        if !matches!(r, Ok(Some(_))) {
            self.done = true;
        }
        r
    }

    fn load(&mut self, i: usize) -> Result<()> {
        let e = self.reader.entry(i);
        self.comp.clear();
        self.comp.resize(e.clen as usize, 0);
        self.reader
            .file
            .read_exact_at(&mut self.comp, e.offset)
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    bad(format!("block {i} ends past the end of the file"))
                }
                _ => Error::Io(err),
            })?;
        self.reader
            .blocks_read
            .fetch_add(1, AtomicOrdering::Relaxed);
        if crc32c::crc32c(&self.comp) != e.crc {
            return Err(bad(format!("block {i}: checksum mismatch")));
        }
        // The capacity is the declared length, never what the frame says.
        let raw = zstd::bulk::decompress(&self.comp, e.ulen as usize)
            .map_err(|err| bad(format!("block {i} does not decompress ({err})")))?;
        if raw.len() != e.ulen as usize {
            return Err(bad(format!(
                "block {i} decompresses to {} bytes, not {}",
                raw.len(),
                e.ulen
            )));
        }
        self.raw = raw;
        self.pos = 0;
        Ok(())
    }

    fn advance(&mut self) -> Result<Option<Annotation>> {
        loop {
            if self.pos == self.raw.len() {
                if self.next_block >= self.end_block {
                    return Ok(None);
                }
                self.load(self.next_block)?;
                self.next_block += 1;
            }
            let block_start = self.pos == 0;
            let mut cur = Cursor(&self.raw[self.pos..]);
            let a = decode_record(&mut cur, true)?;
            self.pos = self.raw.len() - cur.0.len();
            if block_start && a.trace_id != self.reader.first_key(self.next_block - 1) {
                return Err(bad(format!(
                    "block {} does not start with the trace id its table entry names",
                    self.next_block - 1
                )));
            }
            if self
                .prev
                .as_ref()
                .is_some_and(|p| p.identity_cmp(&a) != Ordering::Less)
            {
                return Err(bad("records are not in strictly increasing identity order"));
            }
            self.prev = Some(identity_of(&a));
            if let Some(t) = self.trace {
                match a.trace_id.cmp(&t) {
                    Ordering::Less => continue,
                    Ordering::Greater => return Ok(None),
                    Ordering::Equal => {}
                }
            }
            return Ok(Some(a));
        }
    }
}

#[cfg(test)]
#[path = "part_tests.rs"]
mod tests;
