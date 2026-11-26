//! The two text files of a part, as the writer makes them.
//!
//! `text.idx` is an index (`nwdb_textindex`) with one document per piece of the
//! part's piece store; `text.rows` maps every piece id to the ascending list of
//! rows whose ropes refer to it. Both are produced from the one place every
//! piece and every rope of a part passes, the part writer, so a part made by a
//! merge has them exactly as a flushed part does.

use crate::index::FileChecksum;
use crate::writer::{Digest, SharedFile};
use crate::{TEXT_INDEX_FILE, TEXT_ROWS_FILE};
use nwdb_core::{Error, Result};
use nwdb_payload::json_search_text;
use nwdb_textindex::{
    IdListMeta, IdListWriter, IndexBuilder, IndexMeta, IndexOptions, IndexReader,
};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The default for [`WriterOptions::text_index_budget_bytes`]: 32 MiB.
///
/// [`WriterOptions::text_index_budget_bytes`]: crate::WriterOptions::text_index_budget_bytes
pub const DEFAULT_TEXT_INDEX_BUDGET_BYTES: usize = 32 << 20;

/// The default for [`WriterOptions::row_list_budget_bytes`]: 32 MiB.
///
/// [`WriterOptions::row_list_budget_bytes`]: crate::WriterOptions::row_list_budget_bytes
pub const DEFAULT_ROW_LIST_BUDGET_BYTES: usize = 32 << 20;

/// Bytes of one pair as held in memory and in a run.
const PAIR_BYTES: usize = 8;

/// What the writer learned while finishing the two files.
pub(crate) struct TextFinished {
    pub index: FileChecksum,
    pub rows: FileChecksum,
    /// The checksums in the footers of the two files, which cover what the
    /// files' own checksums chain to.
    pub index_footer: u32,
    pub rows_footer: u32,
}

/// Builds `text.idx` and `text.rows` while a part is written.
pub(crate) struct TextWriter {
    builder: IndexBuilder<SharedFile>,
    index_file: Arc<File>,
    index_digest: Arc<Digest>,
    rows_path: PathBuf,
    pairs: RowPairs,
}

impl TextWriter {
    pub fn create(dir: &Path, index_budget: usize, row_list_budget: usize) -> Result<Self> {
        let index_file = Arc::new(File::create_new(dir.join(TEXT_INDEX_FILE))?);
        let index_digest = Arc::new(Digest::default());
        let sink = SharedFile::new(index_file.clone(), index_digest.clone());
        Ok(Self {
            builder: IndexBuilder::new(
                IndexOptions {
                    segment_budget_bytes: index_budget,
                },
                sink,
            ),
            index_file,
            index_digest,
            rows_path: dir.join(TEXT_ROWS_FILE),
            pairs: RowPairs::new(dir, row_list_budget),
        })
    }

    /// Piece `id` was stored and holds `bytes`. Ids arrive in ascending order,
    /// which is what the index builder needs. What is indexed is the piece as
    /// [`json_search_text`] gives it, which is also what a phrase is confirmed
    /// against.
    pub fn add_piece(&mut self, id: u32, bytes: &[u8]) -> Result<()> {
        self.builder.add(id, &json_search_text(bytes))
    }

    /// Records that row `row` refers to the pieces in `pieces` (any order,
    /// repeats allowed: a row lists a piece once).
    pub fn add_row(&mut self, row: u64, pieces: &mut Vec<u32>) -> Result<()> {
        if pieces.is_empty() {
            return Ok(());
        }
        let row = u32::try_from(row).map_err(|_| {
            Error::invalid_argument("a part holds at most 4,294,967,295 rows when it is indexed")
        })?;
        pieces.sort_unstable();
        pieces.dedup();
        self.pairs.add(row, pieces)
    }

    /// Writes both files; `pieces` is the number of pieces the store holds.
    pub fn finish(self, pieces: u64) -> Result<TextFinished> {
        let meta: IndexMeta = self.builder.finish()?;
        self.index_file.sync_all()?;
        if meta.docs != pieces {
            return Err(Error::other(format!(
                "the text index has {} documents but the piece store has {pieces} pieces",
                meta.docs
            )));
        }
        let rows_file = Arc::new(File::create_new(&self.rows_path)?);
        let digest = Arc::new(Digest::default());
        let out = SharedFile::new(rows_file.clone(), digest.clone());
        let rows_meta = self.pairs.finish(pieces, out)?;
        rows_file.sync_all()?;
        let index_footer = IndexReader::open(self.index_file.try_clone()?)?.footer_crc();
        Ok(TextFinished {
            index: self.index_digest.get(),
            rows: digest.get(),
            index_footer,
            rows_footer: rows_meta.footer_crc,
        })
    }
}

/// `(piece, row)` pairs, collected in bounded memory: a full buffer is sorted
/// and written to a run file, and the runs are merged when the part ends. A
/// pair is `piece << 32 | row`, so the natural order of the numbers is the
/// order the lists need.
struct RowPairs {
    dir: PathBuf,
    cap: usize,
    budget: usize,
    buf: Vec<u64>,
    runs: Vec<PathBuf>,
}

impl RowPairs {
    fn new(dir: &Path, budget_bytes: usize) -> Self {
        Self {
            dir: dir.to_path_buf(),
            cap: (budget_bytes / PAIR_BYTES).max(1),
            budget: budget_bytes,
            buf: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// All pairs of one row go in together, so a row's pairs are never split
    /// between runs.
    fn add(&mut self, row: u32, pieces: &[u32]) -> Result<()> {
        if !self.buf.is_empty() && self.buf.len() + pieces.len() > self.cap {
            self.spill()?;
        }
        // Grow by doubling but never past the cap, so the vector's spare room
        // cannot double the budget.
        let need = self.buf.len() + pieces.len();
        if self.buf.capacity() < need {
            let want = (self.buf.capacity() * 2).max(1024).min(self.cap).max(need);
            self.buf.reserve_exact(want - self.buf.len());
        }
        self.buf.extend(
            pieces
                .iter()
                .map(|&p| (u64::from(p) << 32) | u64::from(row)),
        );
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        self.buf.sort_unstable();
        self.buf.dedup();
        let path = self
            .dir
            .join(format!("{TEXT_ROWS_FILE}.run{}", self.runs.len()));
        let mut out = BufWriter::with_capacity(1 << 16, File::create_new(&path)?);
        self.runs.push(path);
        for pair in &self.buf {
            out.write_all(&pair.to_le_bytes())?;
        }
        out.flush()?;
        self.buf.clear();
        // A run is only read back by the merge in this process, so it needs
        // no sync.
        Ok(())
    }

    fn finish(mut self, pieces: u64, out: SharedFile) -> Result<IdListMeta> {
        self.buf.sort_unstable();
        self.buf.dedup();
        // One read buffer per run, sharing the budget the pair buffer had.
        let per_run = (self.budget / (self.runs.len() + 1)).clamp(4 << 10, 64 << 10);
        let mut sources: Vec<Source> =
            vec![Source::Memory(std::mem::take(&mut self.buf).into_iter())];
        for path in &self.runs {
            sources.push(Source::Run(BufReader::with_capacity(
                per_run,
                File::open(path)?,
            )));
        }
        let mut lists = Lists {
            writer: IdListWriter::new(out),
            next_key: 0,
            pieces,
        };
        let mut heap = BinaryHeap::new();
        for (i, s) in sources.iter_mut().enumerate() {
            if let Some(v) = s.next()? {
                heap.push(Reverse((v, i)));
            }
        }
        let (mut current, mut list, mut last) = (None::<u32>, Vec::<u32>::new(), None::<u64>);
        while let Some(Reverse((v, i))) = heap.pop() {
            if let Some(n) = sources[i].next()? {
                heap.push(Reverse((n, i)));
            }
            if last == Some(v) {
                continue;
            }
            last = Some(v);
            let (piece, row) = ((v >> 32) as u32, v as u32);
            if current != Some(piece) {
                if let Some(c) = current {
                    lists.push(c, &list)?;
                }
                current = Some(piece);
                list.clear();
            }
            list.push(row);
        }
        if let Some(c) = current {
            lists.push(c, &list)?;
        }
        let meta = lists.finish()?;
        drop(sources);
        for path in &self.runs {
            std::fs::remove_file(path)?;
        }
        Ok(meta)
    }
}

enum Source {
    Memory(std::vec::IntoIter<u64>),
    Run(BufReader<File>),
}

impl Source {
    fn next(&mut self) -> Result<Option<u64>> {
        match self {
            Source::Memory(it) => Ok(it.next()),
            Source::Run(r) => {
                let mut b = [0u8; PAIR_BYTES];
                match r.read_exact(&mut b) {
                    Ok(()) => Ok(Some(u64::from_le_bytes(b))),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }
}

/// Pushes the lists of the pieces in key order, filling the pieces that no row
/// refers to with empty lists, so that keys are dense.
struct Lists {
    writer: IdListWriter<SharedFile>,
    next_key: u64,
    pieces: u64,
}

impl Lists {
    fn push(&mut self, piece: u32, rows: &[u32]) -> Result<()> {
        if u64::from(piece) >= self.pieces {
            return Err(Error::other(format!(
                "a row refers to piece {piece} but the store holds {} pieces",
                self.pieces
            )));
        }
        while self.next_key < u64::from(piece) {
            self.writer.push(&[])?;
            self.next_key += 1;
        }
        self.writer.push(rows)?;
        self.next_key += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<IdListMeta> {
        while self.next_key < self.pieces {
            self.writer.push(&[])?;
            self.next_key += 1;
        }
        self.writer.finish()
    }
}
