//! Full-text search over one part.
//!
//! A part's text lives in its pieces. `text.idx` indexes each piece as one
//! document and `text.rows` says which rows refer to each piece. A row's text
//! is the union of its pieces, so a word matches a row when any one of the
//! row's pieces holds it, and a phrase matches when one piece holds it whole.
//! The boolean operators combine sets of rows, never sets of pieces: two words
//! of an `AND` may be in different messages of one span.
//!
//! What is not searched: text that stayed inline in the row (cuts under
//! [`WriterOptions::min_piece_bytes`](crate::WriterOptions::min_piece_bytes),
//! values the splitter does not cut, and anything stored after the part held
//! [`WriterOptions::max_pieces`](crate::WriterOptions::max_pieces) pieces).

use crate::index::{FileChecksum, FileChecksums};
use crate::reader::PartReader;
use crate::{TEXT_INDEX_FILE, TEXT_ROWS_FILE};
use nwdb_core::{Error, Result};
use nwdb_textindex::{
    IdListReader, IndexReader, Leaf, Query, contains_token, evaluate, phrase_candidates,
    phrase_matches,
};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

/// How much work and memory one search of one part may use. A query over the
/// limit is refused with [`Error::InvalidArgument`] saying which limit, and
/// nothing else is affected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchLimits {
    /// How many distinct terms a prefix such as `err*` may expand to. Default
    /// 1,024.
    pub max_prefix_terms: usize,
    /// How many bytes of piece text a search may read to confirm phrases and
    /// words longer than 64 bytes, which the index alone cannot decide. A piece
    /// counts once for every leaf that reads it. Default 256 MiB.
    pub max_confirm_bytes: u64,
    /// How many rows a single leaf of the query (a word, a prefix, a phrase) may
    /// produce, and, for a query with a negation, how many rows the part may
    /// have. Default 10 million.
    pub max_leaf_rows: usize,
}

impl Default for SearchLimits {
    fn default() -> Self {
        Self {
            max_prefix_terms: 1_024,
            max_confirm_bytes: 256 << 20,
            max_leaf_rows: 10_000_000,
        }
    }
}

/// Counts describing the text files of a part.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TextStats {
    /// Size of `text.idx`.
    pub index_bytes: u64,
    /// Size of `text.rows`.
    pub row_list_bytes: u64,
    /// Bytes of piece text the index tokenised: the text it covers.
    pub text_bytes: u64,
    /// Pieces indexed.
    pub pieces: u64,
    /// Distinct terms, summed over the index's segments.
    pub terms: u64,
    /// Row numbers in `text.rows`, over all lists.
    pub row_list_ids: u64,
}

/// The two text files of an open part.
pub(crate) struct TextFiles {
    index: IndexReader<File>,
    rows: IdListReader<File>,
    index_file: File,
    rows_file: File,
    checksums: (FileChecksum, FileChecksum),
}

fn corrupt(file: &str, msg: impl std::fmt::Display) -> Error {
    Error::corrupt(format!("{file}: {msg}"))
}

/// A library error that names no file gets the file's name; a short read of a
/// file whose length was checked means the file changed under us. A key the
/// file does not have is a file that does not match the part, not a missing
/// resource.
fn named(file: &str, e: Error) -> Error {
    match e {
        // An error that already names the file is left as it is.
        Error::Corrupt(m) if m.starts_with(&format!("{file}: ")) => Error::Corrupt(m),
        Error::Corrupt(m) | Error::NotFound(m) => corrupt(file, m),
        Error::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
            corrupt(file, "file ended before its recorded length")
        }
        other => other,
    }
}

/// Refuses a file whose footer checksum is not the one `traces.idx` recorded:
/// it is not the file this part wrote, whatever its length.
fn check_footer(file: &str, found: u32, recorded: u32) -> Result<()> {
    if found != recorded {
        return Err(corrupt(
            file,
            format!(
                "footer checksum is {found:#010x} but {} recorded {recorded:#010x}: not the \
                 file this part wrote",
                crate::INDEX_FILE
            ),
        ));
    }
    Ok(())
}

fn open_file(dir: &Path, name: &str, recorded_len: u64) -> Result<File> {
    let file = File::open(dir.join(name)).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => corrupt(name, "file is missing from the part"),
        _ => Error::Io(e),
    })?;
    let len = file.metadata()?.len();
    if len != recorded_len {
        return Err(corrupt(
            name,
            format!(
                "file has {len} bytes but {} recorded {recorded_len}",
                crate::INDEX_FILE
            ),
        ));
    }
    Ok(file)
}

impl TextFiles {
    /// Opens both files, checking their lengths and footer checksums against
    /// the record and that they cover the part's `pieces`. The footer of each
    /// file covers its directory (`text.idx`: the checksum of every segment) or
    /// its group index (`text.rows`: the checksum of every group), and a
    /// segment or a group is checked against those when it is read, so every
    /// later read is bound to `traces.idx` without reading the files now. The
    /// whole-file checksums are not read here; see [`PartReader::verify`].
    pub(crate) fn open(dir: &Path, sums: &FileChecksums, pieces: u64) -> Result<Self> {
        let index_file = open_file(dir, TEXT_INDEX_FILE, sums.text_index.len)?;
        let rows_file = open_file(dir, TEXT_ROWS_FILE, sums.text_rows.len)?;
        let index =
            IndexReader::open(index_file.try_clone()?).map_err(|e| named(TEXT_INDEX_FILE, e))?;
        let rows =
            IdListReader::open(rows_file.try_clone()?).map_err(|e| named(TEXT_ROWS_FILE, e))?;
        check_footer(TEXT_INDEX_FILE, index.footer_crc(), sums.text_index_footer)?;
        check_footer(
            TEXT_ROWS_FILE,
            rows.meta().footer_crc,
            sums.text_rows_footer,
        )?;
        // Both files are made for exactly this part's pieces.
        if index.doc_count() != pieces {
            return Err(corrupt(
                TEXT_INDEX_FILE,
                format!(
                    "{} documents but the piece store has {pieces} pieces",
                    index.doc_count()
                ),
            ));
        }
        if rows.keys() != pieces {
            return Err(corrupt(
                TEXT_ROWS_FILE,
                format!(
                    "{} lists but the piece store has {pieces} pieces",
                    rows.keys()
                ),
            ));
        }
        Ok(Self {
            index,
            rows,
            index_file,
            rows_file,
            checksums: (sums.text_index, sums.text_rows),
        })
    }
}

/// Length and CRC-32C of a file read in full.
fn file_checksum(name: &str, file: &File, len: u64) -> Result<FileChecksum> {
    let now = file.metadata()?.len();
    if now != len {
        return Err(corrupt(
            name,
            format!("file has {now} bytes but had {len} when it was opened"),
        ));
    }
    let (mut crc, mut pos) = (0u32, 0u64);
    let mut buf = vec![0u8; 1 << 20];
    while pos < len {
        let n = (len - pos).min(buf.len() as u64) as usize;
        file.read_exact_at(&mut buf[..n], pos)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    corrupt(name, "file ended before its recorded length")
                }
                _ => Error::Io(e),
            })?;
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        pos += n as u64;
    }
    Ok(FileChecksum { len, crc32c: crc })
}

/// True if evaluating the query needs the set of all rows: it holds a `Not`,
/// or an `And` with no operands, which is every row.
fn needs_universe(q: &Query) -> bool {
    match q {
        Query::Not(_) => true,
        Query::And(v) => v.is_empty() || v.iter().any(needs_universe),
        Query::Or(v) => v.iter().any(needs_universe),
        _ => false,
    }
}

impl PartReader {
    /// Counts describing the part's text files.
    pub fn text_stats(&self) -> TextStats {
        let t = &self.text;
        let m = t.index.meta();
        TextStats {
            index_bytes: m.index_bytes,
            row_list_bytes: t.rows.meta().bytes,
            text_bytes: m.text_bytes,
            pieces: t.index.doc_count(),
            terms: m.terms,
            row_list_ids: t.rows.meta().ids,
        }
    }

    /// Checks `text.idx` and `text.rows` in full: the length and CRC-32C of
    /// both against `traces.idx`, and every segment of the index. Part of
    /// [`verify`](Self::verify).
    pub(crate) fn verify_text(&self) -> Result<()> {
        let t = &self.text;
        for (name, file, want) in [
            (TEXT_INDEX_FILE, &t.index_file, t.checksums.0),
            (TEXT_ROWS_FILE, &t.rows_file, t.checksums.1),
        ] {
            let now = file_checksum(name, file, want.len)?;
            if now.crc32c != want.crc32c {
                return Err(corrupt(
                    name,
                    format!(
                        "checksum mismatch: CRC-32C is {:#010x}, {} recorded {:#010x}",
                        now.crc32c,
                        crate::INDEX_FILE,
                        want.crc32c
                    ),
                ));
            }
        }
        t.index.verify().map_err(|e| named(TEXT_INDEX_FILE, e))?;
        // The CRC-32C above covers every list of `text.rows`, so the lists are
        // as the writer made them. A row number outside the part is refused
        // when a search reads it.
        Ok(())
    }

    /// Refuses piece ids that the part's pieces do not have: `text.idx` names
    /// a document the piece store has no piece for.
    fn check_pieces(&self, pieces: &[u32]) -> Result<()> {
        let count = self.text.rows.keys();
        if let Some(&bad) = pieces.iter().find(|&&p| u64::from(p) >= count) {
            return Err(corrupt(
                TEXT_INDEX_FILE,
                format!("document {bad} but the part has {count} pieces"),
            ));
        }
        Ok(())
    }

    /// The ascending rows whose text matches `query`.
    ///
    /// A row's text is the union of the pieces its raw columns refer to. A word
    /// matches a row if any of the row's pieces has it; a phrase must lie
    /// inside one piece; `AND`, `OR` and `NOT` combine sets of rows. Text that
    /// stayed inline in a row is not searched (see the module documentation).
    ///
    /// For each leaf the index names the candidate pieces. Where the index
    /// cannot decide (a phrase, a word of more than 64 bytes) each candidate
    /// piece is read once, in the order of its blocks, and confirmed; the
    /// confirmed pieces become rows through `text.rows`. The set of all rows is
    /// built only when the query has a `NOT`.
    ///
    /// A query over a limit in `limits` is [`Error::InvalidArgument`] saying
    /// which; a damaged text file is [`Error::Corrupt`] naming it. The reader
    /// is shared: nothing mutable is kept between searches.
    pub fn search(&self, query: &Query, limits: &SearchLimits) -> Result<Vec<u32>> {
        self.search_counted(query, limits).map(|(rows, _)| rows)
    }

    /// [`search`](Self::search), and the bytes of piece text it read to
    /// confirm phrases and long words, counted as `max_confirm_bytes` counts
    /// them. A caller that searches several parts subtracts this from the
    /// limit it gives the next part.
    pub fn search_counted(&self, query: &Query, limits: &SearchLimits) -> Result<(Vec<u32>, u64)> {
        let universe: Vec<u32> = if needs_universe(query) {
            let rows = self.row_count();
            if rows > limits.max_leaf_rows as u64 {
                return Err(Error::invalid_argument(format!(
                    "the query has a negation or an empty AND, which covers all {rows} rows of \
                     a part, more than the limit of {} rows a leaf may produce",
                    limits.max_leaf_rows
                )));
            }
            let rows = u32::try_from(rows).map_err(|_| {
                corrupt(crate::SPANS_FILE, "more rows than the text lists can name")
            })?;
            (0..rows).collect()
        } else {
            Vec::new()
        };
        let mut confirm_spent = 0u64;
        let rows = evaluate(query, &universe, |leaf| {
            let pieces = self.leaf_pieces(leaf, limits, &mut confirm_spent)?;
            self.pieces_to_rows(&pieces, leaf, limits)
        })?;
        Ok((rows, confirm_spent))
    }

    /// The pieces that match one leaf, ascending.
    fn leaf_pieces(
        &self,
        leaf: &Leaf<'_>,
        limits: &SearchLimits,
        confirm_spent: &mut u64,
    ) -> Result<Vec<u32>> {
        let index = &self.text.index;
        let named_index = |e| named(TEXT_INDEX_FILE, e);
        match *leaf {
            Leaf::Term(token) => {
                let found = index.term(token).map_err(named_index)?;
                if found.exact {
                    Ok(found.ids.into())
                } else {
                    self.confirm(found.ids.into(), limits, confirm_spent, |text| {
                        contains_token(text, token.as_bytes())
                    })
                }
            }
            Leaf::Prefix(prefix) => index
                .prefix(prefix, limits.max_prefix_terms)
                .map(Into::into)
                .map_err(|e| match e {
                    Error::InvalidArgument(m) => {
                        Error::invalid_argument(format!("the prefix `{prefix}*` is too broad: {m}"))
                    }
                    other => named_index(other),
                }),
            Leaf::Phrase(tokens) => {
                let found = phrase_candidates(index, tokens).map_err(named_index)?;
                if found.exact {
                    Ok(found.ids.into())
                } else {
                    self.confirm(found.ids.into(), limits, confirm_spent, |text| {
                        phrase_matches(text, tokens)
                    })
                }
            }
        }
    }

    /// The candidates whose text `holds` accepts. Each is read once, block by
    /// block, after the bytes to be read have been checked against the limit.
    fn confirm(
        &self,
        candidates: Vec<u32>,
        limits: &SearchLimits,
        spent: &mut u64,
        holds: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u32>> {
        self.check_pieces(&candidates)?;
        let store = self.piece_store();
        let mut total = *spent;
        for &id in &candidates {
            total = total.saturating_add(store.piece_len(id)?);
            if total > limits.max_confirm_bytes {
                return Err(Error::invalid_argument(format!(
                    "the query needs more than {} bytes of text read to confirm a phrase or a \
                     word longer than 64 bytes ({} candidate pieces so far); make it more \
                     specific",
                    limits.max_confirm_bytes,
                    candidates.len()
                )));
            }
        }
        *spent = total;
        let mut confirmed = Vec::new();
        store.read_sorted(candidates.iter().copied(), |n, bytes| {
            if holds(bytes) {
                confirmed.push(candidates[n]);
            }
        })?;
        Ok(confirmed)
    }

    /// The ascending rows that refer to any of `pieces`, which are ascending.
    ///
    /// Each group of `text.rows` is read once. A list longer than the limit is
    /// refused from its count, before it is decoded. The rows are collected
    /// and sorted when a list would take them past twice the limit, so a row
    /// that many pieces share does not count many times, and the memory is
    /// about twice the limit however many pieces there are.
    fn pieces_to_rows(
        &self,
        pieces: &[u32],
        leaf: &Leaf<'_>,
        limits: &SearchLimits,
    ) -> Result<Vec<u32>> {
        self.check_pieces(pieces)?;
        let limit = limits.max_leaf_rows;
        let too_broad = || {
            Error::invalid_argument(format!(
                "{} matches more than {limit} rows of a part; make the query more specific",
                describe(leaf)
            ))
        };
        let row_count = self.row_count();
        let mut rows: Vec<u32> = Vec::new();
        self.text
            .rows
            .for_each(pieces.iter().copied(), |piece, list| {
                let n = list.id_count()?;
                // The ids of one list are distinct, so a list over the limit is
                // over it whatever else is collected.
                if n > limit as u64 {
                    return Err(too_broad());
                }
                if rows.len() as u64 + n > (limit as u64).saturating_mul(2) {
                    rows.sort_unstable();
                    rows.dedup();
                    if rows.len() > limit {
                        return Err(too_broad());
                    }
                }
                let ids = list.decode()?;
                if let Some(&last) = ids.last()
                    && u64::from(last) >= row_count
                {
                    return Err(corrupt(
                        TEXT_ROWS_FILE,
                        format!("piece {piece} lists row {last} but the part has {row_count} rows"),
                    ));
                }
                rows.extend(ids);
                Ok(())
            })
            .map_err(|e| named(TEXT_ROWS_FILE, e))?;
        rows.sort_unstable();
        rows.dedup();
        if rows.len() > limit {
            return Err(too_broad());
        }
        Ok(rows)
    }
}

fn describe(leaf: &Leaf<'_>) -> String {
    match leaf {
        Leaf::Term(t) => format!("the word `{t}`"),
        Leaf::Prefix(p) => format!("the prefix `{p}*`"),
        Leaf::Phrase(p) => format!("the phrase \"{}\"", p.join(" ")),
    }
}
