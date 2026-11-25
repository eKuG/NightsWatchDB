//! Building an index file.
//!
//! Documents are tokenised into the postings of one open segment, held in
//! memory. When the memory held for that segment passes the byte budget the
//! segment is closed: its terms are sorted, and its postings, dictionary and
//! sparse index are written to the output and freed. Nothing else grows with
//! the input, so building needs memory proportional to the budget, not to the
//! amount of text.

use std::collections::HashMap;
use std::io::Write;

use nwdb_core::{Error, Result};

use crate::codec::{encode_list, put_varint};
use crate::format::{
    DIR_ENTRY, DirEntry, FOOTER, FooterFields, INLINE_MAX, TERMS_PER_BLOCK, encode_footer,
};
use crate::meta::IndexMeta;
use crate::source::Sink;
use crate::tokenizer::{MAX_KEY_LEN, tokens};

/// The default byte budget of an open segment: 256 MiB.
pub const DEFAULT_SEGMENT_BUDGET: usize = 256 * 1024 * 1024;

/// The smallest segment budget: 1 MiB. Every closed segment leaves a
/// directory entry in memory until `finish`, so a tiny budget would make memory
/// grow with the number of documents. A smaller value is raised to this.
pub const MIN_SEGMENT_BUDGET: usize = 1024 * 1024;

/// Options for [`IndexBuilder`].
#[derive(Clone, Debug)]
pub struct IndexOptions {
    /// An index segment is closed when the memory held for its postings
    /// passes this many bytes. Values below [`MIN_SEGMENT_BUDGET`] (1 MiB)
    /// are raised to it.
    ///
    /// The count is an estimate of heap bytes (term bytes, hash table slots,
    /// list headers and the ids of every list, with allocator rounding) and is
    /// checked after each document, so one large document can overshoot it.
    /// Closing a segment needs a further 32 bytes per term for the sorted
    /// order plus the segment's dictionary and sparse index, and each closed
    /// segment keeps a 72 byte directory entry (at most 144 bytes with the
    /// vector's spare room) in memory until `finish`. The README states the
    /// resulting bound on peak memory.
    pub segment_budget_bytes: usize,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            segment_budget_bytes: DEFAULT_SEGMENT_BUDGET,
        }
    }
}

/// Heap bytes the allocator really uses for a request of `n` bytes: a header
/// word and rounding to 16 bytes.
fn heap_cost(n: usize) -> usize {
    (n + 8).next_multiple_of(16)
}

/// Bytes one hash table slot costs, counting control bytes and the table's
/// spare capacity (a table is at most 7/8 full).
const SLOT_BYTES: usize = (std::mem::size_of::<(Box<[u8]>, u32)>() + 1) * 8 / 7 + 1;

/// The postings of the segment being built.
#[derive(Default)]
struct OpenSegment {
    terms: HashMap<Box<[u8]>, u32>,
    lists: Vec<Vec<u32>>,
    /// Estimated heap bytes held.
    mem: usize,
    /// Documents added. A segment holds at most `u32::MAX`; the builder closes
    /// it before that count could wrap.
    docs: u32,
    doc_lo: u32,
    doc_hi: u32,
    postings: u64,
}

impl OpenSegment {
    fn add(&mut self, doc: u32, text: &[u8]) {
        if self.docs == 0 {
            self.doc_lo = doc;
        }
        self.doc_hi = doc;
        self.docs += 1;
        let table_before = self.terms.capacity();
        let lists_before = self.lists.capacity();
        let mut buf = [0u8; MAX_KEY_LEN];
        for tok in tokens(text) {
            let term = tok.key(&mut buf);
            if let Some(&i) = self.terms.get(term) {
                let list = &mut self.lists[i as usize];
                if list.last() != Some(&doc) {
                    let cap = list.capacity();
                    list.push(doc);
                    self.mem += (list.capacity() - cap) * 4;
                    self.postings += 1;
                }
            } else {
                let i = self.lists.len() as u32;
                let list = vec![doc];
                // A one-element Vec<u32> still gets a 16 byte allocation.
                self.mem += heap_cost(term.len()) + heap_cost(list.capacity() * 4);
                self.lists.push(list);
                self.terms.insert(term.into(), i);
                self.postings += 1;
            }
        }
        self.mem += (self.terms.capacity() - table_before) * SLOT_BYTES;
        self.mem += (self.lists.capacity() - lists_before) * std::mem::size_of::<Vec<u32>>();
    }
}

/// Builds one index file from documents `(id, bytes)` added in ascending id
/// order.
///
/// Index segments are written to the writer as they close, so the writer is
/// given up front; pass `&mut file` to keep ownership of it.
pub struct IndexBuilder<W: Write> {
    options: IndexOptions,
    out: Sink<W>,
    open: OpenSegment,
    last_doc: Option<u32>,
    directory: Vec<DirEntry>,
    docs: u64,
    text_bytes: u64,
    meta: IndexMeta,
    failed: bool,
    /// Documents after which a segment is closed; `u32::MAX` except in tests.
    max_segment_docs: u32,
}

impl<W: Write> IndexBuilder<W> {
    /// Starts an index that will be written to `out`. A segment budget below
    /// [`MIN_SEGMENT_BUDGET`] is raised to it.
    pub fn new(mut options: IndexOptions, out: W) -> Self {
        options.segment_budget_bytes = options.segment_budget_bytes.max(MIN_SEGMENT_BUDGET);
        Self {
            options,
            out: Sink::new(out),
            open: OpenSegment::default(),
            last_doc: None,
            directory: Vec::new(),
            docs: 0,
            text_bytes: 0,
            meta: IndexMeta::default(),
            failed: false,
            max_segment_docs: u32::MAX,
        }
    }

    /// Closes a segment after `n` documents (at least 1), as well as when the
    /// budget is passed. The minimum budget of 1 MiB makes many segments
    /// costly to reach with test data; this makes them cheap. Not part of the
    /// supported interface.
    #[doc(hidden)]
    #[must_use]
    pub fn with_max_segment_docs(mut self, n: u32) -> Self {
        self.max_segment_docs = n.max(1);
        self
    }

    /// Adds a document. Ids must be strictly ascending; they need not be
    /// dense. A document with no tokens is allowed and still counts.
    ///
    /// An id that is not above the previous one is `InvalidArgument` and
    /// changes nothing. If writing a closed segment fails, the builder is
    /// poisoned: the output is incomplete and every later call fails.
    pub fn add(&mut self, doc_id: u32, text: &[u8]) -> Result<()> {
        if self.failed {
            return Err(Error::fatal("index builder failed earlier"));
        }
        if self.last_doc.is_some_and(|last| doc_id <= last) {
            return Err(Error::invalid_argument(
                "document ids must be strictly ascending",
            ));
        }
        if self.open.docs == self.max_segment_docs {
            self.close_segment()?;
        }
        self.last_doc = Some(doc_id);
        self.docs += 1;
        self.text_bytes += text.len() as u64;
        self.open.add(doc_id, text);
        if self.open.mem > self.options.segment_budget_bytes {
            self.close_segment()?;
        }
        Ok(())
    }

    /// Closes the open segment, writes the directory and footer, flushes the
    /// writer, and returns what was written.
    pub fn finish(mut self) -> Result<IndexMeta> {
        if self.failed {
            return Err(Error::fatal("index builder failed earlier"));
        }
        self.close_segment()?;
        let result = self.write_tail();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn write_tail(&mut self) -> Result<IndexMeta> {
        let mut dir = Vec::with_capacity(self.directory.len() * DIR_ENTRY);
        for e in &self.directory {
            e.encode(&mut dir);
        }
        let directory_offset = self.out.pos();
        let footer = encode_footer(
            &FooterFields {
                segments: self.directory.len() as u32,
                docs: self.docs,
                text_bytes: self.text_bytes,
                terms: self.meta.terms,
                postings: self.meta.postings,
                directory_offset,
            },
            &dir,
        );
        self.out.write(&dir)?;
        self.out.write(&footer)?;
        self.out.flush()?;
        let mut meta = self.meta;
        meta.segments = self.directory.len() as u32;
        meta.docs = self.docs;
        meta.text_bytes = self.text_bytes;
        meta.index_bytes = self.out.pos();
        meta.footer_bytes = (dir.len() + FOOTER) as u64;
        Ok(meta)
    }

    fn close_segment(&mut self) -> Result<()> {
        if self.open.docs == 0 {
            return Ok(());
        }
        let open = std::mem::take(&mut self.open);
        let result = self.write_segment(&open);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn write_segment(&mut self, open: &OpenSegment) -> Result<()> {
        let mut order: Vec<(&[u8], &[u32])> = open
            .terms
            .iter()
            .map(|(t, &i)| (&**t, open.lists[i as usize].as_slice()))
            .collect();
        order.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let offset = self.out.pos();
        let (mut dict, mut sparse, mut tmp) = (Vec::new(), Vec::new(), Vec::new());
        let (mut body_crc, mut postings_len) = (0u32, 0u64);
        let mut prev: &[u8] = &[];
        let mut blocks = 0u32;
        for (n, &(term, ids)) in order.iter().enumerate() {
            if n % TERMS_PER_BLOCK as usize == 0 {
                prev = &[];
                put_varint(&mut sparse, term.len() as u64);
                sparse.extend_from_slice(term);
                sparse.extend_from_slice(&(dict.len() as u64).to_le_bytes());
                blocks += 1;
            }
            let shared = prev.iter().zip(term).take_while(|(a, b)| a == b).count();
            put_varint(&mut dict, shared as u64);
            put_varint(&mut dict, (term.len() - shared) as u64);
            dict.extend_from_slice(&term[shared..]);
            prev = term;
            put_varint(&mut dict, ids.len() as u64);
            if ids.len() <= INLINE_MAX {
                let mut next_min = 0u64;
                for &id in ids {
                    put_varint(&mut dict, u64::from(id) - next_min);
                    next_min = u64::from(id) + 1;
                }
            } else {
                put_varint(&mut dict, postings_len);
                tmp.clear();
                encode_list(ids, &mut tmp)?;
                body_crc = crc32c::crc32c_append(body_crc, &tmp);
                self.out.write(&tmp)?;
                postings_len += tmp.len() as u64;
            }
        }
        body_crc = crc32c::crc32c_append(body_crc, &dict);
        self.out.write(&dict)?;
        self.out.write(&sparse)?;

        let entry = DirEntry {
            offset,
            postings_len,
            dict_len: dict.len() as u64,
            sparse_len: sparse.len() as u64,
            terms: order.len() as u64,
            postings: open.postings,
            docs: open.docs,
            doc_lo: open.doc_lo,
            doc_hi: open.doc_hi,
            dict_blocks: blocks,
            body_crc,
            sparse_crc: crc32c::crc32c(&sparse),
        };
        self.meta.terms += entry.terms;
        self.meta.postings += entry.postings;
        self.meta.postings_bytes += postings_len;
        self.meta.dictionary_bytes += entry.dict_len;
        self.meta.sparse_bytes += entry.sparse_len;
        self.directory.push(entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexReader;

    /// The segment's document counter is a `u32`: a full segment is closed,
    /// never wrapped. The limit is lowered so the boundary is cheap to reach.
    #[test]
    fn a_full_segment_is_closed_instead_of_wrapping() {
        let mut out = Vec::new();
        let mut b = IndexBuilder::new(IndexOptions::default(), &mut out).with_max_segment_docs(3);
        for id in 0..10u32 {
            b.add(id * 2, b"alpha beta").unwrap();
        }
        let meta = b.finish().unwrap();
        assert_eq!((meta.segments, meta.docs), (4, 10));
        let r = IndexReader::open(out.as_slice()).unwrap();
        let want: Vec<u32> = (0..10).map(|i| i * 2).collect();
        assert_eq!(r.term("alpha").unwrap().ids.as_slice(), want);
    }
}
