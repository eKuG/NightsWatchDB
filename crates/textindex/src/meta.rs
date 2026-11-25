//! What an index file contains, as counts.

/// Counts describing one index file. The builder returns it from `finish`,
/// and the reader reports the same values from the file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexMeta {
    /// Number of index segments.
    pub segments: u32,
    /// Number of documents added, including documents with no tokens.
    pub docs: u64,
    /// Bytes of document text that were tokenised.
    pub text_bytes: u64,
    /// Distinct terms, summed over segments: a term that occurs in two
    /// segments counts twice.
    pub terms: u64,
    /// Posting entries: one per (segment, term, document).
    pub postings: u64,
    /// Size of the whole file in bytes.
    pub index_bytes: u64,
    /// Bytes of the postings areas of all segments.
    pub postings_bytes: u64,
    /// Bytes of the term dictionaries of all segments.
    pub dictionary_bytes: u64,
    /// Bytes of the sparse indexes of all segments.
    pub sparse_bytes: u64,
    /// Bytes of the directory and footer.
    pub footer_bytes: u64,
}
