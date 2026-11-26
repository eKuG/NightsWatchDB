//! The text index of NightsWatchDB.
//!
//! A small inverted index over documents `(doc id: u32, bytes)`. The crate
//! knows nothing about spans, traces, parts or files beyond its own index
//! file. The database indexes each distinct piece of message text once, so a
//! phrase is checked once however many spans repeat it; this crate sees only
//! pieces and ids.
//!
//! # Pieces
//!
//! - [`tokens`]: the tokeniser, an iterator that allocates nothing.
//! - [`IndexBuilder`]: builds one index file from documents added in
//!   ascending id order, in bounded memory ([`IndexOptions`]).
//! - [`IndexReader`]: opens an index file through positioned reads
//!   ([`ReadAt`]) and answers [`IndexReader::term`], [`IndexReader::prefix`]
//!   and [`IndexReader::intersect`]. It is `Send + Sync`.
//! - [`ids`]: ascending id lists ([`DocIds`]) with intersection, union and
//!   difference.
//! - [`Query`], [`parse`], [`leaves`], [`evaluate`], [`phrase_candidates`]
//!   and [`phrase_matches`]: the query model. The crate does not evaluate a
//!   query against documents; the caller combines the sets it cares about.
//! - [`IdListWriter`] and [`IdListReader`]: a compact store of "dense key to
//!   ascending id list".
//!
//! # No positions
//!
//! The index stores no positions, frequencies or scores. A phrase is answered
//! in two steps: [`phrase_candidates`] finds the documents that contain every
//! token, and the caller checks each with [`phrase_matches`] against the text.
//!
//! # Words longer than 64 bytes
//!
//! A token of more than 64 bytes is listed under its first 64 bytes. Searching
//! for one returns [`Matches`] with `exact: false`: candidates, to be
//! confirmed with [`contains_token`]. Prefix searches see these tokens too and
//! stay exact.
//!
//! # Damaged files
//!
//! Every length, count and offset read from a file is checked before it is
//! used. A file that fails a check is [`nwdb_core::Error::Corrupt`]; there are
//! no panics and no allocations sized by an unchecked number from the file.
//! The README documents the layouts and what is checked when.

mod builder;
mod codec;
mod format;
mod idlist;
mod meta;
mod query;
mod reader;
mod source;
mod tokenizer;

pub mod ids;

pub use builder::{DEFAULT_SEGMENT_BUDGET, IndexBuilder, IndexOptions, MIN_SEGMENT_BUDGET};
pub use idlist::{IdList, IdListMeta, IdListReader, IdListWriter};
pub use ids::DocIds;
pub use meta::IndexMeta;
pub use query::{
    Leaf, MAX_EVAL_DEPTH, Query, contains_token, evaluate, leaves, parse, phrase_candidates,
    phrase_matches,
};
pub use reader::{IndexReader, Matches};
pub use source::ReadAt;
pub use tokenizer::{MAX_TOKEN_LEN, Token, Tokens, tokens};

#[cfg(test)]
mod tests;
