//! Large-text storage for the parts of NightsWatchDB.
//!
//! In agent traces every model call re-sends the system prompt, the tool
//! definitions and the conversation so far, so most of the text in a part
//! repeats text already in it. This crate stores each distinct piece of text
//! once per part and lets span rows refer to it. It knows nothing about spans
//! or OTLP; it depends only on `nwdb-core`.
//!
//! - [`rope`]: how a value is stored when some of its bytes live in pieces.
//! - [`json_array_elements`]: finds the elements of a JSON array, so that a
//!   conversation stored as an array is cut into its messages.
//! - [`PieceWriter`] and [`PieceReader`]: the piece store, `pieces.blk` and
//!   `pieces.idx`. The formats are described in the README.

mod format;
mod json;
mod reader;
pub mod rope;
mod writer;

pub use format::{BLOCKS_FILE, INDEX_FILE, MAX_PIECE_BYTES};
pub use json::{MAX_JSON_DEPTH, json_array_elements};
pub use reader::{PieceReader, PieceSession};
pub use writer::{FileSum, PieceWriter, PieceWriterOptions, StoreSummary};
