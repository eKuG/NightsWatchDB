//! The manifest of NightsWatchDB: which parts are live, and how far the
//! write-ahead log has been written into parts.
//!
//! Parts are immutable, so the manifest is the one piece of mutable metadata.
//! Every change is a new numbered file that is created only if absent. Readers
//! hold an [`Arc<ManifestState>`](std::sync::Arc) snapshot and never wait for
//! writers. One writer per data directory: the engine holds an exclusive
//! lock on the directory and a single `Manifest`; conflict detection between
//! handles is only a second line of defence. See `README.md` in this crate for the publishing protocol and the
//! crash rules.

mod file;
mod state;
mod store;

pub use state::{Edit, MAX_NEXT_PART_ID, ManifestState};
pub use store::{Manifest, ManifestOptions, OpenMode, highest_part_dir_id, unreferenced_part_dirs};
