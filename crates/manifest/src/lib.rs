//! The manifest of NightsWatchDB: which parts are live, and how far the
//! write-ahead log has been written into parts.
//!
//! Parts are immutable, so the manifest is the one piece of mutable metadata.
//! Every change is a new numbered file that is created only if absent. Readers
//! hold an [`Arc<ManifestState>`](std::sync::Arc) snapshot and never wait for
//! writers. See `README.md` in this crate for the publishing protocol and the
//! crash rules.

mod file;
mod state;
mod store;

pub use state::{Edit, ManifestState};
pub use store::{Manifest, ManifestOptions, unreferenced_part_dirs};
