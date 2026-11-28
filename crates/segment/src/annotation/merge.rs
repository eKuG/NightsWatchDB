//! Merging annotation parts of one window into one.

use super::latest::Latest;
use super::part::{AnnotationReader, AnnotationWriter};
use nwdb_core::{Error, PartMeta, Result};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Output blocks between two looks at the stop flag.
const STOP_CHECK_BLOCKS: u64 = 64;

#[cfg(test)]
type BlockHook = Box<dyn Fn(u64)>;

#[cfg(test)]
thread_local! {
    /// Called with the number of output blocks each time one more was closed.
    pub(crate) static ON_OUTPUT_BLOCK: std::cell::RefCell<Option<BlockHook>> =
        const { std::cell::RefCell::new(None) };
}

/// What a merge dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AnnotationMergeStats {
    /// Records of the inputs that are not in the output: older versions of an
    /// identity.
    pub superseded: u64,
}

/// Merges parts of one window into one part: for each identity the record
/// with the largest `arrival` (on a tie the earlier input), in identity
/// order. A winner that is a delete is kept: an older version of the same
/// identity can be in a part of another window, and dropping the delete would
/// bring it back.
///
/// [`Error::InvalidArgument`] if there is no input or an input's window is
/// not the one given. Every input is verified first. `stop` is looked at
/// before each input is verified and after every 64 output blocks; when it is
/// set the temporary directory is removed and the result is
/// [`Error::Unavailable`] ("the merge was stopped"). Memory: one cursor per
/// input (a compressed and a decompressed block, and one record), the output
/// block and the output table.
pub fn merge_annotation_parts(
    inputs: &[&AnnotationReader],
    parts_dir: &Path,
    part_id: u64,
    window_start_ns: i64,
    window_ns: i64,
    level: u8,
    stop: Option<&AtomicBool>,
) -> Result<(PartMeta, AnnotationMergeStats)> {
    if inputs.is_empty() {
        return Err(Error::invalid_argument("a merge needs at least one input"));
    }
    for input in inputs {
        let m = input.meta();
        if m.window_start_ns != window_start_ns || m.window_ns != window_ns {
            return Err(Error::invalid_argument(format!(
                "part {} is of window {} + {}, not {window_start_ns} + {window_ns}",
                m.part_id, m.window_start_ns, m.window_ns
            )));
        }
    }
    let stopped = || stop.is_some_and(|s| s.load(Ordering::Relaxed));
    for input in inputs {
        if stopped() {
            return Err(Error::unavailable("the merge was stopped"));
        }
        input.verify()?;
    }
    let mut latest = Latest::new(inputs.iter().map(|r| r.cursor(None)).collect(), Vec::new());
    // Dropped without `finish` on any error: the temporary directory goes.
    let mut writer = AnnotationWriter::create(parts_dir, part_id, window_start_ns, window_ns)?;
    let mut blocks = 0;
    while let Some(a) = latest.next()? {
        writer.append(&a)?;
        if writer.blocks_written() != blocks {
            blocks = writer.blocks_written();
            #[cfg(test)]
            ON_OUTPUT_BLOCK.with(|h| {
                if let Some(f) = h.borrow().as_ref() {
                    f(blocks)
                }
            });
            if blocks.is_multiple_of(STOP_CHECK_BLOCKS) && stopped() {
                return Err(Error::unavailable("the merge was stopped"));
            }
        }
    }
    let meta = writer.finish(level)?;
    let total: u64 = inputs.iter().map(|r| r.meta().rows).sum();
    Ok((
        meta.clone(),
        AnnotationMergeStats {
            superseded: total - meta.rows,
        },
    ))
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
