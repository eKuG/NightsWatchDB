//! Annotations: scores, labels and comments attached to a trace, or to one
//! span of it, after the spans were written.
//!
//! This module holds the record and its encoding, used inside a write-ahead
//! log record body and inside the blocks of an annotation part. The format is
//! described in the crate's `README.md`.

mod codec;
mod latest;
mod part;

use nwdb_core::{Error, Result, SpanId, TraceId};
use std::cmp::Ordering;

pub use codec::{decode_batch, encode_batch};
pub use latest::Latest;
pub use part::{AnnotationCursor, AnnotationReader, AnnotationWriter};

/// Version of the annotation part format (the file `annotations.nwa`).
/// Recorded in `PartMeta::format_version` of an annotation part.
pub const ANNOTATION_FORMAT_VERSION: u32 = 1;
/// File name of the annotations inside an annotation part directory.
pub const ANNOTATION_FILE: &str = "annotations.nwa";
/// Longest `name`, bytes.
pub const MAX_NAME_BYTES: usize = 128;
/// Longest `source`, bytes.
pub const MAX_SOURCE_BYTES: usize = 128;
/// Longest `label`, bytes.
pub const MAX_LABEL_BYTES: usize = 256;
/// Longest `comment`, bytes.
pub const MAX_COMMENT_BYTES: usize = 4096;
/// Most annotations in one log record.
pub const MAX_BATCH_ITEMS: usize = 1000;

/// One annotation of a trace, or of one span of it.
///
/// Its identity is `(trace_id, span_id, name, source)`. Of all the records
/// with one identity the one with the largest `arrival` is the one shown; if
/// that record has `delete` set there is no annotation.
#[derive(Clone, Debug, PartialEq)]
pub struct Annotation {
    /// The trace annotated. Not all zeros.
    pub trace_id: TraceId,
    /// The span annotated, or none for the trace as a whole. Not all zeros.
    pub span_id: Option<SpanId>,
    /// What is annotated, 1 to [`MAX_NAME_BYTES`] bytes, no control characters.
    pub name: String,
    /// Who or what made it, 0 to [`MAX_SOURCE_BYTES`] bytes, no control
    /// characters.
    pub source: String,
    /// When the annotation was made, Unix nanoseconds.
    pub time_ns: i64,
    /// A score. Finite.
    pub value: Option<f64>,
    /// A label, 1 to [`MAX_LABEL_BYTES`] bytes, no control characters.
    pub label: Option<String>,
    /// A comment, 1 to [`MAX_COMMENT_BYTES`] bytes; of the control characters
    /// only `\n` and `\t` are allowed.
    pub comment: Option<String>,
    /// Removes the annotation with this identity. A delete carries no value,
    /// label or comment.
    pub delete: bool,
    /// Number of the log record the annotation arrived in; 0 until logged.
    pub arrival: u64,
}

fn check_text(what: &str, s: &str, min: usize, max: usize) -> std::result::Result<(), String> {
    if s.len() < min || s.len() > max {
        return Err(format!(
            "{what} must be {min} to {max} bytes, got {}",
            s.len()
        ));
    }
    if s.chars().any(char::is_control) {
        return Err(format!("{what} must not contain control characters"));
    }
    Ok(())
}

impl Annotation {
    /// Ok, or what is wrong with it, in words a client can be shown.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.trace_id.is_zero() {
            return Err("trace id must not be all zeros".into());
        }
        if self.span_id.is_some_and(|s| s.is_zero()) {
            return Err("span id must not be all zeros".into());
        }
        check_text("name", &self.name, 1, MAX_NAME_BYTES)?;
        check_text("source", &self.source, 0, MAX_SOURCE_BYTES)?;
        if let Some(label) = &self.label {
            check_text("label", label, 1, MAX_LABEL_BYTES)?;
        }
        if let Some(comment) = &self.comment {
            if comment.is_empty() || comment.len() > MAX_COMMENT_BYTES {
                return Err(format!(
                    "comment must be 1 to {MAX_COMMENT_BYTES} bytes, got {}",
                    comment.len()
                ));
            }
            if comment
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t')
            {
                return Err(
                    "comment must not contain control characters other than newline and tab".into(),
                );
            }
        }
        if self.value.is_some_and(|v| !v.is_finite()) {
            return Err("value must be a finite number".into());
        }
        if self.delete {
            if self.value.is_some() || self.label.is_some() || self.comment.is_some() {
                return Err("a delete must not carry a value, label or comment".into());
            }
        } else if self.value.is_none() && self.label.is_none() {
            return Err("an annotation needs a value or a label".into());
        }
        Ok(())
    }

    /// Orders by identity: trace id, span id (none first), name bytes, source
    /// bytes.
    pub fn identity_cmp(&self, other: &Self) -> Ordering {
        self.trace_id
            .cmp(&other.trace_id)
            .then_with(|| self.span_id.cmp(&other.span_id))
            .then_with(|| self.name.as_bytes().cmp(other.name.as_bytes()))
            .then_with(|| self.source.as_bytes().cmp(other.source.as_bytes()))
    }
}

/// `Ok` for [`ANNOTATION_FORMAT_VERSION`]; otherwise an error naming the
/// version found and the one understood.
pub fn check_annotation_format_version(found: u32) -> Result<()> {
    if found == ANNOTATION_FORMAT_VERSION {
        return Ok(());
    }
    Err(Error::corrupt(format!(
        "the annotation part has format version {found} but this build reads version \
         {ANNOTATION_FORMAT_VERSION}; there is no migration"
    )))
}
