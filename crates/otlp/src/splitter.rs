//! Deciding which text of a span is stored out of line.
//!
//! [`OtlpSplitter`] reads the raw `attrs`, `events` and `links` columns with
//! the forgiving reader, so it is total: on damaged or hostile bytes it stops
//! at the first thing it cannot read and proposes cuts only for what it has
//! read so far. It never builds an error and never panics.
//!
//! # What is cut
//!
//! Every string and bytes value of an `AnyValue`, at any depth up to
//! [`MAX_DEPTH`] (deeper content stays inline), is a candidate when it is at
//! least [`OtlpSplitter::min_bytes`] long, or when it sits under a content key
//! and is not empty. The content keys are the table in [`CONTENT_KEYS`] and
//! [`CONTENT_PREFIXES`]: the attributes that carry prompts, completions,
//! tool definitions and tool results, where text repeats from one model call
//! to the next. A value nested inside the value of a content key (the
//! messages of an array or key-value list) is under that key; only top-level
//! keys of the span, an event or a link are matched.
//!
//! For a candidate, if its bytes are a JSON array, each top-level element is
//! cut, and the brackets, commas and whitespace between them stay inline: the
//! next call that re-sends the conversation then shares every message it
//! already sent. Otherwise the whole content of the string is one cut. The
//! tag and length prefix of the string always stay inline.

use crate::walk::{Next, Rd, Val};
use nwdb_core::{PayloadSplitter, RawColumn};
use nwdb_payload::json_array_elements;
use std::ops::Range;

/// How deep into nested arrays and key-value lists the splitter looks; the
/// same bound as the JSON writer's.
pub const MAX_DEPTH: usize = crate::MAX_DEPTH;

/// Attribute keys whose values are content: prompts, completions, tool
/// definitions and tool calls.
pub const CONTENT_KEYS: &[&str] = &[
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.system_instructions",
    "gen_ai.tool.definitions",
    "gen_ai.tool.call.arguments",
    "gen_ai.tool.call.result",
    "gen_ai.prompt",
    "gen_ai.completion",
    "input.value",
    "output.value",
];

/// Attribute key prefixes whose values are content (the flattened message
/// attributes of older conventions).
pub const CONTENT_PREFIXES: &[&str] = &[
    "gen_ai.prompt.",
    "gen_ai.completion.",
    "llm.input_messages.",
    "llm.output_messages.",
];

/// The default [`OtlpSplitter::min_bytes`].
pub const DEFAULT_MIN_BYTES: usize = 256;

fn is_content_key(key: &[u8]) -> bool {
    CONTENT_KEYS.iter().any(|k| k.as_bytes() == key)
        || CONTENT_PREFIXES
            .iter()
            .any(|p| key.starts_with(p.as_bytes()))
}

/// The [`PayloadSplitter`] for OTLP spans. See the module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtlpSplitter {
    min_bytes: usize,
}

impl OtlpSplitter {
    /// A splitter that also cuts any string or bytes value of at least
    /// `min_bytes` bytes, whatever its key. Zero is treated as one: an empty
    /// value is never cut.
    pub fn new(min_bytes: usize) -> Self {
        Self {
            min_bytes: min_bytes.max(1),
        }
    }

    /// The length from which any string or bytes value is cut.
    pub fn min_bytes(&self) -> usize {
        self.min_bytes
    }
}

impl Default for OtlpSplitter {
    fn default() -> Self {
        Self::new(DEFAULT_MIN_BYTES)
    }
}

impl PayloadSplitter for OtlpSplitter {
    fn cuts(&self, column: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut w = Walk {
            min_bytes: self.min_bytes,
            origin: value.as_ptr() as usize,
            len: value.len(),
            cuts,
        };
        let mut r = Rd::new(value);
        while let Next::Item(msg) = r.prefixed() {
            match column {
                RawColumn::Attrs => w.key_value(msg, true, false, 0),
                // Span.Event holds its attributes in field 3, Span.Link in 4.
                RawColumn::Events => w.attributes_of(msg, 3),
                RawColumn::Links => w.attributes_of(msg, 4),
            }
        }
    }
}

struct Walk<'c> {
    min_bytes: usize,
    /// Address of the first byte of the column value, to turn the sub-slices
    /// the reader hands out into offsets.
    origin: usize,
    len: usize,
    cuts: &'c mut Vec<Range<usize>>,
}

impl Walk<'_> {
    /// The attribute `KeyValue`s in field `field` of an event or a link.
    fn attributes_of(&mut self, msg: &[u8], field: u32) {
        let mut r = Rd::new(msg);
        while let Next::Item((n, v)) = r.field() {
            if let (true, Val::Len(kv)) = (n == field, v) {
                self.key_value(kv, true, false, 0);
            }
        }
    }

    /// One `KeyValue`. `top` says it is an attribute of the span (not an entry
    /// of a nested list), the only place a key makes its value content;
    /// `inherited` is whether the value around it is already content.
    fn key_value(&mut self, msg: &[u8], top: bool, inherited: bool, depth: usize) {
        let mut key: &[u8] = &[];
        let mut r = Rd::new(msg);
        while let Next::Item((n, v)) = r.field() {
            if let (1, Val::Len(k)) = (n, v) {
                key = k;
            }
        }
        let content = inherited || (top && is_content_key(key));
        let mut r = Rd::new(msg);
        while let Next::Item((n, v)) = r.field() {
            if let (2, Val::Len(any)) = (n, v) {
                self.any_value(any, content, depth);
            }
        }
    }

    fn any_value(&mut self, msg: &[u8], content: bool, depth: usize) {
        if depth > MAX_DEPTH {
            return;
        }
        let mut r = Rd::new(msg);
        while let Next::Item((n, v)) = r.field() {
            let Val::Len(b) = v else { continue };
            match n {
                // string_value and bytes_value
                1 | 7 => self.leaf(b, content),
                // array_value: repeated AnyValue in field 1
                5 => {
                    let mut items = Rd::new(b);
                    while let Next::Item((m, w)) = items.field() {
                        if let (1, Val::Len(any)) = (m, w) {
                            self.any_value(any, content, depth + 1);
                        }
                    }
                }
                // kvlist_value: repeated KeyValue in field 1
                6 => {
                    let mut items = Rd::new(b);
                    while let Next::Item((m, w)) = items.field() {
                        if let (1, Val::Len(kv)) = (m, w) {
                            self.key_value(kv, false, content, depth + 1);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// A string or bytes value: cut it if it is a candidate.
    fn leaf(&mut self, b: &[u8], content: bool) {
        if b.is_empty() || (b.len() < self.min_bytes && !content) {
            return;
        }
        // The reader hands out sub-slices of the column value, so this holds;
        // checked anyway so that no input can make a cut leave the value.
        let Some(at) = (b.as_ptr() as usize)
            .checked_sub(self.origin)
            .filter(|at| at + b.len() <= self.len)
        else {
            return;
        };
        let first = self.cuts.len();
        if json_array_elements(b, self.cuts) {
            for c in &mut self.cuts[first..] {
                *c = c.start + at..c.end + at;
            }
        } else {
            self.cuts.push(at..at + b.len());
        }
    }
}
