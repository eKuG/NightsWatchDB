//! The brute-force model of full-text search that the engine's search tests
//! compare with: which spans match a query, and which of them a search with a
//! time range and a limit must return.
//!
//! A span's text is the pieces the writer stores for it: the lines of its
//! attributes (the test splitter cuts at line ends) that are at least the
//! writer's floor long. Shorter lines stay inline in the row and are not
//! searchable.
#![allow(dead_code)]

use std::ops::Range;

use nwdb_core::{PayloadSplitter, RawColumn, SpanId};
use nwdb_engine::{SearchHit, SearchQuery};
use nwdb_textindex::{Query, contains_token, phrase_matches, tokens};
use rand::Rng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;

use super::common::TSpan;

/// Cuts a value at its line ends.
pub struct Lines;

fn lines(value: &[u8]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut at = 0;
    for line in value.split(|&b| b == b'\n') {
        if !line.is_empty() {
            out.push(at..at + line.len());
        }
        at += line.len() + 1;
    }
    out
}

impl PayloadSplitter for Lines {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.extend(lines(value));
    }
}

/// The pieces stored for a span.
pub fn pieces(s: &TSpan) -> Vec<Vec<u8>> {
    lines(&s.attrs)
        .into_iter()
        .filter(|c| c.len() >= nwdb_segment::DEFAULT_MIN_PIECE_BYTES)
        .map(|c| s.attrs[c].to_vec())
        .collect()
}

fn token_has_prefix(piece: &[u8], prefix: &str) -> bool {
    tokens(piece).any(|t| t.raw().to_ascii_lowercase().starts_with(prefix.as_bytes()))
}

/// Whether a span with these pieces matches.
pub fn matches(q: &Query, pieces: &[Vec<u8>]) -> bool {
    match q {
        Query::Term(t) => pieces.iter().any(|p| contains_token(p, t.as_bytes())),
        Query::Prefix(p) => pieces.iter().any(|piece| token_has_prefix(piece, p)),
        Query::Phrase(toks) => pieces.iter().any(|p| phrase_matches(p, toks)),
        Query::And(v) => v.iter().all(|q| matches(q, pieces)),
        Query::Or(v) => v.iter().any(|q| matches(q, pieces)),
        Query::Not(q) => !matches(q, pieces),
    }
}

pub fn hit_of(s: &TSpan) -> SearchHit {
    SearchHit {
        trace_id: s.trace_id(),
        span_id: SpanId(s.span.to_be_bytes()),
        start_ns: s.start,
        end_ns: s.end,
        name: s.name.clone(),
        service_name: s.service.clone(),
        operation: s.operation.clone(),
        request_model: s.model.clone(),
    }
}

/// What a search must return over these spans (one copy of each span).
pub fn expected<'a>(spans: impl IntoIterator<Item = &'a TSpan>, q: &SearchQuery) -> Vec<SearchHit> {
    let mut hits: Vec<SearchHit> = spans
        .into_iter()
        .filter(|s| s.start >= q.from_ns && s.start < q.to_ns)
        .filter(|s| matches(&q.query, &pieces(s)))
        .map(hit_of)
        .collect();
    hits.sort_by(|a, b| {
        b.start_ns
            .cmp(&a.start_ns)
            .then(a.trace_id.cmp(&b.trace_id))
            .then(a.span_id.cmp(&b.span_id))
    });
    hits.truncate(q.limit);
    hits
}

pub const WORDS: &[&str] = &[
    "alpha",
    "beta",
    "gamma",
    "delta",
    "error",
    "timeout",
    "retry",
    "user",
    "assistant",
    "paris",
    "weather",
    "tool",
    "zq001",
    "zq002",
    "zq003",
    "go",
];

/// One message of 3 to 8 words, at least 8 bytes long.
pub fn message(rng: &mut StdRng) -> String {
    loop {
        let n = rng.random_range(3..=8);
        let m = (0..n)
            .map(|_| *WORDS.choose(rng).unwrap())
            .collect::<Vec<_>>()
            .join(" ");
        if m.len() >= 8 {
            return m;
        }
    }
}

fn leaf(rng: &mut StdRng) -> Query {
    match rng.random_range(0..8) {
        0..=3 => Query::Term((*WORDS.choose(rng).unwrap()).to_string()),
        4 => Query::Prefix(
            ["zq", "zq00", "al", "ti", "as"]
                .choose(rng)
                .unwrap()
                .to_string(),
        ),
        5 | 6 => {
            let n = rng.random_range(2..=3);
            Query::Phrase(
                (0..n)
                    .map(|_| (*WORDS.choose(rng).unwrap()).to_string())
                    .collect(),
            )
        }
        _ => Query::Term("absent".to_string()),
    }
}

pub fn gen_query(rng: &mut StdRng, depth: u32) -> Query {
    if depth == 0 || rng.random_bool(0.4) {
        return leaf(rng);
    }
    let operands = |rng: &mut StdRng| -> Vec<Query> {
        (0..rng.random_range(2..=3))
            .map(|_| gen_query(rng, depth - 1))
            .collect()
    };
    match rng.random_range(0..4) {
        0 => Query::And(operands(rng)),
        1 => Query::Or(operands(rng)),
        2 => Query::Not(Box::new(gen_query(rng, depth - 1))),
        _ => {
            let mut v = operands(rng);
            v.push(Query::Not(Box::new(leaf(rng))));
            Query::And(v)
        }
    }
}
