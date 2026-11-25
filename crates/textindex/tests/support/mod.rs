//! Helpers shared by the integration tests: a seeded document generator, a
//! brute-force model of the index, and builders.
#![allow(dead_code)]

use std::collections::BTreeSet;

use nwdb_textindex::{IndexBuilder, IndexMeta, IndexOptions, Query, tokens};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub type Docs = Vec<(u32, Vec<u8>)>;

/// Builds an index in memory with the given segment budget.
pub fn build(docs: &Docs, budget: usize) -> (Vec<u8>, IndexMeta) {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: budget,
        },
        &mut out,
    );
    for (id, text) in docs {
        b.add(*id, text).unwrap();
    }
    let meta = b.finish().unwrap();
    assert_eq!(meta.index_bytes, out.len() as u64);
    (out, meta)
}

/// A word of the small shared vocabulary.
pub fn vocab_word(i: usize) -> String {
    // Words share prefixes on purpose: w0, w00, w000 ...
    format!("w{}", "0".repeat(i % 4) + &i.to_string())
}

const SEPARATORS: &[&str] = &[
    " ", " ", " ", ", ", ". ", "\n", "-", "_", "/", " (", ") ", "\"", "; ", "\t",
];

/// Random text over the words returned by `word`, with random separators,
/// random upper case, now and then a multi-byte character, a stray invalid
/// byte, and a token of 70 bytes.
pub fn random_text(
    rng: &mut StdRng,
    words: usize,
    mut word: impl FnMut(&mut StdRng) -> String,
) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..words {
        let mut w = word(rng);
        match rng.random_range(0..40) {
            0 => w = w.to_uppercase(),
            1 => w = format!("{w}\u{e9}"),
            2 => w = "x".repeat(70),
            3 => out.push(0xff),
            _ => {}
        }
        out.extend_from_slice(w.as_bytes());
        out.extend_from_slice(SEPARATORS[rng.random_range(0..SEPARATORS.len())].as_bytes());
    }
    out
}

/// Documents with ascending ids that skip some values.
pub fn random_docs(
    rng: &mut StdRng,
    n: usize,
    words: std::ops::Range<usize>,
    mut word: impl FnMut(&mut StdRng) -> String,
) -> Docs {
    let mut id = rng.random_range(0..50u32);
    let mut docs = Vec::with_capacity(n);
    for _ in 0..n {
        let k = rng.random_range(words.clone());
        docs.push((id, random_text(rng, k, &mut word)));
        id += 1 + if rng.random_bool(0.3) {
            rng.random_range(0..40)
        } else {
            0
        };
    }
    docs
}

/// What the brute-force model knows about a document: its tokens in order,
/// lowercased, each with a flag that is false for a token that cannot match a
/// term (too long to index, or not valid UTF-8).
pub struct Model {
    pub ids: Vec<u32>,
    pub toks: Vec<Vec<(String, bool)>>,
}

pub fn model(docs: &Docs) -> Model {
    let mut m = Model {
        ids: Vec::new(),
        toks: Vec::new(),
    };
    for (id, text) in docs {
        m.ids.push(*id);
        m.toks.push(
            tokens(text)
                .map(|t| {
                    // A token that is not valid UTF-8 can never equal a string
                    // of a query, so the model treats it as one that does not
                    // match anything but still occupies a position.
                    match String::from_utf8(t.raw().to_ascii_lowercase()) {
                        Ok(s) => (s, t.is_indexable()),
                        Err(e) => (String::from_utf8_lossy(e.as_bytes()).into_owned(), false),
                    }
                })
                .collect(),
        );
    }
    m
}

impl Model {
    pub fn distinct_terms(&self) -> BTreeSet<String> {
        self.toks
            .iter()
            .flatten()
            .filter(|t| t.1)
            .map(|t| t.0.clone())
            .collect()
    }

    pub fn docs_where(&self, f: impl Fn(&[(String, bool)]) -> bool) -> Vec<u32> {
        self.ids
            .iter()
            .zip(&self.toks)
            .filter(|(_, t)| f(t))
            .map(|(&i, _)| i)
            .collect()
    }

    pub fn term(&self, w: &str) -> Vec<u32> {
        self.docs_where(|t| t.iter().any(|x| x.1 && x.0 == w))
    }

    pub fn prefix(&self, p: &str) -> Vec<u32> {
        self.docs_where(|t| t.iter().any(|x| x.1 && x.0.starts_with(p)))
    }

    /// Slides the phrase over the token stream, the obvious way.
    pub fn phrase(&self, ph: &[String]) -> Vec<u32> {
        self.docs_where(|t| {
            t.len() >= ph.len()
                && (0..=t.len() - ph.len()).any(|s| ph.iter().zip(&t[s..]).all(|(p, x)| &x.0 == p))
        })
    }

    /// Evaluates a query over every document.
    pub fn query(&self, q: &Query) -> Vec<u32> {
        self.docs_where(|t| doc_matches(t, q))
    }
}

fn doc_matches(t: &[(String, bool)], q: &Query) -> bool {
    match q {
        Query::Term(w) => t.iter().any(|x| x.1 && &x.0 == w),
        Query::Prefix(p) => t.iter().any(|x| x.1 && x.0.starts_with(p.as_str())),
        Query::Phrase(ph) => {
            t.len() >= ph.len()
                && (0..=t.len() - ph.len()).any(|s| ph.iter().zip(&t[s..]).all(|(p, x)| &x.0 == p))
        }
        Query::And(v) => v.iter().all(|c| doc_matches(t, c)),
        Query::Or(v) => v.iter().any(|c| doc_matches(t, c)),
        Query::Not(c) => !doc_matches(t, c),
    }
}
