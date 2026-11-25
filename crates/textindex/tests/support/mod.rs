//! Helpers shared by the integration tests: a seeded document generator, a
//! brute-force model of the index, and builders.
#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};

use nwdb_core::Result;
use nwdb_textindex::{
    IndexBuilder, IndexMeta, IndexOptions, IndexReader, Query, ReadAt, contains_token, tokens,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub type Docs = Vec<(u32, Vec<u8>)>;

/// Builds an index in memory. A budget of 1 MiB or more is the real segment
/// budget. A smaller one cannot make a segment small (the builder raises it),
/// so it stands for "a segment of about `budget / 200` documents", which
/// gives the many small segments the tests need from little data.
pub fn build(docs: &Docs, budget: usize) -> (Vec<u8>, IndexMeta) {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: budget,
        },
        &mut out,
    );
    if budget < 1 << 20 {
        b = b.with_max_segment_docs((budget / 200).max(1) as u32);
    }
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
/// lowercased, each with a flag that is false for a token that a query written
/// as a string can never name (one that is not valid UTF-8). A token longer
/// than 64 bytes is ordinary here: the model knows nothing of the index's
/// limit, so it is the true answer the index must reproduce.
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
                        Ok(s) => (s, true),
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

/// The documents that hold `word`, answered as a caller answers a `Term`
/// leaf: the reader's ids, confirmed against the text when the reader says
/// they are only candidates.
pub fn term_ids<S: ReadAt>(
    r: &IndexReader<S>,
    texts: &HashMap<u32, &[u8]>,
    word: &str,
) -> Result<Vec<u32>> {
    let m = r.term(word)?;
    assert_eq!(m.exact, word.len() <= 64, "{word}");
    Ok(if m.exact {
        m.ids.into_vec()
    } else {
        m.ids
            .into_iter()
            .filter(|id| contains_token(texts[id], word.as_bytes()))
            .collect()
    })
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

// ------------------------------------------------- counting reads

/// A `ReadAt` over bytes that counts what is read, and can be slowed down so
/// that threads overlap.
pub struct CountingSource {
    data: Vec<u8>,
    read: std::sync::atomic::AtomicU64,
    delay: std::time::Duration,
}

impl CountingSource {
    pub fn new(data: Vec<u8>) -> Self {
        Self::slow(data, std::time::Duration::ZERO)
    }

    /// Every read takes at least `delay`.
    pub fn slow(data: Vec<u8>, delay: std::time::Duration) -> Self {
        Self {
            data,
            read: std::sync::atomic::AtomicU64::new(0),
            delay,
        }
    }

    /// Bytes read since the last call.
    pub fn take(&self) -> u64 {
        self.read.swap(0, std::sync::atomic::Ordering::SeqCst)
    }
}

impl ReadAt for CountingSource {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.data.len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.read
            .fetch_add(buf.len() as u64, std::sync::atomic::Ordering::SeqCst);
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        <[u8] as ReadAt>::read_exact_at(&self.data, buf, offset)
    }
}

// ------------------------------------------------- hand-written files

pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// What a hand-written dictionary entry points at.
pub enum Hand {
    /// A list of at most 8 ids kept inside the entry.
    Inline(Vec<u32>),
    /// A list of `df` ids in the postings area at `off`.
    List { df: u64, off: u64 },
}

/// An index of one segment written by hand from the documented layout, with
/// every checksum valid. `entries` must be in ascending key order; the
/// segment holds documents `0..docs`.
pub fn hand_index(postings: &[u8], entries: &[(&str, Hand)], docs: u32) -> Vec<u8> {
    let (mut dict, mut sparse) = (Vec::new(), Vec::new());
    let (mut prev, mut dict_blocks, mut total) = (&[][..], 0u32, 0u64);
    for (i, (name, hand)) in entries.iter().enumerate() {
        let t = name.as_bytes();
        if i % 32 == 0 {
            prev = &[];
            put_varint(&mut sparse, t.len() as u64);
            sparse.extend_from_slice(t);
            sparse.extend_from_slice(&(dict.len() as u64).to_le_bytes());
            dict_blocks += 1;
        }
        let shared = prev.iter().zip(t).take_while(|(a, b)| a == b).count();
        put_varint(&mut dict, shared as u64);
        put_varint(&mut dict, (t.len() - shared) as u64);
        dict.extend_from_slice(&t[shared..]);
        prev = t;
        match hand {
            Hand::Inline(ids) => {
                put_varint(&mut dict, ids.len() as u64);
                let mut next_min = 0u64;
                for &id in ids {
                    put_varint(&mut dict, u64::from(id) - next_min);
                    next_min = u64::from(id) + 1;
                }
                total += ids.len() as u64;
            }
            Hand::List { df, off } => {
                put_varint(&mut dict, *df);
                put_varint(&mut dict, *off);
                total += df;
            }
        }
    }
    let mut file = postings.to_vec();
    file.extend_from_slice(&dict);
    file.extend_from_slice(&sparse);
    let body_crc = crc32c::crc32c_append(crc32c::crc32c(postings), &dict);
    let mut dir = Vec::new();
    for v in [
        postings.len() as u64,
        dict.len() as u64,
        sparse.len() as u64,
        entries.len() as u64,
        total,
    ] {
        dir.extend_from_slice(&v.to_le_bytes());
    }
    for v in [
        docs,
        0,
        docs - 1,
        dict_blocks,
        body_crc,
        crc32c::crc32c(&sparse),
    ] {
        dir.extend_from_slice(&v.to_le_bytes());
    }
    dir.extend_from_slice(&0u64.to_le_bytes());
    let mut footer = [0u8; 64];
    footer[0..4].copy_from_slice(&2u32.to_le_bytes());
    footer[4..8].copy_from_slice(&1u32.to_le_bytes());
    footer[8..12].copy_from_slice(&32u32.to_le_bytes());
    footer[16..24].copy_from_slice(&u64::from(docs).to_le_bytes());
    footer[32..40].copy_from_slice(&(entries.len() as u64).to_le_bytes());
    footer[40..48].copy_from_slice(&total.to_le_bytes());
    footer[48..56].copy_from_slice(&(file.len() as u64).to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&dir), &footer[..56]);
    footer[56..60].copy_from_slice(&crc.to_le_bytes());
    footer[60..64].copy_from_slice(b"NWTX");
    file.extend_from_slice(&dir);
    file.extend_from_slice(&footer);
    file
}
