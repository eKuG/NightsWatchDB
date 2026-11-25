//! Size sanity: an index over English-like text must be a small fraction of
//! the text, and its postings must cost far less than the 4 bytes per entry
//! that unpacked ids would. The assertions are loose enough to survive format
//! tuning and tight enough to fail if postings stopped being packed.

mod support;

use nwdb_textindex::IndexReader;
use rand::Rng;
use rand::rngs::StdRng;
use support::*;

const CONSONANTS: &[u8] = b"bcdfghjklmnprstvwz";
const VOWELS: &[u8] = b"aeiouy";

/// A pronounceable word for a rank; common ranks give short words.
fn word_of(rank: u64) -> String {
    let mut h = rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let len = 2 + (rank as f64).log2() as usize / 2 + (h % 3) as usize;
    let mut w = String::new();
    for i in 0..len.min(14) {
        h = h
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let set = if i % 2 == 0 { CONSONANTS } else { VOWELS };
        w.push(set[(h >> 33) as usize % set.len()] as char);
    }
    w
}

/// A document of roughly `bytes` bytes: sentences of Zipf-distributed words.
fn english_like(rng: &mut StdRng, bytes: usize, vocab: u64) -> Vec<u8> {
    let mut s = String::with_capacity(bytes + 100);
    let mut start = true;
    while s.len() < bytes {
        // rank density proportional to 1/rank
        let u: f64 = rng.random();
        let rank = ((vocab as f64).powf(u) as u64).clamp(1, vocab);
        let w = word_of(rank);
        if start {
            let mut c = w.chars();
            s.extend(c.next().map(|f| f.to_ascii_uppercase()));
            s.push_str(c.as_str());
            start = false;
        } else {
            s.push_str(&w);
        }
        match rng.random_range(0..14) {
            0 => {
                s.push_str(". ");
                start = true;
            }
            1 => s.push_str(", "),
            _ => s.push(' '),
        }
    }
    s.into_bytes()
}

#[test]
fn an_index_over_text_is_a_small_fraction_of_it() {
    let mut rng = rng(42);
    let mut docs: Docs = Vec::new();
    let (mut total, mut id) = (0usize, 0u32);
    while total < 4_000_000 {
        let size = rng.random_range(300..3500);
        let text = english_like(&mut rng, size, 30_000);
        total += text.len();
        docs.push((id, text));
        id += 1;
    }
    let (idx, meta) = build(&docs, usize::MAX);
    let share = 100.0 * idx.len() as f64 / total as f64;
    println!(
        "text {total} bytes in {} docs; index {} bytes = {share:.1}% of the text; \
         {} terms, {} postings; postings {} bytes ({:.2} per posting), dictionary {}, sparse {}",
        docs.len(),
        idx.len(),
        meta.terms,
        meta.postings,
        meta.postings_bytes,
        meta.postings_bytes as f64 / meta.postings as f64,
        meta.dictionary_bytes,
        meta.sparse_bytes,
    );
    assert_eq!(meta.text_bytes, total as u64);
    // Unpacked ids would take 4 bytes per entry of a long list, which puts
    // the postings area well above one byte per posting and the index above
    // a third of the text; packed, the postings cost under a byte each.
    assert!(
        meta.postings_bytes < meta.postings,
        "postings are not packed: {} bytes for {} postings",
        meta.postings_bytes,
        meta.postings
    );
    assert!(share < 25.0, "index is {share:.1}% of the text");

    // the index still answers: spot check against a scan
    let r = IndexReader::open(idx.as_slice()).unwrap();
    let m = model(&docs);
    for rank in [1u64, 2, 10, 300, 5000, 29_999] {
        let w = word_of(rank);
        assert_eq!(r.term(&w).unwrap().as_slice(), m.term(&w), "{w}");
    }
}
