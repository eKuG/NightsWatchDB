//! Review: queries whose words are longer than the index can hold. A token
//! over 64 bytes is not indexed (README, "Tokeniser"), but the parser turns
//! such a word into a plain `Term` (or keeps a prefix that only matches
//! indexed terms), and the documented leaf mapping then answers it from the
//! index alone. The search must either find the document or refuse the query;
//! returning nothing is a silently wrong answer.

mod support;

use std::collections::HashMap;

use nwdb_core::{Error, Result};
use nwdb_textindex::{IndexReader, Leaf, evaluate, parse, phrase_candidates, phrase_matches};
use support::*;

/// Parses `text` and answers it over `docs` the way the README tells a caller
/// to. `None` means the parser refused the query.
fn search(docs: &Docs, text: &str) -> Option<Result<Vec<u32>>> {
    let q = match parse(text) {
        Ok(q) => q,
        Err(Error::InvalidArgument(_)) => return None,
        Err(e) => panic!("{text:?}: {e:?}"),
    };
    let (idx, _) = build(docs, usize::MAX);
    let r = IndexReader::open(idx.as_slice()).unwrap();
    let texts: HashMap<u32, &[u8]> = docs.iter().map(|(i, t)| (*i, t.as_slice())).collect();
    let universe: Vec<u32> = docs.iter().map(|(i, _)| *i).collect();
    Some(evaluate(&q, &universe, |l| {
        Ok(match l {
            // candidates for a word over 64 bytes are confirmed in the text
            Leaf::Term(w) => term_ids(&r, &texts, w)?,
            Leaf::Prefix(p) => r.prefix(p, 1000)?.into_vec(),
            Leaf::Phrase(p) => phrase_candidates(&r, p)?
                .ids
                .into_iter()
                .filter(|id| phrase_matches(texts[id], p))
                .collect(),
        })
    }))
}

/// A SHA-512 digest in hex: one 128-byte token, the kind of identifier that
/// shows up in tool calls and logs.
fn digest() -> String {
    (0..128)
        .map(|i| char::from(b"0123456789abcdef"[(i * 7 + 3) % 16]))
        .collect()
}

#[test]
fn a_word_longer_than_64_bytes_is_found() {
    let d = digest();
    let docs: Docs = vec![
        (0, format!("upload finished, sha512 {d} ok").into_bytes()),
        (1, b"upload finished, sha512 missing".to_vec()),
    ];
    let got = search(&docs, &d).expect("the parser accepts the word");
    assert_eq!(
        got.unwrap(),
        [0],
        "searching a word that is in document 0 must find it"
    );
}

#[test]
fn a_prefix_matches_a_document_whose_only_match_is_a_long_token() {
    // document 0 holds a token that starts with "sk" but is 80 bytes long;
    // document 1 holds a short one
    let key = format!("sk{}", "Q7".repeat(39));
    let docs: Docs = vec![
        (0, format!("using key {key} for the call").into_bytes()),
        (1, b"skip the call".to_vec()),
    ];
    let got = search(&docs, "sk*").expect("the parser accepts the prefix");
    assert_eq!(
        got.unwrap(),
        [0, 1],
        "both documents hold a token starting with \"sk\""
    );
}
