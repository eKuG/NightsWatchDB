//! A document that does not fit the open segment is cut between tokens and
//! continues in the next segment, so one document id is in two or more
//! consecutive segments. Every kind of search must still see it once, and an
//! index built with the smallest budget must answer as one built with a huge
//! budget.
//!
//! A segment closes when its estimate passes 1 MiB, which about 10,000 distinct
//! words reach, so a document of 30,000 distinct words is cut into several.

mod support;

use nwdb_textindex::{
    IndexBuilder, IndexMeta, IndexOptions, IndexReader, evaluate, parse, phrase_candidates,
    phrase_matches,
};
use rand::Rng;
use std::collections::BTreeSet;
use support::*;

const SMALL: usize = 1 << 20;
const HUGE: usize = 1 << 30;

/// `n` distinct words `{tag}{i:06}` in order, so word `i` is early or late.
fn words(tag: &str, n: usize) -> Vec<u8> {
    let mut out = String::new();
    for i in 0..n {
        out.push_str(&format!("{tag}{i:06} "));
    }
    out.into_bytes()
}

fn index(docs: &Docs, budget: usize) -> (IndexReader<Vec<u8>>, IndexMeta) {
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
    (IndexReader::open(out).unwrap(), meta)
}

#[test]
fn a_word_of_a_split_document_is_found_once() {
    let docs: Docs = vec![
        (3, words("a", 30_000)),
        (4, b"a000010 other".to_vec()),
        (9, words("b", 30_000)),
    ];
    let (r, meta) = index(&docs, SMALL);
    assert!(meta.segments >= 6, "{meta:?}");
    assert_eq!(meta.docs, 3);
    assert_eq!(r.doc_count(), 3);
    r.verify().unwrap();
    // early, middle and late words of doc 3
    for w in ["a000000", "a015000", "a029999"] {
        assert_eq!(r.term(w).unwrap().ids.as_slice(), [3], "{w}");
    }
    assert_eq!(r.term("a000010").unwrap().ids.as_slice(), [3, 4]);
    assert_eq!(r.term("b029999").unwrap().ids.as_slice(), [9]);
    assert!(r.term("a030000").unwrap().ids.is_empty());
}

#[test]
fn and_finds_an_early_and_a_late_word_of_the_same_split_document() {
    let docs: Docs = vec![
        (3, words("a", 30_000)),
        (4, b"a000000 a029999".to_vec()),
        (5, b"a000000".to_vec()),
        (9, words("b", 30_000)),
    ];
    let (r, _) = index(&docs, SMALL);
    let both = r.intersect(&["a000000", "a029999"]).unwrap();
    assert_eq!(both.ids.as_slice(), [3, 4]);
    assert!(both.exact);
    let three = r.intersect(&["a000000", "a015000", "a029999"]).unwrap();
    assert_eq!(three.ids.as_slice(), [3]);
    // words of two different documents never meet
    assert!(r.intersect(&["a000000", "b000000"]).unwrap().ids.is_empty());
    // the candidates of a phrase over the cut
    let cands = phrase_candidates(&r, &["a009999", "a010000"]).unwrap();
    assert_eq!(cands.ids.as_slice(), [3]);
}

#[test]
fn a_phrase_across_the_cut_is_a_candidate_and_is_confirmed_by_the_text() {
    let docs: Docs = vec![(7, words("a", 30_000))];
    let (r, meta) = index(&docs, SMALL);
    assert!(meta.segments >= 3, "{meta:?}");
    let text = &docs[0].1;
    // every adjacent pair is a candidate and a real phrase
    for i in (0..29_999).step_by(997) {
        let p = [format!("a{i:06}"), format!("a{:06}", i + 1)];
        let c = phrase_candidates(&r, &p).unwrap();
        assert_eq!(c.ids.as_slice(), [7], "{p:?}");
        assert!(phrase_matches(text, &p));
    }
    // two words of the document that are not adjacent are a candidate only
    let p = ["a000001", "a029998"];
    assert_eq!(phrase_candidates(&r, &p).unwrap().ids.as_slice(), [7]);
    assert!(!phrase_matches(text, &p));
}

#[test]
fn a_prefix_finds_a_split_document_once() {
    let docs: Docs = vec![(2, words("zz", 30_000)), (8, words("zy", 30_000))];
    let (r, _) = index(&docs, SMALL);
    assert_eq!(r.prefix("zz", 1_000_000).unwrap().as_slice(), [2]);
    assert_eq!(r.prefix("zy0", 1_000_000).unwrap().as_slice(), [8]);
    assert_eq!(r.prefix("zz0299", 1_000_000).unwrap().as_slice(), [2]);
    assert_eq!(r.prefix("z", 1_000_000).unwrap().as_slice(), [2, 8]);
}

#[test]
fn a_long_word_of_a_split_document_is_listed_and_confirmed() {
    let long_early = "e".repeat(70);
    let long_late = "l".repeat(70);
    let mut text = long_early.clone().into_bytes();
    text.push(b' ');
    text.extend(words("a", 30_000));
    text.extend_from_slice(long_late.as_bytes());
    let docs: Docs = vec![(1, text), (2, long_late.clone().into_bytes())];
    let (r, meta) = index(&docs, SMALL);
    assert!(meta.segments >= 3, "{meta:?}");
    let early = r.term(&long_early).unwrap();
    assert_eq!((early.ids.as_slice(), early.exact), (&[1][..], false));
    assert_eq!(r.term(&long_late).unwrap().ids.as_slice(), [1, 2]);
    let both = r
        .intersect(&[long_early.as_str(), long_late.as_str()])
        .unwrap();
    assert_eq!(both.ids.as_slice(), [1]);
    assert_eq!(r.prefix("eeee", 10).unwrap().as_slice(), [1]);
}

#[test]
fn not_works_over_split_documents() {
    let docs: Docs = vec![
        (3, words("a", 30_000)),
        (4, b"a000010".to_vec()),
        (5, b"plain".to_vec()),
        (9, words("b", 30_000)),
    ];
    let (r, _) = index(&docs, SMALL);
    let universe: Vec<u32> = docs.iter().map(|d| d.0).collect();
    let run = |q: &str| -> Vec<u32> {
        let query = parse(q).unwrap();
        evaluate(&query, &universe, |leaf| match leaf {
            nwdb_textindex::Leaf::Term(t) => Ok(r.term(t)?.ids.into_vec()),
            nwdb_textindex::Leaf::Prefix(p) => Ok(r.prefix(p, 1_000_000)?.into_vec()),
            nwdb_textindex::Leaf::Phrase(p) => Ok(phrase_candidates(&r, p)?.ids.into_vec()),
        })
        .unwrap()
    };
    assert_eq!(run("a000010 -a029999"), [4]);
    assert_eq!(run("a000010 -plain"), [3, 4]);
    assert_eq!(run("plain -a000010 -b000000"), [5]);
    assert_eq!(run("a029999 OR b000000"), [3, 9]);
}

#[test]
fn a_segment_that_holds_only_the_middle_of_a_document_is_valid() {
    // one document large enough to fill several segments by itself, followed
    // by another
    let docs: Docs = vec![(10, words("m", 60_000)), (11, b"m000000 last".to_vec())];
    let (r, meta) = index(&docs, SMALL);
    assert!(meta.segments >= 5, "{meta:?}");
    assert_eq!(meta.docs, 2);
    r.verify().unwrap();
    assert_eq!(r.term("m000000").unwrap().ids.as_slice(), [10, 11]);
    assert_eq!(r.term("last").unwrap().ids.as_slice(), [11]);
    assert_eq!(
        r.intersect(&["m000000", "m059999"]).unwrap().ids.as_slice(),
        [10]
    );
}

/// A random document: runs of fresh words (which fill the segment) mixed with
/// words from a small vocabulary and now and then a long word.
fn random_doc(rng: &mut rand::rngs::StdRng, fresh: usize, serial: &mut u32) -> Vec<u8> {
    let mut out = String::new();
    for _ in 0..fresh {
        if rng.random_bool(0.2) {
            out.push_str(&format!("v{} ", rng.random_range(0..40)));
        } else if rng.random_bool(0.001) {
            out.push_str(&format!("{}{} ", "q".repeat(70), rng.random_range(0..3)));
        } else {
            *serial += 1;
            out.push_str(&format!("f{serial} "));
        }
    }
    out.into_bytes()
}

/// Every kind of answer for one set of queries.
fn answers(r: &IndexReader<Vec<u8>>, queries: &[String], universe: &[u32]) -> Vec<Vec<u32>> {
    let mut out = Vec::new();
    for q in queries {
        let query = parse(q).unwrap();
        let ids = evaluate(&query, universe, |leaf| match leaf {
            nwdb_textindex::Leaf::Term(t) => Ok(r.term(t)?.ids.into_vec()),
            nwdb_textindex::Leaf::Prefix(p) => Ok(r.prefix(p, 1_000_000)?.into_vec()),
            nwdb_textindex::Leaf::Phrase(p) => Ok(phrase_candidates(r, p)?.ids.into_vec()),
        })
        .unwrap();
        out.push(ids);
    }
    out
}

#[test]
fn a_tiny_budget_and_a_huge_budget_give_the_same_answers() {
    let mut rng = rng(42);
    let mut serial = 0u32;
    for round in 0..3 {
        let mut docs: Docs = Vec::new();
        let mut id = rng.random_range(0..10u32);
        for _ in 0..rng.random_range(6..12) {
            let fresh = if rng.random_bool(0.5) {
                rng.random_range(10..200)
            } else {
                rng.random_range(8_000..40_000)
            };
            docs.push((id, random_doc(&mut rng, fresh, &mut serial)));
            id += rng.random_range(1..5);
        }
        let (small, small_meta) = index(&docs, SMALL);
        let (huge, huge_meta) = index(&docs, HUGE);
        assert_eq!(huge_meta.segments, 1);
        assert!(small_meta.segments > docs.len() as u32 / 2, "round {round}");
        assert_eq!(small_meta.docs, huge_meta.docs);
        small.verify().unwrap();

        let m = model(&docs);
        let terms: Vec<String> = m.distinct_terms().into_iter().collect();
        let pick = |rng: &mut rand::rngs::StdRng| terms[rng.random_range(0..terms.len())].clone();
        let mut queries: Vec<String> = Vec::new();
        for _ in 0..60 {
            let (a, b, c) = (pick(&mut rng), pick(&mut rng), pick(&mut rng));
            queries.push(a.clone());
            queries.push(format!("{a} {b}"));
            queries.push(format!("{a} {b} {c}"));
            queries.push(format!("{a} OR {b}"));
            queries.push(format!("{a} -{b}"));
            queries.push(format!("\"{a} {b}\""));
            let cut = a.len().clamp(2, 4);
            queries.push(format!("{}*", &a[..cut.min(a.len())]));
        }
        for p in ["f1*", "f12*", "v1*", "qq*"] {
            queries.push(p.to_string());
        }
        let universe: Vec<u32> = docs.iter().map(|d| d.0).collect();
        let a = answers(&small, &queries, &universe);
        let b = answers(&huge, &queries, &universe);
        for (i, q) in queries.iter().enumerate() {
            assert_eq!(a[i], b[i], "round {round}: `{q}`");
        }
        // and the terms are what the model says
        for t in terms.iter().take(200) {
            let want: BTreeSet<u32> = m
                .ids
                .iter()
                .zip(&m.toks)
                .filter(|(_, toks)| toks.iter().any(|x| x.1 && &x.0 == t))
                .map(|(i, _)| *i)
                .collect();
            if t.len() <= 64 {
                let got: BTreeSet<u32> = small.term(t).unwrap().ids.into_iter().collect();
                assert_eq!(got, want, "round {round}: term {t}");
            }
        }
    }
}
