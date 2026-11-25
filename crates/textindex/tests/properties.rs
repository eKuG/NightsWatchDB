//! Searches against a brute-force scan: single terms, prefixes, phrases
//! (candidates plus verification) and the generic evaluator must agree with
//! the model for random documents, with one segment and with many, and with
//! gaps in the document ids.

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use nwdb_core::Error;
use nwdb_textindex::{
    IndexReader, Leaf, Query, evaluate, ids, leaves, phrase_candidates, phrase_matches,
};
use rand::Rng;
use rand::rngs::StdRng;
use support::*;

type TestReader<'a> = IndexReader<&'a [u8]>;

/// A phrase of two or three consecutive indexable tokens of a random
/// document, or a term if the document has none.
fn phrase_from(rng: &mut StdRng, m: &Model) -> Query {
    let d = rng.random_range(0..m.toks.len());
    let t = &m.toks[d];
    for _ in 0..20 {
        if t.len() < 3 {
            break;
        }
        let n = rng.random_range(2..=3).min(t.len());
        let s = rng.random_range(0..=t.len() - n);
        if t[s..s + n].iter().all(|x| x.1) {
            return Query::Phrase(t[s..s + n].iter().map(|x| x.0.clone()).collect());
        }
    }
    Query::Term("w1".into())
}

fn random_query(rng: &mut StdRng, m: &Model, terms: &[String], depth: usize) -> Query {
    let pick = |rng: &mut StdRng| terms[rng.random_range(0..terms.len())].clone();
    match rng.random_range(0..if depth >= 3 { 3 } else { 7 }) {
        0 => Query::Term(pick(rng)),
        1 => {
            let t = pick(rng);
            let mut n = rng.random_range(2..=t.len().max(2)).min(t.len());
            while !t.is_char_boundary(n) {
                n -= 1;
            }
            Query::Prefix(t[..n].to_string())
        }
        2 => phrase_from(rng, m),
        3 | 4 => {
            let items = (0..rng.random_range(2..4))
                .map(|_| random_query(rng, m, terms, depth + 1))
                .collect();
            if rng.random_bool(0.5) {
                Query::And(items)
            } else {
                Query::Or(items)
            }
        }
        _ => Query::Not(Box::new(random_query(rng, m, terms, depth + 1))),
    }
}

/// Answers each leaf of a query from the index, verifying phrases against
/// the text, as a caller would.
fn leaf_ids(
    r: &TestReader<'_>,
    texts: &HashMap<u32, &[u8]>,
    leaf: &Leaf<'_>,
) -> nwdb_core::Result<Vec<u32>> {
    Ok(match leaf {
        Leaf::Term(t) => r.term(t)?.into_vec(),
        Leaf::Prefix(p) => r.prefix(p, usize::MAX)?.into_vec(),
        Leaf::Phrase(ph) => phrase_candidates(r, ph)?
            .into_iter()
            .filter(|id| phrase_matches(texts[id], ph))
            .collect(),
    })
}

fn check_all(docs: &Docs, budget: usize, seed: u64, words: &[String], queries: usize) {
    let (idx, meta) = build(docs, budget);
    let r = IndexReader::open(idx.as_slice()).unwrap();
    let m = model(docs);
    assert_eq!(r.meta(), &meta);
    assert_eq!(r.doc_count(), docs.len() as u64);
    let terms = m.distinct_terms();
    if budget == usize::MAX {
        assert_eq!(meta.segments, 1);
    } else if budget <= 2_000 {
        assert!(meta.segments > 1, "budget {budget} gave one segment");
    }

    // every term, and some that are absent or in the wrong form
    for t in terms.iter().take(2000) {
        assert_eq!(r.term(t).unwrap().as_slice(), m.term(t), "term {t}");
    }
    let long = "q".repeat(80);
    for absent in ["zzzzz", "", "w", "W1", long.as_str()] {
        assert_eq!(
            r.term(absent).unwrap().as_slice(),
            m.term(absent),
            "{absent}"
        );
    }

    // prefixes, with the limit exactly at and just under the match count
    let mut rng = rng(seed);
    let mut prefixes: Vec<String> = ["w", "w0", "w00", "w1", "zz", "ab"]
        .map(String::from)
        .to_vec();
    for _ in 0..30 {
        let t = words[rng.random_range(0..words.len())].clone();
        let n = rng.random_range(1..=3).min(t.len());
        prefixes.push(t[..n].to_string());
    }
    for p in &prefixes {
        let matching = terms.iter().filter(|t| t.starts_with(p.as_str())).count();
        assert_eq!(
            r.prefix(p, matching).unwrap().as_slice(),
            m.prefix(p),
            "prefix {p}"
        );
        if matching > 0 {
            match r.prefix(p, matching - 1) {
                Err(Error::InvalidArgument(_)) => {}
                other => panic!("prefix {p} with a limit under its {matching} terms: {other:?}"),
            }
        }
    }

    // intersection of several terms
    let tl: Vec<&String> = terms.iter().collect();
    for _ in 0..40 {
        let k = rng.random_range(1..=3);
        let pick: Vec<&String> = (0..k).map(|_| tl[rng.random_range(0..tl.len())]).collect();
        let want: Vec<u32> =
            m.docs_where(|t| pick.iter().all(|w| t.iter().any(|x| x.1 && &x.0 == *w)));
        assert_eq!(r.intersect(&pick).unwrap().as_slice(), want, "{pick:?}");
    }

    // phrases: candidates contain every match, and verification is exact
    let texts: HashMap<u32, &[u8]> = docs.iter().map(|(i, t)| (*i, t.as_slice())).collect();
    for _ in 0..40 {
        let Query::Phrase(ph) = phrase_from(&mut rng, &m) else {
            continue;
        };
        let cand = phrase_candidates(&r, &ph).unwrap();
        let want = m.phrase(&ph);
        assert!(want.iter().all(|id| cand.contains(*id)), "{ph:?}");
        let got: Vec<u32> = cand
            .into_iter()
            .filter(|id| phrase_matches(texts[id], &ph))
            .collect();
        assert_eq!(got, want, "{ph:?}");
    }

    // the evaluator over random queries, universe = every document
    let universe: Vec<u32> = m.ids.clone();
    let words_vec: Vec<String> = terms.iter().cloned().collect();
    for _ in 0..queries {
        let q = random_query(&mut rng, &m, &words_vec, 0);
        let got = evaluate(&q, &universe, |l| leaf_ids(&r, &texts, l)).unwrap();
        assert_eq!(got, m.query(&q), "{q:?}");
        assert!(!leaves(&q).is_empty());
    }
}

#[test]
fn small_vocabulary_one_segment_and_many() {
    for seed in 0..4u64 {
        let mut rng = rng(seed);
        let docs = random_docs(&mut rng, 300, 0..60, |r| vocab_word(r.random_range(0..40)));
        let words: Vec<String> = (0..40).map(vocab_word).collect();
        for budget in [usize::MAX, 20_000, 2_000, 1] {
            check_all(&docs, budget, seed, &words, 40);
        }
    }
}

fn random_word(r: &mut StdRng) -> String {
    let n = r.random_range(1..13);
    (0..n)
        .map(|_| {
            if r.random_bool(0.1) {
                char::from(b'0' + r.random_range(0..10))
            } else {
                char::from(b'a' + r.random_range(0..26))
            }
        })
        .collect()
}

#[test]
fn large_vocabulary_one_segment_and_many() {
    for seed in 10..12u64 {
        let mut rng = rng(seed);
        let vocab: Vec<String> = (0..30_000).map(|_| random_word(&mut rng)).collect();
        let docs = random_docs(&mut rng, 400, 20..200, |r| {
            // a few common words, most words rare
            if r.random_bool(0.3) {
                vocab[r.random_range(0..50)].clone()
            } else {
                vocab[r.random_range(0..vocab.len())].clone()
            }
        });
        for budget in [usize::MAX, 150_000, 1] {
            check_all(&docs, budget, seed, &vocab, 25);
        }
    }
}

#[test]
fn long_lists_cross_block_boundaries() {
    let mut rng = rng(99);
    let mut docs: Docs = Vec::new();
    let mut id = 0u32;
    for i in 0..5000u32 {
        let mut t = String::from("common");
        if i % 2 == 0 {
            t.push_str(" half");
        }
        if i % 3 == 0 {
            t.push_str(" third");
        }
        if i == 4999 || i == 130 || i % 997 == 0 {
            t.push_str(" rare");
        }
        docs.push((id, t.into_bytes()));
        id += 1 + if rng.random_bool(0.2) {
            rng.random_range(0..5)
        } else {
            0
        };
    }
    let m = model(&docs);
    for budget in [usize::MAX, 6_000] {
        let (idx, _) = build(&docs, budget);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        for t in ["common", "half", "third", "rare"] {
            assert_eq!(r.term(t).unwrap().as_slice(), m.term(t), "{t}");
        }
        let want = m.docs_where(|t| {
            ["common", "half", "third"]
                .iter()
                .all(|w| t.iter().any(|x| &x.0 == w))
        });
        assert_eq!(
            r.intersect(&["common", "half", "third"])
                .unwrap()
                .as_slice(),
            want
        );
        assert_eq!(
            r.intersect(&["rare", "third", "half"]).unwrap().as_slice(),
            {
                m.docs_where(|t| {
                    ["rare", "third", "half"]
                        .iter()
                        .all(|w| t.iter().any(|x| &x.0 == w))
                })
            }
        );
        assert_eq!(r.prefix("th", 10).unwrap().as_slice(), m.term("third"));
        assert_eq!(r.prefix("co", 10).unwrap().len(), docs.len());
        // a union of lists that overlap: ha(lf), th(ird), ra(re)
        let all = m.docs_where(|t| {
            t.iter()
                .any(|x| x.0 == "half" || x.0 == "third" || x.0 == "rare")
        });
        let u = ids::union(
            &ids::union(
                r.prefix("ha", 9).unwrap().as_slice(),
                r.prefix("th", 9).unwrap().as_slice(),
            ),
            r.prefix("ra", 9).unwrap().as_slice(),
        );
        assert_eq!(u, all);
    }
}

#[test]
fn builder_ordering_and_degenerate_indexes() {
    use nwdb_textindex::{IndexBuilder, IndexOptions};
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(IndexOptions::default(), &mut out);
    b.add(5, b"hello world").unwrap();
    for bad in [5u32, 4, 0] {
        assert!(matches!(b.add(bad, b"x"), Err(Error::InvalidArgument(_))));
    }
    b.add(6, b"!!! ...").unwrap(); // no tokens, still a document
    b.add(u32::MAX, b"hello").unwrap();
    let meta = b.finish().unwrap();
    assert_eq!((meta.docs, meta.segments), (3, 1));
    let r = IndexReader::open(out.as_slice()).unwrap();
    assert_eq!(r.doc_count(), 3);
    assert_eq!(r.term("hello").unwrap().as_slice(), [5, u32::MAX]);
    assert_eq!(r.term("world").unwrap().as_slice(), [5]);
    assert_eq!(r.intersect(&["hello", "world"]).unwrap().as_slice(), [5]);
    assert!(matches!(
        r.intersect::<&str>(&[]),
        Err(Error::InvalidArgument(_))
    ));

    // an index of no documents, and one of documents without tokens
    let (empty, meta) = build(&Vec::new(), usize::MAX);
    assert_eq!(meta.segments, 0);
    let r = IndexReader::open(empty.as_slice()).unwrap();
    assert!(r.term("a").unwrap().is_empty() && r.prefix("ab", 1).unwrap().is_empty());
    let (blank, _) = build(&vec![(0, b"...".to_vec()), (1, b"".to_vec())], usize::MAX);
    let r = IndexReader::open(blank.as_slice()).unwrap();
    assert_eq!(r.doc_count(), 2);
    assert!(r.term("a").unwrap().is_empty());
    r.verify().unwrap();
}

#[test]
fn a_file_reader_matches_memory_and_serves_many_threads() {
    use std::io::Write;
    let mut rng = rng(5);
    let docs = random_docs(&mut rng, 500, 5..60, |r| vocab_word(r.random_range(0..40)));
    let (idx, _) = build(&docs, 4_000);
    let path = std::env::temp_dir().join(format!("nwdb-textindex-test-{}", std::process::id()));
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&idx)
        .unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let from_file = Arc::new(IndexReader::open(file).unwrap());
    let from_vec = IndexReader::open(idx.clone()).unwrap();
    let m = model(&docs);
    std::thread::scope(|s| {
        for t in 0..4 {
            let (r, m) = (&from_file, &m);
            s.spawn(move || {
                for i in 0..40 {
                    let w = vocab_word((i * 7 + t) % 40);
                    assert_eq!(r.term(&w).unwrap().as_slice(), m.term(&w));
                    assert_eq!(r.prefix("w0", 1000).unwrap().as_slice(), m.prefix("w0"));
                }
            });
        }
    });
    for i in 0..40 {
        let w = vocab_word(i);
        assert_eq!(from_file.term(&w).unwrap(), from_vec.term(&w).unwrap());
    }
    std::fs::remove_file(&path).unwrap();
}
