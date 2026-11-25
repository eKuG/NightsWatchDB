//! Words longer than 64 bytes, terms that are not valid UTF-8, document ids at
//! the top of the range, and a writer that fails midway.

mod support;

use std::collections::BTreeSet;
use std::io::{self, Write};

use nwdb_core::Error;
use nwdb_textindex::{
    IndexBuilder, IndexOptions, IndexReader, contains_token, phrase_candidates, phrase_matches,
    tokens,
};
use rand::Rng;
use support::*;

fn word(c: u8, n: usize) -> String {
    String::from(char::from(c)).repeat(n)
}

/// Documents 0 to 3 hold tokens that share their first 64 bytes.
fn long_docs() -> (Docs, String, String) {
    let a = format!("{}{}", word(b'a', 64), word(b'b', 20));
    let b = format!("{}{}", word(b'a', 64), word(b'c', 20));
    let docs: Docs = vec![
        (0, format!("key {a} here").into_bytes()),
        (1, format!("key {b} here").into_bytes()),
        (2, b"key short".to_vec()),
        (3, format!("KEY {} HERE", a.to_uppercase()).into_bytes()),
        (
            4,
            format!("{} and {}", word(b'a', 64), word(b'a', 65)).into_bytes(),
        ),
    ];
    (docs, a, b)
}

fn confirmed(r: &IndexReader<&[u8]>, docs: &Docs, token: &str) -> Vec<u32> {
    let m = r.term(token).unwrap();
    assert_eq!(m.exact, token.len() <= 64, "{token}");
    m.ids
        .into_iter()
        .filter(|id| {
            let text = &docs.iter().find(|d| d.0 == *id).unwrap().1;
            m.exact || contains_token(text, token.as_bytes())
        })
        .collect()
}

#[test]
fn a_word_over_64_bytes_is_found_as_candidates_and_confirmed_in_the_text() {
    let (docs, a, b) = long_docs();
    for budget in [usize::MAX, 400] {
        let (idx, _) = build(&docs, budget);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        // candidates: every document whose token starts with the same 64 bytes
        let m = r.term(&a).unwrap();
        assert!(!m.exact);
        assert_eq!(m.ids.as_slice(), [0, 1, 3, 4]);
        assert_eq!(r.term(&b).unwrap().ids.as_slice(), [0, 1, 3, 4]);
        // confirmation tells the two apart, and folds case
        assert_eq!(confirmed(&r, &docs, &a), [0, 3]);
        assert_eq!(confirmed(&r, &docs, &b), [1]);
        // the limit is exact: 64 bytes is a term of its own, 65 is not
        assert_eq!(confirmed(&r, &docs, &word(b'a', 64)), [4]);
        assert_eq!(confirmed(&r, &docs, &word(b'a', 65)), [4]);
        assert!(r.term(word(b'a', 64)).unwrap().exact);
        assert!(!r.term(word(b'a', 65)).unwrap().exact);
        // a long word nobody holds
        assert!(confirmed(&r, &docs, &word(b'z', 70)).is_empty());
        // intersections: exact only when every token is short
        let both = r.intersect(&["key", a.as_str()]).unwrap();
        assert!(!both.exact);
        assert_eq!(both.ids.as_slice(), [0, 1, 3]);
        let short = r.intersect(&["key", "short"]).unwrap();
        assert!(short.exact && short.ids.as_slice() == [2]);
    }
}

#[test]
fn a_prefix_sees_long_tokens_and_is_exact() {
    let (docs, a, _) = long_docs();
    for budget in [usize::MAX, 400] {
        let (idx, _) = build(&docs, budget);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        assert_eq!(r.prefix(&a[..10], 10).unwrap().as_slice(), [0, 1, 3, 4]);
        // a prefix of exactly 64 bytes
        assert_eq!(r.prefix(&a[..64], 10).unwrap().as_slice(), [0, 1, 3, 4]);
        // the prefix is of the token, not of its first 64 bytes only
        assert_eq!(r.prefix("ke", 10).unwrap().as_slice(), [0, 1, 2, 3]);
        // an empty prefix is every token, long ones included, once each
        assert_eq!(r.prefix("", 100).unwrap().as_slice(), [0, 1, 2, 3, 4]);
        // six distinct keys: key, here, short, and, the 64 byte word, and the
        // one long key that the other long tokens share
        assert_eq!(r.prefix("", usize::MAX).unwrap().len(), 5);
        assert!(matches!(r.prefix("", 5), Err(Error::InvalidArgument(_))));
        assert!(r.prefix("", 6).is_ok());
        // a prefix longer than 64 bytes is refused, not answered with nothing
        assert!(matches!(
            r.prefix(&a[..65], 10),
            Err(Error::InvalidArgument(_))
        ));
        // the namespace byte never leaks into queries
        assert!(r.prefix([0u8], 10).unwrap().is_empty());
        assert!(r.prefix([0u8, b'a'], 10).unwrap().is_empty());
        assert!(r.term(b"\0aaaa").unwrap().ids.is_empty());
        let key = [&[0u8][..], word(b'a', 64).as_bytes()].concat();
        assert!(r.term(&key).unwrap().ids.is_empty());
        assert!(r.intersect(&[&key[..10]]).unwrap().ids.is_empty());
    }
}

#[test]
fn phrases_with_long_tokens_use_their_candidates() {
    let (docs, a, b) = long_docs();
    for budget in [usize::MAX, 400] {
        let (idx, _) = build(&docs, budget);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        let texts = |id: u32| &docs.iter().find(|d| d.0 == id).unwrap().1;
        let run = |ph: &[&str]| -> Vec<u32> {
            let m = phrase_candidates(&r, ph).unwrap();
            assert!(!m.exact || ph.len() == 1);
            m.ids
                .into_iter()
                .filter(|&id| phrase_matches(texts(id), ph))
                .collect()
        };
        assert_eq!(run(&["key", &a, "here"]), [0, 3]);
        assert_eq!(run(&["key", &b, "here"]), [1]);
        // a phrase of only long tokens works
        assert_eq!(run(&[&a]), [0, 3]);
        assert_eq!(run(&[&a, "here"]), [0, 3]);
        let two = [word(b'a', 64), word(b'a', 65)];
        assert_eq!(run(&[&two[0], "and", &two[1]]), [4]);
        assert!(run(&["here", &a]).is_empty());
    }
}

#[test]
fn contains_token_matches_whole_tokens_with_case_folded() {
    let long = word(b'x', 90);
    let text = format!("Hello, World! foo-bar {long} caf\u{e9} x\u{ff}y");
    let has = |t: &str| contains_token(text.as_bytes(), t.as_bytes());
    assert!(has("hello") && has("HELLO") && has("world") && has("foo") && has("bar"));
    assert!(has(&long) && has(&long.to_uppercase()));
    assert!(has("caf\u{e9}"));
    assert!(has("x\u{ff}y"));
    // not a whole token, not a token at all, empty
    assert!(!has("hell") && !has("foo-bar") && !has("hello world") && !has(""));
    assert!(!has(&word(b'x', 89)) && !has(&format!("{long}x")));
    assert!(!contains_token(b"", b"a"));
}

// ------------------------------------------------ terms that are not UTF-8

/// Brute force over bytes: every token of the text, lowercased.
fn byte_tokens(text: &[u8]) -> Vec<Vec<u8>> {
    tokens(text).map(|t| t.raw().to_ascii_lowercase()).collect()
}

fn random_byte_doc(rng: &mut rand::rngs::StdRng) -> Vec<u8> {
    const ALPHABET: &[u8] = b"aBc\x80\xc3\xa9\xff\xfe";
    const SEPARATORS: &[u8] = b" -\n\0\t.";
    let mut out = Vec::new();
    for _ in 0..rng.random_range(0..14) {
        let len = match rng.random_range(0..30) {
            0 => rng.random_range(60..90),
            _ => rng.random_range(1..4),
        };
        let long = len > 3;
        for k in 0..len {
            // long tokens come from two patterns that agree up to byte 64
            let c = if long {
                if k < 64 || rng.random_bool(0.5) {
                    b'\xff'
                } else {
                    b'B'
                }
            } else {
                ALPHABET[rng.random_range(0..ALPHABET.len())]
            };
            out.push(c);
        }
        out.push(SEPARATORS[rng.random_range(0..SEPARATORS.len())]);
    }
    out
}

#[test]
fn byte_level_terms_that_are_not_utf8_match_brute_force() {
    let mut rng = rng(31);
    let docs: Docs = (0..400u32)
        .map(|i| (i * 2 + 1, random_byte_doc(&mut rng)))
        .collect();
    let per_doc: Vec<Vec<Vec<u8>>> = docs.iter().map(|d| byte_tokens(&d.1)).collect();
    let distinct: BTreeSet<&Vec<u8>> = per_doc.iter().flatten().collect();
    assert!(distinct.iter().any(|t| std::str::from_utf8(t).is_err()));
    assert!(distinct.iter().any(|t| t.len() > 64));
    let ids_where = |f: &dyn Fn(&[Vec<u8>]) -> bool| -> Vec<u32> {
        docs.iter()
            .zip(&per_doc)
            .filter(|(_, t)| f(t))
            .map(|(d, _)| d.0)
            .collect()
    };
    let texts: std::collections::HashMap<u32, &[u8]> =
        docs.iter().map(|(i, t)| (*i, t.as_slice())).collect();
    for budget in [usize::MAX, 1_000] {
        let (idx, _) = build(&docs, budget);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        for t in &distinct {
            let m = r.term(t).unwrap();
            assert_eq!(m.exact, t.len() <= 64);
            let got: Vec<u32> = m
                .ids
                .into_iter()
                .filter(|id| m.exact || contains_token(texts[id], t))
                .collect();
            assert_eq!(got, ids_where(&|d| d.contains(t)), "term {t:?}");
            // every prefix of up to three bytes (all of them for a short term)
            for n in 1..=t.len().min(3) {
                assert_eq!(
                    r.prefix(&t[..n], usize::MAX).unwrap().as_slice(),
                    ids_where(&|d| d.iter().any(|x| x.starts_with(&t[..n]))),
                    "prefix {:?}",
                    &t[..n]
                );
            }
        }
        // pairs of tokens that follow each other
        for pair in per_doc.iter().filter(|t| t.len() >= 2).take(60) {
            let ph = &pair[..2];
            let cand = phrase_candidates(&r, ph).unwrap();
            let got: Vec<u32> = cand
                .ids
                .into_iter()
                .filter(|id| phrase_matches(texts[id], ph))
                .collect();
            let want = ids_where(&|d| d.windows(2).any(|w| w == ph));
            assert_eq!(got, want, "phrase {ph:?}");
            let both = r.intersect(ph).unwrap();
            assert!(both.ids.len() >= want.len());
        }
    }
}

// --------------------------------------------- document ids near u32::MAX

#[test]
fn document_ids_up_to_u32_max_are_found_by_every_search() {
    let mut docs: Docs = vec![(0, b"top t0".to_vec()), (7, b"top t1".to_vec())];
    // 1,001 consecutive ids ending at u32::MAX: a list of eight blocks
    docs.extend(
        (0..=1000u32).map(|k| (u32::MAX - 1000 + k, format!("top t{}", k % 3).into_bytes())),
    );
    let m = model(&docs);
    for budget in [usize::MAX, 1_000] {
        let (idx, meta) = build(&docs, budget);
        assert_eq!(meta.docs, 1003);
        let r = IndexReader::open(idx.as_slice()).unwrap();
        r.verify().unwrap();
        for w in ["top", "t0", "t1", "t2"] {
            let got = r.term(w).unwrap();
            assert!(got.exact);
            assert_eq!(got.ids.as_slice(), m.term(w), "{w}");
        }
        assert_eq!(r.prefix("t", 10).unwrap().as_slice(), m.prefix("t"));
        for pair in [["top", "t1"], ["t0", "t2"], ["top", "top"]] {
            let want = m.docs_where(|t| pair.iter().all(|w| t.iter().any(|x| x.0 == *w)));
            assert_eq!(r.intersect(&pair).unwrap().ids.as_slice(), want, "{pair:?}");
        }
        assert_eq!(r.term("top").unwrap().ids.last(), Some(&u32::MAX));
    }
}

// ----------------------------------------------- a writer that fails midway

/// Accepts `left` bytes, in pieces if need be, then fails every write.
struct FailAfter<'a> {
    left: usize,
    written: &'a mut Vec<u8>,
}

impl Write for FailAfter<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.left == 0 {
            return Err(io::Error::other("disk full"));
        }
        let n = b.len().min(self.left).min(7);
        self.written.extend_from_slice(&b[..n]);
        self.left -= n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.left == 0 {
            return Err(io::Error::other("disk full"));
        }
        Ok(())
    }
}

/// Builds the same small index into a writer that fails after `limit` bytes.
/// Returns what the builder reported first, and the bytes it got out.
fn build_with_limit(limit: usize) -> (Option<Error>, Vec<u8>) {
    let mut written = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions::default(),
        FailAfter {
            left: limit,
            written: &mut written,
        },
    )
    .with_max_segment_docs(6);
    for id in 0..40u32 {
        if let Err(e) = b.add(
            id * 3,
            (0..400)
                .map(|k| format!("w{id}x{k} "))
                .collect::<String>()
                .as_bytes(),
        ) {
            // after a failed write the builder refuses everything, loudly
            assert!(matches!(b.add(id * 3 + 1, b"x"), Err(Error::Fatal(_))));
            assert!(matches!(b.finish(), Err(Error::Fatal(_))));
            return (Some(e), written);
        }
    }
    (b.finish().err(), written)
}

#[test]
fn a_writer_that_fails_midway_gives_an_error_and_never_a_footer() {
    let (none, whole) = build_with_limit(usize::MAX);
    assert!(none.is_none());
    IndexReader::open(whole.as_slice()).unwrap();
    // every length the writer could stop at, in a few steps
    let mut limit = 0;
    while limit < whole.len() {
        let (err, bytes) = build_with_limit(limit);
        assert!(
            matches!(err, Some(Error::Io(_))),
            "a writer that stops after {limit} of {} bytes: {err:?}",
            whole.len()
        );
        assert!(bytes.len() <= limit);
        assert!(
            IndexReader::open(bytes.as_slice()).is_err(),
            "a footer was left after {limit} bytes"
        );
        limit += 1 + limit / 9;
    }
    // the last byte missing is a failure of the final write too
    let (err, bytes) = build_with_limit(whole.len() - 1);
    assert!(matches!(err, Some(Error::Io(_))), "{err:?}");
    assert!(IndexReader::open(bytes.as_slice()).is_err());
}
