//! The parser, `leaves`, and the generic evaluator.

mod support;

use std::collections::{BTreeSet, HashMap};

use nwdb_core::Error;
use nwdb_textindex::{
    IndexReader, Leaf, Query, evaluate, leaves, parse, phrase_candidates, phrase_matches,
};
use rand::Rng;
use rand::rngs::StdRng;
use support::*;

fn t(s: &str) -> Query {
    Query::Term(s.into())
}
fn pre(s: &str) -> Query {
    Query::Prefix(s.into())
}
fn ph(s: &[&str]) -> Query {
    Query::Phrase(s.iter().map(|x| x.to_string()).collect())
}
fn not(q: Query) -> Query {
    Query::Not(Box::new(q))
}
fn and(v: Vec<Query>) -> Query {
    Query::And(v)
}
fn or(v: Vec<Query>) -> Query {
    Query::Or(v)
}

fn invalid(s: &str) {
    match parse(s) {
        Err(Error::InvalidArgument(_)) => {}
        other => panic!("{s:?} should be InvalidArgument, got {other:?}"),
    }
}

#[test]
fn words_phrases_and_prefixes() {
    assert_eq!(parse("foo").unwrap(), t("foo"));
    assert_eq!(parse("  FoO\t").unwrap(), t("foo"));
    assert_eq!(parse("foo bar").unwrap(), and(vec![t("foo"), t("bar")]));
    assert_eq!(parse("a\tb\nc").unwrap(), and(vec![t("a"), t("b"), t("c")]));
    assert_eq!(parse("\"hello world\"").unwrap(), ph(&["hello", "world"]));
    assert_eq!(parse("\"Hello, WORLD!\"").unwrap(), ph(&["hello", "world"]));
    assert_eq!(parse("\"hello\"").unwrap(), t("hello"));
    // a word with several tokens is a phrase of them
    assert_eq!(parse("foo-bar").unwrap(), ph(&["foo", "bar"]));
    assert_eq!(parse("a_b.c/d").unwrap(), ph(&["a", "b", "c", "d"]));
    assert_eq!(parse("foo*").unwrap(), pre("foo"));
    assert_eq!(parse("Fo*").unwrap(), pre("fo"));
    assert_eq!(parse("\u{e9}*").unwrap(), pre("\u{e9}"));
    // a star inside a word is a separator; inside quotes too
    assert_eq!(parse("fo*o").unwrap(), ph(&["fo", "o"]));
    assert_eq!(parse("\"foo*\"").unwrap(), t("foo"));
    assert_eq!(parse("foo**").unwrap(), pre("foo"));
    // only ASCII is folded
    assert_eq!(parse("\u{dc}n\u{ef}CODE").unwrap(), t("\u{dc}n\u{ef}code"));
    // a long token is kept as it is: it matches nothing in an index
    let long = "z".repeat(70);
    assert_eq!(parse(&long).unwrap(), t(&long));
    assert_eq!(parse(&format!("a-{long}")).unwrap(), ph(&["a", &long]));
    assert_eq!(
        parse("foo\"bar baz\"").unwrap(),
        and(vec![t("foo"), ph(&["bar", "baz"])])
    );
}

#[test]
fn boolean_structure_and_precedence() {
    assert_eq!(parse("a OR b").unwrap(), or(vec![t("a"), t("b")]));
    assert_eq!(
        parse("a OR b OR c").unwrap(),
        or(vec![t("a"), t("b"), t("c")])
    );
    // OR binds tighter than the implicit AND
    assert_eq!(
        parse("a b OR c").unwrap(),
        and(vec![t("a"), or(vec![t("b"), t("c")])])
    );
    assert_eq!(
        parse("a OR b c").unwrap(),
        and(vec![or(vec![t("a"), t("b")]), t("c")])
    );
    assert_eq!(
        parse("(a b) OR c").unwrap(),
        or(vec![and(vec![t("a"), t("b")]), t("c")])
    );
    assert_eq!(
        parse("a (b c)").unwrap(),
        and(vec![t("a"), and(vec![t("b"), t("c")])])
    );
    // lower case or is a word
    assert_eq!(parse("a or b").unwrap(), and(vec![t("a"), t("or"), t("b")]));
    assert_eq!(parse("ORANGE").unwrap(), t("orange"));
    assert_eq!(parse("a OR(b)").unwrap(), or(vec![t("a"), t("b")]));
    // negation
    assert_eq!(parse("a -b").unwrap(), and(vec![t("a"), not(t("b"))]));
    assert_eq!(parse("-b a").unwrap(), and(vec![not(t("b")), t("a")]));
    assert_eq!(
        parse("a -\"b c\"").unwrap(),
        and(vec![t("a"), not(ph(&["b", "c"]))])
    );
    assert_eq!(
        parse("a -b-c").unwrap(),
        and(vec![t("a"), not(ph(&["b", "c"]))])
    );
    assert_eq!(parse("a -fo*").unwrap(), and(vec![t("a"), not(pre("fo"))]));
    assert_eq!(
        parse("a -(b OR c)").unwrap(),
        and(vec![t("a"), not(or(vec![t("b"), t("c")]))])
    );
    assert_eq!(parse("a --b").unwrap(), and(vec![t("a"), not(not(t("b")))]));
    assert_eq!(parse("a OR -b").unwrap(), or(vec![t("a"), not(t("b"))]));
    assert_eq!(parse("-a OR b").unwrap(), or(vec![not(t("a")), t("b")]));
    // a '-' that is not directly followed by an operand is just a byte
    assert_eq!(parse("a - b").unwrap(), and(vec![t("a"), t("b")]));
    assert_eq!(parse("a -").unwrap(), t("a"));
    assert_eq!(parse("(a -)").unwrap(), t("a"));
    // groups
    assert_eq!(parse("(a)").unwrap(), t("a"));
    assert_eq!(parse("((a))").unwrap(), t("a"));
    assert_eq!(
        parse("(a b) c").unwrap(),
        and(vec![and(vec![t("a"), t("b")]), t("c")])
    );
    assert_eq!(
        parse("x ((a OR b) -c)").unwrap(),
        and(vec![
            t("x"),
            and(vec![or(vec![t("a"), t("b")]), not(t("c"))])
        ])
    );
}

#[test]
fn words_without_tokens_are_ignored() {
    assert_eq!(parse("a !!! b").unwrap(), and(vec![t("a"), t("b")]));
    assert_eq!(parse("a OR !!!").unwrap(), t("a"));
    assert_eq!(parse("!!! OR a").unwrap(), t("a"));
    assert_eq!(parse("-!!! a").unwrap(), t("a"));
    assert_eq!(parse("\"\" a").unwrap(), t("a"));
    assert_eq!(parse("\"!!\" a").unwrap(), t("a"));
    assert_eq!(parse("() a").unwrap(), t("a"));
    assert_eq!(parse("* a").unwrap(), t("a"));
    assert_eq!(parse("a (\"\" !!!)").unwrap(), t("a"));
}

#[test]
fn syntax_and_usability_errors() {
    for s in [
        "",
        "   ",
        "!!!",
        "\"\"",
        "()",
        "-a",
        "-a -b",
        "-(a OR b)",
        "--a",
        "-a OR -b",
        "-\"a b\"",
        // OR needs two operands
        "a OR",
        "OR a",
        "OR",
        "a OR OR b",
        "(a OR)",
        "(OR a)",
        "a OR )",
        "-OR a",
        // brackets and quotes
        "\"abc",
        "(a",
        "a)",
        ")",
        "((a)",
        "a \"b",
        "a b)",
        // prefixes
        "f*",
        "a*",
        "-a*",
        "foo-bar*",
        "\"a\" b*c*",
        &format!("{}*", "p".repeat(65)),
    ] {
        match parse(s) {
            Err(Error::InvalidArgument(_)) => {}
            other => panic!("{s:?}: {other:?}"),
        }
    }
}

#[test]
fn limits() {
    let nested = |n: usize| format!("x {}a{}", "(".repeat(n), ")".repeat(n));
    assert_eq!(parse(&nested(32)).unwrap(), and(vec![t("x"), t("a")]));
    invalid(&nested(33));
    let negs = |n: usize| format!("x {}a", "-".repeat(n));
    assert!(parse(&negs(32)).is_ok());
    invalid(&negs(33));
    // parentheses and '-' share the limit
    invalid(&format!("x {}-a{}", "(".repeat(32), ")".repeat(32)));
    let words = |n: usize| {
        (0..n)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert!(parse(&words(64)).is_ok());
    invalid(&words(65));
    // a phrase counts as one operand, but not more than 64 tokens long
    let dashed = |n: usize| {
        (0..n)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join("-")
    };
    assert!(matches!(parse(&dashed(64)).unwrap(), Query::Phrase(v) if v.len() == 64));
    invalid(&dashed(65));
    invalid(&"a ".repeat(9000));
    // hundreds of nested minus signs must not overflow the stack
    invalid(&format!("a {}b", "-".repeat(10_000)));
}

#[test]
fn leaves_come_in_order() {
    let q = parse("a (b OR \"c d\") -ee* f-g").unwrap();
    let got: Vec<String> = leaves(&q)
        .iter()
        .map(|l| match l {
            Leaf::Term(s) => format!("t:{s}"),
            Leaf::Prefix(s) => format!("p:{s}"),
            Leaf::Phrase(v) => format!("ph:{}", v.join(" ")),
        })
        .collect();
    assert_eq!(
        got,
        ["t:a", "t:b", "ph:c d", "p:ee", "ph:f g"].map(String::from)
    );
}

// ------------------------------------------------------------- evaluator

fn brute_set(q: &Query, sets: &HashMap<String, BTreeSet<u32>>, id: u32) -> bool {
    match q {
        Query::Term(w) | Query::Prefix(w) => sets.get(w).is_some_and(|s| s.contains(&id)),
        Query::Phrase(p) => sets.get(&p.join(" ")).is_some_and(|s| s.contains(&id)),
        Query::And(v) => v.iter().all(|c| brute_set(c, sets, id)),
        Query::Or(v) => v.iter().any(|c| brute_set(c, sets, id)),
        Query::Not(c) => !brute_set(c, sets, id),
    }
}

fn random_tree(rng: &mut StdRng, depth: usize) -> Query {
    let name = |rng: &mut StdRng| format!("s{}", rng.random_range(0..6));
    match rng.random_range(0..if depth >= 4 { 3 } else { 7 }) {
        0 => Query::Term(name(rng)),
        1 => Query::Prefix(name(rng)),
        2 => Query::Phrase(vec![name(rng), name(rng)]),
        3 => Query::And(
            (0..rng.random_range(0..4))
                .map(|_| random_tree(rng, depth + 1))
                .collect(),
        ),
        4 => Query::Or(
            (0..rng.random_range(0..4))
                .map(|_| random_tree(rng, depth + 1))
                .collect(),
        ),
        _ => Query::Not(Box::new(random_tree(rng, depth + 1))),
    }
}

#[test]
fn evaluator_matches_brute_force_on_random_sets() {
    let mut rng = rng(21);
    for round in 0..400 {
        let universe: Vec<u32> = (0..rng.random_range(0..120))
            .map(|i| i * 3 + (i % 3))
            .collect();
        let mut sets: HashMap<String, BTreeSet<u32>> = HashMap::new();
        for i in 0..6 {
            let density = [0.0, 0.05, 0.3, 0.7, 1.0, 0.5][i];
            let s: BTreeSet<u32> = universe
                .iter()
                .copied()
                .filter(|_| rng.random_bool(density))
                .collect();
            sets.insert(format!("s{i}"), s.clone());
            // a phrase leaf is looked up under the joined tokens
            for j in 0..6 {
                sets.entry(format!("s{i} s{j}")).or_insert_with(|| {
                    universe
                        .iter()
                        .copied()
                        .filter(|_| rng.random_bool(0.4))
                        .collect()
                });
            }
        }
        let q = random_tree(&mut rng, 0);
        let mut calls = 0;
        let got = evaluate(&q, &universe, |l| {
            calls += 1;
            let key = match l {
                Leaf::Term(s) | Leaf::Prefix(s) => s.to_string(),
                Leaf::Phrase(p) => p.join(" "),
            };
            Ok(sets
                .get(&key)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default())
        })
        .unwrap();
        let want: Vec<u32> = universe
            .iter()
            .copied()
            .filter(|&i| brute_set(&q, &sets, i))
            .collect();
        assert_eq!(got, want, "round {round}: {q:?}");
        assert!(calls <= leaves(&q).len());
    }
}

#[test]
fn evaluator_edges() {
    let u = [1u32, 3, 5, 7];
    let no_leaf = |_: &Leaf<'_>| -> nwdb_core::Result<Vec<u32>> { panic!("no leaf expected") };
    assert_eq!(evaluate(&Query::And(vec![]), &u, no_leaf).unwrap(), u);
    assert_eq!(
        evaluate(&Query::Or(vec![]), &u, no_leaf).unwrap(),
        Vec::<u32>::new()
    );
    let some = |_: &Leaf<'_>| Ok(vec![3u32, 7]);
    assert_eq!(evaluate(&not(t("x")), &u, some).unwrap(), [1, 5]);
    // all operands negated: the universe minus them
    assert_eq!(
        evaluate(&and(vec![not(t("x")), not(t("y"))]), &u, some).unwrap(),
        [1, 5]
    );
    // errors from a leaf come back
    let failing = |_: &Leaf<'_>| -> nwdb_core::Result<Vec<u32>> { Err(Error::unavailable("no")) };
    assert!(matches!(
        evaluate(&t("x"), &u, failing),
        Err(Error::Unavailable(_))
    ));
    // lists that are not ascending are refused
    let bad = |_: &Leaf<'_>| Ok(vec![3u32, 3]);
    assert!(matches!(
        evaluate(&t("x"), &u, bad),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        evaluate(&t("x"), &[2, 1], some),
        Err(Error::InvalidArgument(_))
    ));
    // a query built too deep by hand is refused, not a stack overflow
    let mut deep = t("x");
    for _ in 0..200 {
        deep = not(deep);
    }
    assert!(matches!(
        evaluate(&deep, &u, some),
        Err(Error::InvalidArgument(_))
    ));
    // an And stops asking for leaves once it is empty
    let mut calls = 0;
    let q = and(vec![t("a"), t("b"), t("c")]);
    let r = evaluate(&q, &u, |_| {
        calls += 1;
        Ok(vec![])
    })
    .unwrap();
    assert!(r.is_empty() && calls == 1);
}

// ----------------------------------------- parsed text through the index

fn random_query_text(rng: &mut StdRng, words: &[String], depth: usize) -> String {
    let word = |rng: &mut StdRng| words[rng.random_range(0..words.len())].clone();
    let operand = |rng: &mut StdRng| -> String {
        match rng.random_range(0..if depth >= 2 { 4 } else { 6 }) {
            0 | 1 => word(rng),
            2 => format!("{}*", &word(rng)[..2]),
            3 => format!("\"{} {}\"", word(rng), word(rng)),
            4 => format!("({})", random_query_text(rng, words, depth + 1)),
            _ => format!("{}-{}", word(rng), word(rng)),
        }
    };
    let mut parts = vec![operand(rng)];
    for _ in 0..rng.random_range(0..4) {
        let next = operand(rng);
        parts.push(match rng.random_range(0..4) {
            0 => format!("-{next}"),
            1 => format!("OR {next}"),
            _ => next,
        });
    }
    parts.join(" ")
}

#[test]
fn parsed_queries_through_the_index_match_brute_force() {
    let words: Vec<String> = (0..12).map(vocab_word).collect();
    for seed in 0..3u64 {
        let mut rng = rng(seed + 100);
        let docs = random_docs(&mut rng, 200, 3..25, |r| {
            words[r.random_range(0..12)].clone()
        });
        let m = model(&docs);
        let universe = m.ids.clone();
        let texts: HashMap<u32, &[u8]> = docs.iter().map(|(i, t)| (*i, t.as_slice())).collect();
        for budget in [usize::MAX, 1_500] {
            let (idx, _) = build(&docs, budget);
            let r = IndexReader::open(idx.as_slice()).unwrap();
            let mut parsed = 0;
            for _ in 0..150 {
                let text = random_query_text(&mut rng, &words, 0);
                // "OR" first, or only negated operands, is an error by design
                let Ok(q) = parse(&text) else { continue };
                parsed += 1;
                let got = evaluate(&q, &universe, |l| {
                    Ok(match l {
                        Leaf::Term(w) => r.term(w)?.into_vec(),
                        Leaf::Prefix(p) => r.prefix(p, usize::MAX)?.into_vec(),
                        Leaf::Phrase(p) => phrase_candidates(&r, p)?
                            .into_iter()
                            .filter(|id| phrase_matches(texts[id], p))
                            .collect(),
                    })
                })
                .unwrap();
                assert_eq!(got, m.query(&q), "{text:?} parsed as {q:?}");
            }
            assert!(parsed > 50);
        }
    }
}

#[test]
fn phrase_matching() {
    let m = |text: &str, ph: &[&str]| phrase_matches(text.as_bytes(), ph);
    assert!(m("the quick brown fox", &["quick", "brown"]));
    assert!(!m("the quick brown fox", &["brown", "quick"]));
    assert!(!m("the quick brown fox", &["quick", "fox"]));
    assert!(m("THE Quick-Brown_fox", &["quick", "brown", "fox"]));
    assert!(m("a", &["a"]) && !m("", &["a"]) && !m("a", &[] as &[&str]));
    // a phrase that repeats a token, found after a false start
    assert!(m("a a a b", &["a", "a", "b"]));
    assert!(m("a b a b a b c", &["a", "b", "a", "b", "c"]));
    assert!(!m("a b a b a b", &["a", "b", "c"]));
    assert!(m("x a a b a a a b", &["a", "a", "a", "b"]));
    // punctuation between tokens does not matter, other tokens do
    assert!(m("quick, (brown)", &["quick", "brown"]));
    assert!(!m("quick very brown", &["quick", "brown"]));
    // a token longer than 64 bytes still occupies a position
    let long = "z".repeat(70);
    assert!(m(&format!("foo {long} bar"), &["foo", &long, "bar"]));
    assert!(!m(&format!("foo {long} bar"), &["foo", "bar"]));
    assert!(!m(&format!("foo {long} bar"), &["foo", &"y".repeat(70)]));
}

#[test]
fn phrase_candidates_skip_unindexable_tokens() {
    let long = "z".repeat(70);
    let docs: Docs = vec![
        (0, format!("foo {long} bar").into_bytes()),
        (1, b"foo bar".to_vec()),
        (2, b"bar foo".to_vec()),
        (3, b"foo".to_vec()),
    ];
    let (idx, _) = build(&docs, usize::MAX);
    let r = IndexReader::open(idx.as_slice()).unwrap();
    let cand = phrase_candidates(&r, &["foo", long.as_str(), "bar"]).unwrap();
    assert_eq!(cand.as_slice(), [0, 1, 2]);
    let hits: Vec<u32> = cand
        .into_iter()
        .filter(|&i| phrase_matches(&docs[i as usize].1, &["foo", long.as_str(), "bar"]))
        .collect();
    assert_eq!(hits, [0]);
    // nothing to look up
    assert!(matches!(
        phrase_candidates(&r, &[long.as_str()]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        phrase_candidates::<_, &str>(&r, &[]),
        Err(Error::InvalidArgument(_))
    ));
}
