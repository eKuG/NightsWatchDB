//! Full-text search over parts: every search equals a brute-force scan of a
//! model, through one part, several parts, a merge and a restart; the limits
//! refuse what they promise; damaged text files are `Corrupt`, never a panic
//! or a wrong answer.
//!
//! The model's definition of a row's text is "the pieces the writer stored for
//! it": the splitter's cuts of its three raw columns, without the cuts shorter
//! than the writer's floor. Text that stays inline is not searchable, by
//! design, and the model does not have it either.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::{
    DEFAULT_MIN_PIECE_BYTES, MergeOptions, PartReader, PartWriter, SearchLimits, TEXT_INDEX_FILE,
    TEXT_ROWS_FILE, WriterOptions, merge_parts,
};
use nwdb_textindex::{Query, contains_token, parse, phrase_matches, tokens};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::collections::BTreeSet;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;

// ------------------------------------------------------------- splitter ---

/// Cuts a value at its line ends: every non-empty line is a piece. Lines are
/// the unit of repetition in the generated data, as messages are in real
/// traces.
struct Lines;

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

fn options() -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(Lines)),
        ..WriterOptions::default()
    }
}

// ----------------------------------------------------------------- data ---

#[derive(Clone, Debug)]
struct Row {
    trace: u32,
    span: u32,
    start: i64,
    attrs: Vec<u8>,
    events: Option<Vec<u8>>,
    links: Option<Vec<u8>>,
}

impl Row {
    fn record(&self) -> SpanRecord<'_> {
        SpanRecord {
            trace_id: TraceId((u128::from(self.trace) + 1).to_be_bytes()),
            span_id: SpanId((u64::from(self.span) + 1).to_be_bytes()),
            name: "span",
            start_ns: self.start,
            end_ns: self.start + 10,
            attrs: &self.attrs,
            events: self.events.as_deref(),
            links: self.links.as_deref(),
            ..SpanRecord::default()
        }
    }

    fn key(&self) -> (TraceId, SpanId) {
        let r = self.record();
        (r.trace_id, r.span_id)
    }

    /// The pieces the writer stores for the row: the splitter's cuts of each
    /// raw column that are at least the writer's floor.
    fn pieces(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for value in [Some(&self.attrs), self.events.as_ref(), self.links.as_ref()]
            .into_iter()
            .flatten()
        {
            for cut in lines(value) {
                if cut.len() >= DEFAULT_MIN_PIECE_BYTES {
                    out.push(value[cut].to_vec());
                }
            }
        }
        out
    }
}

fn batch_of(rows: &[Row]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 30,
        max_bytes: 1 << 40,
    });
    for r in rows {
        b.append(&r.record());
    }
    b.finish()
}

fn write_part(
    parts: &Path,
    id: u64,
    rows: &[Row],
    batch_rows: usize,
    options: WriterOptions,
) -> PathBuf {
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options).unwrap();
    for chunk in rows.chunks(batch_rows) {
        w.write(&batch_of(chunk)).unwrap();
    }
    w.finish().unwrap();
    parts.join(part_dir_name(id))
}

/// Words, messages and secret-like keys the rows are made of.
struct World {
    words: Vec<String>,
    messages: Vec<String>,
}

const COMMON: &[&str] = &[
    "error",
    "Timeout",
    "user",
    "assistant",
    "SYSTEM",
    "tool",
    "weather",
    "paris",
    "london",
    "the",
    "a",
    "of",
    "retry",
    "connection",
    "refused",
    "payment",
    "invoice",
    "prompt",
    "answer",
    "go",
    "now",
    "please",
    "model",
    "token",
];

const NON_ASCII: &[&str] = &["café", "naïve", "日本語", "привет", "Ünïcode", "αβγ"];

fn alnum(rng: &mut StdRng, n: usize) -> String {
    (0..n)
        .map(|_| char::from(b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.random_range(0..36)]))
        .collect()
}

fn world(rng: &mut StdRng) -> World {
    let mut words: Vec<String> = COMMON.iter().map(|w| (*w).to_string()).collect();
    words.extend(NON_ASCII.iter().map(|w| (*w).to_string()));
    // Many distinct words with one prefix, for prefix expansion.
    words.extend((0..120).map(|i| format!("zq{i:03}")));
    // Secret-like keys: over 64 bytes, two of them sharing their first 64.
    let base = alnum(rng, 100);
    let sibling = format!("{}{}", &base[..70], alnum(rng, 30));
    let other = alnum(rng, 90);
    let long = [base, sibling, other];

    let sentence = |rng: &mut StdRng, words: &[String]| -> String {
        let n = rng.random_range(4..16);
        (0..n)
            .map(|_| words.choose(rng).unwrap().as_str())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let mut messages = Vec::new();
    for i in 0..70 {
        let m = match i % 7 {
            0 => ["ok", "yes", "no way", "done"][rng.random_range(0..4)].to_string(),
            1 | 2 => sentence(rng, &words),
            3 => format!(r#"{{"role":"user","content":"{}"}}"#, sentence(rng, &words)),
            4 => format!(
                "my key is {} and {}",
                long.choose(rng).unwrap(),
                sentence(rng, &words)
            ),
            5 => "go go go now go go please".to_string(),
            _ => format!(
                "{} {} {}",
                NON_ASCII.choose(rng).unwrap(),
                sentence(rng, &words),
                NON_ASCII.choose(rng).unwrap()
            ),
        };
        messages.push(m);
    }
    // A message with each long key on its own, for a clean term lookup.
    for l in &long {
        messages.push(format!("secret {l}"));
    }
    World { words, messages }
}

fn rows(rng: &mut StdRng, w: &World, traces: u32, first_trace: u32) -> Vec<Row> {
    let mut out = Vec::new();
    let mut span = 0u32;
    for t in 0..traces {
        for _ in 0..rng.random_range(1..6) {
            let pick = |rng: &mut StdRng, lo: usize, hi: usize| -> Vec<u8> {
                let mut lines: Vec<String> = (0..rng.random_range(lo..=hi))
                    .map(|_| w.messages.choose(rng).unwrap().clone())
                    .collect();
                if rng.random_bool(0.3) {
                    lines.push(format!("unique {} {span}", w.words.choose(rng).unwrap()));
                }
                lines.join("\n").into_bytes()
            };
            let attrs = pick(rng, 0, 5);
            let events = rng.random_bool(0.4).then(|| pick(rng, 1, 3));
            let links = rng.random_bool(0.2).then(|| pick(rng, 1, 2));
            out.push(Row {
                trace: first_trace + t,
                span,
                start: 1_000 + i64::from(span),
                attrs,
                events,
                links,
            });
            span += 1;
        }
    }
    out
}

// ---------------------------------------------------------------- model ---

fn token_has_prefix(piece: &[u8], prefix: &str) -> bool {
    tokens(piece).any(|t| t.raw().to_ascii_lowercase().starts_with(prefix.as_bytes()))
}

/// Whether a row with these pieces matches: the brute-force definition.
fn matches(q: &Query, pieces: &[Vec<u8>]) -> bool {
    match q {
        Query::Term(t) => pieces.iter().any(|p| contains_token(p, t.as_bytes())),
        Query::Prefix(p) => pieces.iter().any(|piece| token_has_prefix(piece, p)),
        Query::Phrase(toks) => pieces.iter().any(|p| phrase_matches(p, toks)),
        Query::And(v) => v.iter().all(|q| matches(q, pieces)),
        Query::Or(v) => v.iter().any(|q| matches(q, pieces)),
        Query::Not(q) => !matches(q, pieces),
    }
}

fn brute(q: &Query, rows: &[Row]) -> Vec<u32> {
    rows.iter()
        .enumerate()
        .filter(|(_, r)| matches(q, &r.pieces()))
        .map(|(i, _)| i as u32)
        .collect()
}

fn lower(t: &[u8]) -> String {
    String::from_utf8_lossy(&t.to_ascii_lowercase()).into_owned()
}

/// Tokens of a random message of the world, lowercased.
fn some_tokens(rng: &mut StdRng, w: &World) -> Vec<String> {
    loop {
        let m = w.messages.choose(rng).unwrap();
        let t: Vec<String> = tokens(m.as_bytes()).map(|t| lower(t.raw())).collect();
        if !t.is_empty() {
            return t;
        }
    }
}

fn leaf(rng: &mut StdRng, w: &World) -> Query {
    let t = some_tokens(rng, w);
    match rng.random_range(0..10) {
        0..=3 => Query::Term(t.choose(rng).unwrap().clone()),
        // A word that is in no message.
        4 => Query::Term(format!("absent{}", rng.random_range(0..5))),
        5 | 6 => {
            let word = t.choose(rng).unwrap();
            let chars: Vec<(usize, char)> = word.char_indices().collect();
            if chars.len() < 2 {
                return Query::Term(word.clone());
            }
            let n = rng.random_range(2..=chars.len().min(5));
            let end = chars.get(n).map_or(word.len(), |&(i, _)| i);
            Query::Prefix(word[..end].to_string())
        }
        7 => Query::Prefix("zq".to_string()),
        _ => {
            let len = rng.random_range(1..=4).min(t.len());
            let at = rng.random_range(0..=t.len() - len);
            Query::Phrase(t[at..at + len].to_vec())
        }
    }
}

fn query(rng: &mut StdRng, w: &World, depth: u32) -> Query {
    if depth == 0 || rng.random_bool(0.35) {
        return leaf(rng, w);
    }
    let operands = |rng: &mut StdRng| -> Vec<Query> {
        (0..rng.random_range(2..=3))
            .map(|_| query(rng, w, depth - 1))
            .collect()
    };
    match rng.random_range(0..4) {
        0 => Query::And(operands(rng)),
        1 => Query::Or(operands(rng)),
        2 => Query::Not(Box::new(query(rng, w, depth - 1))),
        _ => {
            // An AND with a negation, the common shape of a real filter.
            let mut v = operands(rng);
            v.push(Query::Not(Box::new(leaf(rng, w))));
            Query::And(v)
        }
    }
}

/// `(trace, span)` of every row of a part, by row number.
fn ids_of(r: &PartReader) -> Vec<(TraceId, SpanId)> {
    let mut out = Vec::new();
    for b in r
        .read_rows(0, r.row_count(), Some(&["trace_id", "span_id"]))
        .unwrap()
    {
        let (t, s) = (
            b.column(0).as_fixed_size_binary(),
            b.column(1).as_fixed_size_binary(),
        );
        for i in 0..b.num_rows() {
            out.push((
                TraceId::from_slice(t.value(i)).unwrap(),
                SpanId::from_slice(s.value(i)).unwrap(),
            ));
        }
    }
    out
}

fn found(r: &PartReader, q: &Query) -> Vec<u32> {
    r.search(q, &SearchLimits::default()).unwrap()
}

fn check_queries(r: &PartReader, rows: &[Row], w: &World, seed: u64, n: usize, what: &str) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut nonempty = 0;
    for i in 0..n {
        let q = query(&mut rng, w, 3);
        let got = found(r, &q);
        assert_eq!(got, brute(&q, rows), "{what}: query {i}: {q:?}");
        nonempty += usize::from(!got.is_empty());
    }
    // Agreeing on empty answers alone would prove little.
    assert!(
        nonempty * 4 >= n,
        "{what}: only {nonempty} of {n} queries found rows"
    );
}

// ---------------------------------------------------------------- tests ---

#[test]
fn search_equals_a_brute_force_scan_of_the_model() {
    for seed in 0..3u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let w = world(&mut rng);
        let data = rows(&mut rng, &w, 60, 0);
        let tmp = tempfile::tempdir().unwrap();

        // The default budgets, and budgets so small that the row lists spill
        // to a run for every few rows and the part has many row groups.
        let plain = write_part(tmp.path(), 1, &data, 37, options());
        let spilled = write_part(
            tmp.path(),
            2,
            &data,
            37,
            WriterOptions {
                row_list_budget_bytes: 16,
                text_index_budget_bytes: 1,
                row_group_rows: 16,
                ..options()
            },
        );
        // The row lists do not depend on how they were collected.
        for f in [TEXT_ROWS_FILE, TEXT_INDEX_FILE] {
            assert_eq!(
                std::fs::read(plain.join(f)).unwrap(),
                std::fs::read(spilled.join(f)).unwrap(),
                "{f} differs between a spilled and an unspilled build"
            );
        }
        // No run files are left behind.
        for dir in [&plain, &spilled] {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            assert_eq!(names.len(), 7, "{names:?}");
        }

        for dir in [&plain, &spilled] {
            let r = PartReader::open(dir).unwrap();
            r.verify().unwrap();
            check_queries(&r, &data, &w, seed, 150, "one part");
        }
        // After a restart: a new reader over the same files.
        let r = PartReader::open(&spilled).unwrap();
        check_queries(&r, &data, &w, seed + 100, 50, "after reopening");
    }
}

#[test]
fn queries_written_as_text_match_the_model() {
    let mut rng = StdRng::seed_from_u64(7);
    let w = world(&mut rng);
    let data = rows(&mut rng, &w, 40, 0);
    let tmp = tempfile::tempdir().unwrap();
    let r = PartReader::open(write_part(tmp.path(), 1, &data, 50, options())).unwrap();
    for text in [
        "error timeout",
        "error OR retry",
        "error -timeout",
        "\"go go go\"",
        "\"go go\" -\"go go go now\"",
        "zq0*",
        "zq00* OR (paris london)",
        "(error OR retry) (paris OR london) -zq0*",
        "caf\u{e9}",
        "\"my key is\"",
        "secret",
    ] {
        let q = parse(text).unwrap();
        assert_eq!(found(&r, &q), brute(&q, &data), "{text}");
    }
    // A word with a long key is found, and only where the whole token is.
    for l in w.messages.iter().filter(|m| m.starts_with("secret ")) {
        let key = l.trim_start_matches("secret ");
        let q = Query::Term(key.to_string());
        assert_eq!(found(&r, &q), brute(&q, &data), "{key}");
    }
    // Something was found, or the comparisons above prove nothing.
    assert!(!found(&r, &parse("go").unwrap()).is_empty());
}

#[test]
fn a_part_with_no_text_has_empty_text_files_and_matches_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    // Every value is under the floor, so nothing is cut.
    let data: Vec<Row> = (0..5)
        .map(|i| Row {
            trace: 0,
            span: i,
            start: 1_000 + i64::from(i),
            attrs: b"tiny".to_vec(),
            events: None,
            links: None,
        })
        .collect();
    let r = PartReader::open(write_part(tmp.path(), 1, &data, 5, options())).unwrap();
    r.verify().unwrap();
    assert_eq!(r.text_stats().pieces, 0);
    assert!(found(&r, &parse("tiny").unwrap()).is_empty());
    // The parser refuses a query that is only a negation; a hand-built one is
    // the universe minus the word.
    let not_tiny = Query::Not(Box::new(Query::Term("tiny".into())));
    assert_eq!(found(&r, &not_tiny).len(), 5);
}

#[test]
fn text_under_the_floor_stays_inline_and_is_not_searched() {
    let tmp = tempfile::tempdir().unwrap();
    let data = vec![Row {
        trace: 0,
        span: 0,
        start: 1,
        attrs: b"short\nlonger than eight".to_vec(),
        events: None,
        links: None,
    }];
    let r = PartReader::open(write_part(tmp.path(), 1, &data, 5, options())).unwrap();
    assert!(found(&r, &parse("short").unwrap()).is_empty());
    assert_eq!(found(&r, &parse("longer").unwrap()), [0]);
}

#[test]
fn words_of_one_and_phrases_must_come_from_the_right_pieces() {
    let tmp = tempfile::tempdir().unwrap();
    // Two words in different messages of one span: AND matches the span, a
    // phrase does not.
    let data = vec![
        Row {
            trace: 0,
            span: 0,
            start: 1,
            attrs: b"alpha beta gamma\ndelta epsilon zeta".to_vec(),
            events: None,
            links: None,
        },
        Row {
            trace: 0,
            span: 1,
            start: 2,
            attrs: b"alpha beta gamma delta".to_vec(),
            events: None,
            links: None,
        },
    ];
    let r = PartReader::open(write_part(tmp.path(), 1, &data, 5, options())).unwrap();
    assert_eq!(found(&r, &parse("gamma delta").unwrap()), [0, 1]);
    assert_eq!(found(&r, &parse("\"gamma delta\"").unwrap()), [1]);
    assert_eq!(found(&r, &parse("gamma -epsilon").unwrap()), [1]);
}

#[test]
fn a_merged_part_answers_like_its_inputs() {
    for seed in 0..2u64 {
        let mut rng = StdRng::seed_from_u64(50 + seed);
        let w = world(&mut rng);
        let all = rows(&mut rng, &w, 90, 0);
        let tmp = tempfile::tempdir().unwrap();

        // Part A: traces 0..29 and the first spans of trace 29. Part B: the
        // later spans of trace 29, traces 30..60, and exact copies of the
        // spans of traces 0..3 (a retry that crossed a flush). Part C: traces
        // 60..90.
        let in_traces = |lo: u32, hi: u32| -> Vec<Row> {
            all.iter()
                .filter(|r| (lo..hi).contains(&r.trace))
                .cloned()
                .collect()
        };
        let t29 = in_traces(29, 30);
        let half = t29.len() / 2;
        let mut a = in_traces(0, 29);
        a.extend(t29[..half].iter().cloned());
        let mut b: Vec<Row> = t29[half..].to_vec();
        b.extend(in_traces(30, 60));
        b.extend(in_traces(0, 3));
        let c = in_traces(60, 90);

        let parts: Vec<(Vec<Row>, PathBuf)> = [a, b, c]
            .into_iter()
            .enumerate()
            .map(|(i, rows)| {
                let dir = write_part(tmp.path(), i as u64 + 1, &rows, 41, options());
                (rows, dir)
            })
            .collect();
        let readers: Vec<PartReader> = parts
            .iter()
            .map(|(_, d)| PartReader::open(d).unwrap())
            .collect();
        let refs: Vec<&PartReader> = readers.iter().collect();
        let (meta, stats) = merge_parts(
            &refs,
            tmp.path(),
            10,
            0,
            WINDOW_NS,
            MergeOptions {
                writer: WriterOptions {
                    row_list_budget_bytes: 64,
                    ..options()
                },
                ..MergeOptions::default()
            },
        )
        .unwrap();
        assert!(stats.duplicates_dropped > 0, "{stats:?}");
        let merged = PartReader::open(tmp.path().join(part_dir_name(10))).unwrap();
        merged.verify().unwrap();
        assert_eq!(merged.row_count(), meta.rows);
        let merged_ids = ids_of(&merged);
        let input_ids: Vec<Vec<(TraceId, SpanId)>> = readers.iter().map(ids_of).collect();

        let mut qrng = StdRng::seed_from_u64(seed + 1_000);
        for i in 0..120 {
            let q = query(&mut qrng, &w, 3);
            let mut want: BTreeSet<(TraceId, SpanId)> = BTreeSet::new();
            for (r, ids) in readers.iter().zip(&input_ids) {
                want.extend(found(r, &q).into_iter().map(|row| ids[row as usize]));
            }
            let got: BTreeSet<(TraceId, SpanId)> = found(&merged, &q)
                .into_iter()
                .map(|row| merged_ids[row as usize])
                .collect();
            assert_eq!(got, want, "merge, query {i}: {q:?}");
        }
        // Rows hold the same text as the model says, merged or not.
        let unique: Vec<Row> = {
            let mut seen = BTreeSet::new();
            parts
                .iter()
                .flat_map(|(rows, _)| rows.iter())
                .filter(|r| seen.insert(r.key()))
                .cloned()
                .collect()
        };
        let by_id: std::collections::HashMap<_, _> = unique.iter().map(|r| (r.key(), r)).collect();
        let model: Vec<Row> = merged_ids.iter().map(|id| (*by_id[id]).clone()).collect();
        check_queries(&merged, &model, &w, seed + 2_000, 60, "merged vs model");
    }
}

// --------------------------------------------------------------- limits ---

fn limited_part(tmp: &Path) -> (PartReader, Vec<Row>) {
    let mut rng = StdRng::seed_from_u64(3);
    let w = world(&mut rng);
    let data = rows(&mut rng, &w, 80, 0);
    (
        PartReader::open(write_part(tmp, 1, &data, 50, options())).unwrap(),
        data,
    )
}

fn invalid(r: Result<Vec<u32>, Error>) -> String {
    match r {
        Err(Error::InvalidArgument(m)) => m,
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[test]
fn each_limit_refuses_a_query_that_exceeds_it_and_says_which() {
    let tmp = tempfile::tempdir().unwrap();
    let (r, data) = limited_part(tmp.path());
    let ok = SearchLimits::default();

    // The prefix expands to the 120 words zq000..zq119.
    let q = parse("zq*").unwrap();
    let tight = SearchLimits {
        max_prefix_terms: 50,
        ..ok
    };
    let m = invalid(r.search(&q, &tight));
    assert!(m.contains("prefix") && m.contains("50"), "{m}");
    assert_eq!(r.search(&q, &ok).unwrap(), brute(&q, &data));

    // A phrase needs its candidate pieces read.
    let q = parse("\"go go go\"").unwrap();
    let tight = SearchLimits {
        max_confirm_bytes: 10,
        ..ok
    };
    let m = invalid(r.search(&q, &tight));
    assert!(m.contains("bytes of text"), "{m}");
    assert_eq!(r.search(&q, &ok).unwrap(), brute(&q, &data));
    // A word that needs no confirmation reads no piece, so the limit on bytes
    // read does not touch it.
    let q = parse("go").unwrap();
    assert!(r.search(&q, &tight).is_ok());

    // A leaf that produces too many rows.
    let tight = SearchLimits {
        max_leaf_rows: 5,
        ..ok
    };
    let q = parse("go").unwrap();
    let m = invalid(r.search(&q, &tight));
    assert!(m.contains("more than 5 rows"), "{m}");
    assert_eq!(r.search(&q, &ok).unwrap(), brute(&q, &data));
    // And a negation, which covers every row of the part.
    let q = Query::Not(Box::new(Query::Term("secret".into())));
    let m = invalid(r.search(&q, &tight));
    assert!(m.contains("negation"), "{m}");
    assert_eq!(r.search(&q, &ok).unwrap(), brute(&q, &data));
    // A query that needs no negation is unharmed by the part's size.
    let tight_rows = SearchLimits {
        max_leaf_rows: data.len() - 1,
        ..ok
    };
    assert!(r.search(&parse("secret").unwrap(), &tight_rows).is_ok());
}

#[cfg(target_os = "linux")]
fn status_kb(key: &str) -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with(key)).unwrap();
    line.split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        * 1024
}

const ROWS: u32 = 300_000;
const LIMIT: usize = 100_000;

/// The part of the memory test that runs in a child process: opens the part,
/// runs one search and prints the process's resident memory before it and its
/// peak after. A process of its own, so that the writer's memory is not in the
/// peak.
#[cfg(target_os = "linux")]
fn memory_child(mode: &str, dir: &str) {
    let r = PartReader::open(dir).unwrap();
    let q = parse("common").unwrap();
    let before = status_kb("VmRSS:");
    let limits = if mode == "refused" {
        SearchLimits {
            max_leaf_rows: LIMIT,
            ..SearchLimits::default()
        }
    } else {
        SearchLimits::default()
    };
    let result = r.search(&q, &limits);
    match (mode, &result) {
        ("refused", Err(Error::InvalidArgument(m))) => assert!(m.contains("100000 rows"), "{m}"),
        ("allowed", Ok(rows)) => assert_eq!(rows.len(), ROWS as usize),
        other => panic!("unexpected {other:?}"),
    }
    println!("SEARCHMEM {before} {}", status_kb("VmHWM:"));
}

/// A broad query that the limit refuses, and one that it allows, stay within a
/// few times the limit in memory. Each runs in a child process that has only
/// opened the part; the growth is the kernel's high-water mark of the child's
/// resident memory (`VmHWM`) over its resident memory before the search.
#[cfg(target_os = "linux")]
#[test]
fn a_broad_query_stays_within_its_memory_budget() {
    if let Ok(spec) = std::env::var("NWDB_SEARCH_MEMORY_CHILD") {
        let (mode, dir) = spec.split_once(':').unwrap();
        memory_child(mode, dir);
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    // Every row has its own piece, and every piece has the word `common`.
    let data: Vec<Row> = (0..ROWS)
        .map(|i| Row {
            trace: i / 4,
            span: i,
            start: i64::from(i),
            attrs: format!("common line number {i}").into_bytes(),
            events: None,
            links: None,
        })
        .collect();
    let dir = write_part(tmp.path(), 1, &data, 4_096, options());
    drop(data);
    let mut growth = Vec::new();
    for mode in ["refused", "allowed"] {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "a_broad_query_stays_within_its_memory_budget",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                "NWDB_SEARCH_MEMORY_CHILD",
                format!("{mode}:{}", dir.display()),
            )
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "{mode}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let line = text.lines().find(|l| l.contains("SEARCHMEM")).unwrap();
        let n: Vec<usize> = line[line.find("SEARCHMEM").unwrap()..]
            .split_whitespace()
            .skip(1)
            .map(|x| x.parse().unwrap())
            .collect();
        growth.push(n[1].saturating_sub(n[0]));
    }
    let mib = |n: usize| n as f64 / (1 << 20) as f64;
    eprintln!(
        "peak growth during the refused query {:.1} MiB, during the allowed one {:.1} MiB \
         (limit {LIMIT} rows, {ROWS} matching pieces)",
        mib(growth[0]),
        mib(growth[1])
    );
    // The refused query holds the candidate pieces (4 bytes each) and about
    // twice the limit in row numbers; the allowed one the same and the result.
    assert!(growth[0] <= 16 << 20, "{} MiB", mib(growth[0]));
    assert!(growth[1] <= 24 << 20, "{} MiB", mib(growth[1]));
}

// --------------------------------------------------------------- damage ---

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

fn name_of(e: &Error) -> bool {
    matches!(e, Error::Corrupt(_))
}

/// `open`, `verify` or the search reports `Corrupt` for the damaged file
/// (naming it), and a search that succeeds returns exactly the right rows.
fn check_damaged(dir: &Path, file: &str, expected: &[(Query, Vec<u32>)], what: &str) {
    match PartReader::open(dir) {
        Err(e) => {
            assert!(
                name_of(&e) && e.to_string().contains(file),
                "{what}: open gave {e}"
            );
        }
        Ok(r) => {
            // Whatever the search does, it is right or it is Corrupt.
            for (q, want) in expected {
                match r.search(q, &SearchLimits::default()) {
                    Ok(rows) => assert_eq!(&rows, want, "{what}: a wrong answer for {q:?}"),
                    Err(e) => assert!(name_of(&e), "{what}: search gave {e}"),
                }
            }
            let e = r
                .verify()
                .expect_err(&format!("{what}: verify accepted the damage"));
            assert!(
                name_of(&e) && e.to_string().contains(file),
                "{what}: verify gave {e}"
            );
        }
    }
}

#[test]
fn damage_to_the_text_files_is_corrupt_and_never_a_wrong_answer() {
    let mut rng = StdRng::seed_from_u64(11);
    let w = world(&mut rng);
    let data = rows(&mut rng, &w, 12, 0);
    let tmp = tempfile::tempdir().unwrap();
    let good = write_part(tmp.path(), 1, &data, 20, options());
    let reader = PartReader::open(&good).unwrap();
    let mut qrng = StdRng::seed_from_u64(12);
    let expected: Vec<(Query, Vec<u32>)> = (0..25)
        .map(|_| {
            let q = query(&mut qrng, &w, 2);
            let rows = found(&reader, &q);
            (q, rows)
        })
        .collect();
    drop(reader);

    let scratch = tempfile::tempdir().unwrap();
    let dir = scratch.path().join("p");
    for file in [TEXT_INDEX_FILE, TEXT_ROWS_FILE] {
        let original = std::fs::read(good.join(file)).unwrap();
        assert!(original.len() > 100, "{file} is too small to be a test");
        copy_dir(&good, &dir);

        // Missing.
        std::fs::remove_file(dir.join(file)).unwrap();
        match PartReader::open(&dir) {
            Err(e) => assert!(e.to_string().contains(file) && name_of(&e), "{e}"),
            Ok(_) => panic!("{file}: a part without it was opened"),
        }

        // Truncated: every length at the ends and a sample of the rest.
        let len = original.len();
        let mut lengths: Vec<usize> = (0..len.min(60))
            .chain(len.saturating_sub(60)..len)
            .collect();
        lengths.extend((0..len).step_by((len / 40).max(1)));
        for n in lengths {
            std::fs::write(dir.join(file), &original[..n]).unwrap();
            check_damaged(&dir, file, &expected, &format!("{file} cut to {n}"));
        }
        // Extended.
        let mut longer = original.clone();
        longer.push(0);
        std::fs::write(dir.join(file), &longer).unwrap();
        check_damaged(&dir, file, &expected, &format!("{file} extended"));

        // A flipped bit: every byte of the ends, where the footers are, and a
        // sample of the rest.
        let mut positions: Vec<usize> = (0..len.min(80))
            .chain(len.saturating_sub(100)..len)
            .collect();
        positions.extend((0..len).step_by((len / 60).max(1)));
        for pos in positions {
            let mut bytes = original.clone();
            bytes[pos] ^= 1 << (pos % 8);
            std::fs::write(dir.join(file), &bytes).unwrap();
            check_damaged(&dir, file, &expected, &format!("{file} byte {pos}"));
        }
    }
}

#[test]
fn text_files_of_another_part_are_refused() {
    let mut rng = StdRng::seed_from_u64(21);
    let w = world(&mut rng);
    let a = rows(&mut rng, &w, 12, 0);
    let b = rows(&mut rng, &w, 12, 0);
    let tmp = tempfile::tempdir().unwrap();
    let pa = write_part(tmp.path(), 1, &a, 20, options());
    let pb = write_part(tmp.path(), 2, &b, 20, options());
    let scratch = tempfile::tempdir().unwrap();
    let dir = scratch.path().join("p");
    copy_dir(&pa, &dir);
    std::fs::copy(pb.join(TEXT_INDEX_FILE), dir.join(TEXT_INDEX_FILE)).unwrap();
    std::fs::copy(pb.join(TEXT_ROWS_FILE), dir.join(TEXT_ROWS_FILE)).unwrap();
    // Whatever open decides, verify cannot accept them, and a search is right
    // or Corrupt.
    match PartReader::open(&dir) {
        Err(e) => assert!(name_of(&e), "{e}"),
        Ok(r) => {
            let e = r.verify().unwrap_err();
            assert!(name_of(&e), "{e}");
        }
    }
}

// ----------------------------------------------------------------- size ---

fn words(rng: &mut StdRng, vocabulary: &[String], len: usize) -> String {
    let mut out = String::new();
    while out.len() < len {
        // Zipf-like: low indexes are common.
        let i = (rng.random::<f64>().powi(3) * vocabulary.len() as f64) as usize;
        out.push_str(&vocabulary[i.min(vocabulary.len() - 1)]);
        out.push(' ');
    }
    out.truncate(len);
    out
}

/// Agent-shaped data: a system prompt and tool definitions shared by every
/// call of every trace, conversations that grow by a few messages per call
/// and are re-sent in full, large tool results.
fn agent_rows(seed: u64, traces: u32, calls: u32) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(seed);
    let vocabulary: Vec<String> = (0..4_000)
        .map(|_| {
            let n = 2 + rng.random_range(0..8);
            alnum(&mut rng, n)
        })
        .collect();
    let system = words(&mut rng, &vocabulary, 4_000);
    let tools: Vec<String> = (0..6).map(|_| words(&mut rng, &vocabulary, 900)).collect();
    let mut out = Vec::new();
    let mut span = 0u32;
    for t in 0..traces {
        let mut history: Vec<String> = Vec::new();
        for _ in 0..calls {
            history.push(words(&mut rng, &vocabulary, 1_500));
            history.push(words(&mut rng, &vocabulary, 2_000));
            history.push(words(&mut rng, &vocabulary, 6_000));
            let mut lines = vec![system.clone()];
            lines.extend(tools.iter().cloned());
            lines.extend(history.iter().cloned());
            out.push(Row {
                trace: t,
                span,
                start: 1_000 + i64::from(span),
                attrs: lines.join("\n").into_bytes(),
                events: Some(history.last().unwrap().clone().into_bytes()),
                links: None,
            });
            span += 1;
        }
    }
    out
}

#[test]
fn the_index_is_a_small_fraction_of_the_text_it_covers() {
    let data = agent_rows(5, 4, 12);
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path(), 1, &data, 8, options());
    let r = PartReader::open(&dir).unwrap();
    r.verify().unwrap();
    let t = r.text_stats();
    let payload = r.payload_totals();
    let size = |f: &str| std::fs::metadata(dir.join(f)).unwrap().len();
    let part: u64 = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    eprintln!(
        "text: logical {} bytes (every use counted), unique {} bytes in {} pieces; \
         text.idx {} bytes ({:.1}% of logical, {:.1}% of unique); text.rows {} bytes \
         ({:.2}% of logical); whole part {} bytes (pieces.blk {}); index+rows are {:.1}% of the part",
        payload.logical_bytes,
        payload.unique_bytes,
        payload.pieces,
        size(TEXT_INDEX_FILE),
        100.0 * size(TEXT_INDEX_FILE) as f64 / payload.logical_bytes as f64,
        100.0 * size(TEXT_INDEX_FILE) as f64 / payload.unique_bytes as f64,
        size(TEXT_ROWS_FILE),
        100.0 * size(TEXT_ROWS_FILE) as f64 / payload.logical_bytes as f64,
        part,
        size("pieces.blk"),
        100.0 * (size(TEXT_INDEX_FILE) + size(TEXT_ROWS_FILE)) as f64 / part as f64,
    );
    assert_eq!(t.index_bytes, size(TEXT_INDEX_FILE));
    assert_eq!(t.row_list_bytes, size(TEXT_ROWS_FILE));
    assert_eq!(t.text_bytes, payload.unique_bytes);
    // Each piece is indexed once, however many spans repeat it, so the index
    // is small next to the text the rows refer to.
    assert!(
        size(TEXT_INDEX_FILE) * 10 < payload.logical_bytes,
        "the index is {} bytes for {} bytes of text",
        size(TEXT_INDEX_FILE),
        payload.logical_bytes
    );
    assert!(size(TEXT_ROWS_FILE) * 100 < payload.logical_bytes);
}
