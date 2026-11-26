//! Pieces that are JSON text are indexed, and phrases are confirmed against
//! them, with the escapes of their strings decoded; the index and the
//! confirmation must agree on every piece, whatever its bytes.
//!
//! The pieces here are made of fragments that stress the rule: valid and
//! invalid JSON, every escape, surrogates, quotes, backslashes outside any
//! string, and random bytes. Every word and every phrase of the decoded text
//! of a random piece is searched, and the rows must be those of a scan of the
//! decoded texts. Each search is run twice and must answer the same.

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_payload::json_search_text;
use nwdb_segment::{PartReader, PartWriter, SearchLimits, WriterOptions};
use nwdb_textindex::{parse, phrase_matches, tokens};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::ops::Range;
use std::sync::Arc;

struct Whole;

impl PayloadSplitter for Whole {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.push(0..value.len());
    }
}

const FRAGMENTS: &[&str] = &[
    "{\"role\":\"user\",\"content\":\"",
    "\"}",
    "[",
    "]",
    "{",
    "}",
    "\"",
    ",",
    ":",
    "\\n",
    "\\t",
    "\\\"",
    "\\\\",
    "\\/",
    "\\u00e9",
    "\\u65e5",
    "\\ud83d\\ude00",
    "\\ud83d",
    "\\ude00",
    "\\q",
    "\\",
    "\n",
    " ",
    " ",
    "alpha",
    "beta",
    "Gamma",
    "delta",
    "caf\u{e9}",
    "x1",
    "12",
    "true",
    "C:\\new\\table",
];

fn piece(rng: &mut StdRng, row: usize) -> Vec<u8> {
    let mut out = Vec::new();
    // about half the pieces are wrapped to be valid JSON
    let wrap = rng.random_range(0..4);
    if wrap == 0 {
        out.extend_from_slice(b"[{\"content\":\"");
    } else if wrap == 1 {
        out.push(b'"');
    }
    for _ in 0..rng.random_range(3..40) {
        match rng.random_range(0..30) {
            0 if wrap >= 2 => out.push(rng.random()),
            0 => out.push(b'z'),
            _ => {
                let f = FRAGMENTS[rng.random_range(0..FRAGMENTS.len())];
                // inside a wrapped string keep out what would break it: raw
                // quotes and line breaks, a bad escape and a dangling one
                let breaks = (f.contains('"') && !f.contains("\\\"")) || f.contains('\n');
                if wrap < 2 && (breaks || f == "\\q" || f == "\\") {
                    out.extend_from_slice(b" w ");
                } else {
                    out.extend_from_slice(f.as_bytes());
                }
            }
        }
    }
    // a word of its own, so that no two pieces are the same
    out.extend_from_slice(format!(" row{row} ").as_bytes());
    if wrap == 0 {
        out.extend_from_slice(b"\"}]");
    } else if wrap == 1 {
        out.push(b'"');
    }
    out
}

#[test]
fn the_index_and_the_confirmation_agree_on_every_piece() {
    let mut rng = StdRng::seed_from_u64(77);
    let values: Vec<Vec<u8>> = (0..600).map(|i| piece(&mut rng, i)).collect();
    let valid = values
        .iter()
        .filter(|v| nwdb_payload::is_json_text(v))
        .count();
    assert!(
        valid > 100 && valid < 500,
        "{valid} of {} are JSON",
        values.len()
    );
    assert!(
        values
            .iter()
            .any(|v| matches!(json_search_text(v), std::borrow::Cow::Owned(_)))
    );

    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        splitter: Some(Arc::new(Whole)),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(tmp.path(), 1, 0, 900_000_000_000, options).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1000,
        max_bytes: 1 << 30,
    });
    for (i, v) in values.iter().enumerate() {
        let n = (i as u128 + 1).to_be_bytes();
        b.append(&SpanRecord {
            trace_id: TraceId(n),
            span_id: SpanId(n[8..].try_into().unwrap()),
            name: "span",
            start_ns: 1,
            end_ns: 2,
            attrs: v,
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    let r = PartReader::open(tmp.path().join(part_dir_name(1))).unwrap();

    let texts: Vec<_> = values.iter().map(|v| json_search_text(v)).collect();
    let search = |q: &str| {
        r.search(&parse(q).unwrap(), &SearchLimits::default())
            .unwrap()
    };
    let mut checked = 0;
    for _ in 0..400 {
        let src = rng.random_range(0..values.len());
        // A token that is not UTF-8 cannot be written in a query, but it
        // still separates its neighbours, so it stays in the list as None.
        let toks: Vec<Option<String>> = tokens(&texts[src])
            .map(|t| String::from_utf8(t.raw().to_ascii_lowercase()).ok())
            .collect();
        if toks.len() < 2 {
            continue;
        }
        let at = rng.random_range(0..toks.len() - 1);
        let n = rng.random_range(1..=(toks.len() - at).min(3));
        let Some(phrase) = toks[at..at + n].iter().cloned().collect::<Option<Vec<_>>>() else {
            continue;
        };
        let phrase = &phrase[..];
        let q = if n == 1 {
            phrase[0].clone()
        } else {
            format!("\"{}\"", phrase.join(" "))
        };
        if parse(&q).is_err() {
            continue;
        }
        let want: Vec<u32> = texts
            .iter()
            .enumerate()
            .filter(|(_, t)| phrase_matches(t, phrase))
            .map(|(i, _)| i as u32)
            .collect();
        assert!(want.contains(&(src as u32)), "{q}");
        let first = search(&q);
        assert_eq!(first, want, "`{q}`");
        assert_eq!(
            search(&q),
            first,
            "`{q}` answered differently the second time"
        );
        checked += 1;
    }
    assert!(checked > 300, "{checked}");
}
