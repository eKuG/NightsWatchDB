//! Search over message text as OTLP senders store it.
//!
//! The GenAI conventions put the messages of a call in `gen_ai.input.messages`
//! as a JSON string, so a message's text is JSON-escaped: a line break is the
//! two bytes `\n`, a tab `\t`, and an encoder that writes ASCII only (Python's
//! `json.dumps` by default) writes `é` as `\u00e9`. The splitter cuts each
//! message as stored. A piece that is a complete JSON object, array or string
//! is indexed, and a phrase is confirmed against it, with the escapes of its
//! strings decoded; any other piece is used as it is. The server README
//! promises that a word matches a span if any of its messages has it.

mod common;

use common::*;
use nwdb_core::{BatchLimits, part_dir_name};
use nwdb_otlp::{OtlpSplitter, TraceDecoder};
use nwdb_segment::textindex::parse;
use nwdb_segment::{PartReader, PartWriter, SearchLimits, WriterOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use std::sync::Arc;

/// A part with one span per value of `gen_ai.input.messages`, row `i` holding
/// `values[i]`, and a function that returns the rows a query matches.
struct Rig {
    reader: PartReader,
    _dir: tempfile::TempDir,
}

impl Rig {
    fn new(values: &[&str]) -> Self {
        let spans = values
            .iter()
            .enumerate()
            .map(|(i, v)| Span {
                trace_id: vec![i as u8 + 1; 16],
                span_id: vec![i as u8 + 1; 8],
                name: "chat".into(),
                start_time_unix_nano: 1_000,
                end_time_unix_nano: 2_000,
                attributes: vec![kv("gen_ai.input.messages", str_val(v))],
                ..Default::default()
            })
            .collect();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();
        let batches = TraceDecoder::new(BatchLimits::DEFAULT)
            .decode(&request)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let options = WriterOptions {
            splitter: Some(Arc::new(OtlpSplitter::default())),
            ..WriterOptions::default()
        };
        let mut w = PartWriter::create(dir.path(), 1, 0, 900_000_000_000, options).unwrap();
        for b in &batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
        let reader = PartReader::open(dir.path().join(part_dir_name(1))).unwrap();
        Self { reader, _dir: dir }
    }

    fn found(&self, q: &str) -> Vec<u32> {
        self.reader
            .search(&parse(q).unwrap(), &SearchLimits::default())
            .unwrap()
    }
}

#[test]
fn a_word_after_a_json_escape_in_a_message_is_found() {
    let messages = r#"[{"role":"user","content":"The tool call failed.\nError: connection timed out\tafter 30 seconds at the café"}]"#;
    let rig = Rig::new(&[messages]);
    // The message is a piece and is indexed.
    assert_eq!(rig.found("connection"), [0]);
    for q in ["error", "after", "caf\u{e9}", "\"failed error\""] {
        assert_eq!(
            rig.found(q),
            [0],
            "`{q}` is in the message but is not found"
        );
    }
}

#[test]
fn every_kind_of_escape_is_decoded() {
    let m = r#"[{"role":"user","content":"one\ntwo\tthree\rfour\bfive\fsix \"seven\" eight\\nine sl\/ash caf\u00e9 \u65e5\u672c \ud83d\ude00 smile lone\ud800end x\u0041y"}]"#;
    let rig = Rig::new(&[m]);
    for w in [
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "sl",
        "ash",
        "caf\u{e9}",
        "\u{65e5}\u{672c}",
        "smile",
        "xay",
    ] {
        assert_eq!(rig.found(w), [0], "`{w}`");
    }
    // each escape is a word break, and a quote or a backslash is one too
    assert_eq!(
        rig.found("\"one two three four five six seven eight nine\""),
        [0]
    );
    assert_eq!(rig.found("\"sl ash caf\u{e9}\""), [0]);
    // the pair of surrogates is one character: the emoji sits in its own
    // token, and the letters around the escapes are not glued to anything
    assert_eq!(rig.found("\"\u{65e5}\u{672c} \u{1f600}\""), [0]);
    // a lone surrogate stays as its six bytes: backslash, `ud800end`
    assert_eq!(rig.found("ud800end"), [0]);
    assert!(rig.found("end").is_empty());
    // the letters of an escape are not a word of their own
    for w in ["ntwo", "tthree", "u00e9", "u65e5", "ud83d", "nnine"] {
        assert!(rig.found(w).is_empty(), "`{w}`");
    }
}

#[test]
fn a_phrase_across_an_escaped_newline_is_found() {
    let rig = Rig::new(&[
        r#"[{"role":"user","content":"the job failed.\nError: disk is full"}]"#,
        r#"[{"role":"user","content":"the job failed. Error: disk is full"}]"#,
        r#"[{"role":"user","content":"the job failed. Warning: disk is full"}]"#,
    ]);
    assert_eq!(rig.found("\"failed error disk\""), [0, 1]);
    assert_eq!(rig.found("\"failed warning\""), [2]);
    assert_eq!(rig.found("\"full error\""), Vec::<u32>::new());
}

#[test]
fn one_piece_shared_by_two_rows_is_found_in_both() {
    let m = r#"[{"role":"user","content":"first line\nsecond line caf\u00e9"}]"#;
    let rig = Rig::new(&[m, "[{\"role\":\"user\",\"content\":\"other\"}]", m]);
    assert_eq!(rig.found("second"), [0, 2]);
    assert_eq!(rig.found("\"line second\""), [0, 2]);
    assert_eq!(rig.found("caf\u{e9}"), [0, 2]);
}

#[test]
fn plain_text_with_backslashes_is_tokenised_as_it_is() {
    let rig = Rig::new(&[
        r"C:\new\table notes and more words here",
        r"not json \n at all, so \nthe letter stays",
    ]);
    // a path is not JSON: the letter after a backslash is part of the word
    assert_eq!(rig.found("c"), [0]);
    assert_eq!(rig.found("new"), [0]);
    assert_eq!(rig.found("table"), [0]);
    assert!(rig.found("able").is_empty());
    assert_eq!(rig.found("nthe"), [1]);
    assert!(rig.found("the").is_empty());
}

#[test]
fn invalid_json_with_escapes_is_plain_text() {
    let rig = Rig::new(&[
        // the closing brace is missing
        r#"[{"role":"user","content":"break\nhere and caf\u00e9"#,
        // a bad escape
        r#"[{"role":"user","content":"break\qhere again\nnext"}]"#,
        // a comma too many
        r#"[{"role":"user","content":"stray\ncomma",}]"#,
    ]);
    assert_eq!(rig.found("nhere"), [0]);
    assert!(rig.found("here").iter().all(|&r| r != 0));
    assert_eq!(rig.found("u00e9"), [0]);
    assert_eq!(rig.found("qhere"), [1]);
    assert_eq!(rig.found("nnext"), [1]);
    assert_eq!(rig.found("ncomma"), [2]);
    assert!(rig.found("comma").is_empty());
}

#[test]
fn deeply_nested_json_is_plain_text_and_does_not_overflow_the_stack() {
    let depth = 100_000;
    let deep = format!(
        "{}\"line\\nbreak and more words\"{}",
        "[".repeat(depth),
        "]".repeat(depth)
    );
    let rig = Rig::new(&[&deep]);
    assert_eq!(rig.found("nbreak"), [0]);
    assert!(rig.found("break").is_empty());
    // within the bound the same text is JSON and is decoded
    let shallow = format!(
        "{}\"line\\nbreak and more words\"{}",
        "[".repeat(100),
        "]".repeat(100)
    );
    let rig = Rig::new(&[&shallow]);
    assert_eq!(rig.found("break"), [0]);
}
