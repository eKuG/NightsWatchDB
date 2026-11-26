//! Full-text search over parts written from decoded OTLP requests of
//! agent-shaped traces with the real `OtlpSplitter`: messages repeated across
//! spans and traces, text under content keys as JSON arrays and as structured
//! values, text in events and links, long plain attributes, short values that
//! stay inline, words over 64 bytes (a secret-like key inside a prompt) and
//! non-ASCII text. Every search equals a brute-force scan of a model whose
//! definition of a row's text is "the pieces the writer stored for it".

mod common;

use arrow_array::RecordBatch;
use common::*;
use nwdb_core::{BatchLimits, PayloadSplitter, RawColumn, SpanId, TraceId, part_dir_name};
use nwdb_otlp::{OtlpSplitter, TraceDecoder};
use nwdb_payload::{MAX_PIECE_BYTES, rope};
use nwdb_segment::textindex::{Query, contains_token, phrase_matches, tokens};
use nwdb_segment::{
    DEFAULT_MIN_PIECE_BYTES, MergeOptions, PartReader, PartWriter, SearchLimits, WriterOptions,
    merge_parts,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span,
    span::{Event, Link},
};
use prost::Message;
use rand::Rng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;

const WORDS: &[&str] = &[
    "refund",
    "invoice",
    "Paris",
    "weather",
    "ERROR",
    "timeout",
    "retry",
    "assistant",
    "tool",
    "caf\u{e9}",
    "na\u{ef}ve",
    "\u{65e5}\u{672c}\u{8a9e}",
    "go",
    "now",
    "please",
    "balance",
    "zq001",
    "zq002",
    "zq003",
    "zq004",
];

fn sentence(rng: &mut StdRng, n: usize) -> String {
    (0..n)
        .map(|_| *WORDS.choose(rng).unwrap())
        .collect::<Vec<_>>()
        .join(" ")
}

fn alnum(rng: &mut StdRng, n: usize) -> String {
    (0..n)
        .map(|_| char::from(b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.random_range(0..36)]))
        .collect()
}

struct Pool {
    messages: Vec<String>,
    systems: Vec<String>,
}

fn pool(rng: &mut StdRng) -> Pool {
    let secret = alnum(rng, 90);
    let mut messages: Vec<String> = (0..16)
        .map(|_| {
            let n = rng.random_range(8..30);
            sentence(rng, n)
        })
        .collect();
    messages.push(format!("my api key is {secret} do not share it please"));
    messages.push("go go go now go go please keep going".to_string());
    messages.push(format!("{} {} caf\u{e9}", sentence(rng, 6), secret));
    let systems = (0..2)
        .map(|_| {
            let n = rng.random_range(40..60);
            sentence(rng, n)
        })
        .collect();
    Pool { messages, systems }
}

fn message_json(role: &str, text: &str) -> String {
    format!(r#"{{"role":"{role}","content":"{text}"}}"#)
}

fn spans(rng: &mut StdRng, p: &Pool, traces: u8, calls: u32) -> Vec<Span> {
    let mut out = Vec::new();
    let mut n = 0u64;
    for t in 0..traces {
        let mut history: Vec<String> = Vec::new();
        for c in 0..calls {
            n += 1;
            history.push(message_json(
                "user",
                p.messages.choose(rng).unwrap().as_str(),
            ));
            let input = format!("[{}]", history.join(", "));
            let answer = p.messages.choose(rng).unwrap().clone();
            history.push(message_json("assistant", &answer));
            let attrs = vec![
                kv("gen_ai.operation.name", str_val("chat")),
                kv("gen_ai.input.messages", str_val(&input)),
                kv(
                    "gen_ai.system_instructions",
                    str_val(p.systems.choose(rng).unwrap()),
                ),
                // A structured value under a content key: a list of strings.
                kv(
                    "gen_ai.prompt",
                    Value::ArrayValue(ArrayValue {
                        values: vec![
                            any(str_val(p.messages.choose(rng).unwrap())),
                            any(str_val(p.messages.choose(rng).unwrap())),
                        ],
                    }),
                ),
                // A key-value list under a content key.
                kv(
                    "llm.input_messages.0",
                    Value::KvlistValue(KeyValueList {
                        values: vec![
                            kv("role", str_val("user")),
                            kv("content", str_val(p.messages.choose(rng).unwrap())),
                        ],
                    }),
                ),
                // Long and plain: cut by length, not by key.
                kv("custom.note", str_val(&sentence(rng, 60))),
                // Short: stays in the row, so is not searchable.
                kv("custom.short", str_val("ok fine")),
            ];
            out.push(Span {
                trace_id: vec![t + 1; 16],
                span_id: n.to_be_bytes().to_vec(),
                name: format!("chat {c}"),
                start_time_unix_nano: 1_000_000 + n,
                end_time_unix_nano: 2_000_000 + n,
                attributes: attrs,
                events: vec![Event {
                    name: "gen_ai.choice".into(),
                    time_unix_nano: 1_500_000 + n,
                    attributes: vec![kv("gen_ai.completion", str_val(&answer))],
                    ..Default::default()
                }],
                links: vec![Link {
                    trace_id: vec![t + 1; 16],
                    span_id: (n + 1).to_be_bytes().to_vec(),
                    attributes: vec![kv(
                        "gen_ai.tool.call.result",
                        str_val(p.messages.choose(rng).unwrap()),
                    )],
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
    }
    out
}

fn decode(spans: Vec<Span>) -> Vec<RecordBatch> {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", str_val("agent"))],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    TraceDecoder::new(BatchLimits::DEFAULT)
        .decode(&request)
        .unwrap()
}

fn options() -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(OtlpSplitter::default())),
        row_group_rows: 40,
        ..WriterOptions::default()
    }
}

fn write_part(parts: &Path, id: u64, batches: &[RecordBatch]) -> PartReader {
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options()).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    PartReader::open(parts.join(part_dir_name(id))).unwrap()
}

/// The pieces the writer stores for each row of `batches`: the splitter's
/// cuts of each raw column, under the writer's rules.
fn row_pieces(batches: &[RecordBatch]) -> Vec<(TraceId, SpanId, Vec<Vec<u8>>)> {
    let splitter = OtlpSplitter::default();
    all_rows(batches)
        .iter()
        .map(|r| {
            let mut pieces = Vec::new();
            let columns = [
                (RawColumn::Attrs, Some(r.attrs)),
                (RawColumn::Events, r.events),
                (RawColumn::Links, r.links),
            ];
            for (column, value) in columns {
                let Some(value) = value else { continue };
                let mut cuts = Vec::new();
                splitter.cuts(column, value, &mut cuts);
                if !rope::cuts_are_valid(value.len(), &cuts)
                    || cuts.iter().any(|c| c.len() > MAX_PIECE_BYTES)
                {
                    continue;
                }
                pieces.extend(
                    cuts.into_iter()
                        .filter(|c| c.len() >= DEFAULT_MIN_PIECE_BYTES)
                        .map(|c| value[c].to_vec()),
                );
            }
            (r.trace_id, r.span_id, pieces)
        })
        .collect()
}

fn matches(q: &Query, pieces: &[Vec<u8>]) -> bool {
    match q {
        Query::Term(t) => pieces.iter().any(|p| contains_token(p, t.as_bytes())),
        Query::Prefix(pre) => pieces
            .iter()
            .any(|p| tokens(p).any(|t| t.raw().to_ascii_lowercase().starts_with(pre.as_bytes()))),
        Query::Phrase(toks) => pieces.iter().any(|p| phrase_matches(p, toks)),
        Query::And(v) => v.iter().all(|q| matches(q, pieces)),
        Query::Or(v) => v.iter().any(|q| matches(q, pieces)),
        Query::Not(q) => !matches(q, pieces),
    }
}

fn lower(t: &[u8]) -> String {
    String::from_utf8_lossy(&t.to_ascii_lowercase()).into_owned()
}

fn leaf(rng: &mut StdRng, rows: &[(TraceId, SpanId, Vec<Vec<u8>>)]) -> Query {
    let (_, _, pieces) = loop {
        let r = rows.choose(rng).unwrap();
        if !r.2.is_empty() {
            break r;
        }
    };
    let piece = pieces.choose(rng).unwrap();
    let toks: Vec<String> = tokens(piece).map(|t| lower(t.raw())).collect();
    match rng.random_range(0..9) {
        0..=3 => Query::Term(toks.choose(rng).unwrap().clone()),
        4 => Query::Term("absent".into()),
        5 => {
            let w = toks.choose(rng).unwrap();
            let chars: Vec<(usize, char)> = w.char_indices().collect();
            if chars.len() < 2 {
                return Query::Term(w.clone());
            }
            let n = rng.random_range(2..=chars.len().min(5));
            let end = chars.get(n).map_or(w.len(), |&(i, _)| i);
            Query::Prefix(w[..end].to_string())
        }
        _ => {
            let len = rng.random_range(2..=4).min(toks.len());
            let at = rng.random_range(0..=toks.len() - len);
            Query::Phrase(toks[at..at + len].to_vec())
        }
    }
}

fn query(rng: &mut StdRng, rows: &[(TraceId, SpanId, Vec<Vec<u8>>)], depth: u32) -> Query {
    if depth == 0 || rng.random_bool(0.4) {
        return leaf(rng, rows);
    }
    let mut ops: Vec<Query> = (0..rng.random_range(2..=3))
        .map(|_| query(rng, rows, depth - 1))
        .collect();
    match rng.random_range(0..4) {
        0 => Query::And(ops),
        1 => Query::Or(ops),
        2 => Query::Not(Box::new(ops.remove(0))),
        _ => {
            ops.push(Query::Not(Box::new(leaf(rng, rows))));
            Query::And(ops)
        }
    }
}

fn found(r: &PartReader, q: &Query) -> Vec<u32> {
    r.search(q, &SearchLimits::default()).unwrap()
}

#[test]
fn search_over_otlp_agent_traces_equals_a_brute_force_scan() {
    let mut rng = common::rng(31);
    let p = pool(&mut rng);
    let tmp = tempfile::tempdir().unwrap();
    // Three parts of different traces, then all three merged.
    let groups: Vec<Vec<RecordBatch>> = (0..3)
        .map(|g| {
            let mut spans = spans(&mut rng, &p, 2, 8);
            for s in &mut spans {
                s.trace_id = vec![s.trace_id[0] + 10 * g; 16];
                s.span_id = (u64::from_be_bytes(s.span_id.clone().try_into().unwrap())
                    + 1_000 * u64::from(g))
                .to_be_bytes()
                .to_vec();
            }
            decode(spans)
        })
        .collect();
    let readers: Vec<PartReader> = groups
        .iter()
        .enumerate()
        .map(|(i, b)| write_part(tmp.path(), i as u64 + 1, b))
        .collect();
    let (_, stats) = merge_parts(
        &readers.iter().collect::<Vec<_>>(),
        tmp.path(),
        9,
        0,
        WINDOW_NS,
        MergeOptions {
            writer: options(),
            ..MergeOptions::default()
        },
    )
    .unwrap();
    assert_eq!(stats.duplicates_dropped, 0);
    let merged = PartReader::open(tmp.path().join(part_dir_name(9))).unwrap();
    merged.verify().unwrap();

    let all_batches: Vec<RecordBatch> = groups.iter().flatten().cloned().collect();
    let model_all = row_pieces(&all_batches);
    let mut nonempty = 0;
    for i in 0..150 {
        let q = query(&mut rng, &model_all, 3);
        // Each part against its own rows.
        for (r, batches) in readers.iter().zip(&groups) {
            let model = row_pieces(batches);
            let want: Vec<u32> = model
                .iter()
                .enumerate()
                .filter(|(_, (_, _, p))| matches(&q, p))
                .map(|(i, _)| i as u32)
                .collect();
            assert_eq!(found(r, &q), want, "one part, query {i}: {q:?}");
        }
        // The merged part against the model of all rows, by trace and span.
        let want: BTreeSet<(TraceId, SpanId)> = model_all
            .iter()
            .filter(|(_, _, p)| matches(&q, p))
            .map(|(t, s, _)| (*t, *s))
            .collect();
        let ids = ids_of_full(&merged);
        let got: BTreeSet<(TraceId, SpanId)> = found(&merged, &q)
            .into_iter()
            .map(|row| ids[row as usize])
            .collect();
        nonempty += usize::from(!want.is_empty());
        assert_eq!(got, want, "merged, query {i}: {q:?}");
    }
    assert!(nonempty >= 40, "only {nonempty} queries found spans");

    // The short value stayed inline and the long plain one did not.
    let short = Query::Term("fine".into());
    assert!(found(&readers[0], &short).is_empty());
    let stats = readers[0].text_stats();
    assert!(stats.pieces > 10, "{stats:?}");
    // Every search after a restart.
    let reopened = PartReader::open(tmp.path().join(part_dir_name(1))).unwrap();
    let q = Query::Term("refund".into());
    assert_eq!(found(&reopened, &q), found(&readers[0], &q));
}

fn ids_of_full(r: &PartReader) -> Vec<(TraceId, SpanId)> {
    let batches = r.read_rows(0, r.row_count(), None).unwrap();
    all_rows(&batches)
        .iter()
        .map(|r| (r.trace_id, r.span_id))
        .collect()
}
