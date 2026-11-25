//! Parts written from decoded OTLP requests of agent-shaped traces, with the
//! `OtlpSplitter`: every value reads back byte for byte, and the repeated
//! text is stored once.

mod common;

use arrow_array::RecordBatch;
use common::*;
use nwdb_core::{BatchLimits, PartMeta, part_dir_name};
use nwdb_otlp::{OtlpSplitter, TraceDecoder};
use nwdb_segment::{PartReader, PartWriter, ScanOptions, WriterOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span,
    span::{Event, Link},
};
use prost::Message;
use rand::Rng;
use rand::rngs::StdRng;
use std::path::Path;
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;

fn words(rng: &mut StdRng, len: usize) -> String {
    let mut out = String::with_capacity(len + 8);
    while out.len() < len {
        for _ in 0..rng.random_range(2..9) {
            out.push(char::from(b'a' + rng.random_range(0..26u8)));
        }
        out.push(' ');
    }
    out.truncate(len);
    out
}

fn message(role: &str, text: &str) -> String {
    format!(r#"{{"role":"{role}","content":"{text}"}}"#)
}

/// A conversation that grows by a few messages on every call: a system
/// prompt and tool definitions shared by every trace, a user message, the
/// model's answer and a large tool result per call, each call re-sending
/// everything before it.
fn agent_requests(seed: u64, traces: u8, calls: u32, spans_per_request: usize) -> Vec<Vec<u8>> {
    let mut rng = rng(seed);
    let system = words(&mut rng, 4_000);
    let tools = format!(
        "[{}]",
        (0..6)
            .map(|i| {
                format!(
                    r#"{{"name":"tool_{i}","description":"{}","parameters":{{"type":"object"}}}}"#,
                    words(&mut rng, 900)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut spans: Vec<Span> = Vec::new();
    let mut n = 0u64;
    for t in 0..traces {
        let mut history: Vec<String> = Vec::new();
        for c in 0..calls {
            n += 1;
            history.push(message("user", &words(&mut rng, 1_500)));
            let input = format!("[{}]", history.join(", "));
            let answer = words(&mut rng, 2_000);
            let output = format!("[{}]", message("assistant", &answer));
            history.push(message("assistant", &answer));
            let result = words(&mut rng, 6_000);
            history.push(message("tool", &result));
            let attr = |k: &str, v: &str| kv(k, str_val(v));
            spans.push(Span {
                trace_id: vec![t + 1; 16],
                span_id: n.to_be_bytes().to_vec(),
                name: format!("chat {c}"),
                start_time_unix_nano: 1_000_000 + n,
                end_time_unix_nano: 2_000_000 + n,
                attributes: vec![
                    attr("gen_ai.operation.name", "chat"),
                    attr("gen_ai.request.model", "model-x"),
                    kv(
                        "gen_ai.usage.input_tokens",
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(
                            input.len() as i64 / 4,
                        ),
                    ),
                    attr("gen_ai.system_instructions", &system),
                    attr("gen_ai.tool.definitions", &tools),
                    attr("gen_ai.input.messages", &input),
                    attr("gen_ai.output.messages", &output),
                ],
                events: vec![Event {
                    name: "gen_ai.choice".into(),
                    time_unix_nano: 1_500_000 + n,
                    attributes: vec![attr("gen_ai.completion", &answer)],
                    ..Default::default()
                }],
                links: vec![Link {
                    trace_id: vec![t + 1; 16],
                    span_id: (n + 1).to_be_bytes().to_vec(),
                    attributes: vec![attr("gen_ai.tool.call.result", &result)],
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
    }
    spans
        .chunks(spans_per_request)
        .map(|chunk| {
            ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![kv("service.name", str_val("agent"))],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(InstrumentationScope {
                            name: "test".into(),
                            ..Default::default()
                        }),
                        spans: chunk.to_vec(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        })
        .collect()
}

fn decode_all(requests: &[Vec<u8>]) -> Vec<RecordBatch> {
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    requests
        .iter()
        .flat_map(|r| decoder.decode(r).unwrap())
        .collect()
}

fn write_part(parts: &Path, id: u64, batches: &[RecordBatch], options: WriterOptions) -> PartMeta {
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap()
}

fn with_splitter() -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(OtlpSplitter::default())),
        row_group_rows: 40,
        ..WriterOptions::default()
    }
}

/// Reads the part three ways and compares every row, every column, with the
/// decoded batches it was written from.
fn assert_identical(dir: &Path, written: &[RecordBatch]) {
    let want = all_rows(written);
    let reader = PartReader::open(dir).unwrap();
    reader.verify().unwrap();
    let rows = reader.row_count();
    assert_eq!(rows as usize, want.len());

    let all = reader.read_rows(0, rows, None).unwrap();
    assert_eq!(all_rows(&all), want, "read_rows");
    let scanned: Vec<RecordBatch> = reader
        .scan(ScanOptions::default())
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(all_rows(&scanned), want, "scan");
    for e in reader.trace_entries() {
        let got = reader.read_trace(e.trace_id, None).unwrap().unwrap();
        let got_rows = all_rows(&got);
        let want_trace: Vec<_> = want
            .iter()
            .filter(|r| r.trace_id == e.trace_id)
            .cloned()
            .collect();
        assert_eq!(got_rows, want_trace, "read_trace {}", e.trace_id);
    }
}

#[test]
fn agent_traces_read_back_identical_and_shrink() {
    let requests = agent_requests(1, 3, 30, 7);
    let batches = decode_all(&requests);
    let tmp = tempfile::tempdir().unwrap();
    let plain = write_part(
        tmp.path(),
        1,
        &batches,
        WriterOptions {
            row_group_rows: 40,
            ..WriterOptions::default()
        },
    );
    let split = write_part(tmp.path(), 2, &batches, with_splitter());
    assert_identical(&tmp.path().join(part_dir_name(1)), &batches);
    assert_identical(&tmp.path().join(part_dir_name(2)), &batches);

    let total = |m: &PartMeta| m.files.values().sum::<u64>();
    let p = split.payload;
    eprintln!(
        "OTLP agent data: logical {} unique {} stored {} pieces {} => unique/logical {:.4}, \
         stored/logical {:.4}; part {} bytes vs {} without the store ({:.3})",
        p.logical_bytes,
        p.unique_bytes,
        p.stored_bytes,
        p.pieces,
        p.unique_bytes as f64 / p.logical_bytes as f64,
        p.stored_bytes as f64 / p.logical_bytes as f64,
        total(&split),
        total(&plain),
        total(&split) as f64 / total(&plain) as f64
    );
    // Most text repeats an earlier call's, so what is stored is a small
    // fraction of what the rows say. These bounds fail if sharing stops
    // working.
    assert!(p.unique_bytes * 8 < p.logical_bytes, "{p:?}");
    assert!(p.stored_bytes * 15 < p.logical_bytes, "{p:?}");
    assert!(total(&split) < total(&plain), "{p:?}");
}

#[test]
fn a_tiny_window_and_tiny_blocks_still_read_back_identical() {
    let batches = decode_all(&agent_requests(2, 3, 12, 5));
    let tmp = tempfile::tempdir().unwrap();
    let narrow = write_part(
        tmp.path(),
        1,
        &batches,
        WriterOptions {
            dedup_window_bytes: 16 * 1024,
            piece_block_bytes: 4 * 1024,
            ..with_splitter()
        },
    );
    let wide = write_part(tmp.path(), 2, &batches, with_splitter());
    assert_identical(&tmp.path().join(part_dir_name(1)), &batches);
    assert!(narrow.payload.unique_bytes > 5 * 16 * 1024);
    assert!(narrow.payload.unique_bytes >= wide.payload.unique_bytes);
}

#[test]
fn random_requests_read_back_identical() {
    // Arbitrary attribute shapes from the generator of the decoder tests,
    // with a splitter that cuts almost everything.
    let mut rng = rng(5);
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let tmp = tempfile::tempdir().unwrap();
    let mut batches = Vec::new();
    for _ in 0..12 {
        let mut req = rand_request(&mut rng, false);
        // One trace per request keeps the traces contiguous across requests.
        let fresh = u8::try_from(batches.len() + 1).unwrap();
        for rs in &mut req.resource_spans {
            for ss in &mut rs.scope_spans {
                for s in &mut ss.spans {
                    s.trace_id = vec![fresh; 16];
                }
            }
        }
        batches.extend(decoder.decode(&req.encode_to_vec()).unwrap());
    }
    assert!(!all_rows(&batches).is_empty());
    let options = WriterOptions {
        splitter: Some(Arc::new(OtlpSplitter::new(1))),
        piece_block_bytes: 512,
        dedup_window_bytes: 2_048,
        ..WriterOptions::default()
    };
    let meta = write_part(tmp.path(), 1, &batches, options);
    assert!(meta.payload.pieces > 0);
    assert_identical(&tmp.path().join(part_dir_name(1)), &batches);
}
