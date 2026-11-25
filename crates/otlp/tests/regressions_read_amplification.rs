//! Reading one small span decompresses each piece block at most once, however
//! many references the value has and however they are spread over the blocks:
//! a value whose references go round more blocks than a cache could hold must
//! not pay a whole block for every reference. The pieces under a content key
//! can be as small as one JSON element, so a sender decides how many
//! references a value has and in which blocks they land.

mod common;

use common::*;
use nwdb_core::{BatchLimits, TraceId};
use nwdb_otlp::{OtlpSplitter, TraceDecoder};
use nwdb_segment::{PartReader, PartWriter, WriterOptions};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use rand::RngCore;
use std::sync::Arc;

const MIB: usize = 1 << 20;
/// Bytes of a marker. Elements shorter than 32 bytes on average make the
/// splitter cut a whole array as one piece, and a cut shorter than 8 bytes is
/// stored inline; the markers must be real pieces of their own.
const MARKER: usize = 40;

fn marker(i: usize) -> String {
    format!("m{i:02}-{}", "k".repeat(MARKER - 4))
}
/// Blocks of 1 MiB each (the default block size): more than the 32 MiB a
/// read keeps.
const BLOCKS: usize = 40;
/// How many times the small span goes round the blocks.
const ROUNDS: usize = 25;

fn span(trace: u8, n: u64, messages: Value) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: (n + 1).to_be_bytes().to_vec(),
        name: "chat".into(),
        start_time_unix_nano: 1_000 + n,
        end_time_unix_nano: 2_000 + n,
        attributes: vec![kv("gen_ai.input.messages", messages)],
        ..Default::default()
    }
}

fn request(spans: Vec<Span>) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[test]
fn one_span_decompresses_each_block_at_most_once() {
    let mut rng = rng(3);
    // Trace 1: span i holds the JSON array [m<i>, "<filler>"]. The marker
    // and the filler are two pieces that fill block i exactly (40 + 1 MiB - 40
    // bytes), so marker i lives alone with its filler in block i.
    let mut spans = Vec::new();
    for i in 0..BLOCKS {
        let mut value = format!("[{},\"", marker(i)).into_bytes();
        let mut filler = vec![0u8; MIB - MARKER - 2];
        rng.fill_bytes(&mut filler);
        for b in &mut filler {
            if *b == b'"' || *b == b'\\' {
                *b = b'x';
            }
        }
        value.extend_from_slice(&filler);
        value.extend_from_slice(b"\"]");
        spans.push(span(1, i as u64, Value::BytesValue(value)));
    }
    let fillers = request(spans);
    // Trace 2: one span of a few KiB that names the markers round and round.
    let markers: Vec<String> = (0..ROUNDS).flat_map(|_| (0..BLOCKS).map(marker)).collect();
    let small = format!("[{}]", markers.join(","));
    let target = request(vec![span(2, 1_000, str_val(&small))]);

    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        splitter: Some(Arc::new(OtlpSplitter::default())),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(tmp.path(), 1, 0, 900_000_000_000, options).unwrap();
    for req in [&fillers, &target] {
        for b in decoder.decode(req).unwrap() {
            w.write(&b).unwrap();
        }
    }
    drop(fillers);
    let meta = w.finish().unwrap();
    // 40 markers and 40 fillers, the small span shares the markers.
    assert_eq!(meta.payload.pieces, 2 * BLOCKS as u64);

    let reader = PartReader::open(tmp.path().join(nwdb_core::part_dir_name(1))).unwrap();
    let before = reader.payload_blocks_loaded();
    let got = reader.read_trace(TraceId([2; 16]), None).unwrap().unwrap();
    let loads = reader.payload_blocks_loaded() - before;
    // The value itself comes back right.
    let want = decoder.decode(&target).unwrap();
    assert_eq!(all_rows(&got), all_rows(&want));
    eprintln!(
        "one span of {} bytes with {} references: {loads} block decompressions of 1 MiB, \
         {BLOCKS} distinct blocks",
        small.len(),
        markers.len()
    );
    assert!(
        loads <= BLOCKS as u64,
        "reading one span of {} bytes decompressed {loads} blocks of 1 MiB; it touches only \
         {BLOCKS} distinct blocks",
        small.len()
    );
}
