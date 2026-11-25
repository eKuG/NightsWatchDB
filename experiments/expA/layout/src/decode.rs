//! OTLP records -> `spans` Arrow batches, streamed in input order.
use crate::R;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, unbounded};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, KeyValue};
use prost::Message;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

// spans text columns, in the order of the slots below
const TXT_COLS: [&str; 9] = ["operation", "provider", "request_model", "conversation_id", "session_id", "user_id", "agent_name", "tool_name", "error_type"];

enum Slot {
    Txt(usize),
    Int(usize),
    Payload,
}

fn classify(k: &str) -> Option<Slot> {
    Some(match k {
        "gen_ai.operation.name" => Slot::Txt(0),
        "gen_ai.provider.name" => Slot::Txt(1),
        "gen_ai.request.model" => Slot::Txt(2),
        "gen_ai.conversation.id" => Slot::Txt(3),
        "session.id" => Slot::Txt(4),
        "user.id" => Slot::Txt(5),
        "gen_ai.agent.name" => Slot::Txt(6),
        "gen_ai.tool.name" => Slot::Txt(7),
        "error.type" => Slot::Txt(8),
        "gen_ai.usage.input_tokens" => Slot::Int(0),
        "gen_ai.usage.output_tokens" => Slot::Int(1),
        // listed in the payloads table, so not counted in other_attr_count
        "gen_ai.input.messages" | "gen_ai.output.messages" | "gen_ai.system_instructions" | "gen_ai.tool.definitions" | "gen_ai.tool.call.arguments" | "gen_ai.tool.call.result" => Slot::Payload,
        _ => return None,
    })
}

pub fn spans_schema() -> SchemaRef {
    let f = Field::new;
    let utf = |n| Field::new(n, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        f("trace_id", DataType::FixedSizeBinary(16), false),
        f("span_id", DataType::FixedSizeBinary(8), false),
        f("parent_span_id", DataType::FixedSizeBinary(8), true),
        f("name", DataType::Utf8, false),
        f("kind", DataType::Int32, false),
        f("start_ns", DataType::Int64, false),
        f("end_ns", DataType::Int64, false),
        f("status_code", DataType::Int32, false),
        utf("service_name"),
        utf(TXT_COLS[0]),
        utf(TXT_COLS[1]),
        utf(TXT_COLS[2]),
        f("input_tokens", DataType::Int64, true),
        f("output_tokens", DataType::Int64, true),
        utf(TXT_COLS[3]),
        utf(TXT_COLS[4]),
        utf(TXT_COLS[5]),
        utf(TXT_COLS[6]),
        utf(TXT_COLS[7]),
        utf(TXT_COLS[8]),
        f("other_attr_count", DataType::Int32, false),
    ]))
}

fn strval(kv: &KeyValue) -> Option<&str> {
    match kv.value.as_ref()?.value.as_ref()? {
        Value::StringValue(s) => Some(s),
        _ => None,
    }
}

fn intval(kv: &KeyValue) -> Option<i64> {
    match kv.value.as_ref()?.value.as_ref()? {
        Value::IntValue(i) => Some(*i),
        _ => None,
    }
}

/// Decodes one framed request into one batch.
pub fn decode(buf: &[u8], schema: &SchemaRef) -> R<RecordBatch> {
    let req = ExportTraceServiceRequest::decode(buf)?;
    let spans = || req.resource_spans.iter().flat_map(|r| r.scope_spans.iter().flat_map(move |s| s.spans.iter().map(move |sp| (r, sp))));
    let n = spans().count();
    let mut trace = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut parent = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut name = StringBuilder::with_capacity(n, n * 32);
    let (mut kind, mut status, mut other) = (Int32Builder::with_capacity(n), Int32Builder::with_capacity(n), Int32Builder::with_capacity(n));
    let (mut start, mut end) = (Int64Builder::with_capacity(n), Int64Builder::with_capacity(n));
    let mut service = StringBuilder::with_capacity(n, n * 16);
    let mut txt: [StringBuilder; 9] = std::array::from_fn(|_| StringBuilder::with_capacity(n, n * 16));
    let mut ints: [Int64Builder; 2] = std::array::from_fn(|_| Int64Builder::with_capacity(n));
    for (rs, sp) in spans() {
        let svc = rs.resource.as_ref().and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name")).and_then(strval);
        let (mut t, mut iv, mut oth) = ([None::<&str>; 9], [None::<i64>; 2], 0i32);
        for kv in &sp.attributes {
            match classify(&kv.key) {
                Some(Slot::Txt(i)) => t[i] = strval(kv),
                Some(Slot::Int(i)) => iv[i] = intval(kv),
                Some(Slot::Payload) => {}
                None => oth += 1,
            }
        }
        trace.append_value(&sp.trace_id)?;
        span.append_value(&sp.span_id)?;
        if sp.parent_span_id.is_empty() {
            parent.append_null();
        } else {
            parent.append_value(&sp.parent_span_id)?;
        }
        name.append_value(&sp.name);
        kind.append_value(sp.kind);
        start.append_value(sp.start_time_unix_nano as i64);
        end.append_value(sp.end_time_unix_nano as i64);
        status.append_value(sp.status.as_ref().map_or(0, |s| s.code));
        service.append_option(svc);
        for i in 0..9 {
            txt[i].append_option(t[i]);
        }
        for i in 0..2 {
            ints[i].append_option(iv[i]);
        }
        other.append_value(oth);
    }
    let [op, prov, model, conv, sess, user, agent, tool, err] = txt.map(|mut b| Arc::new(b.finish()) as ArrayRef);
    let [it, ot] = ints.map(|mut b| Arc::new(b.finish()) as ArrayRef);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(trace.finish()),
        Arc::new(span.finish()),
        Arc::new(parent.finish()),
        Arc::new(name.finish()),
        Arc::new(kind.finish()),
        Arc::new(start.finish()),
        Arc::new(end.finish()),
        Arc::new(status.finish()),
        Arc::new(service.finish()),
        op,
        prov,
        model,
        it,
        ot,
        conv,
        sess,
        user,
        agent,
        tool,
        err,
        Arc::new(other.finish()),
    ];
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// Reads the input with one reader thread and `threads` decode workers and hands batches to
/// `sink` in input order on the calling thread. Returns the input bytes read. If `sink` fails
/// the pipeline stops and the error is returned.
pub fn stream(input: &Path, threads: usize, mut sink: impl FnMut(RecordBatch) -> R<()>) -> R<u64> {
    let schema = spans_schema();
    let (in_tx, in_rx) = bounded::<(u64, Vec<u8>)>(threads * 2);
    let (out_tx, out_rx) = bounded::<(u64, RecordBatch)>(threads * 2);
    let (pool_tx, pool_rx) = unbounded::<Vec<u8>>();
    std::thread::scope(|s| -> R<u64> {
        let reader = s.spawn(move || -> R<u64> {
            let mut r = BufReader::with_capacity(1 << 20, File::open(input)?);
            let (mut seq, mut total) = (0u64, 0u64);
            while !r.fill_buf()?.is_empty() {
                let mut h = [0u8; 4];
                r.read_exact(&mut h)?;
                let n = u32::from_be_bytes(h) as usize;
                let mut buf = pool_rx.try_recv().unwrap_or_default();
                buf.resize(n, 0);
                r.read_exact(&mut buf)?;
                total += 4 + n as u64;
                if in_tx.send((seq, buf)).is_err() {
                    break;
                }
                seq += 1;
            }
            Ok(total)
        });
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let (rx, tx, pool, schema) = (in_rx.clone(), out_tx.clone(), pool_tx.clone(), schema.clone());
                s.spawn(move || -> R<()> {
                    for (seq, buf) in rx {
                        let b = decode(&buf, &schema)?;
                        let _ = pool.send(buf);
                        if tx.send((seq, b)).is_err() {
                            break;
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        drop((in_rx, out_tx, pool_tx));
        // reorder: workers finish out of order, the sink sees input order
        let (mut held, mut next) = (BTreeMap::new(), 0u64);
        let mut res = Ok(());
        'outer: for (seq, b) in out_rx {
            held.insert(seq, b);
            while let Some(b) = held.remove(&next) {
                next += 1;
                if let Err(e) = sink(b) {
                    res = Err(e);
                    break 'outer;
                }
            }
        }
        // out_rx is dropped here when the loop ends, which unblocks the workers and the reader
        for w in workers {
            w.join().map_err(|_| "decode worker panicked")??;
        }
        let total = reader.join().map_err(|_| "reader panicked")??;
        res?;
        Ok(total)
    })
}
