//! `otapdoor expect`: reads a generator file (4-byte big-endian length, then one serialized OTLP
//! ExportTraceServiceRequest) and prints the rows the receiver must produce, one per span, in the format of
//! `serve --dump-rows`. Sort both outputs and diff them to compare individual values.
use crate::trace::{row_line, Row};
use crate::{Args, R};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, KeyValue};
use prost::Message;
use std::io::{BufReader, BufWriter, Read, Write};

fn strval(kv: &KeyValue) -> Option<&str> {
    match kv.value.as_ref()?.value.as_ref()? {
        Value::StringValue(s) => Some(s),
        _ => None,
    }
}

pub fn run(a: &Args) -> R<()> {
    let mut r = BufReader::new(std::fs::File::open(a.get("in").ok_or("--in is required")?)?);
    let mut w = BufWriter::new(std::fs::File::create(a.get("out").ok_or("--out is required")?)?);
    let mut h = [0u8; 4];
    let mut buf = vec![];
    let mut n = 0u64;
    while r.read_exact(&mut h).is_ok() {
        buf.resize(u32::from_be_bytes(h) as usize, 0);
        r.read_exact(&mut buf)?;
        let req = ExportTraceServiceRequest::decode(buf.as_slice())?;
        for rs in &req.resource_spans {
            let svc = rs.resource.as_ref().and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name")).and_then(strval);
            for sp in rs.scope_spans.iter().flat_map(|s| &s.spans) {
                let (mut t, mut iv, mut pv, mut other) = ([None::<&str>; 9], [None::<i64>; 2], [None::<&str>; 6], 0i32);
                for kv in &sp.attributes {
                    let k = crate::trace::classify(&kv.key);
                    match k {
                        0..=8 => t[k as usize] = strval(kv),
                        9..=10 => {
                            iv[k as usize - 9] = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                                Some(Value::IntValue(i)) => Some(*i),
                                _ => None,
                            }
                        }
                        11..=16 => pv[k as usize - 11] = strval(kv),
                        _ => other += 1,
                    }
                }
                let row = Row {
                    trace: &sp.trace_id,
                    span: &sp.span_id,
                    parent: (!sp.parent_span_id.is_empty()).then_some(&sp.parent_span_id[..]),
                    name: &sp.name,
                    kind: sp.kind,
                    start: sp.start_time_unix_nano as i64,
                    end: sp.end_time_unix_nano as i64,
                    status: sp.status.as_ref().map_or(0, |s| s.code),
                    svc,
                    txt: t,
                    ints: iv,
                    other,
                    pay: pv,
                };
                w.write_all(row_line(&row).as_bytes())?;
                n += 1;
            }
        }
    }
    w.flush()?;
    eprintln!("expect: {n} spans");
    Ok(())
}
