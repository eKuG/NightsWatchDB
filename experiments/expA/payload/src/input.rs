//! Reads the framed OTLP input (4-byte big-endian length + ExportTraceServiceRequest) span by span.
use crate::util::*;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Calls `f` for every span in file order. Returns the number of input bytes read.
/// Memory is one request (a few MB) at a time.
pub fn for_each_span(path: &Path, mut f: impl FnMut(&SpanRec) -> R<()>) -> R<u64> {
    let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
    let (mut buf, mut total) = (vec![], 0u64);
    while !r.fill_buf()?.is_empty() {
        let mut h = [0u8; 4];
        r.read_exact(&mut h)?;
        let n = u32::from_be_bytes(h) as usize;
        buf.resize(n, 0);
        r.read_exact(&mut buf)?;
        total += 4 + n as u64;
        let req = ExportTraceServiceRequest::decode(&buf[..])?;
        for sp in req.resource_spans.iter().flat_map(|r| r.scope_spans.iter().flat_map(|s| s.spans.iter())) {
            let mut vals: [Option<&[u8]>; 6] = [None; 6];
            for kv in &sp.attributes {
                if let Some(i) = ATTR_KEYS.iter().position(|k| *k == kv.key) {
                    if let Some(Value::StringValue(s)) = kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                        vals[i] = Some(s.as_bytes());
                    }
                }
            }
            let rec = SpanRec {
                trace: sp.trace_id.as_slice().try_into().map_err(|_| "trace id is not 16 bytes")?,
                span: sp.span_id.as_slice().try_into().map_err(|_| "span id is not 8 bytes")?,
                start_ns: sp.start_time_unix_nano,
                vals,
            };
            f(&rec)?;
        }
    }
    Ok(total)
}
