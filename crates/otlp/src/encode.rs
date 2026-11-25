use crate::wire::{Cur, put_varint};
use arrow_array::RecordBatch;
use nwdb_core::{Error, Result, SpanColumns, SpanRecord};

fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, u64::from(field) << 3 | u64::from(wire));
}

fn put_len_field(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// Writes each element of a column of length-prefixed messages as its own
/// repeated field.
fn put_repeated(out: &mut Vec<u8>, field: u32, column: &[u8], what: &str) -> Result<()> {
    let mut c = Cur::new(column);
    // Each element is a varint length followed by that many bytes. The reader
    // is the same bounds-checked one the decoder uses.
    while !c.is_empty() {
        let msg = c.len_delimited().map_err(|_| {
            Error::corrupt(format!("span table column `{what}` is not length-prefixed"))
        })?;
        put_len_field(out, field, msg);
    }
    Ok(())
}

fn put_varint_field(out: &mut Vec<u8>, field: u32, v: u64) {
    if v != 0 {
        put_tag(out, field, 0);
        put_varint(out, v);
    }
}

fn put_str_field(out: &mut Vec<u8>, field: u32, v: Option<&str>) {
    if let Some(s) = v.filter(|s| !s.is_empty()) {
        put_len_field(out, field, s.as_bytes());
    }
}

fn encode_span(out: &mut Vec<u8>, r: &SpanRecord<'_>) -> Result<()> {
    put_len_field(out, 1, r.trace_id.as_bytes());
    put_len_field(out, 2, r.span_id.as_bytes());
    put_str_field(out, 3, r.trace_state);
    if let Some(p) = r.parent_span_id {
        put_len_field(out, 4, p.as_bytes());
    }
    put_str_field(out, 5, Some(r.name));
    put_varint_field(out, 6, r.kind as u32 as u64);
    if r.start_ns != 0 {
        put_tag(out, 7, 1);
        out.extend_from_slice(&(r.start_ns as u64).to_le_bytes());
    }
    if r.end_ns != 0 {
        put_tag(out, 8, 1);
        out.extend_from_slice(&(r.end_ns as u64).to_le_bytes());
    }
    put_repeated(out, 9, r.attrs, "attrs")?;
    put_varint_field(out, 10, u64::from(r.dropped_attributes_count));
    put_repeated(out, 11, r.events.unwrap_or(&[]), "events")?;
    put_varint_field(out, 12, u64::from(r.dropped_events_count));
    put_repeated(out, 13, r.links.unwrap_or(&[]), "links")?;
    put_varint_field(out, 14, u64::from(r.dropped_links_count));
    let message = r.status_message.filter(|s| !s.is_empty());
    if message.is_some() || r.status_code != 0 {
        let mut status = Vec::new();
        put_str_field(&mut status, 2, message);
        put_varint_field(&mut status, 3, r.status_code as u32 as u64);
        put_len_field(out, 15, &status);
    }
    if r.flags != 0 {
        put_tag(out, 16, 5);
        out.extend_from_slice(&r.flags.to_le_bytes());
    }
    Ok(())
}

/// The scope of a row: what [`encode_spans`] groups on besides the resource.
fn scope_of<'a>(r: &SpanRecord<'a>) -> (Option<&'a str>, Option<&'a str>) {
    (
        r.scope_name.filter(|s| !s.is_empty()),
        r.scope_version.filter(|s| !s.is_empty()),
    )
}

/// Serializes span-table batches as an OTLP `ExportTraceServiceRequest`,
/// which is also a valid `TracesData` since the two share their wire form.
///
/// Consecutive rows with the same `resource` bytes go into one
/// `ResourceSpans`, and consecutive rows of those with the same scope name and
/// version into one `ScopeSpans`, so the output keeps the row order. The
/// `resource`, `attrs`, `events` and `links` bytes are written as stored. The
/// promoted columns and `service_name` are not written: they are copies of
/// data that is already in those bytes. Instrumentation scope attributes are
/// not stored in the table and are not written. Decoding the result gives
/// back the same rows.
///
/// Returns [`Error::InvalidArgument`] for a batch that does not have the span
/// schema and [`Error::Corrupt`] for a raw column that is not length-prefixed.
pub fn encode_spans(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut resource: Option<OpenResource> = None;
    let mut span_buf = Vec::new();
    for batch in batches {
        let cols = SpanColumns::try_new(batch)?;
        for row in 0..cols.len() {
            let r = cols.record(row);
            if !resource
                .as_ref()
                .is_some_and(|o| o.bytes.as_deref() == r.resource)
            {
                if let Some(done) = resource.take() {
                    done.close(&mut out);
                }
                resource = Some(OpenResource::new(r.resource));
            }
            span_buf.clear();
            encode_span(&mut span_buf, &r)?;
            if let Some(open) = resource.as_mut() {
                open.push_span(scope_of(&r), &span_buf);
            }
        }
    }
    if let Some(done) = resource {
        done.close(&mut out);
    }
    Ok(out)
}

/// A scope's name and version, as owned strings.
type ScopeKey = (Option<String>, Option<String>);

/// A `ScopeSpans` being filled. A message's length goes before its content,
/// so the children are collected first.
struct OpenScope {
    key: ScopeKey,
    spans: Vec<u8>,
}

impl OpenScope {
    fn close(self, into: &mut Vec<u8>) {
        let mut body = Vec::new();
        let (name, version) = &self.key;
        if name.is_some() || version.is_some() {
            let mut scope = Vec::new();
            put_str_field(&mut scope, 1, name.as_deref());
            put_str_field(&mut scope, 2, version.as_deref());
            put_len_field(&mut body, 1, &scope);
        }
        body.extend_from_slice(&self.spans);
        put_len_field(into, 2, &body);
    }
}

/// A `ResourceSpans` being filled.
struct OpenResource {
    bytes: Option<Vec<u8>>,
    scopes: Vec<u8>,
    scope: Option<OpenScope>,
}

impl OpenResource {
    fn new(bytes: Option<&[u8]>) -> Self {
        Self {
            bytes: bytes.map(<[u8]>::to_vec),
            scopes: Vec::new(),
            scope: None,
        }
    }

    fn push_span(&mut self, (name, version): (Option<&str>, Option<&str>), span: &[u8]) {
        let same = self
            .scope
            .as_ref()
            .is_some_and(|s| s.key.0.as_deref() == name && s.key.1.as_deref() == version);
        if !same {
            if let Some(done) = self.scope.take() {
                done.close(&mut self.scopes);
            }
            self.scope = Some(OpenScope {
                key: (name.map(str::to_owned), version.map(str::to_owned)),
                spans: Vec::new(),
            });
        }
        if let Some(open) = self.scope.as_mut() {
            put_len_field(&mut open.spans, 2, span);
        }
    }

    fn close(mut self, out: &mut Vec<u8>) {
        if let Some(done) = self.scope.take() {
            done.close(&mut self.scopes);
        }
        let mut body = Vec::new();
        if let Some(r) = &self.bytes {
            put_len_field(&mut body, 1, r);
        }
        body.extend_from_slice(&self.scopes);
        put_len_field(out, 1, &body);
    }
}
