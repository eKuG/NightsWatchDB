use crate::repair::{Kind, is_clean, repair};
use crate::walk::{Next, Rd};
use crate::wire::put_varint;
use arrow_array::RecordBatch;
use nwdb_core::{Result, SpanColumns, SpanRecord};

fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, u64::from(field) << 3 | u64::from(wire));
}

fn put_len_field(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// Writes each element of a column of length-prefixed messages as its own
/// repeated field, and returns how many it had to leave out.
///
/// An element that a standard parser would read is written as stored. One it
/// would refuse is repaired, or left out when it cannot be, and the rest of a
/// column whose framing is broken counts as one left out.
fn put_repeated(out: &mut Vec<u8>, field: u32, column: &[u8], kind: Kind) -> u64 {
    let mut dropped = 0;
    let mut r = Rd::new(column);
    loop {
        match r.prefixed() {
            Next::End => return dropped,
            Next::Bad => return dropped + 1,
            Next::Item(m) if is_clean(m, kind) => put_len_field(out, field, m),
            Next::Item(m) => match repair(m, kind) {
                Some(fixed) => put_len_field(out, field, &fixed),
                None => dropped += 1,
            },
        }
    }
}

/// Writes a `Resource` or `InstrumentationScope` as field `field`: as stored
/// when a standard parser reads it, repaired when it can be, and empty when
/// it cannot.
fn put_message(out: &mut Vec<u8>, field: u32, msg: &[u8], kind: Kind) {
    if is_clean(msg, kind) {
        put_len_field(out, field, msg);
    } else {
        put_len_field(out, field, &repair(msg, kind).unwrap_or_default());
    }
}

/// A count as a field, added to what the sender reported and capped at what
/// the field can hold.
fn put_count(out: &mut Vec<u8>, field: u32, sent: u32, left_out: u64) {
    let total = u64::from(sent).saturating_add(left_out);
    put_varint_field(out, field, total.min(u64::from(u32::MAX)));
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

fn encode_span(out: &mut Vec<u8>, r: &SpanRecord<'_>) {
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
    let attrs = put_repeated(out, 9, r.attrs, Kind::KeyValue);
    put_count(out, 10, r.dropped_attributes_count, attrs);
    let events = put_repeated(out, 11, r.events.unwrap_or(&[]), Kind::Event);
    put_count(out, 12, r.dropped_events_count, events);
    let links = put_repeated(out, 13, r.links.unwrap_or(&[]), Kind::Link);
    put_count(out, 14, r.dropped_links_count, links);
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
}

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

/// What [`encode_spans`] groups on for the resource level.
type ResourceKey<'a> = (Option<&'a [u8]>, Option<&'a str>);

fn resource_of<'a>(r: &SpanRecord<'a>) -> ResourceKey<'a> {
    (r.resource, non_empty(r.resource_schema_url))
}

/// What [`encode_spans`] groups on for the scope level: the `scope` bytes, the
/// name and version, and the schema URL. The name and version only count for a
/// row that has no `scope` bytes; with the bytes they are copies of them.
type ScopeKey<'a> = (
    Option<&'a [u8]>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

fn scope_of<'a>(r: &SpanRecord<'a>) -> ScopeKey<'a> {
    let (name, version) = match r.scope {
        Some(_) => (None, None),
        None => (non_empty(r.scope_name), non_empty(r.scope_version)),
    };
    (r.scope, name, version, non_empty(r.scope_schema_url))
}

/// Serializes span-table batches as an OTLP `ExportTraceServiceRequest`,
/// which is also a valid `TracesData` since the two share their wire form.
///
/// Consecutive rows with the same `resource` bytes and `resource_schema_url`
/// go into one `ResourceSpans`, and consecutive rows of those with the same
/// `scope` bytes and `scope_schema_url` into one `ScopeSpans`, so the output
/// keeps the row order. The `resource`, `scope`, `attrs`, `events` and `links`
/// bytes are written as stored. A row whose `scope` is null but which has a
/// scope name or version gets a scope rebuilt from those two. The promoted
/// columns, `service_name` and, when `scope` is present, the scope name and
/// version are not written: they are copies of data that is already in those
/// bytes. Decoding the result gives back the same rows.
///
/// The result is always a message a standard OTLP parser accepts, whatever the
/// stored bytes hold, because the decoder keeps attributes, events, links,
/// resource and scope as sent without reading them. A piece a parser would
/// read is written exactly as stored. Otherwise it is repaired: a string that
/// is not UTF-8 gets U+FFFD for each invalid sequence; a key-value, event or
/// link that cannot be read (or whose values nest more than 64 messages deep)
/// is left out and added to the `dropped_attributes_count`,
/// `dropped_events_count` or `dropped_links_count` of the message it was in;
/// a resource or scope that cannot be read at all is written empty.
///
/// Returns [`nwdb_core::Error::InvalidArgument`] for a batch that does not
/// have the span schema.
pub fn encode_spans(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut resource: Option<OpenResource> = None;
    let mut span_buf = Vec::new();
    for batch in batches {
        let cols = SpanColumns::try_new(batch)?;
        for row in 0..cols.len() {
            let r = cols.record(row);
            let key = resource_of(&r);
            if !resource.as_ref().is_some_and(|o| o.matches(key)) {
                if let Some(done) = resource.take() {
                    done.close(&mut out);
                }
                resource = Some(OpenResource::new(key));
            }
            span_buf.clear();
            encode_span(&mut span_buf, &r);
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

/// A [`ScopeKey`] with owned values.
struct OwnedScopeKey {
    scope: Option<Vec<u8>>,
    name: Option<String>,
    version: Option<String>,
    schema_url: Option<String>,
}

impl OwnedScopeKey {
    fn new((scope, name, version, schema_url): ScopeKey<'_>) -> Self {
        Self {
            scope: scope.map(<[u8]>::to_vec),
            name: name.map(str::to_owned),
            version: version.map(str::to_owned),
            schema_url: schema_url.map(str::to_owned),
        }
    }

    fn matches(&self, (scope, name, version, schema_url): ScopeKey<'_>) -> bool {
        self.scope.as_deref() == scope
            && self.name.as_deref() == name
            && self.version.as_deref() == version
            && self.schema_url.as_deref() == schema_url
    }
}

/// A `ScopeSpans` being filled. A message's length goes before its content,
/// so the children are collected first.
struct OpenScope {
    key: OwnedScopeKey,
    spans: Vec<u8>,
}

impl OpenScope {
    fn close(self, into: &mut Vec<u8>) {
        let mut body = Vec::new();
        let key = &self.key;
        if let Some(scope) = &key.scope {
            put_message(&mut body, 1, scope, Kind::Scope);
        } else if key.name.is_some() || key.version.is_some() {
            let mut scope = Vec::new();
            put_str_field(&mut scope, 1, key.name.as_deref());
            put_str_field(&mut scope, 2, key.version.as_deref());
            put_len_field(&mut body, 1, &scope);
        }
        body.extend_from_slice(&self.spans);
        put_str_field(&mut body, 3, key.schema_url.as_deref());
        put_len_field(into, 2, &body);
    }
}

/// A `ResourceSpans` being filled.
struct OpenResource {
    bytes: Option<Vec<u8>>,
    schema_url: Option<String>,
    scopes: Vec<u8>,
    scope: Option<OpenScope>,
}

impl OpenResource {
    fn new((bytes, schema_url): ResourceKey<'_>) -> Self {
        Self {
            bytes: bytes.map(<[u8]>::to_vec),
            schema_url: schema_url.map(str::to_owned),
            scopes: Vec::new(),
            scope: None,
        }
    }

    fn matches(&self, (bytes, schema_url): ResourceKey<'_>) -> bool {
        self.bytes.as_deref() == bytes && self.schema_url.as_deref() == schema_url
    }

    fn push_span(&mut self, key: ScopeKey<'_>, span: &[u8]) {
        if !self.scope.as_ref().is_some_and(|s| s.key.matches(key)) {
            if let Some(done) = self.scope.take() {
                done.close(&mut self.scopes);
            }
            self.scope = Some(OpenScope {
                key: OwnedScopeKey::new(key),
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
            put_message(&mut body, 1, r, Kind::Resource);
        }
        body.extend_from_slice(&self.scopes);
        put_str_field(&mut body, 3, self.schema_url.as_deref());
        put_len_field(out, 1, &body);
    }
}
