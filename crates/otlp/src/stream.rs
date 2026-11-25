//! Streaming JSON writers for the raw columns.
//!
//! Each function reads the stored bytes once and writes JSON text straight to
//! the writer through a small fixed buffer, so the memory needed does not
//! depend on the size of the column; only the recursion (at most
//! [`MAX_DEPTH`] values deep) grows with the input. They cannot fail because
//! of what the bytes contain: whatever cannot be read as the message it should
//! be is written as `{"$unparsed": "<base64 of the raw bytes>"}` (under the
//! key `"$unparsed"` when it is an entry of an object), and the walk goes on
//! with the next sibling when the framing allows it. The only error is the
//! writer's own.
//!
//! The mapping of values is the one documented on the
//! [`json`](crate::json) module, with these differences from the older
//! functions there:
//!
//! - Strings and keys that are not valid UTF-8 are written with each invalid
//!   sequence replaced by U+FFFD.
//! - Keys are written in stored order and are not de-duplicated, so a key the
//!   sender repeated appears twice. A JSON parser keeps the last occurrence,
//!   which is the rule the promoted columns use. De-duplicating would need
//!   memory proportional to the number of keys.
//! - When a `KeyValue` carries its `value` field twice, the last occurrence is
//!   shown; protobuf would merge them.

use crate::json::MAX_DEPTH;
use crate::walk::{Next, Rd, Val};
use std::io::{self, Write};

const BUF: usize = 8 * 1024;

/// A buffer in front of the caller's writer. The JSON is written in many
/// small pieces and the caller's writer may be a socket.
struct Sink<'a> {
    out: &'a mut dyn Write,
    buf: Vec<u8>,
}

impl<'a> Sink<'a> {
    fn new(out: &'a mut dyn Write) -> Self {
        Self {
            out,
            buf: Vec::with_capacity(BUF),
        }
    }

    fn put(&mut self, b: &[u8]) -> io::Result<()> {
        if self.buf.len() + b.len() > BUF {
            self.drain()?;
        }
        if b.len() >= BUF {
            self.out.write_all(b)
        } else {
            self.buf.extend_from_slice(b);
            Ok(())
        }
    }

    fn drain(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.out.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

impl Write for Sink<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.put(b)?;
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.drain()
    }
}

fn run(out: &mut dyn Write, f: impl FnOnce(&mut Sink<'_>) -> io::Result<()>) -> io::Result<()> {
    let mut s = Sink::new(out);
    f(&mut s)?;
    s.drain()
}

/// The `attrs` column (length-prefixed `KeyValue` messages) as a JSON object.
pub fn write_attrs(out: &mut dyn Write, column: &[u8]) -> io::Result<()> {
    run(out, |s| {
        s.put(b"{")?;
        let mut first = true;
        let mut r = Rd::new(column);
        loop {
            match r.prefixed() {
                Next::End => break,
                Next::Bad => {
                    unparsed_entry(s, &mut first, r.unread())?;
                    break;
                }
                Next::Item(m) => key_value(s, &mut first, m, 0)?,
            }
        }
        s.put(b"}")
    })
}

/// The `resource` column (a `Resource` message) as a JSON object of its
/// attributes. See [`resource_dropped_attributes`] for its dropped count.
pub fn write_resource(out: &mut dyn Write, resource: &[u8]) -> io::Result<()> {
    run(out, |s| object_of(s, resource, 1, 0))
}

/// The `scope` column (an `InstrumentationScope` message) as a JSON object of
/// its attributes. See [`scope_dropped_attributes`] for its dropped count.
pub fn write_scope_attrs(out: &mut dyn Write, scope: &[u8]) -> io::Result<()> {
    run(out, |s| object_of(s, scope, 3, 0))
}

/// The `events` column as a JSON array of
/// `{time_ns, name, attributes, dropped_attributes_count}`; the last is left
/// out when zero.
pub fn write_events(out: &mut dyn Write, column: &[u8]) -> io::Result<()> {
    run(out, |s| array_of(s, column, event))
}

/// The `links` column as a JSON array of
/// `{trace_id, span_id, trace_state, flags, attributes,
/// dropped_attributes_count}`, the ids as lower-case hex; `trace_state`,
/// `flags` and the dropped count are left out when empty or zero.
pub fn write_links(out: &mut dyn Write, column: &[u8]) -> io::Result<()> {
    run(out, |s| array_of(s, column, link))
}

/// The `dropped_attributes_count` of a `Resource` message: 0 when it is absent
/// or the message cannot be read.
pub fn resource_dropped_attributes(resource: &[u8]) -> u32 {
    dropped_of(resource, 2)
}

/// The `dropped_attributes_count` of an `InstrumentationScope` message: 0 when
/// it is absent or the message cannot be read.
pub fn scope_dropped_attributes(scope: &[u8]) -> u32 {
    dropped_of(scope, 4)
}

fn dropped_of(msg: &[u8], field: u32) -> u32 {
    let mut n = 0;
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => return n,
            Next::Bad => return 0,
            Next::Item((f, Val::Varint(v))) if f == field => n = v as u32,
            Next::Item((f, _)) if f == field => return 0,
            Next::Item(_) => {}
        }
    }
}

/// `[` the elements of a column of length-prefixed messages `]`.
fn array_of(
    s: &mut Sink<'_>,
    column: &[u8],
    element: fn(&mut Sink<'_>, &[u8]) -> io::Result<()>,
) -> io::Result<()> {
    s.put(b"[")?;
    let mut first = true;
    let mut r = Rd::new(column);
    loop {
        match r.prefixed() {
            Next::End => break,
            Next::Bad => {
                comma(s, &mut first)?;
                unparsed(s, r.unread())?;
                break;
            }
            Next::Item(m) => {
                comma(s, &mut first)?;
                element(s, m)?;
            }
        }
    }
    s.put(b"]")
}

/// `{` an entry for each `KeyValue` in field `num` of `msg` `}`.
fn object_of(s: &mut Sink<'_>, msg: &[u8], num: u32, depth: usize) -> io::Result<()> {
    s.put(b"{")?;
    let mut first = true;
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => break,
            Next::Item((n, Val::Len(m))) if n == num => key_value(s, &mut first, m, depth)?,
            Next::Item((n, _)) if n != num => {}
            // A bad field, or the wrong wire type for the attribute field:
            // the rest cannot be told apart from what is wrong with it.
            Next::Bad | Next::Item(_) => {
                unparsed_entry(s, &mut first, r.unread())?;
                break;
            }
        }
    }
    s.put(b"}")
}

fn comma(s: &mut Sink<'_>, first: &mut bool) -> io::Result<()> {
    if !std::mem::replace(first, false) {
        s.put(b",")?;
    }
    Ok(())
}

fn unparsed(s: &mut Sink<'_>, raw: &[u8]) -> io::Result<()> {
    s.put(b"{")?;
    unparsed_member(s, raw)?;
    s.put(b"}")
}

fn unparsed_entry(s: &mut Sink<'_>, first: &mut bool, raw: &[u8]) -> io::Result<()> {
    comma(s, first)?;
    unparsed_member(s, raw)
}

fn unparsed_member(s: &mut Sink<'_>, raw: &[u8]) -> io::Result<()> {
    s.put(b"\"$unparsed\":\"")?;
    base64(s, raw)?;
    s.put(b"\"")
}

/// One `KeyValue` as an entry of the object being written.
fn key_value(s: &mut Sink<'_>, first: &mut bool, msg: &[u8], depth: usize) -> io::Result<()> {
    let (mut key, mut value): (&[u8], Option<&[u8]>) = (&[], None);
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => break,
            Next::Item((1, Val::Len(b))) => key = b,
            Next::Item((2, Val::Len(b))) => value = Some(b),
            Next::Bad | Next::Item((1 | 2, _)) => return unparsed_entry(s, first, msg),
            Next::Item(_) => {}
        }
    }
    comma(s, first)?;
    string(s, key)?;
    s.put(b":")?;
    match value {
        Some(v) => any_value(s, v, depth),
        None => s.put(b"null"),
    }
}

enum Scalar<'a> {
    Null,
    Str(&'a [u8]),
    Bool(bool),
    Int(i64),
    Double(f64),
    Array(&'a [u8]),
    List(&'a [u8]),
    Bytes(&'a [u8]),
}

fn any_value(s: &mut Sink<'_>, msg: &[u8], depth: usize) -> io::Result<()> {
    if depth >= MAX_DEPTH {
        return unparsed(s, msg);
    }
    // The fields form a oneof: the last one on the wire wins.
    let mut v = Scalar::Null;
    let mut r = Rd::new(msg);
    loop {
        v = match r.field() {
            Next::End => break,
            Next::Bad => return unparsed(s, msg),
            Next::Item((1, Val::Len(b))) => Scalar::Str(b),
            Next::Item((2, Val::Varint(x))) => Scalar::Bool(x != 0),
            Next::Item((3, Val::Varint(x))) => Scalar::Int(x as i64),
            Next::Item((4, Val::Fixed64(x))) => Scalar::Double(f64::from_bits(x)),
            Next::Item((5, Val::Len(b))) => Scalar::Array(b),
            Next::Item((6, Val::Len(b))) => Scalar::List(b),
            Next::Item((7, Val::Len(b))) => Scalar::Bytes(b),
            Next::Item((1..=7, _)) => return unparsed(s, msg),
            Next::Item(_) => continue,
        };
    }
    match v {
        Scalar::Null => s.put(b"null"),
        Scalar::Str(b) => string(s, b),
        Scalar::Bool(b) => s.put(if b { b"true" } else { b"false" }),
        Scalar::Int(i) => write!(s, "{i}"),
        Scalar::Double(d) => double(s, d),
        Scalar::Array(b) => array(s, b, depth),
        Scalar::List(b) => object_of(s, b, 1, depth + 1),
        Scalar::Bytes(b) => {
            s.put(b"\"")?;
            base64(s, b)?;
            s.put(b"\"")
        }
    }
}

fn array(s: &mut Sink<'_>, msg: &[u8], depth: usize) -> io::Result<()> {
    s.put(b"[")?;
    let mut first = true;
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => break,
            Next::Item((1, Val::Len(m))) => {
                comma(s, &mut first)?;
                any_value(s, m, depth + 1)?;
            }
            Next::Bad | Next::Item((1, _)) => {
                comma(s, &mut first)?;
                unparsed(s, r.unread())?;
                break;
            }
            Next::Item(_) => {}
        }
    }
    s.put(b"]")
}

fn double(s: &mut Sink<'_>, d: f64) -> io::Result<()> {
    match serde_json::Number::from_f64(d) {
        Some(n) => write!(s, "{n}"),
        None if d.is_nan() => s.put(b"\"NaN\""),
        None if d > 0.0 => s.put(b"\"Infinity\""),
        None => s.put(b"\"-Infinity\""),
    }
}

fn event(s: &mut Sink<'_>, msg: &[u8]) -> io::Result<()> {
    let (mut time, mut name, mut dropped) = (0u64, &[][..], 0u32);
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => break,
            Next::Item((1, Val::Fixed64(x))) => time = x,
            Next::Item((2, Val::Len(b))) => name = b,
            Next::Item((3, Val::Len(_))) => {}
            Next::Item((4, Val::Varint(x))) => dropped = x as u32,
            Next::Bad | Next::Item((1..=4, _)) => return unparsed(s, msg),
            Next::Item(_) => {}
        }
    }
    write!(s, "{{\"time_ns\":{time},\"name\":")?;
    string(s, name)?;
    s.put(b",\"attributes\":")?;
    object_of(s, msg, 3, 0)?;
    if dropped != 0 {
        write!(s, ",\"dropped_attributes_count\":{dropped}")?;
    }
    s.put(b"}")
}

fn link(s: &mut Sink<'_>, msg: &[u8]) -> io::Result<()> {
    let (mut trace_id, mut span_id, mut state) = (&[][..], &[][..], &[][..]);
    let (mut dropped, mut flags) = (0u32, 0u32);
    let mut r = Rd::new(msg);
    loop {
        match r.field() {
            Next::End => break,
            Next::Item((1, Val::Len(b))) => trace_id = b,
            Next::Item((2, Val::Len(b))) => span_id = b,
            Next::Item((3, Val::Len(b))) => state = b,
            Next::Item((4, Val::Len(_))) => {}
            Next::Item((5, Val::Varint(x))) => dropped = x as u32,
            Next::Item((6, Val::Fixed32(x))) => flags = x,
            Next::Bad | Next::Item((1..=6, _)) => return unparsed(s, msg),
            Next::Item(_) => {}
        }
    }
    s.put(b"{\"trace_id\":\"")?;
    hex(s, trace_id)?;
    s.put(b"\",\"span_id\":\"")?;
    hex(s, span_id)?;
    s.put(b"\"")?;
    if !state.is_empty() {
        s.put(b",\"trace_state\":")?;
        string(s, state)?;
    }
    if flags != 0 {
        write!(s, ",\"flags\":{flags}")?;
    }
    s.put(b",\"attributes\":")?;
    object_of(s, msg, 4, 0)?;
    if dropped != 0 {
        write!(s, ",\"dropped_attributes_count\":{dropped}")?;
    }
    s.put(b"}")
}

fn hex(s: &mut Sink<'_>, bytes: &[u8]) -> io::Result<()> {
    const D: &[u8; 16] = b"0123456789abcdef";
    for chunk in bytes.chunks(128) {
        let mut tmp = [0u8; 256];
        for (i, b) in chunk.iter().enumerate() {
            tmp[2 * i] = D[usize::from(b >> 4)];
            tmp[2 * i + 1] = D[usize::from(b & 15)];
        }
        s.put(&tmp[..2 * chunk.len()])?;
    }
    Ok(())
}

/// A JSON string. Each invalid UTF-8 sequence becomes U+FFFD.
fn string(s: &mut Sink<'_>, bytes: &[u8]) -> io::Result<()> {
    s.put(b"\"")?;
    for chunk in bytes.utf8_chunks() {
        let valid = chunk.valid().as_bytes();
        let mut start = 0;
        for (i, &c) in valid.iter().enumerate() {
            if c >= 0x20 && c != b'"' && c != b'\\' {
                continue;
            }
            s.put(&valid[start..i])?;
            start = i + 1;
            match c {
                b'"' => s.put(b"\\\"")?,
                b'\\' => s.put(b"\\\\")?,
                b'\n' => s.put(b"\\n")?,
                b'\r' => s.put(b"\\r")?,
                b'\t' => s.put(b"\\t")?,
                _ => write!(s, "\\u{c:04x}")?,
            }
        }
        s.put(&valid[start..])?;
        if !chunk.invalid().is_empty() {
            s.put("\u{fffd}".as_bytes())?;
        }
    }
    s.put(b"\"")
}

/// Standard base64 with padding.
fn base64(s: &mut Sink<'_>, bytes: &[u8]) -> io::Result<()> {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    // 192 input bytes fill 256 output bytes.
    for block in bytes.chunks(192) {
        let mut tmp = [0u8; 256];
        let mut n = 0;
        for group in block.chunks(3) {
            let v =
                group.iter().fold(0u32, |v, b| v << 8 | u32::from(*b)) << (8 * (3 - group.len()));
            for i in 0..4 {
                tmp[n] = if i <= group.len() {
                    A[(v >> (18 - 6 * i) & 63) as usize]
                } else {
                    b'='
                };
                n += 1;
            }
        }
        s.put(&tmp[..n])?;
    }
    Ok(())
}
