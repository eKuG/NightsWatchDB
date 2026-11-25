//! Direct protobuf wire walker for ExportTraceServiceRequest. Reads only the fields the
//! `spans`/`payloads` schema needs and skips everything else by length. Strings are
//! validated as UTF-8 (as prost does) and handed to the sink as slices of the request
//! buffer; the sink copies each exactly once into the Arrow builders.
use crate::convert::{classify, Slot, SpanRow};

type Er = &'static str;

struct Cur<'a>(&'a [u8]);

impl<'a> Cur<'a> {
    fn varint(&mut self) -> Result<u64, Er> {
        let (mut v, mut shift) = (0u64, 0u32);
        for (i, &b) in self.0.iter().enumerate().take(10) {
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                self.0 = &self.0[i + 1..];
                return Ok(v);
            }
            shift += 7;
        }
        Err("bad varint")
    }
    /// Next (field number, wire type).
    fn field(&mut self) -> Result<(u32, u8), Er> {
        let t = self.varint()?;
        if t >> 3 == 0 || t >> 3 > u32::MAX as u64 {
            return Err("bad field number");
        }
        Ok(((t >> 3) as u32, (t & 7) as u8))
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Er> {
        if n > self.0.len() {
            return Err("truncated");
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn ld(&mut self) -> Result<&'a [u8], Er> {
        let n = self.varint()?;
        self.take(usize::try_from(n).map_err(|_| "length")?)
    }
    fn fixed64(&mut self) -> Result<u64, Er> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn skip(&mut self, wt: u8) -> Result<(), Er> {
        match wt {
            0 => self.varint().map(drop),
            1 => self.take(8).map(drop),
            2 => self.ld().map(drop),
            5 => self.take(4).map(drop),
            _ => Err("unsupported wire type"),
        }
    }
    /// Length-delimited field of the expected wire type, or an error like prost's.
    fn ld_at(&mut self, wt: u8) -> Result<&'a [u8], Er> {
        if wt != 2 {
            return Err("wrong wire type");
        }
        self.ld()
    }
}

fn utf8(b: &[u8]) -> Result<&str, Er> {
    std::str::from_utf8(b).map_err(|_| "invalid utf-8")
}

#[derive(Clone, Copy)]
enum Val<'a> {
    Str(&'a str),
    Int(i64),
    Other,
}

/// AnyValue: the last oneof member on the wire wins (unknown members leave it alone).
fn any_value(b: &[u8]) -> Result<Option<Val<'_>>, Er> {
    let mut c = Cur(b);
    let mut v = None;
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        match (f, w) {
            (1, 2) => v = Some(Val::Str(utf8(c.ld()?)?)),
            (3, 0) => v = Some(Val::Int(c.varint()? as i64)),
            (1 | 3, _) => return Err("wrong wire type"),
            (2 | 4..=8, _) => {
                c.skip(w)?;
                v = Some(Val::Other);
            }
            _ => c.skip(w)?,
        }
    }
    Ok(v)
}

fn key_value(b: &[u8]) -> Result<(&str, Option<Val<'_>>), Er> {
    let mut c = Cur(b);
    let (mut k, mut v) = ("", None);
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        match f {
            1 => k = utf8(c.ld_at(w)?)?,
            2 => v = any_value(c.ld_at(w)?)?,
            _ => c.skip(w)?,
        }
    }
    Ok((k, v))
}

/// First `service.name` attribute of a Resource (string value or None), as the full path does.
fn service_name(b: &[u8]) -> Result<Option<&str>, Er> {
    let mut c = Cur(b);
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        if f == 1 {
            let (k, v) = key_value(c.ld_at(w)?)?;
            if k == "service.name" {
                return Ok(match v {
                    Some(Val::Str(s)) => Some(s),
                    _ => None,
                });
            }
        } else {
            c.skip(w)?;
        }
    }
    Ok(None)
}

fn status_code(b: &[u8]) -> Result<i32, Er> {
    let mut c = Cur(b);
    let mut code = 0;
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        if (f, w) == (3, 0) {
            code = c.varint()? as i32;
        } else {
            c.skip(w)?;
        }
    }
    Ok(code)
}

fn span<'a>(b: &'a [u8], svc: Option<&'a str>) -> Result<SpanRow<'a>, Er> {
    let mut r = SpanRow { service: svc, ..Default::default() };
    let mut c = Cur(b);
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        match (f, w) {
            (1, 2) => r.trace_id = c.ld()?,
            (2, 2) => r.span_id = c.ld()?,
            (4, 2) => r.parent = c.ld()?,
            (5, 2) => r.name = utf8(c.ld()?)?,
            (6, 0) => r.kind = c.varint()? as i32,
            (7, 1) => r.start = c.fixed64()? as i64,
            (8, 1) => r.end = c.fixed64()? as i64,
            (15, 2) => r.status = status_code(c.ld()?)?,
            (9, 2) => {
                let (k, v) = key_value(c.ld()?)?;
                match classify(k) {
                    Some(Slot::Txt(i)) => r.txt[i] = if let Some(Val::Str(s)) = v { Some(s) } else { None },
                    Some(Slot::Int(i)) => r.ints[i] = if let Some(Val::Int(n)) = v { Some(n) } else { None },
                    Some(Slot::Pay(i)) => r.pay[i] = if let Some(Val::Str(s)) = v { Some(s.as_bytes()) } else { None },
                    None => r.other += 1,
                }
            }
            (1 | 2 | 4 | 5 | 6 | 7 | 8 | 9 | 15, _) => return Err("wrong wire type"),
            _ => c.skip(w)?,
        }
    }
    Ok(r)
}

fn scope_spans<'a>(b: &'a [u8], svc: Option<&'a str>, sink: &mut impl FnMut(&SpanRow) -> Result<(), String>) -> Result<(), String> {
    let mut c = Cur(b);
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        if f == 2 {
            sink(&span(c.ld_at(w)?, svc)?)?;
        } else {
            c.skip(w)?;
        }
    }
    Ok(())
}

/// Walks a serialized ExportTraceServiceRequest and calls `sink` once per span, in order.
pub fn walk_traces(req: &[u8], mut sink: impl FnMut(&SpanRow) -> Result<(), String>) -> Result<(), String> {
    let mut c = Cur(req);
    while !c.0.is_empty() {
        let (f, w) = c.field()?;
        if f != 1 {
            c.skip(w)?;
            continue;
        }
        let rs = c.ld_at(w)?;
        // The resource may follow the scope spans on the wire, so find it first.
        let (mut svc, mut r) = (None, Cur(rs));
        while !r.0.is_empty() {
            let (f, w) = r.field()?;
            if f == 1 {
                svc = service_name(r.ld_at(w)?)?;
            } else {
                r.skip(w)?;
            }
        }
        let mut r = Cur(rs);
        while !r.0.is_empty() {
            let (f, w) = r.field()?;
            if f == 2 {
                scope_spans(r.ld_at(w)?, svc, &mut sink)?;
            } else {
                r.skip(w)?;
            }
        }
    }
    Ok(())
}
