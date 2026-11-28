//! The encoding of annotation records and of a log record body.

use super::{Annotation, MAX_BATCH_ITEMS};
use nwdb_core::{Error, Result, SpanId, TraceId};

const HAS_SPAN: u8 = 1;
const DELETE: u8 = 2;
const HAS_VALUE: u8 = 4;
const HAS_LABEL: u8 = 8;
const HAS_COMMENT: u8 = 16;
const RESERVED: u8 = !(HAS_SPAN | DELETE | HAS_VALUE | HAS_LABEL | HAS_COMMENT);

/// Fewest bytes of one record: trace id, flags, time, two length bytes and a
/// one-byte name.
const MIN_RECORD_BYTES: usize = 16 + 1 + 8 + 1 + 1 + 1;

/// Appends one record. `with_arrival` is set for records inside part blocks.
/// The record must have passed `validate`.
pub(crate) fn encode_record(a: &Annotation, with_arrival: bool, out: &mut Vec<u8>) {
    out.extend_from_slice(a.trace_id.as_bytes());
    let mut flags = 0;
    if a.span_id.is_some() {
        flags |= HAS_SPAN;
    }
    if a.delete {
        flags |= DELETE;
    }
    if a.value.is_some() {
        flags |= HAS_VALUE;
    }
    if a.label.is_some() {
        flags |= HAS_LABEL;
    }
    if a.comment.is_some() {
        flags |= HAS_COMMENT;
    }
    out.push(flags);
    if let Some(s) = a.span_id {
        out.extend_from_slice(s.as_bytes());
    }
    if with_arrival {
        out.extend_from_slice(&a.arrival.to_le_bytes());
    }
    out.extend_from_slice(&a.time_ns.to_le_bytes());
    out.push(a.name.len() as u8);
    out.extend_from_slice(a.name.as_bytes());
    out.push(a.source.len() as u8);
    out.extend_from_slice(a.source.as_bytes());
    if let Some(v) = a.value {
        out.extend_from_slice(&v.to_bits().to_le_bytes());
    }
    if let Some(l) = &a.label {
        out.extend_from_slice(&(l.len() as u16).to_le_bytes());
        out.extend_from_slice(l.as_bytes());
    }
    if let Some(c) = &a.comment {
        out.extend_from_slice(&(c.len() as u16).to_le_bytes());
        out.extend_from_slice(c.as_bytes());
    }
}

/// Every read checks the length first.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(Error::corrupt("annotation record is truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    fn text(&mut self, n: usize) -> Result<String> {
        let bytes = self.take(n)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| Error::corrupt("annotation text is not valid UTF-8"))
    }
}

/// Reads one record from the front of `cur`, checking everything `validate`
/// checks. `arrival` is 0 unless `with_arrival`.
fn decode_record(cur: &mut Cursor<'_>, with_arrival: bool) -> Result<Annotation> {
    let trace_id = TraceId(cur.array()?);
    let flags = cur.array::<1>()?[0];
    if flags & RESERVED != 0 {
        return Err(Error::corrupt(format!(
            "annotation record has reserved flag bits set ({flags:#04x})"
        )));
    }
    let span_id = if flags & HAS_SPAN != 0 {
        Some(SpanId(cur.array()?))
    } else {
        None
    };
    let arrival = if with_arrival {
        u64::from_le_bytes(cur.array()?)
    } else {
        0
    };
    let time_ns = i64::from_le_bytes(cur.array()?);
    let n = usize::from(cur.array::<1>()?[0]);
    let name = cur.text(n)?;
    let n = usize::from(cur.array::<1>()?[0]);
    let source = cur.text(n)?;
    let value = if flags & HAS_VALUE != 0 {
        Some(f64::from_bits(u64::from_le_bytes(cur.array()?)))
    } else {
        None
    };
    let label = if flags & HAS_LABEL != 0 {
        let n = usize::from(u16::from_le_bytes(cur.array()?));
        Some(cur.text(n)?)
    } else {
        None
    };
    let comment = if flags & HAS_COMMENT != 0 {
        let n = usize::from(u16::from_le_bytes(cur.array()?));
        Some(cur.text(n)?)
    } else {
        None
    };
    let a = Annotation {
        trace_id,
        span_id,
        name,
        source,
        time_ns,
        value,
        label,
        comment,
        delete: flags & DELETE != 0,
        arrival,
    };
    a.validate()
        .map_err(|why| Error::corrupt(format!("annotation record is invalid: {why}")))?;
    Ok(a)
}

/// True if two items share an identity.
fn has_repeated_identity(items: &[Annotation]) -> bool {
    let mut sorted: Vec<&Annotation> = items.iter().collect();
    sorted.sort_by(|a, b| a.identity_cmp(b));
    sorted.windows(2).any(|w| w[0].identity_cmp(w[1]).is_eq())
}

/// Appends the body of a log record: the items in the order given.
///
/// Returns [`Error::InvalidArgument`], and appends nothing, for no items, more
/// than `MAX_BATCH_ITEMS`, an item `validate` refuses, or a repeated identity.
/// The `arrival` of the items is not written.
pub fn encode_batch(items: &[Annotation], out: &mut Vec<u8>) -> Result<()> {
    if items.is_empty() || items.len() > MAX_BATCH_ITEMS {
        return Err(Error::invalid_argument(format!(
            "a batch holds 1 to {MAX_BATCH_ITEMS} annotations, got {}",
            items.len()
        )));
    }
    for (i, a) in items.iter().enumerate() {
        a.validate()
            .map_err(|why| Error::invalid_argument(format!("annotation {i}: {why}")))?;
    }
    if has_repeated_identity(items) {
        return Err(Error::invalid_argument(
            "two annotations of a batch have the same trace, span, name and source",
        ));
    }
    out.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for a in items {
        encode_record(a, false, out);
    }
    Ok(())
}

/// Decodes a log record body and stamps `arrival` on every item.
///
/// [`Error::Corrupt`] for anything `encode_batch` would not have written.
pub fn decode_batch(body: &[u8], arrival: u64) -> Result<Vec<Annotation>> {
    let mut cur = Cursor(body);
    let count = usize::from(u16::from_le_bytes(cur.array()?));
    if count == 0 || count > MAX_BATCH_ITEMS {
        return Err(Error::corrupt(format!(
            "annotation batch has {count} items, expected 1 to {MAX_BATCH_ITEMS}"
        )));
    }
    // A count the remaining bytes cannot hold fails below; do not reserve it.
    let mut items = Vec::with_capacity(count.min(cur.0.len() / MIN_RECORD_BYTES));
    for _ in 0..count {
        let mut a = decode_record(&mut cur, false)?;
        a.arrival = arrival;
        items.push(a);
    }
    if !cur.0.is_empty() {
        return Err(Error::corrupt(format!(
            "annotation batch has {} trailing bytes",
            cur.0.len()
        )));
    }
    if has_repeated_identity(&items) {
        return Err(Error::corrupt("annotation batch repeats an identity"));
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_with_arrival_round_trips_and_is_eight_bytes_longer() {
        let a = Annotation {
            trace_id: TraceId([1; 16]),
            span_id: Some(SpanId([2; 8])),
            name: "n".into(),
            source: String::new(),
            time_ns: -5,
            value: Some(0.5),
            label: Some("l".into()),
            comment: None,
            delete: false,
            arrival: 77,
        };
        let (mut with, mut without) = (Vec::new(), Vec::new());
        encode_record(&a, true, &mut with);
        encode_record(&a, false, &mut without);
        assert_eq!(with.len(), without.len() + 8);
        let mut cur = Cursor(&with);
        assert_eq!(decode_record(&mut cur, true).unwrap(), a);
        assert!(cur.0.is_empty());
        let mut cur = Cursor(&without);
        let b = decode_record(&mut cur, false).unwrap();
        assert_eq!((b.arrival, b.name.as_str()), (0, "n"));
    }
}
