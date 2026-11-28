//! Helpers for the annotation part tests.
#![allow(dead_code)]

use nwdb_core::{PartMeta, SpanId, TraceId, part_dir_name};
use nwdb_segment::{ANNOTATION_FILE, Annotation, AnnotationReader, AnnotationWriter};
use rand::rngs::StdRng;
use rand::{Rng, RngCore};
use std::path::{Path, PathBuf};

pub const WINDOW_START: i64 = 1_700_000_000_000_000_000;
pub const WINDOW_NS: i64 = 900_000_000_000;

/// A trace id whose big-endian value is `n`.
pub fn trace(n: u128) -> TraceId {
    TraceId(n.to_be_bytes())
}

pub fn span(n: u64) -> SpanId {
    SpanId(n.to_be_bytes())
}

/// A score of a whole trace, 8-byte name, no source.
pub fn score(trace_n: u128, arrival: u64) -> Annotation {
    Annotation {
        trace_id: trace(trace_n),
        span_id: None,
        name: "accuracy".into(),
        source: String::new(),
        time_ns: WINDOW_START + trace_n as i64,
        value: Some(0.25),
        label: None,
        comment: None,
        delete: false,
        arrival,
    }
}

/// A random printable string of exactly `n` bytes.
pub fn text(rng: &mut StdRng, n: usize) -> String {
    (0..n)
        .map(|_| char::from(b' ' + rng.random_range(0..95u8)))
        .collect()
}

/// The largest record: every field at its limit.
pub fn largest(rng: &mut StdRng, trace_n: u128, span_n: u64, arrival: u64) -> Annotation {
    Annotation {
        trace_id: trace(trace_n),
        span_id: Some(span(span_n)),
        name: text(rng, 128),
        source: text(rng, 128),
        time_ns: WINDOW_START + rng.random_range(0..WINDOW_NS),
        value: Some(rng.random_range(-1e9..1e9)),
        label: Some(text(rng, 256)),
        comment: Some(text(rng, 4096)),
        delete: false,
        arrival,
    }
}

/// Uncompressed size of the record in a block.
pub fn record_len(a: &Annotation) -> usize {
    16 + 1
        + if a.span_id.is_some() { 8 } else { 0 }
        + 8
        + 8
        + 1
        + a.name.len()
        + 1
        + a.source.len()
        + if a.value.is_some() { 8 } else { 0 }
        + a.label.as_ref().map_or(0, |l| 2 + l.len())
        + a.comment.as_ref().map_or(0, |c| 2 + c.len())
}

/// The block each record lands in: a block is closed once it holds 16 KiB.
pub fn model_blocks(records: &[Annotation]) -> Vec<usize> {
    let (mut block, mut size) = (0, 0);
    let mut out = Vec::new();
    for a in records {
        out.push(block);
        size += record_len(a);
        if size >= 16 * 1024 {
            block += 1;
            size = 0;
        }
    }
    out
}

pub fn write_part(parts: &Path, id: u64, records: &[Annotation]) -> PartMeta {
    write_part_in(parts, id, WINDOW_START, WINDOW_NS, 0, records)
}

pub fn write_part_in(
    parts: &Path,
    id: u64,
    window_start: i64,
    window_ns: i64,
    level: u8,
    records: &[Annotation],
) -> PartMeta {
    let mut w = AnnotationWriter::create(parts, id, window_start, window_ns).unwrap();
    for a in records {
        w.append(a).unwrap();
    }
    w.finish(level).unwrap()
}

pub fn part_dir(parts: &Path, id: u64) -> PathBuf {
    parts.join(part_dir_name(id))
}

pub fn file_of(parts: &Path, id: u64) -> PathBuf {
    part_dir(parts, id).join(ANNOTATION_FILE)
}

pub fn open(parts: &Path, meta: &PartMeta) -> AnnotationReader {
    AnnotationReader::open(part_dir(parts, meta.part_id), meta).unwrap()
}

/// Every record of a cursor, or of one trace.
pub fn read(r: &AnnotationReader, t: Option<TraceId>) -> Vec<Annotation> {
    let mut c = r.cursor(t);
    let mut out = Vec::new();
    while let Some(a) = c.next().unwrap() {
        out.push(a);
    }
    out
}

/// `n` records in identity order: three per trace (the trace, then two spans
/// of it), traces `1..`, comments of `comment_len` random bytes.
pub fn sorted_records(rng: &mut StdRng, n: usize, comment_len: usize) -> Vec<Annotation> {
    (0..n)
        .map(|i| Annotation {
            trace_id: trace(i as u128 / 3 + 1),
            span_id: if i % 3 == 0 {
                None
            } else {
                Some(span(i as u64 % 3))
            },
            name: "n".into(),
            source: "s".into(),
            time_ns: WINDOW_START + rng.random_range(0..WINDOW_NS),
            value: Some(rng.random_range(0.0..1.0)),
            label: if i % 5 == 0 {
                Some("good".into())
            } else {
                None
            },
            comment: if comment_len > 0 {
                Some(text(rng, comment_len))
            } else {
                None
            },
            delete: false,
            arrival: i as u64 * 7 + 1,
        })
        .collect()
}

pub fn random_bytes(rng: &mut StdRng, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    rng.fill_bytes(&mut v);
    v
}
