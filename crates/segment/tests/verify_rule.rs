//! The rule `verify` keeps: a part that passes it opens. For every file of a
//! part, one flipped bit at an offset, while a reader is open: either `verify`
//! on that reader fails with `Corrupt`, or a fresh `open` succeeds and its own
//! `verify` passes. A scrub that calls a part sound and a start-up that then
//! refuses it is the combination this keeps out.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::{PartReader, PartWriter, WriterOptions};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod support;
use support::flip;

struct Chunks(usize);

impl PayloadSplitter for Chunks {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut at = 0;
        while at < value.len() {
            let end = (at + self.0).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

/// A part of `traces` traces with two spans each, every span with `len` random
/// bytes of `attrs`, `events` and `links`, cut into 512-byte pieces.
fn write_part(parts: &Path, traces: u8, len: usize) -> PathBuf {
    let mut rng = StdRng::seed_from_u64(5);
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for t in 0..traces {
        for s in 0..2u8 {
            let mut v = vec![0u8; len];
            rng.fill_bytes(&mut v);
            b.append(&SpanRecord {
                trace_id: TraceId([t + 1; 16]),
                span_id: SpanId([s + 1; 8]),
                name: "s",
                attrs: &v,
                events: Some(&v[len / 2..]),
                links: Some(&v[..len / 2]),
                ..SpanRecord::default()
            });
        }
    }
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(512))),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(parts, 1, 0, 900_000_000_000, options).unwrap();
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(1))
}

/// Checks the rule for one flip at `at` of `file`.
fn check(dir: &Path, reader: &PartReader, file: &str, at: u64) {
    let path = dir.join(file);
    let bit = (at % 8) as u8;
    flip(&path, at, bit);
    match reader.verify() {
        Err(Error::Corrupt(_)) => {}
        Err(e) => panic!("{file} byte {at}: verify failed with {e:?}, not Corrupt"),
        Ok(()) => match PartReader::open(dir) {
            Ok(fresh) => fresh
                .verify()
                .unwrap_or_else(|e| panic!("{file} byte {at}: a fresh open passes, verify: {e}")),
            Err(e) => panic!("{file} byte {at}: verify passed a part that open refuses: {e}"),
        },
    }
    flip(&path, at, bit);
}

/// Every offset of a file of up to `all` bytes; of a longer one, its first and
/// last 64 bytes and `sample` others.
fn offsets(len: u64, all: u64, sample: usize, rng: &mut StdRng) -> Vec<u64> {
    if len <= all {
        return (0..len).collect();
    }
    let mut at: Vec<u64> = (0..64).chain(len - 64..len).collect();
    at.extend((0..sample).map(|_| rng.random_range(0..len)));
    at
}

fn rule_holds(dir: &Path, all: u64, sample: usize) {
    let reader = PartReader::open(dir).unwrap();
    reader.verify().unwrap();
    let mut rng = StdRng::seed_from_u64(9);
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap().file_name().into_string().unwrap())
        .collect();
    files.sort();
    assert!(files.len() >= 5, "{files:?}");
    for file in &files {
        let len = std::fs::metadata(dir.join(file)).unwrap().len();
        for at in offsets(len, all, sample, &mut rng) {
            check(dir, &reader, file, at);
        }
    }
    reader.verify().unwrap();
}

#[test]
fn a_part_that_passes_verify_opens_at_every_offset_of_a_small_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path(), 1, 1024);
    rule_holds(&dir, u64::MAX, 0);
}

#[test]
fn a_part_that_passes_verify_opens_at_sampled_offsets_of_a_larger_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path(), 60, 4096);
    rule_holds(&dir, 0, 150);
}
