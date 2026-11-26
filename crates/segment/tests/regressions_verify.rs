//! `verify` re-reads a part's files to find damage on disk that reads would
//! not notice, for compaction and a scrubber. A part it passes must still be a
//! part that `open` accepts: otherwise a scrubber reports the part sound and
//! the next start-up refuses it.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::{INDEX_FILE, PIECES_IDX, PartReader, PartWriter, WriterOptions};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fs::OpenOptions;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// A part of one trace with two spans of 2 KiB of random `attrs`, cut into
/// 512-byte pieces.
fn write_part(parts: &Path) -> PathBuf {
    let mut rng = StdRng::seed_from_u64(5);
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    let values: Vec<Vec<u8>> = (0..2)
        .map(|_| {
            let mut v = vec![0u8; 2048];
            rng.fill_bytes(&mut v);
            v
        })
        .collect();
    for (i, v) in values.iter().enumerate() {
        b.append(&SpanRecord {
            trace_id: TraceId([1; 16]),
            span_id: SpanId([i as u8 + 1; 8]),
            name: "s",
            attrs: v,
            ..SpanRecord::default()
        });
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

/// Flips one bit of the byte at `at` of `path`, in place.
fn flip(path: &Path, at: u64) {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0u8];
    f.read_exact_at(&mut byte, at).unwrap();
    byte[0] ^= 0x10;
    f.write_all_at(&byte, at).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn verify_finds_damage_in_the_last_four_bytes_of_pieces_idx() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path());
    let reader = PartReader::open(&dir).unwrap();
    assert!(reader.payload_totals().pieces > 0);
    reader.verify().unwrap();

    // One bit of the file's own trailing checksum rots on disk while the part
    // is open.
    let idx = dir.join(PIECES_IDX);
    let len = std::fs::metadata(&idx).unwrap().len();
    flip(&idx, len - 1);

    // The part can no longer be opened...
    match PartReader::open(&dir) {
        Err(Error::Corrupt(m)) => assert!(m.contains(PIECES_IDX), "{m}"),
        Err(e) => panic!("expected Corrupt, got {e:?}"),
        Ok(_) => panic!("a damaged pieces.idx was opened"),
    }
    // ...so the scrub that re-reads the files must not pass it.
    match reader.verify() {
        Err(Error::Corrupt(m)) => assert!(m.contains(PIECES_IDX), "{m}"),
        Err(e) => panic!("expected Corrupt naming {PIECES_IDX}, got {e:?}"),
        Ok(()) => panic!(
            "verify passed a part whose {PIECES_IDX} is damaged on disk and which open now refuses"
        ),
    }
}

#[test]
fn verify_finds_damage_in_traces_idx() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path());
    let reader = PartReader::open(&dir).unwrap();
    reader.verify().unwrap();

    // One bit of the recorded checksum of spans.parquet rots on disk while the
    // part is open.
    flip(&dir.join(INDEX_FILE), 24);

    match PartReader::open(&dir) {
        Err(Error::Corrupt(m)) => assert!(m.contains(INDEX_FILE), "{m}"),
        Err(e) => panic!("expected Corrupt, got {e:?}"),
        Ok(_) => panic!("a damaged traces.idx was opened"),
    }
    match reader.verify() {
        Err(Error::Corrupt(m)) => assert!(m.contains(INDEX_FILE), "{m}"),
        Err(e) => panic!("expected Corrupt naming {INDEX_FILE}, got {e:?}"),
        Ok(()) => panic!(
            "verify passed a part whose {INDEX_FILE} is damaged on disk and which open now refuses"
        ),
    }
}
