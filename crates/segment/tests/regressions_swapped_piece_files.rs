//! The piece files of a part are bound to the part by what `traces.idx`
//! records for them, checked when the part is opened: their lengths and the
//! CRC-32C of `pieces.idx` (which is read in full anyway and holds the checksum
//! of every block). Two parts whose piece files have the same lengths can have
//! them exchanged, and no read may then return the other part's text.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanColumns, SpanId,
    SpanRecord, TraceId, part_dir_name,
};
use nwdb_segment::{PIECES_BLK, PIECES_IDX, PartReader, PartWriter, WriterOptions};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::Path;
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

/// Writes a part of one trace with two spans whose `attrs` are 2 KiB of
/// random bytes, cut into 512-byte pieces, and returns the values.
fn write(parts: &Path, id: u64, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let values: Vec<Vec<u8>> = (0..2)
        .map(|_| {
            let mut v = vec![0u8; 2048];
            rng.fill_bytes(&mut v);
            v
        })
        .collect();
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
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
    let mut w = PartWriter::create(parts, id, 0, 900_000_000_000, options).unwrap();
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    values
}

#[test]
fn piece_files_of_another_part_are_never_read_as_this_parts_text() {
    let tmp = tempfile::tempdir().unwrap();
    let mine = write(tmp.path(), 1, 1);
    let theirs = write(tmp.path(), 2, 2);
    let a = tmp.path().join(part_dir_name(1));
    let b = tmp.path().join(part_dir_name(2));
    for f in [PIECES_BLK, PIECES_IDX] {
        // Same lengths, different bytes: only a checksum tells them apart.
        let (x, y) = (
            std::fs::read(a.join(f)).unwrap(),
            std::fs::read(b.join(f)).unwrap(),
        );
        assert_eq!(x.len(), y.len(), "{f}: the setup needs equal lengths");
        assert_ne!(x, y, "{f}");
        std::fs::copy(b.join(f), a.join(f)).unwrap();
    }

    let reader = match PartReader::open(&a) {
        // What should happen: the recorded CRC-32C of pieces.idx does not
        // match, and pieces.idx holds the CRC of every block.
        Err(Error::Corrupt(_)) => return,
        Err(e) => panic!("expected Corrupt, got {e:?}"),
        Ok(r) => r,
    };
    match reader.read_trace(TraceId([1; 16]), None) {
        Err(Error::Corrupt(_)) => {}
        Err(e) => panic!("expected Corrupt, got {e:?}"),
        Ok(batches) => {
            let batches = batches.unwrap();
            let mut got = Vec::new();
            for batch in &batches {
                let cols = SpanColumns::try_new(batch).unwrap();
                for i in 0..cols.len() {
                    got.push(cols.record(i).attrs.to_vec());
                }
            }
            assert!(
                got == mine,
                "part 1 read back other bytes than it was given, with no error; they are \
                 part 2's text: {}",
                got == theirs
            );
        }
    }
}
