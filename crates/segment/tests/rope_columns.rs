//! The checks that rest on the rope's checksum and length must hold for each of
//! the three raw columns, not only for `attrs`: each test runs once per column
//! with the pieces in that column alone, so a check restricted to one column
//! would be noticed. Also: the length a rope records is the length of the value
//! it rebuilds to, in a part that was written and in one that was merged.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanColumns, SpanId,
    SpanRecord, TraceId, part_dir_name,
};
use nwdb_payload::rope;
use nwdb_payload::{PieceWriter, PieceWriterOptions, StoreSummary};
use nwdb_segment::{
    INDEX_FILE, MergeOptions, PIECES_BLK, PIECES_IDX, PartReader, PartWriter, WriterOptions,
    merge_parts,
};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;

#[derive(Clone, Copy, Debug)]
enum Column {
    Attrs,
    Events,
    Links,
}

const COLUMNS: [Column; 3] = [Column::Attrs, Column::Events, Column::Links];

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

fn options(chunk: usize) -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(Chunks(chunk))),
        min_piece_bytes: 1,
        ..WriterOptions::default()
    }
}

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// Writes part `id` of the traces `traces`, one span each, whose value in
/// `column` is the corresponding entry of `values` and which has nothing in the
/// other raw columns.
fn write_part(parts: &Path, id: u64, column: Column, traces: &[u8], values: &[Vec<u8>]) -> PathBuf {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for (t, v) in traces.iter().zip(values) {
        let mut record = SpanRecord {
            trace_id: TraceId([*t; 16]),
            span_id: SpanId([1; 8]),
            name: "s",
            ..SpanRecord::default()
        };
        match column {
            Column::Attrs => record.attrs = v,
            Column::Events => record.events = Some(v),
            Column::Links => record.links = Some(v),
        }
        b.append(&record);
    }
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options(512)).unwrap();
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(id))
}

/// The value of `column` of every row of the part, rebuilt.
fn values_of(reader: &PartReader, column: Column) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let rows = reader.row_count();
    for batch in reader.read_rows(0, rows, None).unwrap() {
        let cols = SpanColumns::try_new(&batch).unwrap();
        for i in 0..cols.len() {
            let r = cols.record(i);
            out.push(match column {
                Column::Attrs => r.attrs.to_vec(),
                Column::Events => r.events.unwrap_or_default().to_vec(),
                Column::Links => r.links.unwrap_or_default().to_vec(),
            });
        }
    }
    out
}

/// Puts the piece files written in `store` into `part` and patches the lengths
/// and checksums `traces.idx` records for them, and its own checksum.
fn install(part: &Path, store: &Path, sum: &StoreSummary) {
    for f in [PIECES_BLK, PIECES_IDX] {
        std::fs::copy(store.join(f), part.join(f)).unwrap();
    }
    let mut index = std::fs::read(part.join(INDEX_FILE)).unwrap();
    for (at, s) in [(40, sum.blocks), (52, sum.index)] {
        index[at..at + 8].copy_from_slice(&s.len.to_le_bytes());
        index[at + 8..at + 12].copy_from_slice(&s.crc32c.to_le_bytes());
    }
    let body = index.len() - 4;
    let crc = crc32c::crc32c(&index[..body]);
    index[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(part.join(INDEX_FILE), &index).unwrap();
}

fn corrupt(e: Error) -> String {
    match e {
        Error::Corrupt(m) => m,
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn a_store_of_another_text_is_never_read_as_this_parts_text_in_any_column() {
    for column in COLUMNS {
        let tmp = tempfile::tempdir().unwrap();
        let values = vec![random(1, 2048), random(2, 2048)];
        let part = write_part(tmp.path(), 1, column, &[1, 2], &values);
        assert_eq!(
            values_of(&PartReader::open(&part).unwrap(), column),
            values,
            "{column:?}"
        );

        // A store with as many pieces of the same lengths, referenced as often:
        // only the text differs.
        let scratch = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
        for i in 0..8 {
            w.add(&random(100 + i, 512)).unwrap();
        }
        let sum = w.finish().unwrap();
        install(&part, scratch.path(), &sum);

        let reader = PartReader::open(&part).unwrap();
        let rows = reader.row_count();
        let m = corrupt(reader.read_rows(0, rows, None).unwrap_err());
        assert!(m.contains("checksum"), "{column:?}: {m}");
        let m = corrupt(reader.verify().unwrap_err());
        assert!(m.contains("checksum"), "{column:?}: {m}");
    }
}

#[test]
fn a_store_with_fewer_pieces_is_an_error_not_other_text_in_any_column() {
    for column in COLUMNS {
        let tmp = tempfile::tempdir().unwrap();
        let values = vec![random(1, 2048), random(2, 2048)];
        let part = write_part(tmp.path(), 1, column, &[1, 2], &values);
        let original = PartReader::open(&part).unwrap();

        let store = original.piece_store();
        let scratch = tempfile::tempdir().unwrap();
        let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
        let mut session = store.session();
        for id in 0..4 {
            let mut piece = Vec::new();
            store.read_piece(&mut session, id, &mut piece).unwrap();
            w.add(&piece).unwrap();
        }
        let sum = w.finish().unwrap();
        drop(original);
        install(&part, scratch.path(), &sum);

        let reader = PartReader::open(&part).unwrap();
        let rows = reader.row_count();
        let m = corrupt(reader.read_rows(0, rows, None).unwrap_err());
        assert!(m.contains("piece"), "{column:?}: {m}");
        corrupt(reader.verify().unwrap_err());
    }
}

/// Every stored rope of `column` with references records the length of the
/// value it rebuilds to, and the segments add up to it.
fn assert_lengths_recorded(reader: &PartReader, column: Column, want: &[Vec<u8>]) {
    let rows = reader.row_count();
    let store = reader.piece_store();
    let mut seen = 0;
    let mut with_references = 0;
    for batch in reader.read_rows_stored(0, rows, None).unwrap() {
        let cols = SpanColumns::try_new(&batch).unwrap();
        for i in 0..cols.len() {
            let r = cols.record(i);
            let stored = match column {
                Column::Attrs => Some(r.attrs),
                Column::Events => r.events,
                Column::Links => r.links,
            }
            .unwrap_or_default();
            let len = want[seen].len() as u64;
            if rope::checksum(stored).is_some() {
                with_references += 1;
                assert_eq!(rope::recorded_len(stored).unwrap(), Some(len), "{column:?}");
            }
            let summed = rope::logical_len(stored, |id| store.piece_len(id)).unwrap();
            assert_eq!(summed, len, "{column:?}");
            seen += 1;
        }
    }
    assert_eq!(seen, want.len());
    assert!(
        with_references > 0,
        "{column:?}: the setup needs references"
    );
}

#[test]
fn a_written_and_a_merged_part_record_the_length_of_every_value_they_rebuild_to() {
    for column in COLUMNS {
        let tmp = tempfile::tempdir().unwrap();
        // Two parts that share a piece and each have text of their own.
        let shared = random(3, 1024);
        let a_values = vec![[shared.clone(), random(4, 700)].concat(), random(5, 2048)];
        let b_values = vec![[random(6, 90), shared.clone()].concat(), random(7, 1500)];
        let a = write_part(tmp.path(), 1, column, &[1, 2], &a_values);
        let b = write_part(tmp.path(), 2, column, &[3, 4], &b_values);
        let (a, b) = (PartReader::open(a).unwrap(), PartReader::open(b).unwrap());
        assert_lengths_recorded(&a, column, &a_values);
        assert_lengths_recorded(&b, column, &b_values);

        let merge_options = MergeOptions {
            writer: options(512),
            ..MergeOptions::default()
        };
        merge_parts(&[&a, &b], tmp.path(), 3, 0, WINDOW_NS, merge_options).unwrap();
        let merged = PartReader::open(tmp.path().join(part_dir_name(3))).unwrap();
        merged.verify().unwrap();
        let want: Vec<Vec<u8>> = a_values.into_iter().chain(b_values).collect();
        assert_eq!(values_of(&merged, column), want, "{column:?}");
        assert_lengths_recorded(&merged, column, &want);
    }
}
