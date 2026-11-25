//! A rope that names pieces is only as good as the store it is read against.
//! Parquet pages carry no checksum and a read does not run `verify`, so a
//! damaged or doctored reference, or a store that is not the one the part was
//! written with, must never produce text that was not written: the rope carries
//! the checksum of its value, the reader sizes every value against the store
//! before it allocates, and `verify` walks every rope.
//!
//! The tests doctor a part by replacing its piece files and patching the
//! lengths and checksums that `traces.idx` records, so that the part opens.
//! Those that could allocate without bound run the read again in a child
//! process (this test binary, filtered to the one test) whose address space is
//! capped with `ulimit -v`, so that the allocation fails there, quickly,
//! instead of taking the test runner down.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanColumns, SpanId,
    SpanRecord, TraceId, part_dir_name,
};
use nwdb_payload::{PieceReader, PieceWriter, PieceWriterOptions, StoreSummary};
use nwdb_segment::{INDEX_FILE, PIECES_BLK, PIECES_IDX, PartReader, PartWriter, WriterOptions};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

/// Set in the child: the part directory to read.
const CHILD: &str = "NWDB_ROPE_INTEGRITY_PART";

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

/// Writes a part of one trace whose spans have the `attrs` given, cut into
/// chunks of `chunk` bytes (and every chunk a piece, however short).
fn write_part(parts: &Path, attrs: &[Vec<u8>], chunk: usize) -> PathBuf {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for (i, a) in attrs.iter().enumerate() {
        b.append(&SpanRecord {
            trace_id: TraceId([1; 16]),
            span_id: SpanId([i as u8 + 1; 8]),
            name: "s",
            attrs: a,
            ..SpanRecord::default()
        });
    }
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(chunk))),
        min_piece_bytes: 1,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(parts, 1, 0, 900_000_000_000, options).unwrap();
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(1))
}

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// Puts the piece files written in `store` into `part` and patches the
/// lengths and checksums `traces.idx` records for them, and its own checksum.
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

fn attrs_of(reader: &PartReader) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for batch in reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap() {
        let cols = SpanColumns::try_new(&batch).unwrap();
        out.extend((0..cols.len()).map(|i| cols.record(i).attrs.to_vec()));
    }
    out
}

#[test]
fn a_store_of_another_text_with_the_same_shape_is_never_read_as_this_parts_text() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![random(1, 2048), random(2, 2048)];
    let part = write_part(tmp.path(), &attrs, 512);

    // A store with as many pieces of the same lengths, referenced as often, so
    // that every length, every total and every reference is consistent; only
    // the text differs.
    let scratch = tempfile::tempdir().unwrap();
    let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
    for i in 0..8 {
        w.add(&random(100 + i, 512)).unwrap();
    }
    let sum = w.finish().unwrap();
    install(&part, scratch.path(), &sum);

    // The part opens: what open can check, it has. Nothing it returns is
    // text that was not written.
    let reader = PartReader::open(&part).unwrap();
    let m = corrupt(reader.read_trace(TraceId([1; 16]), None).unwrap_err());
    assert!(m.contains("checksum"), "{m}");
    let m = corrupt(reader.verify().unwrap_err());
    assert!(m.contains("checksum"), "{m}");
    let scan = reader.scan(Default::default()).unwrap();
    assert!(scan.into_iter().any(|b| b.is_err()));
}

#[test]
fn a_store_with_fewer_pieces_is_an_error_not_other_text() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![random(1, 2048), random(2, 2048)];
    let part = write_part(tmp.path(), &attrs, 512);
    let original = PartReader::open(&part).unwrap();
    assert_eq!(attrs_of(&original), attrs);

    // The first four pieces of the part's own store, and no more.
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
    let m = corrupt(reader.read_trace(TraceId([1; 16]), None).unwrap_err());
    assert!(m.contains("piece"), "{m}");
    corrupt(reader.verify().unwrap_err());
}

#[test]
fn verify_compares_the_references_with_what_the_store_records() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![random(1, 2048), random(2, 2048)];
    let part = write_part(tmp.path(), &attrs, 512);
    let original = PartReader::open(&part).unwrap();
    original.verify().unwrap();

    // The same pieces, and one more reference than the ropes make: a part
    // whose ropes read back right but do not add up to the store's record.
    let store = original.piece_store();
    let scratch = tempfile::tempdir().unwrap();
    let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
    let mut session = store.session();
    let mut first = Vec::new();
    for id in 0..8 {
        let mut piece = Vec::new();
        store.read_piece(&mut session, id, &mut piece).unwrap();
        w.add(&piece).unwrap();
        if id == 0 {
            first = piece;
        }
    }
    w.find(&first).expect("the piece is known");
    let sum = w.finish().unwrap();
    drop(original);
    install(&part, scratch.path(), &sum);

    let reader = PartReader::open(&part).unwrap();
    // Every value still rebuilds to what was written...
    assert_eq!(attrs_of(&reader), attrs);
    // ...but the part is not what its store says it is, and `verify` says so.
    let m = corrupt(reader.verify().unwrap_err());
    assert!(m.contains(PIECES_IDX) && m.contains("refer to"), "{m}");
}

/// Runs `test` of this binary again in a process whose address space is
/// capped near 3 GB, reading the part in `part`.
fn run_capped(test: &str, part: &Path) {
    let out = Command::new("sh")
        .args(["-c", "ulimit -v 3000000 || exit 97; exec \"$0\" \"$@\""])
        .arg(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, part)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "the read, in a process capped at about 3 GB of address space, failed ({}):\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_value_of_a_size_that_cannot_be_allocated_is_an_error_not_an_abort() {
    const NAME: &str = "a_value_of_a_size_that_cannot_be_allocated_is_an_error_not_an_abort";
    const REFS: usize = 1 << 14;
    const PIECE: usize = 256 << 10;
    if let Some(dir) = std::env::var_os(CHILD) {
        let reader = PartReader::open(Path::new(&dir)).unwrap();
        let m = corrupt(reader.read_trace(TraceId([1; 16]), None).unwrap_err());
        assert!(m.contains("cannot be allocated"), "{m}");
        // `verify` rebuilds every value too, so it cannot pass either.
        corrupt(reader.verify().unwrap_err());
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    // 16,384 one-byte pieces' worth of references to the same piece.
    let part = write_part(tmp.path(), &[vec![b'a'; REFS]], 1);

    // A store whose piece 0 is 256 KiB and that records one reference to it
    // for every reference of the rope: the rope stands for 4 GiB, which is no
    // more than the whole part refers to, so no check on the totals refuses it
    // and only the allocation can.
    let scratch = tempfile::tempdir().unwrap();
    let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
    let big = random(7, PIECE);
    w.add(&big).unwrap();
    for _ in 1..REFS {
        w.find(&big).expect("the piece is known");
    }
    let sum = w.finish().unwrap();
    install(&part, scratch.path(), &sum);
    let store = PieceReader::open(&part).unwrap();
    assert_eq!(store.totals().logical_bytes, (REFS * PIECE) as u64);
    drop(store);
    run_capped(NAME, &part);
}
