//! Stored data that is damaged or doctored must give an error, never make a
//! reader allocate without bound.
//!
//! Each test builds the part in the test process and then runs the read again
//! in a child process (this test binary, filtered to the one test) whose
//! address space is capped with `ulimit -v`, so that a huge allocation fails
//! there, quickly and without touching memory, instead of taking the test
//! runner down.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_payload::{PieceWriter, PieceWriterOptions};
use nwdb_segment::{INDEX_FILE, PIECES_BLK, PIECES_IDX, PartReader, PartWriter, WriterOptions};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

/// Set in the child: the part directory to read.
const CHILD: &str = "NWDB_ROPE_BOUNDS_PART";

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

/// Writes a part of one span of trace 1 whose `attrs` is `attrs`, cut by
/// `splitter`, and returns its directory.
fn write_part(parts: &Path, attrs: &[u8], splitter: Chunks) -> PathBuf {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    b.append(&SpanRecord {
        trace_id: TraceId([1; 16]),
        span_id: SpanId([1; 8]),
        name: "s",
        attrs,
        ..SpanRecord::default()
    });
    // The one-byte cuts of the first test must be pieces: the default floor
    // would store them inline and the rope would have no references at all.
    let options = WriterOptions {
        splitter: Some(Arc::new(splitter)),
        min_piece_bytes: 1,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(parts, 1, 0, 900_000_000_000, options).unwrap();
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(1))
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
        "the read, in a process capped at about 3 GB of address space, did not end with \
         Error::Corrupt ({}):\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_rope_that_names_more_bytes_than_its_store_holds_is_refused_before_allocating() {
    const NAME: &str =
        "a_rope_that_names_more_bytes_than_its_store_holds_is_refused_before_allocating";
    if let Some(dir) = std::env::var_os(CHILD) {
        let reader = match PartReader::open(Path::new(&dir)) {
            Err(Error::Corrupt(_)) => return,
            other => other.unwrap(),
        };
        let got = reader.read_trace(TraceId([1; 16]), None);
        assert!(
            matches!(got, Err(Error::Corrupt(_))),
            "expected Corrupt, got {:?}",
            got.map(|_| ())
        );
        return;
    }

    const REFS: usize = 1 << 16;
    const PIECE: usize = 16 << 20;
    let tmp = tempfile::tempdir().unwrap();
    // A span whose attrs are 65,536 bytes `a`, each its own cut: the rope is
    // 65,536 references to piece 0, which is "a".
    let dir = write_part(tmp.path(), &vec![b'a'; REFS], Chunks(1));

    // A store whose piece 0 is 16 MiB, with traces.idx recording its files'
    // lengths and checksums: the same rope now stands for 1 TiB, while
    // pieces.idx says that all references of the part add up to 16 MiB.
    let scratch = tempfile::tempdir().unwrap();
    let mut pw = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
    let mut big = vec![0u8; PIECE];
    StdRng::seed_from_u64(1).fill_bytes(&mut big);
    assert_eq!(pw.add(&big).unwrap(), 0);
    let sum = pw.finish().unwrap();
    for f in [PIECES_BLK, PIECES_IDX] {
        std::fs::copy(scratch.path().join(f), dir.join(f)).unwrap();
    }
    let mut index = std::fs::read(dir.join(INDEX_FILE)).unwrap();
    for (at, s) in [(40, sum.blocks), (52, sum.index)] {
        index[at..at + 8].copy_from_slice(&s.len.to_le_bytes());
        index[at + 8..at + 12].copy_from_slice(&s.crc32c.to_le_bytes());
    }
    let body = index.len() - 4;
    let crc = crc32c::crc32c(&index[..body]);
    index[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(dir.join(INDEX_FILE), &index).unwrap();

    if let Ok(reader) = PartReader::open(&dir) {
        eprintln!(
            "doctored part: verify() = {:?}; pieces.idx logical bytes {}",
            reader.verify().map(|_| "ok"),
            reader.payload_totals().logical_bytes
        );
    }
    run_capped(NAME, &dir);
}

#[test]
fn a_piece_index_longer_than_recorded_is_refused_before_it_is_read() {
    const NAME: &str = "a_piece_index_longer_than_recorded_is_refused_before_it_is_read";
    if let Some(dir) = std::env::var_os(CHILD) {
        match PartReader::open(Path::new(&dir)) {
            Err(Error::Corrupt(m)) => assert!(m.contains(PIECES_IDX), "{m}"),
            other => panic!(
                "expected Corrupt naming {PIECES_IDX}, got {:?}",
                other.map(|_| ())
            ),
        }
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = write_part(tmp.path(), &[7u8; 4096], Chunks(512));
    // Damage that leaves pieces.idx far longer than traces.idx records: a
    // sparse file of 64 GiB, which takes no space on disk.
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join(PIECES_IDX))
        .unwrap()
        .set_len(64 << 30)
        .unwrap();
    run_capped(NAME, &dir);
}
