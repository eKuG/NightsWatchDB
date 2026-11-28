//! Regression tests for fixed defects of the span part crate: the first three
//! tests are the original ones, the rest cover the other fixes.

use arrow_array::{Array, LargeBinaryArray, RecordBatch};
use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanId, SpanRecord, TraceId, part_dir_name};
use nwdb_segment::{
    INDEX_FILE, PartReader, PartWriter, SPANS_FILE, SUMMARY_FILE, ScanOptions, WriterOptions,
};
use nwdb_testkit::fs::copy_dir;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

const WINDOW_NS: i64 = 900_000_000_000;

fn batch(records: &[SpanRecord<'_>]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 20,
        max_bytes: 1 << 40,
    });
    for r in records {
        b.append(r);
    }
    b.finish()
}

fn span<'a>(trace: u8, i: u32, attrs: &'a [u8]) -> SpanRecord<'a> {
    SpanRecord {
        trace_id: TraceId([trace; 16]),
        span_id: SpanId([i as u8; 8]),
        name: "s",
        start_ns: 1_000 + i as i64,
        end_ns: 2_000 + i as i64,
        attrs,
        ..SpanRecord::default()
    }
}

/// The reader's own comment: "A row group never holds more value bytes than
/// the writer's bound (plus one row), so a batch of this many rows cannot
/// exceed that bound either." `scan` builds one reader over several row
/// groups, and its batches run across group boundaries.
#[test]
fn scan_batches_stay_within_one_row_group() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![7u8; 1000];
    let options = WriterOptions {
        row_group_bytes: 4_000, // about four 1 KB rows per group
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, options).unwrap();
    let rows: Vec<_> = (0..200).map(|i| span(1, i, &attrs)).collect();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();

    let reader = PartReader::open(tmp.path().join(part_dir_name(1))).unwrap();
    assert!(reader.row_group_count() >= 40);
    let largest = reader
        .scan(ScanOptions::default())
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .max()
        .unwrap();
    assert!(
        largest <= 5,
        "one scan batch holds {largest} rows ({} KB of values) although a row group \
         holds at most 5",
        largest
    );
}

/// Parquet pages carry no checksum (the pinned writer never writes one) and
/// zstd stores incompressible data as raw blocks, so a flipped byte in a
/// large random value is returned as data.
#[test]
fn a_flipped_byte_in_span_data_is_caught_by_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let mut attrs = vec![0u8; 64 * 1024];
    StdRng::seed_from_u64(1).fill_bytes(&mut attrs);
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch(&[span(1, 1, &attrs)])).unwrap();
    w.finish().unwrap();

    let dir = tmp.path().join(part_dir_name(1));
    let path = dir.join(SPANS_FILE);
    let mut bytes = std::fs::read(&path).unwrap();
    let needle = &attrs[30_000..30_032];
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("random attrs are stored verbatim");
    bytes[at + 5] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();

    // Changed from the original version: damage is detected by `verify`,
    // which the brief keeps out of `open` and out of every read, so a read
    // alone may still return the damaged bytes. The data is therefore either
    // refused by `verify` or, when `verify` is skipped, the reader may return
    // it; the guarantee is that a caller who verifies is never fooled.
    let reader = PartReader::open(&dir).unwrap();
    expect_corrupt_naming(reader.verify().unwrap_err(), SPANS_FILE);
}

fn read_everything(dir: &Path) -> nwdb_core::Result<()> {
    let reader = PartReader::open(dir)?;
    for e in reader.trace_entries().to_vec() {
        reader.read_trace(e.trace_id, None)?;
    }
    for b in reader.scan(ScanOptions::default())? {
        b?;
    }
    reader.scan_summary(None, None)?;
    Ok(())
}

/// Damage anywhere in a part file must give an error, never a panic.
#[test]
fn damaged_parquet_bytes_never_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![3u8; 50];
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    let rows: Vec<_> = (0..3u8)
        .flat_map(|t| (0..4).map(move |i| (t, i)))
        .map(|(t, i)| span(t, i, &attrs))
        .collect();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();
    let good = tmp.path().join(part_dir_name(1));
    read_everything(&good).unwrap();

    let mut panics = Vec::new();
    for file in [SPANS_FILE, SUMMARY_FILE] {
        let original = std::fs::read(good.join(file)).unwrap();
        for pos in 0..original.len() {
            for mask in [0xffu8] {
                let scratch = tempfile::tempdir().unwrap();
                let dir = scratch.path().join("p");
                std::fs::create_dir(&dir).unwrap();
                for f in [
                    SPANS_FILE,
                    SUMMARY_FILE,
                    "traces.idx",
                    "pieces.blk",
                    "pieces.idx",
                    "text.idx",
                    "text.rows",
                ] {
                    std::fs::copy(good.join(f), dir.join(f)).unwrap();
                }
                let mut bytes = original.clone();
                bytes[pos] ^= mask;
                std::fs::write(dir.join(file), &bytes).unwrap();
                if catch_unwind(AssertUnwindSafe(|| read_everything(&dir))).is_err() {
                    panics.push(format!("{file} byte {pos} ^ {mask:#04x}"));
                }
            }
        }
    }
    assert!(
        panics.is_empty(),
        "{} damaged files panicked instead of failing, e.g. {:?}",
        panics.len(),
        &panics[..panics.len().min(10)]
    );
}

// ------------------------------------------------- further regressions ---

use nwdb_core::{Error, span_schema};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

fn write_simple(
    parts: &Path,
    id: u64,
    traces: u8,
    per_trace: u32,
    options: WriterOptions,
) -> PathBuf {
    let attrs = vec![3u8; 50];
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options).unwrap();
    let rows: Vec<_> = (0..traces)
        .flat_map(|t| (0..per_trace).map(move |i| (t, i)))
        .map(|(t, i)| span(t, i, &attrs))
        .collect();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(id))
}

fn expect_corrupt_naming(err: Error, file: &str) {
    match err {
        Error::Corrupt(msg) => assert!(msg.contains(file), "{file}: {msg}"),
        other => panic!("expected Corrupt naming {file}, got {other:?}"),
    }
}

/// Every single-byte change to a Parquet file is caught by `open` or by
/// `verify`, with `Corrupt` naming the file: the CRC-32C detects all of them.
#[test]
fn every_single_byte_flip_is_detected_by_open_or_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let good = write_simple(tmp.path(), 1, 3, 4, WriterOptions::default());
    let reader = PartReader::open(&good).unwrap();
    reader.verify().unwrap();
    assert!(reader.spans_checksum().len > 0 && reader.summary_checksum().len > 0);

    for file in [SPANS_FILE, SUMMARY_FILE] {
        let original = std::fs::read(good.join(file)).unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let dir = scratch.path().join("p");
        copy_dir(&good, &dir);
        for pos in 0..original.len() {
            let mut bytes = original.clone();
            bytes[pos] ^= 0x10;
            std::fs::write(dir.join(file), &bytes).unwrap();
            match PartReader::open(&dir) {
                Err(e) => expect_corrupt_naming(e, file),
                Ok(r) => expect_corrupt_naming(
                    r.verify()
                        .expect_err(&format!("{file} byte {pos} undetected")),
                    file,
                ),
            }
        }
    }
}

#[test]
fn open_checks_the_recorded_file_lengths() {
    let tmp = tempfile::tempdir().unwrap();
    let good = write_simple(tmp.path(), 1, 2, 3, WriterOptions::default());
    for file in [SPANS_FILE, SUMMARY_FILE] {
        let original = std::fs::read(good.join(file)).unwrap();
        let shorter = original[..original.len() - 1].to_vec();
        let longer = [&original[..], &[0u8][..]].concat();
        for bytes in [shorter, longer] {
            let scratch = tempfile::tempdir().unwrap();
            let dir = scratch.path().join("p");
            copy_dir(&good, &dir);
            std::fs::write(dir.join(file), &bytes).unwrap();
            match PartReader::open(&dir) {
                Err(Error::Corrupt(msg)) => {
                    assert!(msg.contains(file) && msg.contains("recorded"), "{msg}")
                }
                other => panic!("{file}: expected a length error, got {:?}", other.err()),
            }
        }
    }
}

/// A part recorded with an older format version is refused, whatever else
/// is wrong with it, and the message says what the operator needs to know.
#[test]
fn a_part_with_an_older_format_version_is_refused_plainly() {
    let tmp = tempfile::tempdir().unwrap();
    let good = write_simple(tmp.path(), 1, 3, 4, WriterOptions::default());
    let index = std::fs::read(good.join(INDEX_FILE)).unwrap();
    assert_eq!(index[4..8], nwdb_segment::FORMAT_VERSION.to_le_bytes());
    for old in [1u32, nwdb_segment::FORMAT_VERSION - 1] {
        let dir = tmp.path().join(format!("old{old}"));
        copy_dir(&good, &dir);
        let mut bytes = index.clone();
        bytes[4..8].copy_from_slice(&old.to_le_bytes());
        std::fs::write(dir.join(INDEX_FILE), &bytes).unwrap();
        // The columns of the old part would not match either; the version
        // must be what is reported.
        std::fs::write(dir.join(SPANS_FILE), b"not parquet").unwrap();
        match PartReader::open(&dir) {
            Err(Error::Corrupt(msg)) => assert!(
                msg.contains(&format!(
                    "format version {old} but this build reads version {}",
                    nwdb_segment::FORMAT_VERSION
                )) && msg.contains("development versions")
                    && msg.contains("no migration"),
                "{msg}"
            ),
            other => panic!("expected a version error, got {:?}", other.err()),
        }
    }
}

/// A batch is also bounded by bytes: 16 rows of 4 MiB make one row group of
/// 64 MiB, which must come back in batches near 32 MiB, not as one.
#[test]
fn large_rows_come_back_in_byte_bounded_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = vec![0u8; 4 << 20];
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    let rows: Vec<_> = (0..16).map(|i| span(1, i, &attrs)).collect();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();
    let reader = PartReader::open(tmp.path().join(part_dir_name(1))).unwrap();
    assert_eq!(reader.row_group_count(), 1);

    let check = |what: &str, batches: Vec<RecordBatch>| {
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            16,
            "{what}"
        );
        let largest = batches.iter().map(|b| b.num_rows()).max().unwrap();
        assert!(
            (2..=8).contains(&largest),
            "{what}: a batch of {largest} rows of 4 MiB"
        );
    };
    check(
        "scan",
        reader
            .scan(ScanOptions::default())
            .unwrap()
            .map(|b| b.unwrap())
            .collect(),
    );
    check(
        "read_trace",
        reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap(),
    );
    check("read_rows", reader.read_rows(0, 16, None).unwrap());
}

/// No batch of `read_rows`, `read_trace` or `scan_summary` spans two row
/// groups either.
#[test]
fn no_read_batch_spans_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        row_group_rows: 10,
        summary_row_group_rows: 7,
        ..WriterOptions::default()
    };
    let dir = write_simple(tmp.path(), 1, 50, 3, options);
    let reader = PartReader::open(&dir).unwrap();
    assert_eq!(reader.row_group_count(), 15);
    let batches = reader.read_rows(5, 140, None).unwrap();
    assert!(batches.iter().all(|b| b.num_rows() <= 10));
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 140);
    // A trace of three rows that straddles a group boundary.
    let straddling = reader
        .trace_entries()
        .iter()
        .find(|e| e.first_row / 10 != (e.first_row + 2) / 10)
        .unwrap()
        .trace_id;
    let batches = reader.read_trace(straddling, None).unwrap().unwrap();
    assert!(batches.len() >= 2);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    let summary = reader.scan_summary(None, None).unwrap();
    assert!(summary.iter().all(|b| b.num_rows() <= 7));
    assert_eq!(summary.iter().map(|b| b.num_rows()).sum::<usize>(), 50);
}

#[cfg(unix)]
#[test]
fn reads_continue_after_the_part_directory_is_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        row_group_rows: 7,
        summary_row_group_rows: 5,
        ..WriterOptions::default()
    };
    let dir = write_simple(tmp.path(), 1, 12, 4, options);
    let reader = PartReader::open(&dir).unwrap();
    let want: Vec<_> = reader
        .trace_entries()
        .iter()
        .map(|e| {
            (
                e.trace_id,
                reader.read_trace(e.trace_id, None).unwrap().unwrap(),
            )
        })
        .collect();
    let want_summary = reader.scan_summary(None, None).unwrap();

    std::fs::remove_dir_all(&dir).unwrap();
    assert!(!dir.exists());

    for (id, batches) in &want {
        assert_eq!(&reader.read_trace(*id, None).unwrap().unwrap(), batches);
    }
    assert_eq!(reader.scan_summary(None, None).unwrap(), want_summary);
    let rows: usize = reader
        .scan(ScanOptions::default())
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(rows, 48);
    reader.verify().unwrap();
}

/// Statistics are off for the five raw columns, dictionaries are off for
/// `attrs`, `events` and `links` but not `resource` and `scope`, and the offset index is
/// written for every column of every row group.
#[test]
fn raw_columns_have_no_statistics_or_dictionary_but_keep_the_offset_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(5);
    let owned: Vec<Vec<u8>> = (0..20)
        .map(|_| {
            let mut a = vec![0u8; 100];
            rng.fill_bytes(&mut a);
            a
        })
        .collect();
    let resource = b"resource-bytes".to_vec();
    let scope = b"scope-bytes".to_vec();
    let rows: Vec<_> = owned
        .iter()
        .enumerate()
        .map(|(i, a)| SpanRecord {
            resource: Some(&resource),
            scope: Some(&scope),
            events: Some(a),
            links: Some(a),
            ..span(1, i as u32, a)
        })
        .collect();
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();

    let file = std::fs::File::open(tmp.path().join(part_dir_name(1)).join(SPANS_FILE)).unwrap();
    let meta = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();
    let schema = span_schema();
    for g in 0..meta.num_row_groups() {
        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name().as_str();
            let column = meta.row_group(g).column(i);
            let raw = ["resource", "scope", "attrs", "events", "links"].contains(&name);
            assert_eq!(column.statistics().is_none(), raw, "statistics of {name}");
            let pages = meta
                .page_index()
                .unwrap()
                .offset_index(g, i)
                .unwrap()
                .page_locations();
            assert!(!pages.is_empty(), "offset index of {name}");
        }
        let has_dictionary = |n: &str| {
            let column = meta.row_group(g).column(schema.index_of(n).unwrap());
            column.dictionary_page_offset().is_some()
        };
        assert!(has_dictionary("resource"));
        assert!(has_dictionary("scope"));
        for n in ["attrs", "events", "links"] {
            assert!(!has_dictionary(n), "{n}");
        }
        // The columns that are not raw keep statistics and dictionaries.
        assert!(has_dictionary("name"));
    }
}

/// `read_trace` fetches only the pages of its rows: damage to the header of
/// another trace's page does not matter to it, but does to that trace.
#[test]
fn read_trace_skips_the_pages_of_other_traces() {
    let tmp = tempfile::tempdir().unwrap();
    let mut attrs = [vec![0u8; 1 << 20], vec![0u8; 1 << 20]];
    for (i, a) in attrs.iter_mut().enumerate() {
        StdRng::seed_from_u64(i as u64).fill_bytes(a);
    }
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch(&[span(1, 1, &attrs[0]), span(2, 2, &attrs[1])]))
        .unwrap();
    w.finish().unwrap();
    let dir = tmp.path().join(part_dir_name(1));
    let path = dir.join(SPANS_FILE);

    let file = std::fs::File::open(&path).unwrap();
    let meta = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();
    let attrs_col = span_schema().index_of("attrs").unwrap();
    let pages = meta
        .page_index()
        .unwrap()
        .offset_index(0, attrs_col)
        .unwrap()
        .page_locations();
    assert!(pages.len() >= 2, "{} pages", pages.len());
    let second_page = pages[1].offset as usize;
    drop(file);

    let mut bytes = std::fs::read(&path).unwrap();
    // A page starts with a Thrift header; 0xff is not a valid field header,
    // so the page cannot be read if it is fetched.
    bytes[second_page] = 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let reader = PartReader::open(&dir).unwrap();
    let got = reader
        .read_trace(TraceId([1; 16]), Some(&["attrs"]))
        .unwrap()
        .unwrap();
    let col = got[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &attrs[0][..]);
    assert!(
        reader
            .read_trace(TraceId([2; 16]), Some(&["attrs"]))
            .is_err()
    );
}

#[test]
fn a_missing_chain_of_parts_directories_is_created() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("a").join("b").join("parts");
    let dir = write_simple(&parts, 1, 1, 1, WriterOptions::default());
    PartReader::open(&dir).unwrap().verify().unwrap();
}
