//! Pins the Parquet settings that decide the size of a part: the encoding of
//! each column and which columns have a column index. Both are recorded in
//! the file, so a reader needs no knowledge of them; this test only keeps a
//! change of the writer's defaults from undoing them unnoticed. (The file
//! records the compression codec but not its level, so the level of `attrs`
//! can only be seen in the size of a part.)

use arrow_array::RecordBatch;
use nwdb_core::{BatchLimits, SpanBatchBuilder, SpanId, SpanRecord, TraceId, part_dir_name};
use nwdb_segment::{PartReader, PartWriter, SPANS_FILE, SUMMARY_FILE, ScanOptions, WriterOptions};
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::ParquetMetaDataReader;
use std::path::Path;

const WINDOW_NS: i64 = 900_000_000_000;

fn batch(records: &[SpanRecord<'_>]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for r in records {
        b.append(r);
    }
    b.finish()
}

fn metadata(path: &Path) -> parquet::file::metadata::ParquetMetaData {
    let file = std::fs::File::open(path).unwrap();
    ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .unwrap()
}

#[test]
fn columns_have_the_encodings_statistics_and_compression_that_keep_parts_small() {
    let tmp = tempfile::tempdir().unwrap();
    let attrs = b"some attribute bytes".to_vec();
    let rows: Vec<_> = (0..300u32)
        .map(|i| SpanRecord {
            trace_id: TraceId([1 + (i / 10) as u8; 16]),
            span_id: SpanId((u64::from(i) + 1).to_be_bytes()),
            parent_span_id: (i % 10 != 0).then(|| SpanId((u64::from(i)).to_be_bytes())),
            name: "chat",
            start_ns: 1_000_000 + i64::from(i) * 7_919,
            end_ns: 2_000_000 + i64::from(i) * 104_729,
            input_tokens: Some(100 + i64::from(i % 17) * 13),
            output_tokens: Some(i64::from(i % 11)),
            attrs: &attrs,
            // Rows of one trace mostly arrived together.
            arrival: 1 + u64::from(i / 10),
            ..SpanRecord::default()
        })
        .collect();
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch(&rows)).unwrap();
    w.finish().unwrap();
    let dir = tmp.path().join(part_dir_name(1));

    for file in [SPANS_FILE, SUMMARY_FILE] {
        let meta = metadata(&dir.join(file));
        for rg in meta.row_groups() {
            for c in rg.columns() {
                let name = c.column_path().string();
                let encodings = c.encodings().collect::<Vec<_>>();
                let has = |e: Encoding| encodings.contains(&e);
                let dictionary = c.dictionary_page_offset().is_some();
                let what = format!("{file}: {name}");
                match name.as_str() {
                    "trace_id" | "span_id" | "parent_span_id" => {
                        assert!(!dictionary, "{what}");
                        assert!(has(Encoding::PLAIN), "{what}");
                    }
                    "start_ns" => {
                        assert!(!dictionary, "{what}");
                        assert!(has(Encoding::DELTA_BINARY_PACKED), "{what}");
                    }
                    "arrival" => {
                        // Only the span table has it.
                        assert_eq!(file, SPANS_FILE, "{what}");
                        assert!(!dictionary, "{what}");
                        assert!(has(Encoding::DELTA_BINARY_PACKED), "{what}");
                    }
                    "end_ns" | "input_tokens" | "output_tokens" => {
                        assert!(!dictionary, "{what}");
                        assert!(has(Encoding::BYTE_STREAM_SPLIT), "{what}");
                    }
                    _ => {}
                }
                // Only `start_ns` has a column index; every other column that
                // has statistics at all has them for the whole chunk.
                assert_eq!(
                    c.column_index_offset().is_some(),
                    name == "start_ns",
                    "column index of {what}"
                );
                // The file records the codec but not the level.
                assert!(matches!(c.compression(), Compression::ZSTD(_)), "{what}");
            }
        }
    }

    // A reader that knows nothing of the settings gets every value back.
    let reader = PartReader::open(&dir).unwrap();
    reader.verify().unwrap();
    let mut n = 0;
    for b in reader.scan(ScanOptions::default()).unwrap() {
        let b = b.unwrap();
        let cols = nwdb_core::SpanColumns::try_new(&b).unwrap();
        for row in 0..cols.len() {
            assert_eq!(cols.record(row).start_ns, rows[n].start_ns);
            assert_eq!(cols.record(row).end_ns, rows[n].end_ns);
            assert_eq!(cols.record(row).input_tokens, rows[n].input_tokens);
            assert_eq!(cols.record(row).span_id, rows[n].span_id);
            n += 1;
        }
    }
    assert_eq!(n, rows.len());
}
