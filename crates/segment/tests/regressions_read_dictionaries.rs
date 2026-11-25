//! What reading one trace back from a part holds in memory. The rows of a
//! trace are expected to cost about what those rows hold. A part's row group
//! has one dictionary per origin column, shared by every trace in it; a read
//! of one trace must not carry the values that only other traces use.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use nwdb_core::{
    BatchLimits, Origin, SpanBatchBuilder, SpanColumns, SpanId, SpanRecord, TraceId, part_dir_name,
};
use nwdb_segment::{PartReader, PartWriter, WriterOptions};

const WINDOW_NS: i64 = 900_000_000_000;
/// Size of each of the other traces' resources.
const RESOURCE: usize = 60 * 1024;
/// Other traces in the part, each with a resource of its own: together just
/// under the 1 MiB dictionary page limit, so the column stays dictionary
/// encoded.
const OTHERS: u8 = 15;

fn span(trace: u8) -> SpanRecord<'static> {
    SpanRecord {
        trace_id: TraceId([trace; 16]),
        span_id: SpanId([trace; 8]),
        name: "s",
        start_ns: 1_000,
        end_ns: 2_000,
        ..SpanRecord::default()
    }
}

#[test]
fn reading_one_trace_does_not_hold_the_resources_of_other_traces() {
    let resources: Vec<Vec<u8>> = (1..=OTHERS)
        .map(|n| {
            (0..RESOURCE)
                .map(|i| (i % 251) as u8 ^ n.wrapping_mul(37))
                .collect()
        })
        .collect();
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    // Trace 1: one span with a tiny resource.
    let keys = b.intern_origin(&Origin {
        resource: Some(b"tiny"),
        service_name: Some("small"),
        ..Origin::default()
    });
    b.append_span(&span(1), &keys);
    // Traces 2..: one span each, each with its own large resource.
    for (i, res) in resources.iter().enumerate() {
        let keys = b.intern_origin(&Origin {
            resource: Some(res),
            ..Origin::default()
        });
        b.append_span(&span(i as u8 + 2), &keys);
    }
    let batch = b.finish();

    let tmp = tempfile::tempdir().unwrap();
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    drop(batch);

    let reader = PartReader::open(tmp.path().join(part_dir_name(1))).unwrap();
    let got: Vec<RecordBatch> = reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap();
    let rows: usize = got.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);
    let cols = SpanColumns::try_new(&got[0]).unwrap();
    assert_eq!(cols.origin(0).resource, Some(&b"tiny"[..]));

    let values: usize = got
        .iter()
        .map(|b| {
            b.column_by_name("resource")
                .unwrap()
                .as_any_dictionary()
                .values()
                .len()
        })
        .sum();
    let held: usize = got.iter().map(RecordBatch::get_array_memory_size).sum();
    eprintln!("one row read back: {values} resource values, {held} bytes held");
    assert!(
        held < 256 * 1024,
        "reading a trace of one row with a 4-byte resource holds {held} bytes ({values} \
         resource values): the resources of the {OTHERS} other traces in the row group, \
         {} bytes, came with it",
        OTHERS as usize * RESOURCE
    );
}
