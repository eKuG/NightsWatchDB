//! A sender decides how many pieces a part gets. The writer keeps a few bytes
//! for every piece of the part and every open part keeps a few more, so the
//! number must not follow the sender: an array of short elements is one piece
//! (the splitter), a cut under 8 bytes stays inline (the floor of the part
//! writer), and a part stops making new pieces at `max_pieces` (the ceiling).
//! The tables are kept in their compact on-disk form, one to three bytes a
//! piece.
//!
//! The peak is the kernel's high-water mark of resident memory (`VmHWM`), as
//! in `nwdb-segment`'s memory test; this file holds one test so that nothing
//! else runs in the process.

#![cfg(target_os = "linux")]

use nwdb_core::{BatchLimits, PartMeta, SpanBatchBuilder, SpanId, SpanRecord, part_dir_name};
use nwdb_otlp::OtlpSplitter;
use nwdb_segment::{PartReader, PartWriter, WriterOptions};
use nwdb_testkit::mem::memory;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use prost::Message;
use std::path::Path;
use std::sync::Arc;

const MIB: usize = 1 << 20;
/// Distinct numbers per span.
const PER_SPAN: u64 = 1 << 16;
/// Attributes per span in the short-value phases.
const ATTRS_PER_SPAN: u64 = 1 << 14;

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(Value::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// The `attrs` column value of a span whose `gen_ai.input.messages` is the
/// JSON array of the numbers `from..from + PER_SPAN`.
fn numbers(from: u64) -> Vec<u8> {
    let numbers: Vec<String> = (from..from + PER_SPAN).map(|n| n.to_string()).collect();
    string_kv(
        "gen_ai.input.messages".into(),
        format!("[{}]", numbers.join(",")),
    )
    .encode_length_delimited_to_vec()
}

/// The `attrs` column value of a span with `ATTRS_PER_SPAN` attributes under a
/// content key prefix, whose values are `value(n)` for the numbers `from..`.
fn many_values(from: u64, value: impl Fn(u64) -> String) -> Vec<u8> {
    (from..from + ATTRS_PER_SPAN)
        .flat_map(|n| {
            string_kv(format!("gen_ai.prompt.{n}"), value(n)).encode_length_delimited_to_vec()
        })
        .collect()
}

/// Writes a part of `spans` spans, one per trace and one per batch, with a
/// 1 MiB window and small row groups, and returns the peak resident memory
/// afterwards and the part's metadata.
fn write(
    dir: &Path,
    id: u64,
    spans: u64,
    attrs: impl Fn(u64) -> Vec<u8>,
    max_pieces: usize,
) -> (usize, PartMeta) {
    let options = WriterOptions {
        splitter: Some(Arc::new(OtlpSplitter::default())),
        dedup_window_bytes: MIB,
        row_group_rows: 2,
        // The text index and the row lists have budgets of their own; they are
        // kept small so that what this test measures is not hidden by them.
        text_index_budget_bytes: 1 << 20,
        row_list_budget_bytes: 1 << 20,
        max_pieces,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(dir, id, 0, 900_000_000_000, options).unwrap();
    for s in 0..spans {
        let value = attrs(s);
        let mut b = SpanBatchBuilder::new(BatchLimits {
            max_rows: 10,
            max_bytes: 1 << 30,
        });
        b.append(&SpanRecord {
            trace_id: nwdb_core::TraceId((u128::from(s) + 1).to_be_bytes()),
            span_id: SpanId((s + 1).to_be_bytes()),
            name: "chat",
            attrs: &value,
            ..SpanRecord::default()
        });
        w.write(&b.finish()).unwrap();
    }
    let meta = w.finish().unwrap();
    (memory().0, meta)
}

#[test]
fn writer_and_reader_memory_do_not_follow_the_number_of_short_values() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, start) = memory();

    // 1. Arrays of numbers: one piece each, however many numbers.
    let numbers_of = |s: u64| numbers(s * PER_SPAN);
    let (peak_small, small) = write(tmp.path(), 1, 8, numbers_of, usize::MAX);
    let (peak_big, big) = write(tmp.path(), 2, 40, numbers_of, usize::MAX);
    eprintln!(
        "resident at start {} MiB; peak after {} pieces ({} KiB unique) {} MiB, after {} pieces \
         ({} KiB unique) {} MiB",
        start / MIB,
        small.payload.pieces,
        small.payload.unique_bytes / 1024,
        peak_small / MIB,
        big.payload.pieces,
        big.payload.unique_bytes / 1024,
        peak_big / MIB,
    );
    assert_eq!((small.payload.pieces, big.payload.pieces), (8, 40));
    assert!(big.payload.unique_bytes > 2 * MIB as u64);
    assert!(
        peak_big <= peak_small + 8 * MIB,
        "peak grew from {} to {} MiB for {} more KiB of unique text, with a window of 1 MiB",
        peak_small / MIB,
        peak_big / MIB,
        (big.payload.unique_bytes - small.payload.unique_bytes) / 1024
    );

    // 2. Many values of 4 bytes: shorter than the floor, so none is a piece,
    // although every one is under a content key and is distinct.
    let tiny = write(
        tmp.path(),
        3,
        40,
        |s| many_values(s * ATTRS_PER_SPAN, |n| format!("{:04x}", n % 65_536)),
        usize::MAX,
    )
    .1;
    assert_eq!(tiny.payload.pieces, 0, "{:?}", tiny.payload);

    // 3. Many distinct values of 12 bytes: they are pieces, up to the ceiling
    // and no more.
    const CEILING: usize = 20_000;
    let (_, capped) = write(
        tmp.path(),
        4,
        40,
        |s| many_values(s * ATTRS_PER_SPAN, |n| format!("v{n:011}")),
        CEILING,
    );
    assert_eq!(capped.payload.pieces, CEILING as u64);
    assert_eq!(capped.payload.unique_bytes, 12 * CEILING as u64);

    // 4. What an open part holds for its pieces: the table as it is on disk
    // with a small index, a few bytes a piece (12-byte pieces here, one byte
    // each in the table).
    let (_, before) = memory();
    let reader = PartReader::open(tmp.path().join(part_dir_name(4))).unwrap();
    let (_, after) = memory();
    let held = reader.piece_store().memory_bytes();
    eprintln!(
        "an open part with {} pieces holds {held} bytes for them; resident memory grew {} KiB",
        capped.payload.pieces,
        after.saturating_sub(before) / 1024
    );
    assert!(held < 3 * CEILING, "{held} bytes for {CEILING} pieces");
    // The part reads, and its pieces and ropes agree.
    reader.verify().unwrap();
}
