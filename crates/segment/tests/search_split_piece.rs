//! Search over a piece that the text index could not hold in one segment.
//!
//! With a text index budget of 1 MiB, a piece of 40,000 distinct words is cut
//! by the index builder into several segments, so the piece's id is in more
//! than one. Words, a word and a late word, a phrase over a cut, a prefix, a
//! word longer than 64 bytes and a negation must all be answered as if the
//! index were in one piece.

use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::{PartReader, PartWriter, SearchLimits, TEXT_INDEX_FILE, WriterOptions};
use nwdb_textindex::{IndexReader, parse};
use std::ops::Range;
use std::sync::Arc;

const WORDS: usize = 40_000;

/// The whole value is one piece.
struct Whole;

impl PayloadSplitter for Whole {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.push(0..value.len());
    }
}

fn big() -> String {
    let mut s = format!("{} ", "e".repeat(70));
    for i in 0..WORDS {
        s.push_str(&format!("a{i:06} "));
    }
    s.push_str(&"l".repeat(70));
    s
}

fn part(dir: &std::path::Path, values: &[String]) -> PartReader {
    let options = WriterOptions {
        splitter: Some(Arc::new(Whole)),
        text_index_budget_bytes: 1 << 20,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(dir, 1, 0, 900_000_000_000, options).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 10,
        max_bytes: 1 << 30,
    });
    for (i, v) in values.iter().enumerate() {
        b.append(&SpanRecord {
            trace_id: TraceId([i as u8 + 1; 16]),
            span_id: SpanId([i as u8 + 1; 8]),
            name: "span",
            start_ns: 1,
            end_ns: 2,
            attrs: v.as_bytes(),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    PartReader::open(dir.join(part_dir_name(1))).unwrap()
}

#[test]
fn every_query_kind_sees_a_piece_cut_between_index_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let values = vec![
        big(),
        "a000010 and other words of row one".to_string(),
        format!("{} a000000 a039999 row two", "l".repeat(70)),
    ];
    let r = part(tmp.path(), &values);

    // the piece really is in several segments
    let idx = tmp.path().join(part_dir_name(1)).join(TEXT_INDEX_FILE);
    let meta = *IndexReader::open(std::fs::File::open(idx).unwrap())
        .unwrap()
        .meta();
    assert!(meta.segments >= 4, "{meta:?}");
    assert_eq!(meta.docs, 3);

    let found = |q: &str| {
        r.search(&parse(q).unwrap(), &SearchLimits::default())
            .unwrap()
    };
    // one word, early, middle and late
    assert_eq!(found("a000000"), [0, 2]);
    assert_eq!(found("a020000"), [0]);
    assert_eq!(found("a039999"), [0, 2]);
    // an early and a late word of the same piece
    assert_eq!(found("a000000 a039999"), [0, 2]);
    assert_eq!(found("a000010 a039999"), [0]);
    assert_eq!(found("a000010 a020000 a039998"), [0]);
    // a prefix
    assert_eq!(found("a0399*"), [0, 2]);
    assert_eq!(found("a00001*"), [0, 1]);
    // negation
    assert_eq!(found("a000010 -a039999"), [1]);
    assert_eq!(found("row -a000000"), [1]);
    // a word longer than 64 bytes, early and late, which the index lists by
    // its first 64 bytes and the text confirms
    let e = "e".repeat(70);
    let l = "l".repeat(70);
    assert_eq!(found(&e), [0]);
    assert_eq!(found(&l), [0, 2]);
    assert_eq!(found(&format!("{e} {l}")), [0]);
    assert_eq!(found(&format!("{e} a039999 {l}")), [0]);
    // A phrase over every place the piece can be cut. The builder takes the
    // text in chunks of 1 KiB, which is 128 words of 8 bytes, and closes a
    // segment only after a chunk; the first cut is a few words early because
    // of the long word that starts the piece. So every cut is in the window
    // of the 16 words before a multiple of 128.
    for k in 1..WORDS / 128 {
        for i in k * 128 - 16..k * 128 {
            let q = format!("\"a{i:06} a{:06}\"", i + 1);
            assert_eq!(found(&q), [0], "{q}");
        }
    }
    assert!(found("\"a000001 a039998\"").is_empty());
    assert_eq!(found(&format!("\"a039999 {l}\"")), [0]);
}
