//! A merge and the piece ceiling: inputs whose pieces add up to more than a
//! part may hold are refused; inputs that add up to exactly the ceiling are
//! merged and stay searchable. A merge that is stopped leaves nothing behind.

use arrow_array::cast::AsArray;
use nwdb_core::Error;
use nwdb_core::{
    BatchLimits, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::textindex::parse;
use nwdb_segment::{
    MergeOptions, PartReader, PartWriter, PieceObserver, SearchLimits, WriterOptions, merge_parts,
};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const WINDOW_NS: i64 = 900_000_000_000;

/// Each value is one piece.
struct Whole;

impl PayloadSplitter for Whole {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        cuts.push(0..value.len());
    }
}

/// Each input is within the ceiling of 2 pieces; the two together hold 4.
fn options(max_pieces: usize) -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(Whole)),
        max_pieces,
        ..WriterOptions::default()
    }
}

fn write(dir: &Path, id: u64, trace: u8, texts: &[&str]) -> PartReader {
    let mut w = PartWriter::create(dir, id, 0, WINDOW_NS, options(2)).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits::DEFAULT);
    for (i, text) in texts.iter().enumerate() {
        b.append(&SpanRecord {
            trace_id: TraceId([trace; 16]),
            span_id: SpanId([trace, i as u8, 0, 0, 0, 0, 0, 1]),
            name: "s",
            start_ns: i64::from(trace) * 100 + i as i64,
            attrs: text.as_bytes(),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    PartReader::open(dir.join(part_dir_name(id))).unwrap()
}

/// The span ids a search finds in a part.
fn found(r: &PartReader, word: &str) -> Vec<Vec<u8>> {
    let rows = r
        .search(&parse(word).unwrap(), &SearchLimits::default())
        .unwrap();
    rows.iter()
        .map(|&row| {
            let b = r.read_rows(u64::from(row), 1, Some(&["span_id"])).unwrap();
            b[0].column(0).as_fixed_size_binary().value(0).to_vec()
        })
        .collect()
}

fn inputs(dir: &Path) -> (PartReader, PartReader) {
    let a = write(
        dir,
        1,
        1,
        &[
            "alpha text of the first input",
            "beta text of the first input",
        ],
    );
    let b = write(
        dir,
        2,
        2,
        &[
            "omega text of the second input",
            "gamma text of the second input",
        ],
    );
    (a, b)
}

fn names(dir: &Path) -> Vec<String> {
    let mut n: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    n.sort();
    n
}

#[test]
fn a_merge_whose_inputs_hold_more_pieces_than_the_ceiling_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = inputs(tmp.path());
    let before = names(tmp.path());
    let r = merge_parts(
        &[&a, &b],
        tmp.path(),
        3,
        0,
        WINDOW_NS,
        MergeOptions {
            writer: options(3),
            ..MergeOptions::default()
        },
    );
    let Err(Error::InvalidArgument(message)) = r else {
        panic!("expected a refusal, got {r:?}");
    };
    assert!(message.contains("4 pieces"), "{message}");
    assert_eq!(
        names(tmp.path()),
        before,
        "the refused merge wrote something"
    );
}

#[test]
fn inputs_that_add_up_to_exactly_the_ceiling_merge_and_stay_searchable() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = inputs(tmp.path());
    let (meta, _) = merge_parts(
        &[&a, &b],
        tmp.path(),
        3,
        0,
        WINDOW_NS,
        MergeOptions {
            writer: options(4),
            ..MergeOptions::default()
        },
    )
    .unwrap();
    let merged = PartReader::open(tmp.path().join(part_dir_name(3))).unwrap();
    merged.verify().unwrap();
    assert_eq!(meta.rows, 4);
    assert_eq!(meta.payload.pieces, 4);

    for word in ["alpha", "beta", "omega", "gamma"] {
        let mut before = found(&a, word);
        before.extend(found(&b, word));
        assert_eq!(before.len(), 1, "{word}");
        assert_eq!(found(&merged, word), before, "{word}");
    }
}

/// Sets the flag when the first piece of the output is stored, which is
/// during the first chunk.
struct StopAtFirstPiece(Arc<AtomicBool>);

impl PieceObserver for StopAtFirstPiece {
    fn piece_stored(&self, _: u32, _: &[u8]) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn a_stopped_merge_leaves_no_output_and_the_inputs_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = inputs(tmp.path());
    let before = names(tmp.path());
    let stop = Arc::new(AtomicBool::new(false));
    let mut writer = options(4);
    writer.piece_observer = Some(Arc::new(StopAtFirstPiece(stop.clone())));
    let r = merge_parts(
        &[&a, &b],
        tmp.path(),
        3,
        0,
        WINDOW_NS,
        MergeOptions {
            writer,
            // One trace to a chunk: the second chunk sees the flag.
            chunk_bytes: 1,
            stop: Some(stop),
            ..MergeOptions::default()
        },
    );
    assert!(matches!(r, Err(Error::Unavailable(_))), "{r:?}");
    assert_eq!(
        names(tmp.path()),
        before,
        "the stopped merge left something"
    );
    a.verify().unwrap();
    b.verify().unwrap();
}

/// The flag is read before the first input is verified, not only after every
/// input has been checked in full.
#[test]
fn a_merge_stopped_before_it_starts_returns_unavailable_and_creates_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = inputs(tmp.path());
    let before = names(tmp.path());
    let r = merge_parts(
        &[&a, &b],
        tmp.path(),
        3,
        0,
        WINDOW_NS,
        MergeOptions {
            writer: options(4),
            stop: Some(Arc::new(AtomicBool::new(true))),
            ..MergeOptions::default()
        },
    );
    assert!(matches!(r, Err(Error::Unavailable(_))), "{r:?}");
    assert_eq!(
        names(tmp.path()),
        before,
        "the stopped merge left something"
    );
}
