//! The text files of a part are bound to it: files of another part, whatever
//! their length, are refused at open; a `text.idx` that names a piece the part
//! does not have is `Corrupt`; the counts and the rows the files hold are
//! checked against the part. Also the empty `AND`, and that a search refuses a
//! phrase that needs too much text read before it reads any.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::textindex::{Query, parse};
use nwdb_segment::{
    INDEX_FILE, PartReader, PartWriter, SearchLimits, TEXT_INDEX_FILE, TEXT_ROWS_FILE,
    WriterOptions,
};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Every non-empty line of a value is a piece.
struct Lines;

impl PayloadSplitter for Lines {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut at = 0;
        for line in value.split(|&b| b == b'\n') {
            if !line.is_empty() {
                cuts.push(at..at + line.len());
            }
            at += line.len() + 1;
        }
    }
}

/// A part with one span per entry of `rows`, each in a trace of its own, the
/// entry being the span's `attrs`.
fn write_part(parts: &Path, id: u64, rows: &[&str]) -> PathBuf {
    let options = WriterOptions {
        splitter: Some(Arc::new(Lines)),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(parts, id, 0, 900_000_000_000, options).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 20,
        max_bytes: 1 << 30,
    });
    for (i, text) in rows.iter().enumerate() {
        let n = i as u64 + 1;
        b.append(&SpanRecord {
            trace_id: TraceId(u128::from(n).to_be_bytes()),
            span_id: SpanId(n.to_be_bytes()),
            name: "span",
            start_ns: 1_000 + n as i64,
            end_ns: 2_000 + n as i64,
            attrs: text.as_bytes(),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(id))
}

fn search(r: &PartReader, q: &str) -> nwdb_core::Result<Vec<u32>> {
    r.search(&parse(q).unwrap(), &SearchLimits::default())
}

/// The error `open` gives for the part, which must be `Corrupt`.
fn open_error(dir: &Path) -> String {
    match PartReader::open(dir) {
        Err(Error::Corrupt(m)) => m,
        Err(e) => panic!("open gave {e}, not Corrupt"),
        Ok(_) => panic!("the part opened"),
    }
}

/// Patches the lengths and checksums `traces.idx` records for the two text
/// files to what the files are now, and the checksum of `traces.idx` itself:
/// the part a writer would have made with these files.
fn rebind_text(dir: &Path) {
    let mut index = std::fs::read(dir.join(INDEX_FILE)).unwrap();
    for (name, at, footer_at) in [(TEXT_INDEX_FILE, 64, 88), (TEXT_ROWS_FILE, 76, 92)] {
        let bytes = std::fs::read(dir.join(name)).unwrap();
        index[at..at + 8].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        index[at + 8..at + 12].copy_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        // The footer ends with its checksum and then four bytes of magic.
        index[footer_at..footer_at + 4].copy_from_slice(&bytes[bytes.len() - 8..bytes.len() - 4]);
    }
    let body = index.len() - 4;
    let crc = crc32c::crc32c(&index[..body]);
    index[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(dir.join(INDEX_FILE), &index).unwrap();
}

fn copy_part(from: &Path, to: &Path) {
    std::fs::create_dir(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// Two parts with the same pieces in the same order but rows that refer to
/// them differently: their `text.idx` are equal, their `text.rows` differ in
/// content only (same length, same number of lists). With the other part's
/// `text.rows` the part must be refused at open, naming the file.
#[test]
fn text_rows_of_another_part_with_the_same_length_are_not_trusted() {
    let tmp = tempfile::tempdir().unwrap();
    let a = write_part(
        tmp.path(),
        1,
        &["alpha one two\nbravo one two", "alpha one two"],
    );
    let b = write_part(
        tmp.path(),
        2,
        &["alpha one two\nbravo one two", "bravo one two"],
    );
    let rows_a = std::fs::read(a.join(TEXT_ROWS_FILE)).unwrap();
    let rows_b = std::fs::read(b.join(TEXT_ROWS_FILE)).unwrap();
    assert_eq!(rows_a.len(), rows_b.len(), "precondition: same length");
    assert_ne!(rows_a, rows_b, "precondition: different lists");
    assert_eq!(
        std::fs::read(a.join(TEXT_INDEX_FILE)).unwrap(),
        std::fs::read(b.join(TEXT_INDEX_FILE)).unwrap(),
        "precondition: the same pieces"
    );
    {
        let r = PartReader::open(&a).unwrap();
        assert_eq!(search(&r, "bravo").unwrap(), [0]);
        assert_eq!(search(&r, "alpha").unwrap(), [0, 1]);
    }
    std::fs::write(a.join(TEXT_ROWS_FILE), &rows_b).unwrap();
    let m = open_error(&a);
    assert!(m.contains(TEXT_ROWS_FILE), "{m}");
}

/// Two parts whose only piece has words of the same lengths: their
/// `text.idx` have the same length and document count. With the other part's
/// `text.idx` the part must be refused at open, naming the file.
#[test]
fn a_text_index_of_another_part_with_the_same_length_is_not_trusted() {
    let tmp = tempfile::tempdir().unwrap();
    let a = write_part(tmp.path(), 1, &["alpha one two"]);
    let b = write_part(tmp.path(), 2, &["gamma one two"]);
    let idx_b = std::fs::read(b.join(TEXT_INDEX_FILE)).unwrap();
    assert_eq!(
        std::fs::metadata(a.join(TEXT_INDEX_FILE)).unwrap().len(),
        idx_b.len() as u64,
        "precondition: same length"
    );
    std::fs::write(a.join(TEXT_INDEX_FILE), &idx_b).unwrap();
    let m = open_error(&a);
    assert!(m.contains(TEXT_INDEX_FILE), "{m}");
}

/// A `text.idx` whose one document has an id past the part's pieces, with
/// checksums of its own and recorded in `traces.idx` as if the writer had made
/// it, is damage of `text.idx`: the search must say `Corrupt` naming it, not
/// that something was not found.
#[test]
fn a_document_id_past_the_pieces_is_corrupt() {
    use nwdb_segment::textindex::{IndexBuilder, IndexOptions};
    let tmp = tempfile::tempdir().unwrap();
    let a = write_part(tmp.path(), 1, &["alpha one two"]);
    let mut forged = Vec::new();
    let mut builder = IndexBuilder::new(IndexOptions::default(), &mut forged);
    builder.add(100, b"alpha one two").unwrap();
    builder.finish().unwrap();
    std::fs::write(a.join(TEXT_INDEX_FILE), &forged).unwrap();
    rebind_text(&a);
    let r = PartReader::open(&a).unwrap();
    match search(&r, "alpha") {
        Err(Error::Corrupt(m)) => assert!(m.contains(TEXT_INDEX_FILE), "{m}"),
        other => panic!("a document id past the pieces gave {other:?}"),
    }
}

/// `text.idx` and `text.rows` made for a part with more pieces, recorded in
/// `traces.idx` as if the writer had made them for this one, are refused at
/// open by their counts.
#[test]
fn text_files_with_another_number_of_pieces_are_refused_at_open() {
    let tmp = tempfile::tempdir().unwrap();
    let one = write_part(tmp.path(), 1, &["alpha one two"]);
    let two = write_part(tmp.path(), 2, &["alpha one two\nbravo one two"]);

    let a = tmp.path().join("a");
    copy_part(&one, &a);
    std::fs::copy(two.join(TEXT_INDEX_FILE), a.join(TEXT_INDEX_FILE)).unwrap();
    rebind_text(&a);
    let m = open_error(&a);
    assert!(
        m.contains(TEXT_INDEX_FILE) && m.contains("documents"),
        "{m}"
    );

    std::fs::copy(one.join(TEXT_INDEX_FILE), a.join(TEXT_INDEX_FILE)).unwrap();
    std::fs::copy(two.join(TEXT_ROWS_FILE), a.join(TEXT_ROWS_FILE)).unwrap();
    rebind_text(&a);
    let m = open_error(&a);
    assert!(m.contains(TEXT_ROWS_FILE) && m.contains("lists"), "{m}");
}

/// A `text.rows` that lists a row the part does not have, recorded in
/// `traces.idx` as if the writer had made it, opens (it has the right number of
/// lists) and a search that reads the list is `Corrupt` naming the file.
#[test]
fn a_row_outside_the_part_is_corrupt() {
    let tmp = tempfile::tempdir().unwrap();
    let three = write_part(
        tmp.path(),
        1,
        &["alpha one two", "alpha one two", "alpha one two"],
    );
    let one = write_part(tmp.path(), 2, &["alpha one two"]);
    assert_eq!(
        std::fs::read(three.join(TEXT_INDEX_FILE)).unwrap(),
        std::fs::read(one.join(TEXT_INDEX_FILE)).unwrap(),
        "precondition: the same piece"
    );
    std::fs::copy(three.join(TEXT_ROWS_FILE), one.join(TEXT_ROWS_FILE)).unwrap();
    rebind_text(&one);
    let r = PartReader::open(&one).unwrap();
    match search(&r, "alpha") {
        Err(Error::Corrupt(m)) => assert!(m.contains(TEXT_ROWS_FILE) && m.contains("row"), "{m}"),
        other => panic!("a row outside the part gave {other:?}"),
    }
}

/// `AND` combines sets of rows, so an `AND` of nothing is every row
/// (`nwdb_textindex::evaluate` documents `And([])` as the universe).
#[test]
fn an_and_of_no_operands_is_every_row() {
    let tmp = tempfile::tempdir().unwrap();
    let a = write_part(tmp.path(), 1, &["alpha one two", "bravo one two"]);
    let r = PartReader::open(&a).unwrap();
    let limits = SearchLimits::default();
    assert_eq!(r.search(&Query::And(vec![]), &limits).unwrap(), [0, 1]);
    let nested = Query::Or(vec![Query::Term("zzz".into()), Query::And(vec![])]);
    assert_eq!(r.search(&nested, &limits).unwrap(), [0, 1]);
}

/// The bytes a phrase needs read are checked against `max_confirm_bytes`
/// before any piece is read: a refused query loads no block of the piece
/// store, and the same query within the limit does.
#[test]
fn the_confirm_limit_is_checked_before_any_piece_is_read() {
    let tmp = tempfile::tempdir().unwrap();
    let a = write_part(tmp.path(), 1, &["alpha one two", "bravo one two"]);
    let q = parse("\"one two\"").unwrap();

    let r = PartReader::open(&a).unwrap();
    let tight = SearchLimits {
        max_confirm_bytes: 1,
        ..SearchLimits::default()
    };
    match r.search(&q, &tight) {
        Err(Error::InvalidArgument(m)) => assert!(m.contains("bytes of text"), "{m}"),
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    assert_eq!(r.payload_blocks_loaded(), 0, "a piece was read");

    assert_eq!(r.search(&q, &SearchLimits::default()).unwrap(), [0, 1]);
    assert!(r.payload_blocks_loaded() > 0, "the phrase read no piece");
}
