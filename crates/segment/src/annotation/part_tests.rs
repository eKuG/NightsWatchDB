//! Tests that need the private state of the writer, the reader and the
//! cursor.

use super::*;
use crate::writer::FAIL_SYNC_AFTER_RENAME;

fn rec(trace: u128, comment_len: usize) -> Annotation {
    Annotation {
        trace_id: TraceId(trace.to_be_bytes()),
        span_id: None,
        name: "n".into(),
        source: String::new(),
        time_ns: 5,
        value: Some(1.0),
        label: None,
        comment: (comment_len > 0).then(|| "c".repeat(comment_len)),
        delete: false,
        arrival: 1,
    }
}

fn write(dir: &Path, id: u64, records: &[Annotation]) -> PartMeta {
    let mut w = AnnotationWriter::create(dir, id, 0, 1_000).unwrap();
    for a in records {
        w.append(a).unwrap();
    }
    w.finish(0).unwrap()
}

#[test]
fn a_failure_after_the_rename_is_fatal() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w = AnnotationWriter::create(tmp.path(), 5, 0, 1_000).unwrap();
    w.append(&rec(1, 0)).unwrap();
    FAIL_SYNC_AFTER_RENAME.with(|f| f.set(true));
    let err = w.finish(0).unwrap_err();
    assert!(
        matches!(&err, Error::Fatal(m) if m.contains("do not reuse")),
        "{err}"
    );
    // The part is in place and its id is taken.
    assert!(tmp.path().join(part_dir_name(5)).is_dir());
    assert!(
        !tmp.path()
            .join(format!("{}.tmp", part_dir_name(5)))
            .exists()
    );
    let again = AnnotationWriter::create(tmp.path(), 5, 0, 1_000);
    assert!(matches!(again.err(), Some(Error::InvalidArgument(_))));
}

#[test]
fn a_reader_holds_the_table_and_a_cursor_one_block_of_each_kind() {
    // Two parts of about the same number of blocks, one of large records and
    // one of tiny ones: the reader's state is the same size.
    let tmp = tempfile::tempdir().unwrap();
    let big: Vec<_> = (1..=64).map(|t| rec(t, 1024)).collect();
    let m_big = write(tmp.path(), 1, &big);
    let r_big = AnnotationReader::open(tmp.path().join(part_dir_name(1)), &m_big).unwrap();
    // Tiny records of 44 bytes; a block closes at 373 of them.
    let n = (r_big.blocks() - 1) * 373 + 1;
    let small: Vec<_> = (1..=n as u128).map(|t| rec(t, 0)).collect();
    let m_small = write(tmp.path(), 2, &small);
    let r_small = AnnotationReader::open(tmp.path().join(part_dir_name(2)), &m_small).unwrap();
    assert!(r_big.blocks() >= 4);
    assert_eq!(r_big.blocks(), r_small.blocks());
    // The same block count, and so the same 36 bytes a block.
    for r in [&r_big, &r_small] {
        assert_eq!(r.table.len(), ENTRY_BYTES * r.blocks());
        assert_eq!(r.table.capacity(), r.table.len());
        assert_eq!(r.blocks_read(), 0);
    }
    assert!(r_big.meta.rows < r_small.meta.rows / 10);
    // A cursor holds one compressed and one decompressed block.
    for r in [&r_big, &r_small] {
        let mut c = r.cursor(None);
        let mut peak = 0;
        while c.next().unwrap().is_some() {
            peak = peak.max(c.comp.capacity() + c.raw.capacity());
        }
        assert!(peak > 0 && peak <= 2 * 32 * 1024, "{peak}");
    }
}
