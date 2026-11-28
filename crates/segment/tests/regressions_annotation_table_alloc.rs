//! `AnnotationReader::open` compares the length of the file on disk with the
//! one the part's metadata records (`meta.files["annotations.nwa"]`) before it
//! allocates anything sized by the file's content, so a damaged or planted file
//! that is long (a sparse file costs no disk) and ends in a footer of the right
//! part is refused without reading a block table as large as the file.
//!
//! Sequence: a part of one record (its metadata records a file of a few
//! hundred bytes). Its file is replaced by a sparse file of
//! 36 x 4,000,000 + 80 bytes whose last 80 bytes are the part's footer with
//! `blocks` = 4,000,000 and the footer checksum recomputed. Open.
//!
//! Guaranteed: `Corrupt`, without allocating more than a few MiB.

mod support;

use std::os::unix::fs::FileExt;

use nwdb_core::Error;
use nwdb_segment::AnnotationReader;
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use support::ann::*;

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn a_file_longer_than_its_metadata_says_is_refused_before_its_table_is_read() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = write_part(tmp.path(), 1, &[score(1, 1)]);
    let path = file_of(tmp.path(), 1);
    let bytes = std::fs::read(&path).unwrap();
    let mut footer = bytes[bytes.len() - 80..].to_vec();

    let blocks: u64 = 4_000_000;
    let len = 36 * blocks + 80;
    footer[48..56].copy_from_slice(&blocks.to_le_bytes());
    let crc = crc32c::crc32c(&footer[..76]);
    footer[76..80].copy_from_slice(&crc.to_le_bytes());
    std::fs::remove_file(&path).unwrap();
    let f = std::fs::File::create(&path).unwrap();
    f.set_len(len).unwrap();
    f.write_all_at(&footer, len - 80).unwrap();
    drop(f);

    let before = live();
    reset_peak();
    let err = AnnotationReader::open(part_dir(tmp.path(), 1), &meta)
        .err()
        .expect("the file must be refused");
    let grown = peak() - before;
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(
        grown < 16 << 20,
        "the open allocated {grown} bytes for a part whose metadata records a file of {} bytes \
         ({err})",
        meta.files["annotations.nwa"]
    );
}
