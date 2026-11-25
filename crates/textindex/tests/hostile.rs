//! Files written by hand with every checksum valid, so only the structural
//! checks can catch them.

mod support;

use nwdb_core::Error;
use nwdb_textindex::IndexReader;
use support::*;

/// The postings of one list of `blocks` blocks of 128 consecutive ids from
/// id 0: a skip table, then one width-zero block (one byte) per block.
fn dense_list(blocks: usize) -> Vec<u8> {
    let mut p = Vec::new();
    for k in 0..blocks {
        p.extend_from_slice(&((k * 128 + 127) as u32).to_le_bytes());
        p.extend_from_slice(&(k as u32).to_le_bytes());
    }
    p.resize(p.len() + blocks, 0);
    p
}

fn corrupt<T: std::fmt::Debug>(r: nwdb_core::Result<T>) {
    match r {
        Err(Error::Corrupt(_)) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// Many entries pointing at one list: the second entry's list starts before
/// the first one's ends, which a file written by the builder never does.
#[test]
fn entries_that_share_a_list_are_corrupt_where_they_are_walked_in_order() {
    let docs = 256;
    let p = dense_list(2);
    let shared = || Hand::List { df: 256, off: 0 };
    let file = hand_index(
        &p,
        &[("t0", shared()), ("t1", shared()), ("t2", shared())],
        docs,
    );
    let r = IndexReader::open(file.as_slice()).unwrap();
    r.verify().unwrap();
    // the first list is fine on its own, and the walk stops at it
    assert_eq!(r.term("t0").unwrap().ids.len(), 256);
    // reaching the second one means walking past the first
    corrupt(r.term("t1"));
    corrupt(r.prefix("t", 10));
    corrupt(r.intersect(&["t0", "t2"]));
}

/// The same entries with lists one after another are an ordinary file.
#[test]
fn entries_with_lists_one_after_another_are_fine() {
    let one = dense_list(2);
    let mut p = one.clone();
    p.extend_from_slice(&one);
    let file = hand_index(
        &p,
        &[
            ("t0", Hand::List { df: 256, off: 0 }),
            (
                "t1",
                Hand::List {
                    df: 256,
                    off: one.len() as u64,
                },
            ),
        ],
        256,
    );
    let r = IndexReader::open(file.as_slice()).unwrap();
    assert_eq!(r.prefix("t", 10).unwrap().len(), 256);
    assert_eq!(r.intersect(&["t0", "t1"]).unwrap().ids.len(), 256);
}

/// A list whose count cannot be backed by the postings area is refused before
/// anything is read from it.
#[test]
fn a_count_the_file_cannot_hold_is_corrupt() {
    let p = dense_list(2);
    let file = hand_index(
        &p,
        &[(
            "t0",
            Hand::List {
                df: 4_000_000_000,
                off: 0,
            },
        )],
        u32::MAX,
    );
    let r = IndexReader::open(file.as_slice()).unwrap();
    corrupt(r.term("t0"));
    corrupt(r.prefix("t", 10));
}

/// Skip entries are checked page by page as they are read. A fault on the
/// third page (entries 128 and up) is found when the list is read through, and
/// a fault on the seam between two pages is found too.
#[test]
fn a_skip_table_that_is_wrong_beyond_the_first_page_is_corrupt() {
    let blocks = 200;
    for bad in [150usize, 63, 64, 127, 128, 199] {
        let mut p = dense_list(blocks);
        // entry `bad` repeats the last id of the entry before it
        let before = u32::from_le_bytes(p[(bad - 1) * 8..(bad - 1) * 8 + 4].try_into().unwrap());
        p[bad * 8..bad * 8 + 4].copy_from_slice(&before.to_le_bytes());
        let file = hand_index(
            &p,
            &[(
                "t0",
                Hand::List {
                    df: (blocks * 128) as u64,
                    off: 0,
                },
            )],
            (blocks * 128) as u32,
        );
        let r = IndexReader::open(file.as_slice()).unwrap();
        corrupt(r.term("t0"));
    }
}

/// Seeking into a long list finds the right page by binary search, reading
/// only a few bytes per probe, and the answer matches the list.
#[test]
fn a_seek_into_a_long_list_reads_little() {
    let blocks = 4000;
    let p = dense_list(blocks);
    let docs = (blocks * 128) as u32;
    let file = hand_index(
        &p,
        &[
            (
                "t0",
                Hand::List {
                    df: u64::from(docs),
                    off: 0,
                },
            ),
            ("t1", Hand::Inline(vec![3, 300_000, 511_999])),
        ],
        docs,
    );
    let src = CountingSource::new(file);
    let r = IndexReader::open(&src).unwrap();
    r.verify().unwrap();
    src.take();
    let found = r.intersect(&["t0", "t1"]).unwrap();
    assert_eq!(found.ids.as_slice(), [3, 300_000, 511_999]);
    let read = src.take();
    println!("three seeks into a {blocks} block list read {read} bytes");
    // the list is 36 KB; three seeks need a page and a block each, plus a few
    // 4 byte probes and two dictionary blocks
    assert!(read < 12 * 1024, "read {read} bytes");
}
