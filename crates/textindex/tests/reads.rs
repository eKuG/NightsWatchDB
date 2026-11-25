//! Bytes read per call, counted through a `ReadAt` wrapper: a search reads
//! only what it needs, a damaged segment is read once, and many threads that
//! search a fresh reader at the same moment verify a segment once.

mod support;

use std::sync::Barrier;
use std::time::Duration;

use nwdb_core::Error;
use nwdb_textindex::{
    IdListReader, IdListWriter, IndexBuilder, IndexMeta, IndexOptions, IndexReader,
};
use support::*;

/// 300,000 documents. `common` is in two of every three (a long list), `rare`
/// in ten of them (a short list on disk), and 5,000 further words each in 60
/// documents.
fn searchable() -> (Vec<u8>, IndexMeta) {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(IndexOptions::default(), &mut out);
    for i in 0..300_000u32 {
        let mut text = format!("k{}", i % 5000);
        if i % 3 != 0 {
            text.push_str(" common");
        }
        if i % 30_000 == 5 {
            text.push_str(" rare");
        }
        b.add(i, text.as_bytes()).unwrap();
    }
    let meta = b.finish().unwrap();
    assert_eq!(meta.segments, 1);
    (out, meta)
}

#[test]
fn a_search_reads_only_what_it_needs() {
    let (idx, meta) = searchable();
    let body = meta.postings_bytes + meta.dictionary_bytes;
    let src = CountingSource::new(idx);
    let r = IndexReader::open(&src).unwrap();
    r.verify().unwrap();
    assert!(src.take() >= body, "verification reads the whole segment");

    // a short list: a dictionary block and the list itself
    assert_eq!(r.term("rare").unwrap().ids.len(), 10);
    let read = src.take();
    assert!(read < 16 * 1024, "term(rare) read {read} bytes");

    // a rare word next to a long list: blocks of the long list are
    // fetched only around the rare ids
    let found = r.intersect(&["common", "rare"]).unwrap();
    assert_eq!(found.ids.len(), 10);
    let read = src.take();
    println!("intersect read {read} bytes of {body}");
    assert!(
        read < 16 * 1024 && read < body / 10,
        "intersect(common, rare) read {read} of {body} bytes"
    );

    // the long list on its own is read once, in large steps
    let all = r.term("common").unwrap();
    assert_eq!(all.ids.len(), 200_000);
    let read = src.take();
    println!("term(common) read {read} bytes of {body}");
    assert!(
        read <= meta.postings_bytes + 16 * 1024,
        "term(common) read {read} bytes"
    );

    // a prefix reads the dictionary run and the lists it matches, not the file
    let got = r.prefix("k49", 1000).unwrap();
    assert_eq!(got.len(), 60 * 111);
    let read = src.take();
    assert!(read < body / 2, "prefix read {read} of {body} bytes");
}

/// README, "The id-list store": a read fetches one group, and a list never
/// shares a group with a large one, so a small list costs a small read.
#[test]
fn an_id_list_get_reads_one_small_group() {
    let mut out = Vec::new();
    let mut w = IdListWriter::new(&mut out);
    let mut sizes = Vec::new();
    for key in 0..700u32 {
        let list: Vec<u32> = match key % 50 {
            // about 75 KB, far over the 32 KiB of a group
            7 => (0..60_000).map(|i| i * 1000).collect(),
            k => (0..k % 9 + 3).map(|i| i * 13 + key).collect(),
        };
        sizes.push(list.len());
        w.push(&list).unwrap();
    }
    let meta = w.finish().unwrap();
    let src = CountingSource::new(out);
    let r = IdListReader::open(&src).unwrap();
    src.take();
    let (mut worst_small, mut worst_big) = (0, 0);
    for key in 0..700u32 {
        let got = r.get(key).unwrap();
        assert_eq!(got.len(), sizes[key as usize]);
        let read = src.take();
        if sizes[key as usize] > 1000 {
            worst_big = worst_big.max(read);
            assert!(read < 100 * 1024, "key {key} read {read} bytes");
        } else {
            worst_small = worst_small.max(read);
            // a group of small lists is a few KiB; it never holds a big list
            assert!(read <= 8 * 1024, "small key {key} read {read} bytes");
        }
    }
    println!(
        "file {} bytes: a small list read at most {worst_small}, a big one {worst_big}",
        meta.bytes
    );
}

fn damaged_index() -> (Vec<u8>, IndexMeta) {
    let (mut idx, meta) = searchable();
    idx[100] ^= 0x10;
    (idx, meta)
}

#[test]
fn repeated_searches_on_a_damaged_file_do_not_read_it_again() {
    let (idx, meta) = damaged_index();
    let body = meta.postings_bytes + meta.dictionary_bytes;
    let src = CountingSource::new(idx);
    let r = IndexReader::open(&src).unwrap();
    src.take();
    let Err(Error::Corrupt(first)) = r.term("rare") else {
        panic!("the damage was not found");
    };
    assert!(src.take() >= body, "the first search reads the segment");
    for round in 0..4 {
        let results = [
            r.term("rare").map(|_| ()),
            r.term("common").map(|_| ()),
            r.prefix("k4", 100).map(|_| ()),
            r.intersect(&["common", "rare"]).map(|_| ()),
            r.verify(),
        ];
        for res in results {
            match res {
                Err(Error::Corrupt(m)) => assert_eq!(m, first, "round {round}"),
                other => panic!("round {round}: {other:?}"),
            }
        }
        assert_eq!(src.take(), 0, "round {round} read a segment known damaged");
    }
}

/// Many threads on a fresh reader: the verification (a read of the whole
/// segment) happens once, and the threads that arrive during it wait for it.
#[test]
fn concurrent_first_searches_verify_a_segment_once() {
    let (idx, meta) = searchable();
    let body = meta.postings_bytes + meta.dictionary_bytes;
    let threads = 16;
    let src = CountingSource::slow(idx, Duration::from_millis(3));
    let r = IndexReader::open(&src).unwrap();
    src.take();
    let gate = Barrier::new(threads);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                gate.wait();
                assert_eq!(r.term("rare").unwrap().ids.len(), 10);
            });
        }
    });
    let read = src.take();
    println!("{threads} threads read {read} bytes, one verification is {body}");
    assert!(
        read >= body && read <= body + threads as u64 * 16 * 1024,
        "{threads} concurrent first searches read {read} bytes; verifying once reads {body}"
    );
}

#[test]
fn concurrent_first_searches_on_a_damaged_segment_read_it_once() {
    let (idx, meta) = damaged_index();
    let body = meta.postings_bytes + meta.dictionary_bytes;
    let threads = 16;
    let src = CountingSource::slow(idx, Duration::from_millis(3));
    let r = IndexReader::open(&src).unwrap();
    src.take();
    let gate = Barrier::new(threads);
    let messages: Vec<String> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    gate.wait();
                    match r.term("rare") {
                        Err(Error::Corrupt(m)) => m,
                        other => panic!("{other:?}"),
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(messages.iter().all(|m| m == &messages[0]));
    let read = src.take();
    assert!(
        read >= body && read <= body + 64 * 1024,
        "{threads} threads read {read} bytes of a damaged {body} byte segment"
    );
}
