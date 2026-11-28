//! Bytes read per call, counted through a `ReadAt` wrapper.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use nwdb_core::Error;
use nwdb_textindex::{IdListReader, IdListWriter, IndexBuilder, IndexOptions, IndexReader, ReadAt};

struct Counted {
    data: Vec<u8>,
    read: AtomicU64,
}

impl Counted {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            read: AtomicU64::new(0),
        }
    }

    fn take(&self) -> u64 {
        self.read.swap(0, Relaxed)
    }
}

impl ReadAt for Counted {
    fn size(&self) -> io::Result<u64> {
        Ok(self.data.len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.read.fetch_add(buf.len() as u64, Relaxed);
        <[u8] as ReadAt>::read_exact_at(&self.data, buf, offset)
    }
}

/// README, "What is checked, and when": the checksum of a segment is
/// computed the first time it is used. When it does not match, nothing is
/// remembered, so every later search reads the whole damaged segment again
/// before failing: a damaged part costs a full read per query.
#[test]
fn a_damaged_segment_is_not_read_whole_by_every_search() {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(IndexOptions::default(), &mut out);
    for i in 0..20_000u32 {
        b.add(i, format!("w{} w{} u{i}", i % 7, i % 13).as_bytes())
            .unwrap();
    }
    let meta = b.finish().unwrap();
    assert_eq!(meta.segments, 1);
    let body = meta.postings_bytes + meta.dictionary_bytes;
    assert!(body > 128 * 1024, "{body}");
    out[100] ^= 0x10;
    let src = Counted::new(out);
    let r = IndexReader::open(&src).unwrap();
    for round in 0..3 {
        src.take();
        let res = r.term("w1");
        assert!(matches!(res, Err(Error::Corrupt(_))), "{res:?}");
        let read = src.take();
        println!("search {round}: read {read} bytes of a {body} byte segment");
        if round > 0 {
            assert!(
                read < 64 * 1024,
                "search {round} read {read} bytes to fail on a segment already found damaged"
            );
        }
    }
}

/// README, "The id-list store": a group closes after 128 keys or about
/// 32 KiB of lists, "so a read fetches one small group". A group is closed
/// only after the list that passes 32 KiB has been added, so a small list
/// that shares a group with a large one is read, and checksummed, together
/// with all of it.
#[test]
fn reading_a_small_id_list_reads_a_small_group() {
    let mut out = Vec::new();
    let mut w = IdListWriter::new(&mut out);
    w.push(&[7]).unwrap();
    // 200,000 ids with gaps of 999: ten bits each, about 250 KB
    let big: Vec<u32> = (0..200_000u32).map(|i| i * 1000).collect();
    w.push(&big).unwrap();
    for k in 0..10 {
        w.push(&[k]).unwrap();
    }
    w.finish().unwrap();
    let src = Counted::new(out);
    let r = IdListReader::open(&src).unwrap();
    src.take();
    assert_eq!(r.get(0).unwrap(), [7]);
    let read = src.take();
    assert!(read <= 40 * 1024, "reading a one-id list read {read} bytes");
}
