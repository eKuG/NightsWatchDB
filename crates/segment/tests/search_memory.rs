//! The heap a search of one part may use for the rows of one leaf, counted
//! with a global allocator (which counts the whole process: this file has one
//! test).
//!
//! - Many pieces that share the same rows: the rows collected are sorted and
//!   de-duplicated as they pass twice the limit, so the memory is about twice
//!   the limit, not the sum of the lists.
//! - One piece shared by many more rows than the limit: the query is refused
//!   from the count at the head of the list, before the list is decoded.

use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanId, SpanRecord, TraceId,
    part_dir_name,
};
use nwdb_segment::textindex::parse;
use nwdb_segment::{PartReader, PartWriter, SearchLimits, WriterOptions};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

struct Counting;

fn grow(n: isize) {
    let now = LIVE.fetch_add(n, Ordering::Relaxed) + n;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

#[allow(unsafe_code)]
mod alloc_impl {
    use super::{Counting, LIVE, grow};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::Ordering::Relaxed;

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                grow(l.size() as isize);
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                grow(l.size() as isize);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) };
            LIVE.fetch_sub(l.size() as isize, Relaxed);
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let q = unsafe { System.realloc(p, l, n) };
            if !q.is_null() {
                grow(n as isize - l.size() as isize);
            }
            q
        }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

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

/// A part of `rows` spans, each with `attrs` of `text`.
fn write_part(parts: &Path, id: u64, rows: u64, text: &str) -> PathBuf {
    let options = WriterOptions {
        splitter: Some(Arc::new(Lines)),
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(parts, id, 0, 900_000_000_000, options).unwrap();
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 20,
        max_bytes: 1 << 30,
    });
    for n in 0..rows {
        b.append(&SpanRecord {
            trace_id: TraceId(u128::from(n / 4 + 1).to_be_bytes()),
            span_id: SpanId((n + 1).to_be_bytes()),
            name: "span",
            start_ns: n as i64,
            end_ns: n as i64 + 1,
            attrs: text.as_bytes(),
            ..SpanRecord::default()
        });
    }
    w.write(&b.finish()).unwrap();
    w.finish().unwrap();
    parts.join(part_dir_name(id))
}

/// Runs `search` and returns its result and the peak heap it held above what
/// was live when it began.
fn measure<T>(search: impl FnOnce() -> T) -> (T, isize) {
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);
    let out = search();
    (out, PEAK.load(Ordering::SeqCst) - base)
}

#[test]
fn a_leaf_holds_about_twice_the_limit_in_rows_and_a_long_list_is_refused_undecoded() {
    const KIB: isize = 1024;
    let tmp = tempfile::tempdir().unwrap();

    // 1,000 rows that all hold the same 1,000 pieces, each of which has the
    // word `common`: every piece lists the same 1,000 rows. The leaf produces
    // 1,000 rows, which the limit allows; adding the lists up would hold a
    // million row numbers (4 MB).
    let text: String = (0..1_000).map(|i| format!("common word{i}\n")).collect();
    let shared = write_part(tmp.path(), 1, 1_000, &text);
    let r = PartReader::open(&shared).unwrap();
    let limits = SearchLimits {
        max_leaf_rows: 1_000,
        ..SearchLimits::default()
    };
    let q = parse("common").unwrap();
    let (rows, peak) = measure(|| r.search(&q, &limits).unwrap());
    assert_eq!(rows.len(), 1_000);
    assert!(
        peak < 1_024 * KIB,
        "a leaf of 1,000 rows over 1,000 pieces held {} KiB at its peak",
        peak / KIB
    );
    drop(r);

    // One piece that 100,000 rows share, with a limit of 1,000 rows: decoding
    // the list would take 400 KB before the query was refused.
    let many = write_part(tmp.path(), 2, 100_000, "alpha beta gamma delta");
    let r = PartReader::open(&many).unwrap();
    let limits = SearchLimits {
        max_leaf_rows: 1_000,
        ..SearchLimits::default()
    };
    let q = parse("alpha").unwrap();
    let (result, peak) = measure(|| r.search(&q, &limits));
    match result {
        Err(Error::InvalidArgument(m)) => assert!(m.contains("1000 rows"), "{m}"),
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    assert!(
        peak < 200 * KIB,
        "a list of 100,000 rows was refused only after {} KiB were held",
        peak / KIB
    );
}
