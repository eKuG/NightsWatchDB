//! Memory needed to render a raw column as JSON, counted with a global
//! allocator. A single span holds as many events as fit in a 64 MiB request,
//! so the streaming writers must need a small constant amount of heap whatever
//! the size of the column. This is the only test in the file because the
//! allocator counts the whole process.

mod common;

use common::{concat, ld, varint, vf};

use nwdb_otlp::{write_attrs, write_events, write_links, write_resource};
use std::io::{self, Write};
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

/// Takes the JSON and counts its bytes.
struct Count(usize);

impl Write for Count {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 += b.len();
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Peak heap use above what was live before, and the bytes written.
fn measure(f: impl FnOnce(&mut dyn Write) -> io::Result<()>) -> (isize, usize) {
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);
    let mut c = Count(0);
    f(&mut c).unwrap();
    (PEAK.load(Ordering::SeqCst) - base, c.0)
}

fn prefixed(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for m in items {
        out.extend(varint(m.len() as u64));
        out.extend_from_slice(m);
    }
    out
}

fn repeat(item: &[u8], bytes: usize) -> Vec<u8> {
    let one = prefixed(&[item.to_vec()]);
    one.repeat(bytes / one.len())
}

fn kv(key: &[u8], value: &[u8]) -> Vec<u8> {
    concat(&[&ld(1, key), &ld(2, value)])
}

/// An `AnyValue` holding `levels` arrays inside each other, around an int.
fn nested(levels: usize) -> Vec<u8> {
    let mut v = vf(3, 1);
    for _ in 0..levels {
        v = ld(5, &ld(1, &v));
    }
    v
}

#[test]
fn the_json_writers_need_a_constant_amount_of_memory() {
    const LIMIT: isize = 64 * 1024;
    let mb = 1 << 20;

    // Events: 20,000 empty ones, then a few megabytes with attributes.
    let empty = vec![0u8; 20_000];
    let attrs: Vec<u8> = (0..5)
        .flat_map(|i| {
            ld(
                3,
                &kv(format!("key{i}").as_bytes(), &ld(1, b"some text value")),
            )
        })
        .collect();
    let event = concat(&[&ld(2, b"an event"), &attrs, &vf(4, 2)]);
    let events = repeat(&event, 6 * mb);
    let links = repeat(
        &concat(&[&ld(1, &[7; 16]), &ld(2, &[8; 8]), &attrs]),
        6 * mb,
    );
    // Values nested as deeply as accepted, many times, and one hostile column
    // far deeper than that.
    let deep = repeat(&kv(b"deep", &nested(63)), 6 * mb);
    let hostile = prefixed(&[kv(b"deep", &nested(20_000))]);
    // One string of several megabytes, valid and not.
    let long = prefixed(&[kv(b"long", &ld(1, &vec![b'a'; 6 * mb]))]);
    let broken = prefixed(&[kv(b"long", &ld(1, &vec![0xff; 6 * mb]))]);
    // Bytes that are not messages at all.
    let junk = vec![0xffu8; 6 * mb];
    // A resource with many attributes.
    let resource = repeat_field(&kv(b"k", &ld(1, b"v")), 1, 6 * mb);

    type W = fn(&mut dyn Write, &[u8]) -> io::Result<()>;
    let cases: Vec<(&str, W, &[u8])> = vec![
        ("empty events", write_events, &empty),
        ("events", write_events, &events),
        ("links", write_links, &links),
        ("deeply nested attributes", write_attrs, &deep),
        ("hostile nesting", write_attrs, &hostile),
        ("a long string", write_attrs, &long),
        ("a long string that is not UTF-8", write_attrs, &broken),
        ("junk as attributes", write_attrs, &junk),
        ("junk as events", write_events, &junk),
        ("resource", write_resource, &resource),
    ];
    for (what, f, column) in cases {
        let (peak, written) = measure(|out| f(out, column));
        eprintln!(
            "{what}: {} input bytes, peak heap {peak} bytes",
            column.len()
        );
        assert!(
            peak <= LIMIT,
            "{what}: {} bytes of input needed {peak} bytes of heap to render {written} bytes",
            column.len()
        );
    }
}

/// `n` copies of one field of a message, filling `bytes`.
fn repeat_field(item: &[u8], field: u32, bytes: usize) -> Vec<u8> {
    let one = ld(field, item);
    one.repeat(bytes / one.len())
}
