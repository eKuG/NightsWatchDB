//! A counting allocator for tests that bound memory. A test binary declares
//! its own `#[global_allocator] static ALLOC: Counting = Counting;` and reads
//! the counters with [`live`], [`peak`], [`max_single`], [`reset_peak`] and
//! [`allocations`].
//!
//! The ceiling is off (`usize::MAX`) unless the binary names a value:
//! `Counting<{ 64 << 20 }>` refuses any single request above it, which makes
//! the process abort, so a damaged file that asks for an unreasonable buffer
//! fails the test loudly instead of swapping.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicIsize, AtomicUsize};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
static MAX_SINGLE: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

/// The counting allocator; see the module docs for `CEILING`.
pub struct Counting<const CEILING: usize = { usize::MAX }>;

fn grew(by: isize) {
    ALLOCATIONS.fetch_add(usize::from(by > 0), Relaxed);
    let now = LIVE.fetch_add(by, Relaxed) + by;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl<const CEILING: usize> GlobalAlloc for Counting<CEILING> {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() > CEILING {
            return std::ptr::null_mut();
        }
        MAX_SINGLE.fetch_max(l.size(), Relaxed);
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            grew(l.size() as isize);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() > CEILING {
            return std::ptr::null_mut();
        }
        MAX_SINGLE.fetch_max(l.size(), Relaxed);
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            grew(l.size() as isize);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as isize, Relaxed);
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if n > CEILING {
            return std::ptr::null_mut();
        }
        MAX_SINGLE.fetch_max(n, Relaxed);
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            grew(n as isize - l.size() as isize);
        }
        q
    }
}

/// Bytes live now.
pub fn live() -> isize {
    LIVE.load(SeqCst)
}

/// The most bytes that were live at once since the last [`reset_peak`].
pub fn peak() -> isize {
    PEAK.load(SeqCst)
}

/// The largest single request made so far.
pub fn max_single() -> usize {
    MAX_SINGLE.load(SeqCst)
}

/// Allocations and growing reallocations made so far.
pub fn allocations() -> usize {
    ALLOCATIONS.load(SeqCst)
}

/// Starts a new peak measurement from the bytes live now.
pub fn reset_peak() {
    PEAK.store(LIVE.load(SeqCst), SeqCst);
}
