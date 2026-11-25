//! A counting allocator for tests that bound memory. It records live bytes,
//! the peak, and the largest single request, and refuses any single request
//! above `CEILING`, which makes the process abort: a damaged file that asks
//! for an unreasonable buffer fails the test loudly instead of swapping.
#![allow(unsafe_code, dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub static LIVE: AtomicUsize = AtomicUsize::new(0);
pub static PEAK: AtomicUsize = AtomicUsize::new(0);
pub static MAX_SINGLE: AtomicUsize = AtomicUsize::new(0);

/// No test in this crate needs a single allocation this large.
pub const CEILING: usize = 64 * 1024 * 1024;

pub struct Counting;

fn grew(by: usize) {
    let now = LIVE.fetch_add(by, Relaxed) + by;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() > CEILING {
            return std::ptr::null_mut();
        }
        MAX_SINGLE.fetch_max(l.size(), Relaxed);
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            grew(l.size());
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
            grew(l.size());
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Relaxed);
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if n > CEILING {
            return std::ptr::null_mut();
        }
        MAX_SINGLE.fetch_max(n, Relaxed);
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            if n >= l.size() {
                grew(n - l.size());
            } else {
                LIVE.fetch_sub(l.size() - n, Relaxed);
            }
        }
        q
    }
}

pub fn live() -> usize {
    LIVE.load(Relaxed)
}

pub fn peak() -> usize {
    PEAK.load(Relaxed)
}

pub fn max_single() -> usize {
    MAX_SINGLE.load(Relaxed)
}

/// Starts a new peak measurement from the bytes live now.
pub fn reset_peak() {
    PEAK.store(LIVE.load(Relaxed), Relaxed);
}
