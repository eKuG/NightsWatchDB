//! The counting allocator of these tests, from the shared test crate, with
//! the ceiling these tests rely on: any single request above `CEILING`
//! returns null, which makes the process abort, so a damaged file that asks
//! for an unreasonable buffer fails the test loudly instead of swapping.
#![allow(dead_code, unused_imports)]

pub use nwdb_testkit::alloc::{Counting as Counter, max_single, reset_peak};

/// No test in this crate needs a single allocation this large.
pub const CEILING: usize = 64 * 1024 * 1024;

/// Each test binary declares `static ALLOC: alloc::Counting = alloc::Counter;`.
pub type Counting = Counter<CEILING>;

pub fn live() -> usize {
    nwdb_testkit::alloc::live() as usize
}

pub fn peak() -> usize {
    nwdb_testkit::alloc::peak() as usize
}
