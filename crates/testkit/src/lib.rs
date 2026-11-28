//! Test support shared by the crates of this workspace. It is a
//! dev-dependency only: nothing that ships depends on it.

pub mod alloc;
pub mod fs;
pub mod mem;
pub mod proto;
mod wait;

pub use wait::wait_for;
