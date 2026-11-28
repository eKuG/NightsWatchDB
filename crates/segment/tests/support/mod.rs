//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use nwdb_core::Error;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

/// `len` bytes from the generator seeded with `seed`.
pub fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// The message of a [`Error::Corrupt`]; any other error panics.
pub fn corrupt(e: Error) -> String {
    match e {
        Error::Corrupt(m) => m,
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// Flips bit `bit` of the byte at `at` of `path`, in place.
pub fn flip(path: &Path, at: u64, bit: u8) {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0u8];
    f.read_exact_at(&mut byte, at).unwrap();
    byte[0] ^= 1 << bit;
    f.write_all_at(&byte, at).unwrap();
}
