//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use nwdb_wal::{SyncMode, Wal, WalOptions};

/// Options with no fsync, a 1 ms sync interval and the given file size bound.
pub fn opts(max_file_bytes: u64) -> WalOptions {
    WalOptions {
        max_file_bytes,
        sync_interval: Duration::from_millis(1),
        sync: SyncMode::None,
        ..WalOptions::default()
    }
}

/// Appends `n` 30-byte records, then shuts the log down.
pub fn fill(dir: &Path, o: &WalOptions, n: u8) {
    let (wal, _) = Wal::open(dir, o.clone()).unwrap();
    for i in 0..n {
        wal.append_blocking(&[i; 30]).unwrap();
    }
    wal.shutdown().unwrap();
}
