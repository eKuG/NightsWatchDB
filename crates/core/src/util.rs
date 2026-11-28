//! Small helpers that several crates need.

use std::sync::{Mutex, MutexGuard};

/// Appends `v` as a little-endian base-128 varint (the protobuf encoding).
#[inline]
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Locks, ignoring poisoning. For a lock whose holders never leave the data
/// half updated: a panic elsewhere must not stop callers.
pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_bytes() {
        let enc = |v| {
            let mut out = Vec::new();
            put_varint(&mut out, v);
            out
        };
        assert_eq!(enc(0), [0]);
        assert_eq!(enc(127), [127]);
        assert_eq!(enc(128), [0x80, 1]);
        assert_eq!(enc(300), [0xac, 2]);
        assert_eq!(enc(u64::MAX).len(), 10);
    }

    #[test]
    fn lock_survives_poisoning() {
        let m = std::sync::Arc::new(Mutex::new(1));
        let m2 = m.clone();
        let _ = std::thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("poison");
        })
        .join();
        assert_eq!(*lock(&m), 1);
    }
}
