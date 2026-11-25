//! Damaged files. Truncating at every length, flipping every bit of a small
//! file and a sample of a large one, and random bytes must give
//! `Error::Corrupt`: never a panic, never a read outside the file, never a
//! huge allocation. The allocator in this binary refuses any single
//! allocation above 64 MiB, which aborts the process, and the tests assert the
//! largest request stays far below that.
//!
//! A second kind of test repairs the checksums after damaging a byte, so the
//! checksums cannot be what catches it: the structural checks must hold the
//! line alone, and the answer is either success or `Corrupt`.

#[path = "support/alloc.rs"]
mod alloc;
mod support;

use nwdb_core::{Error, Result};
use nwdb_textindex::{IdListReader, IdListWriter, IndexReader};
use rand::Rng;
use support::*;

#[global_allocator]
static ALLOC: alloc::Counting = alloc::Counting;

/// Uses every part of an index: opens it, looks up every word, scans every
/// posting list, intersects, and verifies all checksums.
fn exercise(bytes: &[u8], words: &[String]) -> Result<()> {
    let r = IndexReader::open(bytes)?;
    for w in words {
        r.term(w)?;
    }
    r.prefix("", usize::MAX)?;
    r.prefix("w0", usize::MAX)?;
    r.intersect(&[&words[0], &words[1]])?;
    r.intersect(&[&words[0], &words[1], &words[2]])?;
    r.verify()
}

fn must_be_corrupt(r: Result<()>, what: &str) {
    match r {
        Err(Error::Corrupt(_)) => {}
        other => panic!("{what}: expected Corrupt, got {other:?}"),
    }
}

fn small_index() -> (Vec<u8>, Vec<String>) {
    let mut rng = rng(1);
    let words: Vec<String> = (0..24).map(vocab_word).collect();
    // 300 documents, several words each: some lists have more than 128 ids,
    // some are inline, and the tiny budget makes three or four segments.
    let docs = random_docs(&mut rng, 300, 2..6, |r| {
        words[r.random_range(0..24)].clone()
    });
    let (idx, meta) = build(&docs, 4_000);
    assert!(
        meta.segments >= 2 && idx.len() < 8_000,
        "{} bytes",
        idx.len()
    );
    (idx, words)
}

fn large_index() -> (Vec<u8>, Vec<String>) {
    let mut rng = rng(2);
    let words: Vec<String> = (0..3000).map(|i| format!("w{i}")).collect();
    let docs = random_docs(&mut rng, 1500, 10..50, |r| {
        let i = if r.random_bool(0.5) {
            r.random_range(0..30)
        } else {
            r.random_range(0..3000)
        };
        words[i].clone()
    });
    let (idx, meta) = build(&docs, 60_000);
    assert!(meta.segments >= 3, "{} segments", meta.segments);
    (idx, words)
}

fn flip(b: &[u8], bit: usize) -> Vec<u8> {
    let mut v = b.to_vec();
    v[bit / 8] ^= 1 << (bit % 8);
    v
}

fn assert_small_allocations() {
    assert!(
        alloc::max_single() < 8 << 20,
        "a single allocation of {} bytes",
        alloc::max_single()
    );
}

#[test]
fn every_truncation_is_corrupt() {
    for (idx, words) in [small_index(), large_index()] {
        for len in 0..idx.len() {
            must_be_corrupt(
                exercise(&idx[..len], &words),
                &format!("truncated to {len}"),
            );
        }
        exercise(&idx, &words).unwrap();
    }
    assert_small_allocations();
}

#[test]
fn every_bit_flip_in_a_small_index_is_corrupt() {
    let (idx, words) = small_index();
    for bit in 0..idx.len() * 8 {
        must_be_corrupt(exercise(&flip(&idx, bit), &words), &format!("bit {bit}"));
    }
    assert_small_allocations();
}

#[test]
fn sampled_bit_flips_in_a_large_index_are_corrupt() {
    let (idx, words) = large_index();
    let mut rng = rng(3);
    let nbits = idx.len() * 8;
    // the first and last 300 bytes (the first segment's start, the footer),
    // plus random bits anywhere
    let mut bits: Vec<usize> = (0..2400).chain(nbits - 2400..nbits).collect();
    bits.extend((0..2500).map(|_| rng.random_range(0..nbits)));
    for bit in bits {
        must_be_corrupt(exercise(&flip(&idx, bit), &words), &format!("bit {bit}"));
    }
    assert_small_allocations();
}

#[test]
fn random_bytes_are_corrupt() {
    let words: Vec<String> = (0..24).map(vocab_word).collect();
    let mut rng = rng(4);
    for _ in 0..3000 {
        let n = rng.random_range(0..3000);
        let b: Vec<u8> = (0..n).map(|_| rng.random()).collect();
        must_be_corrupt(exercise(&b, &words), "random bytes");
    }
    // random bytes behind a genuine footer
    let (idx, _) = small_index();
    for _ in 0..500 {
        let mut b: Vec<u8> = (0..rng.random_range(0..2000))
            .map(|_| rng.random())
            .collect();
        b.extend_from_slice(&idx[idx.len() - 64..]);
        must_be_corrupt(exercise(&b, &words), "random bytes, real footer");
    }
    assert_small_allocations();
}

fn le64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        b.get(at..at.checked_add(8)?)?.try_into().ok()?,
    ))
}

/// Recomputes every checksum of an index file from the documented layout,
/// skipping anything the damaged fields no longer describe.
fn refix_index(b: &mut [u8]) {
    let Some(f) = b.len().checked_sub(64) else {
        return;
    };
    let segs = u32::from_le_bytes(b[f + 4..f + 8].try_into().unwrap()) as usize;
    let Some(dir_off) = le64(b, f + 48).and_then(|x| usize::try_from(x).ok()) else {
        return;
    };
    if segs.checked_mul(72).and_then(|d| d.checked_add(dir_off)) != Some(f) {
        return;
    }
    for i in 0..segs {
        let e = dir_off + i * 72;
        let (Some(p), Some(d), Some(s), Some(o)) =
            (le64(b, e), le64(b, e + 8), le64(b, e + 16), le64(b, e + 64))
        else {
            continue;
        };
        let (Some(body_end), Some(sparse_end)) = (
            o.checked_add(p).and_then(|x| x.checked_add(d)),
            o.checked_add(p)
                .and_then(|x| x.checked_add(d))
                .and_then(|x| x.checked_add(s)),
        ) else {
            continue;
        };
        if sparse_end > dir_off as u64 {
            continue;
        }
        let body = crc32c::crc32c(&b[o as usize..body_end as usize]);
        let sparse = crc32c::crc32c(&b[body_end as usize..sparse_end as usize]);
        b[e + 56..e + 60].copy_from_slice(&body.to_le_bytes());
        b[e + 60..e + 64].copy_from_slice(&sparse.to_le_bytes());
    }
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b[dir_off..f]), &b[f..f + 56]);
    b[f + 56..f + 60].copy_from_slice(&crc.to_le_bytes());
}

fn structural_only(
    idx: &[u8],
    words: &[String],
    bits: impl Iterator<Item = usize>,
) -> (usize, usize) {
    let (mut ok, mut corrupt) = (0, 0);
    for bit in bits {
        let mut v = flip(idx, bit);
        refix_index(&mut v);
        match exercise(&v, words) {
            Ok(()) => ok += 1,
            Err(Error::Corrupt(_)) => corrupt += 1,
            other => panic!("bit {bit} with repaired checksums: {other:?}"),
        }
    }
    (ok, corrupt)
}

#[test]
fn damage_with_repaired_checksums_is_ok_or_corrupt() {
    let (idx, words) = small_index();
    let (ok, corrupt) = structural_only(&idx, &words, 0..idx.len() * 8);
    // many single-bit changes break a structural rule; some (an id changed to
    // another id inside the range, a padding bit) are legal
    assert!(corrupt > 200, "ok {ok}, corrupt {corrupt}");
    let (idx, words) = large_index();
    let mut rng = rng(6);
    let nbits = idx.len() * 8;
    let bits: Vec<usize> = (0..600)
        .chain(nbits - 600..nbits)
        .chain((0..500).map(|_| rng.random_range(0..nbits)))
        .collect();
    let (ok, corrupt) = structural_only(&idx, &words[..40], bits.into_iter());
    assert!(
        corrupt > 0 && ok + corrupt == 1700,
        "ok {ok}, corrupt {corrupt}"
    );
    assert_small_allocations();
}

#[test]
fn byte_level_damage_with_repaired_checksums_is_ok_or_corrupt() {
    let (idx, words) = small_index();
    let mut rng = rng(7);
    for _ in 0..6000 {
        let mut v = idx.clone();
        for _ in 0..rng.random_range(1..4) {
            let at = rng.random_range(0..v.len());
            v[at] = match rng.random_range(0..4) {
                0 => 0,
                1 => 0xff,
                2 => v[at].wrapping_add(1),
                _ => rng.random(),
            };
        }
        refix_index(&mut v);
        match exercise(&v, &words) {
            Ok(()) | Err(Error::Corrupt(_)) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_small_allocations();
}

// ------------------------------------------------------ id-list store

fn lists() -> Vec<Vec<u32>> {
    let mut rng = rng(8);
    (0..400u32)
        .map(|k| match k % 7 {
            0 => vec![],
            1 => vec![rng.random_range(0..1000)],
            2 => (0..rng.random_range(2..12)).map(|i| i * 5 + k).collect(),
            3 => (k..k + 150).collect(),
            4 => {
                let mut v: Vec<u32> = (0..rng.random_range(9..300))
                    .map(|_| rng.random_range(0..100_000))
                    .collect();
                v.sort_unstable();
                v.dedup();
                v
            }
            5 => vec![u32::MAX - 1, u32::MAX],
            _ => (0..40).map(|i| i * 1000).collect(),
        })
        .collect()
}

fn idlist_file(lists: &[Vec<u32>]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = IdListWriter::new(&mut out);
    for l in lists {
        w.push(l).unwrap();
    }
    w.finish().unwrap();
    out
}

/// Opens the file and reads every key; returns the lists that were read
/// correctly or the first error.
fn read_all(b: &[u8], want: &[Vec<u32>]) -> Result<()> {
    let r = IdListReader::open(b)?;
    let mut first = Ok(());
    for (k, w) in want.iter().enumerate() {
        match r.get(k as u32) {
            Ok(got) => assert_eq!(&got, w, "key {k} read wrongly from a damaged file"),
            Err(e) if first.is_ok() => first = Err(e),
            Err(_) => {}
        }
    }
    first
}

/// Recomputes the group and footer checksums of an id-list file.
fn refix_idlist(b: &mut [u8]) {
    let Some(f) = b.len().checked_sub(40) else {
        return;
    };
    let groups = u32::from_le_bytes(b[f + 4..f + 8].try_into().unwrap()) as usize;
    let Some(index_off) = le64(b, f + 24).and_then(|x| usize::try_from(x).ok()) else {
        return;
    };
    if groups
        .checked_mul(12)
        .and_then(|d| d.checked_add(index_off))
        != Some(f)
    {
        return;
    }
    let starts: Vec<Option<usize>> = (0..groups)
        .map(|g| le64(b, index_off + g * 12 + 4).and_then(|x| usize::try_from(x).ok()))
        .collect();
    for g in 0..groups {
        let end = if g + 1 < groups {
            starts[g + 1]
        } else {
            Some(index_off)
        };
        if let (Some(s), Some(e)) = (starts[g], end)
            && s + 4 <= e
            && e <= index_off
        {
            let crc = crc32c::crc32c(&b[s..e - 4]);
            b[e - 4..e].copy_from_slice(&crc.to_le_bytes());
        }
    }
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b[index_off..f]), &b[f..f + 32]);
    b[f + 32..f + 36].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn id_list_truncation_and_bit_flips_are_corrupt() {
    let all = lists();
    let big = idlist_file(&all);
    read_all(&big, &all).unwrap();
    for len in 0..big.len() {
        must_be_corrupt(read_all(&big[..len], &all), &format!("truncated to {len}"));
    }
    // flip every bit of a smaller file, a sample of the larger one
    let small_lists = &all[..70];
    let small = idlist_file(small_lists);
    for bit in 0..small.len() * 8 {
        must_be_corrupt(
            read_all(&flip(&small, bit), small_lists),
            &format!("bit {bit}"),
        );
    }
    let mut rng = rng(9);
    let nbits = big.len() * 8;
    for bit in (0..1500)
        .chain(nbits - 1500..nbits)
        .chain((0..2000).map(|_| rng.random_range(0..nbits)))
    {
        must_be_corrupt(read_all(&flip(&big, bit), &all), &format!("bit {bit}"));
    }
    assert_small_allocations();
}

#[test]
fn id_list_damage_with_repaired_checksums_is_ok_or_corrupt() {
    let all = lists();
    let small_lists = &all[..70];
    let small = idlist_file(small_lists);
    let mut rng = rng(10);
    let mut corrupt = 0;
    for bit in 0..small.len() * 8 {
        let mut v = flip(&small, bit);
        refix_idlist(&mut v);
        let r = match IdListReader::open(v.as_slice()) {
            Ok(r) => (0..70u32).try_for_each(|k| r.get(k).map(|_| ())),
            Err(e) => Err(e),
        };
        match r {
            Ok(()) => {}
            Err(Error::Corrupt(_)) => corrupt += 1,
            other => panic!("bit {bit}: {other:?}"),
        }
    }
    assert!(corrupt > 100);
    let big = idlist_file(&all);
    for _ in 0..3000 {
        let mut v = big.clone();
        let at = rng.random_range(0..v.len());
        v[at] = rng.random();
        refix_idlist(&mut v);
        if let Ok(r) = IdListReader::open(v.as_slice()) {
            for k in (0..400).step_by(7) {
                match r.get(k) {
                    Ok(_) | Err(Error::Corrupt(_)) => {}
                    other => panic!("{other:?}"),
                }
            }
        }
    }
    // random bytes
    for _ in 0..2000 {
        let b: Vec<u8> = (0..rng.random_range(0..500))
            .map(|_| rng.random())
            .collect();
        assert!(matches!(
            IdListReader::open(b.as_slice()),
            Err(Error::Corrupt(_))
        ));
    }
    assert_small_allocations();
}
