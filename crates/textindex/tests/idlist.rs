//! The id-list store: round trips over every list shape, group boundaries,
//! errors, and compactness. Damage is tested in `damage.rs`.

mod support;

use nwdb_core::Error;
use nwdb_textindex::{IdListReader, IdListWriter};
use rand::Rng;
use rand::rngs::StdRng;
use support::{CountingSource, rng};

fn write(lists: &[Vec<u32>]) -> (Vec<u8>, nwdb_textindex::IdListMeta) {
    let mut out = Vec::new();
    let mut w = IdListWriter::new(&mut out);
    for l in lists {
        w.push(l).unwrap();
    }
    let meta = w.finish().unwrap();
    assert_eq!(meta.bytes, out.len() as u64);
    (out, meta)
}

fn random_list(rng: &mut StdRng) -> Vec<u32> {
    let shapes = [0usize, 1, 2, 8, 9, 127, 128, 129, 255, 256, 257, 1000];
    let n = if rng.random_bool(0.8) {
        shapes[rng.random_range(0..shapes.len())]
    } else {
        rng.random_range(0..3000)
    };
    match rng.random_range(0..4) {
        // a run of consecutive ids: width zero
        0 => {
            let start = rng.random_range(0..1_000_000);
            (start..start + n as u32).collect()
        }
        // small gaps
        1 => gaps(rng, n, 4),
        // wide gaps
        2 => gaps(rng, n, 1 << 20),
        // the end of the id space
        _ => {
            let n = n.min(500) as u32;
            (u32::MAX - n..u32::MAX)
                .chain([u32::MAX])
                .collect::<Vec<_>>()[..n as usize]
                .to_vec()
        }
    }
}

fn gaps(rng: &mut StdRng, n: usize, max_gap: u32) -> Vec<u32> {
    let mut v = Vec::with_capacity(n);
    let mut next = 0u64;
    for _ in 0..n {
        let id = next + u64::from(rng.random_range(0..max_gap));
        if id > u64::from(u32::MAX) {
            break;
        }
        v.push(id as u32);
        next = id + 1;
    }
    v
}

#[test]
fn every_list_shape_round_trips() {
    let mut rng = rng(1);
    let lists: Vec<Vec<u32>> = (0..4000).map(|_| random_list(&mut rng)).collect();
    let (bytes, meta) = write(&lists);
    assert_eq!(meta.keys, 4000);
    assert_eq!(meta.ids, lists.iter().map(|l| l.len() as u64).sum::<u64>());
    let r = IdListReader::open(bytes.as_slice()).unwrap();
    assert_eq!(r.keys(), 4000);
    assert_eq!(r.meta(), &meta);
    // in order, then in random order, so groups are read both ways
    for (k, l) in lists.iter().enumerate() {
        assert_eq!(&r.get(k as u32).unwrap(), l, "key {k}");
    }
    for _ in 0..2000 {
        let k = rng.random_range(0..lists.len());
        assert_eq!(r.get(k as u32).unwrap(), lists[k]);
    }
}

#[test]
fn group_boundaries() {
    // 128 keys close a group; so do about 32 KiB of lists
    for n in [1usize, 127, 128, 129, 255, 256, 257, 1000] {
        let lists: Vec<Vec<u32>> = (0..n).map(|k| vec![k as u32, k as u32 + 7]).collect();
        let (bytes, _) = write(&lists);
        let r = IdListReader::open(bytes.as_slice()).unwrap();
        for (k, l) in lists.iter().enumerate() {
            assert_eq!(&r.get(k as u32).unwrap(), l, "n {n} key {k}");
        }
    }
    let big: Vec<Vec<u32>> = (0..40u32)
        .map(|k| (0..6000).map(|i| i * 40 + k).collect())
        .collect();
    let (bytes, _) = write(&big);
    let r = IdListReader::open(bytes.as_slice()).unwrap();
    for (k, l) in big.iter().enumerate() {
        assert_eq!(&r.get(k as u32).unwrap(), l);
    }
}

#[test]
fn degenerate_files_and_errors() {
    let (bytes, meta) = write(&[]);
    assert_eq!((meta.keys, meta.ids), (0, 0));
    let r = IdListReader::open(bytes.as_slice()).unwrap();
    assert!(matches!(r.get(0), Err(Error::NotFound(_))));

    let (bytes, _) = write(&[vec![], vec![], vec![3]]);
    let r = IdListReader::open(bytes.as_slice()).unwrap();
    assert_eq!(r.get(0).unwrap(), Vec::<u32>::new());
    assert_eq!(r.get(2).unwrap(), [3]);
    assert!(matches!(r.get(3), Err(Error::NotFound(_))));
    assert!(matches!(r.get(u32::MAX), Err(Error::NotFound(_))));

    // an unordered list is refused and the writer carries on
    let mut out = Vec::new();
    let mut w = IdListWriter::new(&mut out);
    w.push(&[1, 2]).unwrap();
    for bad in [&[2u32, 2][..], &[5, 4], &[1, 3, 2]] {
        assert!(matches!(w.push(bad), Err(Error::InvalidArgument(_))));
    }
    w.push(&[9]).unwrap();
    assert_eq!(w.finish().unwrap().keys, 2);
    let r = IdListReader::open(out.as_slice()).unwrap();
    assert_eq!(r.get(1).unwrap(), [9]);
}

#[test]
fn lists_are_packed() {
    // one id per key, typical of a piece used by one row
    let lists: Vec<Vec<u32>> = (0..20_000u32).map(|k| vec![k * 3]).collect();
    let (bytes, _) = write(&lists);
    assert!(bytes.len() < 20_000 * 6, "{} bytes", bytes.len());
    // a million consecutive ids: width zero, one byte per 128 ids
    let run: Vec<u32> = (500..1_000_500).collect();
    let (bytes, _) = write(std::slice::from_ref(&run));
    assert!(bytes.len() < 12_000, "{} bytes", bytes.len());
    let r = IdListReader::open(bytes.as_slice()).unwrap();
    assert_eq!(r.get(0).unwrap(), run);
    // 100,000 ids with gaps under 16: about 4 bits each, not 32
    let mut rng = rng(2);
    let dense = gaps(&mut rng, 100_000, 16);
    let (bytes, _) = write(std::slice::from_ref(&dense));
    assert!(bytes.len() < dense.len() * 3 / 5, "{} bytes", bytes.len());
}

#[test]
fn a_file_reader_serves_many_threads() {
    use std::io::Write;
    let mut rng = rng(3);
    let lists: Vec<Vec<u32>> = (0..1500).map(|_| random_list(&mut rng)).collect();
    let (bytes, _) = write(&lists);
    let path = std::env::temp_dir().join(format!("nwdb-idlist-test-{}", std::process::id()));
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
    let r = IdListReader::open(std::fs::File::open(&path).unwrap()).unwrap();
    std::thread::scope(|s| {
        for t in 0..4usize {
            let (r, lists) = (&r, &lists);
            s.spawn(move || {
                for k in (t..lists.len()).step_by(4) {
                    assert_eq!(r.get(k as u32).unwrap(), lists[k]);
                }
            });
        }
    });
    std::fs::remove_file(&path).unwrap();
}

/// Two groups of 128 keys, in the lists `base + key` and `base + key + 7`. The
/// ids all take two bytes as varints, so a file built with another `base` has
/// groups of the same lengths.
fn two_groups(base: u32) -> Vec<u8> {
    let lists: Vec<Vec<u32>> = (0..256u32).map(|k| vec![base + k, base + k + 7]).collect();
    write(&lists).0
}

/// Where the second group lies in the file: `(start, end)`.
fn second_group(file: &[u8]) -> (usize, usize) {
    let f = file.len() - 40;
    let groups = u32::from_le_bytes(file[f + 4..f + 8].try_into().unwrap());
    assert_eq!(groups, 2);
    let index = u64::from_le_bytes(file[f + 24..f + 32].try_into().unwrap()) as usize;
    let start = u64::from_le_bytes(file[index + 16 + 4..index + 16 + 12].try_into().unwrap());
    (start as usize, index)
}

fn corrupt_or_panic<T: std::fmt::Debug>(r: nwdb_core::Result<T>, what: &str) {
    assert!(matches!(r, Err(Error::Corrupt(_))), "{what}: {r:?}");
}

#[test]
fn a_flipped_byte_in_one_group_is_corrupt_when_that_group_is_read() {
    let clean = two_groups(1000);
    let r = IdListReader::open(clean.as_slice()).unwrap();
    for k in [0u32, 127, 128, 255] {
        assert_eq!(
            r.get(k).unwrap(),
            [1000 + k, 1007 + k],
            "clean file, key {k}"
        );
    }
    let (start, end) = second_group(&clean);
    // The last four bytes of a group are its own checksum.
    for at in [start, start + 5, end - 5, end - 1] {
        let mut v = clean.clone();
        v[at] ^= 0x01;
        // The file opens: the group index and footer are intact.
        let r = IdListReader::open(v.as_slice()).unwrap();
        assert_eq!(r.get(0).unwrap(), [1000, 1007], "the first group is fine");
        assert_eq!(r.get(127).unwrap(), [1127, 1134]);
        corrupt_or_panic(r.get(128), &format!("byte {at}, key 128"));
        corrupt_or_panic(r.get(255), &format!("byte {at}, key 255"));
    }
}

#[test]
fn a_group_of_another_file_with_its_own_valid_checksum_is_corrupt() {
    let a = two_groups(1000);
    let b = two_groups(2000);
    let (start, end) = second_group(&a);
    assert_eq!(second_group(&b), (start, end), "the files have one layout");
    let mut mixed = a.clone();
    mixed[start..end].copy_from_slice(&b[start..end]);
    let r = IdListReader::open(mixed.as_slice()).unwrap();
    assert_eq!(r.get(5).unwrap(), [1005, 1012]);
    // The group is whole and its own checksum holds; the group index of this
    // file records the checksum of the group it wrote.
    corrupt_or_panic(r.get(128), "a group from another file");
}

/// Keys of one group, asked for in ascending order, cost one read of the
/// group, however many they are; a key of the next group costs one more.
#[test]
fn for_each_reads_a_group_once_for_all_its_keys() {
    let file = two_groups(1000);
    let src = CountingSource::new(file);
    let r = IdListReader::open(&src).unwrap();
    src.take_calls();
    let mut seen = Vec::new();
    r.for_each(0..128, |k, list| {
        seen.push((k, list.decode()?));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen.len(), 128);
    assert_eq!(src.take_calls(), 1, "128 keys of one group");
    r.for_each(0..256, |_, _| Ok(())).unwrap();
    assert_eq!(src.take_calls(), 2, "256 keys of two groups");
    // Asked one key at a time, the same keys read the group each time.
    for k in 0..4 {
        r.get(k).unwrap();
    }
    assert_eq!(src.take_calls(), 4);
}
