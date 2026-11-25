//! Behaviour of the manifest through its public API only.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use nwdb_core::{Error, PartMeta, part_dir_name};
use nwdb_manifest::{Edit, Manifest, ManifestOptions, ManifestState, unreferenced_part_dirs};

fn part(id: u64) -> PartMeta {
    PartMeta {
        part_id: id,
        level: 0,
        window_start_ns: 0,
        window_ns: 900_000_000_000,
        rows: id,
        traces: 1,
        min_start_ns: 1,
        max_end_ns: 2,
        files: BTreeMap::from([("spans.parquet".to_string(), id.wrapping_mul(10))]),
        created_unix_ms: 1_700_000_000_000,
        format_version: 1,
    }
}

fn add(ids: &[u64]) -> Edit {
    Edit {
        add: ids.iter().map(|&i| part(i)).collect(),
        ..Edit::default()
    }
}

fn open(dir: &Path) -> Manifest {
    Manifest::open(dir, ManifestOptions::default()).unwrap()
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn vname(v: u64) -> String {
    format!("{v:020}.json")
}

fn ids(s: &ManifestState) -> Vec<u64> {
    s.parts.iter().map(|p| p.part_id).collect()
}

/// Bit-at-a-time CRC-32C, independent of the crate under test.
fn crc32c_reference(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[test]
fn empty_commit_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");
    let m = open(&dir);
    let s0 = m.current();
    assert_eq!(*s0, ManifestState::empty());
    assert_eq!((s0.version, s0.next_part_id, s0.format_version), (0, 1, 1));
    assert!(file_names(&dir).is_empty());

    let s1 = m.commit(add(&[1, 2])).unwrap();
    assert_eq!(s1.version, 1);
    assert_eq!(s1.next_part_id, 3);
    assert_eq!(file_names(&dir), [vname(1)]);
    assert_eq!(*m.current(), *s1);

    let again = open(&dir);
    assert_eq!(*again.current(), *s1);
}

#[test]
fn known_answer_file_content_and_unknown_fields() {
    let state = ManifestState {
        format_version: 1,
        version: 3,
        next_part_id: 8,
        wal_flushed_lsn: 77,
        parts: vec![part(7)],
        created_unix_ms: 1_700_000_000_123,
    };
    let json = concat!(
        r#"{"format_version":1,"version":3,"next_part_id":8,"wal_flushed_lsn":77,"#,
        r#""parts":[{"part_id":7,"level":0,"window_start_ns":0,"window_ns":900000000000,"#,
        r#""rows":7,"traces":1,"min_start_ns":1,"max_end_ns":2,"files":{"spans.parquet":70},"#,
        r#""created_unix_ms":1700000000000,"format_version":1}],"created_unix_ms":1700000000123}"#,
    );
    let bytes = state.encode().unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let (line1, rest) = text.split_once('\n').unwrap();
    assert_eq!(line1, json);
    assert_eq!(
        rest,
        format!("crc32c:{:08x}\n", crc32c_reference(json.as_bytes()))
    );
    assert_eq!(rest, "crc32c:06d79320\n");
    assert_eq!(ManifestState::decode("x", &bytes).unwrap(), state);

    // A newer writer's extra field is ignored, as long as the checksum
    // covers it.
    let extended = json.replacen('{', r#"{"added_later":{"a":[1,2]},"#, 1);
    let file = format!(
        "{extended}\ncrc32c:{:08x}\n",
        crc32c_reference(extended.as_bytes())
    );
    assert_eq!(ManifestState::decode("x", file.as_bytes()).unwrap(), state);

    // And through the directory.
    let tmp = tempfile::tempdir().unwrap();
    fs::write(tmp.path().join(vname(3)), file).unwrap();
    assert_eq!(*open(tmp.path()).current(), state);
}

#[test]
fn commits_match_a_model_and_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let m = open(tmp.path());
    let mut model: BTreeMap<u64, PartMeta> = BTreeMap::new();
    let mut lsn = 0;
    let mut next = 1;

    // Deterministic pseudo-random walk over add/remove edits.
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    for step in 1..=60u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let mut edit = Edit::default();
        // Only parts live before this edit can be removed by it.
        let live: Vec<u64> = model.keys().copied().collect();
        if !live.is_empty() && x.is_multiple_of(3) {
            let victim = live[(x >> 8) as usize % live.len()];
            edit.remove.push(victim);
            model.remove(&victim);
        }
        for _ in 0..(x % 4) {
            edit.add.push(part(next));
            model.insert(next, part(next));
            next += 1;
        }
        if x.is_multiple_of(2) {
            lsn += x % 100;
            edit.wal_flushed_lsn = Some(lsn);
        }
        let s = m.commit(edit).unwrap();
        assert_eq!(s.version, step);
        assert_eq!(s.wal_flushed_lsn, lsn);
        assert_eq!(s.next_part_id, next);
        assert_eq!(s.parts, model.values().cloned().collect::<Vec<_>>());
    }
    assert_eq!(*open(tmp.path()).current(), *m.current());
}

#[test]
fn invalid_edits_are_rejected_and_change_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let m = open(tmp.path());
    m.commit(Edit {
        wal_flushed_lsn: Some(50),
        next_part_id: Some(20),
        ..add(&[5, 6])
    })
    .unwrap();
    let before = m.current();
    let files = file_names(tmp.path());

    let cases: Vec<(&str, Edit)> = vec![
        (
            "remove not live",
            Edit {
                remove: vec![9],
                ..Edit::default()
            },
        ),
        (
            "remove twice",
            Edit {
                remove: vec![5, 5],
                ..Edit::default()
            },
        ),
        ("add already live", add(&[6])),
        ("add twice", add(&[7, 7])),
        (
            "add and remove same id",
            Edit {
                remove: vec![8],
                ..add(&[8])
            },
        ),
        (
            "lsn goes back",
            Edit {
                wal_flushed_lsn: Some(49),
                ..Edit::default()
            },
        ),
        (
            "next id goes back",
            Edit {
                next_part_id: Some(19),
                ..Edit::default()
            },
        ),
        (
            "added id at next_part_id",
            Edit {
                next_part_id: Some(30),
                ..add(&[30])
            },
        ),
        (
            "added id above next_part_id",
            Edit {
                next_part_id: Some(21),
                ..add(&[40])
            },
        ),
        ("max id", add(&[u64::MAX])),
    ];
    for (name, edit) in cases {
        let err = m.commit(edit).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{name}: {err}");
        assert_eq!(*m.current(), *before, "{name}");
        assert_eq!(file_names(tmp.path()), files, "{name}");
    }

    // Valid neighbours of the rejected cases still work.
    let s = m
        .commit(Edit {
            remove: vec![5],
            wal_flushed_lsn: Some(50),
            ..add(&[20])
        })
        .unwrap();
    assert_eq!(ids(&s), [6, 20]);
    assert_eq!(s.next_part_id, 21);
}

#[test]
fn conflict_leaves_loser_unchanged_until_reload() {
    let tmp = tempfile::tempdir().unwrap();
    let a = open(tmp.path());
    let b = open(tmp.path());
    a.commit(add(&[1])).unwrap();

    let stale = b.current();
    let err = b.commit(add(&[2])).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    assert_eq!(*b.current(), *stale);
    assert_eq!(b.current().version, 0);
    assert_eq!(file_names(tmp.path()), [vname(1)]);

    let reloaded = b.reload().unwrap();
    assert_eq!((reloaded.version, ids(&reloaded)), (1, vec![1]));
    let s = b.commit(add(&[2])).unwrap();
    assert_eq!((s.version, ids(&s)), (2, vec![1, 2]));
    assert_eq!(file_names(tmp.path()), [vname(1), vname(2)]);
}

#[test]
fn eight_writers_lose_nothing() {
    const THREADS: u64 = 8;
    const EACH: u64 = 50;
    let tmp = tempfile::tempdir().unwrap();
    let opts = ManifestOptions {
        keep_versions: usize::MAX,
    };
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let dir = tmp.path().to_path_buf();
            let opts = opts.clone();
            thread::spawn(move || {
                let m = Manifest::open(&dir, opts).unwrap();
                for i in 0..EACH {
                    let id = t * 1000 + i + 1;
                    loop {
                        match m.commit(add(&[id])) {
                            Ok(_) => break,
                            Err(Error::Conflict(_)) => {
                                m.reload().unwrap();
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let s = Manifest::open(tmp.path(), opts).unwrap().current();
    let total = THREADS * EACH;
    assert_eq!(s.version, total);
    let mut want: Vec<u64> = (0..THREADS)
        .flat_map(|t| (0..EACH).map(move |i| t * 1000 + i + 1))
        .collect();
    want.sort();
    assert_eq!(ids(&s), want);
    let expected: Vec<String> = (1..=total).map(vname).collect();
    assert_eq!(file_names(tmp.path()), expected);
}

#[test]
fn snapshots_never_change() {
    let tmp = tempfile::tempdir().unwrap();
    let m = open(tmp.path());
    let snap = m.commit(add(&[1, 2])).unwrap();
    let copy = (*snap).clone();
    m.commit(Edit {
        remove: vec![1],
        wal_flushed_lsn: Some(9),
        ..add(&[3])
    })
    .unwrap();
    m.reload().unwrap();
    assert_eq!(*snap, copy);
    assert_eq!(ids(&m.current()), [2, 3]);
    assert!(!Arc::ptr_eq(&snap, &m.current()));
}

#[test]
fn crash_leftovers() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let m = open(dir);
    m.commit(add(&[1])).unwrap();
    let good = m.current();

    // Temporary file of a crashed commit, plus unrelated files.
    fs::write(dir.join(".tmp-123-0-456"), "half a manif").unwrap();
    fs::write(dir.join("notes.txt"), "hello").unwrap();
    fs::write(dir.join("99.json"), "not a version").unwrap();
    fs::write(dir.join(format!("{}.bak", vname(9))), "x").unwrap();
    fs::write(dir.join("0000000000000000009x.json"), "x").unwrap();
    fs::write(dir.join("99999999999999999999.json"), "above u64::MAX").unwrap();
    let m2 = open(dir);
    assert_eq!(*m2.current(), *good);
    assert!(!dir.join(".tmp-123-0-456").exists());
    assert!(dir.join("notes.txt").exists());
    assert!(dir.join("99.json").exists());

    // A damaged highest version is an error, never an older version.
    let v1 = fs::read(dir.join(vname(1))).unwrap();
    for (label, bytes) in [
        ("truncated", v1[..v1.len() / 2].to_vec()),
        (
            "no checksum line",
            v1[..v1.iter().position(|&b| b == b'\n').unwrap() + 1].to_vec(),
        ),
        ("flipped byte", {
            let mut b = v1.clone();
            b[20] ^= 1;
            b
        }),
        ("empty", Vec::new()),
    ] {
        fs::write(dir.join(vname(2)), bytes).unwrap();
        let err = Manifest::open(dir, ManifestOptions::default())
            .err()
            .unwrap();
        match err {
            Error::Corrupt(msg) => assert!(msg.contains(&vname(2)), "{label}: {msg}"),
            e => panic!("{label}: {e}"),
        }
    }

    // A valid file whose content names another version is corrupt too.
    fs::write(dir.join(vname(2)), &v1).unwrap();
    assert!(matches!(
        Manifest::open(dir, ManifestOptions::default())
            .err()
            .unwrap(),
        Error::Corrupt(_)
    ));
}

#[test]
fn old_versions_are_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let m = Manifest::open(dir, ManifestOptions { keep_versions: 3 }).unwrap();
    for id in 1..=10 {
        m.commit(add(&[id])).unwrap();
    }
    assert_eq!(file_names(dir), [vname(8), vname(9), vname(10)]);
    let again = Manifest::open(dir, ManifestOptions { keep_versions: 3 }).unwrap();
    assert_eq!(again.current().version, 10);
    assert_eq!(again.current().parts.len(), 10);

    // keep_versions of zero still keeps the newest file.
    let z = Manifest::open(dir, ManifestOptions { keep_versions: 0 }).unwrap();
    z.commit(add(&[11])).unwrap();
    assert_eq!(file_names(dir), [vname(11)]);
}

#[test]
fn reserved_ids_and_unreferenced_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    let m = open(&tmp.path().join("manifest"));

    let r1 = m.reserve_part_ids(3);
    let r2 = m.reserve_part_ids(2);
    assert_eq!((r1.clone(), r2.clone()), (1..4, 4..6));
    assert!(m.reserve_part_ids(0).is_empty());

    // Committing only the first range raises next_part_id to the parts added;
    // ids reserved earlier but never committed are not handed out again by
    // this handle.
    let s = m.commit(add(&r1.collect::<Vec<_>>())).unwrap();
    assert_eq!(s.next_part_id, 4);
    assert_eq!(m.reserve_part_ids(1), 6..7);
    let s = m.commit(add(&[4, 5])).unwrap();
    assert_eq!(s.next_part_id, 6);

    // A commit from elsewhere that raises next_part_id moves reservations up.
    m.commit(Edit {
        next_part_id: Some(100),
        ..Edit::default()
    })
    .unwrap();
    assert_eq!(m.reserve_part_ids(1), 100..101);

    // Nothing there yet.
    assert!(unreferenced_part_dirs(&parts, &s).unwrap().is_empty());
    for id in [1, 2, 4, 5, 6, 7] {
        fs::create_dir_all(parts.join(part_dir_name(id))).unwrap();
    }
    fs::create_dir_all(parts.join(format!("{}.tmp", part_dir_name(8)))).unwrap();
    fs::create_dir_all(parts.join(format!("{}.tmp", part_dir_name(1)))).unwrap();
    fs::create_dir_all(parts.join("scratch")).unwrap();
    fs::write(parts.join(part_dir_name(9)), "a file").unwrap();

    let got = unreferenced_part_dirs(&parts, &s).unwrap();
    let want: Vec<_> = [
        part_dir_name(1) + ".tmp",
        part_dir_name(6),
        part_dir_name(7),
        part_dir_name(8) + ".tmp",
    ]
    .iter()
    .map(|n| parts.join(n))
    .collect();
    assert_eq!(got, want);
}
