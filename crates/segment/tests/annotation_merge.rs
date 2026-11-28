//! Merging annotation parts.

mod support;

use nwdb_core::{Error, PartMeta};
use nwdb_segment::{ANNOTATION_FILE, Annotation, AnnotationReader, Latest, merge_annotation_parts};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::path::Path;
use support::ann::*;

fn latest_of(readers: &[AnnotationReader]) -> Vec<Annotation> {
    let mut l = Latest::new(readers.iter().map(|r| r.cursor(None)).collect(), vec![]);
    let mut out = Vec::new();
    while let Some(a) = l.next().unwrap() {
        out.push(a);
    }
    out
}

fn merge(readers: &[AnnotationReader], dir: &Path, id: u64) -> (PartMeta, u64) {
    let refs: Vec<&AnnotationReader> = readers.iter().collect();
    let (meta, stats) =
        merge_annotation_parts(&refs, dir, id, WINDOW_START, WINDOW_NS, 2, None).unwrap();
    (meta, stats.superseded)
}

fn dir_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

#[test]
fn the_merge_equals_latest_over_the_inputs() {
    for seed in 0..40u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let tmp = tempfile::tempdir().unwrap();
        let mut readers = Vec::new();
        for k in 0..rng.random_range(1..7) {
            let mut recs = random_input(&mut rng, k, true);
            if recs.is_empty() {
                recs = random_input(&mut rng, k, false);
            }
            if recs.is_empty() {
                continue;
            }
            // Distinct values, so that a wrong winner shows.
            for (i, a) in recs.iter_mut().enumerate() {
                if !a.delete {
                    a.value = Some((k * 1000 + i) as f64);
                }
            }
            let m = write_part(tmp.path(), k as u64 + 1, &recs);
            readers.push(open(tmp.path(), &m));
        }
        if readers.is_empty() {
            continue;
        }
        // Independent model of the winners: by identity, the largest arrival,
        // the earlier input on a tie.
        let mut model: BTreeMap<_, Annotation> = BTreeMap::new();
        let mut total = 0u64;
        for r in &readers {
            for a in read(r, None) {
                total += 1;
                let key = (a.trace_id, a.span_id, a.name.clone(), a.source.clone());
                match model.get(&key) {
                    Some(w) if w.arrival >= a.arrival => {}
                    _ => {
                        model.insert(key, a);
                    }
                }
            }
        }
        let want: Vec<_> = model.into_values().collect();
        assert_eq!(latest_of(&readers), want, "seed {seed}");

        let (meta, superseded) = merge(&readers, tmp.path(), 50);
        assert_eq!(superseded, total - want.len() as u64, "seed {seed}");
        assert_eq!(meta.rows, want.len() as u64);
        assert_eq!((meta.part_id, meta.level), (50, 2));
        assert_eq!(
            (meta.window_start_ns, meta.window_ns),
            (WINDOW_START, WINDOW_NS)
        );
        let merged = open(tmp.path(), &meta);
        merged.verify().unwrap();
        assert_eq!(read(&merged, None), want, "seed {seed}");
        // Deletes are in the output exactly when they won.
        assert_eq!(
            read(&merged, None).iter().filter(|a| a.delete).count(),
            want.iter().filter(|a| a.delete).count()
        );
        // The inputs are untouched and no temporary directory is left.
        assert!(dir_names(tmp.path()).iter().all(|n| !n.ends_with(".tmp")));
    }
}

#[test]
fn a_delete_that_won_is_kept_and_a_delete_that_lost_is_not() {
    let tmp = tempfile::tempdir().unwrap();
    let mut del = score(1, 9);
    del.value = None;
    del.delete = true;
    let mut old_del = score(2, 3);
    old_del.value = None;
    old_del.delete = true;
    let m1 = write_part(tmp.path(), 1, &[score(1, 5), old_del, score(3, 5)]);
    let m2 = write_part(tmp.path(), 2, &[del.clone(), score(2, 7)]);
    let readers = [open(tmp.path(), &m1), open(tmp.path(), &m2)];
    let (meta, superseded) = merge(&readers, tmp.path(), 3);
    assert_eq!(superseded, 2);
    let out = read(&open(tmp.path(), &meta), None);
    assert_eq!(out, [del, score(2, 7), score(3, 5)]);
}

#[test]
fn a_merge_of_hundreds_of_blocks_and_of_one_input() {
    let mut rng = StdRng::seed_from_u64(11);
    let tmp = tempfile::tempdir().unwrap();
    let mut a = sorted_records(&mut rng, 900, 4000);
    let b = sorted_records(&mut rng, 900, 4000);
    // Same identities, later arrivals in b for the first half, earlier for
    // the rest.
    for (i, x) in a.iter_mut().enumerate() {
        x.arrival = if i < 450 { 1 } else { 10_000 };
    }
    let (ma, mb) = (write_part(tmp.path(), 1, &a), write_part(tmp.path(), 2, &b));
    let readers = [open(tmp.path(), &ma), open(tmp.path(), &mb)];
    let (meta, superseded) = merge(&readers, tmp.path(), 3);
    assert_eq!(superseded, 900);
    let merged = open(tmp.path(), &meta);
    merged.verify().unwrap();
    assert_eq!(read(&merged, None), latest_of(&readers));
    // One input: the same records in a new part.
    let one = [open(tmp.path(), &meta)];
    let (meta2, superseded) = merge(&one, tmp.path(), 4);
    assert_eq!(superseded, 0);
    assert_eq!(read(&open(tmp.path(), &meta2), None), read(&one[0], None));
}

#[test]
fn an_input_of_another_window_and_no_input_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let m1 = write_part(tmp.path(), 1, &[score(1, 1)]);
    let m2 = write_part_in(
        tmp.path(),
        2,
        WINDOW_START + WINDOW_NS,
        WINDOW_NS,
        0,
        &[score(2, 1)],
    );
    let m3 = write_part_in(
        tmp.path(),
        3,
        WINDOW_START,
        WINDOW_NS * 2,
        0,
        &[score(2, 1)],
    );
    let r = [
        open(tmp.path(), &m1),
        open(tmp.path(), &m2),
        open(tmp.path(), &m3),
    ];
    for bad in [&r[1], &r[2]] {
        let e = merge_annotation_parts(
            &[&r[0], bad],
            tmp.path(),
            9,
            WINDOW_START,
            WINDOW_NS,
            1,
            None,
        )
        .unwrap_err();
        assert!(matches!(e, Error::InvalidArgument(_)), "{e}");
    }
    let e =
        merge_annotation_parts(&[], tmp.path(), 9, WINDOW_START, WINDOW_NS, 1, None).unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)));
    // The window asked for is not the inputs' window either.
    let e = merge_annotation_parts(&[&r[0]], tmp.path(), 9, 0, WINDOW_NS, 1, None).unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)));
    assert_eq!(dir_names(tmp.path()).len(), 3);
}

#[test]
fn a_damaged_input_is_found_before_anything_is_written() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(4);
    let m1 = write_part(tmp.path(), 1, &sorted_records(&mut rng, 30, 100));
    let m2 = write_part(tmp.path(), 2, &sorted_records(&mut rng, 30, 100));
    let readers = [open(tmp.path(), &m1), open(tmp.path(), &m2)];
    // Damaged after it was opened: the table is fine, a block is not.
    support::flip(&file_of(tmp.path(), 2), 3, 1);
    let refs: Vec<&AnnotationReader> = readers.iter().collect();
    let e =
        merge_annotation_parts(&refs, tmp.path(), 9, WINDOW_START, WINDOW_NS, 1, None).unwrap_err();
    assert!(matches!(e, Error::Corrupt(_)), "{e}");
    assert_eq!(
        dir_names(tmp.path()),
        ["00000000000000000001", "00000000000000000002"]
    );
    assert!(std::fs::metadata(file_of(tmp.path(), 2)).unwrap().len() > 0);
    let _ = ANNOTATION_FILE;
}

#[test]
fn a_stop_before_the_call_leaves_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let m1 = write_part(tmp.path(), 1, &[score(1, 1)]);
    let r = open(tmp.path(), &m1);
    let flag = std::sync::atomic::AtomicBool::new(true);
    let e = merge_annotation_parts(
        &[&r],
        tmp.path(),
        9,
        WINDOW_START,
        WINDOW_NS,
        1,
        Some(&flag),
    )
    .unwrap_err();
    assert!(matches!(&e, Error::Unavailable(m) if m == "the merge was stopped"));
    assert_eq!(dir_names(tmp.path()), ["00000000000000000001"]);
}
