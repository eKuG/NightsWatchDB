//! Writing and reading an annotation part, looking up one trace, and the
//! latest-wins cursor.

mod support;

use nwdb_core::{Error, PartKind, PartMeta, TraceId};
use nwdb_segment::{
    ANNOTATION_FILE, ANNOTATION_FORMAT_VERSION, Annotation, AnnotationReader, AnnotationWriter,
    Latest,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use support::ann::*;

/// The block count in the footer of the file.
fn blocks_in_file(parts: &std::path::Path, id: u64) -> usize {
    let b = std::fs::read(file_of(parts, id)).unwrap();
    u64::from_le_bytes(b[b.len() - 80 + 48..b.len() - 80 + 56].try_into().unwrap()) as usize
}

/// `(offset, clen, ulen)` and the first key of every block, from the table.
fn table(parts: &std::path::Path, id: u64) -> Vec<(TraceId, u64, u32, u32)> {
    let b = std::fs::read(file_of(parts, id)).unwrap();
    let n = blocks_in_file(parts, id);
    let start = b.len() - 80 - 36 * n;
    (0..n)
        .map(|i| {
            let e = &b[start + 36 * i..start + 36 * i + 36];
            (
                TraceId(e[0..16].try_into().unwrap()),
                u64::from_le_bytes(e[16..24].try_into().unwrap()),
                u32::from_le_bytes(e[24..28].try_into().unwrap()),
                u32::from_le_bytes(e[28..32].try_into().unwrap()),
            )
        })
        .collect()
}

fn check_meta(meta: &PartMeta, records: &[Annotation], parts: &std::path::Path, level: u8) {
    let traces: std::collections::BTreeSet<_> = records.iter().map(|a| a.trace_id).collect();
    assert_eq!(meta.kind, PartKind::Annotations);
    assert_eq!(meta.format_version, ANNOTATION_FORMAT_VERSION);
    assert_eq!(meta.rows, records.len() as u64);
    assert_eq!(meta.traces, traces.len() as u64);
    assert_eq!(
        meta.min_start_ns,
        records.iter().map(|a| a.time_ns).min().unwrap()
    );
    assert_eq!(
        meta.max_end_ns,
        records.iter().map(|a| a.time_ns).max().unwrap()
    );
    assert_eq!(meta.window_start_ns, WINDOW_START);
    assert_eq!(meta.window_ns, WINDOW_NS);
    assert_eq!(meta.level, level);
    assert_eq!(meta.payload, Default::default());
    assert!(meta.created_unix_ms > 1_600_000_000_000);
    let len = std::fs::metadata(file_of(parts, meta.part_id))
        .unwrap()
        .len();
    assert_eq!(meta.files.len(), 1);
    assert_eq!(meta.files[ANNOTATION_FILE], len);
    // The directory holds that one file and nothing else.
    let names: Vec<_> = std::fs::read_dir(part_dir(parts, meta.part_id))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, [ANNOTATION_FILE]);
}

#[test]
fn what_was_written_reads_back_for_one_record_to_hundreds_of_blocks() {
    let mut rng = StdRng::seed_from_u64(1);
    for (n, comment, want_blocks) in [
        (1, 0, 1..=1),
        (2, 0, 1..=1),
        (100, 0, 1..=2),
        (3000, 20, 2..=20),
        (1500, 4000, 300..=2000),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let records = sorted_records(&mut rng, n, comment);
        let meta = write_part_in(tmp.path(), 7, WINDOW_START, WINDOW_NS, 3, &records);
        check_meta(&meta, &records, tmp.path(), 3);
        let blocks = blocks_in_file(tmp.path(), 7);
        assert!(
            want_blocks.contains(&blocks),
            "{n} records: {blocks} blocks"
        );
        assert_eq!(blocks, *model_blocks(&records).last().unwrap() + 1);
        let r = open(tmp.path(), &meta);
        assert_eq!(r.meta(), &meta);
        assert_eq!(r.blocks_read(), 0);
        r.verify().unwrap();
        assert_eq!(read(&r, None), records);
        // The reader outlives the directory.
        std::fs::remove_dir_all(part_dir(tmp.path(), 7)).unwrap();
        assert_eq!(read(&r, None), records);
    }
}

#[test]
fn a_tiny_part_has_the_bytes_the_format_says() {
    let tmp = tempfile::tempdir().unwrap();
    let a = score(5, 9);
    let meta = write_part(tmp.path(), 42, std::slice::from_ref(&a));
    let b = std::fs::read(file_of(tmp.path(), 42)).unwrap();
    let n = b.len();
    let table = &b[n - 80 - 36..n - 80];
    let f = &b[n - 80..];
    let u32_at = |s: &[u8], i: usize| u32::from_le_bytes(s[i..i + 4].try_into().unwrap());
    let u64_at = |s: &[u8], i: usize| u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
    // The record: trace, flags, arrival, time, name, source, value.
    assert_eq!(16 + 1 + 8 + 8 + 1 + 8 + 1 + 8, 51);
    // Table entry.
    assert_eq!(&table[0..16], &5u128.to_be_bytes());
    assert_eq!(u64_at(table, 16), 0);
    let clen = u32_at(table, 24) as usize;
    assert_eq!(u32_at(table, 28), 51);
    assert_eq!(u32_at(table, 32), crc32c::crc32c(&b[..clen]));
    assert_eq!(n, clen + 36 + 80);
    let plain = zstd::bulk::decompress(&b[..clen], 51).unwrap();
    assert_eq!(&plain[0..16], &5u128.to_be_bytes());
    assert_eq!(plain[16], 4, "flags: value only");
    assert_eq!(u64_at(&plain, 17), 9, "arrival");
    // Footer.
    assert_eq!(&f[0..4], b"NWAN");
    assert_eq!(u32_at(f, 4), 1);
    assert_eq!(u64_at(f, 8), 42);
    assert_eq!(u64_at(f, 16) as i64, WINDOW_START);
    assert_eq!(u64_at(f, 24) as i64, WINDOW_NS);
    assert_eq!(u64_at(f, 32), 1);
    assert_eq!(u64_at(f, 40), 1);
    assert_eq!(u64_at(f, 48), 1);
    assert_eq!(u64_at(f, 56) as i64, a.time_ns);
    assert_eq!(u64_at(f, 64) as i64, a.time_ns);
    assert_eq!(u32_at(f, 72), crc32c::crc32c(table));
    assert_eq!(u32_at(f, 76), crc32c::crc32c(&f[..76]));
    assert_eq!(meta.files[ANNOTATION_FILE], n as u64);
}

#[test]
fn bytes_per_small_score_record() {
    // Trace-level, 8-byte name, no source, value only: 51 bytes in a block.
    let tmp = tempfile::tempdir().unwrap();
    let records: Vec<_> = (1..=100_000u128).map(|t| score(t, t as u64)).collect();
    let meta = write_part(tmp.path(), 1, &records);
    let size = meta.files[ANNOTATION_FILE];
    eprintln!(
        "SIZES uncompressed 51 bytes per record, file {} bytes for 100000 records = {:.2} per record",
        size,
        size as f64 / 100_000.0
    );
    assert!(size < 100_000 * 51);
}

fn lookup_part(rng: &mut StdRng) -> Vec<Annotation> {
    // Traces 4, 8, ..., 400 with a score each; trace 200 has 20 records of
    // the largest size (several blocks); trace 300 has 3.
    let mut v = Vec::new();
    for t in (4..=400u128).step_by(4) {
        match t {
            200 => (0..20).for_each(|s| v.push(largest(rng, t, s + 1, t as u64 + s))),
            300 => (0..3).for_each(|s| v.push(largest(rng, t, s + 1, t as u64 + s))),
            _ => {
                // Filler so that there are many blocks.
                let mut a = score(t, t as u64);
                a.comment = Some(text(rng, 1500));
                v.push(a);
            }
        }
    }
    v
}

#[test]
fn a_lookup_returns_exactly_the_trace_and_reads_few_blocks() {
    let mut rng = StdRng::seed_from_u64(2);
    let tmp = tempfile::tempdir().unwrap();
    let records = lookup_part(&mut rng);
    let meta = write_part(tmp.path(), 1, &records);
    let r = open(tmp.path(), &meta);
    let blocks = model_blocks(&records);
    let tab = table(tmp.path(), 1);
    assert!(tab.len() > 10);
    // The big trace spans at least three blocks.
    let spanned = |t: u128| -> usize {
        let mut set = std::collections::BTreeSet::new();
        for (a, b) in records.iter().zip(&blocks) {
            if a.trace_id == trace(t) {
                set.insert(*b);
            }
        }
        set.len()
    };
    assert!(spanned(200) >= 3, "{}", spanned(200));
    // Ids absent before, between and after the stored ones, every stored
    // id, and every block's first key.
    let mut ids: Vec<TraceId> = (0..=404u128).map(trace).collect();
    ids.push(trace(u128::MAX));
    ids.extend(tab.iter().map(|e| e.0));
    for t in ids {
        let want: Vec<_> = records
            .iter()
            .filter(|a| a.trace_id == t)
            .cloned()
            .collect();
        let before = r.blocks_read();
        let got = read(&r, Some(t));
        let cost = (r.blocks_read() - before) as usize;
        assert_eq!(got, want, "trace {t}");
        let spanned_n = spanned(u128::from_be_bytes(t.0));
        assert!(
            cost <= spanned_n + 1,
            "trace {t}: {cost} blocks for {spanned_n} spanned"
        );
        if want.is_empty() {
            // An absent id costs at most the one block that could hold it.
            assert!(cost <= 1, "trace {t}");
        }
    }
    // A trace that is the first key of a block that begins in the middle of
    // it: found, with the earlier block read too.
    let first_keys: Vec<_> = tab.iter().map(|e| e.0).collect();
    assert!(
        first_keys.windows(2).any(|w| w[0] == w[1]),
        "no trace spans blocks"
    );
}

#[test]
fn the_largest_records_fit_the_block_ceiling() {
    let mut rng = StdRng::seed_from_u64(3);
    let tmp = tempfile::tempdir().unwrap();
    let records: Vec<_> = (1..=60)
        .map(|i| largest(&mut rng, i / 6 + 1, i as u64, 1))
        .collect();
    let meta = write_part(tmp.path(), 1, &records);
    let tab = table(tmp.path(), 1);
    for (i, (_, _, clen, ulen)) in tab.iter().enumerate() {
        assert!(*ulen <= 32 * 1024, "{ulen}");
        // Every block but the last was closed by reaching 16 KiB.
        assert!(i + 1 == tab.len() || *ulen >= 16 * 1024, "{ulen}");
        assert!(*clen > 0);
    }
    let r = open(tmp.path(), &meta);
    r.verify().unwrap();
    assert_eq!(read(&r, None), records);
}

fn random_input(rng: &mut StdRng, k: usize, with_deletes: bool) -> Vec<Annotation> {
    let mut by: BTreeMap<_, Annotation> = BTreeMap::new();
    for _ in 0..rng.random_range(0..40) {
        let mut a = score(rng.random_range(1..6), rng.random_range(1..6));
        a.span_id = if rng.random() {
            None
        } else {
            Some(span(rng.random_range(1..3)))
        };
        a.name = format!("n{}", rng.random_range(0..3));
        a.source = format!("s{}", rng.random_range(0..2));
        a.comment = Some(format!("input {k}"));
        a.time_ns = WINDOW_START + k as i64;
        if with_deletes && rng.random_range(0..4) == 0 {
            a.delete = true;
            a.value = None;
            a.comment = None;
            // The input number rides in the time.
        }
        by.insert((a.trace_id, a.span_id, a.name.clone(), a.source.clone()), a);
    }
    by.into_values().collect()
}

#[test]
fn latest_matches_a_brute_force_model() {
    for seed in 0..60u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let tmp = tempfile::tempdir().unwrap();
        let parts = rng.random_range(1..6);
        let inputs: Vec<Vec<Annotation>> = (0..parts)
            .map(|k| random_input(&mut rng, k, true))
            .collect();
        // The in-memory input: sorted by identity, repeats allowed, any
        // order among them.
        let mut extra: Vec<Annotation> = (0..rng.random_range(0..30))
            .filter_map(|_| random_input(&mut rng, 99, true).into_iter().next())
            .collect();
        extra.sort_by(|a, b| a.identity_cmp(b));
        for (i, a) in extra.iter_mut().enumerate() {
            a.time_ns = WINDOW_START + 1000 + i as i64;
        }

        let mut metas = Vec::new();
        for (k, recs) in inputs.iter().enumerate() {
            if !recs.is_empty() {
                metas.push(write_part(tmp.path(), k as u64 + 1, recs));
            }
        }
        let readers: Vec<AnnotationReader> = metas.iter().map(|m| open(tmp.path(), m)).collect();
        let non_empty: Vec<&Vec<Annotation>> = inputs.iter().filter(|r| !r.is_empty()).collect();

        let mut model: BTreeMap<_, Annotation> = BTreeMap::new();
        for a in non_empty.iter().flat_map(|r| r.iter()).chain(extra.iter()) {
            let key = (a.trace_id, a.span_id, a.name.clone(), a.source.clone());
            match model.get(&key) {
                Some(w) if w.arrival >= a.arrival => {}
                _ => {
                    model.insert(key, a.clone());
                }
            }
        }
        let want: Vec<_> = model.into_values().collect();

        let mut latest = Latest::new(readers.iter().map(|r| r.cursor(None)).collect(), extra);
        let mut got = Vec::new();
        while let Some(a) = latest.next().unwrap() {
            got.push(a);
        }
        assert_eq!(got, want, "seed {seed}");
        assert!(latest.next().unwrap().is_none());
    }
}

#[test]
fn latest_breaks_ties_by_input_order_and_the_extra_input_is_last() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = score(1, 5);
    a.comment = Some("first".into());
    let mut b = a.clone();
    b.comment = Some("second".into());
    let mut c = a.clone();
    c.comment = Some("extra".into());
    let m1 = write_part(tmp.path(), 1, &[a.clone()]);
    let m2 = write_part(tmp.path(), 2, &[b.clone()]);
    let (r1, r2) = (open(tmp.path(), &m1), open(tmp.path(), &m2));
    let comment = |l: &mut Latest| l.next().unwrap().unwrap().comment.unwrap();
    // Equal arrival: the earlier input.
    let mut l = Latest::new(vec![r1.cursor(None), r2.cursor(None)], vec![c.clone()]);
    assert_eq!(comment(&mut l), "first");
    assert!(l.next().unwrap().is_none());
    let mut l = Latest::new(vec![r2.cursor(None), r1.cursor(None)], vec![]);
    assert_eq!(comment(&mut l), "second");
    // A larger arrival wins whatever the order, also in the extra input.
    c.arrival = 6;
    let mut l = Latest::new(vec![r1.cursor(None), r2.cursor(None)], vec![c.clone()]);
    assert_eq!(comment(&mut l), "extra");
    let mut late = b.clone();
    late.arrival = 9;
    let m3 = write_part(tmp.path(), 3, &[late]);
    let r3 = open(tmp.path(), &m3);
    let mut l = Latest::new(
        vec![r1.cursor(None), r2.cursor(None), r3.cursor(None)],
        vec![c],
    );
    assert_eq!(comment(&mut l), "second");
    // No inputs at all.
    assert!(Latest::new(vec![], vec![]).next().unwrap().is_none());
}

#[test]
fn latest_keeps_deletes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut del = score(1, 9);
    del.delete = true;
    del.value = None;
    let m1 = write_part(tmp.path(), 1, &[score(1, 5), score(2, 5)]);
    let m2 = write_part(tmp.path(), 2, &[del.clone()]);
    let (r1, r2) = (open(tmp.path(), &m1), open(tmp.path(), &m2));
    let mut l = Latest::new(vec![r1.cursor(None), r2.cursor(None)], vec![]);
    assert_eq!(l.next().unwrap().unwrap(), del);
    assert_eq!(l.next().unwrap().unwrap(), score(2, 5));
    assert!(l.next().unwrap().is_none());
}

#[test]
fn the_writer_refuses_what_it_must() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w = AnnotationWriter::create(tmp.path(), 1, WINDOW_START, WINDOW_NS).unwrap();
    w.append(&score(5, 1)).unwrap();
    // The same identity, a smaller one, and a larger one in a field that
    // sorts lower.
    for bad in [score(5, 2), score(4, 1)] {
        assert!(
            matches!(w.append(&bad), Err(Error::InvalidArgument(_))),
            "{bad:?}"
        );
    }
    let mut lower_span = score(5, 1);
    lower_span.name = "a".into();
    assert!(matches!(
        w.append(&lower_span),
        Err(Error::InvalidArgument(_))
    ));
    // An invalid record.
    let mut invalid = score(6, 1);
    invalid.value = Some(f64::NAN);
    assert!(matches!(w.append(&invalid), Err(Error::InvalidArgument(_))));
    let mut invalid = score(6, 1);
    invalid.trace_id = TraceId([0; 16]);
    assert!(matches!(w.append(&invalid), Err(Error::InvalidArgument(_))));
    // The writer is still usable after a refusal.
    w.append(&score(6, 1)).unwrap();
    let meta = w.finish(0).unwrap();
    assert_eq!(meta.rows, 2);
    assert_eq!(
        read(&open(tmp.path(), &meta), None),
        [score(5, 1), score(6, 1)]
    );
}

#[test]
fn a_part_of_no_records_is_refused_and_leaves_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let w = AnnotationWriter::create(tmp.path(), 1, WINDOW_START, WINDOW_NS).unwrap();
    assert!(matches!(w.finish(0), Err(Error::InvalidArgument(_))));
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
}

#[test]
fn a_dropped_writer_removes_its_temporary_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w = AnnotationWriter::create(tmp.path(), 1, WINDOW_START, WINDOW_NS).unwrap();
    w.append(&score(1, 1)).unwrap();
    let tmp_dir = tmp
        .path()
        .join(format!("{}.tmp", nwdb_core::part_dir_name(1)));
    assert!(tmp_dir.is_dir());
    drop(w);
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
}

#[test]
fn create_refuses_a_taken_id_and_a_bad_window() {
    let tmp = tempfile::tempdir().unwrap();
    write_part(tmp.path(), 1, &[score(1, 1)]);
    assert!(matches!(
        AnnotationWriter::create(tmp.path(), 1, 0, 1).err(),
        Some(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        AnnotationWriter::create(tmp.path(), 2, 0, 0).err(),
        Some(Error::InvalidArgument(_))
    ));
    let _held = AnnotationWriter::create(tmp.path(), 3, 0, 1).unwrap();
    assert!(matches!(
        AnnotationWriter::create(tmp.path(), 3, 0, 1).err(),
        Some(Error::InvalidArgument(_))
    ));
}

#[test]
fn a_missing_file_is_corrupt() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = write_part(tmp.path(), 1, &[score(1, 1)]);
    std::fs::remove_file(file_of(tmp.path(), 1)).unwrap();
    let e = AnnotationReader::open(part_dir(tmp.path(), 1), &meta)
        .err()
        .unwrap();
    assert!(support::corrupt(e).contains("missing"));
}
