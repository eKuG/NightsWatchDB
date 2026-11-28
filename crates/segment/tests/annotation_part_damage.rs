//! Damaged and foreign annotation part files: every open check and every
//! read check, in order, and the guarantee that damage is an error and never
//! a wrong record.

mod support;

use nwdb_core::{Error, PartMeta, TraceId};
use nwdb_segment::{ANNOTATION_FILE, Annotation, AnnotationReader};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::Path;
use support::ann::*;
use support::corrupt;

/// The file split into its blocks, table and footer.
struct Img {
    body: Vec<u8>,
    table: Vec<u8>,
    footer: Vec<u8>,
}

fn split(b: &[u8]) -> Img {
    let f = b.len() - 80;
    let blocks = u64::from_le_bytes(b[f + 48..f + 56].try_into().unwrap()) as usize;
    let t = f - 36 * blocks;
    Img {
        body: b[..t].to_vec(),
        table: b[t..f].to_vec(),
        footer: b[f..].to_vec(),
    }
}

fn put_u32(v: &mut [u8], at: usize, x: u32) {
    v[at..at + 4].copy_from_slice(&x.to_le_bytes());
}

fn put_u64(v: &mut [u8], at: usize, x: u64) {
    v[at..at + 8].copy_from_slice(&x.to_le_bytes());
}

fn get_u32(v: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(v[at..at + 4].try_into().unwrap())
}

fn get_u64(v: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(v[at..at + 8].try_into().unwrap())
}

impl Img {
    /// The file with both checksums recomputed, so that only the check
    /// under test can refuse it.
    fn join(&self) -> Vec<u8> {
        let mut footer = self.footer.clone();
        put_u32(&mut footer, 72, crc32c::crc32c(&self.table));
        let crc = crc32c::crc32c(&footer[..76]);
        put_u32(&mut footer, 76, crc);
        [self.body.as_slice(), &self.table, &footer].concat()
    }

    /// The file as it is, checksums and all.
    fn join_raw(&self) -> Vec<u8> {
        [self.body.as_slice(), &self.table, &self.footer].concat()
    }
}

fn install(parts: &Path, id: u64, bytes: &[u8]) {
    std::fs::create_dir_all(part_dir(parts, id)).unwrap();
    std::fs::write(file_of(parts, id), bytes).unwrap();
}

fn open_err(parts: &Path, id: u64, meta: &PartMeta) -> String {
    corrupt(
        AnnotationReader::open(part_dir(parts, id), meta)
            .err()
            .expect("open must fail"),
    )
}

/// The error of `verify`, which must fail.
fn verify_err(parts: &Path, id: u64, meta: &PartMeta) -> String {
    let r = AnnotationReader::open(part_dir(parts, id), meta).unwrap();
    corrupt(r.verify().expect_err("verify must fail"))
}

/// Two blocks of compressible records: 8 records of 4000 bytes.
fn two_blocks(parts: &Path, id: u64) -> (PartMeta, Vec<Annotation>) {
    let mut records = sorted_records(&mut StdRng::seed_from_u64(1), 8, 0);
    for (i, a) in records.iter_mut().enumerate() {
        a.comment = Some(format!("{i:02}").repeat(2000));
    }
    (write_part(parts, id, &records), records)
}

fn bytes_of(parts: &Path, id: u64) -> Vec<u8> {
    std::fs::read(file_of(parts, id)).unwrap()
}

#[test]
fn magic_comes_first_then_version_then_the_footer_checksum() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    let bad = |edit: &dyn Fn(&mut Vec<u8>), fix: bool| -> String {
        let mut i = Img {
            body: img.body.clone(),
            table: img.table.clone(),
            footer: img.footer.clone(),
        };
        edit(&mut i.footer);
        install(tmp.path(), 2, &if fix { i.join() } else { i.join_raw() });
        let mut m = meta.clone();
        m.part_id = 2;
        m.files
            .insert(ANNOTATION_FILE.into(), bytes_of(tmp.path(), 2).len() as u64);
        open_err(tmp.path(), 2, &m)
    };
    // A foreign file.
    assert!(bad(&|f| f[0..4].copy_from_slice(b"XXXX"), true).contains("magic"));
    // Wrong magic and wrong version and wrong checksum: the magic.
    assert!(
        bad(
            &|f| {
                f[0..4].copy_from_slice(b"XXXX");
                put_u32(f, 4, 2);
            },
            false
        )
        .contains("magic")
    );
    // Versions 0 and 2, with the checksums right: the version.
    for v in [0, 2, u32::MAX] {
        let m = bad(&|f| put_u32(f, 4, v), true);
        assert!(m.contains(&format!("format version {v}")), "{m}");
    }
    // A wrong version and a wrong footer checksum: the version, not the
    // checksum.
    for v in [0, 2] {
        let m = bad(&|f| put_u32(f, 4, v), false);
        assert!(m.contains(&format!("format version {v}")), "{m}");
    }
    // Right version, wrong checksum.
    let m = bad(&|f| f[20] ^= 1, false);
    assert!(m.contains("footer checksum"), "{m}");
    // A checksum error comes before the part id check: the id is wrong too.
    let m = bad(&|f| put_u64(f, 8, 77), false);
    assert!(m.contains("footer checksum"), "{m}");
}

#[test]
fn a_foreign_file_and_a_short_file_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let mut rng = StdRng::seed_from_u64(5);
    for len in [0usize, 1, 79, 80, 81, 1000] {
        install(tmp.path(), 1, &random_bytes(&mut rng, len));
        let mut m = meta.clone();
        m.files.insert(ANNOTATION_FILE.into(), len as u64);
        let msg = open_err(tmp.path(), 1, &m);
        if len >= 80 {
            assert!(msg.contains("magic"), "{msg}");
        }
    }
}

#[test]
fn the_footer_must_be_the_one_of_this_part() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    for (what, edit) in [
        (
            "part id",
            (&|f: &mut Vec<u8>| put_u64(f, 8, 9)) as &dyn Fn(&mut Vec<u8>),
        ),
        ("window start", &|f: &mut Vec<u8>| put_u64(f, 16, 1)),
        ("window width", &|f: &mut Vec<u8>| put_u64(f, 24, 1)),
    ] {
        let mut i = Img {
            body: img.body.clone(),
            table: img.table.clone(),
            footer: img.footer.clone(),
        };
        edit(&mut i.footer);
        install(tmp.path(), 1, &i.join());
        let m = open_err(tmp.path(), 1, &meta);
        assert!(m.contains("not of part"), "{what}: {m}");
    }
}

#[test]
fn a_file_of_another_part_of_equal_length_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta1, records) = two_blocks(tmp.path(), 1);
    let meta2 = write_part(tmp.path(), 2, &records);
    let (b1, b2) = (bytes_of(tmp.path(), 1), bytes_of(tmp.path(), 2));
    assert_eq!(b1.len(), b2.len());
    assert_ne!(b1, b2);
    // Part 2's file in part 1's directory, with part 1's metadata.
    install(tmp.path(), 1, &b2);
    let m = open_err(tmp.path(), 1, &meta1);
    assert!(m.contains("part 2") && m.contains("not of part 1"), "{m}");
    // And the other way round, and the right pair still opens.
    install(tmp.path(), 2, &b1);
    assert!(open_err(tmp.path(), 2, &meta2).contains("not of part 2"));
    install(tmp.path(), 1, &b1);
    AnnotationReader::open(part_dir(tmp.path(), 1), &meta1).unwrap();
}

#[test]
fn the_length_must_match_the_blocks_and_the_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    // A block that is one byte longer in the table than in the file.
    let mut i = Img {
        body: img.body.clone(),
        table: img.table.clone(),
        footer: img.footer.clone(),
    };
    let clen = get_u32(&i.table, 24);
    put_u32(&mut i.table, 24, clen + 1);
    install(tmp.path(), 1, &i.join());
    assert!(open_err(tmp.path(), 1, &meta).contains("add up"));
    // A file one byte longer than its blocks, table and footer.
    let mut i = Img {
        body: [img.body.clone(), vec![0]].concat(),
        table: img.table.clone(),
        footer: img.footer.clone(),
    };
    install(tmp.path(), 1, &i.join());
    let mut m = meta.clone();
    m.files
        .insert(ANNOTATION_FILE.into(), meta.files[ANNOTATION_FILE] + 1);
    assert!(open_err(tmp.path(), 1, &m).contains("add up"));
    // The file is right but the metadata says another length.
    i.body = img.body.clone();
    install(tmp.path(), 1, &i.join());
    let mut m = meta.clone();
    m.files
        .insert(ANNOTATION_FILE.into(), meta.files[ANNOTATION_FILE] + 1);
    assert!(open_err(tmp.path(), 1, &m).contains("metadata says"));
    let mut m = meta.clone();
    m.files.clear();
    assert!(open_err(tmp.path(), 1, &m).contains("metadata says"));
    // More blocks than the file can hold.
    let mut i = Img {
        body: img.body.clone(),
        table: img.table.clone(),
        footer: img.footer.clone(),
    };
    put_u64(&mut i.footer, 48, u64::MAX);
    install(tmp.path(), 1, &i.join());
    assert!(open_err(tmp.path(), 1, &meta).contains("do not fit"));
}

#[test]
fn the_table_has_its_own_checksum() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let mut i = split(&bytes_of(tmp.path(), 1));
    // A byte of the first key of block 1, with the footer checksum of the
    // table not updated.
    i.table[36] ^= 1;
    install(tmp.path(), 1, &i.join_raw());
    assert!(open_err(tmp.path(), 1, &meta).contains("block table checksum"));
}

#[test]
fn blocks_are_contiguous_ordered_and_within_the_ceiling() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    assert_eq!(img.table.len(), 72);
    let with = |edit: &dyn Fn(&mut Vec<u8>)| -> String {
        let mut i = Img {
            body: img.body.clone(),
            table: img.table.clone(),
            footer: img.footer.clone(),
        };
        edit(&mut i.table);
        install(tmp.path(), 1, &i.join());
        open_err(tmp.path(), 1, &meta)
    };
    let off = get_u64(&img.table, 36 + 16);
    assert!(with(&|t| put_u64(t, 36 + 16, off + 1)).contains("starts at"));
    assert!(with(&|t| put_u64(t, 36 + 16, off - 1)).contains("starts at"));
    assert!(with(&|t| put_u64(t, 16, 1)).contains("starts at"));
    // A first key smaller than the one before it.
    assert!(with(&|t| t[36..52].copy_from_slice(&[0; 16])).contains("smaller first key"));
    // Lengths.
    assert!(with(&|t| put_u32(t, 28, 32 * 1024 + 1)).contains("declares"));
    assert!(with(&|t| put_u32(t, 36 + 28, u32::MAX)).contains("declares"));
    assert!(with(&|t| put_u32(t, 28, 0)).contains("declares"));
    // The ceiling itself is allowed at open (the block then fails to read).
    let mut i = Img {
        body: img.body.clone(),
        table: img.table.clone(),
        footer: img.footer.clone(),
    };
    put_u32(&mut i.table, 28, 32 * 1024);
    install(tmp.path(), 1, &i.join());
    AnnotationReader::open(part_dir(tmp.path(), 1), &meta).unwrap();
}

#[test]
fn a_part_of_no_blocks_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let mut i = split(&bytes_of(tmp.path(), 1));
    i.body.clear();
    i.table.clear();
    put_u64(&mut i.footer, 48, 0);
    install(tmp.path(), 1, &i.join());
    let mut m = meta.clone();
    m.files.insert(ANNOTATION_FILE.into(), 80);
    assert!(open_err(tmp.path(), 1, &m).contains("no blocks"));
}

#[test]
fn the_footer_counts_must_be_those_of_the_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let edits: [fn(&mut PartMeta); 4] = [
        |m| m.rows += 1,
        |m| m.traces += 1,
        |m| m.min_start_ns -= 1,
        |m| m.max_end_ns += 1,
    ];
    for edit in edits {
        let mut m = meta.clone();
        edit(&mut m);
        assert!(open_err(tmp.path(), 1, &m).contains("record count"));
    }
}

#[test]
fn a_flipped_byte_in_a_block_fails_its_checksum() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    let clen0 = get_u32(&img.table, 24) as usize;
    for at in [0, clen0 / 2, clen0 - 1, clen0, img.body.len() - 1] {
        let mut b = bytes_of(tmp.path(), 1);
        b[at] ^= 0x10;
        install(tmp.path(), 1, &b);
        let m = verify_err(tmp.path(), 1, &meta);
        assert!(m.contains("checksum mismatch"), "byte {at}: {m}");
    }
    // A cursor stops at the damaged block and then reports the end.
    let r = AnnotationReader::open(part_dir(tmp.path(), 1), &meta).unwrap();
    let mut c = r.cursor(None);
    assert!(matches!(c.next(), Err(Error::Corrupt(_))));
    assert!(c.next().unwrap().is_none());
}

#[test]
fn a_block_decompresses_to_exactly_its_declared_length() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let img = split(&bytes_of(tmp.path(), 1));
    let ulen = get_u32(&img.table, 28);
    for declared in [ulen - 1, ulen + 1, ulen / 2, 32 * 1024] {
        let mut i = Img {
            body: img.body.clone(),
            table: img.table.clone(),
            footer: img.footer.clone(),
        };
        put_u32(&mut i.table, 28, declared);
        install(tmp.path(), 1, &i.join());
        let m = verify_err(tmp.path(), 1, &meta);
        assert!(m.contains("decompress"), "declared {declared}: {m}");
    }
    // A frame that says it holds a megabyte, declared as 100 bytes: refused
    // without being expanded.
    let frame = zstd::bulk::compress(&vec![0u8; 1 << 20], 3).unwrap();
    let mut i = Img {
        body: frame.clone(),
        table: img.table[..36].to_vec(),
        footer: img.footer.clone(),
    };
    put_u64(&mut i.table, 16, 0);
    put_u32(&mut i.table, 24, frame.len() as u32);
    put_u32(&mut i.table, 28, 100);
    put_u32(&mut i.table, 32, crc32c::crc32c(&frame));
    put_u64(&mut i.footer, 48, 1);
    install(tmp.path(), 1, &i.join());
    let mut m = meta.clone();
    m.files
        .insert(ANNOTATION_FILE.into(), bytes_of(tmp.path(), 1).len() as u64);
    assert!(verify_err(tmp.path(), 1, &m).contains("decompress"));
}

/// A block's compressed bytes replaced by the compression of `plain`, with
/// the table entry made to fit.
fn replace_block(img: &Img, block: usize, plain: &[u8]) -> Img {
    let packed = zstd::bulk::compress(plain, 3).unwrap();
    let mut out = Img {
        body: Vec::new(),
        table: img.table.clone(),
        footer: img.footer.clone(),
    };
    let mut offset = 0u64;
    for b in 0..img.table.len() / 36 {
        let (o, c) = (
            get_u64(&img.table, b * 36 + 16),
            get_u32(&img.table, b * 36 + 24),
        );
        let bytes = if b == block {
            packed.as_slice()
        } else {
            &img.body[o as usize..o as usize + c as usize]
        };
        put_u64(&mut out.table, b * 36 + 16, offset);
        put_u32(&mut out.table, b * 36 + 24, bytes.len() as u32);
        put_u32(&mut out.table, b * 36 + 32, crc32c::crc32c(bytes));
        if b == block {
            put_u32(&mut out.table, b * 36 + 28, plain.len() as u32);
        }
        out.body.extend_from_slice(bytes);
        offset += bytes.len() as u64;
    }
    out
}

fn plain_of(img: &Img, block: usize) -> Vec<u8> {
    let (o, c, u) = (
        get_u64(&img.table, block * 36 + 16) as usize,
        get_u32(&img.table, block * 36 + 24) as usize,
        get_u32(&img.table, block * 36 + 28) as usize,
    );
    zstd::bulk::decompress(&img.body[o..o + c], u).unwrap()
}

#[test]
fn records_are_decoded_strictly() {
    let tmp = tempfile::tempdir().unwrap();
    // 51-byte records, three blocks' worth.
    let records: Vec<_> = (1..=1000u128).map(|t| score(t, 1)).collect();
    let meta = write_part(tmp.path(), 1, &records);
    let img = split(&bytes_of(tmp.path(), 1));
    let plain = plain_of(&img, 0);
    assert_eq!(plain.len() % 51, 0);
    let run = |plain: Vec<u8>| -> String {
        let i = replace_block(&img, 0, &plain);
        install(tmp.path(), 1, &i.join());
        let mut m = meta.clone();
        m.files
            .insert(ANNOTATION_FILE.into(), bytes_of(tmp.path(), 1).len() as u64);
        verify_err(tmp.path(), 1, &m)
    };
    // Trailing bytes, a truncated record, garbage.
    assert!(run([plain.clone(), vec![0; 7]].concat()).contains("annotation"));
    assert!(run(plain[..plain.len() - 1].to_vec()).contains("truncated"));
    assert!(run(vec![0xff; 200]).contains("annotation"));
    // Two records swapped inside the block: out of order.
    let mut swapped = plain.clone();
    let (a, b) = (swapped[51..102].to_vec(), swapped[102..153].to_vec());
    swapped[51..102].copy_from_slice(&b);
    swapped[102..153].copy_from_slice(&a);
    assert!(run(swapped).contains("strictly increasing"));
    // The same record twice.
    let mut twice = plain.clone();
    let a = twice[0..51].to_vec();
    twice[51..102].copy_from_slice(&a);
    assert!(run(twice).contains("strictly increasing"));
}

/// The compressed block of a part of one block, with its first key and
/// declared length.
fn one_block(parts: &Path, id: u64, records: &[Annotation]) -> (Vec<u8>, TraceId, u32) {
    write_part(parts, id, records);
    let img = split(&bytes_of(parts, id));
    assert_eq!(img.table.len(), 36);
    (
        img.body,
        TraceId(img.table[0..16].try_into().unwrap()),
        get_u32(&img.table, 28),
    )
}

/// A part made of the given blocks, with the footer and the metadata that
/// fit them, and the first keys the table is given.
fn assemble(
    parts: &Path,
    id: u64,
    blocks: &[(Vec<u8>, TraceId, u32)],
    records: &[Annotation],
) -> PartMeta {
    let traces: std::collections::BTreeSet<_> = records.iter().map(|a| a.trace_id).collect();
    let min = records.iter().map(|a| a.time_ns).min().unwrap();
    let max = records.iter().map(|a| a.time_ns).max().unwrap();
    let (mut body, mut table) = (Vec::new(), Vec::new());
    for (packed, first, ulen) in blocks {
        table.extend_from_slice(first.as_bytes());
        table.extend_from_slice(&(body.len() as u64).to_le_bytes());
        table.extend_from_slice(&(packed.len() as u32).to_le_bytes());
        table.extend_from_slice(&ulen.to_le_bytes());
        table.extend_from_slice(&crc32c::crc32c(packed).to_le_bytes());
        body.extend_from_slice(packed);
    }
    let mut footer = vec![0u8; 80];
    footer[0..4].copy_from_slice(b"NWAN");
    put_u32(&mut footer, 4, 1);
    put_u64(&mut footer, 8, id);
    put_u64(&mut footer, 16, WINDOW_START as u64);
    put_u64(&mut footer, 24, WINDOW_NS as u64);
    put_u64(&mut footer, 32, records.len() as u64);
    put_u64(&mut footer, 40, traces.len() as u64);
    put_u64(&mut footer, 48, blocks.len() as u64);
    put_u64(&mut footer, 56, min as u64);
    put_u64(&mut footer, 64, max as u64);
    let img = Img {
        body,
        table,
        footer,
    };
    let bytes = img.join();
    install(parts, id, &bytes);
    PartMeta {
        part_id: id,
        level: 0,
        window_start_ns: WINDOW_START,
        window_ns: WINDOW_NS,
        rows: records.len() as u64,
        traces: traces.len() as u64,
        min_start_ns: min,
        max_end_ns: max,
        files: [(ANNOTATION_FILE.to_string(), bytes.len() as u64)].into(),
        created_unix_ms: 1,
        format_version: 1,
        payload: Default::default(),
        kind: nwdb_core::PartKind::Annotations,
    }
}

#[test]
fn order_is_checked_across_blocks_and_the_first_key_must_be_right() {
    let tmp = tempfile::tempdir().unwrap();
    let one = |id, ts: &[u128]| {
        let recs: Vec<_> = ts.iter().map(|t| score(*t, 1)).collect();
        one_block(tmp.path(), id, &recs)
    };
    let (b1, b2, b3) = (one(1, &[1, 3]), one(2, &[2, 4]), one(3, &[5, 6]));
    let all = |ts: &[u128]| -> Vec<Annotation> { ts.iter().map(|t| score(*t, 1)).collect() };
    // In order: blocks of 1,3 and 5,6.
    let meta = assemble(
        tmp.path(),
        10,
        &[b1.clone(), b3.clone()],
        &all(&[1, 3, 5, 6]),
    );
    let r = AnnotationReader::open(part_dir(tmp.path(), 10), &meta).unwrap();
    r.verify().unwrap();
    // Blocks of 1,3 and 2,4: the first keys do not decrease, the records do.
    let meta = assemble(
        tmp.path(),
        11,
        &[b1.clone(), b2.clone()],
        &all(&[1, 2, 3, 4]),
    );
    let r = AnnotationReader::open(part_dir(tmp.path(), 11), &meta).unwrap();
    let m = corrupt(r.verify().unwrap_err());
    assert!(m.contains("strictly increasing"), "{m}");
    // The same trace in both blocks with the same identity: not increasing.
    let meta = assemble(
        tmp.path(),
        12,
        &[b1.clone(), b1.clone()],
        &all(&[1, 3, 1, 3]),
    );
    let m = verify_err(tmp.path(), 12, &meta);
    assert!(m.contains("strictly increasing"), "{m}");
    // A table that names another first key than the block starts with, one
    // that is still in order.
    let mut wrong = b3.clone();
    wrong.1 = trace(4);
    let meta = assemble(tmp.path(), 13, &[b1.clone(), wrong], &all(&[1, 3, 5, 6]));
    let m = verify_err(tmp.path(), 13, &meta);
    assert!(m.contains("does not start with"), "{m}");
}

/// Every bit of the file flipped in turn: the part does not open, or fails to
/// verify, or reads exactly what was written.
fn flip_every_bit(parts: &Path, meta: &PartMeta, records: &[Annotation]) -> (usize, usize, usize) {
    let original = bytes_of(parts, meta.part_id);
    let (mut at_open, mut at_verify, mut harmless) = (0, 0, 0);
    for i in 0..original.len() {
        for bit in 0..8 {
            let mut b = original.clone();
            b[i] ^= 1 << bit;
            install(parts, meta.part_id, &b);
            let Ok(r) = AnnotationReader::open(part_dir(parts, meta.part_id), meta) else {
                at_open += 1;
                continue;
            };
            if r.verify().is_err() {
                at_verify += 1;
                continue;
            }
            assert_eq!(read(&r, None), records, "byte {i} bit {bit}");
            harmless += 1;
        }
    }
    install(parts, meta.part_id, &original);
    (at_open, at_verify, harmless)
}

#[test]
fn no_single_bit_flip_gives_a_wrong_record() {
    let tmp = tempfile::tempdir().unwrap();
    // One small block, and two blocks of compressible text.
    let tiny: Vec<_> = (1..=4u128).map(|t| score(t, t as u64)).collect();
    let meta = write_part(tmp.path(), 1, &tiny);
    let (o, v, h) = flip_every_bit(tmp.path(), &meta, &tiny);
    eprintln!("tiny part: {o} refused at open, {v} at verify, {h} harmless");
    let (meta, records) = two_blocks(tmp.path(), 2);
    let (o, v, h) = flip_every_bit(tmp.path(), &meta, &records);
    eprintln!("two blocks: {o} refused at open, {v} at verify, {h} harmless");
}

#[test]
fn truncated_and_extended_files_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, records) = two_blocks(tmp.path(), 1);
    let original = bytes_of(tmp.path(), 1);
    for len in 0..original.len() {
        install(tmp.path(), 1, &original[..len]);
        let r = AnnotationReader::open(part_dir(tmp.path(), 1), &meta);
        assert!(r.is_err(), "{len} of {} bytes opened", original.len());
        assert!(matches!(r.err(), Some(Error::Corrupt(_))));
    }
    let mut rng = StdRng::seed_from_u64(9);
    for extra in [1usize, 36, 80, 81, 1000] {
        let b = [original.clone(), random_bytes(&mut rng, extra)].concat();
        install(tmp.path(), 1, &b);
        assert!(AnnotationReader::open(part_dir(tmp.path(), 1), &meta).is_err());
    }
    // A prefix of zeros in front: refused as well.
    install(tmp.path(), 1, &[vec![0u8; 5], original.clone()].concat());
    assert!(AnnotationReader::open(part_dir(tmp.path(), 1), &meta).is_err());
    install(tmp.path(), 1, &original);
    let r = AnnotationReader::open(part_dir(tmp.path(), 1), &meta).unwrap();
    assert_eq!(read(&r, None), records);
}

#[test]
fn a_file_truncated_after_open_is_corrupt_not_a_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let (meta, _) = two_blocks(tmp.path(), 1);
    let r = AnnotationReader::open(part_dir(tmp.path(), 1), &meta).unwrap();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(file_of(tmp.path(), 1))
        .unwrap();
    f.set_len(10).unwrap();
    assert!(matches!(r.verify(), Err(Error::Corrupt(_))));
}
