//! Parts with a piece store: every value reads back byte for byte, whatever
//! the splitter does, and shared text is stored once.

use arrow_array::{Array, RecordBatch};
use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanColumns, SpanId,
    SpanRecord, TraceId, col, part_dir_name, span_schema,
};
use nwdb_segment::{PIECES_BLK, PIECES_IDX, PartReader, PartWriter, ScanOptions, WriterOptions};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const WINDOW_NS: i64 = 900_000_000_000;

// ------------------------------------------------------------ splitters ---

/// Cuts every value into consecutive chunks of `chunk` bytes. A value that
/// grows at its end therefore shares every full chunk of its earlier self.
struct Chunks(usize);

impl PayloadSplitter for Chunks {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut at = 0;
        while at < value.len() {
            let end = (at + self.0).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

/// Valid random cuts: any gaps, any sizes, adjacent or not.
struct RandomCuts(Mutex<StdRng>);

impl PayloadSplitter for RandomCuts {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut rng = self.0.lock().unwrap();
        let mut at = 0;
        while at < value.len() {
            let start = rng.random_range(at..=value.len());
            if start == value.len() {
                break;
            }
            let end = (start + rng.random_range(1..=300usize)).min(value.len());
            cuts.push(start..end);
            let gap = usize::from(rng.random_bool(0.5));
            at = (end + gap).min(value.len());
        }
    }
}

/// Breaks every rule of the contract, in a different way each call.
struct Hostile(Mutex<u32>);

impl PayloadSplitter for Hostile {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut n = self.0.lock().unwrap();
        *n += 1;
        let len = value.len();
        match *n % 6 {
            0 => cuts.push(0..len + 1),
            1 => cuts.extend([0..len.min(5), 2..len.min(9)]),
            2 => cuts.push(len.min(5)..len.min(5)),
            3 => cuts.extend([len.min(8)..len, 0..len.min(3)]),
            4 => cuts.extend([0..2, 0..len.min(2)]),
            // Valid cuts followed by one outside the value.
            _ => cuts.extend([0..len.min(3), len.saturating_sub(1)..len + 7]),
        }
    }
}

// ----------------------------------------------------------------- data ---

#[derive(Clone, Debug, PartialEq)]
struct Row {
    trace: u8,
    span: u32,
    attrs: Vec<u8>,
    events: Option<Vec<u8>>,
    links: Option<Vec<u8>>,
}

impl Row {
    fn record(&self) -> SpanRecord<'_> {
        SpanRecord {
            trace_id: TraceId([self.trace; 16]),
            span_id: SpanId((self.span as u64 + 1).to_be_bytes()),
            name: "span",
            start_ns: 1_000 + self.span as i64,
            end_ns: 2_000 + self.span as i64,
            attrs: &self.attrs,
            events: self.events.as_deref(),
            links: self.links.as_deref(),
            ..SpanRecord::default()
        }
    }
}

fn batch_of(rows: &[Row]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 30,
        max_bytes: 1 << 40,
    });
    for r in rows {
        b.append(&r.record());
    }
    b.finish()
}

fn write_part(
    parts: &Path,
    id: u64,
    rows: &[Row],
    batch_rows: usize,
    options: WriterOptions,
) -> nwdb_core::PartMeta {
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options).unwrap();
    for chunk in rows.chunks(batch_rows) {
        w.write(&batch_of(chunk)).unwrap();
    }
    w.finish().unwrap()
}

fn part_dir(parts: &Path, id: u64) -> PathBuf {
    parts.join(part_dir_name(id))
}

fn text(rng: &mut StdRng, len: usize) -> Vec<u8> {
    // Words from a small alphabet, so it looks like text and compresses like
    // text, but no long run repeats by chance.
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        for _ in 0..rng.random_range(2..9) {
            out.push(b'a' + rng.random_range(0..26u8));
        }
        out.push(b' ');
    }
    out.truncate(len);
    out
}

/// The rows of `batches`, as owned values.
fn rows_of(batches: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for b in batches {
        assert_eq!(b.schema(), span_schema(), "a read returns the span schema");
        let cols = SpanColumns::try_new(b).unwrap();
        for i in 0..cols.len() {
            let r = cols.record(i);
            out.push(Row {
                trace: r.trace_id.0[0],
                span: (u64::from_be_bytes(r.span_id.0) - 1) as u32,
                attrs: r.attrs.to_vec(),
                events: r.events.map(<[u8]>::to_vec),
                links: r.links.map(<[u8]>::to_vec),
            });
        }
    }
    out
}

/// Reads the whole part three ways and checks each against `rows`.
fn assert_reads_back(dir: &Path, rows: &[Row]) {
    let reader = PartReader::open(dir).unwrap();
    reader.verify().unwrap();

    let by_trace: HashMap<u8, Vec<&Row>> = rows.iter().fold(HashMap::new(), |mut m, r| {
        m.entry(r.trace).or_default().push(r);
        m
    });
    for (trace, want) in &by_trace {
        let got = reader
            .read_trace(TraceId([*trace; 16]), None)
            .unwrap()
            .unwrap();
        let want: Vec<Row> = want.iter().map(|r| (*r).clone()).collect();
        assert_eq!(rows_of(&got), want, "read_trace {trace}");
    }
    let all = reader.read_rows(0, rows.len() as u64, None).unwrap();
    assert_eq!(rows_of(&all), rows, "read_rows");
    let mut scanned = Vec::new();
    for b in reader.scan(ScanOptions::default()).unwrap() {
        scanned.push(b.unwrap());
    }
    assert_eq!(rows_of(&scanned), rows, "scan");
    // A slice in the middle, so that ropes are read from the middle of
    // row groups.
    if rows.len() > 10 {
        let mid = reader.read_rows(3, rows.len() as u64 - 7, None).unwrap();
        assert_eq!(rows_of(&mid), rows[3..rows.len() - 4], "read_rows middle");
    }
}

fn with_splitter(s: impl PayloadSplitter + 'static) -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(s)),
        ..WriterOptions::default()
    }
}

/// Rows of random shapes: empty, null and tiny values, binary noise, text,
/// repeated values.
fn mixed_rows(seed: u64, n: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(seed);
    let shared = text(&mut rng, 2_000);
    (0..n)
        .map(|i| {
            let value = |rng: &mut StdRng| match rng.random_range(0..6) {
                0 => Vec::new(),
                1 => vec![rng.random()],
                2 => {
                    let mut v = vec![0u8; rng.random_range(1..3_000usize)];
                    rng.fill_bytes(&mut v);
                    v
                }
                3 => {
                    let len = rng.random_range(1..5_000usize);
                    text(rng, len)
                }
                4 => shared.clone(),
                _ => shared[..rng.random_range(1..shared.len())].to_vec(),
            };
            let attrs = value(&mut rng);
            let events = rng.random_bool(0.7).then(|| value(&mut rng));
            let links = rng.random_bool(0.3).then(|| value(&mut rng));
            Row {
                trace: (i / 7) as u8,
                span: i as u32,
                attrs,
                events,
                links,
            }
        })
        .collect()
}

// ---------------------------------------------------------------- tests ---

#[test]
fn every_value_reads_back_identical_with_and_without_a_splitter() {
    let rows = mixed_rows(1, 400);
    let tmp = tempfile::tempdir().unwrap();
    let small = |splitter: Option<Arc<dyn PayloadSplitter>>| WriterOptions {
        row_group_rows: 90,
        summary_row_group_rows: 50,
        splitter,
        piece_block_bytes: 8 * 1024,
        ..WriterOptions::default()
    };
    let splitters: Vec<Option<Arc<dyn PayloadSplitter>>> = vec![
        None,
        Some(Arc::new(Chunks(64))),
        Some(Arc::new(Chunks(1))),
        Some(Arc::new(Chunks(100_000))),
        Some(Arc::new(RandomCuts(Mutex::new(StdRng::seed_from_u64(4))))),
    ];
    for (i, s) in splitters.into_iter().enumerate() {
        let id = i as u64 + 1;
        let meta = write_part(tmp.path(), id, &rows, 37, small(s.clone()));
        assert_eq!(s.is_none(), meta.payload.pieces == 0, "splitter {i}");
        assert_reads_back(&part_dir(tmp.path(), id), &rows);
        let reader = PartReader::open(part_dir(tmp.path(), id)).unwrap();
        assert_eq!(reader.payload_totals(), meta.payload);
    }
}

#[test]
fn a_window_far_smaller_than_the_unique_text_loses_nothing() {
    let rows = mixed_rows(2, 300);
    let tmp = tempfile::tempdir().unwrap();
    let options = |window: usize| WriterOptions {
        splitter: Some(Arc::new(Chunks(128))),
        dedup_window_bytes: window,
        piece_block_bytes: 2_048,
        ..WriterOptions::default()
    };
    let wide = write_part(tmp.path(), 1, &rows, 50, options(64 << 20));
    let narrow = write_part(tmp.path(), 2, &rows, 50, options(4_096));
    let none = write_part(tmp.path(), 3, &rows, 50, options(0));
    assert!(wide.payload.unique_bytes < wide.payload.logical_bytes);
    // Unique text is many times the narrow window.
    assert!(narrow.payload.unique_bytes > 20 * 4_096);
    // A smaller window finds fewer repeats but never more than a bigger one.
    assert!(narrow.payload.unique_bytes >= wide.payload.unique_bytes);
    assert!(none.payload.unique_bytes >= narrow.payload.unique_bytes);
    assert_eq!(wide.payload.logical_bytes, narrow.payload.logical_bytes);
    for id in 1..=3 {
        assert_reads_back(&part_dir(tmp.path(), id), &rows);
    }
}

#[test]
fn a_splitter_that_breaks_the_contract_gets_its_values_stored_whole() {
    let rows = mixed_rows(3, 200);
    let tmp = tempfile::tempdir().unwrap();
    let meta = write_part(
        tmp.path(),
        1,
        &rows,
        20,
        with_splitter(Hostile(Mutex::new(0))),
    );
    // Some values have no valid cuts and every value must read back; the
    // values whose garbage happened to be valid may have been cut.
    assert_reads_back(&part_dir(tmp.path(), 1), &rows);
    eprintln!("hostile splitter, pieces stored: {}", meta.payload.pieces);

    // A splitter that is only ever wrong stores nothing out of line.
    struct AlwaysWrong;
    impl PayloadSplitter for AlwaysWrong {
        fn cuts(&self, _: RawColumn, v: &[u8], cuts: &mut Vec<Range<usize>>) {
            cuts.push(0..v.len() + 1);
        }
    }
    let meta = write_part(tmp.path(), 2, &rows, 20, with_splitter(AlwaysWrong));
    assert_eq!(meta.payload.pieces, 0);
    assert_reads_back(&part_dir(tmp.path(), 2), &rows);
}

/// A conversation that grows by a message on every call, a system prompt and
/// tool definitions shared by every trace, and large tool results.
fn agent_rows(seed: u64, traces: u8, calls: u32) -> Vec<Row> {
    const CHUNK: usize = 256;
    let mut rng = StdRng::seed_from_u64(seed);
    let system = text(&mut rng, 8 * CHUNK);
    let tools = text(&mut rng, 24 * CHUNK);
    let mut rows = Vec::new();
    let mut span = 0;
    for t in 0..traces {
        let mut conversation = Vec::new();
        for _ in 0..calls {
            let mut call = system.clone();
            call.extend_from_slice(&tools);
            conversation.extend(text(&mut rng, 3 * CHUNK));
            call.extend_from_slice(&conversation);
            // The model's own answer and a large tool result, new each time.
            let output = text(&mut rng, 4 * CHUNK);
            let result = text(&mut rng, 16 * CHUNK);
            conversation.extend_from_slice(&output);
            conversation.extend_from_slice(&result);
            rows.push(Row {
                trace: t,
                span,
                attrs: call,
                events: Some(output),
                links: None,
            });
            span += 1;
        }
    }
    rows
}

#[test]
fn agent_shaped_traces_shrink_and_read_back_identical() {
    let rows = agent_rows(7, 4, 24);
    let tmp = tempfile::tempdir().unwrap();
    let plain = write_part(tmp.path(), 1, &rows, 16, WriterOptions::default());
    let split = write_part(tmp.path(), 2, &rows, 16, with_splitter(Chunks(256)));
    assert_reads_back(&part_dir(tmp.path(), 2), &rows);

    let total = |m: &nwdb_core::PartMeta| m.files.values().sum::<u64>();
    let p = split.payload;
    eprintln!(
        "agent data: logical {} unique {} stored {} pieces {}; part {} bytes, without the store {}",
        p.logical_bytes,
        p.unique_bytes,
        p.stored_bytes,
        p.pieces,
        total(&split),
        total(&plain)
    );
    // Most of what the rows carry repeats, so the distinct text is a small
    // fraction of it, and what is stored is smaller still.
    assert!(p.unique_bytes * 8 < p.logical_bytes, "{p:?}");
    assert!(p.stored_bytes * 10 < p.logical_bytes, "{p:?}");
    // And the whole part is much smaller than one that stores every value in
    // its rows, even though zstd already finds repeats inside a row group.
    assert!(total(&split) * 4 < total(&plain) * 3, "{p:?}");
}

#[test]
fn low_repetition_text_costs_only_a_few_percent() {
    let mut rng = StdRng::seed_from_u64(9);
    let rows: Vec<Row> = (0..300)
        .map(|i| Row {
            trace: (i / 10) as u8,
            span: i as u32,
            attrs: text(&mut rng, 3_000),
            events: Some(text(&mut rng, 1_500)),
            links: None,
        })
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let plain = write_part(tmp.path(), 1, &rows, 50, WriterOptions::default());
    let split = write_part(tmp.path(), 2, &rows, 50, with_splitter(Chunks(256)));
    assert_reads_back(&part_dir(tmp.path(), 2), &rows);
    let total = |m: &nwdb_core::PartMeta| m.files.values().sum::<u64>();
    eprintln!(
        "low repetition: logical {} unique {} stored {}; part {} bytes, without the store {} ({:.3})",
        split.payload.logical_bytes,
        split.payload.unique_bytes,
        split.payload.stored_bytes,
        total(&split),
        total(&plain),
        total(&split) as f64 / total(&plain) as f64
    );
    assert!(
        (total(&split) as f64) < total(&plain) as f64 * 1.05,
        "{} vs {}",
        total(&split),
        total(&plain)
    );
}

#[test]
fn a_read_without_the_raw_columns_never_touches_the_piece_files() {
    let rows = agent_rows(3, 2, 6);
    let tmp = tempfile::tempdir().unwrap();
    write_part(tmp.path(), 1, &rows, 5, with_splitter(Chunks(256)));
    let dir = part_dir(tmp.path(), 1);

    // Damage both piece files after the fact: only a read that needs them
    // can notice. `open` checks their lengths, so keep those.
    let reader = PartReader::open(&dir).unwrap();
    assert_eq!(reader.payload_blocks_loaded(), 0);
    let names = [col::NAME, col::START_NS, col::SERVICE_NAME];
    let wanted = [col::TRACE_ID, col::INPUT_TOKENS, col::DROPPED_LINKS_COUNT];
    for projection in [&names[..], &wanted[..]] {
        reader
            .read_trace(TraceId([0; 16]), Some(projection))
            .unwrap()
            .unwrap();
        reader.read_rows(0, 7, Some(projection)).unwrap();
        let scan = ScanOptions {
            projection: Some(projection.iter().map(|s| s.to_string()).collect()),
            ..ScanOptions::default()
        };
        for b in reader.scan(scan).unwrap() {
            b.unwrap();
        }
    }
    assert_eq!(reader.payload_blocks_loaded(), 0, "nothing was read");

    // One raw column among the others is enough to need the store.
    for raw in [col::ATTRS, col::EVENTS, col::LINKS] {
        let before = reader.payload_blocks_loaded();
        let got = reader
            .read_trace(TraceId([0; 16]), Some(&[col::NAME, raw]))
            .unwrap()
            .unwrap();
        assert_eq!(got[0].num_columns(), 2);
        if raw != col::LINKS {
            assert!(reader.payload_blocks_loaded() > before, "{raw}");
        }
    }

    // Now really break the blocks: a projection without raw columns still
    // reads, one with them reports the damage.
    let mut blk = std::fs::read(dir.join(PIECES_BLK)).unwrap();
    for i in (0..blk.len()).step_by(7) {
        blk[i] ^= 0xff;
    }
    std::fs::write(dir.join(PIECES_BLK), &blk).unwrap();
    let reader = PartReader::open(&dir).unwrap();
    reader
        .read_trace(TraceId([0; 16]), Some(&[col::NAME]))
        .unwrap()
        .unwrap();
    match reader.read_trace(TraceId([0; 16]), None) {
        Err(Error::Corrupt(m)) => assert!(m.contains(PIECES_BLK), "{m}"),
        other => panic!("expected Corrupt, got {:?}", other.map(|_| ())),
    }
    match reader.verify() {
        Err(Error::Corrupt(m)) => assert!(m.contains(PIECES_BLK), "{m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn rebuilt_batches_respect_the_limits_in_rebuilt_bytes() {
    // 40 rows whose raw values are 1 MiB each but whose ropes are a few
    // bytes: the writer sees small rows, a reader must not return them in
    // one batch.
    const MIB: usize = 1 << 20;
    let mut rng = StdRng::seed_from_u64(5);
    let big = text(&mut rng, MIB);
    let mut rows: Vec<Row> = (0..40)
        .map(|i| Row {
            trace: 1,
            span: i,
            attrs: big.clone(),
            events: None,
            links: None,
        })
        .collect();
    // One row larger than the whole limit, between small ones.
    rows.push(Row {
        trace: 2,
        span: 100,
        attrs: vec![b'q'; 33 * MIB],
        events: None,
        links: None,
    });
    rows.push(Row {
        trace: 2,
        span: 101,
        attrs: b"small".to_vec(),
        events: None,
        links: None,
    });
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(MIB))),
        row_group_rows: 100,
        ..WriterOptions::default()
    };
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, options).unwrap();
    for r in &rows {
        w.write(&batch_of(std::slice::from_ref(r))).unwrap();
    }
    let meta = w.finish().unwrap();
    // The rows are 74 MiB of values in the file's terms, a few KiB stored.
    assert!(
        meta.payload.stored_bytes < 3 * MIB as u64,
        "{:?}",
        meta.payload
    );
    drop(rows);

    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let limit = BatchLimits::DEFAULT.max_bytes;
    let raw_bytes = |b: &RecordBatch| -> usize {
        [col::ATTRS, col::EVENTS, col::LINKS]
            .iter()
            .map(|c| {
                let a = b
                    .column_by_name(c)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap();
                (0..a.len())
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i).len())
                    .sum::<usize>()
            })
            .sum()
    };
    let check = |batches: Vec<RecordBatch>, what: &str| {
        let sizes: Vec<(usize, usize)> = batches
            .iter()
            .map(|b| (b.num_rows(), raw_bytes(b)))
            .collect();
        let mut seen = 0;
        for (n, bytes) in &sizes {
            seen += n;
            assert!(
                *bytes <= limit || *n == 1,
                "{what}: a batch of {n} rows holds {bytes} bytes: {sizes:?}"
            );
        }
        assert_eq!(seen, 42, "{what}");
        assert!(sizes.len() >= 3, "{what}: {sizes:?}");
    };
    check(reader.read_rows(0, 42, None).unwrap(), "read_rows");
    check(
        reader
            .scan(ScanOptions::default())
            .unwrap()
            .map(Result::unwrap)
            .collect(),
        "scan",
    );
    let t1 = reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap();
    assert_eq!(t1.iter().map(|b| b.num_rows()).sum::<usize>(), 40);
    assert!(t1.len() >= 2, "40 MiB cannot be one batch");
    let t2 = reader.read_trace(TraceId([2; 16]), None).unwrap().unwrap();
    assert_eq!(
        t2.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        [1, 1],
        "the huge row is a batch of its own"
    );
    // The values are the right ones.
    let first = rows_of(&t1[..1]);
    assert_eq!(first[0].attrs, big);
    let huge = rows_of(&t2[..1]);
    assert!(huge[0].attrs.iter().all(|&b| b == b'q') && huge[0].attrs.len() == 33 * MIB);
}

// --------------------------------------------------------------- damage ---

fn small_part(tmp: &Path) -> (PathBuf, Vec<Row>) {
    let rows = agent_rows(11, 2, 4);
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(256))),
        piece_block_bytes: 4_096,
        ..WriterOptions::default()
    };
    write_part(tmp, 1, &rows, 3, options);
    (part_dir(tmp, 1), rows)
}

fn copy_part(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for f in std::fs::read_dir(src).unwrap() {
        let f = f.unwrap();
        std::fs::copy(f.path(), dst.join(f.file_name())).unwrap();
    }
}

fn corrupt_naming(err: Error, file: &str) {
    match err {
        Error::Corrupt(m) => assert!(m.contains(file), "{file}: {m}"),
        other => panic!("expected Corrupt naming {file}, got {other:?}"),
    }
}

#[test]
fn truncated_piece_files_are_refused_naming_the_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (good, _) = small_part(tmp.path());
    let mut rng = StdRng::seed_from_u64(1);
    for file in [PIECES_BLK, PIECES_IDX] {
        let original = std::fs::read(good.join(file)).unwrap();
        let mut lengths: Vec<usize> = (0..=80.min(original.len() - 1)).collect();
        lengths.extend((original.len().saturating_sub(40)..original.len()).collect::<Vec<_>>());
        lengths.extend((0..60).map(|_| rng.random_range(0..original.len())));
        let scratch = tempfile::tempdir().unwrap();
        let dir = scratch.path().join("p");
        copy_part(&good, &dir);
        for n in lengths {
            std::fs::write(dir.join(file), &original[..n]).unwrap();
            match PartReader::open(&dir) {
                Err(e) => corrupt_naming(e, file),
                Ok(_) => panic!("{file} cut to {n} bytes was accepted"),
            }
        }
        // Longer is refused too.
        let mut longer = original.clone();
        longer.push(0);
        std::fs::write(dir.join(file), &longer).unwrap();
        corrupt_naming(PartReader::open(&dir).err().unwrap(), file);
    }
    // A missing file.
    for file in [PIECES_BLK, PIECES_IDX] {
        let scratch = tempfile::tempdir().unwrap();
        let dir = scratch.path().join("p");
        copy_part(&good, &dir);
        std::fs::remove_file(dir.join(file)).unwrap();
        corrupt_naming(PartReader::open(&dir).err().unwrap(), file);
    }
}

#[test]
fn flipped_bits_in_piece_files_never_return_wrong_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let (good, rows) = small_part(tmp.path());
    let scratch = tempfile::tempdir().unwrap();
    let dir = scratch.path().join("p");
    copy_part(&good, &dir);

    for file in [PIECES_BLK, PIECES_IDX] {
        let original = std::fs::read(good.join(file)).unwrap();
        for pos in 0..original.len() {
            let mut bytes = original.clone();
            bytes[pos] ^= 1 << (pos % 8);
            std::fs::write(dir.join(file), &bytes).unwrap();
            match PartReader::open(&dir) {
                // A damaged table is refused at open.
                Err(e) => corrupt_naming(e, file),
                Ok(reader) => {
                    // Damaged blocks are found when read or verified, and a
                    // read that succeeds returns the original bytes.
                    corrupt_naming(reader.verify().expect_err("flip undetected"), file);
                    for t in [0u8, 1] {
                        match reader.read_trace(TraceId([t; 16]), None) {
                            Err(e) => corrupt_naming(e, file),
                            Ok(got) => {
                                let want: Vec<Row> =
                                    rows.iter().filter(|r| r.trace == t).cloned().collect();
                                assert_eq!(rows_of(&got.unwrap()), want, "{file} byte {pos}");
                            }
                        }
                    }
                }
            }
        }
        std::fs::copy(good.join(file), dir.join(file)).unwrap();
    }
}

#[test]
fn the_recorded_checksums_cover_the_piece_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (good, _) = small_part(tmp.path());
    // Replace the blocks by another part's: same name, wrong content.
    let other = tempfile::tempdir().unwrap();
    let rows = agent_rows(99, 2, 4);
    let options = WriterOptions {
        splitter: Some(Arc::new(Chunks(256))),
        piece_block_bytes: 4_096,
        ..WriterOptions::default()
    };
    write_part(other.path(), 1, &rows, 3, options);
    let scratch = tempfile::tempdir().unwrap();
    let dir = scratch.path().join("p");
    copy_part(&good, &dir);
    std::fs::copy(
        part_dir(other.path(), 1).join(PIECES_IDX),
        dir.join(PIECES_IDX),
    )
    .unwrap();
    corrupt_naming(PartReader::open(&dir).err().unwrap(), PIECES_IDX);
}
