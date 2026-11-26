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
        // Cuts of one byte are pieces here: the values must read back whatever
        // the cuts are, so the floor that would store them inline is lowered.
        min_piece_bytes: 1,
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

    // The text index and the row lists are not part of what sharing text
    // saves; `search.rs` measures them.
    let total = |m: &nwdb_core::PartMeta| {
        m.files
            .iter()
            .filter(|(name, _)| !name.starts_with("text."))
            .map(|(_, size)| size)
            .sum::<u64>()
    };
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
    // The text index and the row lists are not part of what sharing text
    // saves; `search.rs` measures them.
    let total = |m: &nwdb_core::PartMeta| {
        m.files
            .iter()
            .filter(|(name, _)| !name.starts_with("text."))
            .map(|(_, size)| size)
            .sum::<u64>()
    };
    eprintln!(
        "low repetition: logical {} unique {} stored {}; part {} bytes, without the store {} ({:.3})",
        split.payload.logical_bytes,
        split.payload.unique_bytes,
        split.payload.stored_bytes,
        total(&split),
        total(&plain),
        total(&split) as f64 / total(&plain) as f64
    );
    // Nothing repeats, so sharing finds (almost) nothing: the distinct text is
    // what the rows refer to, and what is stored is what compression leaves.
    assert!(
        split.payload.unique_bytes * 10 >= split.payload.logical_bytes * 9,
        "{:?}",
        split.payload
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
    // The rows are 74 MiB of values in the file's terms; the distinct text is
    // two pieces of 1 MiB (the shared value and the run of `q`), whatever
    // compression does to them.
    assert!(
        meta.payload.logical_bytes > 70 * MIB as u64,
        "{:?}",
        meta.payload
    );
    assert!(
        meta.payload.unique_bytes <= 2 * MIB as u64 + 16,
        "{:?}",
        meta.payload
    );
    drop(rows);

    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let limit = BatchLimits::DEFAULT.max_bytes;
    // The measure of `BatchLimits`: every string and binary value of every
    // column of every row (the builder counts them as it appends).
    let raw_bytes = |b: &RecordBatch| -> usize {
        let cols = SpanColumns::try_new(b).unwrap();
        let mut all = SpanBatchBuilder::new(BatchLimits {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        });
        for i in 0..cols.len() {
            all.append(&cols.record(i));
        }
        all.value_bytes()
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

/// A part of one trace with two spans whose `attrs` are 2 KiB of random
/// bytes, cut into 512-byte pieces. Parts made from different seeds have piece
/// files of equal lengths (random bytes do not compress) and different bytes.
fn random_part(parts: &Path, id: u64, seed: u64) -> PathBuf {
    let mut rng = StdRng::seed_from_u64(seed);
    let rows: Vec<Row> = (0..2)
        .map(|i| {
            let mut attrs = vec![0u8; 2048];
            rng.fill_bytes(&mut attrs);
            Row {
                trace: 1,
                span: i,
                attrs,
                events: None,
                links: None,
            }
        })
        .collect();
    write_part(parts, id, &rows, 2, with_splitter(Chunks(512)));
    part_dir(parts, id)
}

#[test]
fn the_recorded_checksums_cover_the_piece_files() {
    let tmp = tempfile::tempdir().unwrap();
    let good = random_part(tmp.path(), 1, 1);
    let other = random_part(tmp.path(), 2, 2);
    for f in [PIECES_BLK, PIECES_IDX] {
        assert_eq!(
            std::fs::metadata(good.join(f)).unwrap().len(),
            std::fs::metadata(other.join(f)).unwrap().len(),
            "{f}: the files must have equal lengths, or only the lengths are being tested"
        );
        assert_ne!(
            std::fs::read(good.join(f)).unwrap(),
            std::fs::read(other.join(f)).unwrap(),
            "{f}"
        );
    }
    let scratch = tempfile::tempdir().unwrap();
    let dir = scratch.path().join("p");

    // The index of another part, of the same length: refused at open, naming
    // the file, because `traces.idx` records its checksum.
    copy_part(&good, &dir);
    std::fs::copy(other.join(PIECES_IDX), dir.join(PIECES_IDX)).unwrap();
    corrupt_naming(PartReader::open(&dir).err().unwrap(), PIECES_IDX);

    // Both files of another part: also refused at open, since the index holds
    // the checksum of every block and so binds the blocks too.
    std::fs::copy(other.join(PIECES_BLK), dir.join(PIECES_BLK)).unwrap();
    corrupt_naming(PartReader::open(&dir).err().unwrap(), PIECES_IDX);

    // The blocks of another part under this part's index: open cannot know
    // without reading them, but no read returns their bytes, and `verify`
    // names the file.
    copy_part(&good, &dir);
    std::fs::copy(other.join(PIECES_BLK), dir.join(PIECES_BLK)).unwrap();
    let reader = PartReader::open(&dir).unwrap();
    corrupt_naming(
        reader.read_trace(TraceId([1; 16]), None).unwrap_err(),
        PIECES_BLK,
    );
    corrupt_naming(reader.verify().unwrap_err(), PIECES_BLK);
}

// ------------------------------------------- floor, ceiling, by block ---

fn piece_lengths(dir: &Path) -> Vec<u64> {
    let reader = PartReader::open(dir).unwrap();
    let store = reader.piece_store();
    (0..store.totals().pieces as u32)
        .map(|id| store.piece_len(id).unwrap())
        .collect()
}

#[test]
fn a_cut_shorter_than_the_floor_stays_inline() {
    let rows = mixed_rows(21, 120);
    let tmp = tempfile::tempdir().unwrap();
    // The default floor is 8 bytes: cuts of 7 are never pieces.
    let seven = write_part(tmp.path(), 1, &rows, 20, with_splitter(Chunks(7)));
    assert_eq!(seven.payload.pieces, 0);
    assert_eq!(seven.payload.logical_bytes, 0);
    // Cuts of 8 are, and a short last chunk of a value stays inline.
    let eight = write_part(tmp.path(), 2, &rows, 20, with_splitter(Chunks(8)));
    assert!(eight.payload.pieces > 0);
    assert!(
        piece_lengths(&part_dir(tmp.path(), 2))
            .iter()
            .all(|&l| l >= 8)
    );
    // The floor is an option: with it lowered, cuts of 7 are pieces.
    let low = WriterOptions {
        min_piece_bytes: 1,
        ..with_splitter(Chunks(7))
    };
    let seven_low = write_part(tmp.path(), 3, &rows, 20, low);
    assert!(seven_low.payload.pieces > 0);
    for id in 1..=3 {
        assert_reads_back(&part_dir(tmp.path(), id), &rows);
    }
}

#[test]
fn the_piece_ceiling_stops_new_pieces_but_pieces_that_exist_are_still_shared() {
    let mut rng = StdRng::seed_from_u64(33);
    // 75 rows of four distinct 64-byte chunks, then the first 25 of them
    // again: the first 100 chunks fill the part, the next 200 do not fit, and
    // the repeats of the first 100 must still be references.
    let chunks: Vec<Vec<u8>> = (0..300)
        .map(|_| {
            let mut c = vec![0u8; 64];
            rng.fill_bytes(&mut c);
            c
        })
        .collect();
    let row = |i: usize, from: usize| Row {
        trace: (i / 10) as u8,
        span: i as u32,
        attrs: chunks[from * 4..from * 4 + 4].concat(),
        events: None,
        links: None,
    };
    let rows: Vec<Row> = (0..75)
        .map(|i| row(i, i))
        .chain((0..25).map(|i| row(75 + i, i)))
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        max_pieces: 100,
        ..with_splitter(Chunks(64))
    };
    let meta = write_part(tmp.path(), 1, &rows, 10, options);
    assert_eq!(meta.payload.pieces, 100);
    assert_eq!(meta.payload.unique_bytes, 100 * 64);
    // 100 references that made the pieces and 100 that found them again; the
    // 200 chunks that did not fit are inline and are no reference.
    assert_eq!(meta.payload.logical_bytes, 200 * 64);
    // Everything reads back, and the ropes agree with the store (`verify`).
    assert_reads_back(&part_dir(tmp.path(), 1), &rows);

    // With a ceiling of nothing, no piece is ever made.
    let none = WriterOptions {
        max_pieces: 0,
        ..with_splitter(Chunks(64))
    };
    let meta = write_part(tmp.path(), 2, &rows, 10, none);
    assert_eq!(meta.payload.pieces, 0);
    assert_reads_back(&part_dir(tmp.path(), 2), &rows);
}

struct Announced(Mutex<Vec<(u32, Vec<u8>)>>);

impl nwdb_segment::PieceObserver for Announced {
    fn piece_stored(&self, id: u32, bytes: &[u8]) {
        self.0.lock().unwrap().push((id, bytes.to_vec()));
    }
}

#[test]
fn the_observer_hears_of_each_piece_once_when_it_is_first_stored() {
    let rows = agent_rows(5, 2, 6);
    let tmp = tempfile::tempdir().unwrap();
    let seen = Arc::new(Announced(Mutex::new(Vec::new())));
    let options = WriterOptions {
        piece_observer: Some(seen.clone()),
        ..with_splitter(Chunks(256))
    };
    let meta = write_part(tmp.path(), 1, &rows, 5, options);
    let seen = seen.0.lock().unwrap();
    // Every distinct piece, once, in the order of the ids, though the same
    // text was referenced many times.
    assert_eq!(seen.len() as u64, meta.payload.pieces);
    assert!(meta.payload.logical_bytes > 4 * meta.payload.unique_bytes);
    assert!(
        seen.iter()
            .enumerate()
            .all(|(n, (id, _))| *id as usize == n)
    );
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let store = reader.piece_store();
    let mut session = store.session();
    for (id, bytes) in seen.iter() {
        let mut out = Vec::new();
        store.read_piece(&mut session, *id, &mut out).unwrap();
        assert_eq!(&out, bytes);
    }
}

#[test]
fn stored_rows_list_their_pieces_and_pieces_are_fetched_by_id() {
    let rows = agent_rows(8, 2, 5);
    let tmp = tempfile::tempdir().unwrap();
    write_part(tmp.path(), 1, &rows, 4, with_splitter(Chunks(256)));
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let n = rows.len() as u64;
    let before = reader.payload_blocks_loaded();
    let stored = reader.read_rows_stored(0, n, None).unwrap();
    assert_eq!(reader.payload_blocks_loaded(), before, "no piece is read");
    let rebuilt = rows_of(&reader.read_rows(0, n, None).unwrap());

    let store = reader.piece_store();
    let mut refs = 0u64;
    let mut at = 0;
    for batch in &stored {
        for column in [col::ATTRS, col::EVENTS, col::LINKS] {
            let a = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .unwrap();
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                for id in nwdb_payload::rope::piece_ids(a.value(i)) {
                    refs += store.piece_len(id.unwrap()).unwrap();
                }
                // The rope, rebuilt by hand from the store, is the value.
                let mut value = Vec::new();
                let mut session = store.session();
                nwdb_payload::rope::decode(
                    a.value(i),
                    |id, out| store.read_piece(&mut session, id, out),
                    &mut value,
                )
                .unwrap();
                let want = &rebuilt[at + i];
                let expected = match column {
                    col::ATTRS => Some(&want.attrs),
                    col::EVENTS => want.events.as_ref(),
                    _ => want.links.as_ref(),
                };
                assert_eq!(Some(&value), expected, "row {} {column}", at + i);
            }
        }
        at += batch.num_rows();
    }
    // Every reference of the part was seen.
    assert_eq!(refs, store.totals().logical_bytes);
}

#[test]
fn many_threads_reading_one_part_get_what_one_thread_gets() {
    let rows = agent_rows(13, 6, 10);
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        row_group_rows: 15,
        piece_block_bytes: 4_096,
        ..with_splitter(Chunks(256))
    };
    write_part(tmp.path(), 1, &rows, 7, options);
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let single: Vec<Vec<Row>> = (0..6u8)
        .map(|t| rows_of(&reader.read_trace(TraceId([t; 16]), None).unwrap().unwrap()))
        .collect();
    for (t, got) in single.iter().enumerate() {
        let want: Vec<Row> = rows
            .iter()
            .filter(|r| r.trace == t as u8)
            .cloned()
            .collect();
        assert_eq!(*got, want, "trace {t}");
    }
    std::thread::scope(|scope| {
        for seed in 0..8u64 {
            let (reader, single, rows) = (&reader, &single, &rows);
            scope.spawn(move || {
                let mut rng = StdRng::seed_from_u64(seed);
                for _ in 0..40 {
                    let t = rng.random_range(0..6u8);
                    let got = reader.read_trace(TraceId([t; 16]), None).unwrap().unwrap();
                    assert_eq!(rows_of(&got), single[t as usize], "trace {t}");
                    // And ranges that cut across traces and row groups.
                    let first = rng.random_range(0..rows.len() as u64 - 1);
                    let count = rng.random_range(1..=(rows.len() as u64 - first).min(20));
                    let got = reader.read_rows(first, count, None).unwrap();
                    assert_eq!(
                        rows_of(&got),
                        rows[first as usize..(first + count) as usize],
                        "rows {first}+{count}"
                    );
                }
            });
        }
    });
}

#[test]
fn references_spread_over_many_blocks_decompress_each_block_once_per_batch() {
    const PIECE: usize = 8 * 1024;
    const PIECES: usize = 100;
    const ROWS: usize = 20;
    const REFS: usize = 300;
    let mut rng = StdRng::seed_from_u64(44);
    let pieces: Vec<Vec<u8>> = (0..PIECES)
        .map(|_| {
            let mut p = vec![0u8; PIECE];
            rng.fill_bytes(&mut p);
            p
        })
        .collect();
    // Trace 1 makes the pieces, one to a block. Trace 2 has 20 rows of 300
    // references each, going round and round all 100 blocks in a different
    // order on each row: 6,000 references, 2.4 MiB a row, two batches.
    let mut rows: Vec<Row> = pieces
        .iter()
        .enumerate()
        .map(|(i, p)| Row {
            trace: 1,
            span: i as u32,
            attrs: p.clone(),
            events: None,
            links: None,
        })
        .collect();
    for r in 0..ROWS {
        let attrs: Vec<u8> = (0..REFS)
            .flat_map(|k| pieces[(r * 7 + k * 13) % PIECES].clone())
            .collect();
        rows.push(Row {
            trace: 2,
            span: (PIECES + r) as u32,
            attrs,
            events: None,
            links: None,
        });
    }
    let tmp = tempfile::tempdir().unwrap();
    let options = WriterOptions {
        piece_block_bytes: PIECE,
        ..with_splitter(Chunks(PIECE))
    };
    let meta = write_part(tmp.path(), 1, &rows, 5, options);
    assert_eq!(meta.payload.pieces, PIECES as u64);

    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let blocks = reader.piece_store().block_count() as u64;
    assert_eq!(blocks, PIECES as u64);
    let before = reader.payload_blocks_loaded();
    let got = reader.read_trace(TraceId([2; 16]), None).unwrap().unwrap();
    let loads = reader.payload_blocks_loaded() - before;
    eprintln!(
        "{} references over {blocks} blocks, {} batches: {loads} block decompressions",
        ROWS * REFS,
        got.len()
    );
    assert!(got.len() >= 2, "the rows do not fit one batch");
    assert!(
        loads <= blocks * got.len() as u64,
        "{loads} decompressions for {} batches of {blocks} blocks",
        got.len()
    );
    // And the values are what was written.
    let want: Vec<Row> = rows.iter().filter(|r| r.trace == 2).cloned().collect();
    assert_eq!(rows_of(&got), want);
    // The same through the other read path.
    let all = reader.read_rows(PIECES as u64, ROWS as u64, None).unwrap();
    assert_eq!(rows_of(&all), want);
}
