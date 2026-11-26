//! End-to-end tests of writing and reading a part.

use arrow_array::RecordBatch;
use nwdb_core::{
    BatchLimits, Error, PartMeta, SpanBatchBuilder, SpanColumns, SpanId, SpanRecord,
    SummaryBatchBuilder, SummaryBuilder, SummaryColumns, TraceId, TraceSummary, part_dir_name,
    span_schema, summary_schema,
};
use nwdb_segment::{
    INDEX_FILE, PIECES_BLK, PIECES_IDX, PartReader, PartWriter, SPANS_FILE, SUMMARY_FILE,
    ScanOptions, TEXT_INDEX_FILE, TEXT_ROWS_FILE, WriterOptions,
};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;
const MIB: usize = 1 << 20;

// ---------------------------------------------------------------- data ---

/// An owned span, so tests can keep the expected data after the batches that
/// carried it are gone.
#[derive(Clone, Debug)]
struct Span {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: Option<SpanId>,
    name: String,
    kind: i32,
    start_ns: i64,
    end_ns: i64,
    status_code: i32,
    strings: [Option<String>; 16],
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    resource: Option<Vec<u8>>,
    scope: Option<Vec<u8>>,
    attrs: Vec<u8>,
    events: Option<Vec<u8>>,
    links: Option<Vec<u8>>,
    flags: u32,
    dropped: [u32; 3],
    arrival: u64,
}

impl Span {
    /// Order of `strings`: status_message, trace_state, service_name,
    /// scope_name, scope_version, operation, provider, request_model,
    /// response_model, conversation_id, session_id, user_id, agent_name,
    /// tool_name, resource_schema_url, scope_schema_url. `error_type` is derived from `status_code`.
    fn record(&self) -> SpanRecord<'_> {
        let s = |i: usize| self.strings[i].as_deref();
        SpanRecord {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name: &self.name,
            kind: self.kind,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
            status_code: self.status_code,
            status_message: s(0),
            trace_state: s(1),
            flags: self.flags,
            service_name: s(2),
            scope_name: s(3),
            scope_version: s(4),
            operation: s(5),
            provider: s(6),
            request_model: s(7),
            response_model: s(8),
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            conversation_id: s(9),
            session_id: s(10),
            user_id: s(11),
            agent_name: s(12),
            tool_name: s(13),
            error_type: (self.status_code == 2).then_some("boom"),
            resource: self.resource.as_deref(),
            scope: self.scope.as_deref(),
            resource_schema_url: s(14),
            scope_schema_url: s(15),
            attrs: &self.attrs,
            events: self.events.as_deref(),
            links: self.links.as_deref(),
            dropped_attributes_count: self.dropped[0],
            dropped_events_count: self.dropped[1],
            dropped_links_count: self.dropped[2],
            arrival: self.arrival,
        }
    }
}

const WORDS: [&str; 8] = [
    "plain",
    "héllo wörld",
    "日本語のスパン",
    "emoji 🦉🔭",
    "",
    "with \"quotes\" and \n newline",
    "Ünïcödé-ñ",
    "x",
];

fn word(rng: &mut StdRng) -> String {
    WORDS[rng.random_range(0..WORDS.len())].to_string()
}

fn opt<T>(rng: &mut StdRng, v: impl FnOnce(&mut StdRng) -> T) -> Option<T> {
    rng.random_bool(0.65).then(|| v(rng))
}

fn bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

/// What to generate.
struct Gen {
    seed: u64,
    traces: usize,
    spans_per_trace: std::ops::RangeInclusive<usize>,
    /// How many spans carry a 1 MiB `attrs`.
    big_attrs: usize,
    /// Start time of trace `i`; its spans start within 500 ns after it.
    trace_start: fn(usize) -> i64,
}

/// Spans of `traces` traces, trace after trace, each trace contiguous, trace
/// ids in random order.
fn generate(g: &Gen) -> Vec<Span> {
    let mut rng = StdRng::seed_from_u64(g.seed);
    let mut out = Vec::new();
    for t in 0..g.traces {
        let mut id = [0u8; 16];
        rng.fill_bytes(&mut id);
        // The trace number makes ids unique; the reversal puts it last so
        // that file order and id order differ.
        id[0..4].copy_from_slice(&(t as u32).to_be_bytes());
        id.reverse();
        let trace_id = TraceId(id);
        let n = rng.random_range(g.spans_per_trace.clone());
        for j in 0..n {
            let start_ns = (g.trace_start)(t) + rng.random_range(0..500);
            let attrs_len = match rng.random_range(0..4) {
                0 => 0,
                1 => rng.random_range(1..64),
                _ => rng.random_range(64..2048),
            };
            let strings: [Option<String>; 16] = std::array::from_fn(|_| opt(&mut rng, word));
            let status_code = rng.random_range(0..3);
            out.push(Span {
                trace_id,
                span_id: SpanId((j as u64 + 1).to_be_bytes()),
                parent_span_id: (j > 0)
                    .then(|| SpanId((rng.random_range(0..j) as u64 + 1).to_be_bytes())),
                name: word(&mut rng),
                kind: rng.random_range(0..6),
                start_ns,
                end_ns: start_ns + rng.random_range(0..10_000),
                status_code,
                strings,
                input_tokens: opt(&mut rng, |r| r.random_range(0..100_000)),
                output_tokens: opt(&mut rng, |r| r.random_range(0..100_000)),
                resource: opt(&mut rng, |r| bytes(r, 40)),
                scope: opt(&mut rng, |r| bytes(r, 30)),
                attrs: bytes(&mut rng, attrs_len),
                events: opt(&mut rng, |r| bytes(r, 100)),
                links: opt(&mut rng, |_| Vec::new()),
                flags: rng.random(),
                dropped: [rng.random(), rng.random(), rng.random()],
                arrival: rng.random(),
            });
        }
    }
    for _ in 0..g.big_attrs {
        let i = rng.random_range(0..out.len());
        out[i].attrs = bytes(&mut rng, MIB);
    }
    out
}

fn default_gen(seed: u64, traces: usize) -> Gen {
    Gen {
        seed,
        traces,
        spans_per_trace: 1..=300,
        big_attrs: 0,
        trace_start: |i| 1_000_000 + i as i64 * 1_000,
    }
}

fn batch_of(spans: &[Span]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 30,
        max_bytes: 1 << 40,
    });
    for s in spans {
        b.append(&s.record());
    }
    b.finish()
}

fn small_groups() -> WriterOptions {
    WriterOptions {
        row_group_rows: 250,
        row_group_bytes: 64 * MIB,
        summary_row_group_rows: 200,
        ..WriterOptions::default()
    }
}

/// Writes `spans` in batches of random size 1..=`max_batch`, so traces cross
/// batch boundaries.
fn write_part(
    parts: &Path,
    id: u64,
    spans: &[Span],
    max_batch: usize,
    options: WriterOptions,
) -> PartMeta {
    let mut rng = StdRng::seed_from_u64(id);
    let mut w = PartWriter::create(parts, id, 0, WINDOW_NS, options).unwrap();
    let mut rest = spans;
    while !rest.is_empty() {
        let n = rng.random_range(1..=max_batch).min(rest.len());
        w.write(&batch_of(&rest[..n])).unwrap();
        rest = &rest[n..];
    }
    w.finish().unwrap()
}

fn part_dir(parts: &Path, id: u64) -> PathBuf {
    parts.join(part_dir_name(id))
}

fn records(batches: &[RecordBatch]) -> Vec<SpanRecord<'_>> {
    let mut out = Vec::new();
    for b in batches {
        let cols = SpanColumns::try_new(b).unwrap();
        out.extend((0..cols.len()).map(|i| cols.record(i)));
    }
    out
}

fn assert_rows(batches: &[RecordBatch], expected: &[&Span]) {
    let got = records(batches);
    assert_eq!(got.len(), expected.len(), "row count");
    for (i, (g, e)) in got.iter().zip(expected).enumerate() {
        assert_eq!(g, &e.record(), "row {i}");
    }
}

fn by_trace(spans: &[Span]) -> HashMap<TraceId, Vec<&Span>> {
    let mut m: HashMap<TraceId, Vec<&Span>> = HashMap::new();
    for s in spans {
        m.entry(s.trace_id).or_default().push(s);
    }
    m
}

fn expected_summaries(spans: &[Span]) -> Vec<TraceSummary> {
    let mut out = Vec::new();
    let mut b = SummaryBuilder::new();
    let mut current = None;
    for s in spans {
        if current.is_some_and(|c| c != s.trace_id) {
            out.extend(b.finish_trace());
        }
        current = Some(s.trace_id);
        b.push(&s.record()).unwrap();
    }
    out.extend(b.finish_trace());
    out.sort_by_key(|a| (a.start_ns, a.trace_id));
    out
}

fn summaries(batches: &[RecordBatch]) -> Vec<TraceSummary> {
    let mut out = Vec::new();
    for b in batches {
        let cols = SummaryColumns::try_new(b).unwrap();
        out.extend((0..cols.len()).map(|i| cols.summary(i)));
    }
    out
}

fn open_err(dir: &Path) -> Error {
    match PartReader::open(dir) {
        Ok(_) => panic!("open unexpectedly succeeded"),
        Err(e) => e,
    }
}

// --------------------------------------------------------------- tests ---

#[test]
fn reader_is_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<PartReader>();
}

#[test]
fn round_trip_every_column_of_every_row() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        big_attrs: 6,
        ..default_gen(1, 300)
    });
    assert!(spans.iter().any(|s| s.attrs.is_empty()));
    assert!(spans.iter().any(|s| s.attrs.len() == MIB));
    assert!(spans.iter().any(|s| s.strings.iter().any(Option::is_none)));
    // A normal-sized part: several row groups at the default bounds only if
    // there are enough rows, so also cover the multi-group case below.
    let meta = write_part(tmp.path(), 1, &spans, 700, WriterOptions::default());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    assert_eq!(reader.row_count(), spans.len() as u64);
    assert_eq!(meta.rows, spans.len() as u64);

    let all: Vec<&Span> = spans.iter().collect();
    let batches = reader.read_rows(0, reader.row_count(), None).unwrap();
    assert_rows(&batches, &all);
    for b in &batches {
        assert_eq!(b.schema().fields().len(), span_schema().fields().len());
    }

    // The same data through several row groups.
    write_part(tmp.path(), 2, &spans, 700, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 2)).unwrap();
    assert!(reader.row_group_count() > 5);
    let batches = reader.read_rows(0, reader.row_count(), None).unwrap();
    assert_rows(&batches, &all);
}

#[test]
fn read_trace_returns_exactly_the_traces_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(2, 300));
    write_part(tmp.path(), 1, &spans, 400, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let traces = by_trace(&spans);
    assert_eq!(reader.trace_count(), traces.len() as u64);

    let mut crossed_group = 0;
    for (id, expected) in &traces {
        let got = reader.read_trace(*id, None).unwrap().unwrap();
        assert_rows(&got, expected);
        let e = reader.find_trace(*id).unwrap();
        assert_eq!(e.row_count as usize, expected.len());
        // 250-row groups: a trace spanning a multiple of 250 crosses one.
        if e.first_row / 250 != (e.first_row + e.row_count as u64 - 1) / 250 {
            crossed_group += 1;
        }
    }
    assert!(
        crossed_group > 10,
        "only {crossed_group} traces crossed a row group"
    );

    let mut missing = [0u8; 16];
    missing[7] = 0xee;
    assert!(reader.read_trace(TraceId(missing), None).unwrap().is_none());
    assert!(reader.find_trace(TraceId(missing)).is_none());

    let entries = reader.trace_entries();
    assert!(entries.windows(2).all(|w| w[0].trace_id < w[1].trace_id));
}

#[test]
fn projection_returns_requested_columns_in_requested_order() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(3, 40));
    write_part(tmp.path(), 1, &spans, 100, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let (id, expected) = by_trace(&spans).into_iter().next().unwrap();

    let names = ["tool_name", "start_ns", "attrs", "trace_id"];
    let got = reader.read_trace(id, Some(&names)).unwrap().unwrap();
    let mut rows = 0;
    for b in &got {
        let fields: Vec<_> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, names);
        rows += b.num_rows();
    }
    assert_eq!(rows, expected.len());
    let start = got[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(start.value(0), expected[0].start_ns);

    for bad in [&["nope"][..], &["name", "name"][..], &[][..]] {
        assert!(matches!(
            reader.read_trace(id, Some(bad)),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            reader.read_rows(0, 1, Some(bad)),
            Err(Error::InvalidArgument(_))
        ));
    }
    // An unknown column is reported even for a trace that is not there.
    assert!(matches!(
        reader.read_trace(TraceId([9; 16]), Some(&["nope"])),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn read_rows_checks_its_range() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(4, 20));
    write_part(tmp.path(), 1, &spans, 100, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let n = reader.row_count();
    assert!(reader.read_rows(n, 0, None).unwrap().is_empty());
    assert!(reader.read_rows(n - 1, 2, None).is_err());
    assert!(reader.read_rows(u64::MAX, 2, None).is_err());
    // A range that starts and ends inside different row groups.
    let got = reader.read_rows(240, 30, None).unwrap();
    let want: Vec<&Span> = spans[240..270].iter().collect();
    assert_rows(&got, &want);
}

#[test]
fn writer_rejects_a_trace_that_reappears() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(5, 4));
    let t = by_trace(&spans);
    let ids: Vec<TraceId> = {
        let mut seen = Vec::new();
        for s in &spans {
            if !seen.contains(&s.trace_id) {
                seen.push(s.trace_id);
            }
        }
        seen
    };
    let a: Vec<Span> = t[&ids[0]].iter().map(|s| (*s).clone()).collect();
    let b: Vec<Span> = t[&ids[1]].iter().map(|s| (*s).clone()).collect();

    // Across batches.
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch_of(&a)).unwrap();
    w.write(&batch_of(&b)).unwrap();
    let err = w.write(&batch_of(&a[..1])).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");

    // The rejected batch left nothing behind: the writer still works (the
    // trace in progress may continue), and the part holds a, b and that one
    // more span.
    w.write(&batch_of(&b[..1])).unwrap();
    let meta = w.finish().unwrap();
    assert_eq!(meta.rows, (a.len() + b.len() + 1) as u64);
    assert_eq!(meta.traces, 2);

    // Inside one batch.
    let mut mixed = a.clone();
    mixed.extend(b.iter().cloned());
    mixed.push(a[0].clone());
    let mut w = PartWriter::create(tmp.path(), 2, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    assert!(matches!(
        w.write(&batch_of(&mixed)),
        Err(Error::InvalidArgument(_))
    ));
    drop(w);

    // A trace that continues into the next batch is fine; one that was
    // interrupted is not, even when the interruption was in the same batch
    // as its continuation.
    let mut w = PartWriter::create(tmp.path(), 3, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch_of(&a[..1])).unwrap();
    let mut rest = a[1..].to_vec();
    rest.extend(b.iter().cloned());
    w.write(&batch_of(&rest)).unwrap();
    let mut back = vec![b[0].clone()];
    back.push(a[0].clone());
    assert!(w.write(&batch_of(&back)).is_err());
    w.finish().unwrap();
}

#[test]
fn writer_rejects_the_wrong_schema() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();

    let mut sb = SummaryBatchBuilder::new();
    sb.append(&expected_summaries(&generate(&default_gen(6, 1)))[0]);
    assert_eq!(sb.finish().schema(), summary_schema());
    let mut sb = SummaryBatchBuilder::new();
    sb.append(&expected_summaries(&generate(&default_gen(6, 1)))[0]);
    assert!(matches!(
        w.write(&sb.finish()),
        Err(Error::InvalidArgument(_))
    ));

    let spans = generate(&default_gen(6, 2));
    let batch = batch_of(&spans);
    let fewer = batch.project(&[0, 1, 2]).unwrap();
    assert!(matches!(w.write(&fewer), Err(Error::InvalidArgument(_))));
    let order: Vec<usize> = (0..batch.num_columns()).rev().collect();
    assert!(w.write(&batch.project(&order).unwrap()).is_err());

    // Still usable after the rejections.
    w.write(&batch).unwrap();
    assert!(w.finish().unwrap().rows > 0);
}

#[test]
fn index_file_known_answer() {
    // CRC-32C check value, to pin the algorithm independently of the crate.
    assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);

    let tmp = tempfile::tempdir().unwrap();
    let mk = |trace: u8, span: u8, start: i64, end: i64| Span {
        trace_id: TraceId([trace; 16]),
        span_id: SpanId([span; 8]),
        parent_span_id: None,
        name: "s".into(),
        kind: 0,
        start_ns: start,
        end_ns: end,
        status_code: 0,
        strings: Default::default(),
        input_tokens: None,
        output_tokens: None,
        resource: None,
        scope: None,
        attrs: Vec::new(),
        events: None,
        links: None,
        flags: 0,
        dropped: [0; 3],
        arrival: 0,
    };
    // Trace 0x11 first in the file, then trace 0x05; the index sorts by id.
    let spans = [
        mk(0x11, 1, 100, 150),
        mk(0x11, 2, 120, 300),
        mk(0x05, 1, -50, 40),
    ];
    write_part(tmp.path(), 1, &spans, 10, WriterOptions::default());
    let dir = part_dir(tmp.path(), 1);
    let got = std::fs::read(dir.join(INDEX_FILE)).unwrap();

    let mut want: Vec<u8> = Vec::new();
    want.extend_from_slice(b"NWTI");
    want.extend_from_slice(&[6, 0, 0, 0]);
    want.extend_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]);
    // Length (u64) and CRC-32C (u32) of spans.parquet, summary.parquet,
    // pieces.blk, pieces.idx, text.idx and text.rows.
    for file in [
        SPANS_FILE,
        SUMMARY_FILE,
        PIECES_BLK,
        PIECES_IDX,
        TEXT_INDEX_FILE,
        TEXT_ROWS_FILE,
    ] {
        let bytes = std::fs::read(dir.join(file)).unwrap();
        want.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        // pieces.idx ends with its own CRC-32C, which the recorded one leaves
        // out: over the whole file it would be the same for every part.
        let counted = if file == PIECES_IDX {
            &bytes[..bytes.len() - 4]
        } else {
            &bytes[..]
        };
        want.extend_from_slice(&crc32c::crc32c(counted).to_le_bytes());
    }
    assert_eq!(want.len(), 88);
    // The footer checksums of the two text files, each four bytes before the
    // 4-byte magic that ends the file.
    for file in [TEXT_INDEX_FILE, TEXT_ROWS_FILE] {
        let bytes = std::fs::read(dir.join(file)).unwrap();
        want.extend_from_slice(&bytes[bytes.len() - 8..bytes.len() - 4]);
    }
    assert_eq!(want.len(), 96);
    // Trace 0x05: first row 2, one row, start -50, end 40.
    want.extend_from_slice(&[0x05; 16]);
    want.extend_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]);
    want.extend_from_slice(&[1, 0, 0, 0]);
    want.extend_from_slice(&[0xCE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    want.extend_from_slice(&[40, 0, 0, 0, 0, 0, 0, 0]);
    // Trace 0x11: first row 0, two rows, start 100, end 300.
    want.extend_from_slice(&[0x11; 16]);
    want.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    want.extend_from_slice(&[2, 0, 0, 0]);
    want.extend_from_slice(&[100, 0, 0, 0, 0, 0, 0, 0]);
    want.extend_from_slice(&[0x2C, 0x01, 0, 0, 0, 0, 0, 0]);
    assert_eq!(want.len(), 96 + 2 * 44);
    // CRC-32C of everything above, little-endian. The header holds the
    // checksums of the Parquet files, so this one is computed, with the
    // algorithm pinned by the check value at the top of the test.
    let crc = crc32c::crc32c(&want);
    want.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(got, want);

    // The reader exposes the same values.
    let reader = PartReader::open(&dir).unwrap();
    let spans_bytes = std::fs::read(dir.join(SPANS_FILE)).unwrap();
    let sum = reader.spans_checksum();
    assert_eq!(sum.len, spans_bytes.len() as u64);
    assert_eq!(sum.crc32c, crc32c::crc32c(&spans_bytes));
    assert!(reader.summary_checksum().len > 0);
}

/// Copies a valid part and returns the copy's directory.
fn copy_part(src: &Path, dst_parent: &Path) -> PathBuf {
    let dst = dst_parent.join("copy");
    std::fs::create_dir_all(&dst).unwrap();
    for f in [
        SPANS_FILE,
        INDEX_FILE,
        SUMMARY_FILE,
        PIECES_BLK,
        PIECES_IDX,
        TEXT_INDEX_FILE,
        TEXT_ROWS_FILE,
    ] {
        std::fs::copy(src.join(f), dst.join(f)).unwrap();
    }
    dst
}

/// Rewrites the trailing checksum so only the intended defect remains.
fn reseal(bytes: &mut [u8]) {
    let n = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..n]);
    bytes[n..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn corrupt_parts_are_rejected_at_open() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(7, 30));
    write_part(tmp.path(), 1, &spans, 100, small_groups());
    let good = part_dir(tmp.path(), 1);
    PartReader::open(&good).unwrap();
    let original = std::fs::read(good.join(INDEX_FILE)).unwrap();

    let expect_corrupt = |mutate: &dyn Fn(&mut Vec<u8>), what: &str| {
        let scratch = tempfile::tempdir().unwrap();
        let dir = copy_part(&good, scratch.path());
        let mut bytes = original.clone();
        mutate(&mut bytes);
        std::fs::write(dir.join(INDEX_FILE), &bytes).unwrap();
        match open_err(&dir) {
            Error::Corrupt(msg) => assert!(msg.contains(INDEX_FILE), "{what}: {msg}"),
            other => panic!("{what}: expected Corrupt, got {other:?}"),
        }
    };

    expect_corrupt(&|b| b[0] = b'X', "bad magic");
    expect_corrupt(
        &|b| {
            b[4] = 9;
            reseal(b);
        },
        "bad version",
    );
    expect_corrupt(&|b| b.truncate(b.len() - 10), "truncated");
    expect_corrupt(&|b| b.truncate(7), "shorter than the header");
    expect_corrupt(&|b| b.extend_from_slice(&[0; 44]), "extra record");
    expect_corrupt(&|b| b[20] ^= 1, "flipped byte breaks the checksum");
    expect_corrupt(
        &|b| {
            let n = b.len() - 1;
            b[n] ^= 1;
        },
        "bad checksum",
    );
    expect_corrupt(
        &|b| {
            // The first record's row count, made one larger, with a valid
            // checksum: the ranges no longer tile the Parquet rows.
            b[96 + 24] = b[96 + 24].wrapping_add(1);
            reseal(b);
        },
        "row ranges disagree with the Parquet file",
    );
    expect_corrupt(
        &|b| {
            // Two records swapped: ids out of order, checksum valid.
            let (first, second) = (96, 96 + 44);
            let a: Vec<u8> = b[first..second].to_vec();
            let c: Vec<u8> = b[second..second + 44].to_vec();
            b[first..second].copy_from_slice(&c);
            b[second..second + 44].copy_from_slice(&a);
            reseal(b);
        },
        "unsorted index",
    );

    for file in [
        SPANS_FILE,
        INDEX_FILE,
        SUMMARY_FILE,
        PIECES_BLK,
        PIECES_IDX,
        TEXT_INDEX_FILE,
        TEXT_ROWS_FILE,
    ] {
        let scratch = tempfile::tempdir().unwrap();
        let dir = copy_part(&good, scratch.path());
        std::fs::remove_file(dir.join(file)).unwrap();
        match open_err(&dir) {
            Error::Corrupt(msg) => assert!(msg.contains(file), "{file}: {msg}"),
            other => panic!("{file}: expected Corrupt, got {other:?}"),
        }
    }

    // A Parquet file that is not Parquet.
    let scratch = tempfile::tempdir().unwrap();
    let dir = copy_part(&good, scratch.path());
    std::fs::write(dir.join(SPANS_FILE), b"not parquet at all").unwrap();
    match open_err(&dir) {
        Error::Corrupt(msg) => assert!(msg.contains(SPANS_FILE), "{msg}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }

    // The summary of another part has the wrong number of rows.
    let other_spans = generate(&default_gen(8, 31));
    write_part(tmp.path(), 2, &other_spans, 100, small_groups());
    let scratch = tempfile::tempdir().unwrap();
    let dir = copy_part(&good, scratch.path());
    std::fs::copy(
        part_dir(tmp.path(), 2).join(SUMMARY_FILE),
        dir.join(SUMMARY_FILE),
    )
    .unwrap();
    match open_err(&dir) {
        Error::Corrupt(msg) => assert!(msg.contains(SUMMARY_FILE), "{msg}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }

    // A directory that is not a part at all.
    let empty = tempfile::tempdir().unwrap();
    assert!(matches!(open_err(empty.path()), Error::Corrupt(_)));
}

#[test]
fn summary_file_matches_an_independent_computation() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(9, 250));
    write_part(tmp.path(), 1, &spans, 300, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    assert!(reader.summary_row_group_count() > 1);

    let want = expected_summaries(&spans);
    let got = summaries(&reader.scan_summary(None, None).unwrap());
    assert_eq!(got, want);
    assert!(
        got.windows(2)
            .all(|w| (w[0].start_ns, w[0].trace_id) < (w[1].start_ns, w[1].trace_id))
    );
    // The summary of a trace agrees with its index entry.
    for s in &got {
        let e = reader.find_trace(s.trace_id).unwrap();
        assert_eq!(
            (e.start_ns, e.end_ns, e.row_count),
            (s.start_ns, s.end_ns, s.span_count)
        );
    }
}

#[test]
fn scan_summary_prunes_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    // 2,000 single-span traces starting 1 us apart: 10 summary row groups of
    // 200, in time order.
    let spans = generate(&Gen {
        spans_per_trace: 1..=1,
        ..default_gen(10, 2_000)
    });
    write_part(tmp.path(), 1, &spans, 500, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    assert_eq!(reader.summary_row_group_count(), 10);

    let (lo, hi) = (1_000_000 + 700_000, 1_000_000 + 900_000);
    let got = summaries(&reader.scan_summary(Some(lo), Some(hi)).unwrap());
    let in_range = |s: &TraceSummary| s.start_ns >= lo && s.start_ns <= hi;
    let want: Vec<_> = expected_summaries(&spans)
        .into_iter()
        .filter(in_range)
        .collect();
    assert!(!want.is_empty());
    for w in &want {
        assert!(got.contains(w), "trace in range is missing");
    }
    assert!(got.len() < 2_000 / 2, "{} rows read of 2000", got.len());

    // Open ends and an empty range.
    assert_eq!(
        reader
            .scan_summary(None, None)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        2_000
    );
    assert!(
        reader
            .scan_summary(Some(i64::MAX - 1), None)
            .unwrap()
            .is_empty()
    );
    assert!(
        reader
            .scan_summary(None, Some(i64::MIN + 1))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn scan_summary_rev_reads_newest_groups_first_and_lazily() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        spans_per_trace: 1..=1,
        ..default_gen(10, 2_000)
    });
    write_part(tmp.path(), 1, &spans, 500, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    assert_eq!(reader.summary_row_group_count(), 10);

    // Everything: the groups come newest first, their bounds never rise, and
    // together they hold exactly the rows of the forward scan.
    let mut scan = reader.scan_summary_rev(None, None).unwrap();
    let mut last_bound = i64::MAX;
    let mut got = Vec::new();
    while let Some(bound) = scan.next_upper_bound() {
        assert!(bound <= last_bound);
        last_bound = bound;
        let group = scan.next_group().unwrap().unwrap();
        let rows = summaries(&group);
        assert!(rows.iter().all(|s| s.start_ns <= bound));
        // Ascending inside the group, newest group first.
        assert!(rows.windows(2).all(|w| w[0].start_ns <= w[1].start_ns));
        got.push(rows);
    }
    assert_eq!(scan.row_groups_read(), 10);
    assert!(scan.next_group().unwrap().is_none());
    for w in got.windows(2) {
        assert!(w[1].last().unwrap().start_ns <= w[0][0].start_ns);
    }
    let mut got: Vec<TraceSummary> = got.into_iter().flatten().collect();
    let mut want = summaries(&reader.scan_summary(None, None).unwrap());
    got.sort_by_key(|s| s.trace_id);
    want.sort_by_key(|s| s.trace_id);
    assert_eq!(got, want);

    // A bounded range prunes the same groups as the forward scan, and
    // nothing is read before it is asked for.
    let (lo, hi) = (1_000_000 + 700_000, 1_000_000 + 900_000);
    let mut scan = reader.scan_summary_rev(Some(lo), Some(hi)).unwrap();
    assert_eq!(scan.row_groups_read(), 0);
    let (mut groups, mut rows) = (0, 0);
    while let Some(group) = scan.next_group().unwrap() {
        groups += 1;
        rows += summaries(&group).len();
    }
    assert_eq!(scan.row_groups_read(), groups);
    let forward = summaries(&reader.scan_summary(Some(lo), Some(hi)).unwrap());
    assert_eq!(rows, forward.len());
    assert!(groups < 10);
    let mut scan = reader.scan_summary_rev(Some(i64::MAX - 1), None).unwrap();
    assert!(scan.next_upper_bound().is_none());
    assert!(scan.next_group().unwrap().is_none());
}

/// A summary row group of more rows than one batch (8,192) is returned as
/// several batches: what is counted as groups must be groups and what is
/// counted as rows must be rows, whatever the number of batches.
#[test]
fn scan_summary_rev_counts_groups_and_rows_when_a_group_is_several_batches() {
    const TRACES: usize = 9_000;
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        spans_per_trace: 1..=1,
        ..default_gen(12, TRACES)
    });
    let options = WriterOptions {
        summary_row_group_rows: 20_000,
        ..WriterOptions::default()
    };
    write_part(tmp.path(), 1, &spans, 500, options);
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    assert_eq!(reader.summary_row_group_count(), 1);

    let mut scan = reader.scan_summary_rev(None, None).unwrap();
    let group = scan.next_group().unwrap().unwrap();
    assert!(group.len() > 1, "the group is {} batch", group.len());
    let rows = summaries(&group);
    assert_eq!(rows.len(), TRACES);
    assert!(rows.windows(2).all(|w| w[0].start_ns <= w[1].start_ns));
    assert_eq!(scan.row_groups_read(), 1);
    assert!(scan.next_group().unwrap().is_none());

    // A range that overlaps the one group returns all of its rows (statistics
    // prune groups, not rows), counted in rows and not in batches.
    let mut scan = reader
        .scan_summary_rev(Some(1_000_000 + 1_000), Some(1_000_000 + 2_000))
        .unwrap();
    let group = scan.next_group().unwrap().unwrap();
    let forward = reader
        .scan_summary(Some(1_000_000 + 1_000), Some(1_000_000 + 2_000))
        .unwrap();
    assert!(group.len() > 1 && forward.len() > 1);
    assert_eq!(summaries(&group).len(), summaries(&forward).len());
    assert_eq!(summaries(&group).len(), TRACES);
}

#[test]
fn scan_prunes_row_groups_and_projects() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        spans_per_trace: 1..=3,
        ..default_gen(11, 2_000)
    });
    write_part(tmp.path(), 1, &spans, 500, small_groups());
    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    let groups = reader.row_group_count();
    assert!(groups > 10);

    let (lo, hi) = (1_000_000 + 700_000, 1_000_000 + 900_000);
    let scan = reader
        .scan(ScanOptions {
            projection: None,
            start_ns_min: Some(lo),
            start_ns_max: Some(hi),
        })
        .unwrap();
    assert_eq!(scan.row_groups_read() + scan.row_groups_skipped(), groups);
    assert!(scan.row_groups_skipped() >= groups / 2);
    assert!(scan.row_groups_read() >= 1);
    let batches: Vec<RecordBatch> = scan.map(Result::unwrap).collect();
    let got: Vec<SpanRecord<'_>> = records(&batches);
    for s in spans
        .iter()
        .filter(|s| s.start_ns >= lo && s.start_ns <= hi)
    {
        assert!(got.contains(&s.record()), "span in range is missing");
    }
    assert!(got.len() < spans.len() / 2);

    // No range: every row, nothing skipped.
    let scan = reader.scan(ScanOptions::default()).unwrap();
    assert_eq!(scan.row_groups_skipped(), 0);
    let all: Vec<RecordBatch> = scan.map(Result::unwrap).collect();
    assert_eq!(records(&all).len(), spans.len());

    // A range before everything.
    let mut scan = reader
        .scan(ScanOptions {
            start_ns_max: Some(0),
            ..ScanOptions::default()
        })
        .unwrap();
    assert_eq!(scan.row_groups_read(), 0);
    assert!(scan.next().is_none());

    // Projection: only these columns, in this order, same rows.
    let names = ["end_ns", "name", "trace_id"];
    let scan = reader
        .scan(ScanOptions {
            projection: Some(names.iter().map(|s| s.to_string()).collect()),
            start_ns_min: Some(lo),
            start_ns_max: Some(hi),
        })
        .unwrap();
    let mut rows = 0;
    for b in scan {
        let b = b.unwrap();
        let fields: Vec<_> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, names);
        rows += b.num_rows();
    }
    assert_eq!(rows, got.len());

    let bad = reader.scan(ScanOptions {
        projection: Some(vec!["nope".into()]),
        ..ScanOptions::default()
    });
    assert!(matches!(bad, Err(Error::InvalidArgument(_))));
}

#[test]
fn row_groups_are_bounded_by_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(12);
    let mut spans = generate(&Gen {
        spans_per_trace: 200..=200,
        ..default_gen(12, 1)
    });
    assert_eq!(spans.len(), 200);
    for s in &mut spans {
        // Distinct, compressible 1 MiB values.
        let seed = rng.random::<u64>().to_le_bytes();
        s.attrs = seed.iter().copied().cycle().take(MIB).collect();
    }

    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    for chunk in spans.chunks(10) {
        w.write(&batch_of(chunk)).unwrap();
    }
    let meta = w.finish().unwrap();
    assert_eq!(meta.rows, 200);

    let reader = PartReader::open(part_dir(tmp.path(), 1)).unwrap();
    // 64 MiB per group: 200 MiB needs at least three, while 200 rows alone
    // would have fitted in one group.
    assert!(
        reader.row_group_count() >= 3,
        "{} groups",
        reader.row_group_count()
    );
    assert!(reader.row_group_count() <= 5);
    let got = reader
        .read_rows(60, 10, Some(&["attrs", "span_id"]))
        .unwrap();
    let rows: usize = got.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10);
    let attrs = got[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
        .unwrap();
    assert_eq!(attrs.value(0), &spans[60].attrs[..]);
}

#[test]
fn lifecycle_of_the_part_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let parts = tmp.path().join("parts");
    let final_dir = part_dir(&parts, 7);
    let tmp_dir = parts.join(format!("{}.tmp", part_dir_name(7)));
    let spans = generate(&default_gen(13, 20));

    let mut w = PartWriter::create(&parts, 7, 5, WINDOW_NS, WriterOptions::default()).unwrap();
    assert!(tmp_dir.is_dir());
    assert!(!final_dir.exists());
    // A second writer for the same id is refused while the first is live.
    assert!(matches!(
        PartWriter::create(&parts, 7, 5, WINDOW_NS, WriterOptions::default()).err(),
        Some(Error::InvalidArgument(_))
    ));
    assert!(
        tmp_dir.is_dir(),
        "the refused writer must not touch the live one"
    );
    w.write(&batch_of(&spans)).unwrap();
    assert!(tmp_dir.is_dir());
    let meta = w.finish().unwrap();
    assert!(!tmp_dir.exists());

    let mut names: Vec<String> = std::fs::read_dir(&final_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            PIECES_BLK,
            PIECES_IDX,
            SPANS_FILE,
            SUMMARY_FILE,
            TEXT_INDEX_FILE,
            TEXT_ROWS_FILE,
            INDEX_FILE
        ]
    );
    assert_eq!(meta.files.len(), 7);
    for (name, size) in &meta.files {
        assert_eq!(
            std::fs::metadata(final_dir.join(name)).unwrap().len(),
            *size,
            "{name}"
        );
    }
    // With no splitter nothing is cut, so the blocks file is empty.
    assert!(meta.files.iter().all(|(n, &s)| s > 0 || n == PIECES_BLK));

    // The id is taken.
    assert!(matches!(
        PartWriter::create(&parts, 7, 5, WINDOW_NS, WriterOptions::default()).err(),
        Some(Error::InvalidArgument(_))
    ));
    // A stale temporary directory blocks the id too.
    std::fs::create_dir(parts.join(format!("{}.tmp", part_dir_name(8)))).unwrap();
    assert!(PartWriter::create(&parts, 8, 0, WINDOW_NS, WriterOptions::default()).is_err());

    // Dropping an unfinished writer removes its directory.
    let mut w = PartWriter::create(&parts, 9, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch_of(&spans)).unwrap();
    let dir9 = parts.join(format!("{}.tmp", part_dir_name(9)));
    assert!(dir9.is_dir());
    drop(w);
    assert!(!dir9.exists());
    assert!(!part_dir(&parts, 9).exists());

    // Zero rows: refused, and nothing is left.
    let w = PartWriter::create(&parts, 10, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    assert!(matches!(w.finish(), Err(Error::InvalidArgument(_))));
    assert!(!part_dir(&parts, 10).exists());
    assert!(!parts.join(format!("{}.tmp", part_dir_name(10))).exists());

    // An empty batch adds no rows.
    let mut w = PartWriter::create(&parts, 11, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&batch_of(&[])).unwrap();
    assert!(w.finish().is_err());
    assert!(!part_dir(&parts, 11).exists());

    // A window that is not positive is refused before anything is created.
    for window in [0, -1] {
        assert!(matches!(
            PartWriter::create(&parts, 13, 0, window, WriterOptions::default()).err(),
            Some(Error::InvalidArgument(_))
        ));
    }
    assert!(!parts.join(format!("{}.tmp", part_dir_name(13))).exists());

    // Bad options are refused before anything is created.
    let bad = WriterOptions {
        row_group_rows: 0,
        ..WriterOptions::default()
    };
    assert!(PartWriter::create(&parts, 12, 0, WINDOW_NS, bad).is_err());
    assert!(!parts.join(format!("{}.tmp", part_dir_name(12))).exists());
}

#[test]
fn concurrent_reads_on_one_reader_are_correct() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        spans_per_trace: 1..=60,
        big_attrs: 4,
        ..default_gen(14, 200)
    });
    write_part(tmp.path(), 1, &spans, 300, small_groups());
    let reader = Arc::new(PartReader::open(part_dir(tmp.path(), 1)).unwrap());
    let traces = by_trace(&spans);
    let ids: Vec<TraceId> = reader.trace_entries().iter().map(|e| e.trace_id).collect();

    std::thread::scope(|scope| {
        for t in 0..8 {
            let (reader, traces, ids) = (&reader, &traces, &ids);
            scope.spawn(move || {
                // Each thread walks the traces from a different start, so at
                // any moment the threads read different parts of the file.
                for k in 0..ids.len() {
                    let id = ids[(k * 7 + t * 23) % ids.len()];
                    let got = reader.read_trace(id, None).unwrap().unwrap();
                    assert_rows(&got, &traces[&id]);
                }
            });
        }
    });
}

#[test]
fn part_meta_describes_the_part() {
    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&default_gen(15, 120));
    let window_start = 900_000_000_000;
    let mut w =
        PartWriter::create(tmp.path(), 42, window_start, WINDOW_NS, small_groups()).unwrap();
    for chunk in spans.chunks(333) {
        w.write(&batch_of(chunk)).unwrap();
    }
    let meta = w.finish().unwrap();

    assert_eq!(meta.part_id, 42);
    assert_eq!(meta.level, 0);
    assert_eq!(meta.format_version, 6);
    assert_eq!(meta.window_start_ns, window_start);
    assert_eq!(meta.window_ns, WINDOW_NS);
    assert_eq!(meta.rows, spans.len() as u64);
    assert_eq!(meta.traces, by_trace(&spans).len() as u64);
    assert_eq!(
        meta.min_start_ns,
        spans.iter().map(|s| s.start_ns).min().unwrap()
    );
    assert_eq!(
        meta.max_end_ns,
        spans.iter().map(|s| s.end_ns).max().unwrap()
    );
    assert!(meta.created_unix_ms > 1_700_000_000_000);

    let reader = PartReader::open(part_dir(tmp.path(), 42)).unwrap();
    assert_eq!(reader.row_count(), meta.rows);
    assert_eq!(reader.trace_count(), meta.traces);
    assert_eq!(reader.min_start_ns(), meta.min_start_ns);
    assert_eq!(reader.max_end_ns(), meta.max_end_ns);
}

#[test]
fn large_values_do_not_make_oversized_pages() {
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    let tmp = tempfile::tempdir().unwrap();
    let spans = generate(&Gen {
        spans_per_trace: 40..=40,
        big_attrs: 0,
        ..default_gen(16, 1)
    });
    let mut rng = StdRng::seed_from_u64(16);
    let spans: Vec<Span> = spans
        .into_iter()
        .map(|mut s| {
            s.attrs = bytes(&mut rng, MIB);
            s
        })
        .collect();
    write_part(tmp.path(), 1, &spans, 40, WriterOptions::default());

    let file = std::fs::File::open(part_dir(tmp.path(), 1).join(SPANS_FILE)).unwrap();
    let meta = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();
    let attrs = span_schema().index_of("attrs").unwrap();
    let pages = meta
        .page_index()
        .unwrap()
        .offset_index(0, attrs)
        .unwrap()
        .page_locations();
    // Random bytes do not compress, so a page is about as large as the
    // values in it: no more than a couple of 1 MiB values.
    let largest = pages.iter().map(|p| p.compressed_page_size).max().unwrap();
    assert!(largest < 3 * MIB as i32, "largest page {largest} bytes");
    assert!(pages.len() >= 20, "{} pages for 40 values", pages.len());
}
