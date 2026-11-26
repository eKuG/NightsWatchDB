//! Merging parts: the output must be what a flush of all the rows at once
//! would have written, checked against a model; text must be shared, not
//! rebuilt; damage must be an error that leaves nothing behind.

use arrow_array::RecordBatch;
use nwdb_core::{
    BatchLimits, Error, PayloadSplitter, RawColumn, SpanBatchBuilder, SpanColumns, SpanId,
    SpanRecord, SummaryColumns, TraceId, TraceSummary, part_dir_name,
};
use nwdb_payload::{PieceWriter, PieceWriterOptions, StoreSummary};
use nwdb_segment::{
    INDEX_FILE, MergeOptions, MergeStats, PIECES_BLK, PIECES_IDX, PartReader, PartWriter,
    PieceObserver, SPANS_FILE, SUMMARY_FILE, ScanOptions, WriterOptions, merge_parts,
};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WINDOW_NS: i64 = 900_000_000_000;
const CUT: usize = 256;

// ------------------------------------------------------------- splitter ---

/// Cuts every value into consecutive chunks of `CUT` bytes, so equal text
/// aligned to the chunk size makes equal pieces wherever it appears.
struct Chunks;

impl PayloadSplitter for Chunks {
    fn cuts(&self, _: RawColumn, value: &[u8], cuts: &mut Vec<Range<usize>>) {
        let mut at = 0;
        while at < value.len() {
            let end = (at + CUT).min(value.len());
            cuts.push(at..end);
            at = end;
        }
    }
}

// ---------------------------------------------------------------- model ---

/// The values of the seven origin columns of a row.
#[derive(Clone, Debug)]
struct Org {
    resource: Option<Vec<u8>>,
    scope: Option<Vec<u8>>,
    strings: [Option<String>; 5],
}

/// One span, with every column the table has.
#[derive(Clone, Debug)]
struct Row {
    trace: u32,
    span: u32,
    parent: Option<u32>,
    start: i64,
    end: i64,
    name: String,
    kind: i32,
    status_code: i32,
    flags: u32,
    /// status_message, trace_state, operation, provider, request_model,
    /// response_model, conversation_id, session_id, user_id, agent_name,
    /// tool_name, error_type.
    opt: [Option<String>; 12],
    tokens: [Option<i64>; 2],
    dropped: [u32; 3],
    /// The `arrival` column. The generated rows all have 0, so the earlier
    /// input wins; the tests of `arrival` set it.
    arrival: u64,
    org: Org,
    attrs: Vec<u8>,
    events: Option<Vec<u8>>,
    links: Option<Vec<u8>>,
}

fn trace_id(t: u32) -> [u8; 16] {
    (u128::from(t) + 1)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835)
        .to_be_bytes()
}

fn span_id(s: u32) -> [u8; 8] {
    (u64::from(s) + 1)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .to_be_bytes()
}

impl Row {
    fn record(&self) -> SpanRecord<'_> {
        let o = |i: usize| self.opt[i].as_deref();
        let s = |i: usize| self.org.strings[i].as_deref();
        SpanRecord {
            trace_id: TraceId(trace_id(self.trace)),
            span_id: SpanId(span_id(self.span)),
            parent_span_id: self.parent.map(|p| SpanId(span_id(p))),
            name: &self.name,
            kind: self.kind,
            start_ns: self.start,
            end_ns: self.end,
            status_code: self.status_code,
            status_message: o(0),
            trace_state: o(1),
            flags: self.flags,
            service_name: s(0),
            scope_name: s(2),
            scope_version: s(3),
            resource_schema_url: s(1),
            scope_schema_url: s(4),
            operation: o(2),
            provider: o(3),
            request_model: o(4),
            response_model: o(5),
            input_tokens: self.tokens[0],
            output_tokens: self.tokens[1],
            conversation_id: o(6),
            session_id: o(7),
            user_id: o(8),
            agent_name: o(9),
            tool_name: o(10),
            error_type: o(11),
            resource: self.org.resource.as_deref(),
            scope: self.org.scope.as_deref(),
            attrs: &self.attrs,
            events: self.events.as_deref(),
            links: self.links.as_deref(),
            dropped_attributes_count: self.dropped[0],
            dropped_events_count: self.dropped[1],
            dropped_links_count: self.dropped[2],
            arrival: self.arrival,
        }
    }

    fn id(&self) -> ([u8; 16], [u8; 8]) {
        (trace_id(self.trace), span_id(self.span))
    }
}

/// Text that several rows, parts and traces share, and the origins they use.
struct Pool {
    texts: Vec<Vec<u8>>,
    origins: Vec<Org>,
}

fn words(rng: &mut StdRng, len: usize) -> Vec<u8> {
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

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

impl Pool {
    fn new(rng: &mut StdRng) -> Self {
        // Shared texts are whole multiples of the cut, so they are the same
        // pieces wherever they are placed at a multiple of the cut.
        let texts = (0..6)
            .map(|_| {
                let len = CUT * rng.random_range(2..10);
                words(rng, len)
            })
            .collect();
        let mut o = |n: usize, big: bool| Org {
            resource: Some(random_bytes(rng, if big { 3000 } else { 60 + n })),
            scope: (n != 1).then(|| random_bytes(rng, 20)),
            strings: [
                Some(format!("service-{n}")),
                n.is_multiple_of(2)
                    .then(|| format!("https://example.test/r/{n}")),
                Some(format!("scope-{n}")),
                (n != 3).then(|| format!("1.{n}")),
                None,
            ],
        };
        let none = Org {
            resource: None,
            scope: None,
            strings: [None, None, None, None, None],
        };
        let origins = vec![o(0, false), o(1, true), o(2, false), o(3, false), none];
        Pool { texts, origins }
    }

    /// A value of one of the shapes that matter: empty, too short to be a
    /// piece, shared text, shared text twice, text of its own, shared text with
    /// a tail that is not a whole piece.
    fn blob(&self, rng: &mut StdRng) -> Vec<u8> {
        let shared = |rng: &mut StdRng| self.texts[rng.random_range(0..self.texts.len())].clone();
        match rng.random_range(0..7) {
            0 => Vec::new(),
            1 => {
                let len = rng.random_range(1..8);
                random_bytes(rng, len)
            }
            2 => shared(rng),
            3 => [shared(rng), shared(rng)].concat(),
            4 => {
                let len = CUT * rng.random_range(1..5) + rng.random_range(0..CUT);
                random_bytes(rng, len)
            }
            5 => {
                let len = rng.random_range(1..CUT);
                [shared(rng), random_bytes(rng, len)].concat()
            }
            _ => {
                let len = rng.random_range(1..3000);
                words(rng, len)
            }
        }
    }

    fn row(&self, rng: &mut StdRng, trace: u32, span: u32, parent: Option<u32>, start: i64) -> Row {
        let some = |rng: &mut StdRng, p: f64, tag: &str| {
            rng.random_bool(p)
                .then(|| format!("{tag}-{}", rng.random_range(0..4)))
        };
        let opt = [
            some(rng, 0.3, "msg"),
            some(rng, 0.2, "state"),
            some(rng, 0.6, "op"),
            some(rng, 0.6, "prov"),
            some(rng, 0.5, "model"),
            some(rng, 0.5, "model"),
            some(rng, 0.3, "conv"),
            some(rng, 0.4, "sess"),
            some(rng, 0.4, "user"),
            some(rng, 0.3, "agent"),
            some(rng, 0.3, "tool"),
            some(rng, 0.1, "err"),
        ];
        Row {
            trace,
            span,
            parent,
            start,
            end: start + rng.random_range(0..5_000),
            name: format!("span-{}", rng.random_range(0..6)),
            kind: rng.random_range(0..6),
            status_code: rng.random_range(0..3),
            flags: rng.random_range(0..3),
            opt,
            tokens: [
                rng.random_bool(0.5).then(|| rng.random_range(0..5_000)),
                rng.random_bool(0.5).then(|| rng.random_range(0..5_000)),
            ],
            dropped: [
                rng.random_range(0..2),
                rng.random_range(0..2),
                rng.random_range(0..2),
            ],
            org: self.origins[rng.random_range(0..self.origins.len())].clone(),
            attrs: self.blob(rng),
            events: rng.random_bool(0.6).then(|| self.blob(rng)),
            links: rng.random_bool(0.3).then(|| self.blob(rng)),
            arrival: 0,
        }
    }
}

/// An input as a part holds it: traces in the order of a flush, the spans of
/// a trace by start and span id (a stable sort, so equal keys stay in the
/// order they were given).
fn stored_order(rows: Vec<Row>) -> Vec<Row> {
    let mut by_trace: BTreeMap<[u8; 16], Vec<Row>> = BTreeMap::new();
    for r in rows {
        by_trace.entry(trace_id(r.trace)).or_default().push(r);
    }
    let mut traces: Vec<(i64, [u8; 16], Vec<Row>)> = by_trace
        .into_iter()
        .map(|(id, mut rows)| {
            rows.sort_by_key(|r| (r.start, span_id(r.span)));
            (rows.iter().map(|r| r.start).min().unwrap(), id, rows)
        })
        .collect();
    traces.sort_by_key(|(start, id, _)| (*start, *id));
    traces.into_iter().flat_map(|(_, _, rows)| rows).collect()
}

/// Spans of many traces spread over `k` inputs: whole in one, split across
/// two, split across all; starts that are tight, equal, or far apart;
/// several roots; with `dups`, spans repeated in the same input and in
/// others, some identical and some that differ in content (and sometimes in
/// start) but share their ids.
fn generate(seed: u64, k: usize, dups: bool) -> Vec<Vec<Row>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let pool = Pool::new(&mut rng);
    let mut inputs: Vec<Vec<Row>> = vec![Vec::new(); k];
    for t in 0..rng.random_range(25..45u32) {
        let n = rng.random_range(1..10u32);
        let base = rng.random_range(0..2_000_000i64);
        let shape = rng.random_range(0..4);
        let place = rng.random_range(0..3);
        let whole = rng.random_range(0..k);
        let pair = [rng.random_range(0..k), rng.random_range(0..k)];
        for j in 0..n {
            let start = match shape {
                0 => base + i64::from(j) * 10,
                1 => base,
                2 if j % 2 == 0 => base + i64::from(j),
                2 => base + 5_000_000_000 + i64::from(j),
                _ => base + rng.random_range(0..1000),
            };
            let parent = (j > 0 && !rng.random_bool(0.1)).then(|| rng.random_range(0..j));
            let mut row = pool.row(&mut rng, t, j, parent, start);
            // Small values, so that copies often tie, and a copy is as
            // likely to be older as newer than the row it repeats.
            row.arrival = u64::from((t * 7 + j * 3) % 4);
            let target = match place {
                0 => whole,
                1 => pair[rng.random_range(0..2)],
                _ => rng.random_range(0..k),
            };
            inputs[target].push(row.clone());
            if dups && rng.random_bool(0.12) {
                let mut dup = row;
                dup.arrival = u64::from((t * 5 + j * 11 + 1) % 4);
                if rng.random_bool(0.6) {
                    dup.name = format!("variant-{}", rng.random_range(0..100));
                    dup.attrs = pool.blob(&mut rng);
                    if rng.random_bool(0.5) {
                        dup.start += rng.random_range(-9..9);
                        dup.end = dup.end.max(dup.start);
                    }
                }
                inputs[rng.random_range(0..k)].push(dup);
            }
        }
    }
    inputs.retain(|i| !i.is_empty());
    inputs.into_iter().map(stored_order).collect()
}

/// What the merge must produce: of each span the occurrence with the smallest
/// `arrival`, the first of them when several tie (inputs in the order given,
/// rows in stored order); traces by the earliest start of any of their rows
/// and then id, spans by start and span id.
fn expected(inputs: &[Vec<Row>]) -> Vec<Row> {
    let mut at: HashMap<([u8; 16], [u8; 8]), usize> = HashMap::new();
    let mut earliest: HashMap<[u8; 16], i64> = HashMap::new();
    let mut kept: Vec<Row> = Vec::new();
    for row in inputs.iter().flatten() {
        let e = earliest.entry(trace_id(row.trace)).or_insert(row.start);
        *e = (*e).min(row.start);
        match at.get(&row.id()) {
            None => {
                at.insert(row.id(), kept.len());
                kept.push(row.clone());
            }
            Some(&k) if row.arrival < kept[k].arrival => kept[k] = row.clone(),
            Some(_) => {}
        }
    }
    kept.sort_by_key(|r| {
        (
            earliest[&trace_id(r.trace)],
            trace_id(r.trace),
            r.start,
            span_id(r.span),
        )
    });
    kept
}

/// How many traces have rows in more than one of the inputs.
fn stitched(inputs: &[Vec<Row>]) -> u64 {
    let mut seen: HashMap<[u8; 16], HashSet<usize>> = HashMap::new();
    for (i, input) in inputs.iter().enumerate() {
        for r in input {
            seen.entry(trace_id(r.trace)).or_default().insert(i);
        }
    }
    seen.values().filter(|s| s.len() > 1).count() as u64
}

// -------------------------------------------------------------- writing ---

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

fn options(row_group_rows: usize) -> WriterOptions {
    WriterOptions {
        splitter: Some(Arc::new(Chunks)),
        row_group_rows,
        summary_row_group_rows: 7,
        piece_block_bytes: 2048,
        ..WriterOptions::default()
    }
}

fn write_part(
    dir: &Path,
    id: u64,
    rows: &[Row],
    batch_rows: usize,
    opts: WriterOptions,
) -> PartReader {
    let mut w = PartWriter::create(dir, id, 0, WINDOW_NS, opts).unwrap();
    for chunk in rows.chunks(batch_rows) {
        w.write(&batch_of(chunk)).unwrap();
    }
    w.finish().unwrap();
    PartReader::open(dir.join(part_dir_name(id))).unwrap()
}

fn write_inputs(dir: &Path, inputs: &[Vec<Row>], seed: u64) -> Vec<PartReader> {
    inputs
        .iter()
        .enumerate()
        .map(|(i, rows)| {
            let group = 3 + (seed as usize + i) % 9;
            let batch = 1 + (seed as usize * 7 + i) % 17;
            write_part(dir, i as u64 + 1, rows, batch, options(group))
        })
        .collect()
}

fn merge(
    inputs: &[PartReader],
    dir: &Path,
    id: u64,
    opts: MergeOptions,
) -> nwdb_core::Result<(nwdb_core::PartMeta, MergeStats)> {
    let refs: Vec<&PartReader> = inputs.iter().collect();
    merge_parts(&refs, dir, id, 0, WINDOW_NS, opts)
}

fn merge_options(chunk_bytes: usize) -> MergeOptions {
    MergeOptions {
        writer: options(8),
        chunk_bytes,
        ..MergeOptions::default()
    }
}

// ------------------------------------------------------------ comparing ---

fn flat(batches: &[RecordBatch]) -> Vec<SpanColumns<'_>> {
    batches
        .iter()
        .map(|b| SpanColumns::try_new(b).unwrap())
        .collect()
}

fn records<'a>(cols: &[SpanColumns<'a>]) -> Vec<SpanRecord<'a>> {
    cols.iter()
        .flat_map(|c| (0..c.len()).map(move |i| c.record(i)))
        .collect()
}

fn summaries(reader: &PartReader) -> Vec<TraceSummary> {
    let batches = reader.scan_summary(None, None).unwrap();
    batches
        .iter()
        .flat_map(|b| {
            let cols = SummaryColumns::try_new(b).unwrap();
            (0..cols.len()).map(|i| cols.summary(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// Every way of reading `a` gives what it gives for `b`: the index, whole
/// part reads, a scan, each trace, the summary.
fn assert_same_part(a: &PartReader, b: &PartReader, what: &str) {
    assert_eq!(a.row_count(), b.row_count(), "{what}: rows");
    assert_eq!(a.trace_entries(), b.trace_entries(), "{what}: index");
    let n = a.row_count();
    let (ra, rb) = (
        a.read_rows(0, n, None).unwrap(),
        b.read_rows(0, n, None).unwrap(),
    );
    assert_eq!(
        records(&flat(&ra)),
        records(&flat(&rb)),
        "{what}: read_rows"
    );

    let scan = |r: &PartReader| -> Vec<RecordBatch> {
        r.scan(ScanOptions::default())
            .unwrap()
            .map(|b| b.unwrap())
            .collect()
    };
    let (sa, sb) = (scan(a), scan(b));
    assert_eq!(records(&flat(&sa)), records(&flat(&sb)), "{what}: scan");

    for e in a.trace_entries() {
        let ta = a.read_trace(e.trace_id, None).unwrap().unwrap();
        let tb = b.read_trace(e.trace_id, None).unwrap().unwrap();
        assert_eq!(
            records(&flat(&ta)),
            records(&flat(&tb)),
            "{what}: trace {}",
            e.trace_id
        );
    }
    assert_eq!(summaries(a), summaries(b), "{what}: summary");
}

/// The part reads back as exactly the rows of the model, in order.
fn assert_holds(reader: &PartReader, rows: &[Row], what: &str) {
    let batches = reader.read_rows(0, rows.len() as u64, None).unwrap();
    let got = records(&flat(&batches));
    let want: Vec<SpanRecord<'_>> = rows.iter().map(Row::record).collect();
    assert_eq!(got.len(), want.len(), "{what}");
    for (i, (g, w)) in got.iter().zip(&want).enumerate() {
        assert_eq!(g, w, "{what}: row {i}");
    }
}

fn dir_size(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap().metadata().unwrap().len())
        .sum()
}

fn part_path(parts: &Path, id: u64) -> PathBuf {
    parts.join(part_dir_name(id))
}

// ---------------------------------------------------------------- tests ---

#[test]
fn a_merge_is_the_part_a_flush_of_all_the_rows_would_write() {
    let (mut dups, mut stitch, mut shared_pieces) = (0, 0, 0);
    for seed in 0..48u64 {
        let k = 1 + (seed % 8) as usize;
        let inputs = generate(seed, k, true);
        let tmp = tempfile::tempdir().unwrap();
        let readers = write_inputs(tmp.path(), &inputs, seed);
        for r in &readers {
            r.verify().unwrap();
        }
        let want = expected(&inputs);
        let direct = write_part(tmp.path(), 100, &want, 5 + seed as usize % 11, options(8));
        assert_holds(&direct, &want, "the model through the writer");

        let chunk = [1, 900, 6_000, 1 << 20, 32 << 20][seed as usize % 5];
        let (meta, stats) = merge(&readers, tmp.path(), 101, merge_options(chunk)).unwrap();
        assert!(
            !tmp.path()
                .join(format!("{}.tmp", part_dir_name(101)))
                .exists()
        );
        let merged = PartReader::open(part_path(tmp.path(), 101)).unwrap();
        merged.verify().unwrap();
        let what = format!("seed {seed}, {} inputs, chunk {chunk}", readers.len());
        assert_same_part(&merged, &direct, &what);
        assert_holds(&merged, &want, &what);

        let rows_in: usize = inputs.iter().map(Vec::len).sum();
        assert_eq!(stats.inputs, readers.len(), "{what}");
        assert_eq!(stats.rows_in, rows_in as u64, "{what}");
        assert_eq!(stats.rows_out, want.len() as u64, "{what}");
        assert_eq!(
            stats.duplicates_dropped,
            (rows_in - want.len()) as u64,
            "{what}"
        );
        assert_eq!(stats.traces, direct.trace_count(), "{what}");
        assert_eq!(stats.traces_stitched, stitched(&inputs), "{what}");
        assert_eq!(stats.payload_out, meta.payload, "{what}");
        assert_eq!(meta.rows, want.len() as u64);
        assert_eq!(meta.level, 1);
        assert_eq!(meta.window_ns, WINDOW_NS);
        assert_eq!(meta.min_start_ns, direct.min_start_ns());
        assert_eq!(meta.max_end_ns, direct.max_end_ns());

        // Text is the same pieces a flush would have made, stored once: the
        // totals equal those of the direct part and are less than the inputs'
        // when inputs share text.
        let direct_totals = direct.payload_totals();
        assert_eq!(meta.payload.pieces, direct_totals.pieces, "{what}");
        assert_eq!(
            meta.payload.unique_bytes, direct_totals.unique_bytes,
            "{what}"
        );
        assert_eq!(
            meta.payload.logical_bytes, direct_totals.logical_bytes,
            "{what}"
        );
        assert!(
            meta.payload.unique_bytes <= stats.payload_in.unique_bytes,
            "{what}"
        );
        if meta.payload.unique_bytes < stats.payload_in.unique_bytes {
            shared_pieces += 1;
        }
        dups += stats.duplicates_dropped;
        stitch += stats.traces_stitched;
    }
    // The generator exercises what it is meant to.
    assert!(
        dups > 100 && stitch > 100 && shared_pieces > 20,
        "{dups} {stitch} {shared_pieces}"
    );
}

#[test]
fn a_merge_of_one_input_reproduces_it() {
    for seed in 0..6u64 {
        let inputs = generate(seed + 500, 1, false);
        let tmp = tempfile::tempdir().unwrap();
        let readers = write_inputs(tmp.path(), &inputs, seed);
        let (_, stats) = merge(&readers, tmp.path(), 9, merge_options(4_000)).unwrap();
        assert_eq!(stats.duplicates_dropped, 0);
        assert_eq!(stats.traces_stitched, 0);
        let merged = PartReader::open(part_path(tmp.path(), 9)).unwrap();
        merged.verify().unwrap();
        assert_same_part(&merged, &readers[0], &format!("seed {seed}"));
        assert_eq!(
            merged.payload_totals().unique_bytes,
            readers[0].payload_totals().unique_bytes
        );
        assert_eq!(
            merged.payload_totals().logical_bytes,
            readers[0].payload_totals().logical_bytes
        );
    }
}

#[test]
fn a_merged_part_can_be_merged_again_with_a_further_part() {
    for seed in 0..12u64 {
        let inputs = generate(seed + 900, 4, true);
        if inputs.len() < 2 {
            continue;
        }
        let tmp = tempfile::tempdir().unwrap();
        let readers = write_inputs(tmp.path(), &inputs, seed);
        let (first, last) = readers.split_at(readers.len() - 1);
        let (_, s1) = merge(first, tmp.path(), 50, merge_options(3_000)).unwrap();
        let m1 = PartReader::open(part_path(tmp.path(), 50)).unwrap();
        m1.verify().unwrap();

        let (meta, s2) = merge_parts(
            &[&m1, &last[0]],
            tmp.path(),
            51,
            0,
            WINDOW_NS,
            merge_options(1 << 20),
        )
        .unwrap();
        let m2 = PartReader::open(part_path(tmp.path(), 51)).unwrap();
        m2.verify().unwrap();

        // The model of two steps: the first merge's rows, in the order they
        // are stored, are one input of the second.
        let step1 = expected(&inputs[..inputs.len() - 1]);
        let want = expected(&[step1, inputs[inputs.len() - 1].clone()]);
        assert_holds(&m2, &want, &format!("seed {seed}"));
        assert_eq!(meta.rows, want.len() as u64);
        assert_eq!(
            s2.rows_in,
            s1.rows_out + readers.last().unwrap().row_count()
        );
    }
}

#[test]
fn text_held_by_several_inputs_is_stored_once() {
    // Three inputs that all hold the same ten texts (and nothing else), in
    // different spans.
    let mut rng = StdRng::seed_from_u64(77);
    let texts: Vec<Vec<u8>> = (0..10).map(|_| random_bytes(&mut rng, CUT * 6)).collect();
    let pool = Pool {
        texts: texts.clone(),
        origins: vec![Pool::new(&mut rng).origins[0].clone()],
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut inputs: Vec<Vec<Row>> = Vec::new();
    for p in 0..3u32 {
        let mut rows = Vec::new();
        for (i, text) in texts.iter().enumerate() {
            let mut r = pool.row(
                &mut rng,
                p * 100 + i as u32,
                0,
                None,
                i64::from(p) * 1000 + i as i64,
            );
            r.attrs = text.clone();
            r.events = Some(texts[(i + 1) % texts.len()].clone());
            r.links = None;
            rows.push(r);
        }
        inputs.push(stored_order(rows));
    }
    let readers = write_inputs(tmp.path(), &inputs, 1);
    let (meta, stats) = merge(&readers, tmp.path(), 10, merge_options(1 << 20)).unwrap();
    let one = readers[0].payload_totals();
    // Each input holds the ten texts (60 pieces); the output holds them once.
    assert_eq!(one.unique_bytes, (10 * 6 * CUT) as u64);
    assert_eq!(stats.payload_in.unique_bytes, 3 * one.unique_bytes);
    assert_eq!(meta.payload.unique_bytes, one.unique_bytes);
    assert_eq!(meta.payload.pieces, one.pieces);
    // Every reference is still counted.
    assert_eq!(meta.payload.logical_bytes, stats.payload_in.logical_bytes);
    PartReader::open(part_path(tmp.path(), 10))
        .unwrap()
        .verify()
        .unwrap();
}

#[test]
fn text_that_no_two_inputs_share_costs_no_more_than_the_sum() {
    let mut rng = StdRng::seed_from_u64(78);
    let tmp = tempfile::tempdir().unwrap();
    let mut inputs: Vec<Vec<Row>> = Vec::new();
    let pool = Pool::new(&mut rng);
    for p in 0..4u32 {
        let rows = (0..12u32)
            .map(|i| {
                let mut r = pool.row(&mut rng, p * 100 + i, 0, None, i64::from(i));
                r.attrs = random_bytes(&mut rng, CUT * 5);
                r.events = Some(random_bytes(&mut rng, CUT * 3));
                r.links = None;
                r
            })
            .collect();
        inputs.push(stored_order(rows));
    }
    let readers = write_inputs(tmp.path(), &inputs, 2);
    let (meta, stats) = merge(&readers, tmp.path(), 10, merge_options(1 << 20)).unwrap();
    assert_eq!(meta.payload.unique_bytes, stats.payload_in.unique_bytes);
    assert_eq!(meta.payload.pieces, stats.payload_in.pieces);
    assert_eq!(meta.payload.logical_bytes, stats.payload_in.logical_bytes);
    assert!(
        meta.payload.stored_bytes <= stats.payload_in.stored_bytes,
        "{:?}",
        stats
    );
}

#[test]
fn span_only_parts_shrink_a_lot_when_merged() {
    // Many small parts of the same kind of spans: each carries its own
    // footer, page index, dictionary pages and row groups.
    let mut rng = StdRng::seed_from_u64(5);
    let pool = Pool::new(&mut rng);
    let tmp = tempfile::tempdir().unwrap();
    let mut readers = Vec::new();
    let mut all: Vec<Vec<Row>> = Vec::new();
    for p in 0..40u32 {
        let mut rows = Vec::new();
        for t in 0..8u32 {
            // Trace `t` of every part is the same trace: stitched 40 ways.
            for j in 0..6u32 {
                let mut r = pool.row(
                    &mut rng,
                    t,
                    p * 10 + j,
                    (j > 0).then_some(p * 10),
                    i64::from(t) * 100 + i64::from(p) * 7 + i64::from(j),
                );
                r.attrs = words(&mut rng, 40);
                r.events = None;
                r.links = None;
                rows.push(r);
            }
        }
        let rows = stored_order(rows);
        let own = WriterOptions {
            row_group_rows: 16,
            ..WriterOptions::default()
        };
        readers.push(write_part(tmp.path(), p as u64 + 1, &rows, 50, own));
        all.push(rows);
    }
    let sum: u64 = (1..=40).map(|i| dir_size(&part_path(tmp.path(), i))).sum();
    let started = std::time::Instant::now();
    let (meta, stats) = merge(&readers, tmp.path(), 99, MergeOptions::default()).unwrap();
    let took = started.elapsed();
    let merged = dir_size(&part_path(tmp.path(), 99));
    let merged_reader = PartReader::open(part_path(tmp.path(), 99)).unwrap();
    eprintln!(
        "span-only: {} inputs, {} rows, sum of inputs {sum} bytes, merged {merged} bytes ({:.1}%), \
         row groups {} -> {}; merge took {took:?} ({:.0} rows/s, debug build)",
        stats.inputs,
        stats.rows_out,
        merged as f64 * 100.0 / sum as f64,
        readers
            .iter()
            .map(PartReader::row_group_count)
            .sum::<usize>(),
        merged_reader.row_group_count(),
        stats.rows_in as f64 / took.as_secs_f64()
    );
    assert_eq!(meta.payload.pieces, 0);
    assert_eq!(merged_reader.row_group_count(), 1);
    assert_eq!(merged_reader.summary_row_group_count(), 1);
    assert!(
        merged * 100 < sum * 20,
        "merged {merged} bytes is not clearly smaller than the {sum} bytes of its inputs"
    );
    assert_holds(&merged_reader, &expected(&all), "size test");
}

#[test]
fn agent_shaped_text_merges_to_less_than_its_parts() {
    // Each part is a flush of some conversations that grow by a message per
    // call; every part has the system prompt and tool definitions, and the
    // conversations continue across parts.
    let mut rng = StdRng::seed_from_u64(6);
    let system = words(&mut rng, CUT * 20);
    let tools = words(&mut rng, CUT * 30);
    let pool = Pool::new(&mut rng);
    let tmp = tempfile::tempdir().unwrap();
    let mut readers = Vec::new();
    let mut messages: Vec<Vec<Vec<u8>>> = vec![Vec::new(); 6];
    let mut all: Vec<Vec<Row>> = Vec::new();
    for p in 0..8u32 {
        let mut rows = Vec::new();
        for (t, convo) in messages.iter_mut().enumerate() {
            for call in 0..3u32 {
                convo.push(words(&mut rng, CUT * 4));
                let mut r = pool.row(
                    &mut rng,
                    t as u32,
                    p * 3 + call,
                    None,
                    i64::from(p) * 1000 + i64::from(call),
                );
                r.attrs = [system.clone(), tools.clone(), convo.concat()].concat();
                r.events = None;
                r.links = None;
                rows.push(r);
            }
        }
        let rows = stored_order(rows);
        readers.push(write_part(tmp.path(), p as u64 + 1, &rows, 6, options(4)));
        all.push(rows);
    }
    let sum: u64 = (1..=8).map(|i| dir_size(&part_path(tmp.path(), i))).sum();
    let (meta, stats) = merge(&readers, tmp.path(), 99, MergeOptions::default()).unwrap();
    let merged = dir_size(&part_path(tmp.path(), 99));
    eprintln!(
        "text: {} inputs, {} rows, sum of inputs {sum} bytes, merged {merged} bytes ({:.1}%); \
         unique text {} -> {} bytes, pieces {} -> {}, logical {}",
        stats.inputs,
        stats.rows_out,
        merged as f64 * 100.0 / sum as f64,
        stats.payload_in.unique_bytes,
        meta.payload.unique_bytes,
        stats.payload_in.pieces,
        meta.payload.pieces,
        meta.payload.logical_bytes,
    );
    assert!(meta.payload.unique_bytes * 3 < stats.payload_in.unique_bytes);
    assert!(merged < sum);
    let merged_reader = PartReader::open(part_path(tmp.path(), 99)).unwrap();
    merged_reader.verify().unwrap();
    assert_holds(&merged_reader, &expected(&all), "text test");
}

#[test]
fn one_trace_larger_than_a_chunk_is_split_across_chunks_and_stays_one_run() {
    let mut rng = StdRng::seed_from_u64(8);
    let pool = Pool::new(&mut rng);
    let tmp = tempfile::tempdir().unwrap();
    // One trace of 300 spans spread over three inputs, and a few small ones.
    let mut inputs: Vec<Vec<Row>> = vec![Vec::new(); 3];
    for j in 0..300u32 {
        let mut r = pool.row(&mut rng, 0, j, (j > 0).then_some(0), i64::from(j % 50));
        r.attrs = random_bytes(&mut rng, 100);
        r.events = Some(random_bytes(&mut rng, 100));
        inputs[(j % 3) as usize].push(r);
    }
    for t in 1..6u32 {
        let r = pool.row(&mut rng, t, 0, None, i64::from(t) * 3);
        inputs[(t % 3) as usize].push(r);
    }
    let inputs: Vec<Vec<Row>> = inputs.into_iter().map(stored_order).collect();
    let readers = write_inputs(tmp.path(), &inputs, 3);
    let want = expected(&inputs);
    // A chunk of 2 KiB is about ten rows; the trace is thirty times that.
    let (_, stats) = merge(&readers, tmp.path(), 20, merge_options(2_048)).unwrap();
    let merged = PartReader::open(part_path(tmp.path(), 20)).unwrap();
    assert_holds(&merged, &want, "large trace");
    assert_eq!(merged.trace_count(), 6);
    assert_eq!(stats.traces_stitched, 1);
    let big = merged.find_trace(TraceId(trace_id(0))).unwrap();
    assert_eq!(big.row_count, 300);
    merged.verify().unwrap();
}

struct Heard(std::sync::Mutex<Vec<(u32, Vec<u8>)>>);

impl PieceObserver for Heard {
    fn piece_stored(&self, id: u32, bytes: &[u8]) {
        self.0.lock().unwrap().push((id, bytes.to_vec()));
    }
}

#[test]
fn the_observer_hears_of_the_outputs_pieces_as_on_a_flush() {
    let inputs = generate(4321, 3, true);
    let tmp = tempfile::tempdir().unwrap();
    let readers = write_inputs(tmp.path(), &inputs, 5);
    let heard = Arc::new(Heard(std::sync::Mutex::new(Vec::new())));
    let mut opts = merge_options(3_000);
    opts.writer.piece_observer = Some(heard.clone());
    let (meta, _) = merge(&readers, tmp.path(), 40, opts).unwrap();
    let heard = heard.0.lock().unwrap();
    assert_eq!(heard.len() as u64, meta.payload.pieces);
    assert!(
        heard
            .iter()
            .enumerate()
            .all(|(n, (id, _))| *id as usize == n)
    );
    let merged = PartReader::open(part_path(tmp.path(), 40)).unwrap();
    let store = merged.piece_store();
    let mut session = store.session();
    for (id, bytes) in heard.iter() {
        let mut out = Vec::new();
        store.read_piece(&mut session, *id, &mut out).unwrap();
        assert_eq!(&out, bytes);
    }
}

fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|f| {
            let f = f.unwrap();
            (
                f.file_name().to_string_lossy().into_owned(),
                std::fs::read(f.path()).unwrap(),
            )
        })
        .collect()
}

fn corrupt_message(err: Error) -> String {
    match err {
        Error::Corrupt(m) => m,
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// Flips one bit of `file` in place, through a handle that does not replace
/// the file, so that readers that have it open see the damage.
fn flip_in_place(path: &Path, at: u64) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, at).unwrap();
    b[0] ^= 0x10;
    f.write_all_at(&b, at).unwrap();
}

#[test]
fn damage_in_any_file_of_any_input_is_corrupt_and_leaves_nothing() {
    let inputs = generate(2024, 3, true);
    assert!(inputs.len() >= 2);
    for victim in 0..inputs.len() {
        for file in [SPANS_FILE, SUMMARY_FILE, PIECES_BLK, PIECES_IDX] {
            for place in [0.0, 0.5, 1.0] {
                let tmp = tempfile::tempdir().unwrap();
                let readers = write_inputs(tmp.path(), &inputs, 6);
                let path = part_path(tmp.path(), victim as u64 + 1).join(file);
                let len = std::fs::metadata(&path).unwrap().len();
                if len == 0 {
                    continue;
                }
                // The last four bytes of `pieces.idx` are its own checksum,
                // which the part's recorded checksum leaves out and which
                // is only read when the part is opened.
                let last = if file == PIECES_IDX { len - 5 } else { len - 1 };
                let at = (last as f64 * place) as u64;
                flip_in_place(&path, at);

                let before: Vec<_> = (1..=inputs.len() as u64)
                    .map(|i| snapshot(&part_path(tmp.path(), i)))
                    .collect();
                let err = merge(&readers, tmp.path(), 60, merge_options(5_000)).unwrap_err();
                let m = corrupt_message(err);
                assert!(
                    m.contains(&format!("merge input {victim}")) && m.contains(file),
                    "{file} at {at}/{len}: {m}"
                );
                assert!(!part_path(tmp.path(), 60).exists());
                assert!(
                    !tmp.path()
                        .join(format!("{}.tmp", part_dir_name(60)))
                        .exists()
                );
                let after: Vec<_> = (1..=inputs.len() as u64)
                    .map(|i| snapshot(&part_path(tmp.path(), i)))
                    .collect();
                assert_eq!(before, after, "the inputs were changed");
            }
        }
    }
}

#[test]
fn damage_to_the_trace_index_is_refused_when_the_part_is_opened() {
    let inputs = generate(2025, 2, true);
    let tmp = tempfile::tempdir().unwrap();
    write_inputs(tmp.path(), &inputs, 7);
    let path = part_path(tmp.path(), 1).join(INDEX_FILE);
    let len = std::fs::metadata(&path).unwrap().len();
    for at in [0, len / 2, len - 1] {
        flip_in_place(&path, at);
        corrupt_message(PartReader::open(part_path(tmp.path(), 1)).err().unwrap());
        flip_in_place(&path, at);
    }
}

#[test]
fn a_part_of_another_format_version_is_refused_before_it_can_be_merged() {
    let inputs = generate(2026, 2, false);
    let tmp = tempfile::tempdir().unwrap();
    write_inputs(tmp.path(), &inputs, 8);
    let path = part_path(tmp.path(), 2).join(INDEX_FILE);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&4u32.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    let m = corrupt_message(PartReader::open(part_path(tmp.path(), 2)).err().unwrap());
    assert!(m.contains("format version 4"), "{m}");
}

#[test]
fn damage_without_the_up_front_check_is_still_an_error_or_a_copy_never_a_panic() {
    let inputs = generate(2027, 3, true);
    let mut errors = 0;
    for victim in 0..inputs.len() {
        for file in [SPANS_FILE, PIECES_BLK] {
            let tmp = tempfile::tempdir().unwrap();
            let readers = write_inputs(tmp.path(), &inputs, 9);
            let path = part_path(tmp.path(), victim as u64 + 1).join(file);
            let len = std::fs::metadata(&path).unwrap().len();
            for at in (0..len).step_by((len as usize / 40).max(1)) {
                flip_in_place(&path, at);
                let mut opts = merge_options(5_000);
                opts.verify_inputs = false;
                match merge(&readers, tmp.path(), 61, opts) {
                    Ok(_) => {
                        std::fs::remove_dir_all(part_path(tmp.path(), 61)).unwrap();
                    }
                    Err(e) => {
                        errors += 1;
                        let m = corrupt_message(e);
                        assert!(m.contains(&format!("merge input {victim}")), "{m}");
                        assert!(!part_path(tmp.path(), 61).exists());
                        assert!(
                            !tmp.path()
                                .join(format!("{}.tmp", part_dir_name(61)))
                                .exists()
                        );
                    }
                }
                flip_in_place(&path, at);
            }
        }
    }
    assert!(errors > 0);
}

/// Replaces the piece store of `part` by `store` and patches what
/// `traces.idx` records of it, so that the part opens.
fn install(part: &Path, store: &Path, sum: &StoreSummary) {
    for f in [PIECES_BLK, PIECES_IDX] {
        std::fs::copy(store.join(f), part.join(f)).unwrap();
    }
    let mut index = std::fs::read(part.join(INDEX_FILE)).unwrap();
    for (at, s) in [(40, sum.blocks), (52, sum.index)] {
        index[at..at + 8].copy_from_slice(&s.len.to_le_bytes());
        index[at + 8..at + 12].copy_from_slice(&s.crc32c.to_le_bytes());
    }
    let body = index.len() - 4;
    let crc = crc32c::crc32c(&index[..body]);
    index[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(part.join(INDEX_FILE), &index).unwrap();
}

#[test]
fn a_forged_translation_is_caught_by_the_checksum_the_ropes_carry() {
    // Pieces of equal length, all different, so that a store with its pieces
    // in another order is consistent in every length and total, and only the
    // text differs: what a wrong translation table would produce.
    let mut rng = StdRng::seed_from_u64(31);
    let pool = Pool::new(&mut rng);
    let tmp = tempfile::tempdir().unwrap();
    let mut inputs = Vec::new();
    for p in 0..2u32 {
        let rows = (0..6u32)
            .map(|i| {
                let mut r = pool.row(&mut rng, p * 10 + i, 0, None, i64::from(i));
                r.attrs = random_bytes(&mut rng, CUT * 3);
                r.events = None;
                r.links = None;
                r
            })
            .collect();
        inputs.push(stored_order(rows));
    }
    let readers = write_inputs(tmp.path(), &inputs, 10);
    merge(&readers, tmp.path(), 70, merge_options(1 << 20)).unwrap();
    let part = part_path(tmp.path(), 70);
    let honest = PartReader::open(&part).unwrap();
    honest.verify().unwrap();
    let pieces = honest.payload_totals().pieces as u32;
    assert_eq!(pieces, 36);

    let scratch = tempfile::tempdir().unwrap();
    let mut w = PieceWriter::create(scratch.path(), PieceWriterOptions::default()).unwrap();
    let store = honest.piece_store();
    let mut session = store.session();
    for id in 0..pieces {
        // The piece that the references to `id` would find after a
        // translation that is off by one.
        let mut bytes = Vec::new();
        store
            .read_piece(&mut session, (id + 1) % pieces, &mut bytes)
            .unwrap();
        w.add(&bytes).unwrap();
    }
    let sum = w.finish().unwrap();
    drop((honest, session));
    install(&part, scratch.path(), &sum);

    let forged = PartReader::open(&part).unwrap();
    let m = corrupt_message(forged.read_trace(TraceId(trace_id(0)), None).unwrap_err());
    assert!(m.contains("checksum"), "{m}");
    corrupt_message(forged.verify().unwrap_err());
}

#[test]
fn readers_on_the_inputs_are_not_disturbed_by_a_merge() {
    let inputs = generate(3030, 4, true);
    let tmp = tempfile::tempdir().unwrap();
    let readers = write_inputs(tmp.path(), &inputs, 11);
    let readers = Arc::new(readers);
    let baseline: Vec<Vec<String>> = readers
        .iter()
        .map(|r| {
            r.trace_entries()
                .iter()
                .map(|e| {
                    format!(
                        "{:?}",
                        records(&flat(&r.read_trace(e.trace_id, None).unwrap().unwrap()))
                    )
                })
                .collect()
        })
        .collect();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let threads: Vec<_> = (0..3)
        .map(|n| {
            let (readers, stop, baseline) = (readers.clone(), stop.clone(), baseline.clone());
            std::thread::spawn(move || {
                let mut rounds = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) || rounds < 2 {
                    let r = &readers[(n + rounds) % readers.len()];
                    let i = (n + rounds) % readers.len();
                    for (j, e) in r.trace_entries().iter().enumerate() {
                        let got = format!(
                            "{:?}",
                            records(&flat(&r.read_trace(e.trace_id, None).unwrap().unwrap()))
                        );
                        assert_eq!(got, baseline[i][j]);
                    }
                    rounds += 1;
                }
                rounds
            })
        })
        .collect();
    for id in 80..84u64 {
        merge(&readers, tmp.path(), id, merge_options(2_000)).unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in threads {
        assert!(t.join().unwrap() >= 2);
    }
}

#[test]
fn bad_arguments_are_refused_and_leave_nothing() {
    let inputs = generate(1, 2, false);
    let tmp = tempfile::tempdir().unwrap();
    let readers = write_inputs(tmp.path(), &inputs, 12);
    let none: [&PartReader; 0] = [];
    assert!(matches!(
        merge_parts(&none, tmp.path(), 5, 0, WINDOW_NS, MergeOptions::default()),
        Err(Error::InvalidArgument(_))
    ));
    let zero = MergeOptions {
        chunk_bytes: 0,
        ..MergeOptions::default()
    };
    assert!(matches!(
        merge(&readers, tmp.path(), 5, zero),
        Err(Error::InvalidArgument(_))
    ));
    let refs: Vec<&PartReader> = readers.iter().collect();
    assert!(matches!(
        merge_parts(&refs, tmp.path(), 5, 0, 0, MergeOptions::default()),
        Err(Error::InvalidArgument(_))
    ));
    // The id of an input is taken.
    assert!(matches!(
        merge(&readers, tmp.path(), 1, MergeOptions::default()),
        Err(Error::InvalidArgument(_))
    ));
    assert!(!part_path(tmp.path(), 5).exists());
    assert!(
        !tmp.path()
            .join(format!("{}.tmp", part_dir_name(5)))
            .exists()
    );
    readers[0].verify().unwrap();
}

// -------------------------------------------------------------- arrival ---

fn permutations(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for rest in permutations(n - 1) {
        for at in 0..=rest.len() {
            let mut p = rest.clone();
            p.insert(at, n - 1);
            out.push(p);
        }
    }
    out
}

fn merged_rows(inputs: &[Vec<Row>], seed: u64) -> (Vec<(String, u64)>, MergeStats) {
    let tmp = tempfile::tempdir().unwrap();
    let readers = write_inputs(tmp.path(), inputs, seed);
    let (_, stats) = merge(&readers, tmp.path(), 100, merge_options(1 << 20)).unwrap();
    let merged = PartReader::open(part_path(tmp.path(), 100)).unwrap();
    let batches = merged.read_rows(0, merged.row_count(), None).unwrap();
    let rows = records(&flat(&batches))
        .iter()
        .map(|r| (r.name.to_owned(), r.arrival))
        .collect();
    (rows, stats)
}

#[test]
fn the_copy_with_the_smallest_arrival_wins_whatever_the_order_of_the_inputs() {
    let mut rng = StdRng::seed_from_u64(77);
    let pool = Pool::new(&mut rng);
    // One span under the same ids in four parts, each copy with its own name.
    // Two copies share the smallest arrival.
    let copies = [("late", 9u64), ("earliest", 3), ("middle", 5), ("tie", 3)];
    let rows: Vec<Row> = copies
        .iter()
        .map(|&(name, arrival)| {
            let mut r = pool.row(&mut rng, 1, 0, None, 100);
            r.name = name.into();
            r.arrival = arrival;
            r
        })
        .collect();
    for order in permutations(copies.len()) {
        let inputs: Vec<Vec<Row>> = order.iter().map(|&i| vec![rows[i].clone()]).collect();
        let (got, stats) = merged_rows(&inputs, 0);
        // The smallest arrival wins; of the two that tie, the earlier input.
        let winner = order.iter().find(|&&i| copies[i].1 == 3).unwrap();
        let want = (copies[*winner].0.to_string(), 3);
        assert_eq!(got, vec![want], "inputs in the order {order:?}");
        assert_eq!(stats.duplicates_dropped, 3, "{order:?}");
    }
}

#[test]
fn the_copy_with_the_smallest_arrival_wins_inside_one_input_too() {
    let mut rng = StdRng::seed_from_u64(78);
    let pool = Pool::new(&mut rng);
    let mut a = pool.row(&mut rng, 1, 0, None, 100);
    a.name = "first row, later arrival".into();
    a.arrival = 8;
    let mut b = a.clone();
    b.name = "second row, earlier arrival".into();
    b.arrival = 2;
    let (got, _) = merged_rows(&[vec![a, b]], 0);
    assert_eq!(got, vec![("second row, earlier arrival".to_string(), 2)]);
}

#[test]
fn the_order_of_the_inputs_changes_only_which_of_equal_arrivals_wins() {
    for seed in 0..6u64 {
        let inputs = generate(seed + 3_000, 3, true);
        // The smallest arrival of each span among all its copies.
        let mut smallest: HashMap<([u8; 16], [u8; 8]), u64> = HashMap::new();
        for r in inputs.iter().flatten() {
            let e = smallest.entry(r.id()).or_insert(r.arrival);
            *e = (*e).min(r.arrival);
        }
        for order in permutations(inputs.len()) {
            let ordered: Vec<Vec<Row>> = order.iter().map(|&i| inputs[i].clone()).collect();
            let tmp = tempfile::tempdir().unwrap();
            let readers = write_inputs(tmp.path(), &ordered, seed);
            merge(&readers, tmp.path(), 100, merge_options(900)).unwrap();
            let merged = PartReader::open(part_path(tmp.path(), 100)).unwrap();
            let what = format!("seed {seed}, inputs in the order {order:?}");
            let want = expected(&ordered);
            assert_holds(&merged, &want, &what);
            for r in &want {
                assert_eq!(r.arrival, smallest[&r.id()], "{what}: {:?}", r.id());
            }
        }
    }
}

/// The output's traces are ordered by the earliest start in the inputs' index
/// entries, which can come from a copy of a span that the merge drops. The
/// summary and the trace index describe the rows that were kept, and the
/// summary file is sorted by those, so listing is unaffected.
#[test]
fn a_trace_whose_earliest_start_is_in_a_dropped_copy_has_the_kept_rows_times() {
    let mut rng = StdRng::seed_from_u64(79);
    let pool = Pool::new(&mut rng);
    // Trace 1, span 0: the copy in input A starts at 5 and arrived second; the
    // copy in input B starts at 10 and arrived first, so it is the one kept.
    let mut in_a = pool.row(&mut rng, 1, 0, None, 5);
    in_a.end = 6;
    in_a.name = "copy in a".into();
    in_a.arrival = 2;
    let mut in_b = in_a.clone();
    in_b.start = 10;
    in_b.end = 11;
    in_b.name = "copy in b".into();
    in_b.arrival = 1;
    // Trace 2 starts at 7, between the two.
    let mut other = pool.row(&mut rng, 2, 0, None, 7);
    other.end = 8;
    other.name = "trace two".into();
    other.arrival = 3;

    let tmp = tempfile::tempdir().unwrap();
    let inputs = [stored_order(vec![in_a, other]), stored_order(vec![in_b])];
    let readers = write_inputs(tmp.path(), &inputs, 0);
    let (_, stats) = merge(&readers, tmp.path(), 100, merge_options(1 << 20)).unwrap();
    assert_eq!(stats.duplicates_dropped, 1);
    let merged = PartReader::open(part_path(tmp.path(), 100)).unwrap();
    merged.verify().unwrap();

    // spans.parquet: trace 1 first (ordered by the earliest start of the
    // inputs' entries, 5), with the copy that arrived first; then trace 2.
    let batches = merged.read_rows(0, merged.row_count(), None).unwrap();
    let rows: Vec<(String, i64, u64)> = records(&flat(&batches))
        .iter()
        .map(|r| (r.name.to_owned(), r.start_ns, r.arrival))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("copy in b".to_string(), 10, 1),
            ("trace two".to_string(), 7, 3)
        ]
    );

    // The summary is sorted by the kept rows' starts: trace 2, then trace 1.
    let got: Vec<(TraceId, i64, i64)> = summaries(&merged)
        .iter()
        .map(|s| (s.trace_id, s.start_ns, s.end_ns))
        .collect();
    assert_eq!(
        got,
        vec![(TraceId(trace_id(2)), 7, 8), (TraceId(trace_id(1)), 10, 11)]
    );

    // The trace index carries the kept rows' times too.
    let t1 = merged.find_trace(TraceId(trace_id(1))).unwrap();
    assert_eq!((t1.start_ns, t1.end_ns, t1.first_row), (10, 11, 0));
    let t2 = merged.find_trace(TraceId(trace_id(2))).unwrap();
    assert_eq!((t2.start_ns, t2.end_ns, t2.first_row), (7, 8, 1));
}
