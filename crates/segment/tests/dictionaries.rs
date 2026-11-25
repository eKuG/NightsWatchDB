//! The origin columns are dictionary columns. These tests write parts whose
//! batches have different dictionaries and check what the reader gives back:
//! the same values, in batches that have exactly the span schema.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{LargeBinaryArray, RecordBatch};
use nwdb_core::{
    BatchLimits, Origin, OriginKeys, SpanBatchBuilder, SpanColumns, SpanId, SpanRecord, TraceId,
    part_dir_name, span_schema,
};
use nwdb_segment::{PartReader, PartWriter, ScanOptions, WriterOptions};
use std::path::Path;

const WINDOW_NS: i64 = 900_000_000_000;

/// An owned origin, so tests can keep what they expect.
#[derive(Clone, Debug, PartialEq)]
struct Org {
    resource: Option<Vec<u8>>,
    service: Option<String>,
    resource_url: Option<String>,
    scope: Option<Vec<u8>>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_url: Option<String>,
}

impl Org {
    fn new(n: u8) -> Self {
        Org {
            resource: Some(vec![n; 40 + n as usize]),
            service: Some(format!("service-{n}")),
            resource_url: (n.is_multiple_of(2)).then(|| format!("https://example.test/r/{n}")),
            scope: Some(vec![n + 100; 7]),
            scope_name: Some(format!("scope-{n}")),
            scope_version: (!n.is_multiple_of(3)).then(|| format!("1.{n}")),
            scope_url: None,
        }
    }

    fn none() -> Self {
        Org {
            resource: None,
            service: None,
            resource_url: None,
            scope: None,
            scope_name: None,
            scope_version: None,
            scope_url: None,
        }
    }

    fn origin(&self) -> Origin<'_> {
        Origin {
            resource: self.resource.as_deref(),
            service_name: self.service.as_deref(),
            resource_schema_url: self.resource_url.as_deref(),
            scope: self.scope.as_deref(),
            scope_name: self.scope_name.as_deref(),
            scope_version: self.scope_version.as_deref(),
            scope_schema_url: self.scope_url.as_deref(),
        }
    }
}

fn span(trace: u8, i: u32) -> SpanRecord<'static> {
    SpanRecord {
        trace_id: TraceId([trace; 16]),
        span_id: SpanId([(i % 250) as u8 + 1, (i / 250) as u8, 0, 0, 0, 0, 0, 1]),
        name: "s",
        start_ns: 1_000 + i64::from(i),
        end_ns: 2_000 + i64::from(i),
        attrs: b"a",
        ..SpanRecord::default()
    }
}

/// One expected row: the span and the origin it was written with.
type Row = (SpanRecord<'static>, Org);

/// Builds a batch of `rows`, interning the origins in the order given by
/// `intern_first` before any row, so that two batches number the same value
/// differently.
fn batch(rows: &[Row], intern_first: &[&Org]) -> RecordBatch {
    let mut b = SpanBatchBuilder::new(BatchLimits {
        max_rows: 1 << 20,
        max_bytes: 1 << 40,
    });
    let mut known: Vec<(Org, OriginKeys)> = Vec::new();
    let mut keys_of = |b: &mut SpanBatchBuilder, o: &Org| {
        if let Some((_, k)) = known.iter().find(|(k, _)| k == o) {
            return *k;
        }
        let k = b.intern_origin(&o.origin());
        known.push((o.clone(), k));
        k
    };
    for o in intern_first {
        keys_of(&mut b, o);
    }
    for (s, o) in rows {
        let k = keys_of(&mut b, o);
        b.append_span(s, &k);
    }
    b.finish()
}

fn open(dir: &Path, id: u64) -> PartReader {
    PartReader::open(dir.join(part_dir_name(id))).unwrap()
}

fn rows_of(batches: &[RecordBatch]) -> Vec<(SpanRecord<'_>, Origin<'_>)> {
    let mut out = Vec::new();
    for b in batches {
        let cols = SpanColumns::try_new(b).unwrap();
        out.extend((0..cols.len()).map(|i| (cols.record(i), cols.origin(i))));
    }
    out
}

fn check(batches: &[RecordBatch], want: &[&Row]) {
    let got = rows_of(batches);
    assert_eq!(got.len(), want.len());
    for (i, ((rec, origin), (s, o))) in got.iter().zip(want).enumerate() {
        assert_eq!(*origin, o.origin(), "row {i} origin");
        let mut expect = *s;
        expect.resource = o.resource.as_deref();
        expect.service_name = o.service.as_deref();
        expect.resource_schema_url = o.resource_url.as_deref();
        expect.scope = o.scope.as_deref();
        expect.scope_name = o.scope_name.as_deref();
        expect.scope_version = o.scope_version.as_deref();
        expect.scope_schema_url = o.scope_url.as_deref();
        assert_eq!(rec, &expect, "row {i}");
    }
}

/// Every batch has exactly the span schema, or the projection of it.
fn assert_schema(batches: &[RecordBatch], columns: Option<&[&str]>) {
    let full = span_schema();
    for b in batches {
        let want: Vec<_> = match columns {
            None => full.fields().iter().map(|f| f.as_ref().clone()).collect(),
            Some(names) => names
                .iter()
                .map(|n| full.field_with_name(n).unwrap().clone())
                .collect(),
        };
        let have: Vec<_> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        assert_eq!(have, want);
        if columns.is_none() {
            assert_eq!(b.schema(), full);
        }
    }
}

const ORIGIN_COLUMNS: [&str; 7] = [
    "resource",
    "service_name",
    "resource_schema_url",
    "scope",
    "scope_name",
    "scope_version",
    "scope_schema_url",
];

/// Two batches that number the same values differently, with a value that
/// only the second has, and rows without any origin, in one part.
fn two_batch_part(dir: &Path, options: WriterOptions) -> (Vec<Row>, Vec<Row>) {
    let (a, b, c) = (Org::new(1), Org::new(2), Org::new(3));
    // Trace 1 crosses the batch boundary.
    let first: Vec<Row> = vec![
        (span(1, 0), a.clone()),
        (span(1, 1), b.clone()),
        (span(1, 2), a.clone()),
        (span(2, 3), Org::none()),
        (span(2, 4), b.clone()),
    ];
    let second: Vec<Row> = vec![
        (span(2, 5), b.clone()),
        (span(2, 6), c.clone()),
        (span(3, 7), a.clone()),
        (span(3, 8), c.clone()),
        (span(3, 9), Org::none()),
    ];
    // In the second batch `b` is interned first and `c` second, so `a` and
    // `b` have other keys than in the first.
    let batches = [batch(&first, &[&a, &b]), batch(&second, &[&c, &b, &a])];
    let mut w = PartWriter::create(dir, 1, 0, WINDOW_NS, options).unwrap();
    for b in &batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    (first, second)
}

fn small_groups() -> WriterOptions {
    WriterOptions {
        row_group_rows: 4,
        ..WriterOptions::default()
    }
}

#[test]
fn rows_with_different_dictionaries_read_back_whole_and_by_trace() {
    for options in [WriterOptions::default(), small_groups()] {
        let tmp = tempfile::tempdir().unwrap();
        let (first, second) = two_batch_part(tmp.path(), options);
        let all: Vec<&Row> = first.iter().chain(&second).collect();
        let reader = open(tmp.path(), 1);
        reader.verify().unwrap();

        // The whole part, by scan.
        let batches: Vec<RecordBatch> = reader
            .scan(ScanOptions::default())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_schema(&batches, None);
        check(&batches, &all);

        // Each trace, including the one that crosses the batch boundary.
        for trace in 1..=3u8 {
            let want: Vec<&Row> = all
                .iter()
                .copied()
                .filter(|(s, _)| s.trace_id == TraceId([trace; 16]))
                .collect();
            let got = reader
                .read_trace(TraceId([trace; 16]), None)
                .unwrap()
                .unwrap();
            assert_schema(&got, None);
            check(&got, &want);
        }

        // A row range that starts and ends inside different row groups.
        let got = reader.read_rows(3, 5, None).unwrap();
        assert_schema(&got, None);
        check(&got, &all[3..8]);
    }
}

#[test]
fn projections_of_origin_columns_keep_their_types() {
    let tmp = tempfile::tempdir().unwrap();
    let (first, second) = two_batch_part(tmp.path(), small_groups());
    let all: Vec<&Row> = first.iter().chain(&second).collect();
    let reader = open(tmp.path(), 1);

    // Origin columns only, in an order that is not the file's.
    let columns = ["scope_name", "resource", "trace_id", "scope"];
    let got = reader
        .read_trace(TraceId([2; 16]), Some(&columns))
        .unwrap()
        .unwrap();
    assert_schema(&got, Some(&columns));
    let want: Vec<&Row> = all
        .iter()
        .copied()
        .filter(|(s, _)| s.trace_id == TraceId([2; 16]))
        .collect();
    let mut resources: Vec<Option<Vec<u8>>> = Vec::new();
    for b in &got {
        let column = b.column_by_name("resource").unwrap();
        let typed = column
            .as_dictionary::<Int32Type>()
            .downcast_dict::<LargeBinaryArray>()
            .unwrap();
        resources.extend(typed.into_iter().map(|v| v.map(<[u8]>::to_vec)));
    }
    let expected: Vec<Option<Vec<u8>>> = want.iter().map(|(_, o)| o.resource.clone()).collect();
    assert_eq!(resources, expected);

    // Scan with a projection of the origin columns only.
    let names: Vec<String> = ORIGIN_COLUMNS.iter().map(|s| s.to_string()).collect();
    let batches: Vec<RecordBatch> = reader
        .scan(ScanOptions {
            projection: Some(names),
            ..ScanOptions::default()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_schema(&batches, Some(&ORIGIN_COLUMNS));
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
}

/// A resource that thousands of rows share is stored once in memory and in
/// the file, and read back as one dictionary entry per batch.
#[test]
fn a_large_shared_resource_is_stored_once() {
    let tmp = tempfile::tempdir().unwrap();
    let mut org = Org::new(5);
    org.resource = Some((0..60_000u32).map(|i| (i % 251) as u8).collect());
    let rows: Vec<Row> = (0..6_000u32).map(|i| (span(1, i), org.clone())).collect();
    let b = batch(&rows, &[]);
    assert!(b.get_array_memory_size() < 3_000_000);
    let mut w = PartWriter::create(tmp.path(), 1, 0, WINDOW_NS, WriterOptions::default()).unwrap();
    w.write(&b).unwrap();
    let meta = w.finish().unwrap();
    // Without a dictionary the file would hold 6,000 x 60,000 bytes of
    // resource, a little less after compression.
    assert!(meta.files["spans.parquet"] < 1_000_000, "{:?}", meta.files);

    let reader = open(tmp.path(), 1);
    reader.verify().unwrap();
    let got = reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap();
    assert_schema(&got, None);
    let all: Vec<&Row> = rows.iter().collect();
    check(&got, &all);
    // What a read holds in memory is bounded the same way.
    let held: usize = got.iter().map(|b| b.get_array_memory_size()).sum();
    assert!(
        held < 6 * 3_000_000,
        "{held} bytes for {} batches",
        got.len()
    );
}

/// Every dictionary column of every batch holds exactly the values that its
/// rows use, each once, and nothing else.
fn assert_compact(batches: &[RecordBatch]) {
    for b in batches {
        for (field, column) in b.schema().fields().iter().zip(b.columns()) {
            let Some(any) = column.as_any_dictionary_opt() else {
                continue;
            };
            let used: std::collections::HashSet<i32> = column
                .as_dictionary::<Int32Type>()
                .keys()
                .iter()
                .flatten()
                .collect();
            assert_eq!(
                any.values().len(),
                used.len(),
                "column {} holds values its rows do not use",
                field.name()
            );
        }
    }
}

/// A part of `many` origins, one trace of `rows` rows each, in one row group
/// unless `options` says otherwise.
fn many_origin_part(dir: &Path, many: u8, options: WriterOptions) -> Vec<Row> {
    let rows: Vec<Row> = (0..many)
        .flat_map(|n| {
            (0..3u32).map(move |i| {
                let org = if n % 5 == 4 { Org::none() } else { Org::new(n) };
                (span(n + 1, u32::from(n) * 3 + i), org)
            })
        })
        .collect();
    let all: Vec<&Org> = rows.iter().map(|(_, o)| o).collect();
    let mut w = PartWriter::create(dir, 1, 0, WINDOW_NS, options).unwrap();
    w.write(&batch(&rows, &all)).unwrap();
    w.finish().unwrap();
    rows
}

#[test]
fn a_trace_carries_only_the_values_its_rows_use() {
    let tmp = tempfile::tempdir().unwrap();
    let rows = many_origin_part(tmp.path(), 40, WriterOptions::default());
    let reader = open(tmp.path(), 1);
    assert_eq!(reader.row_group_count(), 1);
    for trace in 1..=40u8 {
        let got = reader
            .read_trace(TraceId([trace; 16]), None)
            .unwrap()
            .unwrap();
        assert_schema(&got, None);
        assert_compact(&got);
        let want: Vec<&Row> = rows
            .iter()
            .filter(|(s, _)| s.trace_id == TraceId([trace; 16]))
            .collect();
        check(&got, &want);
    }
    // A row range inside one trace, and one that covers several.
    for (first, count) in [(1, 1), (4, 7)] {
        let got = reader.read_rows(first, count, None).unwrap();
        assert_compact(&got);
        let want: Vec<&Row> = rows[first as usize..(first + count) as usize]
            .iter()
            .collect();
        check(&got, &want);
    }
    // One value used by several rows of a trace is held once: the first
    // trace's three rows all use the same origin.
    let got = reader.read_trace(TraceId([1; 16]), None).unwrap().unwrap();
    let held = got[0].column_by_name("resource").unwrap();
    assert_eq!(held.as_any_dictionary().values().len(), 1);
}

#[test]
fn rows_without_a_value_hold_no_dictionary_values() {
    let tmp = tempfile::tempdir().unwrap();
    let rows = many_origin_part(tmp.path(), 40, WriterOptions::default());
    let reader = open(tmp.path(), 1);
    // Trace 5 is the one with no origin at all (n = 4).
    let got = reader.read_trace(TraceId([5; 16]), None).unwrap().unwrap();
    assert_compact(&got);
    for name in ORIGIN_COLUMNS {
        let c = got[0].column_by_name(name).unwrap();
        assert_eq!(c.null_count(), c.len(), "{name}");
        assert_eq!(c.as_any_dictionary().values().len(), 0, "{name}");
    }
    let want: Vec<&Row> = rows
        .iter()
        .filter(|(s, _)| s.trace_id == TraceId([5; 16]))
        .collect();
    check(&got, &want);
}

#[test]
fn projections_with_some_or_no_dictionary_columns_are_compact_too() {
    let tmp = tempfile::tempdir().unwrap();
    many_origin_part(tmp.path(), 40, WriterOptions::default());
    let reader = open(tmp.path(), 1);
    for columns in [
        &["trace_id", "name", "start_ns"][..],
        &["name", "scope", "trace_id", "service_name"][..],
        &["resource"][..],
    ] {
        let got = reader
            .read_trace(TraceId([3; 16]), Some(columns))
            .unwrap()
            .unwrap();
        assert_schema(&got, Some(columns));
        assert_compact(&got);
        assert_eq!(got.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}

/// A trace that spans row groups, whose values also recur in the next group:
/// each batch holds each of its values once, and the values are those of an
/// uncompacted read.
#[test]
fn a_trace_across_row_groups_matches_the_uncompacted_read() {
    let tmp = tempfile::tempdir().unwrap();
    let rows = many_origin_part(tmp.path(), 12, small_groups());
    let reader = open(tmp.path(), 1);
    assert!(reader.row_group_count() > 3);

    let scanned: Vec<RecordBatch> = reader
        .scan(ScanOptions::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // The scan keeps whole dictionaries.
    let uncompacted = rows_of(&scanned);
    assert_eq!(uncompacted.len(), rows.len());

    let mut first = 0;
    for trace in 1..=12u8 {
        let count = rows
            .iter()
            .filter(|(s, _)| s.trace_id == TraceId([trace; 16]))
            .count();
        let got = reader
            .read_trace(TraceId([trace; 16]), None)
            .unwrap()
            .unwrap();
        assert_schema(&got, None);
        assert_compact(&got);
        assert_eq!(rows_of(&got), uncompacted[first..first + count], "{trace}");
        first += count;
    }
    // One trace of three rows starting at row 2 crosses a group boundary of
    // four rows; read all rows at once too.
    let got = reader.read_rows(0, rows.len() as u64, None).unwrap();
    assert!(got.len() > 3);
    assert_compact(&got);
    assert_eq!(rows_of(&got), uncompacted);
}
