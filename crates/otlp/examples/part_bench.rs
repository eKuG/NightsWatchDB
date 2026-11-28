//! Writes the parts a flush would write from a file of generated traces, and
//! measures what they cost.
//!
//!     part_bench --in FILE --out DIR [--runs 5] [--sample 3000] [--digest]
//!     part_bench --read PARTS_DIR [--runs 5] [--sample 3000] [--digest]
//!
//! `FILE` is the output of `bench/tracegen --out-file`: records of a 4-byte
//! big-endian length and one `ExportTraceServiceRequest`. The requests are
//! decoded with `TraceDecoder`, all spans are grouped into traces and the
//! traces into 15-minute windows exactly as the engine does on a flush, and one
//! part is written per window with the options the server uses. Then, over the
//! parts written last:
//!
//! - `write_s`: wall time to write all the parts (decode and grouping are not
//!   counted), and `write_peak_mb`, how far the resident memory rose above
//!   what it was before the write started;
//! - `open_trace_us`: time to find one trace by the index and read all its
//!   rows, the median over `--sample` traces spread over all the parts;
//! - `scan5_s`: time to scan five columns of every part, as a token
//!   aggregate does;
//! - with `--digest`, a hash of every row of every part (after `verify`), to show that two
//!   writer settings give files that read back identically.
//!
//! Each timing is repeated `--runs` times and reported with its median and
//! range. The parts stay in `DIR/parts`; `part_stats` prints where their bytes
//! go. Memory figures need Linux.
#![allow(missing_docs)]

use nwdb_core::{
    BatchLimits, DEFAULT_WINDOW_NS, SpanBatchBuilder, SpanColumns, part_dir_name, window_start,
};
use nwdb_otlp::{OtlpSplitter, TraceDecoder};
use nwdb_segment::{PartReader, PartWriter, ScanOptions, WriterOptions};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

type R<T> = Result<T, Box<dyn std::error::Error>>;

fn status_kb(field: &str) -> u64 {
    nwdb_testkit::mem::status_kb(field).unwrap_or(0) as u64
}

/// Restarts the peak resident size from the current one.
fn reset_peak() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn summarize(label: &str, mut v: Vec<f64>, unit: &str) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = v[v.len() / 2];
    println!(
        "{label}: median {med:.3} {unit}, min {:.3}, max {:.3}, runs {}",
        v[0],
        v[v.len() - 1],
        v.len()
    );
}

/// Start, trace id and the range of its spans among the entries.
type TraceRange = (i64, [u8; 16], usize, usize);

struct Entry {
    trace: [u8; 16],
    span: [u8; 8],
    start_ns: i64,
    batch: u32,
    row: u32,
}

fn main() -> R<()> {
    let mut input = None;
    let mut out = None;
    let mut runs = 5usize;
    let mut sample = 3000usize;
    let mut digest = false;
    let mut read_only = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--in" => input = args.next(),
            "--out" => out = args.next(),
            "--runs" => runs = args.next().ok_or("--runs needs a value")?.parse()?,
            "--sample" => sample = args.next().ok_or("--sample needs a value")?.parse()?,
            "--digest" => digest = true,
            "--read" => read_only = args.next(),
            _ => return Err(format!("unknown argument {a}").into()),
        }
    }
    if let Some(dir) = read_only {
        // Only read parts that already exist, for instance ones written by
        // another build, to show that they read back the same.
        let mut dirs: Vec<_> = std::fs::read_dir(&dir)?
            .flatten()
            .map(|e| e.path())
            .collect();
        dirs.sort();
        let readers = dirs
            .iter()
            .map(PartReader::open)
            .collect::<Result<Vec<_>, _>>()?;
        let rows = readers.iter().map(|r| r.row_count() as usize).sum();
        return read_phase(&readers, rows, sample, runs, digest);
    }
    let input = input.ok_or("--in is required")?;
    let out = out.ok_or("--out is required")?;
    let parts_dir = Path::new(&out).join("parts");

    // Decode.
    let t = Instant::now();
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let mut batches = Vec::new();
    let mut r = BufReader::with_capacity(1 << 20, File::open(&input)?);
    let mut buf = Vec::new();
    loop {
        let mut h = [0u8; 4];
        if r.read(&mut h[..1])? == 0 {
            break;
        }
        r.read_exact(&mut h[1..])?;
        buf.resize(u32::from_be_bytes(h) as usize, 0);
        r.read_exact(&mut buf)?;
        batches.extend(decoder.decode(&buf)?);
    }
    let cols = batches
        .iter()
        .map(SpanColumns::try_new)
        .collect::<Result<Vec<_>, _>>()?;
    println!("decode_s: {:.2}", t.elapsed().as_secs_f64());

    // Group as the engine does: duplicates dropped, spans of a trace together
    // ordered by start, traces ordered by (start, id), windows adjacent.
    let mut entries = Vec::new();
    for (b, c) in cols.iter().enumerate() {
        for row in 0..c.len() {
            let rec = c.record(row);
            entries.push(Entry {
                trace: rec.trace_id.0,
                span: rec.span_id.0,
                start_ns: rec.start_ns,
                batch: b as u32,
                row: row as u32,
            });
        }
    }
    entries.sort_unstable_by_key(|e| (e.trace, e.span, e.batch, e.row));
    entries.dedup_by(|later, kept| later.trace == kept.trace && later.span == kept.span);
    entries.sort_unstable_by_key(|e| (e.trace, e.start_ns, e.span, e.batch, e.row));
    let mut traces = Vec::new();
    let mut from = 0;
    while from < entries.len() {
        let mut to = from + 1;
        while to < entries.len() && entries[to].trace == entries[from].trace {
            to += 1;
        }
        traces.push((entries[from].start_ns, entries[from].trace, from, to));
        from = to;
    }
    traces.sort_unstable_by_key(|t| (t.0, t.1));
    let mut windows: Vec<(i64, Vec<TraceRange>)> = Vec::new();
    for t in traces {
        let w = window_start(t.0, DEFAULT_WINDOW_NS);
        match windows.last_mut() {
            Some((last, v)) if *last == w => v.push(t),
            _ => windows.push((w, vec![t])),
        }
    }
    let rows: usize = entries.len();
    println!("spans: {rows}, windows: {}", windows.len());

    // Write, several times.
    let mut write_s = Vec::new();
    let mut write_mb = Vec::new();
    for _ in 0..runs {
        let _ = std::fs::remove_dir_all(&parts_dir);
        let before = status_kb("VmRSS:");
        reset_peak();
        let t = Instant::now();
        for (id, (w, ts)) in windows.iter().enumerate() {
            let mut writer = PartWriter::create(
                &parts_dir,
                id as u64 + 1,
                *w,
                DEFAULT_WINDOW_NS,
                WriterOptions {
                    splitter: Some(Arc::new(OtlpSplitter::default())),
                    ..WriterOptions::default()
                },
            )?;
            let mut builder = SpanBatchBuilder::new(BatchLimits::DEFAULT);
            for tr in ts {
                for e in &entries[tr.2..tr.3] {
                    builder.append_from(&cols[e.batch as usize], e.batch as usize, e.row as usize);
                    if builder.is_full() {
                        writer.write(&builder.finish())?;
                    }
                }
            }
            if !builder.is_empty() {
                writer.write(&builder.finish())?;
            }
            writer.finish()?;
        }
        write_s.push(t.elapsed().as_secs_f64());
        write_mb.push(status_kb("VmHWM:").saturating_sub(before) as f64 / 1024.0);
    }
    summarize("write_s", write_s, "s");
    summarize("write_peak_mb", write_mb, "MB");
    drop(cols);
    drop(batches);
    drop(entries);

    let readers = (0..windows.len())
        .map(|i| PartReader::open(parts_dir.join(part_dir_name(i as u64 + 1))))
        .collect::<Result<Vec<_>, _>>()?;
    read_phase(&readers, rows, sample, runs, digest)
}

/// Times the reads over the parts and, with `digest`, hashes every row.
fn read_phase(
    readers: &[PartReader],
    rows: usize,
    sample: usize,
    runs: usize,
    digest: bool,
) -> R<()> {
    let mut ids = Vec::new();
    for (p, r) in readers.iter().enumerate() {
        for e in r.trace_entries() {
            ids.push((p, e.trace_id));
        }
    }
    let stride = (ids.len() / sample.max(1)).max(1);
    let picked: Vec<_> = ids.iter().step_by(stride).take(sample).collect();
    let mut open_us = Vec::new();
    let mut open_rows = 0usize;
    for _ in 0..runs {
        let mut times = Vec::new();
        open_rows = 0;
        for (p, id) in &picked {
            let t = Instant::now();
            let found = readers[*p].read_trace(*id, None)?.ok_or("trace missing")?;
            times.push(t.elapsed().as_secs_f64() * 1e6);
            open_rows += found.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        open_us.push(times[times.len() / 2]);
    }
    println!(
        "open_trace sampled {} traces, {open_rows} rows",
        picked.len()
    );
    summarize("open_trace_us", open_us, "us (median over traces)");

    let five: Vec<String> = [
        "start_ns",
        "request_model",
        "input_tokens",
        "output_tokens",
        "operation",
    ]
    .map(String::from)
    .to_vec();
    let mut scan_s = Vec::new();
    for _ in 0..runs {
        let t = Instant::now();
        let mut n = 0usize;
        for r in readers {
            for b in r.scan(ScanOptions {
                projection: Some(five.clone()),
                ..ScanOptions::default()
            })? {
                n += b?.num_rows();
            }
        }
        assert_eq!(n, rows);
        scan_s.push(t.elapsed().as_secs_f64());
    }
    summarize("scan5_s", scan_s, "s");

    if digest {
        let mut h = DefaultHasher::new();
        for r in readers {
            r.verify()?;
            for b in r.scan(ScanOptions::default())? {
                let b = b?;
                let c = SpanColumns::try_new(&b)?;
                for row in 0..c.len() {
                    format!("{:?}", c.record(row)).hash(&mut h);
                }
            }
        }
        println!("digest: {:016x}", h.finish());
    }
    Ok(())
}
