# Bake-off result: Go vs Rust on the database's own data

Run 2026-11-24 with `run.sh 8GB 2 4g`. Docker Desktop on an Apple M4 Pro
(linux/arm64 VM), each step alone in a container limited to 2 CPUs and 4 GB.
Input: 8.02 GB from `bench/tracegen` (seed 1, real corpus): 91,419 spans,
5,014 traces, 93% repeated content. Go 1.27.4 + arrow-go 18.9.0 (pure-Go
zstd); Rust 1.100.0 + arrow/parquet 60.2.0 (C zstd).

The language was already decided (Rust, because DataFusion is required). This
run answers the separate question "would Go have performed the same?".

## Same work check

Rows, token sums, payload byte totals, the sampled trace ids and the inverted
index file (SHA-256 `1497d79e...`) are identical between the two
implementations.

## Numbers

| Step | Go | Rust | Go / Rust |
|---|---|---|---|
| Decode OTLP to Arrow, MB/s | 6,215 | 6,866 | 0.91 |
| Decode peak memory, MB | 2,615 | 1,625 | 1.61 |
| Decode + write Parquet, MB/s | 2,132 | 2,242 | 0.95 |
| Decode + write, CPU seconds | 6.44 | 5.49 | 1.17 |
| Decode + write peak memory, MB | 3,783 | 3,909 | 0.97 |
| payloads.parquet, MB (from 8 GB) | 85.8 | 146.8 | 0.58 |
| spans.parquet, MB | 3.45 | 3.02 | 1.14 |
| Aggregate query, median ms | 7.6 | 2.2 | 3.5 |
| Open one trace, p50 / p99 ms | 18.1 / 29.2 | 3.6 / 5.6 | 5.1 / 5.2 |
| Open one trace, peak memory, MB | 442 | 51 | 8.6 |
| Build text index, MB/s | 196 | 221 | 0.88 |
| Build text index, CPU seconds | 0.93 | 1.22 | 0.76 |
| Mixed load at 4 GB | completed, 3.82 GB | killed for memory (exit 137) | |

Mixed load re-run at 2 CPUs and 8 GB so both complete (60 s, one ingest
worker, one query worker):

| | Go | Rust |
|---|---|---|
| Ingest, MB/s | 1,733 | 2,006 |
| Aggregate p50 / p99 ms | 6.1 / 33.1 | 2.5 / 43.3 |
| Open trace p50 / p99 / max ms | 17.0 / 66.4 / 230.6 | 3.8 / 46.1 / 56.8 |
| Peak memory, MB | 6,797 | 5,975 |
| Go GC | 704 collections, max pause 43.6 ms, total 416 ms | |

## Reading

- **Write path: a tie.** Decoding, writing Parquet and building the text index
  are within about 10% of each other. On this data the work is copying and
  compressing large text, and the language hardly matters.
- **Selective reads: Rust's library is well ahead.** 3.5x on the aggregate, 5x
  on opening a trace, with a ninth of the memory. Most of this is the Parquet
  library, not the language: the Rust crate reads only the selected rows and
  keeps metadata in memory, while arrow-go can only seek to a row and read
  forward, and re-reads its page index for every row group it touches.
- **Memory needs a byte budget in either language.** The spec batched 8,192
  rows at a time. Rows here average about 90 KB, so one batch is about 740 MB
  and both implementations sat near the 4 GB limit. Under mixed load Rust was
  killed and Go survived, because Go's soft memory limit made its collector
  work harder instead of growing. The fix is the same for both: size buffers
  by bytes, never by rows.
- **Library defaults moved file size more than language did.** The same data
  came out 1.7x smaller from arrow-go than from the Rust crate at "zstd 3",
  because page size and compression window defaults differ. This is a tuning
  knob to explore, not a language property.
- **The storage finding.** 8 GB of agent-trace text became 86 to 147 MB in
  plain Parquet with zstd, a 55 to 93 times reduction, because a trace's spans
  sit next to each other and most of each span repeats the one before it.

## Caveats

Docker on a Mac: ARM, virtualised disk, warm page cache. One input shape. Only
about 91,000 rows, so the aggregate and lookup timings say little about large
tables. Each side was written by a separate automated agent to one spec; the
Go side needed one correction (it re-read bloom filters on every lookup)
before this run.
