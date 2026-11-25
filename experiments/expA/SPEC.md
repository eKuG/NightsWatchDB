# Experiment A: does the chosen storage layout hold up?

This experiment decides two things before the real engine is built:

1. **Span layout.** Is "time-windowed segments, each trace's spans kept
   together, plus a trace index and a per-trace summary" fast for opening a
   trace without making list and aggregate queries slow, compared with the two
   usual layouts and with ClickHouse?
2. **Payload store.** Agent traces repeat themselves: every LLM call re-sends
   the system prompt, the tool definitions and the whole conversation so far
   (measured 92 to 97% of input bytes). How much does each of three ways of
   storing that text save, and what does each cost to read?

Everything here is throwaway experiment code. Keep it small and direct. The
numbers that count will be produced later by one neutral harness that runs each
piece alone under fixed container limits, so your job is correct, reasonably
efficient code with the exact command-line surface below, plus a small smoke
run. Do not optimise beyond what a careful engineer would do first.

There are three independent work streams, each in its own directory. Do not
edit another stream's directory.

    experiments/expA/layout/      Rust crate `layout`      (span layouts)
    experiments/expA/payload/     Rust crate `payload`     (payload stores)
    experiments/expA/chbaseline/  scripts + SQL            (ClickHouse baseline)

## Shared facts

- Input format: the output of `bench/tracegen` (see its README): records of a
  4-byte big-endian length followed by one OTLP `ExportTraceServiceRequest`.
  Spans of a trace are adjacent; trace start times are non-decreasing.
- Two datasets, generated with seed 1 and the real corpus at
  `.data/corpus` (in the repository root):
  - **D1 "content"**: default flags, about 8 GB, about 90,000 spans. Large text
    attributes on every chat span. Used for the payload stores and for
    opening traces with their text.
  - **D2 "narrow"**: `--no-content` (no message, system-instruction,
    tool-definition, tool-argument or tool-result attributes; everything else
    unchanged), 10,000,000 spans. Used for the span layouts, where row count
    matters. This flag is being added to tracegen now.
- Rust code may copy the OTLP decoding from
  `experiments/bakeoff/rust/src/convert.rs` (copy, do not depend on it). Rust
  1.100, `arrow`/`parquet` 60.2.0, `prost` 0.14, `opentelemetry-proto` 0.33
  with only the message types; release profile `lto = "thin"`,
  `codegen-units = 1`; no async runtime; no `unsafe` of your own.
- Each Rust crate has a multi-stage `Dockerfile` (`rust:1.100.0-bookworm` to
  build, `debian:bookworm-slim` to run, binary as the entry point). Add a
  dependency-caching layer so rebuilds are quick.
- Every subcommand prints exactly one JSON object on one line to stdout when
  done, with at least `task`, `variant`, `wall_s`, `user_cpu_s`, `sys_cpu_s`,
  `peak_rss_bytes` (VmHWM), `threads`, plus the fields listed for it.
  Diagnostics go to stderr. `--threads N` everywhere; default is the CPUs the
  runtime reports (must respect the container limit).
- Latency lists are reported as `p50_ms`, `p90_ms`, `p99_ms`, `max_ms` by
  nearest rank. "MB" means 1e6 bytes.
- **Data discipline, strictly.** Generated data and outputs live only in Docker
  named volumes prefixed with your stream name (`expA-layout-`,
  `expA-payload-`, `expA-ch-`). Generate inputs inside a container straight
  into a volume (Dockerfile for the generator:
  `experiments/bakeoff/tracegen.Dockerfile`, build context `bench/tracegen`; mount
  the corpus read-only). Never write generated data to the host. Never
  bind-mount a host directory that contains large generated files. For your
  own smoke runs use at most 1 GB of D1 and 500,000 spans of D2. Remove your
  volumes, containers and images when finished. Do not prune or remove any
  other Docker resource on this machine.
- No `git init`, commits or pushes. Plain code comments (no tool or mode
  names). Do not edit files outside your stream's directory.

## Stream 1: span layouts (`experiments/expA/layout/`)

One span table, three physical layouts. Columns: the `spans` table from
`experiments/bakeoff/SPEC.md` (same names and types).

`layout build --in FILE --out DIR --variant V [--window-s 900]`

- `cotrace`: rows stay in arrival order. Start a new segment file
  (`seg-00000.parquet`, ...) whenever a trace's start time crosses into the
  next `--window-s` window (a trace is never split). Row groups of at most
  16,384 rows, page index on, zstd 3, no bloom filter. Also write:
  - `traces.idx`: one fixed-size record per trace, sorted by trace id:
    trace_id (16 bytes), segment (u32), first_row (u32, row within the
    segment), row_count (u32), start_ns (i64), end_ns (i64). Little-endian.
  - `summary.parquet`: one row per trace in start order: trace_id, start_ns,
    end_ns, service_name and name of the root span, span_count, error (true if
    any span has an error_type), input_tokens and output_tokens (sums),
    models (list of distinct request_model values), session_id, user_id.
    Row groups of 65,536 rows, page index on, zstd 3.
- `svc`: all rows sorted by (service_name, name, start_ns), one file
  `spans.parquet`, row groups of 65,536 rows, bloom filter on trace_id, page
  index on, zstd 3. This mirrors the ClickHouse OpenTelemetry exporter's
  order. An in-memory sort is acceptable; say what it needs.
- `traceid`: all rows sorted by (trace_id, start_ns), one file, row groups of
  65,536 rows, page index on, zstd 3, no bloom filter.

Build fields: `rows`, `traces`, `segments`, `bytes_on_disk` (object: per file
kind), `input_mb_per_s`.

Queries. Each takes `--dir DIR --variant V` and opens files and metadata once
before timing. Parameters must be derived deterministically from the data and
printed in the output so the ClickHouse stream can use the same ones: `S` is
the most frequent root service_name, `M` the most frequent request_model, and
the window is the middle half of the start-time range, exactly as `agg` in the
bake-off spec defines it. Put the derivation in a `params` subcommand that
prints `{"service":..,"model":..,"from_ns":..,"to_ns":..}`.

- `layout sample-ids --dir DIR --variant V --n 500 --out FILE`: same rule as
  the bake-off `sample-ids`, applied to traces in start order.
- `layout open --ids FILE`: for each id return all rows of that trace, ordered
  by start_ns. `cotrace` uses `traces.idx` (binary search) and a row-range
  read; `svc` uses bloom filter and statistics; `traceid` uses statistics and
  the page index. Fields: `lookups`, `rows_returned`, latency list.
- `layout list1 --runs 20`: the 50 most recent traces whose root span has
  service_name = S and start_ns in the window, newest first; return trace_id,
  start_ns, duration, root name. `cotrace` reads `summary.parquet`; the other
  two find root spans (parent_span_id is null) in the span table. Fields:
  `result_sha256` (hash of the 50 trace ids in order, hex, lowercase,
  concatenated), `runs_ms`.
- `layout list2 --runs 20`: the 50 most recent traces in the window that
  contain at least one span with a non-null error_type and at least one span
  with request_model = M; newest first. `cotrace` answers from
  `summary.parquet` alone; the other two must work from the span table. Fields
  as list1.
- `layout agg --runs 20`: the bake-off `agg` query (chat spans in the window,
  grouped by request_model: count and token sums). Fields: `result`,
  `runs_ms`.

All three variants must return identical results for every query. Unit tests:
`traces.idx` round trip and binary search; segment splitting never splits a
trace; list1/list2/agg agree across the three variants on a small synthetic
set.

## Stream 2: payload stores (`experiments/expA/payload/`)

The six payload attributes of every span (see the `payloads` table in the
bake-off spec), stored three ways. Rows are in arrival order in all three.

`payload build --in FILE --out DIR --variant V`

- `parquet`: `payloads.parquet` exactly as in the bake-off spec (row groups of
  2,048 rows, zstd 3), with the page index on.
- `zstdlong`: one data file. For each trace, concatenate the payload values of
  its spans (each value preceded by a presence byte and a u32 length) and
  compress that as one zstd frame at level 3 with long-distance matching
  enabled and window log 27. An index gives each trace's frame offset and
  length, and each span's offset inside the uncompressed frame.
- `dedup`: store each distinct piece of text once.
  - Pieces: the whole value for system_instructions, tool_definitions,
    tool_call_arguments and tool_call_result; for input_messages and
    output_messages, each top-level element of the JSON array, as the raw
    bytes of that element exactly as they appear in the value.
  - A value must be reconstructible byte for byte from its pieces. Check this
    for every value at build time (the array is `[` + pieces joined by `,` +
    `]`). If a value is not a JSON array, or does not reconstruct exactly,
    store it whole as a single piece and count it in `whole_fallbacks`.
  - Identify pieces by a 128-bit hash (xxh3-128 is fine). On a hash match,
    compare the bytes before treating it as the same piece; count mismatches
    in `hash_collisions` (expected 0).
  - Dedup scope is the segment: start a new scope whenever a trace's start
    time crosses into the next `--window-s` window (default 900), as in
    stream 1. Within a scope, write pieces in first-seen order into blocks of
    about 1 MiB of uncompressed bytes, each block compressed with zstd 3 (a
    piece larger than the block size gets its own block). Keep a piece table
    (piece id to block, offset, length) and, per span and per attribute, the
    ordered list of piece ids. Choose simple file formats and describe them
    in the README.
  - In the same pass, also measure what other scopes would give, without
    building them: unique piece bytes divided by total bytes if the scope were
    one trace, one segment, and the whole dataset. Report as `scope_ratio`.

Build fields: `rows`, `traces`, `raw_payload_bytes` (object per attribute),
`bytes_on_disk` (object per file kind, and total), `input_mb_per_s`,
`whole_fallbacks`, `hash_collisions`, `scope_ratio` (dedup only), `pieces`,
`unique_piece_bytes`.

At build time every variant also writes `check.bin`: for each trace in arrival
order, the trace id and the SHA-256 of all its payload values concatenated in
row order and attribute order (absent values contribute nothing), plus the
total byte count.

- `payload sample-ids --dir DIR --n 500 --out FILE` (same rule as stream 1).
- `payload open --dir DIR --variant V --ids FILE`: for each trace, read back
  every payload value of every span and verify the SHA-256 and byte count
  against `check.bin`. Fields: `lookups`, `bytes_returned`, `mismatches`
  (must be 0), latency list.
- `payload span --dir DIR --variant V --n 500 --seed 1`: fetch the six payload
  values of one span, for n spans chosen with a seeded generator from all
  rows. Fields: `bytes_returned`, latency list.
- `payload scan --dir DIR --variant V`: read every value once (as a full
  export or re-index would). Fields: `bytes_returned`, `mb_per_s`.

Unit tests: array splitting reconstructs byte for byte on tricky inputs
(nested arrays and objects, strings containing `,` `[` `]` and escaped quotes,
whitespace between elements, empty array, non-array value); every variant
round-trips a small synthetic set through `open` with zero mismatches.

## Stream 3: ClickHouse baseline (`experiments/expA/chbaseline/`)

The comparison point is "ClickHouse with the OpenTelemetry Collector's
ClickHouse exporter schema", loaded through the real pipeline so nothing about
the schema is our invention:

    tracegen --otlp-endpoint  ->  otelcol-contrib (otlp receiver, clickhouse exporter)  ->  ClickHouse

- Pin exact image versions (current stable ClickHouse server; current
  otelcol-contrib). Let the exporter create its own schema; record the DDL it
  created (`SHOW CREATE TABLE` for every table and materialized view) in the
  README. Do not tune the schema.
- `load.sh DATASET` starts ClickHouse and the collector on a private Docker
  network, runs tracegen against the collector, waits until the collector's
  queue is empty, and checks that `count()` in the traces table equals the
  number of spans tracegen reports. A mismatch is a failure, not a warning;
  report dropped or refused spans and why (for example message-size limits).
  Record the CPU seconds each container used during the load (from its
  cgroup `cpu.stat`), the wall time, and input MB/s.
- `sizes.sh` prints, as JSON: bytes on disk, compressed and uncompressed bytes
  per table (from `system.parts`, active parts only, after `OPTIMIZE ... FINAL`
  has finished or merges have settled; say which), and per column for the
  traces table (from `system.parts_columns`).
- `query.sh` runs the equivalents of the stream 1 and stream 2 queries and
  prints one JSON line per query with a latency list over the runs:
  - open a trace: all spans of a trace id with all their attributes, using the
    lookup pattern the exporter's own time-range helper table is meant for;
  - list1, list2 and agg with the same `S`, `M` and window (parameters are
    passed in as arguments);
  - the 500 sampled trace ids are passed in as a file.
  Write the SQL to a `queries/` directory, one file per query, and explain in
  the README any choice that could be argued either way. Measure server-side
  elapsed time and client-side wall time; say which is reported.
- `cold.sh` restarts the ClickHouse container and drops the Linux page cache
  in the Docker VM, so the harness can take cold measurements.
- ClickHouse runs with the container limits passed in (`--cpus`, `--memory`);
  the collector and tracegen run without limits. Leave ClickHouse server
  settings at their defaults apart from what a limited container requires;
  list anything you changed.

Smoke run: load 1 GB of D1 and 500,000 spans of D2, run every query once, and
include the JSON in your report.

## Report back (all streams)

Files with line counts; exact versions; the commands you ran and their real
output; the file formats you chose; anything in this document you could not
follow and what you did instead; anything you could not verify. State
shortfalls plainly.
