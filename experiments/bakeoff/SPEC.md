# Language bake-off: one small pipeline, written twice

Purpose: decide between Go and Rust for the database by measuring the same
work in both, on the database's own kind of data, inside a Linux container
with CPU and memory limits. This code is an experiment and will be thrown
away. Keep it small and direct. It must be a fair picture of what a careful
engineer would ship in each language with mainstream libraries, not a contest
of exotic tricks.

Two implementations live side by side:

    experiments/bakeoff/go/      Go module `bake`
    experiments/bakeoff/rust/    Cargo package `bake`

Each builds one binary named `bake` with the subcommands below. Both must
follow this document exactly, because their outputs are compared with each
other. If something here is ambiguous or impossible with your libraries, do
the closest reasonable thing and say so in your report. Do not silently differ.

## Input

A file produced by `bench/tracegen`: a sequence of records, each a 4-byte
big-endian length followed by one serialized OTLP `ExportTraceServiceRequest`
protobuf message. Spans of one trace are adjacent. Trace start times are
non-decreasing. Files are several GB, so every subcommand must stream and keep
memory bounded. Never load a whole input or a whole Parquet file into memory.

## Tables

Every span becomes one row in each of two tables. Both tables have the same
number of rows in the same order (input order; do not sort).

`spans` (written to `spans.parquet`):

| column | Arrow type | source |
|---|---|---|
| trace_id | FixedSizeBinary(16) | span.trace_id |
| span_id | FixedSizeBinary(8) | span.span_id |
| parent_span_id | FixedSizeBinary(8), null when empty | span.parent_span_id |
| name | Utf8 | span.name |
| kind | Int32 | span.kind |
| start_ns | Int64 | span.start_time_unix_nano |
| end_ns | Int64 | span.end_time_unix_nano |
| status_code | Int32 | span.status.code (0 when absent) |
| service_name | Utf8, nullable | resource attribute `service.name` |
| operation | Utf8, nullable | `gen_ai.operation.name` |
| provider | Utf8, nullable | `gen_ai.provider.name` |
| request_model | Utf8, nullable | `gen_ai.request.model` |
| input_tokens | Int64, nullable | `gen_ai.usage.input_tokens` |
| output_tokens | Int64, nullable | `gen_ai.usage.output_tokens` |
| conversation_id | Utf8, nullable | `gen_ai.conversation.id` |
| session_id | Utf8, nullable | `session.id` |
| user_id | Utf8, nullable | `user.id` |
| agent_name | Utf8, nullable | `gen_ai.agent.name` |
| tool_name | Utf8, nullable | `gen_ai.tool.name` |
| error_type | Utf8, nullable | `error.type` |
| other_attr_count | Int32 | number of span attributes not listed in either table |

`payloads` (written to `payloads.parquet`):

| column | Arrow type | source attribute |
|---|---|---|
| trace_id | FixedSizeBinary(16) | |
| span_id | FixedSizeBinary(8) | |
| input_messages | LargeBinary, nullable | `gen_ai.input.messages` |
| output_messages | LargeBinary, nullable | `gen_ai.output.messages` |
| system_instructions | LargeBinary, nullable | `gen_ai.system_instructions` |
| tool_definitions | LargeBinary, nullable | `gen_ai.tool.definitions` |
| tool_call_arguments | LargeBinary, nullable | `gen_ai.tool.call.arguments` |
| tool_call_result | LargeBinary, nullable | `gen_ai.tool.call.result` |

Attribute values are string values in the input. For the payload columns store
the raw bytes of the string value. An attribute that is absent is null.

Parquet settings, identical in both languages:
- compression zstd level 3 on every column;
- `spans.parquet`: row groups of at most 65,536 rows; bloom filter on
  `trace_id` if the library supports writing one (report if not);
- `payloads.parquet`: row groups of at most 2,048 rows;
- column statistics on; everything else at the library default.
- Arrow batches of 8,192 rows between decoder and writer.

## Subcommands

Every subcommand prints exactly one JSON object on one line to stdout when it
finishes (diagnostics go to stderr) with at least:
`lang` ("go" or "rust"), `task`, `wall_s`, `user_cpu_s`, `sys_cpu_s`,
`peak_rss_bytes` (VmHWM from /proc/self/status), `cgroup_peak_bytes`
(/sys/fs/cgroup/memory.peak if readable, else null), `threads`, plus the
task-specific fields listed below.

`--threads N` is accepted everywhere; default is the number of CPUs available
to the process as the language runtime reports it (both must respect the
container's CPU limit). How you parallelise is your choice (for example one
reader, N decode workers, one ordered writer), but row order must be preserved.

1. `bake convert --in FILE --out DIR [--stage decode|all]`
   Decode the input, build Arrow batches, and with `--stage all` (default)
   write both Parquet files into DIR. With `--stage decode` build the Arrow
   batches and drop them without writing, to isolate decode cost.
   Fields: `input_bytes`, `input_mb_per_s`, `rows`, `sum_input_tokens`,
   `sum_output_tokens`, `nonnull` (object: count of non-null values per payload
   column), `payload_bytes` (object: total raw bytes per payload column),
   `spans_parquet_bytes`, `payloads_parquet_bytes`, `bloom_filter` (true/false).

2. `bake agg --dir DIR [--runs 5]`
   From `spans.parquet` only. First find the minimum and maximum `start_ns`
   (from column statistics if available, else by scanning). Let the window be
   the middle half of that range: [min + (max-min)/4, min + 3*(max-min)/4).
   Compute, for rows with `operation = 'chat'` and `start_ns` in the window,
   grouped by `request_model`: row count, sum(input_tokens), sum(output_tokens).
   Use row-group pruning by statistics where the library offers it. Run the
   query `--runs` times in the same process.
   Fields: `window` ([from, to]), `result` (array of
   {model, count, input_tokens, output_tokens} sorted by model), `runs_ms`
   (array of per-run milliseconds).

3. `bake sample-ids --dir DIR --n 200 --out FILE`
   Walk `spans.parquet` in row order and collect distinct trace ids in order of
   first appearance. Let k = max(1, distinct / n). Write, as lowercase hex one
   per line, the ids at positions 0, k, 2k, ... until n ids are written.
   Fields: `distinct_traces`, `written`.

4. `bake trace --dir DIR --ids FILE`
   For each id in FILE: return every row of that trace from `spans.parquet`,
   and the total stored bytes of all payload columns for that trace from
   `payloads.parquet`. Avoid reading row groups that cannot contain the id:
   use the bloom filter and/or statistics the library exposes, and say which.
   Open each Parquet file once, then time each lookup separately.
   Fields: `lookups`, `rows_returned` (total), `payload_bytes_returned`
   (total), `p50_ms`, `p99_ms`, `max_ms`, `pruning` (short string).

5. `bake index --dir DIR --out FILE`
   Build an inverted index over the text of two columns of
   `payloads.parquet`: `output_messages` and `tool_call_result`. The document
   id is the row index (0-based) in the table. A row's text is the bytes of
   `output_messages` followed by the bytes of `tool_call_result` (nulls are
   empty). Tokenisation, exactly: map ASCII A-Z to a-z; a token is a maximal
   run of bytes that are a-z, 0-9, or >= 0x80; every other byte separates
   tokens; discard tokens longer than 64 bytes. The index maps each distinct
   token to the ascending list of distinct document ids containing it.
   Output file format, exactly: tokens in ascending byte order; for each token:
   unsigned LEB128 varint of token length, the token bytes, varint of the
   number of document ids, then the ids delta-encoded as varints (first id as
   is, each following id as the difference from the previous one).
   Memory must stay bounded by the index, not by the text: read row groups one
   at a time.
   Fields: `text_bytes`, `text_mb_per_s`, `docs`, `distinct_tokens`,
   `postings`, `index_bytes`, `index_sha256`.

6. `bake mixed --in FILE --dir DIR --ids FILE --seconds 60`
   Tail latency under load. One ingest worker repeatedly runs the `convert`
   pipeline over the input into a scratch directory (deleting its output after
   each pass) using `threads - 1` workers (at least 1). At the same time one
   query worker alternates forever between the `agg` query and one `trace`
   lookup (cycling through the ids) against DIR, which was converted earlier
   and does not change. Stop after `--seconds`.
   Fields: `ingest_passes`, `ingest_mb_per_s`, `agg_p50_ms`, `agg_p99_ms`,
   `agg_max_ms`, `trace_p50_ms`, `trace_p99_ms`, `trace_max_ms`, and for Go
   only `gc_count`, `gc_pause_total_ms`, `gc_pause_max_ms`.

## Rules for fairness

- Mainstream libraries only. Go: `github.com/apache/arrow-go` (arrow, parquet,
  pqarrow) and either `go.opentelemetry.io/collector/pdata` or the generated
  OTLP protobuf types for decoding; the zstd that arrow-go uses. Rust: the
  `arrow` and `parquet` crates with zstd, `prost` with OTLP messages
  (`opentelemetry-proto` or generated code), `sha2`, and optionally `rayon` or
  `crossbeam-channel`. No async runtime is needed. No cgo in Go. No `unsafe`
  in Rust beyond what the libraries do internally.
- Write it the way you would for production: avoid needless copies and
  per-row allocations, reuse buffers, but no hand-written assembly, no custom
  protobuf parser, no SIMD intrinsics. If you see a simple change that would
  make a large difference and that a production codebase would obviously make,
  make it and mention it.
- Go: at start-up read `/sys/fs/cgroup/memory.max` and, if it is a number, set
  the runtime soft memory limit to 80% of it. Leave GOGC at the default.
- Rust: release build with `lto = "thin"` and `codegen-units = 1`. Use the
  system allocator.
- No target-specific CPU flags in either language.

## Container

Each implementation has a `Dockerfile` (multi-stage: official language image
to build, `debian:bookworm-slim` to run, binary at `/usr/local/bin/bake`,
`ENTRYPOINT ["bake"]`). Use the current stable official image for the
language and pin its exact tag. Images must build for linux/arm64.

## Checks you must run before reporting

- Unit tests for the tokeniser and the varint/index format using the worked
  example below, and for the trace-id sampling rule.
- An end-to-end run inside your container on a small input (generate about
  300 MB with `bench/tracegen`; build it from source, see its README) with
  `--cpus 2 --memory 4g`: run every subcommand and include the JSON lines in
  your report. Keep generated data in a Docker named volume or under the
  scratchpad directory you are given, never inside the source tree, and
  remove it when done.

Worked example for the index. Three documents:

    doc 0: "Error: file_not_found (code 404)"
    doc 1: "error error OK"
    doc 2: "404"

Tokens: doc 0 -> error, file, not, found, code, 404; doc 1 -> error, ok;
doc 2 -> 404. Index entries in order: `404` [0,2], `code` [0], `error` [0,1],
`file` [0], `found` [0], `not` [0], `ok` [1]. Bytes (hex):

    03 34 30 34 02 00 02
    04 63 6f 64 65 01 00
    05 65 72 72 6f 72 02 00 01
    04 66 69 6c 65 01 00
    05 66 6f 75 6e 64 01 00
    03 6e 6f 74 01 00
    02 6f 6b 01 01

## Report back

Files with line counts; exact library and toolchain versions; the commands you
ran and their real output; anything in this document you could not follow and
what you did instead; anything you could not verify. State shortfalls plainly.
Do not run `git init`, commit, or push. Do not edit files outside your own
directory (`experiments/bakeoff/go` or `experiments/bakeoff/rust`). No tool or mode
names in code comments.
