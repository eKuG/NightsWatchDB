# Experiment C: what does each ingest door cost?

The database will accept telemetry through two doors from its first version:
plain OTLP over gRPC, and the OpenTelemetry Arrow protocol (OTAP, "OTel-Arrow").
This experiment measures the front half of the write path for each door:
receive from the network, make the data durable in a write-ahead log, and turn
it into the engine's Arrow batches. The back half (writing segments, the
payload store and the text index) was measured in experiments A and B.

Questions:

1. CPU seconds and peak memory per GB and per million spans, for each door, on
   agent traces with large repeated text, on small spans without text, and on
   classic logs and metrics.
2. Does OTel-Arrow help on traces at all? On the wire (bytes) and at the
   receiver (CPU)? Published figures exist only for small logs; nothing is
   published for large text.
3. How much does skipping full protobuf decoding save on the OTLP door?
4. Can we write our own OTel-Arrow receiver that interoperates with the real
   OpenTelemetry Collector exporter? (The plan is to own it rather than depend
   on the upstream Rust engine, which is still marked incubating.)

Throwaway experiment code: small, direct, correct. The official measurements
are taken later by one neutral harness that runs each piece alone under fixed
container limits; you deliver correct code with exactly this surface plus a
smoke run.

Two work streams, each its own crate and directory; do not edit the other's.

    experiments/expC/otlpdoor/    Rust crate `otlpdoor`   (OTLP gRPC receiver)
    experiments/expC/otapdoor/    Rust crate `otapdoor`   (OTel-Arrow receiver + collector set-up)

## Shared facts

- Rust 1.100. `tokio` and `tonic` are allowed here (a gRPC server needs
  them), with `prost`, `arrow` 60.2.0 (and `arrow-ipc` for the OTAP stream),
  `opentelemetry-proto` 0.33 message types, `zstd` 0.14.5, `sha2`. Release
  profile `lto = "thin"`, `codegen-units = 1`. No `unsafe` of your own. Check
  current crate versions and pin exactly; check the tonic and arrow-ipc APIs
  against the versions you pin instead of relying on memory. Multi-stage
  `Dockerfile` with a dependency-caching layer (`rust:1.100.0-bookworm` then
  `debian:bookworm-slim`), binary as the entry point.
- **Target schema for traces** is the pair of tables in
  `experiments/bakeoff/SPEC.md` (`spans` and `payloads`). A working OTLP-to-Arrow
  converter for it is in `experiments/bakeoff/rust/src/convert.rs` (copy from it;
  do not depend on it or edit it).
- **Batches are bounded by bytes, never by rows.** Close an Arrow batch when
  it holds 8,192 rows or 32 MiB of value bytes, whichever comes first. Rows in
  this data average 90 KB and reach 1 MiB per value.
- **Write-ahead log.** With `--wal DIR` the receiver appends every accepted
  request to a log file before acknowledging it: a 4-byte length, a 4-byte
  CRC32C, then the request bytes as received (the serialized OTLP request, or
  the serialized OTAP message). Group commit: `fsync` at most every
  `--wal-sync-ms` (default 10) and acknowledge the requests of a group only
  after its fsync returns. Roll to a new file at 256 MiB. `--wal none` skips
  the log (to show its cost). Nothing reads the log back in this experiment.
- **The receiver process.** `DOOR serve --listen 0.0.0.0:PORT [--wal DIR|none]
  [--decode MODE] [--stats-file PATH]`. It serves until SIGTERM, or until
  `--exit-after-idle-s N` seconds pass with no request after at least one
  request arrived. On exit it writes one JSON object to `--stats-file` and to
  stdout with: `door`, `decode`, `wal`, `requests`, `wire_bytes` (bytes of
  request messages as received from gRPC, after gRPC decompression),
  `spans` / `log_records` / `metric_points`, `batches`, `wall_s` (first request
  to last), `user_cpu_s`, `sys_cpu_s`, `peak_rss_bytes`, `wal_bytes`,
  `wal_fsyncs`, and for traces the check block below.
- **Check block (traces).** Computed from the Arrow batches the receiver
  produced, so it proves the conversion: `rows`, `sum_input_tokens`,
  `sum_output_tokens`, `nonnull` (count per payload column) and
  `payload_bytes` (total bytes per payload column). These must equal what the
  generator reports for the same run (`bench/tracegen` prints the span count
  and bytes by attribute class; `bake convert` in `experiments/bakeoff/rust` prints
  exactly these fields for a generator file). A mismatch is a failure.
- After conversion the Arrow batches are dropped. This experiment stops there.
- gRPC limits: accept messages up to 64 MiB. Report anything a sender had to
  change to get data through.
- Senders: `bench/tracegen` sends OTLP gRPC with `--otlp-endpoint` (see its
  README; `--no-content --spans N` gives small spans). The OpenTelemetry
  Collector contrib image `otel/opentelemetry-collector-contrib:0.165.0` is
  already on this machine. Blitz (`github.com/observIQ/blitz`, v0.22.0, Go)
  generates logs and host metrics over OTLP gRPC.
- **Data discipline, strictly.** Everything generated lives in Docker named
  volumes prefixed with your stream name (`expC-otlp-`, `expC-otap-`) or is
  streamed over the network and dropped. Never write generated data to the
  host; never bind-mount a host directory that holds large generated files
  (small config files are fine). Smoke runs: at most 1 GB of content data and
  500,000 small spans. Containers run with `--cpus 2 --memory 4g` for the
  receiver; senders are not limited. Use a private Docker network. Remove
  your volumes, containers, networks and images when finished; touch no other
  Docker resource.
- No `git init`, commits or pushes. Plain code comments. Do not edit files
  outside your stream's directory.

## Stream 1: the OTLP door (`experiments/expC/otlpdoor/`)

Implements the three OTLP gRPC services (`TraceService`, `LogsService`,
`MetricsService` `Export`).

- `--decode full`: decode each request with prost into the generated message
  types, then build the Arrow batches (as the bake-off converter does).
- `--decode lazy`: do not materialise message objects. Walk the protobuf wire
  format of the request directly (field numbers and lengths) and append
  values straight into the Arrow builders, copying each large string exactly
  once, from the request buffer into the builder. Write this small walker
  yourself for the fields the target schema needs; skip everything else by
  length. Both modes must produce identical check blocks.
- `--decode none`: receive, log to the WAL, acknowledge, and do no decoding
  (isolates the network and log cost).
- Logs and metrics: flat Arrow schemas are enough here. Logs: time_ns,
  severity_number, severity_text, body (string form), service_name,
  attr_count. Metrics: one row per data point with metric name, time_ns,
  kind (gauge, sum, histogram, exponential histogram, summary), numeric value
  where there is one, service_name, attr_count. Count `log_records` and
  `metric_points`. `full` and `lazy` are both required for traces; for logs
  and metrics `full` alone is enough.
- gRPC request compression: accept `gzip` and `zstd` if tonic supports them at
  the pinned version, and say which you enabled.

Unit tests: the lazy walker and prost decoding give identical Arrow batches
for a request containing every field the schema uses, absent optional fields,
an unknown field, and a 1 MiB attribute; WAL framing and CRC; batch closing on
the byte bound.

Smoke run: content data (1 GB) and small spans (500,000) from tracegen straight
into the receiver, for each of `full`, `lazy`, `none`, with and without the
WAL; show the check block equals the generator's numbers.

## Stream 2: the OTel-Arrow door (`experiments/expC/otapdoor/`)

Our own receiver for the OTel-Arrow protocol, written from the protocol
definition and the reference implementations, not by depending on the
upstream Rust engine.

- Sources to read first, in the `open-telemetry/otel-arrow` repository (use
  `gh api` with `-H "Accept: application/vnd.github.raw"`, or a shallow clone
  into a fresh scratch directory): `proto/opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`,
  `docs/otap-spec.md`, `docs/data_model.md`, and the Go consumer under
  `go/pkg/otel/arrow_record` plus the collector's
  `receiver/otelarrowreceiver` in opentelemetry-collector-contrib at v0.165.0.
  An earlier study of these found: three bidirectional-streaming services
  (traces, logs, metrics); one Arrow IPC stream per payload type and schema id
  inside each gRPC stream, so dictionaries must be kept per stream; a new
  schema id means a reset; the exporter recycles streams (about every 30 s by
  default); a status reply per batch; the written spec disagrees with the
  implementations in places (attribute type codes are 1 str, 2 int, 3 double,
  4 bool, 5 map, 6 slice, 7 bytes; implementations key IPC streams by schema
  id; the Go producer dictionary-encodes columns the spec does not list, so
  accept a dictionary on any column); traces arrive as up to 8 related tables
  joined by parent ids that are delta-encoded and only unique within a batch;
  the exporter may apply its own gRPC-level compression codec name in
  addition to Arrow IPC compression. Verify each of these against the source
  rather than trusting this list, and follow the implementations where the
  spec differs.
- Sender: the collector contrib image with an `otlp` receiver and the
  `otelarrow` exporter pointed at your receiver. Read the exporter's README at
  v0.165.0 for its settings (number of streams, max stream lifetime, payload
  compression, gRPC compression, how to disable the fallback to plain OTLP so
  a failure is visible instead of silently downgraded). Deliver the collector
  configuration and a `run-collector.sh`.
- Traces: convert the OTAP tables to the target `spans` and `payloads` schema
  and produce the same check block. This is the proof that the receiver is
  correct end to end: generator -> collector (OTLP in, OTel-Arrow out) ->
  receiver must give exactly the generator's numbers.
- Logs and metrics: receive, log to the WAL, decode the Arrow IPC messages to
  record batches, count `log_records` and `metric_points`; no conversion.
- Also report from the receiver: `arrow_ipc_bytes` (bytes of Arrow payloads
  before IPC decompression), `streams_opened`, `schema_resets`, and per
  payload type the number of batches and rows. `--decode none` receives, logs
  and acknowledges without decoding the Arrow payloads.
- Deliver a second collector configuration that forwards plain OTLP to the
  OTLP door of stream 1 (`otlp` receiver, `otlp` exporter, same batching), so
  the harness can compare the two doors behind an identical collector, and
  record how to read each exporter's sent-bytes and the collector's own CPU.

Unit tests: parent-id decoding (delta and plain) and the joins from attribute
tables back to spans on a hand-built batch set; a stream that changes schema
id mid-way; dictionary deltas across messages; the attribute type codes.

Smoke run: content data (1 GB) and small spans (500,000) through the collector
into your receiver, with the fallback disabled; show the check block equals
the generator's numbers, the wire bytes for OTel-Arrow, and how many streams
and schema resets occurred. State plainly any data that did not arrive or did
not match and why. Then one logs run and one metrics run with Blitz (build it
in a container from the Go module at v0.22.0; a few hundred thousand records)
through the same collector.

## Report back (both streams)

Files with line counts; exact versions; commands run and their real output;
what you learned about the protocol that the summary above got wrong or
missed; anything in this document you could not follow and what you did
instead; anything you could not verify. State shortfalls plainly.
