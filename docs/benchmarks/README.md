# Measured results

These are the results of the experiments that tested the design before the
engine was written. They are not benchmarks of a finished product.

## How they were run

- Docker Desktop on an Apple M4 Pro (linux/arm64 virtual machine).
- Each system ran alone in a container limited to 2 CPUs and 4 GB.
- Data came from `bench/tracegen` with seed 1: 8.02 GB of agent traces
  (91,419 spans, 5,014 traces, 93% repeated text, text sliced from public
  agent-trajectory datasets) and 10,000,039 small spans without text.
- The baseline is ClickHouse 26.10.12.13 loaded through the OpenTelemetry
  Collector 0.165.0 with the ClickHouse exporter's own schema, unchanged.
- Every comparison first checked that both sides returned the same answer.

## Limits

- Warm caches and a virtualised local disk. The cache-dropping step did not
  produce truly cold reads on this machine. Object storage will be slower.
- The NightsWatchDB side was experiment code timed in-process, one query at a
  time, with files and metadata already open. ClickHouse is a complete server
  parsing and planning SQL. Part of every sub-millisecond gap is that.
- ClickHouse ran the exporter's schema as shipped. A schema tuned for this
  workload would close much of the gap on trace-level filters and aggregates.
- One synthetic dataset. Its repetition matches what was measured on public
  agent traces; real workloads vary, and with content capture turned off there
  is no text at all.

## Results

| Experiment | Question | Result |
|---|---|---|
| [Language](../../experiments/bakeoff/results/RESULTS.md) | Would Go have performed like Rust on this data? | Write path a tie; selective reads 3.5 to 5 times faster in Rust, mostly from its Parquet library. |
| [A: layout and text store](../../experiments/expA/results/RESULTS.md) | Does the chosen layout hold up, and how should large text be stored? | All three pass marks met. Storing each distinct message once was the best of four stores: 8 GB became 62 MB. |
| [B: text index](../../experiments/expB/results/RESULTS.md) | What does our own index cost, and what should it index? | Index distinct messages: 24 MB, built in 2 CPU-seconds, identical answers to a brute-force scan on 90 queries. |
| [C: ingest](../../experiments/expC/results/RESULTS.md) | What does each ingest door cost? | OTLP door about 1 CPU-second per GB in 57 MB. Own OTel-Arrow receiver interoperates with the Collector and sends a fifth of the bytes, at much higher receiver cost. |

Each experiment directory holds its specification (`SPEC.md`), the code, the
scripts that produced the numbers, and the raw output.

## Reproducing

Requires Docker, Go and about 20 GB of free disk.

```sh
cd bench/corpusprep && go build && ./corpusprep fetch && ./corpusprep extract
cd ../../experiments/expA && ./official-d2.sh prep && ./official-d2.sh measure
```

The other experiments have an `official.sh` (or `run.sh`) of the same kind.
All generated data stays in Docker volumes.

## Benchmarks of the server

`scripts/bench.sh` measures the server image the same way after every
checkpoint, so runs can be set beside each other. See `scripts/README.md` for
usage.

**Method.** One run is one dataset on one resource profile. The server runs
alone in a container; the generator (`bench/tracegen`) and the query tool
(`bench/querybench`) run in their own containers on the same Docker network and
are not limited. The datasets use the definitions of experiment A: `d1` is
`--bytes 8GB --seed 1` (text-heavy) and `d2` is `--no-content --spans 10000000
--seed 1` (spans only). The load goes through the OTLP gRPC door with gzip. The
run then records storage, a clean restart (`docker stop`, then start), a crash
restart (`kill -9` right after a further small batch), and the latency of the
HTTP API, warm, on the restarted server.

**Profiles.** `small` is 2 CPUs and 4 GB for the server, as in the
experiments. `standard` is 8 CPUs and 12 GB.

**What the numbers mean.**

- *Ingest:* spans and bytes sent, wall time of the load (the generator makes the
  data while it sends, so this is a floor for the server, not its ceiling),
  spans and MB per second. Encoded bytes are the protobuf before compression;
  wire bytes are what the server's network interface received, headers
  included. Server CPU seconds and peak memory come from the container's cgroup
  (`cpu.stat`, `memory.peak`); if the files are missing the script falls back
  to `docker stats`, which has no CPU seconds and no peak, and says so.
- *Storage:* bytes on the data volume from `du`, split into `parts/` (the
  stored data), `wal/` (the log, which is kept after a flush) and `manifest/`,
  measured after a clean stop. Bytes per span are given for the whole volume
  and for `parts/` alone.
- *Restart:* milliseconds from the start of the server process to the first
  answer of `/healthz`. After a crash this includes replaying the log.
- *Queries:* open a trace by id (ids drawn with a fixed seed), list the newest
  50 traces, the same with a `service`, a `model` and an `error=true` filter,
  token totals over the whole range and over one hour. Count, errors, p50, p95,
  p99 and max in milliseconds (nearest rank) and bytes received. One request at
  a time unless a concurrency is set. The peak memory of the query phase is the
  peak since the last start of the server, so it includes the log replay.

**Caveats.**

- Docker on a laptop is for comparing one commit with another on the same
  machine, with nothing else running. Publishable numbers need real Linux
  hardware.
- The earlier experiment numbers above were experiment code timed in-process,
  with files already open. A server measured over HTTP pays for connections,
  request parsing and JSON, so the two are not directly comparable.
- The server has no call that returns a span count; the harness sums the
  counts of the token endpoint over the whole range instead.
- Warm caches only: the page cache of the Docker VM is not dropped.
