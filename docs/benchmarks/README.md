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
- The NightwatchDB side was experiment code timed in-process, one query at a
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
