# NightsWatchDB

A self-hosted database for OpenTelemetry data, built first for LLM and agent
traces. One binary, written in Rust, storing data on a local disk or an object
store.

> **Status: early development.** The design has been tested with throwaway
> experiments and the engine is being built in checkpoints. Today it runs as a
> single process that accepts traces over OTLP gRPC and reads them back over
> HTTP. It is not ready for production: there is no authentication, no
> compaction, and the large-text store that the design rests on is not built
> yet. See [docs/CHECKPOINTS.md](docs/CHECKPOINTS.md) for what works today.

## Why another database

Agent traces do not look like the traces existing stores were designed for.
Every model call re-sends the system prompt, the tool definitions and the whole
conversation so far. On public agent traces, 92 to 97% of the text in a call is
an exact repeat of an earlier call in the same trace. A span is tens of
kilobytes, a trace is megabytes, and almost all of it is repetition.

NightsWatchDB is built around that shape:

- **A trace's spans are stored next to each other**, in time-windowed immutable
  segments, with a small trace index and a one-row-per-trace summary. Opening a
  trace is one lookup and one contiguous read.
- **Each distinct message is stored once.** Large text is kept out of the span
  rows and split into pieces; spans hold references. Listing and aggregating
  never touch the text.
- **Its own text index, over distinct messages** instead of over spans, so the
  index is small and a phrase is checked once however many spans repeat it.
- **OTLP and OTel-Arrow at the door**, with its own receivers.
- **One process and nothing beside it.** No coordinator, queue or second
  database to operate.

## What the experiments measured

Before any engine code, four experiments tested the design on 8 GB of
agent-trace data and on 10 million small spans. Each system ran alone in a
container with 2 CPUs and 4 GB. ClickHouse was loaded through the
OpenTelemetry Collector with the exporter's own schema, unchanged, as the
baseline.

| | NightsWatchDB design | ClickHouse baseline |
|---|---|---|
| 8 GB of agent-trace text, stored | 62 MB text + 4 MB span tables | 496 MB |
| ... with a text index | + 24 MB | + 119 MB index and a second copy of the text |
| Open a trace (10M spans), median | 0.6 ms | 11.5 ms |
| List newest 50 traces for a service | 7.0 ms | 4.9 ms |
| List with a trace-level filter | 8.8 ms | 773 ms |
| Aggregate tokens by model | 86 ms | 272 ms |
| Word search, median | 0.004 to 0.5 ms | 1.2 to 3.0 ms with its text index |

Every query returned identical results on both sides.

**Read these numbers with care.** They come from Docker on a laptop with warm
caches. The NightsWatchDB side was experiment code timed in-process with files
already open; ClickHouse is a complete server parsing SQL. ClickHouse ran the
exporter's schema as shipped, and a schema tuned for this workload would close
much of the list and aggregate gap. The numbers were good enough to commit to
the design, and they are not a product benchmark. Full results, method and
caveats are in [docs/benchmarks](docs/benchmarks/README.md).

## Repository layout

| Path | What it is |
|---|---|
| `crates/` | The database: storage crates, the engine, OTLP decoding, the server |
| `scripts/` | Test, end-to-end and benchmark scripts |
| `bench/tracegen` | Deterministic generator of agent-shaped OTLP traces |
| `bench/corpusprep` | Fetches public agent-trajectory datasets used as text |
| `experiments/` | The experiments that tested the design, with their specs and raw results |
| `docs/DESIGN.md` | The design and the reasoning behind each choice |
| `docs/CHECKPOINTS.md` | The build plan and current state |
| `docs/benchmarks/` | Measured results |

## What the server measures today

At checkpoint 4, on a laptop in Docker with 2 CPUs and 4 GB. Method, caveats
and every number: [docs/benchmarks](docs/benchmarks/README.md).

| | NightsWatchDB server | ClickHouse baseline (earlier run) |
|---|---|---|
| 8 GB of agent-trace text, stored | 62 MB | 496 MB |
| ... list traces with a filter, median | about 1 ms | 586 ms |
| 10M spans without text, stored | 560 MB | 469 MB |
| ... open a trace, median | 0.9 ms | 11.5 ms |
| ... list the newest 50 traces, median | 0.8 ms | 4.9 ms |
| ... token totals over the whole range, median | 379 ms | 272 ms |

It is ahead where the design is built and behind where it is not: span-only
data is stored in many small parts until compaction exists, and long-range
aggregates read them one by one. The baseline used the exporter's schema as
shipped and was not measured in the same run.

## Running it

```sh
docker build -t nightswatchdb .
docker run -d --name nwdb \
  -v nwdb-data:/var/lib/nightswatchdb \
  -p 127.0.0.1:4317:4317 -p 127.0.0.1:4320:4320 \
  nightswatchdb
```

Send traces to `127.0.0.1:4317` with any OTLP gRPC exporter, then:

```sh
curl 'http://127.0.0.1:4320/api/v1/traces?limit=10'
curl  http://127.0.0.1:4320/api/v1/traces/<trace id>
curl  http://127.0.0.1:4320/api/v1/tokens
```

The ports are published on the loopback address on purpose: there is no
authentication yet. Configuration, the API and the limits are described in
the [server README](crates/server/README.md).

## Building and testing

The engine is developed and tested in Docker so results match a Linux
deployment. See [scripts](scripts/README.md):

```sh
scripts/test.sh    # format check, lints, every test, in a Linux container
scripts/e2e.sh     # build the image, load traces, kill -9, check the data
scripts/bench.sh   # measure a dataset on a fixed resource profile
```

## Licence

Apache License 2.0. See [LICENSE](LICENSE).

The benchmark text comes from public datasets that are downloaded by
`bench/corpusprep` and are not redistributed here; their licences and required
attribution are listed in [bench/corpusprep/README.md](bench/corpusprep/README.md).
