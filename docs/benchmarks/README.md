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
  token totals over the whole range and over one hour, and eight searches of
  message text over the whole range, newest 20 spans each (a word, a common
  word, AND, OR, NOT, a phrase, a prefix, a word that is nowhere). Count, errors, p50, p95,
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

### Results at checkpoint 4

Run on 2026-11-25 with the `small` profile (2 CPUs, 4 GB) on an Apple M4 Pro
under Docker Desktop, nothing else running, commit `dade925` (tag
`checkpoint-04`). The files written by each run are in [server/](server/).

**Text-heavy data (`d1`): 8.0 GB of agent traces, 91,419 spans, 5,034 traces.**

| | |
|---|---|
| Sent (protobuf before compression) | 8,018 MB |
| Stored, all part files | 62.1 MB, 129 times smaller |
| Left in the log after the last flush | 15.4 MB |
| Server CPU for the whole load | 19.8 s, or 2.5 CPU-seconds per GB |
| Server peak memory during the load | 846 MiB |
| Restart after a clean stop / after `kill -9` | 92 ms / 122 ms |

| Query | p50 ms | p99 ms |
|---|---|---|
| Open a trace (2.3 MB of JSON on average) | 1.6 | 59 |
| List the newest 50 traces | 0.48 | 0.64 |
| List 50 for one service | 1.08 | 1.20 |
| List 50 for one model | 0.76 | 0.95 |
| List 50 with an error | 0.62 | 0.73 |
| Token totals, whole range | 5.5 | 5.7 |
| Token totals, one hour | 0.78 | 0.87 |

**Span-only data (`d2`): 10,000,039 spans, 545,163 traces, no message text.**

| | |
|---|---|
| Sent (protobuf before compression) | 3,622 MB |
| Stored, all part files | 560 MB, 56 bytes per span, in 125 parts |
| Left in the log after the last flush | 134 MB |
| Server CPU for the whole load | 37.3 s, or 3.7 CPU-seconds per million spans |
| Server peak memory during the load | 1,218 MiB |
| Restart after a clean stop / after `kill -9` | 160 ms / 203 ms |

| Query | p50 ms | p99 ms |
|---|---|---|
| Open a trace | 0.89 | 1.19 |
| List the newest 50 traces | 0.76 | 0.83 |
| List 50 for one service | 0.95 | 1.12 |
| List 50 for one model | 0.84 | 1.15 |
| List 50 with an error | 0.79 | 0.93 |
| Token totals, whole range | 379 | 386 |
| Token totals, one hour | 21.8 | 22.6 |

**What the first run found.** The same span-only dataset was first run on the
commit before (`484516a`, while other work was using the machine). Listing
took 6.6 seconds at the median, and the harness needed 31 minutes to page
through the traces. The list query read every trace summary in the range and
asked every part about every trace. It now reads newest first and stops when
the answer cannot change. The responses have the same sizes before and after.

| Query, span-only data | p50 before, ms | p50 after, ms |
|---|---|---|
| List the newest 50 traces | 6,620 | 0.76 |
| List 50 for one service | 6,044 | 0.95 |
| List 50 for one model | 5,206 | 0.84 |
| List 50 with an error | 4,348 | 0.79 |
| Open a trace | 2.1 | 0.89 |
| Server memory after the query phase | 1,715 MiB | 104 MiB |

**Beside the earlier numbers.** The datasets and the profile are the ones of
experiment A, so the server can be set beside that experiment and beside the
ClickHouse baseline measured then. They are not one run: the baseline was
ClickHouse server loaded through the OpenTelemetry Collector's exporter with
the exporter's own table (`ORDER BY (ServiceName, SpanName,
toDateTime(Timestamp))`, one partition per day), measured on another day
through its own pipeline. A schema tuned for these queries would beat it.

The harness also runs the experiment's three queries with the experiment's
parameters (`exp_list1`, `exp_list2`, `exp_agg` in the result files of
2026-11-26: the most frequent root service and request model, and the middle
half of the time range), so these rows compare the same thing:

| | Server now | Experiment code | ClickHouse baseline |
|---|---|---|---|
| 8 GB of text, stored | 62.1 MB | 62.0 MB + 3.9 MB span tables | 495.8 MB |
| 8 GB of text, newest 50 traces with an error and one model, p50 | 1.4 ms | 0.34 ms | 586 ms |
| 8 GB of text, token totals of chat spans by model, p50 | 3.4 ms | 1.4 ms | 420 ms |
| 10M spans, stored | 560 MB | 385 MB | 469 MB |
| 10M spans, open a trace, p50 | 0.9 ms | 0.59 ms | 11.5 ms |
| 10M spans, newest 50 traces of one service, p50 | 0.92 ms | 7.0 ms | 4.9 ms |
| 10M spans, newest 50 traces with an error and one model, p50 | 0.99 ms | 8.8 ms | 773 ms |
| 10M spans, token totals of chat spans by model, p50 | 196 ms | 86 ms | 272 ms |

What still differs between the columns:

- The server is measured over HTTP from another container and returns whole
  trace summaries as JSON. The experiment code was timed in-process with files
  open and returned fewer fields.
- The baseline was measured on 2026-11-24 after `OPTIMIZE FINAL`, through its
  own client. The server has no compaction, so it answers from 105 and 125
  parts.
- The server's data holds the small extra batch the harness sends before its
  crash restart (750 and 2,464 spans), so the time window moves by a hair.
- The server counts a trace as having an error when a span has an `error.type`
  or an error status; the experiment used `error.type` alone. The generator
  sets both together, so the answers are the same on this data.
- The other list and token rows in the result files (`list_newest_50`,
  `tokens_range` and so on) are the harness's own queries over the whole
  range. They are not the experiment's queries and are not compared with it.
  An earlier version of this page did compare them; that was a mistake.

**Where the server is behind.**

- Span-only data takes 560 MB, more than the experiment (385 MB) and more
  than the baseline (469 MB). It is in 125 small parts that are never merged;
  compaction is checkpoint 6. Until that is built and measured, this is a
  deficit, not a promise.
- Token totals on the span-only data take 196 ms for the experiment's query,
  ahead of the baseline (272 ms) and behind the experiment code (86 ms). The
  query reads five columns of every part on one thread.
- Opening a large trace has a long tail: 59 ms at the 99th percentile on the
  text-heavy data, where a trace is megabytes of JSON.
- Peak memory during a load is 0.8 to 1.2 GB with the default buffer sizes.
- The log keeps up to one file of already-flushed records on disk.
- Full-text search is not in the server yet, so it is not measured.

### Results at checkpoint 6

Run on 2026-11-26 with the `small` profile (2 CPUs, 4 GB) on the same machine,
nothing else running, commit `a30eb0f`. Since checkpoint 4 the server has
gained the text index and search (checkpoint 5) and compaction, retention,
log rotation and new column encodings (checkpoint 6). Compaction ran during
the load with a quiet period of 5 seconds, and storage was measured once every
window had settled into one part. The files are in [server/](server/).

**Text-heavy data (`d1`): 8.0 GB of agent traces, 91,419 spans, 5,034 traces.**

| | Checkpoint 4 | Now |
|---|---|---|
| Stored, all part files | 62.1 MB, no text index | 87.8 MB with the text index, 91 times smaller than what was sent |
| Parts | 105 | 61, one per window |
| Left in the log after the last flush | 15.4 MB | 4 KB |
| Server CPU for the whole load | 19.8 s, 2.5 CPU-seconds per GB | 25.0 s, 3.1 CPU-seconds per GB |
| Server peak memory during the load | 846 MiB | 919 MiB |
| Restart after a clean stop / after `kill -9` | 92 ms / 122 ms | 82 ms / 123 ms |

Of the 8.0 GB sent, 7,964 MB is text that went to the piece store; 306 MB of
it is distinct, and it is stored in 47.5 MB. The rest of the 87.8 MB is the
span tables, the text index and the lists of rows per piece.

| Query | p50 ms | p99 ms |
|---|---|---|
| Open a trace (2.3 MB of JSON on average) | 1.5 | 54 |
| List the newest 50 traces | 0.45 | 0.58 |
| List 50 for one service | 0.74 | 0.84 |
| List 50 for one model | 0.56 | 0.68 |
| List 50 with an error | 0.51 | 0.63 |
| Token totals, whole range | 3.4 | 3.9 |
| Token totals, one hour | 0.53 | 0.58 |
| Search: `traceback` | 0.27 | 0.38 |
| Search: `error` | 0.27 | 0.33 |
| Search: `error timeout` (AND) | 0.30 | 0.37 |
| Search: `timeout OR retry` | 0.24 | 0.35 |
| Search: `error -import` | 0.57 | 0.68 |
| Search: `"not found"` (phrase) | 1.50 | 1.75 |
| Search: `config*` (prefix) | 0.27 | 0.37 |
| Search: a word that is nowhere | 0.18 | 0.22 |

Every search asks for the newest 20 matching spans of the whole range, and
all but the last find matches. A search stops once the parts it has read fill
the limit and no older part can change the answer, so these are times for
words that occur often. A rare word must look into every part.

**Span-only data (`d2`): 10,000,039 spans, 545,163 traces, no message text.**

| | Checkpoint 4 | Now |
|---|---|---|
| Stored, all part files | 560 MB, 56 bytes per span | 431 MB, 43 bytes per span |
| Parts | 125 | 95, one per window |
| Left in the log after the last flush | 134 MB | 4 KB |
| Server CPU for the whole load | 37.3 s, 3.7 CPU-seconds per million spans | 47.7 s, 4.8 CPU-seconds per million spans |
| Server peak memory during the load | 1,218 MiB | 1,148 MiB |
| Restart after a clean stop / after `kill -9` | 160 ms / 203 ms | 135 ms / 140 ms |

| Query | p50 ms | p99 ms |
|---|---|---|
| Open a trace | 0.56 | 0.89 |
| List the newest 50 traces | 0.71 | 0.83 |
| List 50 for one service | 0.90 | 1.05 |
| List 50 for one model | 0.78 | 0.91 |
| List 50 with an error | 0.74 | 0.88 |
| Token totals, whole range | 296 | 313 |
| Token totals, one hour | 16.1 | 17.0 |

**What made the span-only data smaller.** Not merging. The checkpoint 4 text
above blames the many small parts, and that was wrong: the merges of this run
rewrote 143.2 MB of parts into 142.8 MB. The 129 MB came from choosing an
encoding per column (deltas for values that are sorted or nearly so, bytes
split by position for ids, no dictionary where values do not repeat). What
merging changes is the number of parts a query opens and the sharing of text
across flushes: on the text-heavy data the merges rewrote 73.0 MB into 65.6 MB.

**Beside the earlier numbers.** The same three columns as at checkpoint 4,
with the same reservations: the baseline is the Collector exporter's schema as
shipped, measured on another day through its own client, and the experiment
code was timed in-process.

| | Server now | Experiment code | ClickHouse baseline |
|---|---|---|---|
| 8 GB of text, stored with a text index | 87.8 MB | 62.0 MB + 3.9 MB span tables + 24 MB index | 495.8 MB + 119 MB index |
| 8 GB of text, newest 50 traces with an error and one model, p50 | 0.93 ms | 0.34 ms | 586 ms |
| 8 GB of text, token totals of chat spans by model, p50 | 2.2 ms | 1.4 ms | 420 ms |
| 10M spans, stored | 431 MB | 385 MB | 469 MB |
| 10M spans, open a trace, p50 | 0.56 ms | 0.59 ms | 11.5 ms |
| 10M spans, newest 50 traces of one service, p50 | 0.79 ms | 7.0 ms | 4.9 ms |
| 10M spans, newest 50 traces with an error and one model, p50 | 0.84 ms | 8.8 ms | 773 ms |
| 10M spans, token totals of chat spans by model, p50 | 154 ms | 86 ms | 272 ms |

The experiment's 385 MB is not a like-for-like target, and the checkpoint 4
text should not have treated it as one: its span table kept a fixed set of
columns and only a count of a span's other attributes. The server keeps every
attribute, the resource, the scope, events and links, as sent. The baseline
keeps them too, so 469 MB is the fair comparison, and the server is now under
it. The search rows are not set beside experiment B or the baseline's text
index: the queries are not the same ones.

**Where the server is behind, or pays more than before.**

- Token totals over ten million spans take 154 ms for the experiment's query,
  ahead of the baseline and behind the experiment code (86 ms). The query
  reads five columns of every part on one thread.
- Opening a large trace still has a long tail: 54 ms at the 99th percentile on
  the text-heavy data.
- Loading costs more CPU than at checkpoint 4: 3.1 instead of 2.5 CPU-seconds
  per GB of text (the text is indexed as it is written, and merged), and 4.8
  instead of 3.7 per million spans (parts are merged while the load runs).
- Peak memory during a load is still 0.9 to 1.1 GB with the default buffers.
- A phrase costs several times a word, because its candidates are confirmed
  against the text.
- Search was measured with eight fixed queries on one dataset. That shows the
  order of magnitude and nothing about rare words over many parts, large
  result sets or concurrent searches.
