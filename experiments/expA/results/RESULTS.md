# Experiment A result: the layout holds, and storing each message once wins

Run 2026-11-24 with `official-d2.sh` and `official-d1.sh`. Docker Desktop on an
Apple M4 Pro (linux/arm64 VM). Every step ran alone in a container limited to
2 CPUs and 4 GB. ClickHouse 26.10.12.13 loaded through otelcol-contrib 0.165.0
with the exporter's own schema, untouched, measured after `OPTIMIZE FINAL`.
Reproduce the tables with `python3 -I summarize.py results/d2-cpu2-mem4g results/d1-cpu2-mem4g`.

## Pass marks

| Pass mark | Result | Verdict |
|---|---|---|
| Open a trace, median under 100 ms | 0.6 ms for span rows on 10M spans; 0.14 ms rows + 0.76 ms text on content data | pass |
| List and aggregate within 2x of ClickHouse | list 1.4x slower; filtered list 88x faster; aggregate 3.2x faster (10M spans) | pass |
| Stored bytes no more than ClickHouse | 385 MB vs 469 MB (narrow); 66 MB vs 496 MB (content) | pass |

All four systems (three layouts and ClickHouse) returned identical results for
every query, on both datasets.

## Narrow data: 10,000,039 spans, 545,063 traces, no text

| | cotrace (chosen) | service-sorted | trace-id-sorted | ClickHouse |
|---|---|---|---|---|
| Size on disk, MB | 385 | 403 | 354 | 469 |
| Build or load, wall s | 10.1 | 18.3 | 18.1 | 73 |
| Build or load, CPU s | 19.9 | 25.3 | 25.2 | 124 (with collector) |
| Build peak memory, MB | 125 | 645 | 641 | n/a |
| Open trace p50 / p99, ms | 0.59 / 0.71 | 4.96 / 16.0 | 1.89 / 2.23 | 11.5 / 30.2 |
| List newest 50 by service, p50 ms | 7.0 | 4.6 | 183 | 4.9 |
| List with trace-level filter, p50 ms | 8.8 | 209 | 180 | 773 |
| Aggregate tokens by model, p50 ms | 86 | 75 | 145 | 272 |

Warm cache. Cold-cache figures were within about 15% of these for our side and
for ClickHouse, which suggests the "cold" runs were not truly cold on this
machine (see caveats).

## Content data: 8.02 GB, 91,419 spans, 5,014 traces, 93% repeated text

| Payload store | Size, MB | Shrink | Build MB/s | Build memory, MB | Open trace text p50 / p99, ms | One span p50 / p99, ms |
|---|---|---|---|---|---|---|
| Plain Parquet | 166.7 | 48x | 1,120 | 214 | 1.72 / 10.1 | 1.69 / 2.62 |
| Tuned Parquet (32 MiB pages, zstd 9) | 72.7 | 110x | 797 | 394 | 18.3 / 28.1 | 17.0 / 24.6 |
| Per-trace zstd, long window | 102.1 | 78x | 656 | 117 | 0.10 / 7.6 | 0.25 / 2.8 |
| Each message stored once (dedup) | 62.0 | 129x | 876 | 35 | 0.76 / 8.7 | 0.44 / 1.4 |
| ClickHouse, whole table | 495.8 | 16x | 191 (pipeline) | n/a | 7.5 / 18.1 (rows and text) | n/a |

Every store returned every byte correctly (SHA-256 per trace, 0 mismatches,
0 hash collisions, 0 fallbacks). Our span tables for this data add 3.9 MB.

Unique bytes as a share of all text, by how widely pieces are shared:
one trace 8.2%, one 15-minute segment 3.9%, whole dataset 3.5%.

Span queries on the content data, warm, p50: filtered list 0.34 ms vs 586 ms;
aggregate 1.4 ms vs 420 ms. ClickHouse has to read its whole attributes map,
text included, to reach a token count.

## Reading

- **Decision 1 holds.** Keeping a trace together with a trace index and a
  per-trace summary gives the fastest trace open by a wide margin, makes
  trace-level filters cheap, needs no sort at build time, and costs about 9%
  more disk than the smallest layout.
- **One weak spot.** The plain "newest 50 for a service" list is 1.4x slower
  than ClickHouse and 1.5x slower than the service-sorted layout, because the
  summary is ordered by time only. Within the pass mark, and fixable (scan
  backwards and stop at 50, or keep a per-service order).
- **Store each message once.** It is the smallest, uses the least memory to
  build by a factor of three or more, and reads a single span fastest. Sharing
  within a 15-minute segment captures nearly everything that sharing across the
  whole dataset would (3.9% against 3.5%), so no global index is needed.
- **Tuned Parquet is not the answer.** It gets close on size only by using
  pages so large that every read decompresses megabytes.
- **Per-trace zstd** opens a whole trace fastest at the median, but is 65%
  larger and cannot share anything between traces.

## Caveats

- Docker on a Mac: ARM, virtualised disk. Cold and warm numbers are almost the
  same, so the page-cache drop inside the VM probably did not reach a cache
  below it. Treat every latency as warm. Object storage will be slower.
- Our side is purpose-built read code timed in-process, one query at a time,
  with files and metadata already open. ClickHouse is a full server parsing
  and planning SQL. Part of each sub-millisecond gap is that difference, and a
  real engine will add its own overhead.
- ClickHouse ran the exporter's schema exactly as shipped. A tuned schema with
  the `gen_ai` fields as real columns and the text in its own column would
  close most of the filtered-list and aggregate gap.
- Ingest CPU is not comparable yet: our builds read a file with no network,
  no write-ahead log and no durability. Experiment C measures that.
- One synthetic dataset. Its repetition (93%) matches what was measured on
  public agent traces (92 to 97%), but those measurements were themselves
  small. With content capture off there is no text and only the narrow-data
  results apply.
