# ClickHouse baseline (experiment A, stream 3)

Pipeline: `tracegen --otlp-endpoint` -> `otelcol-contrib` (otlp receiver, clickhouse exporter) -> ClickHouse,
all on a private Docker network. The exporter creates its own schema; nothing in it is tuned.

## Versions (pinned)

- ClickHouse server `clickhouse/clickhouse-server:26.10.12.13` (latest regular release on 2026-11-24; the LTS line is 26.3 / 26.8).
- Collector `otel/opentelemetry-collector-contrib:0.165.0` (latest release, `otelcol-contrib version 0.165.0`). Exporter docs read at tag v0.165.0.
- tracegen built from `bench/tracegen` with `golang:1.25-bookworm` / `debian:bookworm-slim` (image `expa-ch-tracegen`).
- Helper image for cgroup and page-cache access: `alpine:3.24.3` (already on the machine, not removed by `clean.sh`).

## Files

| file | purpose |
|---|---|
| `load.sh DATASET` | start ClickHouse + collector, run tracegen through the collector, drain, check counts, merge parts |
| `sizes.sh DATASET` | one JSON line: per-table and per-column sizes |
| `query.sh DATASET params\|sample-ids\|run` | queries, timing |
| `cold.sh DATASET` | restart ClickHouse and drop the VM page cache |
| `clean.sh DATASET [images]` | remove containers, network, volume, result files (and images) |
| `collector.yaml` | collector config |
| `queries/*.sql` | one file per query; `*_check.sql` are definition-literal versions used only to check answers |
| `common.sh` | names and helpers |

Everything is plain POSIX sh plus `docker` (and `awk`, `sed`, `sort`, `paste` on the host for small text). No ClickHouse server config file is used.

Naming: for dataset `X` everything is called `expA-ch-X-...`: volume `expA-ch-X-data` (mounted at
`/var/lib/clickhouse`), network `expA-ch-X-net`, containers `expA-ch-X-clickhouse` / `-collector` / `-tracegen`.
Host bind mounts: only the read-only corpus (into tracegen) and the small `collector.yaml` (read-only).
No generated data touches the host. `results/` holds small text files only.

## Usage

    CPUS=2 MEM=4g GEN_ARGS="--bytes 1GB" ./load.sh content-smoke          # D1 smoke
    CPUS=2 MEM=4g GEN_ARGS="--no-content --spans 500000" ./load.sh narrow-smoke   # D2 smoke
    ./load.sh content      # full D1: default GEN_ARGS "--bytes 8GB"
    ./load.sh narrow       # full D2: default GEN_ARGS "--no-content --spans 10000000"
    ./sizes.sh content-smoke
    ./query.sh content-smoke params
    ./query.sh content-smoke sample-ids 500 /path/ids.txt
    ./cold.sh content-smoke
    ./query.sh content-smoke run --service S --model M --from-ns N --to-ns N --ids /path/ids.txt [--runs 20] [--only open,list1,list2,agg] [--check]
    ./clean.sh content-smoke [images]

`CPUS`/`MEM` only matter when `load.sh` (or another script) has to create the ClickHouse container; an already
running container keeps its limits. `load.sh` refuses to run if the volume exists. The ClickHouse container
and its volume stay after `load.sh` (the collector container is removed). Dataset names starting with
`content` / `narrow` get the full-size defaults; any other name needs `GEN_ARGS`.

`--spans N` in tracegen stops at the first trace boundary at or after N, so 500,000 became 500,006.

## What load.sh does

1. Creates network and volume, starts ClickHouse with `--cpus/--memory`, starts the collector (`collector.yaml`).
2. Records cgroup `cpu.stat` for ClickHouse and the collector (read from the VM's root mount namespace by a
   privileged container, because the collector image has no shell), runs tracegen (its own `cpu.stat` is
   printed by a wrapper inside its container), then waits until `otelcol_exporter_queue_size` is 0 and
   `sent_spans == accepted_spans` for two consecutive 3-second polls. `wall_s` runs from tracegen start to that
   drain, so it includes up to ~5 s of batch flush timeout plus poll granularity; `send_wall_s` is tracegen only.
   `input_mb_per_s` = tracegen `encoded_bytes` / `wall_s`.
3. Checks `count()` of `otel.otel_traces` equals the span count tracegen reports (failure = non-zero exit, after printing the JSON).
4. For content data, also compares the byte totals of the five attribute classes tracegen reports
   (`bytes_by_class`) with `sum(length(SpanAttributes[key]))` read back from ClickHouse. This catches silent truncation.
5. Runs `OPTIMIZE TABLE ... FINAL` on both tables and waits for `system.merges` to be empty (set `OPTIMIZE=0` to skip). This is
   outside `wall_s`/CPU figures (`optimize_wall_s` is reported). Sizes and queries are therefore measured on fully merged
   parts. That favours ClickHouse relative to a live system with background merges still catching up; it is what a competent
   operator would converge to.

## Pipeline configuration changes (collector.yaml) and why

Defaults left alone: `ttl` (0 = no TTL, so the 2026-01-01 data cannot expire), `async_insert: true`, `compress: lz4`,
`create_schema: true`, table names/engine, `num_consumers: 10`. Changed:

| key | default | set | reason |
|---|---|---|---|
| `receivers.otlp.protocols.grpc.max_recv_msg_size_mib` | 4 | 64 | a tracegen batch is up to 3 MiB of spans plus the span that crosses the limit, and single values reach 1 MiB, so a request can exceed 4 MiB. **Precautionary: I ran the 1 GB D1 smoke with the 4 MiB default (copy of the config without this line) and it loaded all 10,533 spans with 0 refused. I did not observe a refusal and cannot say whether the full 8 GB run would hit one.** |
| `exporters.clickhouse.timeout` | 5s | 120s | a timed-out attempt that actually committed would be retried and insert duplicates |
| `sending_queue.queue_size` / `sizer` | 1000 requests | 64 requests | bounded memory (requests are up to ~4 MiB) |
| `sending_queue.block_on_overflow` | false | true | default drops data when the queue is full; blocking pushes back on tracegen, which sends synchronously |
| `sending_queue.batch` | off | `sizer: bytes`, `min_size: 8 MiB`, `max_size: 32 MiB`, `flush_timeout: 5s` | batching inside the queue is what the exporter README recommends over the batch processor. Bytes rather than the README's 5000 rows because rows are 1 KB (D2) to 1 MB (D1): 5000 content rows would be gigabytes per INSERT |
| `retry_on_failure.max_elapsed_time` | 300s | 3600s | do not drop a batch during a long stall |
| `service.telemetry.metrics.readers` | n/a | prometheus pull on :8888 | to read queue size and sent/accepted/refused counts |

ClickHouse side: no setting changed. `CLICKHOUSE_SKIP_USER_SETUP=1` only lets the collector container log in as the
passwordless `default` user over the private network; `--ulimit nofile=262144:262144`. `max_threads` is `auto(2)`, so the server
honours `--cpus 2` (checked in `system.settings`).

Not verified: that the full 8 GB / 10 M-span runs load without loss; only the smoke sizes were run.

## Exporter-created DDL (verbatim, `SHOW CREATE TABLE`, ClickHouse 26.10.12.13, exporter 0.165.0)

```sql
CREATE TABLE otel.otel_traces
(
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(3)),
    `TraceId` String CODEC(ZSTD(3)),
    `SpanId` String CODEC(ZSTD(3)),
    `ParentSpanId` String CODEC(ZSTD(3)),
    `TraceState` String CODEC(ZSTD(3)),
    `SpanName` LowCardinality(String) CODEC(ZSTD(3)),
    `SpanKind` LowCardinality(String) CODEC(ZSTD(3)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(3)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    `ScopeName` String CODEC(ZSTD(3)),
    `ScopeVersion` String CODEC(ZSTD(3)),
    `SpanAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    `Duration` UInt64 CODEC(ZSTD(3)),
    `StatusCode` LowCardinality(String) CODEC(ZSTD(3)),
    `StatusMessage` String CODEC(ZSTD(3)),
    `Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(3)),
    `Events.Name` Array(LowCardinality(String)) CODEC(ZSTD(3)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(3)),
    `Links.TraceId` Array(String) CODEC(ZSTD(3)),
    `Links.SpanId` Array(String) CODEC(ZSTD(3)),
    `Links.TraceState` Array(String) CODEC(ZSTD(3)),
    `Links.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(3)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1

CREATE TABLE otel.otel_traces_trace_id_ts
(
    `TraceId` String CODEC(ZSTD(3)),
    `Start` DateTime CODEC(Delta(4), ZSTD(3)),
    `End` DateTime CODEC(Delta(4), ZSTD(3)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1

CREATE MATERIALIZED VIEW otel.otel_traces_trace_id_ts_mv TO otel.otel_traces_trace_id_ts
(
    `TraceId` String,
    `Start` DateTime64(9),
    `End` DateTime64(9)
)
AS SELECT
    TraceId,
    min(Timestamp) AS Start,
    max(Timestamp) AS End
FROM otel.otel_traces
WHERE TraceId != ''
GROUP BY TraceId

```

No TTL clause. Primary key and sort key are `(ServiceName, SpanName, toDateTime(Timestamp))`, partition is one day.
The materialized view writes one `(TraceId, Start, End)` row per trace per insert block, so a trace split across blocks has
several helper rows (smoke: 27,590 helper rows for 27,570 traces). `Start` and `End` are `DateTime`: second resolution.

## Queries (queries/)

Parameters come from `queries/params.sql` (printed by `query.sh DS params`) and are passed to `run` as arguments; the layout
stream's `params` prints the same four fields. Rules: S = most frequent root-span `ServiceName` (root = empty `ParentSpanId`),
M = most frequent `gen_ai.request.model` (spans that have it), window = `[min + (max-min)/4, min + 3*(max-min)/4)` integer
nanoseconds over all span start times. Ties in S/M go to the smaller string; the spec does not say.
`query.sh DS sample-ids N OUT` orders distinct trace ids by (earliest span start, id) and takes positions 0, k, 2k, ...
with k = max(1, distinct/n); the layout stream orders by arrival, which differs only if two traces start in the same nanosecond.
The real harness passes its own file anyway.

- **open** (`open.sql`): the README's "find traces with traceID" pattern: scalar subqueries on `otel_traces_trace_id_ts`
  give min(Start) and max(End)+1, then `WHERE TraceId = id AND Timestamp BETWEEN start AND end` on the big table. `SELECT *`
  (all columns, all attributes). The README example's `LIMIT 100` is dropped (all spans); ordered by `Timestamp, SpanId`.
  EXPLAIN shows 2 of 65 granules read (bloom filter on TraceId after the time bound). The count of rows returned is
  checked against `count()` of those trace ids (`rows_expected`).
- **list1** (`list1.sql`): root spans of service S in the window, newest 50, ordered by `Timestamp DESC, TraceId`.
  `ServiceName` is the first key column. Added `toDateTime(Timestamp)` bounds next to the exact nanosecond ones so the
  third key column can be used (EXPLAIN: 7 of 65 granules). Could be argued: tie-break on TraceId is mine.
- **list2** (`list2.sql`): trace-level condition, so ClickHouse must group by trace. Written as: candidate trace ids from the
  small helper table (traces whose first second falls in the window, a superset at the boundary seconds); main table
  read from the window start only (PREWHERE), `GROUP BY TraceId HAVING min(Timestamp) in window AND error present AND model = M`.
  Trace start = earliest span start. `--check` compares against `list2_check.sql`, the literal-definition full scan, and
  `list1_check.sql` for list1.
  Could be argued: error = `mapContains(SpanAttributes, 'error.type')` (attribute present), not `StatusCode`.
- **agg** (`agg.sql`): chat spans (`SpanAttributes['gen_ai.operation.name'] = 'chat'`) with the window test in PREWHERE,
  grouped by `SpanAttributes['gen_ai.request.model']`, count and `sum(toInt64OrNull(...))` of the two token attributes
  (attributes are strings in the Map). It reads the whole `SpanAttributes` column for granules with window rows (EXPLAIN:
  46 of 65 granules on D2 smoke) because a Map is stored as one column; this is a property of the exporter's schema, not
  something I added. No projection or materialised column (schema not tuned).

### Timing method

Each query kind runs in one `clickhouse-client` session (connection made once, outside timing) through the server
container's own client over the loopback network (no Docker exec per query). Reported:

- `p50_ms/p90_ms/p99_ms/max_ms` and `runs_ms`: **client wall time** from `clickhouse-client --time` (send until the last
  result block is received; connection setup excluded; 1 ms resolution; results received and discarded with `FORMAT Null`,
  so transfer cost is included, formatting cost is not).
- `server_p50_ms...` and `server_runs_ms`: **server-side**, `event_time_microseconds - query_start_time_microseconds`
  from `system.query_log` (microseconds; `query_duration_ms` is the same number rounded to ms), selected by a per-run `log_comment` tag.
  Also `read_rows`, `read_bytes`, `peak_query_memory_bytes` (max `memory_usage`), `server_cpu_s` (ClickHouse cgroup CPU
  delta over the whole query kind).
- Percentiles by nearest rank. No warm-up run is discarded: run 1 is in the list. For `open`, each of the 500 ids is looked up once.
  Query cache is off (default). `result_sha256` is computed by a separate, untimed run of the same SQL wrapped in `SHA256(arrayStringConcat(groupArray(trace_id)))`;
  `result` for agg is likewise from an untimed run.

## Cold cache

`cold.sh` stops the container, runs `sync; echo 3 > /proc/sys/vm/drop_caches` in a `--privileged --pid=host` container
(page cache, dentries and inodes of the Docker VM), starts ClickHouse again (empty mark/uncompressed caches), waits for it.
How I checked that this works: (1) `Cached:` in the VM's `/proc/meminfo` fell from ~13.6 GB to 0.7 GB in a standalone test,
and cold.sh prints before/after (e.g. 4,023,828 kB -> 727,352 kB). (2) Reading the 264 MB `store/` of the content-smoke volume
with `cat` from a helper container took 0.05 s warm (twice), 0.62 s right after the drop, 0.04 s again after: the drop
makes the next read go to the virtual disk. Not checked: whether macOS caches the VM's disk image below the VM
(the 0.62 s figure, ~430 MB/s, suggests reads still come back fast).

## Smoke results

Real outputs are in `results/smoke/` (load, sizes, params, query JSON with full per-run lists, tracegen summaries,
collector logs). Both datasets: ClickHouse `--cpus 2 --memory 4g`, collector and tracegen unlimited.

- D1 `content-smoke` (`--bytes 1GB`): 10,533 spans sent, 10,533 rows, 0 refused/failed, per-class attribute bytes equal
  to tracegen's totals exactly (input 474,785,216; output 6,953,593; system 13,245,892; tool defs 521,007,039; tool args+results 11,388,548).
  tracegen window check: 2026-01-01 00:00:43 .. 14:39:19.
- D2 `narrow-smoke` (`--no-content --spans 500000`): 500,006 spans sent, 500,006 rows, 0 refused/failed.
  **Observed: spans run from 2026-01-01 00:00:01 to 2026-01-02 00:10:04, i.e. 10 minutes past the 24-hour window** in
  `--spans` mode (tracegen behaviour, not changed). It also means the table has two day partitions. Nothing was dropped.
- Compression: D1 smoke 1,032,015,908 B uncompressed -> 62,114,070 B (16.6x) in `otel_traces`;
  `SpanAttributes` is 99.6% of it. I did not investigate why the ratio is this high (ZSTD(3) on a sorted, heavily repeated column is the likely reason). Full-size numbers will differ (parts, window).


## Where this might be unfair to ClickHouse (a ClickHouse expert could object)

- Schema is untouched on purpose: sort key `(ServiceName, SpanName, toDateTime(Timestamp))` is not trace-oriented; Map columns
  force whole-column reads for any attribute filter (agg, list2); no projections, materialised columns, `map_serialization_version`,
  or typed columns for `gen_ai.*`. A tuned schema would be much faster on agg and list2.
- list2 is the hardest query for this schema; my formulation (helper-table candidates plus a bounded scan) is one of several.
- `open` uses `SELECT *`, i.e. returns all content to the client; a UI might fetch attributes separately.
- Client timing has 1 ms resolution; list1 (~3-4 ms) is therefore coarse on the client side; use the server figures there.
- Server settings are defaults at 2 CPUs / 4 GB; `max_threads` auto(2), mark cache and uncompressed cache default sizes.
- Smoke datasets are tiny (62 MB / 20 MB on disk) and fit in the page cache and mark cache, so nothing here says how it behaves at 8 GB / 10 M spans.
- Parts are fully merged (`OPTIMIZE FINAL`) before measuring; ingest-time CPU excludes that merge.

## Where this might be unfair to the other candidates

- ClickHouse is measured after `OPTIMIZE FINAL`, with a client in the same container (no network), and with async inserts the
  ingest CPU split (ClickHouse vs collector) includes the collector's protobuf decode and batching, which the others do not have.
- Collector and tracegen run unlimited; only ClickHouse is capped, as the spec says.

## Not done / could not match exactly

- Trace start for list2 is the earliest span start; list1 uses the root span's start. Same as the stream 1 text, but I could not
  compare with stream 1's output (sibling directory not read), so equality of `result_sha256` across engines is unchecked.
- Windows are applied to `Timestamp` (span start). Ties are broken by `TraceId`.
- Full-size D1/D2 loads were not run (smoke limits).

## Full-text search baseline (`search.sh`, `queries/search/`)

Added for experiment B (stream 2). New files only: `search.sh`, `queries/search/{untuned,tuned}/*.sql`,
`queries/search/sample_queries.sql`; no existing script was changed.

    ./search.sh DATASET untuned --queries FILE [--runs 5] [--reference FILE]
    ./search.sh DATASET tuned   --queries FILE [--runs 5] [--reference FILE]   # builds the table first if missing
    ./search.sh DATASET build | drop | sample-queries OUT
    PHRASE_POSITIONS=0 ... build     # index without phrase positions (default is with)
    NO_COND_CACHE=1 ./search.sh ...  # disable the server's query condition cache for the queries

Query file: `{"queries":[{"id","class","terms":[..]} | {"id","class","prefix"} | {"id","class","phrase":[a,b]}]}`
(parsed inside the server container with clickhouse-local). Output: one JSON line per query (`id`, `class`,
`rows`, `traces`, `server_runs_ms` from `system.query_log` in microseconds, `server_first_run_ms`, `server_p50_ms`, client
times, read statistics, `reference_ok` if `--reference` is given), then a summary with the per-class p50
(median over queries of the per-query p50) and `first_run_p50_ms`. Reference file = JSON lines with `id`, `rows`, `traces`.
`tuned` only touches the new table `otel.otel_traces_text`; the build line reports `otel_traces_untouched` (part names, row
count and bytes of `otel_traces` before and after). `search.sh DATASET drop` removes the table.

### Versions and documentation read
ClickHouse 26.10.12.13. The text index docs read at tag `v26.10.12.13-stable`
(`docs/reference/engines/table-engines/mergetree-family/textindexes.mdx`, plus the `hasAllTokens`, `hasAnyTokens`,
`hasPhrase`, `hasToken`, `splitByNonAlpha` and `mergeTreeTextIndex` pages). Points that matter and that I checked on the server
rather than trusting the page:
- Text indexes are GA from 26.2; `hasPhrase` is new in 26.4; storing positions (`support_phrase_search = 1`) is still experimental
  and needs the MergeTree setting `allow_experimental_text_index_phrase_search = 1` (the server refuses the DDL otherwise).
- The `hasToken` page says a token is `[0-9A-Za-z_]`. That is wrong for 26.10: testing every byte 0..255 between two letters shows
  `splitByNonAlpha`, `tokens()`, `hasToken` and `hasTokenCaseInsensitive` all treat exactly `0-9 A-Z a-z` and bytes >= 0x80 as token bytes and
  everything else (including `_` and NUL) as a separator. That is the reference tokenizer, so tokenisation matches exactly,
  with one exception: ClickHouse does not discard tokens longer than 64 bytes (see below).
- `lower()` is ASCII-only, so `lower(text)` is the reference case folding (A-Z only).
- `ilike`/`LIKE` dictionary scans need an index without a pre/postprocessor, and `ilike` falls back to a full scan for needles containing `k`; so
  the index is defined on the expression `lower(text)` rather than with a `preprocessor`, and queries use `lower(text)`.

### untuned
The exporter table as is. v1..v6 are `SpanAttributes[...]` of the six content keys; a row's tokens are the union.
- term/and/or/not: `hasTokenCaseInsensitive(v, term)` OR-ed over the six values (best plain token function: exact tokenisation, no allocation of a lower-cased copy).
- prefix: `match(v, '(?i)(?:^|[^a-z0-9\x{80}-\x{10FFFF}])prefix')` over the six values.
- phrase: `match(v, '(?i)(?:^|[^T])a[^T\x00]+b(?:$|[^T])')` per value, T = `a-z0-9\x{80}-\x{10FFFF}`, so adjacency never crosses values.
No index applies (EXPLAIN shows only the MinMax/partition/primary-key entries, all with every granule; the exporter's bloom filter
indexes cannot answer token queries). Every query reads the whole `SpanAttributes` column, i.e. all content.

### tuned
`otel.otel_traces_text(TraceId, SpanId, text)`, `ORDER BY (TraceId, SpanId)`, `text` = the six values joined by one NUL byte
(absent value = empty), `CODEC(ZSTD(3))` like the exporter's columns, one MergeTree part after `OPTIMIZE FINAL`:
```sql
CREATE TABLE otel.otel_traces_text
(TraceId String CODEC(ZSTD(3)), SpanId String CODEC(ZSTD(3)), text String CODEC(ZSTD(3)),
 INDEX idx_text lower(text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1))
ENGINE = MergeTree ORDER BY (TraceId, SpanId) SETTINGS index_granularity = 8192, allow_experimental_text_index_phrase_search = 1
```
The load is `INSERT ... SELECT concat(six values, char(0)) FROM otel.otel_traces` with 32 MB insert blocks
(`min_insert_block_size_rows = 0, min_insert_block_size_bytes = 33554432, max_insert_threads = 1, max_threads = 2`). With default
256 MB blocks the 1 GB smoke build ran out of the 4 GB memory limit (server limit about 3 GiB). Peak with the small blocks: 336 MB.
Queries (exact SQL in `queries/search/tuned/`):
- rare/mid/common: `hasAllTokens(lower(text), [t])`; and2/and3: `hasAllTokens(lower(text), [t1, t2, ...])`; or2: `hasAnyTokens(...)`;
  not: `hasAllTokens(.., [t1]) AND NOT hasAllTokens(.., [t2])`. All direct reads from the index.
- prefix: the prefix is expanded against the index dictionary, `hasAnyTokens(lower(text), (SELECT groupArray(token) FROM mergeTreeTextIndex('otel','otel_traces_text','idx_text') WHERE startsWith(token, lower(pre)) AND length(token) <= 64))`; exact and index-only.
  `mergeTreeTextIndex` is documented as an introspection function; using it for prefix expansion is my choice.
- phrase: `hasPhrase(lower(text), [a, b]) AND match(text, '<same adjacency regex as untuned>')`; `hasPhrase` is the index-assisted filter
  (positions), the regex guarantees no match across the NUL between attributes. On the 10 smoke phrases the guard changed no count.

#### EXPLAIN evidence (smoke table, `EXPLAIN indexes = 1, actions = 1`)
Every class uses the text index; none is left to a scan:
- term/and/or/not: `Prewhere filter column: __text_index_idx_text_hasAllTokens_...` (direct read, virtual column), `Skip / Name: idx_text`,
  e.g. term: `Granules: 33/97`; and2 `0/97` (empty intersection); not: `NOT __text_index_..hasAllTokens AND __text_index_..hasAllTokens`.
- prefix: `__text_index_idx_text_hasAnyTokens_...`, `idx_text Granules: 19/97`, plus a `mergeTreeTextIndex` dictionary read.
- phrase: the filter is `match(...) AND hasPhrase(...) AND __text_index_idx_text_has...` (hint), `idx_text` applied (97/97 for the
  very common pair in the no-positions run shown; with positions the work after the index is much smaller).
Full EXPLAIN outputs were inspected interactively; the key lines are the ones above.

### Smoke results (1 GB content dataset `content-search-smoke`, 10,533 spans, 597 traces, ClickHouse `--cpus 2 --memory 4g`)
Hand-written-by-SQL query file (`results/smoke-search/queries.json`, 10 queries per class, 90 in all, terms picked from the data by
row-level document frequency with `queries/search/sample_queries.sql`, which is NOT the real piece-level df tool). With these smoke
terms and and2/and3 the intersection is empty (0 rows) for all 10 queries: a property of the picked terms, not a failure.

Build (`results/smoke-search/build-positions.json`): positions on: wall 7 s (insert 5 s, optimize FINAL 2 s), CPU 9.95 s user + 1.76 s sys,
insert peak memory 336 MB, table 254,667,516 B on disk, of which index 189,267,271 B; the rest (text and keys) about 65.4 MB. Positions off:
wall 6 s, CPU 6.24 s + 0.16 s, table 87,854,345 B, index 22,451,432 B (positions make the index 8.4x bigger). The exporter table is
62,179,579 B for comparison. `otel_traces` unchanged in both builds.

Rows / traces per query (identical in untuned and tuned; `reference_mismatches` 0 when tuned is checked against the untuned output):
rare 2 / 1; mid 10 / 1-2; common 524-528 / 21-71; and2 and and3 0 / 0; or2 12 / 2-3; not 524-528 / 21-71; prefix 10-5544 rows, 1-531 traces; phrase 1-1279 rows, 1-107 traces.
Independent check with the `tokens()` function over the six values (a different code path) agreed for terms aantal (2) and wouldn (527),
prefix alth (195) and all 10 phrases.

Server-side p50 per class in ms (median over queries of each query's p50, 5 runs):

| class | untuned, condition cache off | untuned, run 1 only after cold.sh | tuned positions on, cache off | tuned run 1 | tuned no positions |
|---|---|---|---|---|---|
| rare | 143.1 | 140.3 | 1.12 | 1.75 | 1.05 |
| mid | 141.7 | 141.1 | 1.19 | 1.80 | 1.07 |
| common | 146.0 | 145.0 | 1.21 | 1.95 | 1.16 |
| and2 | 145.3 | 147.8 | 1.14 | 1.69 | 1.01 |
| and3 | 151.4 | 139.7 | 1.08 | 1.68 | 1.11 |
| or2 | 180.2 | 179.6 | 1.12 | 1.81 | 1.11 |
| not | 162.5 | 152.0 | 1.52 | 2.19 | 1.37 |
| prefix | 916.9 | 709.4 | 2.31 | 2.99 | 1.95 |
| phrase | 923.5 | 905.3 | 9.42 | 11.30 | 273.6 |

(untuned "cache on" p50s are meaningless as latencies: with the query condition cache on, runs 2-5 skip all granules that failed the filter
in run 1, e.g. rare 2.6 ms; this is why first-run and cache-off figures are given. The cache is on by default in ClickHouse, so a user who repeats the same search benefits.)
Client wall times are in the JSON. Raw outputs: `results/smoke-search/*.jsonl`.

### Things that do not match the definitions, and caveats
- **No `rows_sha256`.** The result unit is the ascending list of arrival-order row ids. The exporter table has no row id and span
  arrival order cannot be reconstructed from it, so only `rows` and `traces` are comparable with the reference. I did not compare against
  a reference file from the other stream (it did not exist yet); the check was untuned vs tuned and the `tokens()` re-computation.
- **Tokens over 64 bytes** are not discarded by the index tokenizer. For term queries (a-z, 4-20 bytes) this cannot change results; for prefix I
  filter them out of the expansion; for phrases tokens count for adjacency in both. Index size is slightly larger than the reference index would be
  (long tokens are indexed). I did not measure by how much.
- **Phrase across array elements.** The reference defines a phrase within one piece (one element of a message array); ClickHouse only has
  whole attribute values, so two tokens that are the last token of one message and the first token of the next (separated by `"}]}`, `,`, `{"role"`)
  would match here and not in a per-piece index. It needs a phrase whose second token is a structural word such as `role`
  after a message's last word; phrases are taken from inside pieces, so it should be very rare, but it cannot be excluded or measured from the exporter table.
  The reference's span-level texts have the same adjacency across attributes only through NUL, which the guard excludes.
- The dev query file uses row-level df; the official file uses piece-level df, so class sizes will differ.
- Not tested: the 8 GB dataset. The build at that size has not been run: expect many parts and merges of the text index (with positions the index is about 8x larger than without), and the 4 GB limit
  is a risk (the default insert block size already failed at 1 GB). I did not touch the official volumes.
- Might be called unfair to ClickHouse: positions are experimental (an expert may want them on, and I did); a text-only table with `ORDER BY` chosen for compression is a design
  decision (I used (TraceId, SpanId)); results are counts, not ids (cheaper than returning row lists); I did not tune cache sizes
  (`text_index_*_cache_*`), `posting_list_codec`, or `dictionary_block_size`.
- Might be called unfair to the others: the tuned table is a second copy of the text (its size is extra; the `otel_traces` copy is still needed to return spans); the build reads the whole exporter table once.
