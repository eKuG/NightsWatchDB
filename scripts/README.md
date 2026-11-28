# scripts

## test.sh

Runs the checks that count, in one `rust:1.100.0-bookworm` container, failing
at the first problem:

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. `cargo test --workspace --locked`

```sh
scripts/test.sh                 # everything
scripts/test.sh -- --nocapture  # extra arguments go to `cargo test`
CPUS=2 MEM=4g scripts/test.sh   # container limits (defaults: 4 CPUs, 6g)
```

The source tree is mounted read-only at `/src`. Build output goes to the
Docker volume `nwdb-target` and the cargo registry to `nwdb-cargo`, never to
the host directory. To start from a cold cache, remove those two volumes.
Engineers working at the same time set `NWDB_TARGET_VOLUME` (and `NWDB_CARGO_VOLUME`)
to names of their own.

The image has no rustfmt or clippy, so the script installs them at the start
of each run (a few seconds).

## e2e.sh

Builds the server image (`Dockerfile`) and the load generator image
(`bench/tracegen/Dockerfile`), starts the server in a container limited to 2
CPUs and 1 GB, loads generated traces over OTLP gRPC (half uncompressed, half gzip, from a
real grpc-go client) and checks the data
through the HTTP API: the span count must equal what the generator sent, and a
listed trace must open with as many spans as its summary says, and a word and a
two-word phrase taken from the text of a stored trace (read through the API) must
find that trace in `/api/v1/search` once the data is flushed (search covers
flushed parts only, so this waits up to 150 seconds for a flush). It repeats the
checks after `kill -9` and after a `docker stop`, which must exit with code 0.

Annotations: after the load it picks four traces from the listing and writes
through `POST /api/v1/annotations`: on trace 1 a `quality` score that it then
overwrites (0.9 `good`, then 0.2 `bad`), a span-level `thumbs` label with a
comment and a `checked` label; on trace 2 a `quality` score that it then
deletes; nothing on trace 3; on trace 4 a `quality` score of 0.9 and a `thumbs`
label `up`. One request mixes a valid item with one for a trace id that does
not exist (accepted 1, one rejection with index 1). The same
write with `Content-Type: text/plain` must get 415, and with a foreign `Origin`
or `Sec-Fetch-Site: cross-site` 403, and must change nothing. The trace views
and the filters of the list are checked in the same three states as
the rest (right after the writes, from the memory buffer; after `kill -9`, from
the log; after the clean stop, from parts). Each filter is checked both ways, so
that a server that ignored it would fail: `annotation_max=0.5` lists trace 1
(0.2) and not trace 4 (0.9); `annotation_min=0.5` the reverse;
`annotation_label=down` lists trace 1 and not trace 4 (`up`), and `up` the
reverse; `annotation=quality` lists trace 1 and not trace 2, whose score was
deleted (also with `annotation_source=e2e`). After the clean stop
`/api/v1/stats` must show `annotation_parts` of at least 1 and
`annotations_buffered` of 0. A failed check prints its name and the response it
got.

It prints `PASS` or `FAIL` and removes its containers, network and volume on
exit; the two images (`nightswatchdb:e2e`, `nwdb-tracegen:e2e`) stay.
`E2E_TRACES` sets the number of traces (default 3000).

## bench.sh

Benchmarks the server image: one dataset on one resource profile.

```sh
scripts/bench.sh [--dataset d1|d2|smoke] [--profile small|standard] [--out DIR]
```

- Profiles: `small` limits the server to 2 CPUs and 4 GB, `standard` to 8 CPUs
  and 12 GB. The generator and the query tool are not limited.
- Datasets: `d1` is `tracegen --bytes 8GB --seed 1` (text-heavy) and `d2` is
  `tracegen --no-content --spans 10000000 --seed 1` (spans only), the
  definitions of `experiments/expA`. `smoke` is `--no-content --spans 30000
  --seed 1`, for checking the harness (about a minute); it needs no corpus.
- `d1` and `d2` read the text corpus from the Docker volume `nwdb-bench-corpus`.
  The first run copies `.data/corpus` into it (read-only, once); later runs
  reuse it. If `.data/corpus` is missing the script says so and names
  `bench/corpusprep`.
- Steps: build the three images, create a network and a fresh data volume,
  start the server, load the dataset over OTLP gRPC with gzip, record storage,
  restart cleanly, load a small extra batch and `kill -9` the server, restart,
  then run `bench/querybench` against the restarted server. After each step the
  span count the server reports is compared with what was sent; any difference
  fails the run.
- Output: `result.json` and `result.md` in `DIR`
  (default `.data/bench/<date>-<commit>-<dataset>-<profile>/`), plus the
  generator summaries and the raw `querybench.json` and `querybench-annotations.json`. Only these small files
  reach the host; data stays in Docker volumes.
- `BENCH_QUERIES`, `BENCH_WARMUP` and `BENCH_CONCURRENCY` change the query
  phase (defaults 200, 20, 1).
- Compaction runs in the server while the data loads. After the load the script
  waits until the buffer is flushed, no merge is running and no window has more
  than one part (`BENCH_COMPACTION_TIMEOUT`, default 3600 seconds), and records
  how long that took, the server CPU it used and the compaction statistics;
  storage is measured after that. The server runs with a short quiet period,
  `BENCH_COMPACTION_QUIET` (default `5s`; production defaults to 60 seconds), so
  the wait is short. `BENCH_NO_COMPACTION=1` starts the server with
  `--no-compaction` and skips the wait, to see what compaction saves.
- `BENCH_CORPUS_DIR` is the host directory that fills the corpus volume (the
  first time only; default `.data/corpus`).
- It removes its containers, network and data volume on exit. The images and
  the corpus volume stay. It refuses to start while another `nwdb-bench-*`
  container is running.
- Memory peaks: the annotation phase is a second querybench run
  (`-annotate-only`; the first run has `-annotate=false`), so that the peak is
  read between them. `server_peak_memory_after_queries_bytes` (the row "after
  queries") is the peak before the annotation phase, as in earlier results;
  `server_peak_memory_after_annotations_bytes` the peak after it. The first
  run's output is `querybench.json` (`queries` in `result.json`), the
  second's `querybench-annotations.json` (`annotation_queries`), which has
  the annotation step and the kinds that use annotations.
- Annotations: after its normal query kinds, querybench writes annotations
  (10% of the traces get a `quality` score, 1% a `human` label `down`, the
  two sets disjoint: the human ones are taken from the other end of the
  permutation of the trace ids, in requests of 500), waits until `annotations_buffered` is 0 and times the
  queries that use them (see below). The script then waits for compaction a
  second time and adds to `result.json` (`annotations`) and `result.md`:
  annotations sent and per second, seconds until they were in parts, seconds
  until compaction settled, `annotation_parts` and `annotation_bytes` right
  after the flush and after settling, bytes per annotation, and a second
  measurement of the data volume. That second `du` runs in the helper
  container while the server runs (the helper only reads), so it also holds the
  crash batch and any log not yet folded into parts; the first one is taken
  with the server stopped, as before. `BENCH_NO_ANNOTATIONS=1` skips the whole
  annotation phase, for a server without annotations.

### Query kinds

The kinds of `bench/querybench`, in the order they run (all are in the
`Query` table of `result.md`): `open_trace`, `list_newest_50`,
`list_service_50`, `list_model_50`, `list_error_50`, `tokens_range`,
`tokens_hour`, `exp_list1`, `exp_list2`, `exp_agg`, `search_*`. Then, unless
the phase is skipped (`querybench -annotate=false`):

- `annotate_post_500`: one request of 500 annotations, one at a time; latency
  per request. The totals (sent, accepted, rejected, seconds, per second, the
  wait until they were in parts and the annotation fields of `/api/v1/stats`)
  are in the `annotation` object of its output. A rejected item fails the run,
  and so does a buffer that is not empty after 120 seconds.
- `open_trace_annotated`: a trace that has an annotation.
- `list_annotated_50`: `annotation=quality` (10% of the traces).
- `list_annotation_label_50`: `annotation=human&annotation_label=down` (about
  1%).
- `list_annotation_value_50`: `annotation=quality&annotation_min=0.999` (about
  0.01%, the selective case that walks the whole range).
- `open_trace_after` and `list_newest_50_after`: the requests of `open_trace`
  and `list_newest_50` again, to show what annotations cost the queries that do
  not ask for them.
