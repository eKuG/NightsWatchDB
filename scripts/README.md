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
listed trace must open with as many spans as its summary says. It repeats the
checks after `kill -9` and after a `docker stop`, which must exit with code 0.
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
  generator summaries and the raw `querybench.json`. Only these small files
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
