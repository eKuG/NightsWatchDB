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

The image has no rustfmt or clippy, so the script installs them at the start
of each run (a few seconds).

## e2e.sh

Builds the server image (`Dockerfile`) and the load generator image
(`bench/tracegen/Dockerfile`), starts the server in a container limited to 2
CPUs and 1 GB, loads generated traces over OTLP gRPC and checks the data
through the HTTP API: the span count must equal what the generator sent, and a
listed trace must open with as many spans as its summary says. It repeats the
checks after `kill -9` and after a `docker stop`, which must exit with code 0.
It prints `PASS` or `FAIL` and removes its containers, network and volume on
exit; the two images (`nightswatchdb:e2e`, `nwdb-tracegen:e2e`) stay.
`E2E_TRACES` sets the number of traces (default 3000).
