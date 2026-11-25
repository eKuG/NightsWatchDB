#!/bin/sh
# Format check, lint and test the workspace inside a pinned Rust container.
# Extra arguments are passed to `cargo test`. See scripts/README.md.
set -eu

IMAGE=rust:1.100.0-bookworm
ROOT=$(cd "$(dirname "$0")/.." && pwd)

# The source is mounted read-only, so the target directory lives in a volume.
# The registry lives in a volume too so dependencies download once.
# Two checkouts tested at the same time must not share a target volume:
# set NWDB_TARGET_VOLUME to a different name for each.
TARGET_VOLUME=${NWDB_TARGET_VOLUME:-nwdb-target}
CARGO_VOLUME=${NWDB_CARGO_VOLUME:-nwdb-cargo}
exec docker run --rm \
  --cpus "${CPUS:-4}" \
  --memory "${MEM:-6g}" \
  -v "$ROOT":/src:ro \
  -v "$TARGET_VOLUME":/nwdb-target \
  -v "$CARGO_VOLUME":/nwdb-cargo \
  -e CARGO_TARGET_DIR=/nwdb-target \
  -e CARGO_HOME=/nwdb-cargo \
  -e CARGO_TERM_COLOR=never \
  -w /src \
  "$IMAGE" \
  sh -eu -c '
    # The image ships without rustfmt and clippy.
    rustup component add rustfmt clippy >/dev/null 2>&1
    echo "== cargo fmt"
    cargo fmt --all --check
    echo "== cargo clippy"
    cargo clippy --workspace --all-targets --locked -- -D warnings
    echo "== cargo test"
    cargo test --workspace --locked "$@"
  ' sh "$@"
