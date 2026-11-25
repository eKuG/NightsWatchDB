#!/bin/sh
# Official run for experiment B on the 8 GB content dataset's dedup store.
#
# usage: [CPUS=2 MEM=4g] ./official.sh ours|clickhouse
#   ours        unit tests in a container, build both index variants, derive
#               the query set, check against the brute-force scan, time search
#   clickhouse  the same query file against ClickHouse, untuned and tuned
#
# Reads the store from the volume expA-official (read-only). Writes only to
# the volume expB-official and small JSON files under results/.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
CPUS=${CPUS:-2}; MEM=${MEM:-4g}
export CPUS MEM
OUT="$HERE/results/cpu${CPUS}-mem${MEM}"
STORE=/official/d1-pay-dedup
mkdir -p "$OUT"

ti() { docker run --rm --cpus "$CPUS" --memory "$MEM" -v expA-official:/official:ro -v expB-official:/data expb-textindex "$@"; }
step() { # output file, then the command; a failure is recorded, not fatal
  f=$1; shift
  echo "$*" >&2
  if "$@" > "$f" 2> "${f%.jsonl}.stderr"; then :; else
    rc=$?  # 137 means killed for exceeding the memory limit
    echo "{\"failed\":true,\"exit\":$rc}" >> "$f"
  fi
}

case "${1:?ours or clickhouse}" in
ours)
  # unit tests inside a Linux container with the same limits
  docker run --rm --cpus "$CPUS" --memory "$MEM" -v "$HERE/textindex":/src:ro rust:1.100.0-bookworm \
    sh -c 'cp -r /src /work && cd /work && rm -rf target && cargo test --release --locked 2>&1 | tail -6' > "$OUT/unit-tests.txt" 2>&1 || true
  docker build -q -t expb-textindex "$HERE/textindex" >/dev/null
  docker volume create expB-official >/dev/null
  docker run --rm -v expB-official:/data alpine:3.24.3 sh -c 'rm -rf /data/ix-piece /data/ix-span'
  step "$OUT/build-piece.jsonl" ti build --store "$STORE" --out /data/ix-piece --variant piece
  step "$OUT/build-span.jsonl"  ti build --store "$STORE" --out /data/ix-span --variant span
  step "$OUT/queries.jsonl"     ti queries --index /data/ix-piece --store "$STORE" --out /data/queries.json
  docker run --rm -v expB-official:/data alpine:3.24.3 cat /data/queries.json > "$OUT/queries.json"
  step "$OUT/scan.jsonl"         ti scan --store "$STORE" --queries /data/queries.json
  step "$OUT/search-piece.jsonl" ti search --index /data/ix-piece --store "$STORE" --variant piece --queries /data/queries.json --runs 5
  step "$OUT/search-span.jsonl"  ti search --index /data/ix-span --store "$STORE" --variant span --queries /data/queries.json --runs 5
  docker run --rm -v expB-official:/data alpine:3.24.3 sh -c 'du -sk /data/ix-piece /data/ix-span' > "$OUT/du.txt"
  ;;
clickhouse)
  cd "$HERE/../expA/chbaseline"
  REF="$OUT/scan.jsonl"   # rows and traces per query from our brute-force scan
  echo "clickhouse untuned" >&2
  ./search.sh content untuned --queries "$OUT/queries.json" --runs 3 --reference "$REF" > "$OUT/ch-untuned.jsonl" 2> "$OUT/ch-untuned.stderr" ||
    echo "ClickHouse untuned failed, see $OUT/ch-untuned.stderr" >&2
  # text index with phrase positions (experimental in this server version)
  ./search.sh content drop >/dev/null 2>&1 || true
  echo "clickhouse tuned, positions on" >&2
  ./search.sh content tuned --queries "$OUT/queries.json" --runs 5 --reference "$REF" > "$OUT/ch-tuned-pos.jsonl" 2> "$OUT/ch-tuned-pos.stderr" ||
    echo "ClickHouse tuned (positions) failed, see $OUT/ch-tuned-pos.stderr" >&2
  # text index without positions (the generally available configuration)
  ./search.sh content drop >/dev/null 2>&1 || true
  echo "clickhouse tuned, positions off" >&2
  PHRASE_POSITIONS=0 ./search.sh content tuned --queries "$OUT/queries.json" --runs 5 --reference "$REF" > "$OUT/ch-tuned-nopos.jsonl" 2> "$OUT/ch-tuned-nopos.stderr" ||
    echo "ClickHouse tuned (no positions) failed, see $OUT/ch-tuned-nopos.stderr" >&2
  ./search.sh content drop >/dev/null 2>&1 || true
  docker stop expA-ch-content-clickhouse >/dev/null 2>&1 || true
  ;;
esac
echo done >&2
