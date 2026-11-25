#!/bin/sh
# Runs the same tasks for both bake-off implementations under identical
# container limits and collects their JSON lines. See SPEC.md.
#
# usage: ./run.sh [input size, default 8GB] [cpus, default 2] [memory, default 4g]
#
# Data lives in the Docker volume `bakeoff-data`. Nothing large is written to
# the host: bind-mounting big host files into Docker Desktop keeps deleted
# files open and their disk space is not returned until Docker restarts.
set -eu

SIZE=${1:-8GB}
CPUS=${2:-2}
MEM=${3:-4g}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CORPUS="$ROOT/.data/corpus"
OUT="$HERE/results/cpu${CPUS}-mem${MEM}-${SIZE}"
VOL=bakeoff-data
mkdir -p "$OUT"

docker build -q -t bakeoff-go "$HERE/go" >/dev/null
docker build -q -t bakeoff-rust "$HERE/rust" >/dev/null
docker build -q -t bakeoff-tracegen -f "$HERE/tracegen.Dockerfile" "$ROOT/bench/tracegen" >/dev/null

docker volume create "$VOL" >/dev/null
if ! docker run --rm -v "$VOL":/data alpine:3.20 test -f "/data/in-$SIZE.bin"; then
  docker run --rm -v "$VOL":/data -v "$CORPUS":/corpus:ro bakeoff-tracegen \
    --corpus /corpus --bytes "$SIZE" --seed 1 --out-file "/data/in-$SIZE.bin" > "$OUT/input-summary.json"
fi
IN="/data/in-$SIZE.bin"

for lang in go rust; do
  log="$OUT/$lang.jsonl"
  : > "$log"
  d="/data/$lang-$SIZE"
  run() { docker run --rm --cpus "$CPUS" --memory "$MEM" -v "$VOL":/data "bakeoff-$lang" "$@"; }
  step() { # name, then the command; a failure (for example an out-of-memory kill) is recorded, not fatal
    name=$1; shift
    echo "[$lang] $name" >&2
    if run "$@" >> "$log" 2>"$OUT/$lang-$name.stderr"; then :; else
      rc=$?  # 137 means the container was killed for exceeding its memory limit
      echo "{\"lang\":\"$lang\",\"task\":\"$name\",\"failed\":true,\"exit\":$rc}" >> "$log"
    fi
  }
  docker run --rm -v "$VOL":/data alpine:3.20 sh -c "rm -rf $d && cat $IN > /dev/null"
  step decode-1 convert --in "$IN" --out "$d" --stage decode
  step decode-2 convert --in "$IN" --out "$d" --stage decode
  step convert-1 convert --in "$IN" --out "$d"
  step convert-2 convert --in "$IN" --out "$d"
  step agg agg --dir "$d" --runs 5
  step sample-ids sample-ids --dir "$d" --n 200 --out "$d/ids.txt"
  step trace trace --dir "$d" --ids "$d/ids.txt"
  step index index --dir "$d" --out "$d/index.bin"
  step mixed mixed --in "$IN" --dir "$d" --ids "$d/ids.txt" --seconds 60
done

# Files both sides must agree on, byte for byte.
docker run --rm -v "$VOL":/data alpine:3.20 sh -c \
  "sha256sum /data/go-$SIZE/ids.txt /data/rust-$SIZE/ids.txt /data/go-$SIZE/index.bin /data/rust-$SIZE/index.bin; ls -l /data/go-$SIZE /data/rust-$SIZE" \
  > "$OUT/files.txt" 2>&1 || true

python3 -I "$HERE/compare.py" "$OUT"
