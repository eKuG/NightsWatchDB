#!/bin/sh
# Official run for the content dataset (D1): four payload stores, the chosen
# span layout, and the ClickHouse baseline, one thing at a time under the same
# container limits.
#
# usage: [CPUS=2 MEM=4g SIZE=8GB] ./official-d1.sh prep|measure
#
# All data stays in Docker volumes. Only small JSON files reach the host.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CPUS=${CPUS:-2}; MEM=${MEM:-4g}; SIZE=${SIZE:-8GB}
export CPUS MEM
OUT="$HERE/results/d1-cpu${CPUS}-mem${MEM}"
VOL=expA-official
CH=expA-ch-content-clickhouse
VARIANTS="parquet parquet-tuned zstdlong dedup"
mkdir -p "$OUT"

pay() { docker run --rm --cpus "$CPUS" --memory "$MEM" -v "$VOL":/data expa-payload "$@"; }
lay() { docker run --rm --cpus "$CPUS" --memory "$MEM" -v "$VOL":/data expa-layout "$@"; }
field() { python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["params"][sys.argv[2]])' "$1" "$2"; }
drop_cache() { docker run --rm --privileged --pid=host alpine:3.24.3 sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; }
step() { # output file, then the command; a failure is recorded, not fatal
  f=$1; shift
  if "$@" > "$f" 2> "${f%.json}.stderr"; then :; else
    rc=$?  # 137 means killed for exceeding the memory limit
    echo "{\"failed\":true,\"exit\":$rc,\"cmd\":\"$*\"}" > "$f"
  fi
}

case "${1:?prep or measure}" in
prep)
  docker build -q -t expa-payload "$HERE/payload" >/dev/null
  docker build -q -t expa-layout "$HERE/layout" >/dev/null
  docker build -q -t expa-tracegen -f "$ROOT/experiments/bakeoff/tracegen.Dockerfile" "$ROOT/bench/tracegen" >/dev/null
  docker volume create "$VOL" >/dev/null
  if ! docker run --rm -v "$VOL":/data alpine:3.24.3 test -f /data/d1.bin; then
    echo "generate $SIZE of content data" >&2
    docker run --rm -v "$VOL":/data -v "$ROOT/.data/corpus":/corpus:ro expa-tracegen \
      --corpus /corpus --bytes "$SIZE" --seed 1 --out-file /data/d1.bin > "$OUT/input-summary.json"
  fi
  for v in $VARIANTS; do
    echo "payload build $v" >&2
    docker run --rm -v "$VOL":/data alpine:3.24.3 rm -rf "/data/d1-pay-$v"
    step "$OUT/build-$v.json" pay build --in /data/d1.bin --out "/data/d1-pay-$v" --variant "$v"
  done
  echo "layout build cotrace" >&2
  docker run --rm -v "$VOL":/data alpine:3.24.3 rm -rf /data/d1-cotrace
  step "$OUT/build-cotrace.json" lay build --in /data/d1.bin --out /data/d1-cotrace --variant cotrace
  lay params --dir /data/d1-cotrace --variant cotrace > "$OUT/params.json"
  lay sample-ids --dir /data/d1-cotrace --variant cotrace --n 500 --out /data/d1-ids.txt > "$OUT/sample-ids.json"
  docker run --rm -v "$VOL":/data alpine:3.24.3 cat /data/d1-ids.txt > "$OUT/ids.txt"
  docker run --rm -v "$VOL":/data alpine:3.24.3 sh -c 'du -sk /data/d1-* /data/d1.bin' > "$OUT/du.txt"

  echo "load ClickHouse" >&2
  ( cd "$HERE/chbaseline" && GEN_ARGS="--bytes $SIZE --seed 1" ./load.sh content > "$OUT/ch-load.json" 2> "$OUT/ch-load.stderr" ) ||
    echo "ClickHouse load FAILED, see $OUT/ch-load.stderr" >&2
  ( cd "$HERE/chbaseline" && ./sizes.sh content > "$OUT/ch-sizes.json" && ./query.sh content params > "$OUT/ch-params.json" &&
    ./query.sh content sample-ids 500 "$OUT/ch-ids.txt" > "$OUT/ch-sample-ids.json" ) || echo "ClickHouse post-load step failed" >&2
  cmp -s "$OUT/ids.txt" "$OUT/ch-ids.txt" && echo "sampled ids identical on both sides" >&2 || echo "WARNING: sampled ids differ" >&2
  ;;
measure)
  S=$(field "$OUT/params.json" service); M=$(field "$OUT/params.json" model)
  F=$(field "$OUT/params.json" from_ns); T=$(field "$OUT/params.json" to_ns)
  docker stop "$CH" >/dev/null 2>&1 || true   # nothing else running while our side is timed
  for v in $VARIANTS; do
    for q in open span scan; do
      case $q in
        open) args="--ids /data/d1-ids.txt" ;;
        span) args="--n 500 --seed 1" ;;
        scan) args="" ;;
      esac
      for temp in cold warm; do
        [ $temp = cold ] && drop_cache
        echo "payload $v $q $temp" >&2
        # shellcheck disable=SC2086
        step "$OUT/$temp-pay-$v-$q.json" pay "$q" --dir "/data/d1-pay-$v" --variant "$v" $args
      done
    done
  done
  for q in open list1 list2 agg; do
    case $q in open) args="--ids /data/d1-ids.txt" ;; *) args="--runs 20" ;; esac
    for temp in cold warm; do
      [ $temp = cold ] && drop_cache
      echo "layout cotrace $q $temp" >&2
      # shellcheck disable=SC2086
      step "$OUT/$temp-cotrace-$q.json" lay "$q" --dir /data/d1-cotrace --variant cotrace $args
    done
  done
  cd "$HERE/chbaseline"
  echo "ClickHouse cold" >&2
  ./cold.sh content > "$OUT/ch-cold-drop.txt" 2>&1
  ./query.sh content run --service "$S" --model "$M" --from-ns "$F" --to-ns "$T" --ids "$OUT/ids.txt" --runs 20 > "$OUT/ch-cold.jsonl" 2> "$OUT/ch-cold.stderr" || echo "ClickHouse cold run failed" >&2
  echo "ClickHouse warm" >&2
  ./query.sh content run --service "$S" --model "$M" --from-ns "$F" --to-ns "$T" --ids "$OUT/ids.txt" --runs 20 --check > "$OUT/ch-warm.jsonl" 2> "$OUT/ch-warm.stderr" || echo "ClickHouse warm run failed" >&2
  ;;
esac
echo done >&2
