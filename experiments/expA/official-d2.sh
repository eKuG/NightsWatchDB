#!/bin/sh
# Official run for the narrow dataset (D2): three span layouts and the
# ClickHouse baseline, one thing at a time under the same container limits.
#
# usage: [CPUS=2 MEM=4g SPANS=10000000] ./official-d2.sh prep|measure
#   prep     generate data, build the three layouts, load ClickHouse
#   measure  timed queries, cold then warm, for each layout and for ClickHouse
#
# All data stays in Docker volumes. Only small JSON files reach the host.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CPUS=${CPUS:-2}; MEM=${MEM:-4g}; SPANS=${SPANS:-10000000}
export CPUS MEM
OUT="$HERE/results/d2-cpu${CPUS}-mem${MEM}"
VOL=expA-official
CH=expA-ch-narrow-clickhouse
mkdir -p "$OUT"

lay() { docker run --rm --cpus "$CPUS" --memory "$MEM" -v "$VOL":/data expa-layout "$@"; }
field() { python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["params"][sys.argv[2]])' "$1" "$2"; }
drop_cache() { docker run --rm --privileged --pid=host alpine:3.24.3 sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; }

case "${1:?prep or measure}" in
prep)
  docker build -q -t expa-layout "$HERE/layout" >/dev/null
  docker build -q -t expa-tracegen -f "$ROOT/experiments/bakeoff/tracegen.Dockerfile" "$ROOT/bench/tracegen" >/dev/null
  docker volume create "$VOL" >/dev/null
  if ! docker run --rm -v "$VOL":/data alpine:3.24.3 test -f /data/d2.bin; then
    echo "generate $SPANS narrow spans" >&2
    docker run --rm -v "$VOL":/data -v "$ROOT/.data/corpus":/corpus:ro expa-tracegen \
      --corpus /corpus --no-content --spans "$SPANS" --seed 1 --out-file /data/d2.bin > "$OUT/input-summary.json"
  fi
  for v in cotrace svc traceid; do
    echo "build $v" >&2
    docker run --rm -v "$VOL":/data alpine:3.24.3 rm -rf "/data/d2-$v"
    lay build --in /data/d2.bin --out "/data/d2-$v" --variant "$v" > "$OUT/build-$v.json" 2> "$OUT/build-$v.stderr" ||
      echo "{\"task\":\"build\",\"variant\":\"$v\",\"failed\":true,\"exit\":$?}" > "$OUT/build-$v.json"
  done
  lay params --dir /data/d2-cotrace --variant cotrace > "$OUT/params.json"
  lay sample-ids --dir /data/d2-cotrace --variant cotrace --n 500 --out /data/d2-ids.txt > "$OUT/sample-ids.json"
  docker run --rm -v "$VOL":/data alpine:3.24.3 cat /data/d2-ids.txt > "$OUT/ids.txt"
  docker run --rm -v "$VOL":/data alpine:3.24.3 sh -c 'du -sk /data/d2-* /data/d2.bin' > "$OUT/du.txt"

  echo "load ClickHouse" >&2
  ( cd "$HERE/chbaseline" && GEN_ARGS="--no-content --spans $SPANS --seed 1" ./load.sh narrow > "$OUT/ch-load.json" 2> "$OUT/ch-load.stderr" ) ||
    echo "ClickHouse load FAILED, see $OUT/ch-load.stderr" >&2
  ( cd "$HERE/chbaseline" && ./sizes.sh narrow > "$OUT/ch-sizes.json" && ./query.sh narrow params > "$OUT/ch-params.json" &&
    ./query.sh narrow sample-ids 500 "$OUT/ch-ids.txt" > "$OUT/ch-sample-ids.json" ) || echo "ClickHouse post-load step failed" >&2
  cmp -s "$OUT/ids.txt" "$OUT/ch-ids.txt" && echo "sampled ids identical on both sides" >&2 || echo "WARNING: sampled ids differ" >&2
  ;;
measure)
  S=$(field "$OUT/params.json" service); M=$(field "$OUT/params.json" model)
  F=$(field "$OUT/params.json" from_ns); T=$(field "$OUT/params.json" to_ns)
  docker stop "$CH" >/dev/null 2>&1 || true   # nothing else running while the layouts are timed
  for v in cotrace svc traceid; do
    for q in open list1 list2 agg; do
      case $q in open) args="--ids /data/d2-ids.txt" ;; *) args="--runs 20" ;; esac
      for temp in cold warm; do
        [ $temp = cold ] && drop_cache
        echo "$v $q $temp" >&2
        # shellcheck disable=SC2086
        lay "$q" --dir "/data/d2-$v" --variant "$v" $args > "$OUT/$temp-$v-$q.json" 2> "$OUT/$temp-$v-$q.stderr" ||
          echo "{\"task\":\"$q\",\"variant\":\"$v\",\"failed\":true,\"exit\":$?}" > "$OUT/$temp-$v-$q.json"
      done
    done
  done
  cd "$HERE/chbaseline"
  echo "ClickHouse cold" >&2
  ./cold.sh narrow > "$OUT/ch-cold-drop.txt" 2>&1
  ./query.sh narrow run --service "$S" --model "$M" --from-ns "$F" --to-ns "$T" --ids "$OUT/ids.txt" --runs 20 > "$OUT/ch-cold.jsonl" 2> "$OUT/ch-cold.stderr" || echo "ClickHouse cold run failed" >&2
  echo "ClickHouse warm" >&2
  ./query.sh narrow run --service "$S" --model "$M" --from-ns "$F" --to-ns "$T" --ids "$OUT/ids.txt" --runs 20 --check > "$OUT/ch-warm.jsonl" 2> "$OUT/ch-warm.stderr" || echo "ClickHouse warm run failed" >&2
  ;;
esac
echo done >&2
