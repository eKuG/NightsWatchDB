#!/bin/sh
# Official run for experiment C: both ingest doors, full-size datasets,
# receiver alone at fixed container limits.
#
# usage: [CPUS=2 MEM=4g] ./official.sh
#
# Senders stream over a private Docker network; nothing generated is stored
# except each run's write-ahead log, in a volume that is removed afterwards.
# Only small JSON and text files reach the host (results/).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CPUS=${CPUS:-2}; MEM=${MEM:-4g}
OUT="$HERE/results/cpu${CPUS}-mem${MEM}"
NET=expC-official-net
COL=expC-official-collector
DOOR=expC-official-door
WALVOL=expC-official-wal
COLIMG=otel/opentelemetry-collector-contrib:0.165.0
D1="--bytes 8GB --seed 1"
D2="--no-content --spans 10000000 --seed 1"
mkdir -p "$OUT"

echo "unit tests in containers" >&2
docker build -q --target test -t expc-otlp-test "$HERE/otlpdoor" > "$OUT/unit-otlpdoor.txt" 2>&1 && echo PASS >> "$OUT/unit-otlpdoor.txt" || echo FAIL >> "$OUT/unit-otlpdoor.txt"
docker build -q --target test -t expc-otap-test "$HERE/otapdoor" > "$OUT/unit-otapdoor.txt" 2>&1 && echo PASS >> "$OUT/unit-otapdoor.txt" || echo FAIL >> "$OUT/unit-otapdoor.txt"
docker build -q -t expc-otlp-door "$HERE/otlpdoor" >/dev/null || exit 1
docker build -q -t expc-otap-door "$HERE/otapdoor" >/dev/null || exit 1
docker build -q -t expa-tracegen -f "$ROOT/experiments/bakeoff/tracegen.Dockerfile" "$ROOT/bench/tracegen" >/dev/null || exit 1
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null

cleanup() { docker rm -f "$DOOR" "$COL" >/dev/null 2>&1; docker volume rm "$WALVOL" >/dev/null 2>&1; }

start_door() { # image, then door arguments
  img=$1; shift
  cleanup
  docker volume create "$WALVOL" >/dev/null
  docker run -d --name "$DOOR" --network "$NET" --cpus "$CPUS" --memory "$MEM" -v "$WALVOL":/wal "$img" \
    serve --listen 0.0.0.0:4317 --wal /wal --exit-after-idle-s 20 "$@" >/dev/null
  sleep 2
}

finish_door() { # run name
  docker wait "$DOOR" > "$OUT/$1.exit" 2>/dev/null
  echo "oom=$(docker inspect -f '{{.State.OOMKilled}}' "$DOOR" 2>/dev/null)" >> "$OUT/$1.exit"
  docker logs "$DOOR" 2> "$OUT/$1.door.stderr" | grep '^{' | tail -1 > "$OUT/$1.door.json"
  docker run --rm -v "$WALVOL":/wal alpine:3.24.3 sh -c 'du -sk /wal | cut -f1' > "$OUT/$1.wal-kb.txt" 2>/dev/null
  cleanup
}

gen() { # run name, target host:port, generator flags
  # shellcheck disable=SC2086
  docker run --rm --network "$NET" -v "$ROOT/.data/corpus":/corpus:ro expa-tracegen \
    --corpus /corpus --otlp-endpoint "$2" $3 > "$OUT/$1.gen.json" 2> "$OUT/$1.gen.stderr" || echo "sender failed: $1" >&2
}

col_cpu() { curl -s 127.0.0.1:18889/metrics | awk '/^otelcol_process_cpu_seconds/ {print $NF}' | tail -1; }
col_tx() { docker run --rm --network "container:$COL" --entrypoint cat alpine:3.24.3 /proc/net/dev | awk '/eth0/ {print $10}'; }
col_queue() { curl -s 127.0.0.1:18889/metrics | awk '/^otelcol_exporter_queue_size/ {s+=$NF} END {printf "%d", s}'; }

direct() { # run name, decode mode, generator flags
  echo "direct OTLP: $1" >&2
  start_door expc-otlp-door --decode "$2"
  gen "$1" "$DOOR:4317" "$3"
  finish_door "$1"
}

via_collector() { # run name, collector config (otlp|otelarrow), door image, decode mode, generator flags
  echo "via collector ($2): $1" >&2
  start_door "$3" --decode "$4"
  docker run -d --name "$COL" --network "$NET" -p 127.0.0.1:18889:8888 \
    -e DOOR_ENDPOINT="$DOOR:4317" -e MAX_STREAM_LIFETIME=30s -e DISABLE_DOWNGRADE=true \
    -v "$HERE/otapdoor/collector/$2.yaml":/etc/otelcol/config.yaml:ro "$COLIMG" --config /etc/otelcol/config.yaml >/dev/null
  sleep 4
  c0=$(col_cpu); t0=$(col_tx)
  gen "$1" "$COL:4317" "$5"
  i=0; while [ "$(col_queue)" != 0 ] && [ $i -lt 120 ]; do sleep 2; i=$((i + 1)); done
  sleep 7   # last batch flush; read counters while the door is still up
  c1=$(col_cpu); t1=$(col_tx)
  curl -s 127.0.0.1:18889/metrics | grep -E '^otelcol_exporter_(sent|send_failed|enqueue_failed)' > "$OUT/$1.collector-metrics.txt"
  echo "{\"collector_cpu_s\": $(echo "$c1 - $c0" | bc -l), \"collector_tx_bytes\": $((t1 - t0))}" > "$OUT/$1.collector.json"
  docker logs "$COL" 2>&1 | grep -c -i -E 'error|warn' > "$OUT/$1.collector-warnings.txt"
  finish_door "$1"
}

direct d1-otlp-lazy lazy "$D1"
direct d1-otlp-full full "$D1"
direct d1-otlp-none none "$D1"
direct d2-otlp-lazy lazy "$D2"
direct d2-otlp-full full "$D2"
via_collector d1-col-otlp      otlp      expc-otlp-door lazy "$D1"
via_collector d1-col-otelarrow otelarrow expc-otap-door full "$D1"
via_collector d2-col-otlp      otlp      expc-otlp-door lazy "$D2"
via_collector d2-col-otelarrow otelarrow expc-otap-door full "$D2"

cleanup
docker network rm "$NET" >/dev/null 2>&1
echo done >&2
