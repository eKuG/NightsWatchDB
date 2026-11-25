#!/bin/sh
# One Blitz run: Blitz (OTLP gRPC out) -> collector (otelarrow exporter) -> otapdoor. Logs or metrics, chosen by the
# Blitz flags. Records the door's JSON and Blitz's own output.
#   blitz-run.sh NAME "BLITZ FLAGS" [MAX_STREAM_LIFETIME] [DOOR FLAGS...]
set -eu
NAME=$1; BZ=$2; LIFE=${3:-30s}; shift 3 || shift $#
DOORFLAGS=${*:---decode full --wal /wal --exit-after-idle-s 12}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-/tmp/nightwatchdb-expC-otap}
mkdir -p "$OUT"
for v in expC-otap-data expC-otap-wal; do docker volume create $v >/dev/null; done
docker rm -f expc-otap-door expc-otap-collector >/dev/null 2>&1 || true
docker run --rm -v expC-otap-wal:/wal -v expC-otap-data:/data --entrypoint sh expc-otap-door:dev -c 'rm -rf /wal/* /data/'"$NAME"'-*'
docker run -d --name expc-otap-door --network expc-otap-net --cpus 2 --memory 4g -v expC-otap-wal:/wal -v expC-otap-data:/data \
  expc-otap-door:dev serve --listen 0.0.0.0:4317 --stats-file /data/$NAME-door.json --exit-after-idle-s 20 $DOORFLAGS >/dev/null
"$HERE/run-collector.sh" otelarrow expc-otap-door:4317 "$LIFE" >/dev/null
sleep 3
net() { docker run --rm --network container:expc-otap-collector --entrypoint cat alpine:3.24.3 /proc/net/dev | grep eth0; }
net > "$OUT/$NAME-net0.txt"; curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-col0.txt" || true
# shellcheck disable=SC2086
docker run --rm --name expc-otap-blitz --network expc-otap-net expc-otap-blitz:dev $BZ \
  --output-type otlp-grpc --output-otlp-grpc-host expc-otap-collector --output-otlp-grpc-port 4317 > "$OUT/$NAME-blitz.log" 2>&1 || true
sleep 7
net > "$OUT/$NAME-net1.txt"; curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-col1.txt" || true
until [ "$(docker inspect -f '{{.State.Running}}' expc-otap-door)" = false ]; do sleep 2; done
docker logs expc-otap-collector > "$OUT/$NAME-collector.log" 2>&1 || true
docker logs expc-otap-door > "$OUT/$NAME-door.log" 2>&1 || true
docker run --rm -v expC-otap-data:/data alpine:3.24.3 cat /data/$NAME-door.json > "$OUT/$NAME-door.json"
echo done "$NAME"
