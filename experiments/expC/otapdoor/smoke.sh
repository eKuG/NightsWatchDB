#!/bin/sh
# One smoke run: tracegen -> collector (OTLP in, otelarrow out) -> otapdoor, all on the private network.
#   smoke.sh NAME "TRACEGEN FLAGS" [MAX_STREAM_LIFETIME] [DOOR FLAGS...]
# e.g. smoke.sh content "--bytes 1GB" 30s --decode full --wal /wal
# Generated data stays in the Docker volume expC-otap-data (the generator's own file, kept for the reference
# decode) and expC-otap-wal; nothing large touches the host. Small result files go to $OUT.
set -eu
NAME=$1; TG=$2; LIFE=${3:-30s}; shift 3 || shift $#
DOORFLAGS=${*:---decode full --wal /wal}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-/tmp/nightswatchdb-expC-otap}
CORPUS=$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus
mkdir -p "$OUT"
for v in expC-otap-data expC-otap-wal; do docker volume create $v >/dev/null; done
docker rm -f expc-otap-door expc-otap-collector >/dev/null 2>&1 || true
docker run --rm -v expC-otap-wal:/wal -v expC-otap-data:/data --entrypoint sh expc-otap-door:dev -c 'rm -rf /wal/* /data/'"$NAME"'-*'
docker run -d --name expc-otap-door --network expc-otap-net --cpus 2 --memory 4g -v expC-otap-wal:/wal -v expC-otap-data:/data \
  expc-otap-door:dev serve --listen 0.0.0.0:4317 --stats-file /data/$NAME-door.json --exit-after-idle-s 20 $DOORFLAGS >/dev/null
"$HERE/run-collector.sh" otelarrow expc-otap-door:4317 "$LIFE" >/dev/null
sleep 3
net() { docker run --rm --network container:expc-otap-collector --entrypoint cat alpine:3.24.3 /proc/net/dev | grep eth0; }
net > "$OUT/$NAME-net0.txt"
curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-col0.txt" || true
# shellcheck disable=SC2086
docker run --rm --name expc-otap-tracegen --network expc-otap-net -v $CORPUS:/corpus:ro -v expC-otap-data:/data expc-otap-tracegen:dev \
  --corpus /corpus --out-file /data/$NAME-gen.bin --otlp-endpoint expc-otap-collector:4317 $TG > "$OUT/$NAME-gen.json"
# The collector flushes its last batch after at most 5 s (flush_timeout). Read its counters after 7 s, while the
# door is still up: once the door has exited the exporter retries in a tight loop and burns CPU that is not part of the run.
sleep 7
net > "$OUT/$NAME-net1.txt"
curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-col1.txt" || true
# wait for the door to finish (idle exit)
until [ "$(docker inspect -f '{{.State.Running}}' expc-otap-door)" = false ]; do sleep 2; done
docker logs expc-otap-collector > "$OUT/$NAME-collector.log" 2>&1 || true
docker logs expc-otap-door > "$OUT/$NAME-door.log" 2>&1 || true
docker run --rm -v expC-otap-data:/data alpine:3.24.3 cat /data/$NAME-door.json > "$OUT/$NAME-door.json"
# reference numbers from the generator file, by the bake-off converter (decode only)
docker run --rm -v expC-otap-data:/data expc-otap-bake:dev convert --in /data/$NAME-gen.bin --out /tmp --stage decode > "$OUT/$NAME-bake.json"
echo done "$NAME"
