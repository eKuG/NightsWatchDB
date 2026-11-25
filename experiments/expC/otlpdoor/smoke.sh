#!/bin/bash
# Usage: smoke.sh OUTDIR DATASET(content|small) DECODE(full|lazy|none) WAL(on|off) [extra receiver args...]
# One receiver container (2 CPUs, 4 GB) on a private network, one tracegen sender, a fresh WAL volume per run.
set -euo pipefail
OUT=$1; DS=$2; MODE=$3; WAL=$4; shift 4
CORPUS=$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus
NAME=expc-otlp-recv-$DS-$MODE-$WAL
VOL=expc-otlp-wal-$DS-$MODE
case $DS in
  content) GEN=(--bytes 1GB) ;;
  small)   GEN=(--no-content --spans 500000) ;;
esac
mkdir -p "$OUT"
docker rm -f "$NAME" >/dev/null 2>&1 || true
VOLARGS=(); WALARG=none
if [ "$WAL" = on ]; then
  docker volume create "$VOL" >/dev/null
  VOLARGS=(-v "$VOL:/wal"); WALARG=/wal
fi
docker run -d --name "$NAME" --network expc-otlp-net --cpus 2 --memory 4g ${VOLARGS[@]+"${VOLARGS[@]}"} expc-otlp-door:latest \
  serve --listen 0.0.0.0:4317 --wal "$WALARG" --decode "$MODE" --exit-after-idle-s 2 "$@" >/dev/null
for i in $(seq 50); do docker logs "$NAME" 2>&1 | grep -q listening && break; sleep 0.2; done
docker run --rm --network expc-otlp-net -v "$CORPUS:/corpus:ro" expc-otlp-tracegen:latest \
  --corpus /corpus "${GEN[@]}" --otlp-endpoint "$NAME:4317" > "$OUT/$DS-$MODE-$WAL.gen.json" 2> "$OUT/$DS-$MODE-$WAL.gen.err" || echo "sender failed (see $OUT/$DS-$MODE-$WAL.gen.err)"
docker wait "$NAME" >/dev/null
docker logs "$NAME" 2>"$OUT/$DS-$MODE-$WAL.recv.err" | grep '^{' > "$OUT/$DS-$MODE-$WAL.stats.json"
docker rm "$NAME" >/dev/null
[ "$WAL" = on ] && docker volume rm "$VOL" >/dev/null
cat "$OUT/$DS-$MODE-$WAL.stats.json"
