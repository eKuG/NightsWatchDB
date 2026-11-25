#!/bin/bash
# Usage: smoke-par.sh OUTDIR DECODE WAL(on|off): eight concurrent tracegen senders (seeds 101..108, 128 MB each) into one receiver.
set -euo pipefail
OUT=$1; MODE=$2; WAL=$3
CORPUS=$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus
NAME=expc-otlp-recv-par-$MODE-$WAL; VOL=expc-otlp-wal-par
mkdir -p "$OUT"; docker rm -f "$NAME" >/dev/null 2>&1 || true
VOLARGS=(); WALARG=none
if [ "$WAL" = on ]; then docker volume create "$VOL" >/dev/null; VOLARGS=(-v "$VOL:/wal"); WALARG=/wal; fi
docker run -d --name "$NAME" --network expc-otlp-net --cpus 2 --memory 4g ${VOLARGS[@]+"${VOLARGS[@]}"} expc-otlp-door:latest \
  serve --listen 0.0.0.0:4317 --wal "$WALARG" --decode "$MODE" --exit-after-idle-s 2 >/dev/null
for i in $(seq 50); do docker logs "$NAME" 2>&1 | grep -q listening && break; sleep 0.2; done
for n in 1 2 3 4 5 6 7 8; do
  docker run --rm --network expc-otlp-net -v "$CORPUS:/corpus:ro" expc-otlp-tracegen:latest --corpus /corpus --bytes 128MB --seed $((100+n)) --otlp-endpoint "$NAME:4317" > "$OUT/par-$MODE-$WAL.gen$n.json" 2>"$OUT/par-$MODE-$WAL.gen$n.err" &
done
wait
docker wait "$NAME" >/dev/null
docker logs "$NAME" 2>/dev/null | grep '^{' > "$OUT/par-$MODE-$WAL.stats.json"
docker rm "$NAME" >/dev/null
[ "$WAL" = on ] && docker volume rm "$VOL" >/dev/null
cat "$OUT/par-$MODE-$WAL.stats.json"
