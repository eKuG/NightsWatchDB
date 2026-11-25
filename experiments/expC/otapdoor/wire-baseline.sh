#!/bin/sh
# Plain-OTLP baseline for the byte comparison: the same collector (otlp exporter, zstd, same batching) sends the
# same generator data (same flags and seed) to a discarding collector. Records the sending collector's network
# transmit bytes and CPU, the numbers to set against the OTel-Arrow run of the same data.
#   wire-baseline.sh NAME "TRACEGEN FLAGS"
set -eu
NAME=$1; TG=$2
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-/tmp/nightwatchdb-expC-otap}
CORPUS=$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus
docker rm -f expc-otap-sink expc-otap-collector >/dev/null 2>&1 || true
docker run -d --name expc-otap-sink --network expc-otap-net -v "$HERE/collector/sink.yaml:/etc/otelcol/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.165.0 --config /etc/otelcol/config.yaml >/dev/null
"$HERE/run-collector.sh" otlp expc-otap-sink:4317 >/dev/null
sleep 3
net() { docker run --rm --network container:expc-otap-collector --entrypoint cat alpine:3.24.3 /proc/net/dev | grep eth0; }
net > "$OUT/$NAME-otlp-net0.txt"; curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-otlp-col0.txt"
# shellcheck disable=SC2086
docker run --rm --network expc-otap-net -v $CORPUS:/corpus:ro expc-otap-tracegen:dev --corpus /corpus --otlp-endpoint expc-otap-collector:4317 $TG > "$OUT/$NAME-otlp-gen.json"
sleep 8
net > "$OUT/$NAME-otlp-net1.txt"; curl -s 127.0.0.1:18888/metrics > "$OUT/$NAME-otlp-col1.txt"
docker rm -f expc-otap-sink expc-otap-collector >/dev/null
echo done "$NAME"
