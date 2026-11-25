#!/bin/sh
# Shows what the exporter does when the receiver does not serve OTel-Arrow (a collector's OTLP receiver stands in).
# With disable_downgrade=true nothing may reach the receiver; with false the exporter silently switches to plain OTLP.
#   fallback-test.sh true|false
set -eu
DD=$1
HERE=$(cd "$(dirname "$0")" && pwd)
CORPUS=$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus
docker rm -f expc-otap-sink expc-otap-collector >/dev/null 2>&1 || true
docker run -d --name expc-otap-sink --network expc-otap-net -v "$HERE/collector/sink.yaml:/etc/otelcol/config.yaml:ro" otel/opentelemetry-collector-contrib:0.165.0 --config /etc/otelcol/config.yaml >/dev/null
DISABLE_DOWNGRADE=$DD "$HERE/run-collector.sh" otelarrow expc-otap-sink:4317 >/dev/null
sleep 3
docker run --rm --network expc-otap-net -v $CORPUS:/corpus:ro expc-otap-tracegen:dev --corpus /corpus --spans 300 --otlp-endpoint expc-otap-collector:4317 | tail -1
sleep 8
echo "disable_downgrade=$DD: collector transmit bytes:"; docker run --rm --network container:expc-otap-collector --entrypoint cat alpine:3.24.3 /proc/net/dev | grep eth0 | awk '{print $10}'
curl -s 127.0.0.1:18888/metrics | grep -E '^otelcol_exporter_(sent_spans|send_failed_spans)' | cut -c1-120
docker logs expc-otap-collector 2>&1 | grep -iE "arrow is not supported|downgrad" | head -2 | cut -c1-200
docker rm -f expc-otap-sink expc-otap-collector >/dev/null
