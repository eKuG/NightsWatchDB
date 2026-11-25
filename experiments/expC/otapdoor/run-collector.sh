#!/bin/sh
# Starts the collector in the background on the private network and prints its container name.
#   run-collector.sh otelarrow|otlp DOOR_HOST:PORT [MAX_STREAM_LIFETIME]
# Container: expc-otap-collector (OTLP in on 4317 inside the network); its Prometheus metrics are published on
# 127.0.0.1:18888. Only the small config file is bind-mounted.
#
# Reading the numbers (Prometheus text at http://127.0.0.1:18888/metrics, scrape before and after the run):
#   collector CPU      otelcol_process_cpu_seconds (cumulative; use the difference; scrape while the receiver is still
#                      up, because an exporter that cannot reach its receiver retries in a tight loop and burns CPU)
#   otelarrow exporter otelcol_exporter_sent (OTLP-sized bytes) and otelcol_exporter_sent_wire (bytes on the wire, after
#                      gRPC compression), per method label
#   otlp exporter      has no byte counters; read the container's transmit bytes instead:
#                      docker run --rm --network container:expc-otap-collector --entrypoint cat alpine:3.24.3 /proc/net/dev
#                      (eth0, 10th column; this works for both exporters and includes HTTP/2 framing)
# Overrides (environment): DISABLE_DOWNGRADE (default true), GRPC_COMPRESSION, PAYLOAD_COMPRESSION, BATCH_MIN_BYTES,
# BATCH_MAX_BYTES (otelarrow.yaml only).
set -eu
MODE=${1:?otelarrow or otlp}
DOOR=${2:?door host:port}
LIFE=${3:-30s}
HERE=$(cd "$(dirname "$0")" && pwd)
docker rm -f expc-otap-collector >/dev/null 2>&1 || true
docker run -d --name expc-otap-collector --network expc-otap-net -p 127.0.0.1:18888:8888 \
  -e DOOR_ENDPOINT="$DOOR" -e MAX_STREAM_LIFETIME="$LIFE" -e DISABLE_DOWNGRADE="${DISABLE_DOWNGRADE:-true}" \
  ${GRPC_COMPRESSION:+-e GRPC_COMPRESSION=$GRPC_COMPRESSION} ${PAYLOAD_COMPRESSION:+-e PAYLOAD_COMPRESSION=$PAYLOAD_COMPRESSION} \
  ${BATCH_MIN_BYTES:+-e BATCH_MIN_BYTES=$BATCH_MIN_BYTES} ${BATCH_MAX_BYTES:+-e BATCH_MAX_BYTES=$BATCH_MAX_BYTES} \
  -v "$HERE/collector/$MODE.yaml:/etc/otelcol/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.165.0 --config /etc/otelcol/config.yaml
