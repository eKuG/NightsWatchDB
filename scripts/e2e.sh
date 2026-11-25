#!/bin/sh
# End-to-end check of the container image: load generated traces over OTLP
# gRPC, then check the data through the HTTP API after a normal run, after
# the server is killed with SIGKILL and after a clean stop with SIGTERM.
# Everything it creates (containers, network, volume) is removed on exit; the
# two images stay. See scripts/README.md.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SERVER_IMAGE=nightswatchdb:e2e
GEN_IMAGE=nwdb-tracegen:e2e
NAME=nwdb-e2e-$$
SERVER=$NAME-server
TRACES=${E2E_TRACES:-3000}
OUT=$(mktemp -d)
# Every span of the run starts inside this range (nanoseconds).
RANGE='from=0&to=9000000000000000000'

cleanup() {
  docker rm -f "$SERVER" "$NAME-gen" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  docker volume rm "$NAME" >/dev/null 2>&1 || true
  rm -rf "$OUT"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() {
  echo "FAIL: $*" >&2
  docker logs "$SERVER" 2>&1 | tail -n 20 >&2 || true
  exit 1
}

start_server() {
  docker run -d --name "$SERVER" --network "$NAME" --cpus 2 --memory 1g \
    -v "$NAME":/var/lib/nightswatchdb -p 127.0.0.1::4320 "$SERVER_IMAGE" >/dev/null
  API=$(docker port "$SERVER" 4320/tcp | head -n 1)
  i=0
  until curl -fsS "http://$API/healthz" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -le 120 ] || fail "the server did not become ready"
    sleep 0.5
  done
}

# Compares the API with what the generator sent.
check() {
  label=$1
  tokens=$(curl -fsS "http://$API/api/v1/tokens?$RANGE")
  got=$(printf '%s' "$tokens" | grep -o '"count":[0-9]*' | cut -d: -f2 | awk '{s += $1} END {print s + 0}')
  [ "$got" = "$SPANS" ] || fail "$label: the API counts $got spans, the generator sent $SPANS"

  list=$(curl -fsS "http://$API/api/v1/traces?$RANGE&limit=5")
  ids=$(printf '%s' "$list" | grep -o '"trace_id":"[0-9a-f]*"' | cut -d'"' -f4)
  counts=$(printf '%s' "$list" | grep -o '"span_count":[0-9]*' | cut -d: -f2)
  [ -n "$ids" ] || fail "$label: no traces listed"
  n=0
  for id in $ids; do
    n=$((n + 1))
    want=$(printf '%s\n' "$counts" | sed -n "${n}p")
    have=$(curl -fsS "http://$API/api/v1/traces/$id" | grep -o '"span_id":"' | wc -l | tr -d ' ')
    [ "$have" = "$want" ] || fail "$label: trace $id has $have spans, its summary says $want"
  done
  echo "ok: $label: $got spans, $n traces opened"
}

echo "== building images"
docker build -q -t "$SERVER_IMAGE" "$ROOT" >/dev/null
docker build -q -t "$GEN_IMAGE" "$ROOT/bench/tracegen" >/dev/null
docker network create "$NAME" >/dev/null
docker volume create "$NAME" >/dev/null

echo "== start and load $TRACES traces (half uncompressed, half gzip)"
start_server
# The generator needs some text to slice. A small one is made inside its own
# container. The two halves use different seeds, so no trace is sent twice.
HALF=$((TRACES / 2))
docker run --rm --name "$NAME-gen" --network "$NAME" --entrypoint sh "$GEN_IMAGE" -c "
  mkdir /tmp/corpus
  base64 /dev/urandom | head -c 400000 > /tmp/corpus/text.txt
  for half in 'none 7' 'gzip 8'; do
    set -- \$half
    tracegen --corpus /tmp/corpus --traces $HALF --seed \$2 --window 1h \
      --otlp-endpoint $SERVER:4317 --compression \$1 || exit 1
  done
" > "$OUT/gen.json"
SPANS=$(grep '^  "spans":' "$OUT/gen.json" | tr -dc '0-9\n' | awk '{s += $1} END {print s + 0}')
[ "$(grep -c '^  "spans":' "$OUT/gen.json")" = 2 ] || fail "expected two generator summaries"
echo "generator sent $SPANS spans"
check "after load"

echo "== kill -9 and restart"
docker kill -s KILL "$SERVER" >/dev/null
docker rm "$SERVER" >/dev/null
start_server
check "after SIGKILL"

echo "== SIGTERM and restart"
docker stop "$SERVER" >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")
[ "$code" = 0 ] || fail "the server exited with code $code after SIGTERM, expected 0"
docker rm "$SERVER" >/dev/null
start_server
check "after SIGTERM"
parts=$(curl -fsS "http://$API/api/v1/stats" | grep -o '"parts":[0-9]*' | cut -d: -f2)
[ "${parts:-0}" -gt 0 ] || fail "no parts after a clean shutdown"
echo "ok: $parts parts after the clean shutdown"

echo "PASS: end-to-end ($SPANS spans survived kill -9 and a clean stop)"
