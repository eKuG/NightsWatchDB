#!/bin/sh
# End-to-end check of the container image: load generated traces over OTLP
# gRPC, then check the data through the HTTP API after a normal run, after
# the server is killed with SIGKILL and after a clean stop with SIGTERM. A
# word and a two-word phrase taken from the text of a stored trace must find
# that trace in a search, in each of the three states.
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

# Takes a word and a two-word phrase from the text of a stored trace, read
# through the API: the first run of base64 characters after a "content" key of
# a message (the generator's text is base64, and the API returns a message as
# an escaped JSON string). The first time it finds them it keeps them in
# SEARCH_ID, SEARCH_WORD and SEARCH_PHRASE, and later calls reuse them.
pick_search_text() {
  [ -z "${SEARCH_ID:-}" ] || return 0
  list=$(curl -fsS "http://$API/api/v1/traces?$RANGE&limit=50")
  for id in $(printf '%s' "$list" | grep -o '"trace_id":"[0-9a-f]*"' | cut -d'"' -f4); do
    run=$(curl -fsS "http://$API/api/v1/traces/$id" |
      grep -o 'content\\":\\"[A-Za-z0-9+/]\{40,\}' | head -n 1 | sed 's/.*\\"//')
    [ -n "$run" ] || continue
    tokens=$(printf '%s' "$run" | tr '+/' '\n\n')
    word=$(printf '%s\n' "$tokens" | awk 'length($0) >= 6 {print; exit}')
    phrase=$(printf '%s\n' "$tokens" |
      awk 'NF {n++; t[n] = $0; if (n == 2) {print t[1] "%20" t[2]; exit}} !NF {n = 0}')
    if [ -n "$word" ] && [ -n "$phrase" ]; then
      SEARCH_ID=$id SEARCH_WORD=$word SEARCH_PHRASE=$phrase
      echo "searching trace $id for the word $word and the phrase $phrase"
      return 0
    fi
  done
  fail "no listed trace has message text to search for"
}

# Waits until a search for $2 returns trace $SEARCH_ID. Search covers only
# flushed parts, so after a load or a kill the data becomes searchable at the
# next flush, within 30 seconds.
wait_for_hit() {
  label=$1
  i=0
  until curl -fsS "http://$API/api/v1/search?$RANGE&limit=1000&q=$2" |
    grep -q "\"trace_id\":\"$SEARCH_ID\""; do
    i=$((i + 1))
    [ "$i" -le 150 ] || fail "$label: a search for $3 did not return trace $SEARCH_ID"
    sleep 1
  done
}

check_search() {
  label=$1
  pick_search_text
  wait_for_hit "$label" "$SEARCH_WORD" "the word $SEARCH_WORD"
  wait_for_hit "$label" "%22$SEARCH_PHRASE%22" "the phrase $SEARCH_PHRASE"
  echo "ok: $label: a word and a phrase of trace $SEARCH_ID are found by search"
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
check_search "after load"

echo "== kill -9 and restart"
docker kill -s KILL "$SERVER" >/dev/null
docker rm "$SERVER" >/dev/null
start_server
check "after SIGKILL"
check_search "after SIGKILL"

echo "== SIGTERM and restart"
docker stop "$SERVER" >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")
[ "$code" = 0 ] || fail "the server exited with code $code after SIGTERM, expected 0"
docker rm "$SERVER" >/dev/null
start_server
check "after SIGTERM"
check_search "after SIGTERM"
parts=$(curl -fsS "http://$API/api/v1/stats" | grep -o '"parts":[0-9]*' | cut -d: -f2)
[ "${parts:-0}" -gt 0 ] || fail "no parts after a clean shutdown"
echo "ok: $parts parts after the clean shutdown"

echo "PASS: end-to-end ($SPANS spans survived kill -9 and a clean stop)"
