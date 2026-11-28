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
    # A span-level annotation carries a span id too: count only what comes
    # before the annotations, which follow the spans.
    have=$(curl -fsS "http://$API/api/v1/traces/$id" | sed 's/\(.*\),"annotations":\[.*/\1/' |
      grep -o '"span_id":"' | wc -l | tr -d ' ')
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

# --- annotations ---------------------------------------------------------------

# Three traces from the listing of the last check (newest first) and a span of
# the first, read from its trace view before it has any annotation.
pick_traces() {
  set -- $ids
  [ $# -ge 3 ] || fail "annotations: the listing has fewer than three traces"
  T1=$1 T2=$2 T3=$3
  SPAN1=$(curl -fsS "http://$API/api/v1/traces/$T1" | grep -o '"span_id":"[0-9a-f]*"' | head -n 1 | cut -d'"' -f4)
  [ -n "$SPAN1" ] || fail "annotations: trace $T1 shows no span"
  echo "annotating traces $T1, $T2 and $T3 (span $SPAN1 of the first)"
}

# Drops the white space of a JSON text, so that a match does not depend on how
# the server spaces its output.
compact() { tr -d ' \n\r\t'; }

# post_ann BODY: sends the body as JSON; sets REPLY to the answer and fails
# unless it is a 200.
post_ann() {
  code=$(curl -sS -o "$OUT/reply" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    -d "$1" "http://$API/api/v1/annotations") || fail "annotations: POST failed: $1"
  REPLY=$(cat "$OUT/reply")
  [ "$code" = 200 ] || fail "annotations: POST got status $code, expected 200: $REPLY (request: $1)"
}

# post_ann_ok BODY: post_ann, and every item must be accepted.
post_ann_ok() {
  post_ann "$1"
  printf '%s' "$REPLY" | compact | grep -q '"rejected":\[\]' ||
    fail "annotations: an item was rejected: $REPLY (request: $1)"
}

# refused CODE BODY CURL_ARGS...: the POST must get status CODE.
refused() {
  want=$1 body=$2; shift 2
  code=$(curl -sS -o "$OUT/reply" -w '%{http_code}' -X POST -d "$body" "$@" \
    "http://$API/api/v1/annotations") || fail "annotations: refused-POST failed: $*"
  [ "$code" = "$want" ] ||
    fail "annotations: POST with $* got status $code, expected $want: $(cat "$OUT/reply")"
}

post_annotations() {
  ann() { printf '{"annotations":[{"trace_id":"%s",%s}]}' "$1" "$2"; }
  post_ann_ok "$(ann "$T1" '"name":"quality","source":"e2e","value":0.9,"label":"good"')"
  post_ann_ok "$(ann "$T1" '"name":"quality","source":"e2e","value":0.2,"label":"bad"')"
  post_ann_ok "$(ann "$T1" '"span_id":"'"$SPAN1"'","name":"thumbs","label":"down","comment":"off-topic"')"
  post_ann_ok "$(ann "$T2" '"name":"quality","source":"e2e","value":0.8')"
  post_ann_ok "$(ann "$T2" '"name":"quality","source":"e2e","delete":true')"
  # One valid item and one for a trace the server does not have.
  RND=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
  post_ann '{"annotations":[{"trace_id":"'"$T1"'","name":"checked","label":"yes"},{"trace_id":"'"$RND"'","name":"checked","label":"yes"}]}'
  c=$(printf '%s' "$REPLY" | compact)
  case $c in *'"accepted":1,'*|*'"accepted":1}'*) ;; *) fail "annotations: expected accepted 1: $REPLY" ;; esac
  [ "$(printf '%s' "$c" | grep -o '"index":[0-9]*' | tr '\n' ' ')" = '"index":1 ' ] ||
    fail "annotations: expected exactly one rejection, with index 1: $REPLY"
  printf '%s' "$REPLY" | grep -q 'unknown trace' || fail "annotations: the rejection is not 'unknown trace': $REPLY"
  # Refusals; they would change trace 3 if they were let through, and
  # check_annotations looks at trace 3.
  bad=$(ann "$T3" '"name":"quality","source":"e2e","value":0.1,"label":"bad"')
  refused 415 "$bad" -H 'Content-Type: text/plain'
  refused 403 "$bad" -H 'Content-Type: application/json' -H 'Origin: http://evil.example'
  refused 403 "$bad" -H 'Content-Type: application/json' -H 'Sec-Fetch-Site: cross-site'
  echo "ok: annotations written, one unknown trace rejected, three cross-site or wrong-type writes refused"
}

# lists_has LABEL QUERY ID yes|no: a listing over the whole range for QUERY
# must (yes) or must not (no) have trace ID.
lists_has() {
  curl -fsS "http://$API/api/v1/traces?$RANGE&limit=1000&$2" > "$OUT/list" || fail "$1: GET traces?$2 failed"
  if grep -o '"trace_id":"[0-9a-f]*"' "$OUT/list" | grep -q "\"$3\""; then have=yes; else have=no; fi
  [ "$have" = "$4" ] ||
    fail "$1: the listing for '$2' should have trace $3: $4, but has it: $have. Answer: $(head -c 2000 "$OUT/list")"
}

# Needs: T1 T2 T3 SPAN1. The annotations are in the trace view and the list in
# every state: in the memory buffer after the writes, from the replayed log
# after kill -9, from parts after a clean stop.
check_annotations() {
  label=$1
  v1=$(curl -fsS "http://$API/api/v1/traces/$T1" | compact)
  objs=$(printf '%s' "$v1" | sed -n 's/.*"annotations":\[\(.*\)/\1/p' | grep -o '{[^{}]*}' || true)
  [ -n "$objs" ] || fail "$label: trace $T1 shows no annotations: $v1"
  q=$(printf '%s\n' "$objs" | grep '"name":"quality"' || true)
  [ "$(printf '%s\n' "$q" | grep -c .)" = 1 ] || fail "$label: trace $T1 should show one quality annotation, shows: $q"
  case $q in *'"value":0.2,'*|*'"value":0.2}'*) ;; *) fail "$label: trace $T1 quality is not 0.2: $q" ;; esac
  case $q in *'"label":"bad"'*) ;; *) fail "$label: trace $T1 quality has not the label bad: $q" ;; esac
  case $q in *'"source":"e2e"'*) ;; *) fail "$label: trace $T1 quality has not the source e2e: $q" ;; esac
  case $q in *'"value":0.9'*) fail "$label: trace $T1 still shows the overwritten value 0.9: $q" ;; esac
  th=$(printf '%s\n' "$objs" | grep '"name":"thumbs"' || true)
  for part in "\"span_id\":\"$SPAN1\"" '"label":"down"' '"comment":"off-topic"'; do
    case $th in *"$part"*) ;; *) fail "$label: trace $T1 thumbs is not as written (span $SPAN1, label down, comment off-topic; missing $part): $th" ;; esac
  done
  ck=$(printf '%s\n' "$objs" | grep '"name":"checked"' || true)
  case $ck in *'"label":"yes"'*) ;; *) fail "$label: trace $T1 does not show checked/yes: $ck" ;; esac

  v2=$(curl -fsS "http://$API/api/v1/traces/$T2" | compact)
  case $v2 in *'"name":"quality"'*) fail "$label: trace $T2 shows quality although it was deleted: $v2" ;; esac
  v3=$(curl -fsS "http://$API/api/v1/traces/$T3" | compact)
  case $v3 in *'"annotations":['*) fail "$label: trace $T3 shows annotations, but every write for it was refused: $v3" ;; esac

  lists_has "$label" 'annotation=quality&annotation_max=0.5' "$T1" yes
  lists_has "$label" 'annotation=quality&annotation_max=0.5' "$T2" no
  lists_has "$label" 'annotation=quality&annotation_max=0.5' "$T3" no
  lists_has "$label" 'annotation=quality&annotation_min=0.5' "$T1" no
  lists_has "$label" 'annotation=thumbs&annotation_label=down' "$T1" yes
  echo "ok: $label: annotations of trace $T1 (overwrite, span-level, checked), delete on $T2, no change on $T3, annotation filters"
}

# need_stat NAME: the value of a number in /api/v1/stats; fails by name when
# the server does not report it.
need_stat() {
  st=$(curl -fsS "http://$API/api/v1/stats") || fail "stats: GET failed"
  v=$(printf '%s' "$st" | grep -o "\"$1\":[0-9]*" | cut -d: -f2)
  [ -n "$v" ] || fail "/api/v1/stats has no number $1: $st"
  echo "$v"
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
pick_traces
check_search "after load"
post_annotations
check_annotations "after load"

echo "== kill -9 and restart"
docker kill -s KILL "$SERVER" >/dev/null
docker rm "$SERVER" >/dev/null
start_server
check "after SIGKILL"
check_annotations "after SIGKILL"
check_search "after SIGKILL"

echo "== SIGTERM and restart"
docker stop "$SERVER" >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")
[ "$code" = 0 ] || fail "the server exited with code $code after SIGTERM, expected 0"
docker rm "$SERVER" >/dev/null
start_server
check "after SIGTERM"
check_annotations "after SIGTERM"
check_search "after SIGTERM"
parts=$(curl -fsS "http://$API/api/v1/stats" | grep -o '"parts":[0-9]*' | cut -d: -f2)
[ "${parts:-0}" -gt 0 ] || fail "no parts after a clean shutdown"
echo "ok: $parts parts after the clean shutdown"
aparts=$(need_stat annotation_parts)
[ "$aparts" -ge 1 ] || fail "no annotation parts after a clean shutdown: $(curl -fsS "http://$API/api/v1/stats")"
abuf=$(need_stat annotations_buffered)
[ "$abuf" = 0 ] || fail "$abuf annotations are still buffered after a clean shutdown: $(curl -fsS "http://$API/api/v1/stats")"
echo "ok: $aparts annotation parts, none buffered after the clean shutdown"

echo "PASS: end-to-end ($SPANS spans survived kill -9 and a clean stop)"
