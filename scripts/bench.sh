#!/bin/sh
# Benchmark of the server: one dataset on one resource profile. Builds the
# images, loads the dataset over OTLP gRPC with gzip, then records ingest cost,
# storage, a clean restart, a crash restart and query latency over HTTP. Writes
# result.json and result.md. See scripts/README.md.
#
#   scripts/bench.sh [--dataset d1|d2|smoke] [--profile small|standard] [--out DIR]
#
# Everything it creates (containers, network, data volume) is removed on exit;
# the images and the text corpus volume stay. Generated data never touches the
# host: only the small result files are written to DIR.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATASET=smoke
PROFILE=small
OUT=
while [ $# -gt 0 ]; do
  case $1 in
    --dataset) DATASET=${2:?--dataset needs a value}; shift 2 ;;
    --profile) PROFILE=${2:?--profile needs a value}; shift 2 ;;
    --out) OUT=${2:?--out needs a value}; shift 2 ;;
    *) echo "usage: scripts/bench.sh [--dataset d1|d2|smoke] [--profile small|standard] [--out DIR]" >&2; exit 2 ;;
  esac
done

# The datasets are the ones of the earlier experiments (experiments/expA):
# same generator, same seed, same sizes. EXTRA is the small batch sent right
# before the crash. Smoke has no content, so it needs no corpus: a small text
# is made inside the generator container instead.
case $DATASET in
  d1) GEN_FLAGS="--bytes 8GB --seed 1"; EXTRA="--traces 20 --seed 2"; NEEDS_CORPUS=1 ;;
  d2) GEN_FLAGS="--no-content --spans 10000000 --seed 1"; EXTRA="--no-content --traces 100 --seed 2"; NEEDS_CORPUS=1 ;;
  smoke) GEN_FLAGS="--no-content --spans 30000 --seed 1"; EXTRA="--no-content --traces 100 --seed 2"; NEEDS_CORPUS=0 ;;
  *) echo "unknown dataset: $DATASET (d1, d2 or smoke)" >&2; exit 2 ;;
esac
case $PROFILE in
  small) CPUS=2; MEM=4g ;;
  standard) CPUS=8; MEM=12g ;;
  *) echo "unknown profile: $PROFILE (small or standard)" >&2; exit 2 ;;
esac

COMMIT=$(git -C "$ROOT" rev-parse --short=10 HEAD 2>/dev/null || echo unknown)
DIRTY=false
[ -z "$(git -C "$ROOT" status --porcelain 2>/dev/null)" ] || DIRTY=true
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
[ -n "$OUT" ] || OUT="$ROOT/.data/bench/$STAMP-$COMMIT-$DATASET-$PROFILE"

CORPUS_VOL=nwdb-bench-corpus
TOOLS=alpine:3.24.3      # helper containers: copying the corpus, du
SERVER_IMG=nightswatchdb:bench
GEN_IMG=nwdb-tracegen:bench
QB_IMG=nwdb-querybench:bench
NAME=nwdb-bench-$$
SERVER=$NAME-server
WAITER=$NAME-wait
GEN=$NAME-gen
QB_N=${BENCH_QUERIES:-200}
QB_WARMUP=${BENCH_WARMUP:-20}
QB_CONC=${BENCH_CONCURRENCY:-1}

fail() {
  echo "FAIL: $*" >&2
  docker logs "$SERVER" 2>&1 | tail -n 20 >&2 || true
  exit 1
}

# One system under test at a time, and no clash with another run's cleanup.
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
[ -z "$(docker ps -q --filter name=nwdb-bench-)" ] ||
  { echo "another nwdb-bench-* container is running; wait for it to finish" >&2; exit 1; }

NCPU=$(docker info --format '{{.NCPU}}')
MEMTOTAL=$(docker info --format '{{.MemTotal}}')
[ "$NCPU" -ge "$CPUS" ] || { echo "profile $PROFILE needs $CPUS CPUs, the Docker VM has $NCPU" >&2; exit 1; }
MEM_BYTES=$((${MEM%g} * 1024 * 1024 * 1024))
[ "$MEMTOTAL" -ge "$MEM_BYTES" ] || { echo "profile $PROFILE needs $MEM of memory, the Docker VM has $((MEMTOTAL / 1048576)) MiB" >&2; exit 1; }

if [ "$NEEDS_CORPUS" = 1 ]; then
  # Inspect first: mounting a missing volume would create it empty.
  if ! docker volume inspect "$CORPUS_VOL" >/dev/null 2>&1 ||
    ! docker run --rm -v "$CORPUS_VOL":/corpus:ro "$TOOLS" test -f /corpus/.copied 2>/dev/null; then
    [ -d "$ROOT/.data/corpus" ] || {
      echo "dataset $DATASET needs the text corpus in .data/corpus, which is missing." >&2
      echo "Build it with bench/corpusprep (see bench/corpusprep/README.md)." >&2
      exit 1
    }
    echo "== copying the corpus into the volume $CORPUS_VOL (once)" >&2
    # The only time a host directory is mounted, read-only, to fill the volume.
    # The marker is written last, so an interrupted copy is redone.
    docker run --rm -v "$CORPUS_VOL":/corpus -v "$ROOT/.data/corpus":/src:ro "$TOOLS" \
      sh -c 'cp -a /src/. /corpus/ && touch /corpus/.copied'
  fi
fi

mkdir -p "$OUT"
TMP=$(mktemp -d)
cleanup() {
  docker rm -f "$SERVER" "$WAITER" "$GEN" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  docker volume rm "$NAME" >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

echo "== building images" >&2
docker build -q -t "$SERVER_IMG" "$ROOT" >/dev/null
docker build -q -t "$GEN_IMG" "$ROOT/bench/tracegen" >/dev/null
docker build -q -t "$QB_IMG" "$ROOT/bench/querybench" >/dev/null
docker network create "$NAME" >/dev/null
docker volume create "$NAME" >/dev/null

# --- helpers ---------------------------------------------------------------

start_server() { # first start
  docker run -d --name "$SERVER" --network "$NAME" --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
    -v "$NAME":/var/lib/nightswatchdb -p 127.0.0.1::4320 "$SERVER_IMG" >/dev/null
  wait_ready
}

wait_ready() {
  API=$(docker port "$SERVER" 4320/tcp | head -n 1)
  i=0
  until curl -fsS "http://$API/healthz" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -le 600 ] || fail "the server did not become ready"
    sleep 0.5
  done
}

# Starts the stopped server and sets RESTART_MS to the time from the start of
# its process to the first /healthz answer. The host has no clock finer than a
# second, so a poller container (5 ms steps) is already waiting when the
# server starts, and both ends are read on the Docker VM's clock.
restart_timed() {
  docker run -d --name "$WAITER" --network "$NAME" "$QB_IMG" -wait "http://$SERVER:4320/healthz" >/dev/null
  sleep 1
  docker start "$SERVER" >/dev/null
  docker wait "$WAITER" >/dev/null
  ready=$(docker logs "$WAITER" 2>/dev/null | tail -n 1)
  docker rm "$WAITER" >/dev/null
  case $ready in *[!0-9]*|'') fail "the server did not answer /healthz after the restart" ;; esac
  started=$(docker inspect -f '{{.State.StartedAt}}' "$SERVER")
  RESTART_MS=$(docker run --rm "$QB_IMG" -elapsed "$started" "$ready")
  wait_ready
}

# Runs the generator against the server. Its summary goes to $1.
gen() {
  summary=$1; shift
  if [ "$NEEDS_CORPUS" = 1 ]; then
    docker run --rm --name "$GEN" --network "$NAME" -v "$CORPUS_VOL":/corpus:ro "$GEN_IMG" \
      --corpus /corpus --otlp-endpoint "$SERVER:4317" --compression gzip "$@" > "$summary"
  else
    docker run --rm --name "$GEN" --network "$NAME" --entrypoint sh "$GEN_IMG" -c '
      mkdir /tmp/corpus && base64 /dev/urandom | head -c 400000 > /tmp/corpus/text.txt
      exec tracegen --corpus /tmp/corpus --otlp-endpoint "$0:4317" --compression gzip "$@"
    ' "$SERVER" "$@" > "$summary"
  fi
}

# Server resource counters, read from inside the server container. cgroup v2
# gives CPU time (cpu.stat) and the peak of memory use since the container
# started (memory.peak). Without them, docker stats gives current memory only.
CG=cgroup
cgread() { docker exec "$SERVER" cat "/sys/fs/cgroup/$1" 2>/dev/null; }
cpu_field() { cgread cpu.stat | awk -v k="$1" '$1 == k {print $2}'; }
mem_peak() {
  v=$(cgread memory.peak || true)
  case $v in ''|*[!0-9]*)
    docker stats --no-stream --format '{{.MemUsage}}' "$SERVER" | awk '{print $1}' | awk '
      /GiB/ {sub(/GiB/, ""); printf "%d\n", $1 * 1073741824; next}
      /MiB/ {sub(/MiB/, ""); printf "%d\n", $1 * 1048576; next}
      /KiB/ {sub(/KiB/, ""); printf "%d\n", $1 * 1024; next}
      {printf "%d\n", $1}'
    return ;;
  esac
  echo "$v"
}
rx_bytes() { docker exec "$SERVER" awk '/eth0:/ {sub(/.*:/, ""); print $1}' /proc/net/dev 2>/dev/null || true; }

# Reads a top-level number from a generator summary.
num() { grep "^  \"$2\":" "$1" | head -n 1 | tr -dc '0-9.\n'; }

# Spans the server holds, from the token sums (the server has no count call).
server_spans() {
  curl -fsS "http://$API/api/v1/tokens?from=0&to=9000000000000000000&limit=10000" |
    grep -o '"count":[0-9]*' | cut -d: -f2 | awk '{s += $1} END {print s + 0}'
}
stat_field() { curl -fsS "http://$API/api/v1/stats" | grep -o "\"$1\":[0-9]*" | cut -d: -f2; }

# --- the run -----------------------------------------------------------------

echo "== start the server ($PROFILE: $CPUS CPUs, $MEM)" >&2
start_server
if ! { cgread cpu.stat && cgread memory.peak; } >/dev/null; then CG=docker-stats; fi
CPU0=$(cpu_field usage_usec); USER0=$(cpu_field user_usec); SYS0=$(cpu_field system_usec)
RX0=$(rx_bytes)
[ -n "$CPU0" ] || { CG=docker-stats; CPU0=; }

echo "== load $DATASET: $GEN_FLAGS" >&2
# shellcheck disable=SC2086
gen "$OUT/generator-load.json" $GEN_FLAGS || fail "the generator failed"
CPU1=$(cpu_field usage_usec); USER1=$(cpu_field user_usec); SYS1=$(cpu_field system_usec)
RX1=$(rx_bytes)
PEAK_LOAD=$(mem_peak)
SPANS=$(num "$OUT/generator-load.json" spans)
TRACES=$(num "$OUT/generator-load.json" traces)
ENC_BYTES=$(num "$OUT/generator-load.json" encoded_bytes)
LOAD_S=$(num "$OUT/generator-load.json" elapsed_seconds)
[ -n "$SPANS" ] && [ -n "$LOAD_S" ] || fail "could not read the generator summary"
SERVER_SPANS=$(server_spans)
[ "$SERVER_SPANS" = "$SPANS" ] || fail "the server counts $SERVER_SPANS spans, the generator sent $SPANS"
if [ -n "$CPU0" ] && [ -n "$CPU1" ]; then
  CPU_S=$(awk -v a="$CPU0" -v b="$CPU1" 'BEGIN {printf "%.2f", (b - a) / 1e6}')
  CPU_USER_S=$(awk -v a="$USER0" -v b="$USER1" 'BEGIN {printf "%.2f", (b - a) / 1e6}')
  CPU_SYS_S=$(awk -v a="$SYS0" -v b="$SYS1" 'BEGIN {printf "%.2f", (b - a) / 1e6}')
else
  CPU_S=null; CPU_USER_S=null; CPU_SYS_S=null; CG=docker-stats
fi
if [ -n "$RX0" ] && [ -n "$RX1" ]; then WIRE_BYTES=$((RX1 - RX0)); else WIRE_BYTES=null; fi

echo "== clean restart" >&2
docker stop "$SERVER" >/dev/null
CODE=$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")
[ "$CODE" = 0 ] || fail "the server exited with code $CODE after SIGTERM, expected 0"
# Measured with the server stopped: the stop has flushed the log into parts, so
# the split shows the finished layout and not a write in progress.
docker run --rm -v "$NAME":/data:ro "$TOOLS" sh -c '
  cd /data
  for d in parts wal manifest; do echo "$d $(du -sb "$d" | cut -f1)"; done
  echo "total $(du -sb . | cut -f1)"
  echo "files $(find parts -type f | wc -l)"
' > "$TMP/du.txt"
du_of() { awk -v k="$1" '$1 == k {print $2}' "$TMP/du.txt"; }
PARTS_B=$(du_of parts); WAL_B=$(du_of wal); MANIFEST_B=$(du_of manifest); TOTAL_B=$(du_of total)
restart_timed
CLEAN_MS=$RESTART_MS
[ "$(server_spans)" = "$SPANS" ] || fail "after the clean restart the server counts $(server_spans) spans, expected $SPANS"
PARTS=$(stat_field parts)
BYTES_PER_SPAN=$(awk -v b="$TOTAL_B" -v s="$SPANS" 'BEGIN {printf "%.1f", b / s}')
PARTS_PER_SPAN=$(awk -v b="$PARTS_B" -v s="$SPANS" 'BEGIN {printf "%.1f", b / s}')

echo "== crash restart" >&2
gen "$OUT/generator-extra.json" $EXTRA || fail "the generator failed on the extra batch"
EXTRA_SPANS=$(num "$OUT/generator-extra.json" spans)
docker kill -s KILL "$SERVER" >/dev/null
restart_timed
CRASH_MS=$RESTART_MS
TOTAL_SPANS=$((SPANS + EXTRA_SPANS))
[ "$(server_spans)" = "$TOTAL_SPANS" ] || fail "after the crash restart the server counts $(server_spans) spans, expected $TOTAL_SPANS"
PEAK_START=$(mem_peak)

echo "== queries ($QB_N per kind, $QB_WARMUP warm-up, concurrency $QB_CONC)" >&2
docker run --rm --network "$NAME" "$QB_IMG" -addr "$SERVER:4320" -n "$QB_N" -warmup "$QB_WARMUP" \
  -concurrency "$QB_CONC" > "$OUT/querybench.json" || fail "querybench failed"
PEAK_QUERY=$(mem_peak)
QB_SPANS=$(grep -o '"server_span_count":[0-9]*' "$OUT/querybench.json" | cut -d: -f2)
[ "$QB_SPANS" = "$TOTAL_SPANS" ] || fail "querybench saw $QB_SPANS spans, expected $TOTAL_SPANS"

# --- results -----------------------------------------------------------------

DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
img_id() { docker image inspect -f '{{.Id}}' "$1"; }
case $(uname -s) in
  Darwin) HOST_CPU=$(sysctl -n machdep.cpu.brand_string) ;;
  *) HOST_CPU=$(awk -F': *' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || echo unknown) ;;
esac
HOST_CORES=$(getconf _NPROCESSORS_ONLN)
DOCKER_VERSION=$(docker version --format '{{.Server.Version}}')
DOCKER_ARCH=$(docker info --format '{{.OSType}}/{{.Architecture}}')
DOCKER_KERNEL=$(docker info --format '{{.KernelVersion}}')
SPS=$(awk -v s="$SPANS" -v t="$LOAD_S" 'BEGIN {printf "%.0f", s / t}')
MBPS=$(awk -v b="$ENC_BYTES" -v t="$LOAD_S" 'BEGIN {printf "%.1f", b / 1e6 / t}')
if [ "$WIRE_BYTES" = null ]; then WIRE_MBPS=null; else
  WIRE_MBPS=$(awk -v b="$WIRE_BYTES" -v t="$LOAD_S" 'BEGIN {printf "%.1f", b / 1e6 / t}'); fi
CPU_PER_MSPAN=null
[ "$CPU_S" = null ] || CPU_PER_MSPAN=$(awk -v c="$CPU_S" -v s="$SPANS" 'BEGIN {printf "%.2f", c / s * 1e6}')
# A peak that is not a number (no cgroup file and no docker stats) is null.
nn() { case $1 in ''|*[!0-9]*) echo null ;; *) echo "$1" ;; esac; }

cat > "$OUT/result.json" <<EOF
{
  "commit": "$COMMIT",
  "dirty": $DIRTY,
  "date": "$DATE",
  "dataset": {"name": "$DATASET", "generator_flags": "$GEN_FLAGS", "crash_batch_flags": "$EXTRA", "compression": "gzip", "spans": $SPANS, "traces": $TRACES, "encoded_bytes": $ENC_BYTES},
  "profile": {"name": "$PROFILE", "cpus": $CPUS, "memory": "$MEM"},
  "environment": {
    "host_cpu": "$HOST_CPU", "host_cores": $HOST_CORES,
    "docker_version": "$DOCKER_VERSION", "docker_platform": "$DOCKER_ARCH", "docker_kernel": "$DOCKER_KERNEL",
    "docker_vm_cpus": $NCPU, "docker_vm_memory_bytes": $MEMTOTAL
  },
  "images": {"server": "$(img_id "$SERVER_IMG")", "tracegen": "$(img_id "$GEN_IMG")", "querybench": "$(img_id "$QB_IMG")"},
  "measured_with": "$CG",
  "ingest": {
    "spans": $SPANS, "encoded_bytes": $ENC_BYTES, "wire_bytes": $WIRE_BYTES, "wall_s": $LOAD_S,
    "spans_per_s": $SPS, "encoded_mb_per_s": $MBPS, "wire_mb_per_s": $WIRE_MBPS,
    "server_cpu_s": $CPU_S, "server_cpu_user_s": $CPU_USER_S, "server_cpu_sys_s": $CPU_SYS_S,
    "server_cpu_s_per_million_spans": $CPU_PER_MSPAN, "server_peak_memory_bytes": $(nn "$PEAK_LOAD")
  },
  "storage": {
    "parts_bytes": $PARTS_B, "wal_bytes": $WAL_B, "manifest_bytes": $MANIFEST_B, "total_bytes": $TOTAL_B,
    "parts": $PARTS, "bytes_per_span": $BYTES_PER_SPAN, "parts_bytes_per_span": $PARTS_PER_SPAN
  },
  "restart": {"clean_ms": $CLEAN_MS, "crash_ms": $CRASH_MS, "crash_batch_spans": $EXTRA_SPANS, "server_peak_memory_after_restarts_bytes": $(nn "$PEAK_START")},
  "queries": $(cat "$OUT/querybench.json"),
  "server_peak_memory_after_queries_bytes": $(nn "$PEAK_QUERY")
}
EOF

mb() { awk -v b="$1" 'BEGIN {if (b == "null" || b == "") print "n/a"; else printf "%.1f", b / 1048576}'; }
{
  echo "# $DATASET on $PROFILE ($CPUS CPUs, $MEM), commit $COMMIT, $DATE"
  echo
  echo "Dataset: \`tracegen $GEN_FLAGS\`, sent with gzip. CPU and memory from: $CG."
  echo "Host: $HOST_CPU, Docker $DOCKER_VERSION ($NCPU CPUs, $((MEMTOTAL / 1048576)) MiB in the VM)."
  echo
  echo "| Ingest | |"
  echo "|---|---|"
  echo "| Spans sent (server agrees) | $SPANS |"
  echo "| Encoded bytes / wire bytes (MB) | $(awk -v a="$ENC_BYTES" 'BEGIN {printf "%.1f", a / 1e6}') / $(awk -v a="$WIRE_BYTES" 'BEGIN {if (a == "null") print "n/a"; else printf "%.1f", a / 1e6}') |"
  echo "| Wall time (s) | $LOAD_S |"
  echo "| Spans per second | $SPS |"
  echo "| Encoded MB per second | $MBPS |"
  echo "| Server CPU seconds (user / system) | $CPU_S ($CPU_USER_S / $CPU_SYS_S) |"
  echo "| Server CPU seconds per million spans | $CPU_PER_MSPAN |"
  echo "| Server peak memory during load (MiB) | $(mb "$(nn "$PEAK_LOAD")") |"
  echo
  echo "| Storage and restart | |"
  echo "|---|---|"
  echo "| parts/ / wal/ / manifest/ (bytes) | $PARTS_B / $WAL_B / $MANIFEST_B |"
  echo "| Total on the volume (bytes) | $TOTAL_B |"
  echo "| Parts | $PARTS |"
  echo "| Bytes per span, whole volume / parts/ only | $BYTES_PER_SPAN / $PARTS_PER_SPAN |"
  echo "| Clean restart to /healthz (ms) | $CLEAN_MS |"
  echo "| Crash restart to /healthz (ms), after $EXTRA_SPANS more spans | $CRASH_MS |"
  echo "| Server peak memory since the last start, after queries (MiB) | $(mb "$(nn "$PEAK_QUERY")") |"
  echo
  echo "Queries: $QB_N per kind after $QB_WARMUP warm-up, concurrency $QB_CONC, against the restarted server."
  echo
  echo "| Query | Count | Errors | p50 ms | p95 ms | p99 ms | max ms | Bytes received |"
  echo "|---|---|---|---|---|---|---|---|"
  sed -n 's/^{"kind":"\([a-z_0-9]*\)","count":\([0-9]*\),"errors":\([0-9]*\),"p50_ms":\([^,]*\),"p95_ms":\([^,]*\),"p99_ms":\([^,]*\),"max_ms":\([^,]*\),"bytes":\([0-9]*\).*/| \1 | \2 | \3 | \4 | \5 | \6 | \7 | \8 |/p' "$OUT/querybench.json"
  echo
  # The three exp_* rows are the earlier experiment's list1, list2 and agg,
  # run with the parameters it derives (also in result.json, queries.exp_params).
  sed -n 's/^"exp_params":{"service":"\([^"]*\)","model":"\([^"]*\)","from_ns":\([0-9]*\),"to_ns":\([0-9]*\),"min_span_start_ns":\([0-9]*\),"max_span_start_ns":\([0-9]*\)}.*/Parameters of the exp_* rows: service=\1, model=\2, window [\3, \4) ns, the middle half of the span start range [\5, \6]./p' "$OUT/querybench.json"
  echo "- exp_list1: the 50 newest traces whose root span is from that service, starting in the window."
  echo "- exp_list2: the 50 newest traces in the window with an error and at least one span with that request model."
  echo "- exp_agg: token totals by model of the chat spans that start in the window."
} > "$OUT/result.md"

echo "PASS: $DATASET on $PROFILE, results in $OUT" >&2
