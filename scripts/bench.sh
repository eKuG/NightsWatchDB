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
#
# Compaction runs in the server as it does in production, with a short quiet
# period (BENCH_COMPACTION_QUIET, default 5s) so that the wait for it is short.
# After the load the script waits until the buffer is flushed, no merge is
# running and no time window has more than one part, and records how long that
# took and the CPU the server used meanwhile. BENCH_NO_COMPACTION=1 runs the
# server without compaction (and skips the wait) to measure what it saves.
# After the queries, querybench writes annotations (10% of the traces get a
# score) and times the queries that use them. The script then waits for
# compaction once more, and records the annotation counts, the time until they
# were in parts, the time until compaction settled, the annotation parts and
# bytes and the size of the volume. BENCH_NO_ANNOTATIONS=1 skips all of that,
# for a server that has no annotations.
# BENCH_CORPUS_DIR is the host directory the corpus volume is filled from
# (default .data/corpus).
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
QUIET=${BENCH_COMPACTION_QUIET:-5s}
NO_COMPACTION=${BENCH_NO_COMPACTION:-0}
NO_ANNOTATIONS=${BENCH_NO_ANNOTATIONS:-0}
COMPACT_TIMEOUT=${BENCH_COMPACTION_TIMEOUT:-3600}
CORPUS_DIR=${BENCH_CORPUS_DIR:-$ROOT/.data/corpus}

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
    [ -d "$CORPUS_DIR" ] || {
      echo "dataset $DATASET needs the text corpus in $CORPUS_DIR, which is missing." >&2
      echo "Build it with bench/corpusprep (see bench/corpusprep/README.md)." >&2
      exit 1
    }
    echo "== copying the corpus into the volume $CORPUS_VOL (once)" >&2
    # The only time a host directory is mounted, read-only, to fill the volume.
    # The marker is written last, so an interrupted copy is redone.
    docker run --rm -v "$CORPUS_VOL":/corpus -v "$CORPUS_DIR":/src:ro "$TOOLS" \
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

# The poller and querybench reach the API by the container's name on the Docker
# network, which the API's Host check only answers when it is listed.
start_server() { # first start; the settings stay for every restart
  docker run -d --name "$SERVER" --network "$NAME" --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
    -e NWDB_COMPACTION_QUIET="$QUIET" -e NWDB_NO_COMPACTION="$NO_COMPACTION" \
    -e NWDB_API_ALLOWED_HOSTS="$SERVER" \
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
# A stats number that must be there (the annotation fields are not, on an old server).
need_stat() {
  v=$(stat_field "$1")
  [ -n "$v" ] || fail "/api/v1/stats has no number $1: $(curl -fsS "http://$API/api/v1/stats")"
  echo "$v"
}
# Sizes of the data directories of the volume, one "name bytes" line each, to $1.
# The helper only reads, so it also works while the server runs.
du_volume() {
  docker run --rm -v "$NAME":/data:ro "$TOOLS" sh -c '
    cd /data
    for d in parts wal manifest; do echo "$d $(du -sb "$d" | cut -f1)"; done
    echo "total $(du -sb . | cut -f1)"
    echo "files $(find parts -type f | wc -l)"
  ' > "$1"
}

# Waits until the buffer is flushed, no merge is running and no window has more
# than one part (two polls in a row), then sets SETTLE_S to how long that took
# (whole seconds). Fails after COMPACT_TIMEOUT seconds.
wait_compaction() {
  t0=$(date +%s); ok=0
  while :; do
    st=$(curl -fsS "http://$API/api/v1/stats") || fail "the server did not answer /api/v1/stats"
    if echo "$st" | grep -q '"merging":false' && echo "$st" | grep -q '"windows_with_many_parts":0[,}]' &&
      echo "$st" | grep -q '"buffered_bytes":0[,}]'; then
      ok=$((ok + 1)); [ "$ok" -lt 2 ] || break
    else
      ok=0
    fi
    [ $(($(date +%s) - t0)) -le "$COMPACT_TIMEOUT" ] || fail "compaction did not settle within ${COMPACT_TIMEOUT}s: $st"
    sleep 0.5
  done
  SETTLE_S=$(($(date +%s) - t0))
}

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

PARTS_AT_LOAD_END=$(stat_field parts)
if [ "$NO_COMPACTION" = 1 ]; then
  echo "== compaction is off" >&2
  SETTLE_S=0
else
  echo "== waiting for compaction to settle (quiet period $QUIET)" >&2
  wait_compaction
fi
CPU2=$(cpu_field usage_usec)
if [ -n "$CPU1" ] && [ -n "$CPU2" ]; then
  SETTLE_CPU_S=$(awk -v a="$CPU1" -v b="$CPU2" 'BEGIN {printf "%.2f", (b - a) / 1e6}')
else
  SETTLE_CPU_S=null
fi
ENGINE_STATS=$(curl -fsS "http://$API/api/v1/stats")
PARTS_SETTLED=$(stat_field parts)

echo "== clean restart" >&2
docker stop "$SERVER" >/dev/null
CODE=$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")
[ "$CODE" = 0 ] || fail "the server exited with code $CODE after SIGTERM, expected 0"
# Measured with the server stopped: the stop has flushed the log into parts, so
# the split shows the finished layout and not a write in progress.
du_volume "$TMP/du.txt"
du_of() { awk -v k="$1" '$1 == k {print $2}' "${2:-$TMP/du.txt}"; }
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
# Two runs, so that the peak of memory is read between them: the first has every
# kind but the annotation phase (the figure that compares with earlier results),
# the second only the annotation phase.
docker run --rm --network "$NAME" "$QB_IMG" -addr "$SERVER:4320" -n "$QB_N" -warmup "$QB_WARMUP" \
  -concurrency "$QB_CONC" -annotate=false > "$OUT/querybench.json" || fail "querybench failed"
PEAK_QUERY=$(mem_peak)
QB_SPANS=$(grep -o '"server_span_count":[0-9]*' "$OUT/querybench.json" | cut -d: -f2)
[ "$QB_SPANS" = "$TOTAL_SPANS" ] || fail "querybench saw $QB_SPANS spans, expected $TOTAL_SPANS"
QB_FILES="$OUT/querybench.json"
PEAK_ANN=null
if [ "$NO_ANNOTATIONS" != 1 ]; then
  echo "== annotations" >&2
  docker run --rm --network "$NAME" "$QB_IMG" -addr "$SERVER:4320" -n "$QB_N" -warmup "$QB_WARMUP" \
    -concurrency "$QB_CONC" -annotate-only > "$OUT/querybench-annotations.json" || fail "querybench failed in the annotation phase"
  PEAK_ANN=$(mem_peak)
  QB_SPANS=$(grep -o '"server_span_count":[0-9]*' "$OUT/querybench-annotations.json" | cut -d: -f2)
  [ "$QB_SPANS" = "$TOTAL_SPANS" ] || fail "querybench saw $QB_SPANS spans after the annotations, expected $TOTAL_SPANS"
  QB_FILES="$OUT/querybench.json $OUT/querybench-annotations.json"
fi

# The annotation phase printed its step on one line starting with "annotation":
# (null when it was skipped).
ANN_ENABLED=false
if [ -f "$OUT/querybench-annotations.json" ] && grep -q '^"annotation":{' "$OUT/querybench-annotations.json"; then ANN_ENABLED=true; fi
ann_num() { grep '^"annotation":' "$OUT/querybench-annotations.json" | grep -o "\"$1\":[0-9.]*" | head -n 1 | cut -d: -f2; }
if [ "$ANN_ENABLED" = true ]; then
  ANN_SENT=$(ann_num sent); ANN_ACCEPTED=$(ann_num accepted); ANN_REJECTED=$(ann_num rejected)
  ANN_POST_S=$(ann_num seconds); ANN_PER_S=$(ann_num per_s); ANN_FLUSH_S=$(ann_num flush_wait_s)
  ANN_PARTS_FLUSH=$(ann_num annotation_parts); ANN_BYTES_FLUSH=$(ann_num annotation_bytes)
  [ -n "$ANN_SENT" ] && [ -n "$ANN_PER_S" ] && [ -n "$ANN_FLUSH_S" ] && [ -n "$ANN_BYTES_FLUSH" ] ||
    fail "could not read the annotation step of querybench-annotations.json"
  [ "$ANN_REJECTED" = 0 ] && [ "$ANN_ACCEPTED" = "$ANN_SENT" ] ||
    fail "querybench sent $ANN_SENT annotations, the server accepted $ANN_ACCEPTED and rejected $ANN_REJECTED"
  if [ "$NO_COMPACTION" = 1 ]; then
    echo "== compaction is off, not waiting after the annotations" >&2
    ANN_SETTLE_S=0
  else
    echo "== waiting for compaction to settle after the annotations" >&2
    LOAD_SETTLE_S=$SETTLE_S
    wait_compaction
    ANN_SETTLE_S=$SETTLE_S; SETTLE_S=$LOAD_SETTLE_S
  fi
  ANN_PARTS_SETTLED=$(need_stat annotation_parts); ANN_BYTES_SETTLED=$(need_stat annotation_bytes)
  ANN_BUFFERED=$(need_stat annotations_buffered)
  [ "$ANN_BUFFERED" = 0 ] || fail "$ANN_BUFFERED annotations are buffered after the wait"
  ANN_BYTES_PER=$(awk -v b="$ANN_BYTES_SETTLED" -v n="$ANN_ACCEPTED" 'BEGIN {printf "%.1f", b / n}')
  # The volume again, with the server running. Unlike the first measurement it
  # also holds the crash batch and whatever the log has not yet been folded
  # into parts.
  du_volume "$TMP/du2.txt"
  TOTAL_B2=$(du_of total "$TMP/du2.txt"); PARTS_B2=$(du_of parts "$TMP/du2.txt"); WAL_B2=$(du_of wal "$TMP/du2.txt")
  ANN_JSON=$(cat <<ANN
{"sent": $ANN_SENT, "accepted": $ANN_ACCEPTED, "rejected": $ANN_REJECTED, "post_s": $ANN_POST_S, "per_s": $ANN_PER_S,
    "in_parts_after_s": $ANN_FLUSH_S, "settle_after_s": $ANN_SETTLE_S,
    "annotation_parts_after_flush": $ANN_PARTS_FLUSH, "annotation_bytes_after_flush": $ANN_BYTES_FLUSH,
    "annotation_parts_settled": $ANN_PARTS_SETTLED, "annotation_bytes_settled": $ANN_BYTES_SETTLED,
    "bytes_per_annotation": $ANN_BYTES_PER,
    "volume_after": {"total_bytes": $TOTAL_B2, "parts_bytes": $PARTS_B2, "wal_bytes": $WAL_B2, "measured_with_server_running": true}}
ANN
)
else
  ANN_JSON=null
fi

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
  "compaction": {
    "enabled": $([ "$NO_COMPACTION" = 1 ] && echo false || echo true), "quiet": "$QUIET",
    "settle_s": $SETTLE_S, "settle_cpu_s": $SETTLE_CPU_S,
    "parts_at_end_of_load": $PARTS_AT_LOAD_END, "parts_settled": $PARTS_SETTLED,
    "engine_stats_settled": $ENGINE_STATS
  },
  "storage": {
    "parts_bytes": $PARTS_B, "wal_bytes": $WAL_B, "manifest_bytes": $MANIFEST_B, "total_bytes": $TOTAL_B,
    "parts": $PARTS, "bytes_per_span": $BYTES_PER_SPAN, "parts_bytes_per_span": $PARTS_PER_SPAN
  },
  "restart": {"clean_ms": $CLEAN_MS, "crash_ms": $CRASH_MS, "crash_batch_spans": $EXTRA_SPANS, "server_peak_memory_after_restarts_bytes": $(nn "$PEAK_START")},
  "annotations": $ANN_JSON,
  "queries": $(cat "$OUT/querybench.json"),
  "annotation_queries": $(if [ -f "$OUT/querybench-annotations.json" ]; then cat "$OUT/querybench-annotations.json"; else echo null; fi),
  "server_peak_memory_after_queries_bytes": $(nn "$PEAK_QUERY"),
  "server_peak_memory_after_annotations_bytes": $(nn "$PEAK_ANN")
}
EOF

json_num() { echo "$ENGINE_STATS" | grep -o "\"$1\":[0-9]*" | cut -d: -f2; }
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
  echo "| Compaction (quiet period $QUIET) | |"
  echo "|---|---|"
  echo "| Wait after the load until settled (s) / server CPU in it (s) | $SETTLE_S / $SETTLE_CPU_S |"
  echo "| Parts at the end of the load / settled | $PARTS_AT_LOAD_END / $PARTS_SETTLED |"
  echo "| Merges done / parts merged away | $(json_num merges_done) / $(json_num parts_merged_away) |"
  echo "| Duplicate spans dropped | $(json_num duplicate_spans_dropped) |"
  echo "| Bytes of merge inputs / outputs | $(json_num merge_input_bytes) / $(json_num merge_output_bytes) |"
  echo "| Last compaction error | $(echo "$ENGINE_STATS" | grep -q '"last_compaction_error":null' && echo none || echo "see result.json") |"
  echo
  echo "| Storage and restart | |"
  echo "|---|---|"
  echo "| parts/ / wal/ / manifest/ (bytes) | $PARTS_B / $WAL_B / $MANIFEST_B |"
  echo "| Total on the volume (bytes) | $TOTAL_B |"
  echo "| Parts (after the restart; at the end of the load: $PARTS_AT_LOAD_END) | $PARTS |"
  echo "| Bytes per span, whole volume / parts/ only | $BYTES_PER_SPAN / $PARTS_PER_SPAN |"
  echo "| Clean restart to /healthz (ms) | $CLEAN_MS |"
  echo "| Crash restart to /healthz (ms), after $EXTRA_SPANS more spans | $CRASH_MS |"
  echo "| Server peak memory since the last start, after queries (MiB) | $(mb "$(nn "$PEAK_QUERY")") |"
  [ "$ANN_ENABLED" != true ] || echo "| Server peak memory since the last start, after the annotation phase (MiB) | $(mb "$(nn "$PEAK_ANN")") |"
  if [ "$ANN_ENABLED" = true ]; then
    echo
    echo "| Annotations (written by querybench after the queries; 10% of the traces get a quality score and 1% a human label, and the two sets are disjoint) | |"
    echo "|---|---|"
    echo "| Sent / accepted / rejected | $ANN_SENT / $ANN_ACCEPTED / $ANN_REJECTED |"
    echo "| Annotations per second (requests of 500, one at a time) / seconds | $ANN_PER_S / $ANN_POST_S |"
    echo "| Seconds until they were in parts / until compaction settled | $ANN_FLUSH_S / $ANN_SETTLE_S |"
    echo "| Annotation parts after the flush / settled | $ANN_PARTS_FLUSH / $ANN_PARTS_SETTLED |"
    echo "| Annotation bytes after the flush / settled | $ANN_BYTES_FLUSH / $ANN_BYTES_SETTLED |"
    echo "| Bytes per annotation (settled) | $ANN_BYTES_PER |"
    echo "| Volume with the server running, after the annotations: total / parts/ / wal/ (bytes) | $TOTAL_B2 / $PARTS_B2 / $WAL_B2 |"
    echo
    echo "The rows annotate_post_500, open_trace_annotated, list_annotated_50, list_annotation_label_50 and list_annotation_value_50 below are measured after the annotations were written; open_trace_after and list_newest_50_after repeat open_trace and list_newest_50 to show what annotations cost the queries that do not ask for them."
  fi
  echo
  echo "Queries: $QB_N per kind after $QB_WARMUP warm-up, concurrency $QB_CONC, against the restarted server."
  echo
  echo "| Query | Count | Errors | p50 ms | p95 ms | p99 ms | max ms | Bytes received |"
  echo "|---|---|---|---|---|---|---|---|"
  sed -n 's/^{"kind":"\([a-z_0-9]*\)","count":\([0-9]*\),"errors":\([0-9]*\),"p50_ms":\([^,]*\),"p95_ms":\([^,]*\),"p99_ms":\([^,]*\),"max_ms":\([^,]*\),"bytes":\([0-9]*\).*/| \1 | \2 | \3 | \4 | \5 | \6 | \7 | \8 |/p' $QB_FILES
  echo
  # The three exp_* rows are the earlier experiment's list1, list2 and agg,
  # run with the parameters it derives (also in result.json, queries.exp_params).
  sed -n 's/^"exp_params":{"service":"\([^"]*\)","model":"\([^"]*\)","from_ns":\([0-9]*\),"to_ns":\([0-9]*\),"min_span_start_ns":\([0-9]*\),"max_span_start_ns":\([0-9]*\)}.*/Parameters of the exp_* rows: service=\1, model=\2, window [\3, \4) ns, the middle half of the span start range [\5, \6]./p' "$OUT/querybench.json"
  echo "- exp_list1: the 50 newest traces whose root span is from that service, starting in the window."
  echo "- exp_list2: the 50 newest traces in the window with an error and at least one span with that request model."
  echo "- exp_agg: token totals by model of the chat spans that start in the window."
} > "$OUT/result.md"

echo "PASS: $DATASET on $PROFILE, results in $OUT" >&2
