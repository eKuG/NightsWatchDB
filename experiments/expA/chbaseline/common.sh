# Shared by every script. Source after setting DS (dataset name). POSIX sh.
CH_IMAGE=clickhouse/clickhouse-server:26.10.12.13
OTEL_IMAGE=otel/opentelemetry-collector-contrib:0.165.0
GEN_IMAGE=expa-ch-tracegen
HELPER_IMAGE=alpine:3.24.3
CORPUS=${CORPUS:-$(cd "$(dirname "$0")/../../.." && pwd)/.data/corpus}
TRACEGEN_CONTEXT=${TRACEGEN_CONTEXT:-$(cd "$(dirname "$0")/../../.." && pwd)/bench/tracegen}
TRACEGEN_DOCKERFILE=${TRACEGEN_DOCKERFILE:-$(cd "$(dirname "$0")/../../.." && pwd)/experiments/bakeoff/tracegen.Dockerfile}
CPUS=${CPUS:-2}
MEM=${MEM:-4g}
DIR=$(cd "$(dirname "$0")" && pwd)
RESULTS=$DIR/results
P="expA-ch-$DS"
NET=$P-net
VOL=$P-data
CH=$P-clickhouse
OT=$P-collector
mkdir -p "$RESULTS"

die() { echo "error: $*" >&2; exit 1; }

# run a client inside the server container (connection is made once per invocation)
chq() { docker exec -i "$CH" clickhouse-client "$@"; }

start_ch() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
  docker volume inspect "$VOL" >/dev/null 2>&1 || docker volume create "$VOL" >/dev/null
  if [ "$(docker inspect -f '{{.State.Running}}' "$CH" 2>/dev/null)" = true ]; then return; fi
  docker rm -f "$CH" >/dev/null 2>&1 || true
  # CLICKHOUSE_SKIP_USER_SETUP=1: lets the collector (another container) log in as the passwordless
  # default user; the network is private. No server setting is changed.
  docker run -d --name "$CH" --network "$NET" --network-alias clickhouse \
    --cpus "$CPUS" --memory "$MEM" --ulimit nofile=262144:262144 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 -v "$VOL":/var/lib/clickhouse "$CH_IMAGE" >/dev/null
  wait_ch
}

wait_ch() {
  i=0
  until docker exec "$CH" clickhouse-client -q 'select 1' >/dev/null 2>&1; do
    i=$((i + 1)); [ $i -lt 300 ] || die "clickhouse did not start"; sleep 1
  done
}

# prints "user_usec system_usec" for each named container (cgroup v2 cpu.stat read from the VM's root
# mount namespace, because the collector image has no shell)
cpu_usec() {
  ids=""
  for c in "$@"; do ids="$ids $(docker inspect -f '{{.Id}}' "$c")"; done
  docker run --rm --privileged --pid=host "$HELPER_IMAGE" nsenter -t 1 -m -- sh -c '
    for id in '"$ids"'; do
      f=$(find /sys/fs/cgroup -maxdepth 4 -name cpu.stat -path "*$id*" | head -1)
      echo $(grep -E "^(user_usec|system_usec)" "$f" | cut -d" " -f2)
    done'
}

# nearest-rank percentile summary of newline-separated numbers on stdin:
# prints "p50 p90 p99 max"
pct() {
  sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "null null null null"; exit}
    split("50 90 99",q," "); out="";
    for(i=1;i<=3;i++){r=int(q[i]/100*NR); if(r<q[i]/100*NR)r++; if(r<1)r=1; out=out a[r] " "}
    print out a[NR]}'
}
