#!/bin/sh
# usage:
#   query.sh DATASET params                      print S, M and the window as one JSON line
#   query.sh DATASET sample-ids N OUTFILE        write N trace ids (rule of the bake-off sample-ids)
#   query.sh DATASET run --service S --model M --from-ns N --to-ns N --ids FILE [--runs 20] [--only open,list1,list2,agg] [--check]
# Environment: CPUS and MEM (only used if the server container has to be started).
# One clickhouse-client session per query kind (connection made once, before timing). Each statement is
# timed twice: client wall time as printed by clickhouse-client --time (send until last block received,
# 1 ms resolution, connection setup excluded, results discarded with FORMAT Null after being fully
# received) and server-side time from system.query_log (microseconds, tagged via log_comment).
# p50_ms/p90_ms/p99_ms/max_ms are the client figures; server_* the server figures. No warm-up runs.
set -eu
DS=${1:?usage: query.sh DATASET params|sample-ids|run ...}
CMD=${2:?missing command}
shift 2
. "$(dirname "$0")/common.sh"
Q=$DIR/queries
start_ch
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT INT TERM

case "$CMD" in
params)
  chq --format JSONEachRow --output_format_json_quote_64bit_integers=0 < "$Q/params.sql"
  exit 0 ;;
sample-ids)
  N=${1:?N}; OUT=${2:?OUTFILE}
  chq --format TSV < "$Q/sample_ids.sql" > "$TMP/all"
  awk -v n="$N" '{a[NR-1]=$0} END{k=int(NR/n); if(k<1)k=1; for(i=0;i<NR && w<n;i+=k){print a[i]; w++}}' "$TMP/all" > "$OUT"
  printf '{"task":"sample-ids","distinct_traces":%s,"written":%s}\n' "$(wc -l < "$TMP/all" | tr -d ' ')" "$(wc -l < "$OUT" | tr -d ' ')"
  exit 0 ;;
run) ;;
*) die "unknown command $CMD" ;;
esac

RUNS=20; ONLY=open,list1,list2,agg; CHECK=0; IDS=""; S=""; M=""; FROM=""; TO=""
while [ $# -gt 0 ]; do
  case "$1" in
    --runs) RUNS=$2; shift 2 ;;
    --only) ONLY=$2; shift 2 ;;
    --service) S=$2; shift 2 ;;
    --model) M=$2; shift 2 ;;
    --from-ns) FROM=$2; shift 2 ;;
    --to-ns) TO=$2; shift 2 ;;
    --ids) IDS=$2; shift 2 ;;
    --check) CHECK=1; shift ;;
    *) die "unknown option $1" ;;
  esac
done
[ -n "$S" ] && [ -n "$M" ] && [ -n "$FROM" ] && [ -n "$TO" ] || die "need --service --model --from-ns --to-ns"
case "$FROM$TO" in *[!0-9]*) die "--from-ns/--to-ns must be integers" ;; esac
case "$S$M" in *[\'\\\ ]*) die "quote, backslash or space in --service/--model not supported" ;; esac
PARAMS="--param_service=$S --param_model=$M --param_from_ns=$FROM --param_to_ns=$TO"
RUNID=$(date +%s)-$$

# run_timed NAME SCRIPTFILE EXPECTED_N: client session over the script, then collect both timings
run_timed() {
  name=$1; script=$2; n=$3; tag="expA-$RUNID-$name"
  cpu_a=$(cpu_usec "$CH")
  s0=$(date +%s)
  chq -n --time --format Null --log_comment "$tag" $PARAMS < "$script" 2> "$TMP/$name.time" || {
    cat "$TMP/$name.time" >&2; die "query $name failed"; }
  s1=$(date +%s)
  cpu_b=$(cpu_usec "$CH")
  grep -E '^[0-9]+\.[0-9]+$' "$TMP/$name.time" | awk '{printf "%.3f\n", $1*1000}' > "$TMP/$name.client"
  [ "$(wc -l < "$TMP/$name.client" | tr -d ' ')" = "$n" ] || die "$name: expected $n client timings"
  chq -q "SYSTEM FLUSH LOGS"
  chq --format TSV -q "SELECT toUnixTimestamp64Micro(event_time_microseconds) - toUnixTimestamp64Micro(query_start_time_microseconds),
      read_rows, read_bytes, result_rows, result_bytes, memory_usage
    FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$tag' AND current_database = 'default'
    ORDER BY query_start_time_microseconds" > "$TMP/$name.server"
  [ "$(wc -l < "$TMP/$name.server" | tr -d ' ')" = "$n" ] || die "$name: expected $n query_log rows"
  set -- $cpu_a $cpu_b
  CPU_U=$(awk -v a="$1" -v b="$3" 'BEGIN{print (b-a)/1e6}'); CPU_S=$(awk -v a="$2" -v b="$4" 'BEGIN{print (b-a)/1e6}')
  WALL=$((s1 - s0))
  set -- $(pct < "$TMP/$name.client"); CP50=$1; CP90=$2; CP99=$3; CMAX=$4
  cut -f1 "$TMP/$name.server" | awk '{printf "%.3f\n", $1/1000}' > "$TMP/$name.server_ms"
  set -- $(pct < "$TMP/$name.server_ms"); SP50=$1; SP90=$2; SP99=$3; SMAX=$4
  RES_ROWS=$(awk '{s+=$4} END{print s+0}' "$TMP/$name.server")
  RES_BYTES=$(awk '{s+=$5} END{print s+0}' "$TMP/$name.server")
  READ_ROWS=$(awk '{s+=$2} END{print s+0}' "$TMP/$name.server")
  READ_BYTES=$(awk '{s+=$3} END{print s+0}' "$TMP/$name.server")
  PEAK_MEM=$(awk '{if($6>m)m=$6} END{print m+0}' "$TMP/$name.server")
  CLIENT_LIST=$(paste -sd, "$TMP/$name.client"); SERVER_LIST=$(paste -sd, "$TMP/$name.server_ms")
}

# head NAME: common JSON prefix
common() {
  printf '{"task":"ch_%s","variant":"clickhouse-otel-exporter","dataset":"%s","cpus":"%s","memory":"%s",' "$1" "$DS" "$CPUS" "$MEM"
}
tail_common() {
  printf '"p50_ms":%s,"p90_ms":%s,"p99_ms":%s,"max_ms":%s,"server_p50_ms":%s,"server_p90_ms":%s,"server_p99_ms":%s,"server_max_ms":%s,' \
    "$CP50" "$CP90" "$CP99" "$CMAX" "$SP50" "$SP90" "$SP99" "$SMAX"
  printf '"timing":"p*_ms = client wall (clickhouse-client --time, connection excluded, 1ms resolution); server_* = system.query_log event_time-query_start_time",'
  printf '"wall_s":%s,"server_cpu_s":{"user":%s,"sys":%s},"read_rows":%s,"read_bytes":%s,"peak_query_memory_bytes":%s,' \
    "$WALL" "$CPU_U" "$CPU_S" "$READ_ROWS" "$READ_BYTES" "$PEAK_MEM"
}
# one untimed execution of a query file returning TraceIds in order, hashed inside ClickHouse
result_hash() {
  { echo "SELECT lower(hex(SHA256(arrayStringConcat(groupArray(trace_id))))) FROM ("; cat "$Q/$1.sql"; echo ")"; } |
    chq --format TSV $PARAMS
}

for name in $(echo "$ONLY" | tr ',' ' '); do
  case "$name" in
  open)
    [ -f "$IDS" ] || die "open needs --ids FILE"
    N=$(wc -l < "$IDS" | tr -d ' ')
    : > "$TMP/open.sql"
    while read -r id; do
      case "$id" in *[!0-9a-f]*|"") die "bad trace id '$id'" ;; esac
      sed "s/{id:String}/'$id'/" "$Q/open.sql" >> "$TMP/open.sql"; echo ';' >> "$TMP/open.sql"
    done < "$IDS"
    run_timed open "$TMP/open.sql" "$N"
    # untimed correctness check: rows returned by the lookups equal a plain count of those trace ids
    inlist=$(sed "s/.*/'&'/" "$IDS" | paste -sd, -)
    expected=$(chq -q "SELECT count() FROM otel.otel_traces WHERE TraceId IN ($inlist)")
    common open
    printf '"lookups":%s,"rows_returned":%s,"rows_expected":%s,"bytes_returned_uncompressed":%s,' "$N" "$RES_ROWS" "$expected" "$RES_BYTES"
    tail_common
    printf '"runs_ms":[%s],"server_runs_ms":[%s]}\n' "$CLIENT_LIST" "$SERVER_LIST"
    ;;
  list1|list2|agg)
    : > "$TMP/$name.sql"; i=0
    while [ $i -lt "$RUNS" ]; do cat "$Q/$name.sql" >> "$TMP/$name.sql"; echo ';' >> "$TMP/$name.sql"; i=$((i + 1)); done
    run_timed "$name" "$TMP/$name.sql" "$RUNS"
    common "$name"
    printf '"runs":%s,' "$RUNS"
    case "$name" in
      agg)
        res=$(chq --format JSONEachRow --output_format_json_quote_64bit_integers=0 $PARAMS < "$Q/agg.sql" | paste -sd, -)
        printf '"result":[%s],' "$res" ;;
      *)
        printf '"result_sha256":"%s",' "$(result_hash "$name")"
        if [ "$name" != agg ] && [ "$CHECK" = 1 ]; then
          a=$(chq --format TSV $PARAMS < "$Q/$name.sql" | cut -f1 | paste -sd, -)
          b=$(chq --format TSV $PARAMS < "$Q/${name}_check.sql" | cut -f1 | paste -sd, -)
          [ "$a" = "$b" ] && printf '"matches_definition_query":true,' || printf '"matches_definition_query":false,'
        fi ;;
    esac
    tail_common
    printf '"runs_ms":[%s],"server_runs_ms":[%s]}\n' "$CLIENT_LIST" "$SERVER_LIST"
    ;;
  *) die "unknown query $name" ;;
  esac
done
