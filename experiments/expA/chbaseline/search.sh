#!/bin/sh
# usage:
#   search.sh DATASET untuned|tuned --queries FILE [--runs 5] [--reference FILE]
#   search.sh DATASET build            build the separate table otel.otel_traces_text (tuned); prints one JSON line
#   search.sh DATASET drop             drop that table (otel.otel_traces is never touched)
#   search.sh DATASET sample-queries OUT   development helper: write a query file in the experiment's format
# Environment: CPUS, MEM (only if the server container must be started); PHRASE_POSITIONS=0 builds the text
# index without phrase positions (positions are experimental in 26.10 but make phrase search much faster; default on).
# `tuned` builds the table first if it does not exist (the build line is then the first output line).
# One output line per query: id, class, rows, traces, server_runs_ms (system.query_log, microsecond
# resolution), server_p50_ms, plus client wall times and read statistics. Last line: summary with p50 per class.
# A query file is {"queries":[{"id","class","terms":[..]} | {"id","class","prefix"} | {"id","class","phrase":[a,b]}]}.
# A reference file is JSON lines with id, rows, traces (for example the scan output); mismatches are flagged.
# The server's query condition cache (on by default) remembers per-granule filter results across runs, so only
# run 1 of a query is a real first execution; the cache is cleared at the start of every search.sh run, and
# first-run figures are reported next to the all-run ones. NO_COND_CACHE=1 turns the cache off for the queries.
# rows_sha256 is not produced: the exporter table has no arrival-order row id to hash.
set -eu
DS=${1:?usage: search.sh DATASET untuned|tuned|build|drop|sample-queries ...}
MODE=${2:?missing mode}
shift 2
. "$(dirname "$0")/common.sh"
SQ=$DIR/queries/search
TBL=otel.otel_traces_text
start_ch
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT INT TERM

table_exists() { [ "$(chq -q "EXISTS TABLE $TBL")" = 1 ]; }
# part and index sizes of the tuned table as a JSON object body
sizes_json() {
  chq --format TSV -q "SELECT sum(rows), count(), sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes)
    FROM system.parts WHERE database = 'otel' AND table = 'otel_traces_text' AND active" > "$TMP/p"
  chq --format TSV -q "SELECT ifNull(sum(data_compressed_bytes), 0), ifNull(sum(data_uncompressed_bytes), 0), ifNull(sum(marks_bytes), 0)
    FROM system.data_skipping_indices WHERE database = 'otel' AND table = 'otel_traces_text' AND name = 'idx_text'" > "$TMP/i"
  set -- $(cat "$TMP/p" "$TMP/i")
  printf '"rows":%s,"parts":%s,"table_bytes_on_disk":%s,"table_compressed_bytes":%s,"table_uncompressed_bytes":%s,"index_compressed_bytes":%s,"index_uncompressed_bytes":%s,"index_marks_bytes":%s' "$@"
}

build() {
  table_exists && die "$TBL exists; run: search.sh $DS drop"
  base0=$(chq --format TSV -q "SELECT count(), sum(bytes_on_disk), arraySort(groupArray(name)) FROM system.parts WHERE database='otel' AND table='otel_traces' AND active")
  opts=""; ixargs=""
  if [ "${PHRASE_POSITIONS:-1}" = 1 ]; then
    opts=", allow_experimental_text_index_phrase_search = 1"; ixargs=", support_phrase_search = 1"
  fi
  chq -q "CREATE TABLE $TBL
    (TraceId String CODEC(ZSTD(3)), SpanId String CODEC(ZSTD(3)), text String CODEC(ZSTD(3)),
     INDEX idx_text lower(text) TYPE text(tokenizer = 'splitByNonAlpha'$ixargs))
    ENGINE = MergeTree ORDER BY (TraceId, SpanId) SETTINGS index_granularity = 8192$opts"
  tag="expA-search-build-$$"
  cpu_a=$(cpu_usec "$CH"); t0=$(date +%s)
  # Small insert blocks (32 MB) bound memory: with the default 256 MB blocks the 1 GB smoke dataset
  # already exceeded the 4 GB limit.
  chq -q "INSERT INTO $TBL SELECT TraceId, SpanId,
      concat(SpanAttributes['gen_ai.input.messages'], char(0), SpanAttributes['gen_ai.output.messages'], char(0),
             SpanAttributes['gen_ai.system_instructions'], char(0), SpanAttributes['gen_ai.tool.definitions'], char(0),
             SpanAttributes['gen_ai.tool.call.arguments'], char(0), SpanAttributes['gen_ai.tool.call.result'])
    FROM otel.otel_traces
    SETTINGS min_insert_block_size_rows = 0, min_insert_block_size_bytes = 33554432, max_insert_threads = 1, max_threads = 2,
             log_comment = '$tag'"
  t_ins=$(date +%s)
  chq -q "OPTIMIZE TABLE $TBL FINAL"
  while [ "$(chq -q 'SELECT count() FROM system.merges')" != 0 ]; do sleep 1; done
  t1=$(date +%s); cpu_b=$(cpu_usec "$CH")
  chq -q "SYSTEM FLUSH LOGS"
  peak=$(chq -q "SELECT max(memory_usage) FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$tag'")
  base1=$(chq --format TSV -q "SELECT count(), sum(bytes_on_disk), arraySort(groupArray(name)) FROM system.parts WHERE database='otel' AND table='otel_traces' AND active")
  same=false; [ "$base0" = "$base1" ] && same=true
  set -- $cpu_a $cpu_b
  printf '{"task":"ch_search_build","variant":"tuned","dataset":"%s","cpus":"%s","memory":"%s","phrase_positions":%s,"wall_s":%s,"insert_wall_s":%s,"optimize_wall_s":%s,' \
    "$DS" "$CPUS" "$MEM" "$([ "${PHRASE_POSITIONS:-1}" = 1 ] && echo true || echo false)" "$((t1 - t0))" "$((t_ins - t0))" "$((t1 - t_ins))"
  printf '"cpu_s":{"user":%s,"sys":%s},"insert_peak_memory_bytes":%s,' \
    "$(awk -v a="$1" -v b="$3" 'BEGIN{print (b-a)/1e6}')" "$(awk -v a="$2" -v b="$4" 'BEGIN{print (b-a)/1e6}')" "${peak:-0}"
  printf '"otel_traces_untouched":%s,' "$same"
  sizes_json; echo "}"
}

case "$MODE" in
build) build; exit 0 ;;
drop) chq -q "DROP TABLE IF EXISTS $TBL SYNC"; echo "dropped $TBL"; exit 0 ;;
sample-queries)
  OUT=${1:?OUTFILE}
  table_exists || build >&2
  chq --format TSVRaw < "$SQ/sample_queries.sql" > "$OUT"; exit 0 ;;
untuned|tuned) ;;
*) die "unknown mode $MODE" ;;
esac

RUNS=5; QFILE=""; REF=""
while [ $# -gt 0 ]; do
  case "$1" in
    --queries) QFILE=$2; shift 2 ;;
    --runs) RUNS=$2; shift 2 ;;
    --reference) REF=$2; shift 2 ;;
    *) die "unknown option $1" ;;
  esac
done
[ -f "$QFILE" ] || die "need --queries FILE"
[ "$MODE" = tuned ] && ! table_exists && build
chq -q "SYSTEM DROP QUERY CONDITION CACHE"
CC=""; [ "${NO_COND_CACHE:-0}" = 1 ] && CC="--use_query_condition_cache=0"
WALL0=$(date +%s); CPU_A=$(cpu_usec "$CH")

# parse the query file inside the server container: id|class|t1|t2|t3|prefix|p1|p2 ('|' keeps empty fields)
docker exec -i "$CH" clickhouse-local --input-format JSONAsString --structure 'json String' --format TSVRaw -q "
  SELECT arrayStringConcat([JSONExtractString(q, 'id'), JSONExtractString(q, 'class'),
         JSONExtractString(q, 'terms', 1), JSONExtractString(q, 'terms', 2), JSONExtractString(q, 'terms', 3),
         JSONExtractString(q, 'prefix'), JSONExtractString(q, 'phrase', 1), JSONExtractString(q, 'phrase', 2)], '|')
  FROM (SELECT arrayJoin(JSONExtractArrayRaw(json, 'queries')) AS q FROM table)" < "$QFILE" > "$TMP/queries.tsv"
[ -s "$TMP/queries.tsv" ] || die "no queries parsed from $QFILE"
if [ -n "$REF" ]; then
  docker exec -i "$CH" clickhouse-local --input-format JSONAsString --structure 'json String' --format TSV -q "
    SELECT JSONExtractString(json, 'id'), JSONExtractUInt(json, 'rows'), JSONExtractUInt(json, 'traces')
    FROM table WHERE JSONHas(json, 'id') AND JSONHas(json, 'rows')" < "$REF" > "$TMP/ref.tsv"
fi

RUNID=$(date +%s)-$$
: > "$TMP/classp50"; mism=0; nq=0
while IFS='|' read -r id cls t1 t2 t3 pre p1 p2; do
  case "$id$cls$t1$t2$t3$pre$p1$p2" in *[!A-Za-z0-9_-]*) die "unexpected characters in query $id" ;; esac
  case "$cls" in
    rare|mid|common) f=term; set -- "--param_t1=$t1" ;;
    and2|or2|not) f=$cls; set -- "--param_t1=$t1" "--param_t2=$t2" ;;
    and3) f=and3; set -- "--param_t1=$t1" "--param_t2=$t2" "--param_t3=$t3" ;;
    prefix) f=prefix; set -- "--param_pre=$pre" ;;
    phrase) f=phrase; set -- "--param_p1=$p1" "--param_p2=$p2" ;;
    *) die "unknown class $cls" ;;
  esac
  : > "$TMP/script.sql"; i=0
  while [ $i -lt "$RUNS" ]; do cat "$SQ/$MODE/$f.sql" >> "$TMP/script.sql"; echo ';' >> "$TMP/script.sql"; i=$((i + 1)); done
  tag="expA-search-$RUNID-$id"
  docker exec -i "$CH" clickhouse-client -n --time --format TSV $CC --log_comment "$tag" "$@" < "$TMP/script.sql" > "$TMP/out" 2> "$TMP/time" ||
    { cat "$TMP/time" >&2; die "query $id failed"; }
  [ "$(sort -u "$TMP/out" | wc -l | tr -d ' ')" = 1 ] || die "query $id: runs disagree"
  rows=$(cut -f1 "$TMP/out" | head -1); traces=$(cut -f2 "$TMP/out" | head -1)
  grep -E '^[0-9]+\.[0-9]+$' "$TMP/time" | awk '{printf "%.3f\n", $1*1000}' > "$TMP/client"
  chq -q "SYSTEM FLUSH LOGS" < /dev/null
  chq --format TSV -q "SELECT toUnixTimestamp64Micro(event_time_microseconds) - toUnixTimestamp64Micro(query_start_time_microseconds), read_rows, read_bytes, memory_usage
    FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$tag' AND current_database = 'default' ORDER BY query_start_time_microseconds" < /dev/null > "$TMP/server"
  [ "$(wc -l < "$TMP/server" | tr -d ' ')" = "$RUNS" ] || die "query $id: expected $RUNS query_log rows"
  cut -f1 "$TMP/server" | awk '{printf "%.3f\n", $1/1000}' > "$TMP/server_ms"
  set -- $(pct < "$TMP/server_ms"); sp50=$1; sp90=$2; sp99=$3; smax=$4
  first=$(head -1 "$TMP/server_ms")
  echo "$cls $sp50 $first" >> "$TMP/classp50"
  rd=$(awk '{s+=$2} END{print s/NR}' "$TMP/server"); rb=$(awk '{s+=$3} END{print s/NR}' "$TMP/server"); pm=$(awk '{if($4>m)m=$4} END{print m+0}' "$TMP/server")
  refjson=""
  if [ -n "$REF" ]; then
    r=$(awk -F'\t' -v i="$id" '$1==i{print $2" "$3}' "$TMP/ref.tsv")
    if [ -z "$r" ]; then refjson=',"reference":null'
    else
      set -- $r
      if [ "$1" = "$rows" ] && [ "$2" = "$traces" ]; then refjson=',"reference_ok":true'; else mism=$((mism + 1)); refjson=",\"reference_ok\":false,\"reference_rows\":$1,\"reference_traces\":$2"; fi
    fi
  fi
  nq=$((nq + 1))
  printf '{"task":"ch_search","variant":"%s","dataset":"%s","id":"%s","class":"%s","rows":%s,"traces":%s,"runs":%s,"server_runs_ms":[%s],"server_first_run_ms":%s,"server_p50_ms":%s,"server_p90_ms":%s,"server_p99_ms":%s,"server_max_ms":%s,"client_runs_ms":[%s],"read_rows_avg":%s,"read_bytes_avg":%s,"peak_query_memory_bytes":%s%s}\n' \
    "$MODE" "$DS" "$id" "$cls" "$rows" "$traces" "$RUNS" "$(paste -sd, "$TMP/server_ms")" "$first" "$sp50" "$sp90" "$sp99" "$smax" "$(paste -sd, "$TMP/client")" "$rd" "$rb" "$pm" "$refjson"
done < "$TMP/queries.tsv"

# summary: per class, the nearest-rank median of the per-query server p50s
CPU_B=$(cpu_usec "$CH"); WALL1=$(date +%s)
set -- $CPU_A $CPU_B
cls_json=$(for c in $(cut -d' ' -f1 "$TMP/classp50" | sort -u); do
  v=$(grep "^$c " "$TMP/classp50" | cut -d' ' -f2 | pct | cut -d' ' -f1); printf '"%s":%s,' "$c" "$v"; done)
first_json=$(for c in $(cut -d' ' -f1 "$TMP/classp50" | sort -u); do
  v=$(grep "^$c " "$TMP/classp50" | cut -d' ' -f3 | pct | cut -d' ' -f1); printf '"%s":%s,' "$c" "$v"; done)
printf '{"task":"ch_search_summary","variant":"%s","dataset":"%s","cpus":"%s","memory":"%s","queries":%s,"runs":%s,"p50_ms":{%s},"first_run_p50_ms":{%s},"p50_ms_meaning":"median over queries of each query'"'"'s server-side p50 (first_run: of each query'"'"'s first run)","query_condition_cache":%s,"reference_mismatches":%s,"wall_s":%s,"server_cpu_s":{"user":%s,"sys":%s}}\n' \
  "$MODE" "$DS" "$CPUS" "$MEM" "$nq" "$RUNS" "${cls_json%,}" "${first_json%,}" "$([ "${NO_COND_CACHE:-0}" = 1 ] && echo '"off"' || echo '"on (default)"')" "$([ -n "$REF" ] && echo $mism || echo null)" "$((WALL1 - WALL0))" \
  "$(awk -v a="$1" -v b="$3" 'BEGIN{print (b-a)/1e6}')" "$(awk -v a="$2" -v b="$4" 'BEGIN{print (b-a)/1e6}')"
