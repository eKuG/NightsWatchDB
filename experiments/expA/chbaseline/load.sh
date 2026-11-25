#!/bin/sh
# usage: [CPUS=2 MEM=4g GEN_ARGS="..."] load.sh DATASET
# DATASET "content*" or "narrow*" picks the full-size tracegen flags; GEN_ARGS overrides them.
# Starts ClickHouse and the collector on a private network, sends the dataset through the collector,
# waits for the queue to drain, checks count() against the spans tracegen reports, then merges parts.
# Leaves the ClickHouse container and volume in place (the collector is removed).
set -eu
DS=${1:?usage: load.sh DATASET}
. "$(dirname "$0")/common.sh"
case "$DS" in
  content*) : "${GEN_ARGS:=--bytes 8GB}" ;;
  narrow*)  : "${GEN_ARGS:=--no-content --spans 10000000}" ;;
  *) : "${GEN_ARGS:?set GEN_ARGS for dataset $DS}" ;;
esac
COLLECTOR_CONFIG=${COLLECTOR_CONFIG:-$DIR/collector.yaml}
OPTIMIZE=${OPTIMIZE:-1}
docker volume inspect "$VOL" >/dev/null 2>&1 && die "volume $VOL exists; run clean.sh $DS first"
docker image inspect "$GEN_IMAGE" >/dev/null 2>&1 ||
  docker build -q -t "$GEN_IMAGE" -f "$TRACEGEN_DOCKERFILE" "$TRACEGEN_CONTEXT" >/dev/null

start_ch
docker run -d --name "$OT" --network "$NET" --network-alias collector \
  -v "$COLLECTOR_CONFIG":/etc/otelcol-contrib/config.yaml:ro "$OTEL_IMAGE" >/dev/null
i=0
until docker logs "$OT" 2>&1 | grep -q 'Everything is ready'; do
  i=$((i + 1)); [ $i -lt 120 ] || { docker logs "$OT" 2>&1 | tail -20; die "collector did not start"; }
  sleep 1
done
metrics() { docker exec "$CH" wget -qO- http://collector:8888/metrics; }
msum() { metrics | grep "^$1" | awk '{s+=$NF} END{printf "%d", s}'; }

cpu0=$(cpu_usec "$CH" "$OT")
t0=$(date +%s)
# tracegen runs without limits; its own cgroup CPU is printed after it finishes
set +e
docker run --rm --name "$P-tracegen" --network "$NET" -v "$CORPUS":/corpus:ro --entrypoint sh "$GEN_IMAGE" -c '
  tracegen --corpus /corpus --otlp-endpoint collector:4317 '"$GEN_ARGS"' 2>/tmp/err; rc=$?
  cat /tmp/err >&2; echo "CGROUP_CPU $(grep -E "^(user_usec|system_usec)" /sys/fs/cgroup/cpu.stat | cut -d" " -f2 | tr "\n" " ")" >&2
  exit $rc' >"$RESULTS/tracegen-$DS.json" 2>"$RESULTS/tracegen-$DS.err"
gen_rc=$?
set -e
t1=$(date +%s)
[ $gen_rc -eq 0 ] || { tail -5 "$RESULTS/tracegen-$DS.err" >&2; die "tracegen failed rc=$gen_rc (see $RESULTS/tracegen-$DS.err)"; }
spans=$(grep -E '^ *"spans":' "$RESULTS/tracegen-$DS.json" | head -1 | tr -dc '0-9')
enc=$(grep -E '^ *"encoded_bytes":' "$RESULTS/tracegen-$DS.json" | head -1 | tr -dc '0-9')
gcpu=$(grep CGROUP_CPU "$RESULTS/tracegen-$DS.err" | cut -d' ' -f2-)

# drain: queue empty and everything accepted has been sent, held for two polls past the flush timeout
i=0; ok=0
while [ $ok -lt 2 ]; do
  q=$(msum otelcol_exporter_queue_size); acc=$(msum otelcol_receiver_accepted_spans); sent=$(msum otelcol_exporter_sent_spans)
  if [ "$q" = 0 ] && [ "$acc" = "$sent" ]; then ok=$((ok + 1)); else ok=0; fi
  i=$((i + 1)); [ $i -lt 3600 ] || break
  sleep 3
done
t2=$(date +%s)
cpu1=$(cpu_usec "$CH" "$OT")
refused=$(msum otelcol_receiver_refused_spans); failed=$(msum otelcol_exporter_send_failed_spans)
enqfail=$(msum otelcol_exporter_enqueue_failed_spans); sent=$(msum otelcol_exporter_sent_spans)
warns=$(docker logs "$OT" 2>&1 | grep -cE '"?(warn|error)"?' || true)
docker logs "$OT" >"$RESULTS/collector-$DS.log" 2>&1

# rows: async inserts are acknowledged after the flush (wait_for_async_insert=1), so count is final
rows=$(chq -q 'SELECT count() FROM otel.otel_traces')
t_opt0=$(date +%s)
if [ "$OPTIMIZE" = 1 ]; then
  chq -q 'OPTIMIZE TABLE otel.otel_traces FINAL'
  chq -q 'OPTIMIZE TABLE otel.otel_traces_trace_id_ts FINAL'
fi
# merges settled: none running
while [ "$(chq -q 'SELECT count() FROM system.merges')" != 0 ]; do sleep 2; done
t_opt1=$(date +%s)
rows2=$(chq -q 'SELECT count() FROM otel.otel_traces')
parts=$(chq -q "SELECT count() FROM system.parts WHERE database='otel' AND table='otel_traces' AND active")

# content integrity: tracegen's per-class attribute byte totals vs the same sums read back from the map
cls() { v=$(grep -E "^ *\"$1\":" "$RESULTS/tracegen-$DS.json" | head -1 | tr -dc 0-9); echo "${v:-0}"; }
g_in=$(cls input_messages); g_out=$(cls output_messages); g_sys=$(cls system_instructions)
g_td=$(cls tool_definitions); g_tar=$(cls tool_arguments_results)
c_in=$(chq -q "SELECT sum(length(SpanAttributes['gen_ai.input.messages'])) FROM otel.otel_traces")
c_out=$(chq -q "SELECT sum(length(SpanAttributes['gen_ai.output.messages'])) FROM otel.otel_traces")
c_sys=$(chq -q "SELECT sum(length(SpanAttributes['gen_ai.system_instructions'])) FROM otel.otel_traces")
c_td=$(chq -q "SELECT sum(length(SpanAttributes['gen_ai.tool.definitions'])) FROM otel.otel_traces")
c_tar=$(chq -q "SELECT sum(length(SpanAttributes['gen_ai.tool.call.arguments']) + length(SpanAttributes['gen_ai.tool.call.result'])) FROM otel.otel_traces")
content_ok=false
[ "$g_in" = "$c_in" ] && [ "$g_out" = "$c_out" ] && [ "$g_sys" = "$c_sys" ] && [ "$g_td" = "$c_td" ] && [ "$g_tar" = "$c_tar" ] && content_ok=true
tsr=$(chq -q "SELECT toString(min(Timestamp)) || ' .. ' || toString(max(Timestamp)) FROM otel.otel_traces")
set -- $cpu0 $cpu1   # ch_user0 ch_sys0 ot_user0 ot_sys0 ch_user1 ch_sys1 ot_user1 ot_sys1 (microseconds)
ch_u=$(($5 - $1)); ch_s=$(($6 - $2)); ot_u=$(($7 - $3)); ot_s=$(($8 - $4))
set -- $gcpu
gen_u=$1; gen_s=$2
wall=$((t2 - t0))
match=false; [ "$rows" = "$spans" ] && [ "$rows2" = "$spans" ] && match=true
printf '{"task":"load","variant":"clickhouse-otel-exporter","dataset":"%s","gen_args":"%s","cpus":"%s","memory":"%s",' "$DS" "$GEN_ARGS" "$CPUS" "$MEM"
printf '"time_range":"%s","content_attr_bytes_match_tracegen":%s,"spans_sent":%s,"rows_in_clickhouse":%s,"rows_after_optimize":%s,"match":%s,' "$tsr" "$content_ok" "$spans" "$rows" "$rows2" "$match"
printf '"encoded_bytes":%s,"wall_s":%s,"send_wall_s":%s,"input_mb_per_s":%s,"optimize_wall_s":%s,"parts_after_optimize":%s,' \
  "$enc" "$wall" "$((t1 - t0))" "$(awk -v b="$enc" -v w="$wall" 'BEGIN{printf "%.2f", b/1e6/w}')" "$((t_opt1 - t_opt0))" "$parts"
printf '"cpu_s":{"clickhouse":{"user":%s,"sys":%s},"collector":{"user":%s,"sys":%s},"tracegen":{"user":%s,"sys":%s}},' \
  "$(awk "BEGIN{print $ch_u/1e6}")" "$(awk "BEGIN{print $ch_s/1e6}")" "$(awk "BEGIN{print $ot_u/1e6}")" "$(awk "BEGIN{print $ot_s/1e6}")" \
  "$(awk "BEGIN{print $gen_u/1e6}")" "$(awk "BEGIN{print $gen_s/1e6}")"
printf '"collector":{"accepted_spans":%s,"refused_spans":%s,"sent_spans":%s,"send_failed_spans":%s,"enqueue_failed_spans":%s,"warn_or_error_log_lines":%s}}\n' \
  "$acc" "$refused" "$sent" "$failed" "$enqfail" "$warns"
docker rm -f "$OT" >/dev/null
[ "$match" = true ] || die "row count $rows (after optimize $rows2) != spans sent $spans"
[ "$content_ok" = true ] || die "attribute bytes in ClickHouse differ from tracegen's per-class totals (tracegen in=$g_in out=$g_out sys=$g_sys td=$g_td tar=$g_tar; clickhouse in=$c_in out=$c_out sys=$c_sys td=$c_td tar=$c_tar)"
