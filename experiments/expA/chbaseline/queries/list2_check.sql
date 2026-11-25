-- Definition-literal version of list2 (full scan, no helper table), used only to check list2's answer.
SELECT TraceId AS trace_id, toUnixTimestamp64Nano(min(Timestamp)) AS start_ns
FROM otel.otel_traces
GROUP BY TraceId
HAVING min(Timestamp) >= fromUnixTimestamp64Nano({from_ns:Int64})
   AND min(Timestamp) < fromUnixTimestamp64Nano({to_ns:Int64})
   AND countIf(mapContains(SpanAttributes, 'error.type')) > 0
   AND countIf(SpanAttributes['gen_ai.request.model'] = {model:String}) > 0
ORDER BY start_ns DESC, trace_id
LIMIT 50
