-- The 50 most recent traces that start inside the window and contain at least one span with an
-- error.type attribute and at least one span with gen_ai.request.model = M. Trace start = earliest span
-- start. ClickHouse cannot prune by trace, so: the helper table (one row per trace and insert block, with
-- second-resolution start) first selects candidate traces whose start second falls in the window (a
-- superset at the two boundary seconds); the main table is then read only from the window start
-- (every span of a window trace is at or after it) and the exact nanosecond test is applied per trace.
SELECT TraceId AS trace_id, toUnixTimestamp64Nano(min(Timestamp)) AS start_ns
FROM otel.otel_traces
PREWHERE Timestamp >= toDateTime(fromUnixTimestamp64Nano({from_ns:Int64}))
WHERE TraceId IN (
    SELECT TraceId FROM otel.otel_traces_trace_id_ts
    GROUP BY TraceId
    HAVING min(Start) >= toDateTime(fromUnixTimestamp64Nano({from_ns:Int64}))
       AND min(Start) <= toDateTime(fromUnixTimestamp64Nano({to_ns:Int64})))
GROUP BY TraceId
HAVING min(Timestamp) >= fromUnixTimestamp64Nano({from_ns:Int64})
   AND min(Timestamp) < fromUnixTimestamp64Nano({to_ns:Int64})
   AND countIf(mapContains(SpanAttributes, 'error.type')) > 0
   AND countIf(SpanAttributes['gen_ai.request.model'] = {model:String}) > 0
ORDER BY start_ns DESC, trace_id
LIMIT 50
