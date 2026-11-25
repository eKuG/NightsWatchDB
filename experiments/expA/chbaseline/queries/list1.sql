-- The 50 most recent traces whose root span (no parent) is from service S and starts inside the window.
-- ServiceName is the first key column. The toDateTime() bounds repeat the window in the form of the
-- third key column so the index can use it; the exact nanosecond bounds are still applied.
SELECT TraceId AS trace_id, toUnixTimestamp64Nano(Timestamp) AS start_ns, Duration AS duration_ns, SpanName AS name
FROM otel.otel_traces
WHERE ServiceName = {service:String}
  AND ParentSpanId = ''
  AND toDateTime(Timestamp) >= toDateTime(fromUnixTimestamp64Nano({from_ns:Int64}))
  AND toDateTime(Timestamp) <= toDateTime(fromUnixTimestamp64Nano({to_ns:Int64}))
  AND Timestamp >= fromUnixTimestamp64Nano({from_ns:Int64})
  AND Timestamp < fromUnixTimestamp64Nano({to_ns:Int64})
ORDER BY Timestamp DESC, TraceId
LIMIT 50
