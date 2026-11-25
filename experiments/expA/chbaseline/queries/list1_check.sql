-- Definition-literal version of list1 (no key-shaped bounds), used only to check list1's answer.
SELECT TraceId AS trace_id, toUnixTimestamp64Nano(Timestamp) AS start_ns, Duration AS duration_ns, SpanName AS name
FROM otel.otel_traces
WHERE ParentSpanId = '' AND ServiceName = {service:String}
  AND Timestamp >= fromUnixTimestamp64Nano({from_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({to_ns:Int64})
ORDER BY Timestamp DESC, TraceId
LIMIT 50
