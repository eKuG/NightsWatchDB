-- Distinct trace ids in start order (trace start = earliest span start, ties by id). The caller keeps
-- ids at positions 0, k, 2k, ... with k = max(1, distinct / n), n ids in all.
SELECT TraceId FROM otel.otel_traces WHERE TraceId != ''
GROUP BY TraceId ORDER BY min(Timestamp), TraceId
