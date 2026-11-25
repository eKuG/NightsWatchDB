-- Parameters shared with the layout stream: S = most frequent root-span service name, M = most frequent
-- gen_ai.request.model, window = middle half of the span start-time range
-- [min + (max-min)/4, min + 3*(max-min)/4) in integer nanoseconds. Ties are broken by value, ascending.
WITH
    (SELECT toUnixTimestamp64Nano(min(Timestamp)) FROM otel.otel_traces) AS mn,
    (SELECT toUnixTimestamp64Nano(max(Timestamp)) FROM otel.otel_traces) AS mx
SELECT
    (SELECT ServiceName FROM otel.otel_traces WHERE ParentSpanId = ''
     GROUP BY ServiceName ORDER BY count() DESC, ServiceName LIMIT 1) AS service,
    (SELECT SpanAttributes['gen_ai.request.model'] AS m FROM otel.otel_traces
     WHERE mapContains(SpanAttributes, 'gen_ai.request.model')
     GROUP BY m ORDER BY count() DESC, m LIMIT 1) AS model,
    mn + intDiv(mx - mn, 4) AS from_ns,
    mn + intDiv(3 * (mx - mn), 4) AS to_ns
