-- Chat spans whose start is in the window, grouped by request model: count and token sums.
-- Attribute values are strings in the Map column, so tokens are converted at read time. The window test
-- is in PREWHERE so the attribute map (the widest column) is read only for granules that have window rows.
SELECT SpanAttributes['gen_ai.request.model'] AS model,
       count() AS count,
       sum(toInt64OrNull(SpanAttributes['gen_ai.usage.input_tokens'])) AS input_tokens,
       sum(toInt64OrNull(SpanAttributes['gen_ai.usage.output_tokens'])) AS output_tokens
FROM otel.otel_traces
PREWHERE Timestamp >= fromUnixTimestamp64Nano({from_ns:Int64})
     AND Timestamp < fromUnixTimestamp64Nano({to_ns:Int64})
WHERE SpanAttributes['gen_ai.operation.name'] = 'chat'
GROUP BY model
ORDER BY model
