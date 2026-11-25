-- Open one trace: every span with every column (all attributes, events, links), oldest first.
-- This is the lookup the exporter's helper table exists for (pattern from the exporter README): the
-- tiny otel_traces_trace_id_ts table gives the trace's first and last second, which bounds the read of
-- the big table by partition and by the primary key's time component, instead of probing the bloom
-- filter over every granule. The README example has "LIMIT 100"; that is dropped to return all spans.
WITH
    {id:String} AS trace_id,
    (SELECT min(Start) FROM otel.otel_traces_trace_id_ts WHERE TraceId = trace_id) AS start,
    (SELECT max(End) + 1 FROM otel.otel_traces_trace_id_ts WHERE TraceId = trace_id) AS end
SELECT *
FROM otel.otel_traces
WHERE TraceId = trace_id AND Timestamp >= start AND Timestamp <= end
ORDER BY Timestamp, SpanId
