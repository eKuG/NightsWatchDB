-- tuned: otel.otel_traces_text, one row per span, text = six values joined by NUL; index on lower(text), splitByNonAlpha.
-- not: terms [common, mid]: t1 AND NOT t2
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces_text
WHERE hasAllTokens(lower(text), [{t1:String}])
  AND NOT hasAllTokens(lower(text), [{t2:String}])
