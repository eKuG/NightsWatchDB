-- tuned: otel.otel_traces_text, one row per span, text = six values joined by NUL; index on lower(text), splitByNonAlpha.
-- or2: terms [rare, mid]
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces_text
WHERE hasAnyTokens(lower(text), [{t1:String}, {t2:String}])
