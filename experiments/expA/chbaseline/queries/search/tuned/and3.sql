-- tuned: otel.otel_traces_text, one row per span, text = six values joined by NUL; index on lower(text), splitByNonAlpha.
-- and3: terms [mid, common, common+1]
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces_text
WHERE hasAllTokens(lower(text), [{t1:String}, {t2:String}, {t3:String}])
