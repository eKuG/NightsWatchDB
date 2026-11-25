-- tuned: otel.otel_traces_text, one row per span, text = six values joined by NUL; index on lower(text), splitByNonAlpha.
-- phrase: {p1} then {p2} separated only by non-token bytes, within one attribute value (the class excludes NUL so it cannot cross values)
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces_text
WHERE hasPhrase(lower(text), [{p1:String}, {p2:String}])   -- index narrows candidates
  AND match(text, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {p1:String}, '[^a-z0-9\\x{80}-\\x{10FFFF}\\x00]+', {p2:String}, '(?:$|[^a-z0-9\\x{80}-\\x{10FFFF}])'))   -- exact, never across NUL
