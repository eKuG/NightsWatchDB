-- tuned: otel.otel_traces_text, one row per span, text = six values joined by NUL; index on lower(text), splitByNonAlpha.
-- prefix: a token starting with {pre}. Step 1 expands the prefix against the index's own dictionary
-- (mergeTreeTextIndex lists the lower-cased tokens of the part); step 2 is an exact hasAnyTokens over the
-- expanded tokens, answered from the index alone (direct read). Tokens over 64 bytes are skipped to
-- follow the reference tokenizer, which discards them.
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces_text
WHERE hasAnyTokens(lower(text), (
    SELECT groupArray(token)
    FROM mergeTreeTextIndex('otel', 'otel_traces_text', 'idx_text')
    WHERE startsWith(token, lower({pre:String})) AND length(token) <= 64))
