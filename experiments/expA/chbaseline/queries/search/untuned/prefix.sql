-- untuned: exporter table as is. v1..v6 are the six content attributes; a row's tokens are the union of their tokens.
-- hasTokenCaseInsensitive splits on ASCII non-alphanumerics (bytes >= 0x80 are token bytes), same as the reference tokenizer (checked byte by byte).
-- prefix: a token starting with {pre}: start of value or a non-token byte, then the prefix
WITH
  SpanAttributes['gen_ai.input.messages'] AS v1,
  SpanAttributes['gen_ai.output.messages'] AS v2,
  SpanAttributes['gen_ai.system_instructions'] AS v3,
  SpanAttributes['gen_ai.tool.definitions'] AS v4,
  SpanAttributes['gen_ai.tool.call.arguments'] AS v5,
  SpanAttributes['gen_ai.tool.call.result'] AS v6
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces
WHERE (match(v1, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})) OR match(v2, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})) OR match(v3, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})) OR match(v4, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})) OR match(v5, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})) OR match(v6, concat('(?i)(?:^|[^a-z0-9\\x{80}-\\x{10FFFF}])', {pre:String})))
