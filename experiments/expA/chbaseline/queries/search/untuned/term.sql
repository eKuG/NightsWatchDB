-- untuned: exporter table as is. v1..v6 are the six content attributes; a row's tokens are the union of their tokens.
-- hasTokenCaseInsensitive splits on ASCII non-alphanumerics (bytes >= 0x80 are token bytes), same as the reference tokenizer (checked byte by byte).
-- classes rare, mid, common: one term {t1}
WITH
  SpanAttributes['gen_ai.input.messages'] AS v1,
  SpanAttributes['gen_ai.output.messages'] AS v2,
  SpanAttributes['gen_ai.system_instructions'] AS v3,
  SpanAttributes['gen_ai.tool.definitions'] AS v4,
  SpanAttributes['gen_ai.tool.call.arguments'] AS v5,
  SpanAttributes['gen_ai.tool.call.result'] AS v6
SELECT count() AS rows, uniqExact(TraceId) AS traces
FROM otel.otel_traces
WHERE (hasTokenCaseInsensitive(v1, {t1:String}) OR hasTokenCaseInsensitive(v2, {t1:String}) OR hasTokenCaseInsensitive(v3, {t1:String}) OR hasTokenCaseInsensitive(v4, {t1:String}) OR hasTokenCaseInsensitive(v5, {t1:String}) OR hasTokenCaseInsensitive(v6, {t1:String}))
