# nwdb-core

Layer 0 of NightsWatchDB: the types every other crate shares.

- `TraceId`, `SpanId`
- `Error` / `Result`
- time windows (`window_start`)
- `BatchLimits` (byte-bounded buffers)
- the span table: schema, `SpanRecord`, `SpanBatchBuilder`, `SpanColumns`
- `TraceSummary`, `SummaryBuilder`, `summary_schema`
- `PartMeta`

No file or network I/O, no async, no protobuf. See the crate docs
(`cargo doc -p nwdb-core --open`) for the encoding of the raw columns.
