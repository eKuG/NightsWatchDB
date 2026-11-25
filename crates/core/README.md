# nwdb-core

Layer 0 of NightsWatchDB: the types every other crate shares.

- `TraceId`, `SpanId`
- `Error` / `Result`
- time windows (`window_start`)
- `BatchLimits` (byte-bounded buffers)
- the span table: schema, `SpanRecord`, `SpanBatchBuilder`, `SpanColumns`.
  The seven origin columns (`resource`, `service_name`, `resource_schema_url`,
  `scope`, `scope_name`, `scope_version`, `scope_schema_url`) are dictionary
  columns: the builder stores each distinct value once per batch and each row
  holds a 4-byte key, so a large resource shared by many spans is not copied
  into every row. `SpanColumns::record` hands out plain slices as before.
- `TraceSummary`, `SummaryBuilder`, `summary_schema`
- `PartMeta`

No file or network I/O, no async, no protobuf. See the crate docs
(`cargo doc -p nwdb-core --open`) for the encoding of the raw columns.
