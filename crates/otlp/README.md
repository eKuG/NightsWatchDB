# nwdb-otlp

Converts a serialized OTLP `ExportTraceServiceRequest` into batches of the
span table (`nwdb_core::span_schema()`), and span-table batches back into OTLP
bytes. It reads the protobuf wire format itself and has no protobuf dependency.

```rust
let decoder = nwdb_otlp::TraceDecoder::new(nwdb_core::BatchLimits::DEFAULT);
let batches = decoder.decode(request_bytes)?;               // rows in request order
let (batches, stats) = decoder.decode_with_stats(request_bytes)?;
let otlp_bytes = nwdb_otlp::encode_spans(&batches)?;
```

`TraceDecoder` also implements `nwdb_core::RecordDecoder`, registered under
`RECORD_TAG_OTLP_TRACES` (1), so the engine can replay its log with it.

## Column mapping

| Column | Source |
|---|---|
| `trace_id`, `span_id` | `Span.trace_id` (16 bytes), `Span.span_id` (8 bytes); a span with any other length, or an all-zero id, is rejected (see below) |
| `parent_span_id` | `Span.parent_span_id`; null when empty or all zero |
| `name`, `kind`, `start_ns`, `end_ns`, `flags` | the span fields of the same name |
| `status_code`, `status_message` | `Span.status.code`, `Span.status.message` (empty is null) |
| `trace_state` | `Span.trace_state` (empty is null) |
| `dropped_*_count` | `Span.dropped_{attributes,events,links}_count` |
| `service_name` | string value of the resource attribute `service.name` |
| `scope_name`, `scope_version` | `InstrumentationScope.name`, `.version` (empty is null) |
| `resource_schema_url`, `scope_schema_url` | `ResourceSpans.schema_url`, `ScopeSpans.schema_url` (empty is null) |
| `operation` | `gen_ai.operation.name` |
| `provider` | `gen_ai.provider.name`, else `gen_ai.system` |
| `request_model` | `gen_ai.request.model` |
| `response_model` | `gen_ai.response.model` |
| `input_tokens` | `gen_ai.usage.input_tokens`, else `gen_ai.usage.prompt_tokens` |
| `output_tokens` | `gen_ai.usage.output_tokens`, else `gen_ai.usage.completion_tokens` |
| `conversation_id` | `gen_ai.conversation.id` |
| `session_id` | `session.id` |
| `user_id` | `user.id` |
| `agent_name` | `gen_ai.agent.name` |
| `tool_name` | `gen_ai.tool.name` |
| `error_type` | `error.type` |
| `resource`, `scope` | the `Resource` and `InstrumentationScope` messages exactly as received (so scope attributes and dropped-attributes count are kept); null when absent |

`service_name`, `scope_name`, `scope_version`, `resource_schema_url`,
`scope_schema_url`, `resource` and `scope` are the origin columns. They are
dictionary columns of the span table: a value that many spans share is stored
once per batch, and each row holds a key.
| `attrs` | every span `KeyValue`, bytes as received, each preceded by its length as a varint |
| `events`, `links` | the same for `Span.Event` and `Span.Link`; null when there are none |

The promotion table is the `PROMOTED` constant in `src/decode.rs`: new name
first, older names after. Promoted columns are copies; `attrs` keeps the
originals. A text column takes a string value. A token column takes an int
value, or a string that parses as a base-10 `i64`. Any other value type is
unusable. The first name, newest first, whose value is usable selects the
value, so a newer name beats an older one whatever their order, and a newer
name with an unusable value does not hide a usable older one. If no name has a
usable value the column is null. A key that occurs twice takes its last
occurrence.

## Wire rules

- Varints, 64-bit, length-delimited and 32-bit fields are walked directly;
  unknown fields are skipped by wire type, at every level.
- Fields may come in any order. A resource after the scope spans, or a scope
  after its spans, is fine (two passes over each enclosing message).
- A field that should occur once takes its last occurrence. A repeated
  `resource`, `scope` or `status` message, or a repeated `value` of a
  `KeyValue`, is replaced, not merged as a strict protobuf decoder would
  (merging would, for example, keep the first `value` when a second, empty one
  follows it). This is a deliberate deviation: no real sender encodes these
  fields twice, and the attribute is kept as received in `attrs`. A test pins
  it.
- `Error::Corrupt`, with the field path in the message, for: group wire types
  and the reserved wire types 6 and 7, field number 0, truncated input, a length
  past the end of the enclosing message, a varint over 10 bytes or over 64 bits,
  the wrong wire type on a known field, a parent span id that is neither empty
  nor 8 bytes, and invalid UTF-8 in any string the
  decoder reads (name, trace state, status message, scope name and version,
  both schema URLs, attribute keys, and the values of promoted attributes and `service.name`).
- Strings over 1 GiB are rejected, because the table's `Utf8` columns use 32-bit
  offsets.
- No `unsafe`, no panics on any input, and no allocation from a length that has
  not been compared with the remaining input.

## Rejected spans

A span is invalid when its trace id is not 16 bytes or is all zero, or its
span id is not 8 bytes or is all zero. OTLP defines an all-zero id as invalid,
the same as an empty one, and the engine removes duplicate spans by span id,
so several spans with a zero id in one trace would collapse into one and
acknowledged data would be lost.

An invalid span is skipped: it is not stored and it does not fail the request.
`DecodeStats::rejected_spans` counts it and `rejected_reason` holds a short
sentence about the first one (`span 2 of the request: trace_id is all zero`),
for the reply to the sender. `decode`, which the log replay uses, skips exactly
the same spans, so replay gives the same rows as the first decode. A request
whose spans are all invalid decodes to no batches without an error.

Damage to the structure of the request (see the wire rules) still fails the
whole request.

## Limits

One origin value (the serialized resource, its schema URL, the serialized
scope, its schema URL) may be at most 64 KiB
(`DEFAULT_MAX_ORIGIN_BYTES`, `TraceDecoder::with_max_origin_bytes`). All spans
of a `ResourceSpans` or `ScopeSpans` with a larger value are rejected and
counted like the spans above, with a reason that names the value; the rest of
the request is decoded. The contents of an oversized resource or scope are
not read.

## Cost

Strings are copied once, from the request into the Arrow builder. `attrs`,
`events` and `links` are not contiguous in the request (each element has a tag
byte before it), so each is assembled in a reused scratch buffer and then
appended to the builder: two copies for those columns. Writing straight into
the builder needs a streaming append in `nwdb-core`.

The origin values are not copied per span. The decoder hashes and stores the
resource-level values once per `ResourceSpans` and the scope-level values once
per `ScopeSpans` (`SpanBatchBuilder::intern_origin`), and each span then
appends four bytes of key for each of the seven origin columns
(`append_span`). The work per span does not depend on the size of the origin,
and the decoded batches hold each distinct origin value once per batch. When a
batch fills and is finished in the middle of a message, the next span interns
the origin again, so each batch is complete in itself. A message with no span
that is stored interns nothing.

### Memory of a decoded request

The memory of the decoded batches (`get_array_memory_size` summed) is at most
8 times the size of the request, plus 64 KiB for each batch, for the shapes
the tests try: one large resource with many small spans, many small
`ResourceSpans` each with its own resource, many small `ScopeSpans` under one
large resource, and requests of random shape. The origin limit and the
request size bound the dictionaries; the keys cost 28 bytes a row.

Spans that carry almost nothing are the exception. A span with only its two
ids is 30 bytes on the wire (with the field header around it) and takes about
195 bytes in the table, because every column has a slot per row (offsets,
fixed-width values, 28 bytes of keys). Arrow's buffers also grow by doubling,
so a batch whose row count is just past a power of two holds up to twice what
its rows need. Measured on requests of only such spans, the worst case is 10.1
times the request size (4,097 spans in one batch; 6 to 7 times for 8,192
rows), not counting the 64 KiB per batch. A span with a short
name and two timestamps is about 60 bytes on the wire and stays under the 8
times bound.
Both are pinned by `crates/otlp/tests/regressions_decode.rs`.

## JSON view of the raw columns

`write_attrs`, `write_resource`, `write_scope_attrs`, `write_events` and
`write_links` write JSON text straight to a `std::io::Write` in one pass. They
use a fixed 8 KiB buffer plus the recursion stack (at most `MAX_DEPTH`, 64,
values deep), so the memory does not grow with the column, and the only error
they return is the writer's own: whatever the stored bytes hold, the output is
valid JSON. `resource_dropped_attributes` and `scope_dropped_attributes` give
the `dropped_attributes_count` of a resource and a scope (0 when absent or
unreadable). The older `attrs_to_json` and its siblings build a
`serde_json::Value`, fail on the first thing they cannot read and need hundreds
of times the column's size in memory; they are superseded and will be removed.

Mapping: string, bool, int (number), double (number; `"NaN"`, `"Infinity"` and
`"-Infinity"` as strings), array, key-value list (object), bytes (standard
base64), unset (null). Events carry `time_ns`, `name`, `attributes` and
`dropped_attributes_count` when not zero. Links carry `trace_id` and `span_id`
(hex), `trace_state` when not empty, `flags` when not zero, `attributes` and
`dropped_attributes_count` when not zero.

Deviations from what a protobuf parser would make of the same bytes:

- A string or key that is not valid UTF-8 is written with each invalid
  sequence replaced by U+FFFD.
- What cannot be read as the message it should be (a key-value, an any-value,
  an event, a link, or the rest of a column once its framing is broken) and a
  value nested deeper than `MAX_DEPTH` is written as
  `{"$unparsed": "<base64 of the raw bytes>"}` in the place of the value; for
  an unreadable entry of an object, as the member `"$unparsed"` (it repeats if
  that happens more than once in one object). The walk goes on with the next
  sibling when the framing allows it.
- Keys are written in stored order and are not de-duplicated: a key the sender
  repeated appears twice. JSON parsers keep the last occurrence, which is the
  rule the promoted columns use. De-duplicating would need memory proportional
  to the number of keys.
- When a `KeyValue` carries its `value` field twice, the last occurrence is
  shown. Protobuf merges them (a second string replaces the first, an empty
  second value leaves the first in place, and two key-value lists are
  appended); merging needs the whole message in memory.

## The protobuf view is always valid OTLP

The decoder keeps attributes, events, links, resource and scope as sent
without reading them, so `encode_spans` checks each stored piece before writing
it, with a walk that allocates nothing: the structure of its message type at
every level, wire types of known fields, and UTF-8 in every string field. A
piece that passes is written byte for byte as stored. Otherwise a repaired copy
is written:

- A string that is not UTF-8 gets U+FFFD for each invalid sequence.
- A key-value, event or link that cannot be read is left out and counted in the
  `dropped_attributes_count`, `dropped_events_count` or `dropped_links_count`
  of the message it was in (for an event or link attribute, of the event or
  link). The rest of a column whose length prefixes are broken counts as one.
- A value nested more than 64 messages deep (a value in an array in an array is
  four) is treated as unreadable. This is counted in messages rather than
  values because common parsers refuse more than 100 levels of messages.
- A resource or scope that cannot be read at all is written as an empty
  message.

## Not handled

- The bodies of `events` and `links`, and attribute values that are not
  promoted, are not looked into when decoding: they are stored as received and
  malformed contents there are not detected. The readers above cope with that.
- OTLP/JSON and OTLP/HTTP framing, gzip, gRPC: the input is the protobuf body.
- Metrics and logs.
- `encode_spans` groups on consecutive equal resource bytes and resource
  schema URL, and within those on equal scope bytes and scope schema URL, so
  two `ResourceSpans` or `ScopeSpans` that were adjacent and equal come back
  merged. A row with no `scope` bytes but a scope name or version gets a scope
  rebuilt from those two.
