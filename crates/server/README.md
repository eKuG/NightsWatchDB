# nightswatchdb (the server)

The first runnable form of NightsWatchDB. One process opens a data directory
with the storage engine and offers two doors:

- an **OTLP gRPC door** that accepts trace exports, and
- a small **HTTP API** that reads traces back.

The crate is a library (`nwdb_server`, so tests can start a server on
ephemeral ports) plus the binary `nightswatchdb`.

## Security: read this first

There is **no authentication and no TLS**. Trace content includes prompts and
model output. For that reason both listeners default to the loopback address
`127.0.0.1`.

- **Host check.** A web page can make a browser send requests to
  `127.0.0.1` under the page's own host name (DNS rebinding) and read the
  answers. The API therefore refuses, with 421 and a JSON error, every request
  whose `Host` header (port ignored) is not `localhost`, a loopback IP address,
  the IP address the API is bound to, or a name from `--api-allowed-hosts`.
  If the API is bound to a non-loopback address and no allowed hosts are
  configured, the check is **off**: whoever exposed it did so on purpose, and
  the server cannot know which names reach it. Set `--api-allowed-hosts`
  whenever the API is reachable by a name other than an IP address.
- **Docker.** The container image listens on `0.0.0.0` inside the container,
  so the published port is the access control. Publish the ports on the
  loopback address only: `-p 127.0.0.1:4317:4317 -p 127.0.0.1:4320:4320`.
  `-p 4320:4320` publishes to every interface of the host. The host check does
  not protect that case (see above); a network firewall or `-p 127.0.0.1:...`
  does.
- **Files.** A data directory that the server creates has mode 0700. An
  existing directory is not changed.

## Running it

Binary:

```sh
cargo build --release -p nightswatchdb
./target/release/nightswatchdb --data-dir ./data
```

Container (build from the repository root):

```sh
docker build -t nightswatchdb .
docker run -d --name nwdb \
  -v nwdb-data:/var/lib/nightswatchdb \
  -p 127.0.0.1:4317:4317 -p 127.0.0.1:4320:4320 \
  nightswatchdb
```

The image runs as a non-root user and keeps its data in the volume
`/var/lib/nightswatchdb`. Stop it with `docker stop`: SIGTERM flushes the
database and the process exits with code 0. `scripts/e2e.sh` runs the image
against generated traces, including a `kill -9`.

Point an OpenTelemetry Collector at it with the `otlp` exporter, endpoint
`127.0.0.1:4317`, `tls: insecure: true`. gzip is the exporter's default and is
accepted, as is zstd.

## Configuration

Each setting is a flag, with an environment variable as fallback. A flag wins.
Both `--flag value` and `--flag=value` work.

| Flag | Environment | Default |
|---|---|---|
| `--data-dir` | `NWDB_DATA_DIR` | `./data` |
| `--otlp-grpc-addr` | `NWDB_OTLP_GRPC_ADDR` | `127.0.0.1:4317` |
| `--api-addr` | `NWDB_API_ADDR` | `127.0.0.1:4320` |
| `--api-allowed-hosts` | `NWDB_API_ALLOWED_HOSTS` | none; comma-separated host names |
| `--accept-lost-log` | `NWDB_ACCEPT_LOST_LOG` | off; a switch (the variable is `1`, `true`, `0` or `false`) |
| `--help` | | print the options |
| `--version` | | print the version |

Addresses are `IP:PORT`; host names are not resolved. The data directory is
locked: a second process on the same directory fails to start. An argument or
a `NWDB_*` variable that is not valid UTF-8 is a usage error (exit code 2).

### A lost log

The database records how far the write-ahead log has been flushed into parts.
If the log's files are gone or end below that position (a deleted `wal`
directory, a restored partial backup), the records acknowledged after the last
flush are lost, and the engine refuses to start rather than carry on as if
nothing happened. The process prints the engine's message, a line naming
`--accept-lost-log`, and exits with code 1. First look for the files: restoring
them lets the database open with nothing lost. Pass `--accept-lost-log` (or
set `NWDB_ACCEPT_LOST_LOG=1`) only when they cannot be found and losing those
records is acceptable: the server then starts a new log above the position and
carries on with the parts that exist. A new database is never refused.

Logs are plain lines on standard error: the addresses, a recovery summary,
shutdown and errors. Nothing is logged per request.

## The OTLP door

The door is a small HTTP/2 server of its own (no TLS, prior knowledge only),
not a gRPC library: it reads the call itself so that every byte is counted
before it is held. Service `opentelemetry.proto.collector.trace.v1.TraceService`,
method `Export`. Messages up to 64 MiB are accepted (or the log's record
limit, if that is smaller), uncompressed or after decompression.

Limits, with their defaults. The ones with a field name are set in
`ServerOptions` (for a program that embeds the server); the binary uses the
defaults and has no flags for them.

| Limit | Default |
|---|---|
| Open connections (`otlp_max_connections`) | 1024; more wait in the accept queue |
| Concurrent streams per connection | 64; a 65th is refused with `REFUSED_STREAM` |
| Export calls in progress (`max_calls`) | 256, counted from before the first byte of the body is read |
| Header list size | 16 KiB |
| HTTP/2 flow-control window | 128 KiB per stream and per connection, adaptive windows off; largest frame 16 KiB |
| Keepalive pings | every 30 s, a connection that does not answer within 10 s is closed |
| Silence from a sender in the middle of a call (`stall_timeout`) | 20 s, then `DEADLINE_EXCEEDED` |
| Time to deliver a whole message (`receive_timeout`) | 30 s from the moment its budget is taken, then `DEADLINE_EXCEEDED` |
| Wait for a call slot or for budget (`budget_wait`) | 5 s, then `UNAVAILABLE` |
| Budget (`max_inflight_bytes`) | 512 MiB, see below |
| Zstd window | at most 8 MiB |

For each call:

1. `grpc-encoding` may be absent, `identity`, `gzip` or `zstd`. Replies always
   carry `grpc-accept-encoding: identity,gzip,zstd`.
2. The call takes one of the `max_calls` slots before the first byte of its
   body is read, and keeps it until the call ends. A call that cannot get a
   slot within `budget_wait` is refused with `UNAVAILABLE`.
3. The 5-byte message header is read. A length above the limit is refused at
   once, before any payload is read.
4. The length is taken from the budget, then the payload is read. A call that
   cannot get the budget within `budget_wait` is refused with `UNAVAILABLE`.
   From the moment the budget is taken the sender has `receive_timeout` to
   deliver the whole message, however steadily it sends; after that the call
   fails with `DEADLINE_EXCEEDED`, the stream is dropped and the budget is
   free. This deadline stops counting when the message is complete: the time
   the server spends decoding and waiting for the log is not held against the
   sender, because a failure reply after the record is durable would make it
   retry something that is already stored.
5. The stream must end after the message. A second message, or any further
   byte, is an error and nothing is stored.
6. A compressed message states its size: the gzip trailer, or the zstd frame's
   content size. Streaming zstd encoders (the ones gRPC libraries in Go use)
   write frames without a size; for those the size is assumed to be 64 times
   the frame's length, at least 1 MiB and at most the limit. If the frame
   expands to more than that, the output is discarded, the charge is given
   back, the whole decoding pool is taken (so this call runs alone) and the
   frame is expanded again with the limit as the cap; more than the limit is
   `RESOURCE_EXHAUSTED`. A size above the limit is refused. For a stated size,
   more output, a different size, bytes after the compressed data, or corrupt
   data is an error.
7. The request is decoded on a blocking thread. Spans that cannot be stored (a
   trace or span id of the wrong length or all zero, a resource or scope over
   the decoder's limit) are skipped and counted; the rest is stored. A request
   with no spans left gets a success reply and nothing is logged.
8. Otherwise it is handed to the engine. **The reply is sent only after the
   record is durable in the write-ahead log.** The reply is HTTP 200 with
   `content-type: application/grpc`, one message and the trailer
   `grpc-status: 0`. The message is an empty `ExportTraceServiceResponse`, or,
   if spans were skipped, one with `partial_success` carrying their number and
   the reason; senders do not retry those.

Errors are replies with HTTP 200 and `grpc-status` and `grpc-message` in the
headers and no body:

| Situation | gRPC status | Sender should |
|---|---|---|
| Not `TraceService/Export`, not `application/grpc`, unknown `grpc-encoding` | `UNIMPLEMENTED` | not retry |
| Request does not decode; bad framing; second message; bad or mismatched compressed data | `INVALID_ARGUMENT` | not retry |
| Message, or its decompressed size, above the limit | `RESOURCE_EXHAUSTED` | not retry: it can never succeed |
| Sender silent for too long (`stall_timeout`), or the whole message not delivered in time (`receive_timeout`) | `DEADLINE_EXCEEDED` | retry |
| No call slot or no budget within `budget_wait`; the message says which limit | `UNAVAILABLE` | retry with back-off |
| Buffers full (`Unavailable`), or shutting down | `UNAVAILABLE` | retry later |
| Fatal engine error | `UNAVAILABLE` | retry; the process shuts down and exits with code 1 so a supervisor restarts it and recovery runs |
| Anything else | `INTERNAL` | |

### What the budget bounds

The budget (`max_inflight_bytes`, 512 MiB by default) is split in two halves
of one pool each:

- **Bytes read.** A message takes its length from the first half before its
  payload is read, and keeps it until the record is durable or the call has
  failed. Stalled and slow senders therefore hold only what they have sent, and
  only up to the budget, and only until `receive_timeout`.
- **Expansion and decoding.** Before decompression and decoding a message
  takes 16 times its decompressed size plus 64 KiB from the second half (the
  stated size for compressed messages, the assumed size for zstd frames that
  state none). Decoded batches are the main memory use. The factor comes from
  the heap the decoder holds at its peak, which is more than the finished
  batches: the worst shape measured is 13.6 times the request (see the README
  of the otlp crate), and a compressed message also has its decompressed
  request in this pool, one more time its size. The test
  `decoding_never_needs_more_than_the_door_charges` checks the charge against
  the worst shapes at several sizes, using the constants the door uses.

Two pools, not one, so that a request never waits for more of a pool it holds
a part of. A request that needs more than a whole pool is charged the whole
pool: it runs alone instead of never. So the budget is a bound for requests
that fit it; a single request can use more (up to about 64 MiB uncompressed,
more while decoding).

The permits are held by the work, not by the call. If the sender resets its
stream or closes the connection while a message is being expanded and decoded,
the blocking task goes on, and the charge stays taken until that task has
finished and freed its memory. Once the record is handed to the engine, the
charge is given back when the log has it, whether or not the call is still
there.

What remains **outside** the budget, and its bound:

- **First frames.** A call reads the first DATA frame of its body (up to
  16 KiB) before it knows the message's length and so before it can wait for
  budget. At most `max_calls` calls are past the slot at once, so this is at
  most `max_calls` x 16 KiB: 4 MiB by default. Streams that wait for a slot
  have read nothing.
- **What HTTP/2 buffers.** Data a client has sent that no call has read yet
  waits in the HTTP/2 library, bounded by flow control: the connection window
  is 128 KiB and is shared by all streams of the connection, so a connection
  holds at most 128 KiB of unread data (128 MiB for 1024 connections).
  Windows are set explicitly (128 KiB per stream and per connection, no
  adaptive resizing, frames of 16 KiB). Each open stream also holds its
  request headers, at most 16 KiB (the header list limit) and in practice a
  few hundred bytes; with 64 streams a connection can hold up to 1 MiB of
  them. These bounds multiply with `otlp_max_connections`; lower it if the
  door is exposed to many untrusted connections.
- **The engine's buffers**, which hold accepted records until they are flushed
  and have their own limits (see the engine README).

## The HTTP API

The API speaks HTTP/1.1 only. It serves at most 256 connections at once (more
wait in the accept queue), gives a client 10 s to send the request headers,
and runs at most 64 requests at once (more wait), except `/healthz` and
`/api/v1/stats`, which answer from memory and never wait for a slot. A request
holds its slot until its response has been sent, and also as long as work for it
is running: a query that was started keeps the slot until it has finished even
if the client has gone, and the task that renders a trace keeps it for as long
as it holds the trace's rows.

A client that does not read its response cannot hold a slot for ever: a
response that makes no progress on the socket for `api_write_timeout` (30 s by
default, a field of `ServerOptions`) is aborted. The connection is closed, the
task that renders the response ends, and its rows and its slot are freed. A
client that reads slowly but steadily is not cut off.

All responses are JSON, except `/healthz` and the protobuf form of a trace.
Errors are always `{"error": "<message>"}`: 400 for bad input (including an
unknown parameter name and percent-encoding that is wrong or not UTF-8), 404
for a missing trace or path, 405 for a method other than GET, 421 for a `Host`
that is not allowed (see Security), 503 for an engine that is unavailable or
fatal, 500 otherwise. The one exception is a request that the HTTP library
cannot parse at all (for example a header line without a colon): the library
answers it itself with status 400 and no body, before any of this code runs.
Fields that are null are left out. In percent-encoding `%` must be followed by
exactly two hex digits.

Time values are Unix nanoseconds, written as the unsigned numbers the sender
sent. `from` is inclusive and `to` exclusive (both are signed 64-bit numbers).
By default `to` is now and `from` is one hour earlier.

**`GET /healthz`** returns `ok`, or 503 with a JSON error when the engine is in
its fatal state (it accepts and flushes nothing; restart it).

```sh
curl http://127.0.0.1:4320/healthz
```

**`GET /api/v1/stats`** returns the engine counters: `records_ingested`,
`bytes_ingested`, `acked_lsn`, `flushed_lsn`, `buffered_bytes`, `parts`,
`flushes`, `flushing`, `fatal`, `last_error`, `payload_logical_bytes`,
`payload_unique_bytes` and `payload_stored_bytes`. The last three are for the
flushed parts: the large text (prompts, completions, tool definitions and
results, and other long values) their spans hold, counting every use; the same
text with each distinct piece counted once per part; and what that takes on
disk after compression. In agent traces the second is a small fraction of the
first. A trace reads back exactly as it was
sent either way.

```sh
curl http://127.0.0.1:4320/api/v1/stats
```

**`GET /api/v1/traces/{trace_id}`** (32 hex digits) returns all spans of the
trace in the engine's order (by start time).

```sh
curl http://127.0.0.1:4320/api/v1/traces/0af7651916cd43dd8448eb211c80319c
# {"trace_id":"0af7...","spans":[{"span_id":"b7ad...","name":"chat gpt", ...}]}
```

A span has `span_id`, `parent_span_id`, `name`, `kind`, `start_ns`, `end_ns`,
`status_code`, `status_message`, `trace_state`, `service_name`, `scope_name`,
`scope_version`, `resource_schema_url`, `scope_schema_url`, the promoted
columns (`operation`, `provider`,
`request_model`, `response_model`, `input_tokens`, `output_tokens`,
`conversation_id`, `session_id`, `user_id`, `agent_name`, `tool_name`,
`error_type`), `flags` and the three `dropped_*_count` fields when not zero,
and `resource` and `attributes` (objects), `resource_dropped_attributes_count`
and `scope_dropped_attributes_count` (when not zero), `scope_attributes`
(object, only when the scope has attributes), `events` and `links` (arrays).
The JSON of a trace is streamed in chunks of about 64 KiB as it is written
(`Transfer-Encoding: chunked`), so a trace is never held as JSON; the rows of
the trace are still held in memory. Whatever is stored, the output is valid
JSON: bytes that cannot be read are shown as `{"$unparsed": "<base64>"}` (see
the otlp README). If the server fails in the middle of a response, the
connection is cut instead of the body being ended. Keys of an object appear in
stored order and a key the sender repeated appears twice.
Attribute values map as follows: strings, bools and ints as themselves;
doubles as numbers, with NaN and the infinities as the strings `"NaN"`,
`"Infinity"`, `"-Infinity"`; arrays as arrays; key-value lists as objects;
bytes as base64; an unset value as `null`. 

If `Accept` contains the media range `application/x-protobuf` with a quality
other than 0, the response is the trace as an OTLP `ExportTraceServiceRequest`
instead:

```sh
curl -H 'Accept: application/x-protobuf' -o trace.pb http://127.0.0.1:4320/api/v1/traces/0af7651916cd43dd8448eb211c80319c
```

**`GET /api/v1/traces?from=&to=&service=&model=&error=&limit=`** lists trace
summaries. `service` is the root span's service, `model` matches any span's
request model, `error` is `true` or `false`, `limit` is 1 to 1000 (default 20).

```sh
curl 'http://127.0.0.1:4320/api/v1/traces?service=agent&error=true&limit=5'
# {"traces":[{"trace_id":"...","start_ns":...,"end_ns":...,"root_service":"agent",
#   "root_name":"invoke_agent","span_count":12,"error":true,"input_tokens":900,
#   "output_tokens":120,"models":["gpt-4o"],"session_id":"...","user_id":"..."}]}
```

**`GET /api/v1/tokens?from=&to=&operation=&limit=`** sums token counts per
request model over spans that start in the range. Rows are ordered by `count`
descending, then by model name. `limit` is 1 to 10000 (default 1000); when rows
were cut the reply has `"truncated": true`.

```sh
curl 'http://127.0.0.1:4320/api/v1/tokens?operation=chat'
# {"models":[{"model":"gpt-4o","count":42,"input_tokens":12000,"output_tokens":3400}]}
```

## Shutdown

On SIGTERM or SIGINT the server stops accepting connections and asks every
open connection to finish. It waits up to 5 s (`drain_timeout` in
`ServerOptions`) for requests in progress, then drops whatever is left, so an
open but stalled export stream or an idle client never delays shutdown. Then
it flushes the engine and exits with code 0. After a fatal engine error (met by
a request, or by the background flush, which is checked once a second) it
does the same and exits with code 1. A second SIGTERM or SIGINT exits at once
with code 130; that is safe, because the next start replays the log. So does
a SIGTERM or SIGINT that arrives while the server is still starting (opening
the database and replaying the log can take long): nothing has been accepted
yet, the process exits with code 130 at once, and recovery is redone at the
next start. After a
crash or `kill -9` the next start replays the log; nothing that was
acknowledged is lost. Requests that were cut off by the drain deadline were not
acknowledged, and their senders retry.

## What is missing

- OTLP over HTTP, and logs and metrics (only trace export over gRPC).
- Authentication and TLS.
- Streaming from storage: the rows of a trace are held in memory while it is
  rendered, though its JSON is not.
- Counts and token sums may include a span twice if a sender retried it across
  a flush (see the engine README, "Known limits").
