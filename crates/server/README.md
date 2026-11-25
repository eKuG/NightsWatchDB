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
`127.0.0.1`. The container image listens on `0.0.0.0` because a published port
is the access control there; publish the ports only on a network you trust
(for example `-p 127.0.0.1:4320:4320`).

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
| `--help` | | print the options |
| `--version` | | print the version |

Addresses are `IP:PORT`; host names are not resolved. The data directory is
locked: a second process on the same directory fails to start.

Logs are plain lines on standard error: the addresses, a recovery summary,
shutdown and errors. Nothing is logged per request.

## The OTLP door

Service `opentelemetry.proto.collector.trace.v1.TraceService`, method
`Export`. Messages up to 64 MiB are accepted (or the log's record limit, if
that is smaller).

For each request:

1. It takes its size from a budget of bytes in flight (256 MiB), so many large
   concurrent requests cannot use unbounded memory. Requests wait for room.
2. It is decoded on a blocking thread.
3. A request with no spans gets a success reply and nothing is stored or
   logged.
4. Otherwise it is handed to the engine. **The reply is sent only after the
   record is durable in the write-ahead log.**
5. The reply is an empty `ExportTraceServiceResponse`.

| Situation | gRPC status | Sender should |
|---|---|---|
| The request does not decode | `INVALID_ARGUMENT` | not retry |
| Buffers full (`Unavailable`) | `UNAVAILABLE` | retry later |
| Fatal engine error | `UNAVAILABLE` | retry; the process shuts down and exits with code 1 so a supervisor restarts it and recovery runs |
| Anything else | `INTERNAL` | |

## The HTTP API

All responses are JSON, except `/healthz` and the protobuf form of a trace.
Errors are `{"error": "<message>"}`: 400 for bad input (including an unknown
parameter name), 404 for a missing trace or path, 405 for a method other than
GET, 503 for an engine that is unavailable or fatal, 500 otherwise. Fields
that are null are left out.

Time values are Unix nanoseconds. `from` is inclusive and `to` exclusive. By
default `to` is now and `from` is one hour earlier.

**`GET /healthz`** returns `ok`.

```sh
curl http://127.0.0.1:4320/healthz
```

**`GET /api/v1/stats`** returns the engine counters: `records_ingested`,
`bytes_ingested`, `acked_lsn`, `flushed_lsn`, `buffered_bytes`, `parts`,
`flushes`, `flushing`, `last_error`.

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
and `resource` and `attributes` (objects), `scope_attributes` (object, only
when the scope has attributes), `events` and `links` (arrays).
Attribute values map as follows: strings, bools and ints as themselves;
doubles as numbers, with NaN and the infinities as the strings `"NaN"`,
`"Infinity"`, `"-Infinity"`; arrays as arrays; key-value lists as objects;
bytes as base64; an unset value as `null`. When a key occurs twice the last
value wins.

With `Accept: application/x-protobuf` the response is the trace as an OTLP
`ExportTraceServiceRequest` instead:

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

**`GET /api/v1/tokens?from=&to=&operation=`** sums token counts per request
model over spans that start in the range.

```sh
curl 'http://127.0.0.1:4320/api/v1/tokens?operation=chat'
# {"models":[{"model":"gpt-4o","count":42,"input_tokens":12000,"output_tokens":3400}]}
```

## Shutdown

On SIGTERM or SIGINT the server stops accepting connections, lets requests in
progress finish, flushes the engine and exits with code 0. After a fatal engine
error it does the same as far as it can and exits with code 1. After a crash or
`kill -9` the next start replays the log; nothing that was acknowledged is lost.

## What is missing

- OTLP over HTTP, and logs and metrics (only trace export over gRPC).
- Authentication and TLS.
- Streaming of large traces: a whole trace is held in memory to render it.
- Counts and token sums may include a span twice if a sender retried it across
  a flush (see the engine README, "Known limits").
