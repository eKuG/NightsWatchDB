# tracegen

Temporary load generator that emits deterministic, LLM-agent-shaped OpenTelemetry traces for three
storage-engine experiments. It is meant to be deleted once blitz can generate the same data.

Traces have many spans, large text payloads, and heavy repetition (every chat span's input holds the
whole conversation so far). Text comes from a corpus directory you supply, sliced at seeded offsets.
The same flags always produce byte-identical output.

## Example

    go build -o tracegen . && ./tracegen --corpus ../../.data/corpus --bytes 4GB --out-file /tmp/traces.bin

A JSON summary (counts, bytes, throughput, p50/p90/p99/max per profile, chat-span payloads, bytes by attribute
class, repeated-content fractions) is printed to stdout.

## Corpus

`--corpus DIR` may contain role subdirectories `system/`, `user/`, `assistant/`, `tool/`, `tooldefs/` made by
`../corpusprep`. Each holds shard files of records: 4-byte big-endian length, then that many bytes of UTF-8.
`system/` and `tooldefs/` are used as whole entries; the other roles are concatenated and sliced at seeded
offsets aligned to UTF-8 boundaries. A role without a subdirectory falls back to the plain files elsewhere under
DIR (sliced the same way); tool definitions and system prompts are then synthesised from that text. A missing or
empty corpus is an error. `--corpus-max-bytes` caps total bytes loaded; whole-entry roles load first and the
rest is split between the other roles.

Memory use is the loaded corpus (up to `--corpus-max-bytes`) plus roughly 100 MB, independent of run size.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--traces N` / `--bytes SIZE` / `--spans N` | none | Stop after N traces / at least SIZE encoded bytes / at least N spans, always at a trace boundary. Exactly one required. Sizes: `KB MB GB` (decimal), `KiB MiB GiB` (binary). |
| `--corpus DIR` | required | See above |
| `--corpus-max-bytes` | 1GiB | Cap on corpus bytes loaded |
| `--out-file PATH` | | Write framed OTLP requests (format below) |
| `--otlp-endpoint HOST:PORT` | | Send the same batches over OTLP gRPC, insecure. Any export error or rejected span aborts the run |
| `--rate` | 0 | gRPC spans per second, 0 = unlimited |
| `--seed` | 1 | Seed for everything |
| `--mix` | `simple=0.50,chat=0.25,task=0.17,coding=0.08` | Profile weights |
| `--traces-per-session` | 3 | Mean of the geometric traces-per-session distribution |
| `--context-cap` | 500KB | When accumulated messages exceed this, the oldest half is dropped (about 128K tokens) |
| `--max-payload-bytes` | 1MiB | Cap on any one attribute value. Only the input message history is truncated (from the front, whole messages); other values are generated below the cap |
| `--no-content` | off | Omit gen_ai.input.messages, output.messages, system_instructions, tool.definitions, tool.call.arguments and tool.call.result. Everything else, including ids, timestamps and token counts, is identical to the same seed without the flag. Content is still generated internally, so the corpus is still required and the class and repeated-fraction statistics describe the content that was left out |
| `--subagent-prob` | 0 | Chance a tool call is a nested agent run (max 3 agent levels including the root) |
| `--window`, `--start` | 24h, 2026-01-01T00:00:00Z | Trace start times are spread over the window with seeded jitter, non-decreasing. In `--bytes` mode the trace count is unknown, so spacing assumes about 1 MB per trace |
| `--batch-spans`, `--batch-bytes` | 512, 3MiB | A batch closes at whichever limit hits first (bytes counts encoded spans) |

At least one of `--out-file` / `--otlp-endpoint` is required. gRPC sends are synchronous.

## Output file format

A sequence of records, each: 4-byte big-endian length N, then N bytes of one serialized
`opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest` protobuf. No header or footer.
A trace may span several records (batches cut at span or byte limits); a record may hold several traces.
Spans are written in finish order, so children come before their parent and the root is the trace's last span.
`encoded_bytes` in the summary is the file size (length prefixes included).

## Data model

Sessions (session id, user id, one resource) contain a geometric number of traces. A trace is one
`invoke_agent` root for an agent drawn from a pool of 12 names, with steps; each step is a `chat` span then zero
or more `execute_tool` spans. Span kinds: chat CLIENT, execute_tool and invoke_agent INTERNAL.

Each agent name has one system prompt and one tool-definition value, fixed for the whole run and emitted on
every chat span of that agent, as real SDKs do. Every chat span also carries the full conversation so far, so
most input bytes repeat an earlier span. Compaction drops the oldest half of the messages above the context
cap. Message bodies are lognormal slices: first user message median 3 KB / p99 25 KB; assistant median 400 B /
p99 7 KB (max 18 KB); tool result median 500 B / p99 20 KB (coding: 900 B / 30 KB), max 100 KB. Tokens are
bytes/4. Chat spans fail 5% of the time; tool spans 3% (15% in coding); failures set error status and
`error.type`. LLM spans last 1 to 30 s scaled by output size; tool spans 10 ms to 30 s, short-heavy. Tool
definitions come from the `tooldefs/` entry closest in size to a draw from {0.7 KB 20%, 20 KB 50%, 75 KB 25%,
340 KB 5%} (synthesised to that size without a corpus directory). System prompts are whole `system/` entries
or synthesised at 2 to 12 KB, median 5 KB.

Content is emitted as JSON strings (message list, tool definitions). Structured AnyValue encoding of these
attributes is not generated yet.

Default profile table (`profiles` in `gen.go`):

| Profile | LLM calls | Tool calls per non-final call | Tool result median / p99 | Tool error |
|---|---|---|---|---|
| simple (0.50) | 1 to 2 | none | 500 B / 20 KB | 3% |
| chat (0.25) | 3 to 8 | 1 with p=0.05 | 500 B / 20 KB | 3% |
| task (0.17) | lognormal median 12, p90 40, cap 150 | 1 p=0.72, 2 p=0.20, else 0; burst of 3 to 15 p=0.01 | 500 B / 20 KB | 3% |
| coding (0.08) | lognormal median 50, p90 90, cap 150 | exactly 1 | 900 B / 30 KB | 15% |

## Summary statistics

Besides the per-profile and overall distributions, the JSON has `chat_span_payload_bytes`, `bytes_by_class`
(string value bytes of input messages, output messages, system instructions, tool definitions, tool
arguments/results, and everything else as the remainder of the encoded bytes), and `repeated_fraction`: bytes of
input messages, system instructions and tool definitions byte-identical to content already emitted earlier in
the same trace, over the total of those three. `repeated_across_traces` gives the share of system-instruction
and tool-definition bytes whose agent had already appeared in an earlier trace.

## Attribute names

All names live in one struct, `names`, in `gen.go`. GenAI conventions moved out of
`open-telemetry/semantic-conventions` (v1.45.0: `docs/gen-ai/README.md` is a "moved" notice and
`model/gen-ai/` holds only `deprecated/`) to `open-telemetry/semantic-conventions-genai` (latest release v0.1.0;
files read from the default branch). Read there:

- `model/gen-ai/registry.yaml`: gen_ai.operation.name (values chat, execute_tool, invoke_agent),
  gen_ai.provider.name, gen_ai.request.model, gen_ai.response.model, gen_ai.usage.input_tokens,
  gen_ai.usage.output_tokens, gen_ai.conversation.id, gen_ai.agent.name, gen_ai.tool.name,
  gen_ai.tool.call.id, gen_ai.tool.type, gen_ai.tool.call.arguments, gen_ai.tool.call.result,
  gen_ai.response.finish_reasons, gen_ai.input.messages, gen_ai.output.messages, gen_ai.system_instructions,
  gen_ai.tool.definitions
- `model/gen-ai/spans.yaml`: span kinds (inference CLIENT, execute_tool INTERNAL, in-process invoke_agent
  INTERNAL) and span names (`{operation} {model|agent|tool}`)
- `model/gen-ai/gen-ai-input-messages.json`, `gen-ai-output-messages.json`, `gen-ai-system-instructions.json`:
  message shape (role, parts of type text / tool_call / tool_call_response)

Read in `open-telemetry/semantic-conventions` at v1.45.0: `model/error/registry.yaml` (error.type),
`model/user/registry.yaml` (user.id), `model/session/registry.yaml` (session.id).

`gen_ai.tool.definitions` is typed `any` in the registry (key at `model/gen-ai/registry.yaml`).

Not fully confirmed: the exact value `tool_calls` for finish reasons (the registry gives examples such as `stop`,
`length`, `error`, and says values are provider-specific); the tool argument/result attributes are typed `any`
in the registry and are emitted here as strings (arguments as a JSON object string, result as plain text).
The system prompt is emitted in `gen_ai.system_instructions`, not duplicated inside `gen_ai.input.messages`,
as the registry intends.
