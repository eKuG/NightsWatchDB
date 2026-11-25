# Experiment C result: both ingest doors work; they cost very different things

Run 2026-11-25 with `official.sh`. Docker Desktop on an Apple M4 Pro
(linux/arm64 VM). The receiver ran alone at 2 CPUs and 4 GB with a
write-ahead log (fsync before acknowledging, 10 ms group window) on a Docker
volume. Senders were not limited. Collector: otelcol-contrib 0.165.0.
Datasets: 8.02 GB of agent traces (91,419 spans) and 10,000,039 small spans
without text (3.62 GB), both seed 1.

## Correctness

Every path produced exactly the same rows, token sums, non-null counts and
bytes per text column: OTLP with full protobuf decoding, OTLP with our own
wire walker, OTLP through a collector, and OTel-Arrow through a collector into
our own receiver. They also equal the figures two earlier experiments computed
independently from the generator's file. Unit tests pass in containers
(7 for the OTLP door, 13 for the OTel-Arrow door).

The OTel-Arrow run on the text data hit the receiver's dictionary memory cap
8 times. Each time the receiver refused the batch, the exporter retried on a
new stream, and nothing was lost or duplicated.

## Agent traces with text: 8.02 GB

| Path | Receiver CPU s | per GB | Receiver memory | Log on disk | Collector sends | Collector CPU s |
|---|---|---|---|---|---|---|
| Sender -> OTLP door, wire walker | 8.0 | 1.00 | 57 MB | 8,018 MB | | |
| Sender -> OTLP door, full decode | 8.2 | 1.03 | 67 MB | 8,018 MB | | |
| Sender -> OTLP door, no decode | 6.8 | 0.85 | 16 MB | 8,018 MB | | |
| Sender -> collector -> OTLP door | 5.6 | 0.70 | 246 MB | 8,018 MB | 469 MB | 15.6 |
| Sender -> collector -> OTel-Arrow door | 36.1 | 4.50 | 2,296 MB | 84 MB | 97 MB | 43.9 |

## Small spans without text: 10,000,039 spans

| Path | Receiver CPU s | per million spans | Receiver memory | Requests | Collector sends | Collector CPU s |
|---|---|---|---|---|---|---|
| Sender -> OTLP door, wire walker | 26.6 | 2.66 | 10 MB | 19,560 | | |
| Sender -> OTLP door, full decode | 35.5 | 3.55 | 12 MB | 19,560 | | |
| Sender -> collector -> OTLP door | 5.9 | 0.59 | 46 MB | 458 (batched by collector) | 442 MB | 27.7 |
| Sender -> collector -> OTel-Arrow door | 3.4 | 0.34 | 83 MB | | 361 MB | 69.4 |

## Reading

- **The OTLP door is cheap and small.** About one CPU-second per GB of
  agent-trace data in 57 MB of memory. Receiving and logging are 85% of that;
  decoding is the small part.
- **Request size matters more than the decoder.** The same ten million spans
  cost 26.6 CPU-seconds as 19,560 small requests and 5.9 as 458 large ones,
  because each request pays for its own log sync and HTTP/2 handling. Group
  commit only helps when several senders overlap.
- **Our own wire walker** saves 25% on small spans and 3% on text. Worth
  keeping, not decisive.
- **OTel-Arrow is a real saving on the wire and on the log for text-heavy
  traces**: 97 MB sent against 469 MB for compressed OTLP, and an 84 MB log
  against 8 GB. The exporter sorts attribute values, so conversation texts that
  share long prefixes sit together and compress away.
- **It is expensive at the receiver on that same data, as built**: 6 times the
  CPU and 9 times the memory of the OTLP door, because the exporter keeps
  every distinct text in a per-stream dictionary that the receiver must hold,
  and this receiver copies the dictionary each time it grows. At 1 GB the
  receiver cost 1.1 CPU-seconds per GB; at 8 GB, 4.5. That growth is the
  copying, and it is the first thing to fix.
- **The sender pays too**: the collector uses 2.5 to 2.8 times the CPU to
  produce OTel-Arrow.
- **On small spans** OTel-Arrow is the cheapest door at the receiver, saves
  only 1.2 times on the wire, and still costs the sender 2.5 times the CPU.

## Against the ClickHouse pipeline

ClickHouse with its collector used 87.8 CPU-seconds to ingest the text dataset
and 124 for the small spans. Adding our measured parts (door, then the text
store, span tables and text index from experiments A and B):

| | Ours, direct | Ours, via collector | ClickHouse + collector |
|---|---|---|---|
| Text dataset, CPU s | about 21 | about 34 | 87.8 |
| Small spans, CPU s | about 47 | about 54 | 124 |

These are sums of separate measurements, not one run. Our storage steps each
decode the input again, which overstates our total. ClickHouse's figure
includes background merges during the load and has no text index.

## Caveats

Docker on a Mac, one run each, one sender for the direct runs. The OTel-Arrow
receiver is a first working version: correct, not tuned. The dictionary cap
was exercised only by this run. Nothing reads the write-ahead log back. The
logs and metrics paths were exercised only in smoke runs with Blitz
(300,000 log records and 404,445 metric points, counts matching).
