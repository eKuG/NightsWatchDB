# Checkpoints

The engine is built in small steps. Each checkpoint is a tested increment:
its tests pass in a Linux container, it has been through an independent
review, and it is committed, tagged `checkpoint-NN` and pushed.

```
Progress  [█████░░░░░░░░░░░]  5 of 16 checkpoints
```

| # | Checkpoint | State |
|---|---|---|
| 0 | Repository set-up: licence, design, experiments and measured results | done |
| 1 | Storage core as libraries: shared types, write-ahead log, span parts (segment files, trace index, summary), manifest | done |
| 2 | Engine: memory buffer, flush, recovery from the log, snapshot reads; open a trace, list traces, aggregate | done |
| 3 | Server: OTLP door for traces, HTTP query API, container image, end-to-end test with the generator | done |
| 4 | Large text out of line, each distinct piece stored once | done |
| 5 | Text index over distinct pieces; search | index library built and reviewed; search not wired in yet |
| 6 | Compaction, retention, late spans | |
| 7 | Scores and annotations | |
| 8 | Logs | |
| 9 | Metrics | |
| 10 | SQL and a Postgres-compatible endpoint | |
| 11 | OTel-Arrow door | |
| 12 | Semantic-convention mapping | |
| 13 | Object-store backend | |
| 14 | Entity graph | |
| 15 | Profiles | |

Benchmarks against the baseline are re-run when a checkpoint changes the
write or read path, and added to [benchmarks](benchmarks/README.md).

## Checkpoint 1: storage core

Four library crates, 152 tests, no server yet.

| Crate | What it does |
|---|---|
| `nwdb-core` | Ids, errors, time windows, byte-based batch limits, the span table and its builder, per-trace summaries and how to merge them, part metadata |
| `nwdb-wal` | Write-ahead log with group commit. A record is acknowledged only after the sync that covers it |
| `nwdb-segment` | One immutable part: span rows with a trace's spans adjacent, a trace index, a per-trace summary. Writer and reader |
| `nwdb-manifest` | The list of live parts, published as numbered files with create-if-absent |

Each crate's README documents its file formats and rules.

An independent review of the first version found twelve defects, each proven
with a failing test, among them three that could lose acknowledged data
without an error. All were fixed and the tests kept as regressions:

- Damage in the middle of the log is now reported and the file left alone; it
  used to be treated as the end of the log, discarding every later record and
  reusing its sequence numbers. A torn tail is still repaired automatically.
- Replaying from a position that was already trimmed, or beyond the end of
  the log, is an error; the engine can also tell the log never to number below
  what is already in data files.
- A missing manifest is never read as an empty database next to existing
  parts, and a manifest must be opened explicitly as new or as existing.
- State read at start-up is synced before it is trusted.
- A component whose state is unknown after a failed sync returns a distinct
  error that means "restart and recover" and refuses further work.
- Damaged Parquet files produce an error; the Parquet library's panics on bad
  input are contained. Span files carry checksums that can be verified.
- Readers never wait for a manifest commit, read batches are bounded by
  bytes, and part files are held open so a read survives the part's deletion.

Known limits at this checkpoint: behaviour under real power loss rests on the
order of writes and syncs and is not yet exercised by crash injection; that
arrives with the engine in checkpoint 2.

## Checkpoint 2: engine

`nwdb-engine` joins the four storage crates into a database library. A
request is written to the log, acknowledged once the log is synced, visible to
queries from that moment, written into parts later, and recovered after a
crash at any point. Queries read a snapshot that neither blocks nor is blocked
by ingest and flush. Three queries exist: open a trace, list traces in a time
range with filters, and token totals per model.

How it was tested:

- A model-based randomized test: random ingest, flushes, restarts, retries
  and malformed traces, with every query compared against a simple model.
- Crash injection at every stage of a flush and of start-up: the process
  aborts at a named point, the on-disk state is checked, the database is
  reopened, and every acknowledged record must be present exactly once.
- Repeated `kill -9` at random moments under load.
- Injected failures and panics inside a flush.
- For every start-up refusal (lost or older manifest, lost log, damaged part,
  a log record that does not decode): the data directory must be byte for
  byte what it was before the refused start.

An independent review found eight defects, six of them proven with failing
tests. All were fixed and the tests kept as regressions:

- Start-up used to delete part directories and commit a manifest version
  before it had checked that the log and the manifest agree. With the newest
  manifest file lost, it deleted the parts only that file knew and then
  refused to start. Start-up now changes nothing until every check has
  passed, and deletes leftovers only after the log has replayed.
- A log that ends before the position the manifest records proves that log
  files were lost. That used to be accepted silently; it is now refused,
  unless the operator explicitly accepts the loss.
- A damaged log file that is no longer needed no longer blocks start-up.
- Token totals counted a retried span twice while it was still in memory.
- A trace's summary could depend on when its spans were flushed. It is now a
  function of the trace's set of spans only.
- A stray part directory with an impossible id could stop all flushes; the
  directory lock could be bypassed by deleting a file; a panic inside a flush
  could lose acknowledged data; a flush racing shutdown could write after the
  lock was released.

The span table also gained the instrumentation scope's attributes and the
resource and scope schema URLs, so nothing a sender puts in an OTLP request is
dropped.

A second independent review, of the fixes themselves, ran after the tag and
found three more defects. They are fixed in the commits that follow
`checkpoint-02`:

- The statement above, that a refused start leaves the directory unchanged,
  was not true for one kind of refusal: a log record that does not decode.
  By the time replay reached it, start-up had already written parts and
  committed manifest versions. Start-up now reads and checks every log record
  before its first change. No acknowledged data was at risk.
- A stray part directory with an id a little below the limit could still use
  up the id space. Ids far above anything the database has handed out no
  longer count.
- Token totals could depend on flush timing when a sender reported negative
  token counts. A negative number is no longer treated as a count.

Four tests that could pass for the wrong reason were tightened; the
crash-injection tests now require proof that the process died at the named
stage.

Known limits at this checkpoint:

- Killing a process does not drop the operating system's page cache, so the
  order of syncs is still untested against real power loss.
- No compaction: every flush adds parts, and a span retried across a flush is
  stored twice (reads of one trace remove the duplicate; counts do not).
- A whole trace is held in memory to read it.

## Checkpoint 3: server

The database now runs as one process, `nightswatchdb`, with a container
image. It accepts traces over OTLP gRPC and reads them back over a small HTTP
API: open a trace as JSON or as OTLP, list traces with filters, token totals
per model. How to run it is in the [server README](../crates/server/README.md).

What was added, beyond the process itself:

- **OTLP decoding without a protobuf library.** Requests are read with the
  crate's own wire reader. A sender gets its reply only after the request is
  in the log and synced.
- **The OTLP door handles the gRPC call itself**, on the HTTP library, with
  no gRPC library in the server. That is what lets it take memory budget for
  a message from its 5-byte header, before any of the message is held, and
  cap decompression at the size the message claims.
- **Resource and scope are stored once per batch**, as dictionary columns,
  instead of once per span.
- **One bad span does not sink its request or its trace.** A span with an
  invalid id is rejected alone and reported to the sender as OTLP partial
  success. Stored bytes that are not valid (a string cut inside a character,
  for instance) are shown with a replacement character in JSON and repaired
  in the OTLP view, so the rest of the trace always reads.
- **Reading streams.** A trace is written to the client as it is rendered,
  with a few kilobytes of working memory whatever the size of the trace.
- **A benchmark harness** (`scripts/bench.sh`, `bench/querybench`) that loads
  a dataset into the container and records ingest cost, storage, restart
  times and query latencies.

How it was tested: unit and integration tests in each crate (395 in the
workspace at this checkpoint); requests truncated at every length, with
flipped bits and random bytes; comparison against the reference OTLP types;
hand-written HTTP/2 clients that stall, trickle, reset streams and open more
streams than allowed, with memory measured by a counting allocator; the real
binary under signals; and an end-to-end run in containers, in CI on every
push, that loads generated traces from a Go gRPC client, half of them
compressed, and checks them after `kill -9` and after a clean stop.

Two independent reviews. The first found fifteen defects, the second, of the
fixes, ten more. Each proven one is kept as a regression test. The ones that
mattered most:

- A small request could make the decoder allocate without bound, because the
  resource was copied onto every span. Decoded size is now bounded by the
  request size, and the door charges for the measured worst case.
- The door's memory budget did not bound its memory: the gRPC library
  buffered whole messages before the budget was asked, a reset stream gave
  its budget back while its work went on, a slow sender could hold the
  budget for ever, and waiting streams held data outside it. All four are
  closed, with limits on calls, connections, waits and receive time.
- One stored span with a bad byte made its whole trace unreadable.
- Shutdown hung while any sender held a stream open; clients that did not
  read could freeze the API, health check included.
- A web page could read traces from an instance on the user's own machine
  (DNS rebinding). The API now checks the `Host` header.
- The part format changed without its version changing. It is now version 3,
  and an older part is refused with a message that says so.

Known limits at this checkpoint:

- No authentication and no TLS. Both listeners bind to loopback by default.
  An unauthenticated door cannot be made proof against a sender that keeps
  retrying; what is bounded is memory, waiting time and what a refused sender
  is told (retry or do not).
- The second round of server fixes has not itself had an independent review.
- OTLP over HTTP, logs and metrics are not accepted yet.
- No compaction and no large-text store: every flush adds small parts, and
  message text is stored in the span rows. Both are the next checkpoints, and
  the storage and query numbers will not resemble the experiments' until
  they land.
- Benchmarks: the harness has only been run on its smoke dataset. Measured
  results for the two reference datasets are not in the repository yet.
- A log file stays on disk until it reaches its size limit even when
  everything in it has been flushed.
- A whole trace is held in memory by the engine while it is read.

## Checkpoint 4: large text stored once

In agent traces every model call sends the system prompt, the tool
definitions and the conversation so far again. A part now stores each distinct
piece of that text once.

- When a part is written, large text is cut out of the bytes of a span's
  attributes, events and links exactly as the sender sent them: any long
  value, every value under a known content attribute, and each message of a
  JSON array of messages separately. The span row keeps the small bytes and
  references to the pieces.
- Nothing is re-encoded. A value is read by putting its bytes and pieces back
  together, so it is the sender's bytes by construction, and each rebuilt
  value is checked against a checksum taken when it was written.
- Two pieces share storage only if their bytes are equal. A hash finds the
  candidates and the bytes are then compared, because a hash alone would let
  one sender's text stand in for another's.
- Listing traces and summing tokens never open the text files.
- The part format is version 5. Parts written by earlier development versions
  are refused with a message that says so.

Measured in the tests (small data, debug builds; not a benchmark):

| Data | Text as sent | Stored | |
|---|---|---|---|
| Agent conversations, 3 traces of 30 calls | 14.4 MB | 0.54 MB | 27 times smaller |
| A growing conversation through the server | 2.08 MB | 0.10 MB | 21 times smaller |
| Text with no repetition | 1.35 MB | | the part is 1.4% larger than without the store |

Measured with the benchmark harness on the reference datasets (2 CPUs, 4 GB;
full results in [benchmarks](benchmarks/README.md)): 8.0 GB of agent-trace
text is stored in 62.1 MB, 129 times smaller, which is what the experiment
predicted and about an eighth of what the ClickHouse baseline needed for the
same data. Ten million spans without text take 560 MB, which is worse than
the experiment and worse than the baseline.

Sharing is within one part. Parts are only merged by compaction, which is
checkpoint 6, so the same system prompt is stored once per flush rather than
once per time window. On the text-heavy dataset that already reaches the
experiment's size; on span-only data the many small parts cost space.

An independent review found no input for which a value read back differs from
what was written. It did find five defects in bounds and in the handling of
damaged files, each proven with a failing test and fixed:

- Two parts whose text files had equal lengths could have them swapped, and a
  part would then return the other part's text without an error. The text
  files are now bound to their part by checksum when the part is opened.
- One flipped bit in a span row could make a read return a piece belonging to
  another span. Every rebuilt value now carries a checksum.
- Reading one 4 KB span could decompress a thousand blocks; a crafted row
  could ask for a terabyte and abort the process. Blocks are now read once
  per batch, and sizes are checked before anything is allocated.
- Memory grew with the number of pieces, and a sender could choose to send
  millions of tiny ones. There is now a smallest piece, a largest number of
  pieces per part, and compact tables: an open part holds about one byte per
  piece instead of twelve.

Two things found outside the review, by running the benchmark harness on ten
million spans:

- Listing the newest traces took 6.4 seconds. It read every trace summary in
  the range and asked every part about every trace. It now reads newest first
  and stops when the answer cannot change, and it keeps a map of the traces
  that are spread over more than one part. The tests count the work done per
  query, and were checked to fail when the stopping rule is deliberately
  broken.
- The harness itself needed half an hour to page through the traces, for the
  same reason.

Known limits at this checkpoint:

- The fixes of this checkpoint have not yet had a second independent review.
- A value shorter than 8 bytes is never stored as a piece, and text beyond
  16.7 million pieces in one part is stored in the row.
- Verifying a part rebuilds every value in it.
- Span-only data takes more space than the baseline, and token totals over
  a long range are slower than the baseline, until parts are compacted.
