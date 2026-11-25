# Checkpoints

The engine is built in small steps. Each checkpoint is a tested increment:
its tests pass in a Linux container, it has been through an independent
review, and it is committed, tagged `checkpoint-NN` and pushed.

```
Progress  [███░░░░░░░░░░░░░]  3 of 16 checkpoints
```

| # | Checkpoint | State |
|---|---|---|
| 0 | Repository set-up: licence, design, experiments and measured results | done |
| 1 | Storage core as libraries: shared types, write-ahead log, span parts (segment files, trace index, summary), manifest | done |
| 2 | Engine: memory buffer, flush, recovery from the log, snapshot reads; open a trace, list traces, aggregate | done |
| 3 | Server: OTLP door for traces, HTTP query API, container image, end-to-end test with the generator | built, in review |
| 4 | Large text out of line, each distinct piece stored once | |
| 5 | Text index over distinct pieces; search | |
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

Known limits at this checkpoint:

- Killing a process does not drop the operating system's page cache, so the
  order of syncs is still untested against real power loss.
- No compaction: every flush adds parts, and a span retried across a flush is
  stored twice (reads of one trace remove the duplicate; counts do not).
- A whole trace is held in memory to read it.
