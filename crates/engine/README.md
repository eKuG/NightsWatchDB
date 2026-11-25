# nwdb-engine

The storage engine of NightsWatchDB. It wires the write-ahead log, a memory
buffer, immutable parts and the manifest together: a request is logged,
acknowledged once durable, visible to queries, later written into parts, and
survives a crash at any point. It is a library; the OTLP door and the server
come later and call it.

```rust
let engine = Engine::open(EngineOptions::new(dir), vec![(TAG, Arc::new(my_decoder))])?;
engine.ingest(TAG, wire_bytes, batches, |result| { /* durable and visible */ })?;
let trace = engine.get_trace(trace_id)?;
engine.shutdown()?;
```

## Life of a request

1. A door calls `ingest(tag, record, batches, on_durable)`: the bytes in its
   own wire format, a one-byte tag naming the format, and the already-decoded
   span batches. The engine checks the tag has a decoder, the batches have the
   span schema, and that the buffer and the log have room (`Unavailable`
   otherwise, nothing changed), then appends `[tag] + record` to the log.
2. The log's thread syncs a group of records and runs each record's callback in
   log order. The callback only pushes the result onto a queue.
3. The applier thread pops the queue in order, inserts the batches into the
   active buffer, and only then calls `on_durable(Ok)`. Data is therefore never
   visible before it is durable, and an acknowledged sender can read its own
   write. A record the log failed gets the log's error (`Fatal`).
4. The flusher thread writes the buffer into parts when it reaches
   `buffer_bytes`, or when its oldest data is `flush_interval` old; `flush()`
   does it on demand.
5. After a crash `open` loads the manifest, opens the parts and replays the log
   from the position the manifest records through the registered decoders.

## Directory layout

```
<data_dir>/LOCK        exclusive flock, held for the life of the engine
<data_dir>/manifest/   numbered manifest files
<data_dir>/parts/      one directory per part (and <id>.tmp while being written)
<data_dir>/wal/        log files
```

## Start-up

In this order; each step finishes before the next.

1. Create the directory if allowed, then take `LOCK` (`File::try_lock`).
   A second `open` is `Conflict`. The lock is released by `shutdown` or when the
   last handle is dropped.
2. Open `manifest/`: create it only if `create_if_missing` and `parts/` holds no
   part directory, otherwise it must exist. Part directories with no manifest
   are `Corrupt`: refusing is the only safe answer to a lost manifest. A new
   database commits an empty first manifest version at once, so a crash during
   the very first flush (parts on disk, no manifest file yet) is not mistaken
   for a lost manifest.
3. If the highest id of any part directory (final or `.tmp`) is at or above the
   manifest's `next_part_id`, commit a higher `next_part_id` first, so an id that
   ever reached the disk is never handed out again.
4. Delete part directories the manifest does not reference.
5. Open a `PartReader` for every live part. Any failure stops start-up.
6. Open the log with `min_next_lsn = wal_flushed_lsn + 1`, so the log can never
   number a new record at or below what is already in parts, even if its files
   were deleted. If the log starts above `wal_flushed_lsn + 1` (empty or not) the
   records in between were acknowledged and are gone: `Corrupt`. Replay from
   `wal_flushed_lsn + 1`; each record is `[tag] + bytes`. An empty record, an
   unknown tag, a decode failure, a decoded batch without the span schema, or a
   gap in the numbers is `Corrupt`. While replaying, the buffer is flushed
   whenever it reaches `buffer_bytes`, so a long log needs no more memory than a
   running engine; the log is not trimmed until it has been read.
7. Start the threads and accept requests.

## Threads

| Thread | Does |
|---|---|
| log thread (in `nwdb-wal`) | writes and syncs groups, runs callbacks in order; its callbacks only push to a queue |
| applier (`nwdb-applier`) | pops results in log order, inserts into the buffer, calls `on_durable` outside every lock |
| flusher (`nwdb-flusher`) | waits for the size or age trigger, then flushes; after a failed flush waits one second before retrying |
| callers | `flush()` runs on the calling thread, serialised with the flusher by a gate |

One mutex (`Core`) holds the visible state and counters, and is held only to
move pointers, never across I/O. A snapshot is three `Arc` clones taken under
it. The only blocking calls in the log's callback path are the queue's short
lock; the log is never called from a callback.

`Engine` is a cheap clone. `shutdown()` stops accepting, waits for every
accepted request to be acknowledged, stops the flusher, flushes, shuts the log
down, stops the applier and releases the lock. Dropping the last handle does
the same without the flush (what is acknowledged is in the log), and if the
last handle is dropped by a callback running on an engine thread, the cleanup
runs on a separate thread rather than joining itself.

## The flush

`do_flush` (one at a time):

1. **Freeze.** Under the core lock, move the whole active buffer to the
   frozen slot (it stays visible to queries) and record `applied_lsn`. Because
   requests enter the buffer in log order under the same lock, every log
   position up to `applied_lsn` is in the frozen buffer or in a part, and the
   position is always a contiguous prefix.
2. **Group.** Sort the frozen rows by trace and span id, drop duplicate
   `(trace, span)` pairs (the first arrival stays), order each trace's spans by
   `(start_ns, span_id)` and the traces by `(earliest start, trace id)`. A
   trace's window is `window_start` of its earliest span in this buffer.
3. **Write parts.** Reserve fresh part ids from the manifest. For each window
   write one part through `PartWriter` in `parts/<id>.tmp`, which syncs and
   renames it to `parts/<id>`. Open a `PartReader` for it.
4. **Commit.** One manifest commit adds all the parts and sets
   `wal_flushed_lsn` to the frozen position.
5. **Publish.** Under the core lock, replace the part list and clear the frozen
   buffer in one step, so a snapshot sees the data exactly once.
6. **Trim.** `truncate_before(wal_flushed_lsn + 1)`, only after the commit
   returned `Ok`. A failure is recorded in `stats().last_error` and retried by
   the next flush; the data is safe.

What a crash leaves, and why recovery is right:

| Crash during | On disk | Recovery |
|---|---|---|
| freeze, group | nothing new | replay from the old `wal_flushed_lsn` rebuilds the buffer |
| a part write | `<id>.tmp`, maybe finished parts, manifest unchanged | the leftovers are unreferenced and deleted; `next_part_id` is first moved above them; replay rebuilds the buffer |
| between the last part and the commit | finished parts the manifest does not know | same: deleted, never reused, data replayed |
| the commit | the new manifest file either exists or not (create-if-absent link, directory sync) | not there: as above. There: parts are referenced and `wal_flushed_lsn` covers them, so replay starts after them |
| after the commit, before trimming | log files wholly inside parts | replay starts at `wal_flushed_lsn + 1` and skips them; the next trim removes them |
| trimming | a prefix of the old files removed, oldest first | the log is still contiguous and covers `wal_flushed_lsn + 1` |

The two invariants behind the table: `wal_flushed_lsn` only moves in the same
commit that makes the data live, and the log is only trimmed after that commit
returned. A record is therefore always in the log or in live parts, and
replaying from `wal_flushed_lsn + 1` never replays something already in a part.

A non-`Fatal` error in steps 2 to 4 puts the frozen buffer back in front of
the active one (nothing is lost), removes the parts this attempt wrote, and
the retry uses fresh ids. `Fatal` from the log, the manifest or a part writer
puts the engine in the fatal state: ingest and flush return `Fatal`, pending
callbacks get `Fatal`, reads keep working on what is visible, nothing is
retried. A restart recovers from the log and the manifest.

## Reads

`snapshot()` returns the live parts, the frozen buffer if there is one, and the
active buffer's batches, all as of one instant, and queries on it never change
however long they run. Files are immutable and parts stay open, so a snapshot
does not block, and is not blocked by, ingest or flush.

- `get_trace`: the trace's rows from every part (through the part reader's
  byte-bounded batches) and from the buffers, duplicates by span id removed
  (first found kept), ordered by `(start_ns, span_id)`, returned in
  byte-bounded batches. A trace in a single part that is already in order is
  returned untouched.
- `list_traces`: late spans can put one trace in several parts and the buffer,
  each holding a partial summary. A trace's merged start is the smallest
  partial start, so any trace that belongs in the result has a partial whose
  own start is in range. The candidates are those partials, found with the
  readers' range-pruned summary scans and in the buffer. For each candidate the
  other parts that hold it (trace index lookup) and the buffer supply their
  summary rows, they are merged with `TraceSummary::merge`, and only then are
  the range, the filters and the order applied. Cost: the summary row groups
  overlapping the range, plus one trace-index lookup per part per candidate
  and one summary row-group read per extra part that holds the trace;
  candidates' summaries are held in memory, and the buffer is summarised on
  every call.
- `aggregate_tokens`: a pruned scan of five columns per part, and the buffers
  (frozen and active together, as one set) grouped by `group_traces`, the same
  routine `list_traces` uses, so a span id seen twice for one trace in the
  buffers is counted once and the three queries agree inside one snapshot.

A trace's summary is a function of its set of spans only: merging partial
summaries in any order gives the same result as summarising all the spans at
once. `session_id` and `user_id` are the smallest non-null value among the
spans, and the root fields come from the root with the smallest `(name,
service)`. A malformed trace with several roots therefore shows the smallest
root, not the earliest.

## Known limits

- Duplicate spans (same trace id and span id) are removed only among the rows
  that are in the buffers (the frozen and the active one together) at the
  moment of a query, and within one flush. A duplicate that landed in a part and
  again in the buffers, or in two different parts (a sender retry that crossed
  a flush), is removed by `get_trace` but still counted by `list_traces`
  (`span_count`, token sums) and `aggregate_tokens`. The compactor will remove
  the rest.
- A trace's rows are found in the buffers by reading the trace id column of
  every buffered batch; `list_traces` summarises the whole buffer per call. Both
  cost time proportional to the buffer, and are fine for a buffer of a few
  hundred megabytes but are not an index.
- `get_trace` holds a whole trace in memory (up to twice, while merging
  sources). Batches are byte-bounded, the trace as a whole is not.
- Memory accounting uses Arrow's own memory estimate, including spare capacity
  of the builders. Small batches therefore count for more than their values.
  The bound is on the buffer, not on in-flight query results.
- Back-pressure is checked before a request is admitted; concurrent requests
  can overshoot `max_buffered_bytes` by what is in flight between the check and
  the insert, which is itself counted for later requests. One request larger
  than the limit is admitted when nothing else is buffered.
- Flush is one part per window per flush: many windows in one buffer make many
  small parts. Compaction is a later checkpoint.
- No retention, no compaction, no object store.
- The age limit counts from the first batch that entered an empty buffer.
- A callback that panics is caught and ignored; a panic anywhere else on the
  applier thread would leave `shutdown` waiting.

## The crash test

`src/bin/nwdb-crashtest.rs` is a test tool, not part of the database. The test
`tests/crash.rs` runs it as a child process on one directory 24 times, kills it
with SIGKILL after a seeded random delay, reopens the directory, and checks
that every record the child printed as acknowledged is present exactly once,
that queries work, and that the engine accepts and flushes a new record. It
reports how many kills found a flush in progress and how many left a temporary
or an unreferenced finished part directory behind.
