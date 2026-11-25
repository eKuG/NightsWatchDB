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
<data_dir>/            exclusively locked (flock on the directory itself) for the life of the engine
<data_dir>/manifest/   numbered manifest files
<data_dir>/parts/      one directory per part (and <id>.tmp while being written)
<data_dir>/wal/        log files
```

## Start-up

Nothing is created, committed, renamed or deleted under `manifest/` or `parts/`
until every check that can refuse the start has passed. The only changes
before that are: creating the data directory, the log's own repair of a torn
tail, its removal of files that lie wholly inside parts (see step 3), and the
manifest crate's removal of its temporary files. In this order; each step
finishes before the next.

0. Validate the arguments: the options and duplicate decoder tags.
1. Create the directory if allowed, then lock it: the lock is `File::try_lock`
   on the open directory itself, so no file in it can be deleted to get around
   it, and no `LOCK` file exists. A second `open` is `Conflict`. The lock is
   released by `shutdown` or when the last handle is dropped.
2. Read, without changing: whether a manifest exists. If there is none and
   `parts/` holds any part directory (final or `.tmp`, whatever its id), the
   manifest is lost and the start is `Corrupt`: refusing is the only safe
   answer. If there is none and the database may not be created, `NotFound`.
   Otherwise load the manifest state, and open a `PartReader` for every live
   part. Any failure stops start-up.
3. Open the log with `min_next_lsn = wal_flushed_lsn + 1` (position 0 for a
   database with no manifest yet). The log first removes files wholly below
   that position, as a trim would, so damage in them cannot block the start.
   Then, if the log ends below the position (its files were lost: in healthy
   operation the next record number is never below `wal_flushed_lsn + 1`,
   because records are applied only after they are durable and trimming never
   removes the newest file), the start is `Corrupt` and the message names the
   missing records and `accept_lost_log`. With `EngineOptions::accept_lost_log`
   set, the log is instead renumbered above the position and the records
   acknowledged after the last flush are gone. If the log starts above
   `wal_flushed_lsn + 1` (empty or not) the records in between were
   acknowledged and are gone: `Corrupt`. A new database has nothing to
   refuse here.
4. Check the records to be replayed. Read every record from
   `wal_flushed_lsn + 1` once and check exactly what the replay would refuse:
   not empty, a known tag, decodes, decodes to the span table, numbers
   contiguous, the count as expected. Each record's batches are dropped at
   once, so one record's worth of memory is held. Any failure is `Corrupt` and
   nothing has changed. The stage `OpenChecked` is reached only after this
   pass.
5. Only now the disk changes. For a new database, create the manifest and commit
   an empty first version (so a crash during the very first flush, parts on
   disk and no manifest file, is not mistaken for a lost manifest). Then raise
   `next_part_id` above the highest part directory id (final or `.tmp`), so an
   id that ever reached the disk is never handed out again. Only ids below
   `next_part_id + PART_ID_LEFTOVER_WINDOW` (2^32) count: ids are handed out in
   order and only flushes that did not commit leave ids above `next_part_id`,
   so a leftover of this database is close above it. A directory at or above
   that (and any id from `MAX_NEXT_PART_ID` up) is a stray: it never raises
   `next_part_id`, because that would use up the id space and stop every later
   flush; it is an ordinary unreferenced directory and is deleted in step 7.
   The arithmetic is checked.
6. Replay from `wal_flushed_lsn + 1`; each record is `[tag] + bytes`. The
   checks of step 4 are made again as internal-error guards (the decoders are
   deterministic, so they cannot refuse for those reasons). While replaying,
   the buffer is flushed whenever it reaches `buffer_bytes`, so a long log
   needs no more memory than a running engine; the log is not trimmed until it
   has been read.
7. Only after the replay succeeded: delete the part directories the manifest
   does not reference. One of them may be the only decoded copy of data whose
   log record no longer decodes, so it must outlive a refused replay.
8. Start the threads and accept requests.

A refusal is a verdict on what is on disk: log, manifest or parts are damaged
or missing, and the start changes nothing. The one exception is a failure
while working, as opposed to a refusal: an I/O error or a full disk during a
flush made by the replay can leave the start-up half done (some parts written,
some manifest versions committed, the leftovers not yet deleted). That is a
failure, not a refusal, and the next start continues from where it stopped.

## Threads

| Thread | Does |
|---|---|
| log thread (in `nwdb-wal`) | writes and syncs groups, runs callbacks in order; its callbacks only push to a queue |
| applier (`nwdb-applier`) | pops results in log order, inserts into the buffer, calls `on_durable` outside every lock |
| flusher (`nwdb-flusher`) | waits for the size or age trigger, then flushes; after a failed flush waits one second before retrying |
| callers | `flush()` runs on the calling thread, serialised with the flusher by a gate; it checks the closing state after taking the gate |

One mutex (`Core`) holds the visible state and counters, and is held only to
move pointers, never across I/O. A snapshot is three `Arc` clones taken under
it. The only blocking calls in the log's callback path are the queue's short
lock; the log is never called from a callback.

`Engine` is a cheap clone. `shutdown()` stops accepting, waits for every
accepted request to be acknowledged, stops the flusher, then takes the flush
gate and holds it while it flushes, shuts the log down, stops the applier and
releases the lock; a `flush()` that raced with it finishes before it or finds
the engine closed, so nothing is written after the lock is gone. A second
concurrent `shutdown()` waits for the first to finish. Dropping the last handle
does the same without the flush (what is acknowledged is in the log), and if the
last handle is dropped by a callback running on an engine thread, the cleanup
runs on a separate thread rather than joining itself.

## The flush

`do_flush` (one at a time):

1. **Freeze.** Under the core lock, refuse (fatal) if a frozen buffer is
   already present, then move the whole active buffer to the frozen slot (it stays visible to queries) and record `applied_lsn`. Because
   requests enter the buffer in log order under the same lock, every log
   position up to `applied_lsn` is in the frozen buffer or in a part, and the
   position is always a contiguous prefix.
2. **Group.** Sort the frozen rows by trace and span id, drop duplicate
   `(trace, span)` pairs (the first arrival stays), order each trace's spans by
   `(start_ns, span_id)` and the traces by `(earliest start, trace id)`. A
   trace's window is `window_start` of its earliest span in this buffer.
3. **Write parts.** Reserve fresh part ids from the manifest. For each window
   write one part through `PartWriter` in `parts/<id>.tmp`, which syncs and
   renames it to `parts/<id>`. Open a `PartReader` for it. The part writer is
   given `EngineOptions::splitter` and `EngineOptions::dedup_window_bytes`:
   with a splitter, the large text of the raw columns is cut out of the rows
   and stored once per part in the part's piece store (see `nwdb-segment` and
   `nwdb-payload`); without one every value stays in its row. Either way a read
   returns the original bytes, so nothing else here depends on it. The memory
   of a flush is the buffer being written plus, for the part being written,
   its row group and its de-duplication window (64 MiB by default).
4. **Commit.** One manifest commit adds all the parts and sets
   `wal_flushed_lsn` to the frozen position.
5. **Publish.** Extend the split map for the new parts (outside the lock, see
   Reads), then under the core lock replace the part list and the split map and
   clear the frozen buffer in one step, so a snapshot sees the data exactly once
   and its map matches its parts.
6. **Trim.** `truncate_before(wal_flushed_lsn + 1)`, only after the commit
   returned `Ok`. A failure is recorded in `stats().last_error` and retried by
   the next flush; the data is safe.

What a crash leaves, and why recovery is right. The stage names are those of
`EngineOptions::stage_hook` (below); the crash tests abort the process at each
and check the column "On disk":

| Crash at / during | On disk | Recovery |
|---|---|---|
| `FlushFrozen` (freeze, group) | nothing new | replay from the old `wal_flushed_lsn` rebuilds the buffer |
| a part write, `PartWritten` | `<id>.tmp`, maybe finished parts, manifest unchanged | the leftovers are unreferenced; `next_part_id` is first moved above them; replay rebuilds the buffer; then they are deleted |
| `AllPartsWritten` (before the commit) | finished parts the manifest does not know | same: never reused, data replayed, then deleted |
| the commit | the new manifest file either exists or not (create-if-absent link, directory sync) | not there: as above. There: parts are referenced and `wal_flushed_lsn` covers them, so replay starts after them |
| `Committed`, `Published` (before trimming) | parts referenced; log files wholly inside parts | replay starts at `wal_flushed_lsn + 1`; `Wal::open` removes the obsolete files first, damaged or not |
| trimming | a prefix of the old files removed, oldest first | the log is still contiguous and covers `wal_flushed_lsn + 1` |
| `Trimmed` | nothing obsolete | nothing to do |

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

A panic while writing or committing (after the freeze) is caught and treated
as `Fatal`: the frozen buffer stays visible, no later flush can freeze over it
and commit a position that covers records not in a part, and a restart recovers
from the log. A panic in the stage hook after the commit is ignored.

## Stored text

`EngineStats::payload_logical_bytes`, `payload_unique_bytes` and
`payload_stored_bytes` are the sums, over the live parts, of what the parts'
rows refer to in their piece stores counting every use, of the distinct pieces
(what is stored once, before compression), and of what the piece stores take on
disk, compressed. They come from the part's `PartMeta::payload`, which the
manifest records, so they survive a restart. Text still in the buffer is in
none of them: it is stored once it is flushed. The distance between logical and
unique is what sharing saves, and between unique and stored what compression
saves; stored alone cannot tell the two apart, so a test of sharing asserts on
unique.

## Stage hook

`EngineOptions::stage_hook` is a test facility, in the manner of the log's
`sync_hook`: the engine calls it with a `Stage` at each of these points.

- Start-up: `OpenChecked` (every refusing check passed, including the read of
  the records to replay; nothing changed yet),
  `IdsRaised`, `ReplayFinished`, `LeftoversDeleted`.
- A flush: `FlushFrozen`, `PartWritten` (once per part), `AllPartsWritten`
  (before the commit), `Committed` (before publishing), `Published`
  (before the trim), `Trimmed`.

An error returned at a stage before the manifest commit is handled exactly as
if that step had failed with that error (`Fatal` puts the engine in the fatal
state; anything else restores the buffer and removes the parts of that
attempt). After the commit the return value is ignored, and so is a panic.


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
  own start is in range. The partials of all parts (each summary file is
  sorted by start and is read from its newest row group backwards, one group
  per part at a time, and a part is not read at all until its newest possible
  start reaches the front) and of the buffer are taken newest first. The first
  time a trace is seen, its other partials are fetched (the split map names the
  parts, the buffer is a hash map), merged with `TraceSummary::merge`, and
  only then are the range and the filters applied; the best `limit` merged
  summaries are kept (order: start descending, trace id ascending). The scan
  stops once `limit` results are held and the next partial starts strictly
  below the worst held start: every trace not yet seen has all its partials
  at or below that start, so its merged start, the minimum, is below the worst
  held one and the trace cannot enter. Equal starts keep the scan going,
  because the tie goes to the smaller trace id. A filter that rejects most
  traces keeps the scan going back to the range's lower bound. Cost: about
  `limit` summary rows and one row group per part that can reach the front,
  one split-map lookup per trace seen, and one trace-index lookup and one row
  group read per extra part of a split trace. A trace first met in the buffer
  that is not split has its one part, if any, searched for in the indexes (at
  most one lookup per part), which only buffered traces pay. Memory: the results, the ids of
  the traces seen so far, one summary row group per part being read, and the
  buffer's summaries (the buffer is summarised on every call).
- The split map: each part's trace index is sorted by trace id, and when the
  set of parts changes (start-up, a flush publishing parts) the traces that
  are in more than one part are found with a merge over the sorted indexes of
  the new parts, then looked up in the old parts with a galloping walk, all
  outside the core lock. The result is a map from trace id to the parts that
  hold it, for split traces only, and it is published in the same step as the
  part list, so a snapshot's map always matches its parts. Memory: about 40
  bytes per split trace (the id, a small boxed list of part numbers and the
  table overhead), nothing for the rest; a flush copies the old map, which
  costs the number of split traces. The cost of building it for a flush is the
  new parts' traces, plus for each of them a galloping step in every old part's
  index. `get_trace` uses it too: a trace in the map is read from its parts
  only, and any other trace is in at most one part, so the search of the
  indexes ends at the first part that has it. If compaction later removes
  parts, the part numbers in the map must be rebuilt with the list.
- Token sums: a negative count in a column (possible from another ingest
  path; the OTLP door never stores one) counts as 0, so every term is
  non-negative and saturating addition gives the same total however the spans
  were grouped by flushes.
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
- Back-pressure is checked before a request is admitted, and the bytes of
  requests already in flight (admitted, not yet in the buffer) are counted at
  the check, so concurrent requests do not overshoot `max_buffered_bytes`. A
  request is admitted if the total including it stays within the limit; one
  request larger than the limit is admitted only when nothing else is
  buffered.
- Flush is one part per window per flush: many windows in one buffer make many
  small parts. Compaction is a later checkpoint.
- No retention, no compaction, no object store.
- The age limit counts from the first batch that entered an empty buffer.
- A callback that panics is caught and ignored; a panic anywhere else on the
  applier thread would leave `shutdown` waiting.

## The crash test

`src/bin/nwdb-crashtest.rs` is a test tool, not part of the database. Its
child process ingests records, prints each acknowledged id, and marks a flush
from `FlushFrozen` to `Trimmed` through the stage hook. Two kinds of test run it.

- `tests/crash.rs` runs it as a child process on one directory 24 times, kills
  it with SIGKILL after a seeded random delay, reopens the directory (half the
  recoveries with a small buffer, so the replay-time flush runs), and checks
  that every record the child printed as acknowledged is present exactly once,
  that queries work, and that the engine accepts and flushes a new record. It
  reports how many kills found a flush in progress (the marker covers the
  trim) and how many left a temporary or an unreferenced finished part
  directory behind, looking at the directory without opening the manifest.
- `tests/crash_stages.rs` makes the child abort itself (`std::process::abort`)
  at a named stage on its n-th occurrence, so every flush stage and every
  start-up stage is hit deterministically. For each it checks the on-disk state
  that stage should leave (for example unreferenced finished parts after
  `AllPartsWritten`; referenced parts and obsolete log files after
  `Committed`), reopens, and checks every acknowledged record is present
  exactly once, queries work, and a new record is accepted and flushed. The
  start-up stages are run on a directory with leftovers and a log to replay,
  with a buffer small enough that the replay flushes.

`tests/startup_safety.rs` and `tests/regressions_second_review.rs` check that
every refusal, including each kind of refused replay with a buffer so small
that the replay would flush after every record, leaves the directory byte for
byte as it was and that restoring what was lost lets it open;
`tests/injected.rs` injects errors and panics through the hook;
`tests/shutdown_races.rs` runs `flush`, `ingest` and two `shutdown` calls at
once.

Known limit: killing a process does not drop the operating system's page
cache, so a write that was never synced is still seen by the next start. The
tests therefore prove the order of the steps and the recovery rules, not the
order of the syncs; a power-loss test would be needed for that.
