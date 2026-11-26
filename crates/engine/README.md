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
   does it on demand. The maintenance thread removes parts that retention has
   expired (see Retention) and merges the parts of a window (see Compaction).
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
8. Start the threads and accept requests. The maintenance thread's first pass
   runs at once, so what expired while the engine was down goes soon.

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
| maintenance (`nwdb-maintenance`) | wakes every `maintenance_interval` (default 10 s) and after every flush, and runs one pass: it retries directory deletions that failed, applies retention, then merges parts until the policy finds nothing to merge (see Compaction) |
| callers | `flush()` runs on the calling thread, serialised with the flusher by a gate; it checks the closing state after taking the gate. `run_maintenance()` runs one pass on the calling thread, serialised with the thread |

One mutex (`Core`) holds the visible state and counters, and is held only to
move pointers, never across I/O. A snapshot is three `Arc` clones taken under
it. The only blocking calls in the log's callback path are the queue's short
lock; the log is never called from a callback.

Three more mutexes order the long operations: the flush gate (one flush at a
time), the maintenance gate (one pass at a time) and the publish gate (a
manifest commit that changes the part list, together with the publish that
follows it; see Replacing parts). Where a thread needs two, it takes them in
the order flush gate or maintenance gate first, then the publish gate, then
the core mutex. No gate is held while parts are written or directories are
deleted.

`Engine` is a cheap clone. `shutdown()` stops accepting, waits for every
accepted request to be acknowledged, stops the flusher and the maintenance
thread, then takes the maintenance gate and the flush gate and holds them while
it flushes, shuts the log down, stops the applier and releases the lock; a `flush()` that raced with it finishes before it or finds
the engine closed, so nothing is written after the lock is gone. A second
concurrent `shutdown()` waits for the first to finish. Dropping the last handle
does the same without the flush (what is acknowledged is in the log), and if the
last handle is dropped by a callback running on an engine thread, the cleanup
runs on a separate thread rather than joining itself.

**How long shutdown can wait for maintenance.** A pass looks at the stop flag
between its steps (and compaction between its merges), and shutdown sets it
first, so shutdown waits for the step in progress and no more. A step is one
manifest commit together with the publish that follows it (a file write and two
syncs, plus the rebuild of the split map described under Replacing parts, which
is in memory), or the deletion of one part directory, or **one merge**. A merge
in progress is finished, not abandoned (see Compaction for why): shutdown
waits for the call to `merge_parts` that is running, which is bounded by
`max_input_bytes` of input (4 GiB by default; the segment crate measured a
debug build at about 16,000 rows a second, a release build is much faster),
and then for one open, one commit and one publish. No further merge is started.
If shutdown arrives while a replacement is deleting its directories, the ones
not yet reached stay on disk, unreferenced; the next start-up deletes them. A
pass that is waiting for the publish gate waits for a flush's commit and
publish, which is the same order of time. Nothing in a pass waits for ingest,
for queries or for a flush's writing of parts.

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
4. **Commit.** Take the publish gate, then one manifest commit adds all the
   parts and sets `wal_flushed_lsn` to the frozen position. The gate is taken
   only now, after the long work of step 3.
5. **Publish.** Build the new part list and split map (outside the core lock,
   see Replacing parts), then under the core lock replace the part list and the
   split map and clear the frozen buffer in one step, so a snapshot sees the
   data exactly once and its map matches its parts. Then release the publish
   gate.
6. **Rotate and trim.** `Wal::rotate` starts a new log file at the current
   write position (nothing happens if the newest file holds no records), then
   `truncate_before(wal_flushed_lsn + 1)` removes the files wholly inside
   parts, only after the commit returned `Ok`. A trim never removes the newest
   file, so without the rotation the records of this flush would stay in it
   until it reached `max_file_bytes` (256 MiB by default): a benchmark found
   134 MB of log on disk after everything had been flushed. After a flush with
   no ingest in between, `wal/` holds one file of 20 bytes (its header). Records
   that arrive between the freeze and the rotation stay in the old file, which
   the next flush removes. A failure of either step is recorded in
   `stats().last_error` and retried by the next flush; the data is safe. The
   same rotation and trim run when a flush finds nothing to write, so a log
   that is all flushed (the state older versions left) is cleaned too, and at
   shutdown.

What a crash leaves, and why recovery is right. The stage names are those of
`EngineOptions::stage_hook` (below); the crash tests abort the process at each
and check the column "On disk":

| Crash at / during | On disk | Recovery |
|---|---|---|
| `FlushFrozen` (freeze, group) | nothing new | replay from the old `wal_flushed_lsn` rebuilds the buffer |
| a part write, `PartWritten` | `<id>.tmp`, maybe finished parts, manifest unchanged | the leftovers are unreferenced; `next_part_id` is first moved above them; replay rebuilds the buffer; then they are deleted |
| `AllPartsWritten` (before the commit) | finished parts the manifest does not know | same: never reused, data replayed, then deleted |
| the commit | the new manifest file either exists or not (create-if-absent link, directory sync) | not there: as above. There: parts are referenced and `wal_flushed_lsn` covers them, so replay starts after them |
| `Committed`, `Published` (before rotating) | parts referenced; log files wholly inside parts | replay starts at `wal_flushed_lsn + 1`; `Wal::open` removes the obsolete files first, damaged or not |
| the rotation | a `wal-<n>.log.tmp` that is being written, or a new file holding only its header after the old files | the log opens either way: `open` removes the temporary file; a header-only newest file is an ordinary newest file with no records, numbered after the last record |
| `LogRotated` (before trimming) | as after `Published`, plus the new file | same as above |
| trimming | a prefix of the old files removed, oldest first | the log is still contiguous and covers `wal_flushed_lsn + 1`; it may consist of the header-only file alone, which starts exactly at `wal_flushed_lsn + 1`: that is a healthy log, not a lost one |
| `Trimmed` | nothing obsolete | nothing to do |

The invariants behind the table: `wal_flushed_lsn` only moves in the same
commit that makes the data live, and the log is only rotated and trimmed after
that commit returned. A record is therefore always in the log or in live
parts, and replaying from `wal_flushed_lsn + 1` never replays something already
in a part.

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

## Replacing parts

Parts are immutable and a flush only adds them. Everything that takes parts
away (retention and merging) goes through one internal operation,
`Inner::replace_parts(remove: &[u64], add: Vec<LivePart>) -> Result<Replaced>`:
the ids of live parts to remove and zero or more new parts, each already
written to `parts/` with its `PartMeta` and an open `PartReader` (ids reserved
with `manifest.reserve_part_ids`). It is "replace these by those". In this
order:

1. **Commit.** One manifest commit that removes and adds them.
2. **Publish.** The new part list and a split map that matches it replace the
   old ones in one step under the core lock. A snapshot sees the list from
   before or the list from after, each with its own map; never a removed part
   and its replacement both present or both absent.
3. **Delete.** The directories of the removed parts are deleted, one at a
   time, and `parts/` is synced.

Rules:

- **A flush and a replacement never interleave.** The publish gate is held from
  just before the manifest commit to the end of the publish, by a flush and by
  a replacement alike. Each therefore reads the list the other left, and each
  commit's edit is valid against the state it was built from. Nothing long
  happens under it: a flush takes it only after its parts are written, and
  deleting directories comes after it is released. The part list stays in part
  id order whichever of the two published first (a replacement can reserve an
  id, a flush then reserve and publish higher ones, and the replacement publish
  last).
- **Before the commit an error changes nothing.** An id in `remove` that is not
  live, or listed twice, is `InvalidArgument`, and so is anything the manifest
  refuses (such as a new part whose id is live). The stage hook is called at
  `BeforeReplaceCommit` after these checks, with the publish gate held and
  just before the commit; an error it returns is handled as if the commit had
  failed with that error (a panic in it is a panic of the pass, which is
  fatal). The new parts are then not
  live; their directories stay the caller's to delete, and a restart deletes
  them otherwise. `Fatal` from the manifest puts the engine in the fatal state
  as it does in a flush.
- **After the commit the replacement is done.** A failure to delete a directory
  is returned in `Replaced::deletion_errors`, recorded in the statistics by the
  pass, and the directory is kept in a list that the next pass retries (the
  directory is unreferenced, so a restart removes it too). A stage hook's
  result or panic after the commit is ignored.
- **Old snapshots are not disturbed.** A snapshot taken before the removal reads
  the removed parts from files its readers hold open, so a read never fails
  because a directory is gone. A directory that is already gone when the
  deletion gets to it is not an error.

The split map stores positions in the part list, so it is rebuilt for the new
list, in memory and outside the core lock:

- The old map is renumbered: each of its entries (one per split trace) loses
  the holders that were removed, is mapped to the new positions, and is
  dropped if fewer than two holders are left. Cost: the number of split
  traces; no part is read.
- For the added parts, the same merge over their sorted trace indexes and a
  galloping step in every other part's index as a flush does: the added parts'
  traces times, per other part, a step of at most the logarithm of its size
  over those traces. With nothing added (retention) there is no such step.
- Allocating the new list costs the number of live parts.

So a removal costs the number of live parts plus the number of split traces,
and a merge adds the traces of the merged part times the number of live parts
(with the logarithm above).

Crash points. The stage names are those of the stage hook; the crash tests
abort the process at each and reopen. Start-up deletes the directories the
manifest does not reference after the replay, and a part id that reached the
disk is never reused, so no new rule is needed:

| Crash at / during | On disk | Recovery |
|---|---|---|
| before the commit (`BeforeReplaceCommit`, or any earlier step of a merge) | the new parts' directories, unreferenced; the old parts live | the new directories are deleted at start-up; nothing was removed |
| `ReplaceCommitted` (after the commit, before the publish) | the manifest has the new list; the removed parts' directories still exist, unreferenced | the manifest decides: the removed parts are gone, the new ones live; the leftover directories are deleted at start-up |
| `ReplacePublished` (before any deletion) | as above | as above |
| `PartDirDeleted` (once per directory; in the middle of deleting several) | some of the removed directories deleted, the rest unreferenced | the rest are deleted at start-up |

After each crash the database reopens with exactly the surviving data (what
the manifest lists, plus the log's records after `wal_flushed_lsn`), and
nothing acknowledged is lost: a replacement does not touch `wal_flushed_lsn`
or the log.

## Retention

`EngineOptions::retention` is `None` (keep everything, the default) or a
duration. A part is expired when its whole time window is older than the
retention: `window_start_ns + window_ns <= now - retention`, computed without
overflow. A maintenance pass removes all expired parts with one replacement,
so retention is atomic for queries: a snapshot sees the expired parts or not,
never some of them. The clock is `EngineOptions::clock` (Unix nanoseconds; a
test facility like the stage hook), by default the system clock. A retention
of zero and a `maintenance_interval` of zero are refused by `open`.

**A trace is kept for at least the retention after the start of its window,
and for at most the retention plus one window plus one maintenance
interval.** At least, because a part is not removed before its whole window
has ended and the retention after that has passed, and a span starts inside
the window of the part that holds it. At most, because a part is expired one
window plus the retention after its window starts, and a pass looks at the
clock every `maintenance_interval` (or sooner, after a flush). Time spent in
the pass comes on top (a commit and some deletions), and so does the age of
data in the buffer: a part is written when the data is flushed, up to
`flush_interval` after it arrived.

What this means for particular data:

- **Data that is already expired when it arrives** is nothing special: it is
  logged, acknowledged, visible to queries while it is in the buffer, written
  by a flush into a part like any other, and removed by the first pass after
  that. It leaves queries one flush and one pass after it arrived.
- **Data with timestamps in the future** is kept until its window ages out: its
  part is not expired until `now` has passed the end of its window plus the
  retention. A sender whose clock is a year ahead has its data kept a year
  longer.
- **A trace is cut by window.** A part holds the traces of one window of one
  flush, each in the window of its earliest span in that flush's buffer.
  Retention removes parts, so a trace whose spans were flushed into different
  windows (late spans, a trace longer than a window) can lose its older part
  and keep the newer one; queries then return what is left.
- **The buffer is not touched.** Data in the buffer, expired or not, is
  removed only after a flush has written it into a part.

Statistics: `maintenance_passes` (every pass counts, failed ones too),
`retention_parts_removed` and `last_maintenance_error`. A pass that fails
records its error there (a later success does not clear it, like `last_error`)
and the next pass tries again; a failed step does not keep the later steps of
the pass from running.

## Compaction

Every flush writes one small part per window, so a window that keeps receiving
data ends up as many small parts and every query opens all of them, while a
window that has stopped receiving data should be one part. A merge is
`nwdb_segment::merge_parts`: the parts of one window become one part in which a
trace split over the inputs is one run, a span that occurs more than once is
kept once (the earliest input wins) and text is shared across the inputs and
never rebuilt. `EngineOptions::compaction` (`CompactionOptions`, validated by
`open`) holds the settings; `compaction.rs` holds the policy.

**The policy** is a pure function of the live parts: for each part its id,
window, size on disk (the sum of the files in its metadata), level and how long
ago it became live in this process (parts the manifest listed at start-up count
from start-up, and a merged part from the moment it was published). It returns
at most one merge, of parts of one window; a merge never crosses windows.

- **Tiered.** Within a window, sort the parts by size. If some run of at least
  `min_parts` (4) parts has its largest at most `size_ratio` (4) times its
  smallest, merge the first such run (the smallest parts), at most `max_parts`
  (32) of them, smallest first. Each byte is then rewritten once per tier it
  climbs, about `log(window size / flush size)` times, not once per later
  flush: a simulation of 4,096 flushes into one window (the unit test) rewrites
  5.9 bytes per byte ingested and never holds more than 13 parts.
- **Final.** If a window has at least two parts and none became live less than
  `quiet` (60 s) ago, merge all of them (at most 65,535) into one. This leaves
  one part per window once the window has stopped receiving data. It does not
  look at the event time of the window: data may be loaded long after it
  happened.
- **Byte cap.** No merge takes more than `max_input_bytes` (4 GiB) of inputs:
  the largest candidates are dropped until it fits, and a window with fewer
  than two left is skipped.
- Among the windows that qualify, the one with the most parts goes first (the
  oldest on a tie). Windows that are backing off or damaged are left out.

One huge part and many tiny ones: the tiny ones are merged among themselves and
climb tiers; the huge one is in a run only when `min_parts - 1` other parts are
within `size_ratio` of it, so it is rewritten once per roughly three quarters
of its own size in new data (the simulation: a part of 1 MB with 10 MB of late
data in parts of 1 KB is rewritten twice, and 6.1 bytes are rewritten per byte
ingested). The final rule rewrites it once more when the window goes quiet.
**A late small part for a window that was merged into one is merged into it
after the quiet period, which rewrites the whole window: late data that
arrives less often than `quiet` costs one rewrite of the window each time.**
That is the price of one part per window; lengthen `quiet` to batch late data,
or switch compaction off for a database that is loaded in arbitrary order.

**The pass.** Retention runs first, so a merge never works on an expired
window (one that expires during a merge is removed by the next pass). Then,
until the policy returns nothing or shutdown asks maintenance to stop:

1. the policy chooses a merge; an id is reserved;
2. `merge_parts` is called with the inputs oldest first (by part id), the
   window of the inputs, the writer options a flush uses (`splitter`,
   `dedup_window_bytes`) and a level one above the highest input level;
3. a reader is opened on the result and the stage `MergeWritten` is called;
4. `replace_parts(inputs, [result])` replaces them (see Replacing parts).

A merge holds no lock while it reads and writes; only step 4's commit and
publish hold the publish gate, as a flush does. Flushes, queries and retention
go on during a merge (a part a flush added meanwhile to the same window is not
an input and is merged by the next round of the loop). The inputs stay live
throughout, because only maintenance removes parts and passes run one at a
time. Snapshots taken before the replacement keep reading the inputs from open
files.

**Crash points of a merge and its replacement.** Nothing needs a rule beyond
those of Replacing parts: a part id that reached the disk is never reused and
start-up deletes the directories the manifest does not reference.

| Crash at / during | On disk | Recovery |
|---|---|---|
| inside `merge_parts` | `<id>.tmp` and the live inputs | the temporary directory is deleted at start-up |
| `MergeWritten`, `BeforeReplaceCommit` | the finished merged part, unreferenced; every input live | the merged part is deleted at start-up; nothing was replaced |
| the commit | the new manifest version exists or not | not there: as above. There: as below |
| `ReplaceCommitted`, `ReplacePublished` | the manifest lists the merged part; the inputs' directories remain, unreferenced | the manifest decides; the directories are deleted at start-up |
| `PartDirDeleted` | some input directories deleted | the rest are deleted at start-up |

A merge does not touch the log or `wal_flushed_lsn`, so every acknowledged
record is in live parts or in the log whichever side of the commit the crash
fell on; the crash tests abort at each stage and check that the reopened
database holds each acknowledged record exactly once.

**Failures.** An error before the commit leaves the database as it was and
removes the new directory (after `Fatal`, which includes a panic, the outcome is
not known and the directory is left for the next start). It is recorded in
`last_compaction_error` (and the pass returns it, so it is also
`last_maintenance_error`). The window is left alone for `retry_backoff` (30 s,
doubling with each failure in a row up to 64 times that; zero retries
at the next pass) and for the rest of the pass, so a failure that persists
neither spins in a pass nor holds back the other windows. Damage found in an
input is `Corrupt` (`merge_parts` checks every input in full before it starts
and names the input and its directory); the message also says which parts, the
window is not tried again until the next start, the inputs stay live and
readable as far as they are, and the engine is not put in the fatal state. A
failure to delete an input directory after the commit is not a failed merge:
the merge counts, and the pass records the deletion error and retries it.

**Shutdown during a merge.** A merge in progress is finished, not abandoned.
`merge_parts` has no way to be interrupted, so it has run to its end by the time
any decision could be made, and dropping its output would throw the work away to
save one manifest commit; after it, no further merge is started and the
directories of the replaced parts are left for the next start. The wait is the
rest of one `merge_parts` call, bounded by `max_input_bytes`, plus an open and a
commit. Lower `max_input_bytes` to shorten it. A merge that is abandoned for
another reason (the stage hook returns an error at `MergeWritten` or
`BeforeReplaceCommit`) deletes its output and leaves nothing behind; tests do
both.

**What a merge changes for queries.** A trace that was split over parts is one
run in one part and leaves the split map; a duplicate span (a sender's retry
that crossed a flush) is counted once by `list_traces` and `aggregate_tokens`
from then on (`get_trace` removed it all along). The spans of a trace are
grouped into a part by the window of the trace's earliest span in one flush, so
a trace whose spans reached the engine in different windows can have copies of
one span in different windows; those are not in one merge and stay duplicates.

Statistics (`EngineStats`, `/api/v1/stats`): `merges_done`,
`parts_merged_away`, `duplicate_spans_dropped`, `merge_input_bytes` and
`merge_output_bytes` (bytes on disk) since the engine opened;
`windows_with_many_parts` (windows that now have more than one live part, so
`0` means every window is settled); `merging` (a merge is being written or
replaced); `last_compaction_error`. `Engine::run_maintenance()` runs a pass,
merges included. `CompactionOptions::background` false (a test facility) keeps
the thread from merging so a test decides when.

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
  (before the log is rotated), `LogRotated` (before the trim), `Trimmed`.
- A merge: `MergeWritten` (its output is written and open; nothing is
  replaced yet). An error returned here abandons the merge: its directory is
  deleted and nothing else changes (`Fatal` puts the engine in the fatal
  state and the directory is left for the next start).
- A replacement of parts: `BeforeReplaceCommit` (arguments checked, publish
  gate held, just before the manifest commit; an error returned here is
  honoured, nothing changes, and `Fatal` puts the engine in the fatal state),
  `ReplaceCommitted` (before publishing),
  `ReplacePublished` (before any directory is deleted), `PartDirDeleted`
  (after each directory).

An error returned at a stage before the manifest commit is handled exactly as
if that step had failed with that error (`Fatal` puts the engine in the fatal
state; anything else restores the buffer and removes the parts of that
attempt). After the commit the return value is ignored, and so is a panic;
this covers every stage from `Committed` on, the replacement stages included.


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
  indexes ends at the first part that has it. When parts are removed, the part
  numbers in the map are rebuilt with the list (see Replacing parts).
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
  (`span_count`, token sums) and `aggregate_tokens` until the window is merged
  (the merge keeps one copy), and, if the two copies are in the buffer and a
  part, until that flush has happened too. Copies in parts of different
  windows (see Compaction) are never merged together.
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
  small parts, which compaction merges.
- Retention removes whole parts and nothing else: the buffer and the log are
  not touched by it. There is no object store.
- A merge cannot be interrupted (`merge_parts` has no cancellation), so
  shutdown waits for it, up to `max_input_bytes` of input. A window that gets a
  small late part more rarely than `quiet` is rewritten whole each time.
- The inputs of a merge are ordered by part id, which is the order of flushes,
  not of the data they hold (a merged part has a newer id than a flush
  published before it was merged). Only which of two identical copies is kept
  depends on it.
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
  at a named stage on its n-th occurrence, so every flush stage, every
  replacement stage and every start-up stage is hit deterministically. For each it checks the on-disk state
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
once. `tests/log_rotation.rs` checks the size of `wal/` after a flush and
opens a copy of the directory taken at each stage of a flush, with records
acknowledged after the freeze, to see that exactly the records not in parts are
replayed. `tests/compaction.rs` tests merging end to end (a resent span counted twice
and then once, a trace split over flushes in one part afterwards, the quiet
rule and late data, errors before the commit, a failing window that backs off
without holding back another, a damaged input, a flush and queries during a
merge, shutdown during a merge, the size of span-only data before and after,
and ingest, flush, queries, retention and merging at once); `tests/model.rs`
also runs the random walk with compaction and retention, in which spans are sent
again across flushes; `tests/crash_stages.rs` aborts the child at each stage of
a merge and its replacement. `tests/retention.rs` runs retention against a model with a clock the
test moves (across flushes and restarts), checks the boundaries, the thread,
shutdown during a pass, and ingest, flush, queries and maintenance at once for
a few seconds. The replace operation itself, which no public call reaches, is
tested inside the crate (`src/replace_tests.rs`): an old snapshot reads every
trace after the removal, a flush and a replacement wait for each other, errors
before the commit change nothing, and a directory that cannot be deleted is
retried. `tests/crash_stages.rs` also starts the child on a directory with
expired parts and a fixed clock, so that retention's replacement is aborted at
each of its stages.

Known limit: killing a process does not drop the operating system's page
cache, so a write that was never synced is still seen by the next start. The
tests therefore prove the order of the steps and the recovery rules, not the
order of the syncs; a power-loss test would be needed for that.
