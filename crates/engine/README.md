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

### Annotations

An annotation is a score, label or comment attached, after the spans were
written, to a trace or to one span of it (`nwdb_segment::Annotation`; its
identity is the trace, the span or none, the name and the source). A door does
not decode them: `annotate(items, on_durable)` takes the items themselves and
the engine writes one log record `[0xA1] + nwdb_segment::encode_batch(items)`
(`ANNOTATION_RECORD_TAG`). The life of such a request is the one above:

- `annotate` first checks each item, in this order: `validate` fails, the item
  is rejected with its message; the trace is unknown, it is rejected with
  "unknown trace". A trace is known if it has rows in the buffers or in a live
  part (`Snapshot::known_traces`, see Reads). The span id is not checked
  against the trace's spans, because spans may arrive later. The rejected
  items come back with their index in the request.
- Two accepted items of one request with the same identity: the later one
  wins, the earlier counts as accepted and is not logged (the log body never
  repeats an identity). If nothing is left to log (every item rejected) nothing
  is logged or admitted, `on_durable` is dropped uncalled and the call returns
  `Ok` with the rejections; admission errors are not returned for such a
  request. No items, or more than `MAX_BATCH_ITEMS` (1000), is
  `InvalidArgument` for the whole call.
- Admission is the same as for `ingest` and shares its code: `Fatal`,
  `Unavailable` when closing, when the buffer would pass `max_buffered_bytes`
  and when the log has too many bytes waiting. An annotation counts for 160
  bytes plus the bytes of its name, source, label and comment, in the
  in-flight total, in the buffer and everywhere else, so the limit means the
  same for both kinds of request.
- The record's number is the `arrival` of its items: it is stamped when the
  record is applied to the buffer, for live requests and replay alike, and the
  caller's value is ignored. Of all records of one identity the one with the
  largest `arrival` wins; a winning delete means there is no annotation.
  `time_ns` plays no part.
- Visibility is that of spans: the items enter the annotation buffer in log
  order, in the same step that makes a span record visible, and only then is
  `on_durable` called. They are not visible when `annotate` returns, and are
  visible to the sender when `on_durable` runs. `on_durable` is called exactly
  once, if and only if something was logged.
- Replay decodes annotation records with the record's number as `arrival`.
  The start-up pre-check reads them like any record (see Start-up), so one
  that does not decode refuses the start with the directory unchanged.

Statistics: `annotations_buffered` (in the active buffer and the buffer being
flushed), `annotation_parts` and `annotation_bytes` (live annotation parts and
the sum of their file sizes), `annotations_written` (records written into
parts by flushes since the engine opened) and `annotations_dropped` (see The
flush).

A binary that does not know the tag meets it in the log and refuses to start
with "unknown format tag" before it changes anything, so an older binary cannot
silently drop annotations.

## Directory layout

```
<data_dir>/            exclusively locked (flock on the directory itself) for the life of the engine
<data_dir>/manifest/   numbered manifest files
<data_dir>/parts/      one directory per part, of spans or of annotations (and <id>.tmp while being written)
<data_dir>/wal/        log files
```

## Start-up

Nothing is created, committed, renamed or deleted under `manifest/` or `parts/`
until every check that can refuse the start has passed. The only changes
before that are: creating the data directory, the log's own repair of a torn
tail, its removal of files that lie wholly inside parts (see step 3), and the
manifest crate's removal of its temporary files. In this order; each step
finishes before the next.

0. Validate the arguments: the options and duplicate decoder tags. A decoder
   registered under the annotation tag 0xA1 is `InvalidArgument`.
1. Create the directory if allowed, then lock it: the lock is `File::try_lock`
   on the open directory itself, so no file in it can be deleted to get around
   it, and no `LOCK` file exists. A second `open` is `Conflict`. The lock is
   released by `shutdown` or when the last handle is dropped.
2. Read, without changing: whether a manifest exists. If there is none and
   `parts/` holds any part directory (final or `.tmp`, whatever its id), the
   manifest is lost and the start is `Corrupt`: refusing is the only safe
   answer. If there is none and the database may not be created, `NotFound`.
   Otherwise load the manifest state, and open every live part by its kind: a
   span part through `PartReader` after its format version is checked, an
   annotation part through `AnnotationReader` after
   `check_annotation_format_version`. Any failure (another format version, a
   truncated or missing file) stops start-up with the directory unchanged.
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
   not empty, a known tag (a registered decoder's, or the annotation tag),
   decodes, decodes to the span table (for an annotation record: it decodes),
   numbers contiguous, the count as
   expected. Each record's batches are dropped at
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
| maintenance (`nwdb-maintenance`) | wakes every `maintenance_interval` (default 10 s) and after every flush, and runs one pass: it retries directory deletions that failed, applies retention, then merges parts until the policy finds nothing to merge, applying retention again between merges once an interval has passed (see Retention and Compaction) |
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

`Engine` is a cheap clone. `shutdown()` stops accepting and, in the same step,
tells maintenance to stop (the pass flag and the merge stop flag), waits for
every accepted request to be acknowledged, stops the flusher (a request in
flight may need a flush), waits for the maintenance thread, then takes the maintenance gate and the flush gate and holds them while
it flushes, shuts the log down, stops the applier and releases the lock; a `flush()` that raced with it finishes before it or finds
the engine closed, so nothing is written after the lock is gone. A second
concurrent `shutdown()` waits for the first to finish. Dropping the last handle
does the same without the flush (what is acknowledged is in the log), and if the
last handle is dropped by a callback running on an engine thread, the cleanup
runs on a separate thread rather than joining itself.

**How long shutdown can wait for maintenance.** A pass looks at the stop flag
between its steps (and compaction between its merges), and shutdown sets it
in the same critical section that refuses new requests, before it waits for
them, so no merge starts or finishes after ingest is refused. Shutdown waits for
the step in progress and no more. A step is one
manifest commit together with the publish that follows it (a file write and two
syncs; the split map is rebuilt before the gate, see Replacing parts), or the
deletion of one part directory, or a stretch of a merge that cannot be
interrupted. A merge in progress is abandoned: `merge_parts` checks the stop
flag before it verifies each input and before each chunk
(`MergeOptions::chunk_bytes`, 32 MiB of rows by default), removes its output and
returns `Unavailable`. Verifying one input (a sequential read of its files) and
the writer's `finish` at the end are not interrupted, so shutdown waits for the
longer of those and one chunk, not for one chunk in all cases. A merge that has already written its output is finished: shutdown then
waits for one open, one commit and one publish. No further merge is started.
If shutdown arrives while a replacement is deleting its directories, the ones
not yet reached stay on disk, unreferenced; the next start-up deletes them. A
pass that is waiting for the publish gate waits for a flush's commit and
publish, which is the same order of time. Nothing in a pass waits for ingest,
for queries or for a flush's writing of parts.

## The flush

`do_flush` (one at a time):

1. **Freeze.** Under the core lock, refuse (fatal) if a frozen buffer (of
   spans or of annotations) is already present, then move the whole active
   buffer, spans and annotations together, to the frozen slots (they stay
   visible to queries) and record `applied_lsn`. Because requests enter the
   buffers in log order under the same lock, every log position up to
   `applied_lsn` is in a frozen buffer or in a part, and the position is
   always a contiguous prefix. There is nothing to flush only when both active
   buffers are empty and `applied_lsn` equals `flushed_lsn`; a buffer that
   holds only annotations is flushed like any other, by size, by age or on
   demand.
2. **Group.** Sort the frozen rows by trace and span id, drop duplicate
   `(trace, span)` pairs (the row with the smallest `arrival` stays, then the
   first in batch order), order each trace's spans by
   `(start_ns, span_id)` and the traces by `(earliest start, trace id)`. A
   trace's window is `window_start` of its earliest span in this buffer.
   **Annotations.** The frozen annotations are reduced to one record per
   identity (the largest `arrival`). Each goes to a window, the largest (by
   `(window_start_ns, window_ns)`) of: the window its trace's rows go to in
   this flush, if the trace has rows in it, and the windows of the live span
   parts that hold the trace (found in the parts' in-memory trace indexes,
   newest window first, stopping at the first holder). The largest, because an
   annotation must never be in an older window than an earlier annotation of
   the same identity: removing the older window later would otherwise bring
   an overwritten value back. If no window results (the trace is in no live
   part and has no rows in this flush: retention removed it between the
   write and the flush) the annotation is dropped; the number of drops
   becomes part of `annotations_dropped` at the publish, so a flush that
   fails and is retried counts nothing twice.
3. **Write parts.** Reserve fresh part ids from the manifest, one per window
   of spans and one per window of annotations, in one reservation. For each window
   write one part through `PartWriter` in `parts/<id>.tmp`, which syncs and
   renames it to `parts/<id>`. Open a `PartReader` for it. The part writer is
   given `EngineOptions::splitter` and `EngineOptions::dedup_window_bytes`:
   with a splitter, the large text of the raw columns is cut out of the rows
   and stored once per part in the part's piece store (see `nwdb-segment` and
   `nwdb-payload`); without one every value stays in its row. Either way a read
   returns the original bytes, so nothing else here depends on it. The memory
   of a flush is the buffer being written plus, for the part being written,
   its row group and its de-duplication window (64 MiB by default). After
   the span parts, one annotation part per window that received annotations
   is written through `AnnotationWriter` (records in identity order, no
   splitter: annotations are small) and opened with `AnnotationReader`.
   `Stage::PartWritten` is called once per part of either kind, span parts
   first.
4. **Commit.** Take the publish gate, then one manifest commit adds all the
   parts, span parts and annotation parts, and sets `wal_flushed_lsn` to the
   frozen position. The gate is taken only now, after the long work of step 3.
   A flush that carries only annotations commits too, even when every
   annotation was dropped (the commit then only moves `wal_flushed_lsn`), and
   rotates and trims the log as after any flush.
5. **Publish.** Build the new part list and split map (outside the core lock,
   in memory, from the list the last replacement left; see Replacing parts),
   then under the core lock replace the part list and the
   split map, the annotation part list and its window index, and clear both
   frozen buffers in one step, so a snapshot sees the data, annotations
   included, exactly once and its map matches its parts. Then release the
   publish gate.
6. **Rotate and trim.** `Wal::rotate` starts a new log file at the current
   write position (nothing happens if the newest file holds no records), then
   `truncate_before(wal_flushed_lsn + 1)` removes the files wholly inside
   parts, only after the commit returned `Ok`. A trim never removes the newest
   file, so without the rotation the records of this flush would stay in it
   until it reached `max_file_bytes` (256 MiB by default): a benchmark found
   134 MB of log on disk after everything had been flushed. After a flush with
   no ingest in between, `wal/` holds one file of 20 bytes (its header). Records
   that arrive between the freeze and the rotation stay in the old file, which
   the next flush removes. The same rotation and trim run when a flush finds
   nothing to write, so a log that is all flushed (the state older versions
   left) is cleaned too, and at shutdown. The data is in parts whatever
   happens here, and a failure is recorded in `stats().last_error`:
   - If the new log file cannot be created (a full disk, say), the log keeps
     writing to the current file, whose records are intact, nothing is
     poisoned, and ingest goes on. The flush returns the error (it is not
     `Fatal`), the trim still runs, and the next flush tries the rotation
     again.
   - If syncing the current file fails, that is a failed sync like any other:
     the log is poisoned and the engine is in the fatal state at once, not at
     the next ingest, and the flush returns `Fatal`. A restart recovers from
     the log.
   - A failed trim is only recorded; the next flush tries it again.

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

A non-`Fatal` error in steps 2 to 4 puts the frozen buffers, spans and
annotations, back in front of the active ones (nothing is lost, and the
annotations keep their order), removes the parts this attempt wrote, of either
kind, and the retry uses fresh ids. `Fatal` from the log, the manifest or a part writer
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
`Inner::replace_parts(remove: &[u64], add: Vec<LivePart>, add_ann:
Vec<LiveAnnotationPart>) -> Result<Replaced>`: the ids of live parts to remove,
of either kind, and zero or more new span parts and new annotation parts, each
already written to `parts/` with its `PartMeta` and an open reader
(`PartReader` or `AnnotationReader`; ids reserved with
`manifest.reserve_part_ids`, one id space for both kinds). It is "replace these
by those". Every id in `remove` must be live in exactly one of the two lists
(span parts, annotation parts), otherwise the call is refused as below. There
are two lists but one edit, one commit and one publish:

1. **Commit.** One manifest commit that removes and adds them, parts of both
   kinds in the same edit.
2. **Publish.** The new span list, a split map that matches it, the new
   annotation list and its window index replace the old ones in one step under
   the core lock. A snapshot sees the state from before or the state from
   after, each with its own map; never a removed part and its replacement both
   present or both absent, and never one kind changed without the other (a
   window's spans without its annotations after both were removed together, or
   a merged annotation part missing). The span list and the map are built
   before the gate is taken (see below); the annotation list is small and is
   rebuilt under the gate from the list as it is then, so annotation parts a
   flush published in the meantime stay.
3. **Delete.** The directories of the removed parts are deleted, one at a
   time, and `parts/` is synced.

Rules:

- **A flush and a replacement never interleave.** The publish gate is held from
  just before the manifest commit to the end of the publish, by a flush and by
  a replacement alike. Each therefore reads the list the other left, and each
  commit's edit is valid against the state it was built from. Nothing long
  happens under it: a flush takes it only after its parts are written, a
  replacement only after its span list and split map are built, and deleting
  directories comes after it is released. The part list stays in part
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
  the removed parts, of either kind, from files its readers hold open, so a read never fails
  because a directory is gone. A directory that is already gone when the
  deletion gets to it is not an error.

The split map stores positions in the part list, so it is rebuilt for the new
list, in memory, outside the core lock and **before the publish gate is
taken**, from the list the replacement sees then. The step costs the traces of
the added parts times the number of live parts (see below); with 700 live
parts and a merged window of a million traces that is hundreds of millions of
steps, which must not block flushes, so the gate is taken only afterwards.
Under the gate the replacement reads the list again. A flush may have published
in between, and only a flush can have: replacements are made only by a
maintenance pass (retention and merging), passes run one at a time, and so no
other replacement can run between the build and the publish. The list now is
therefore the list seen plus the parts a flush added, and the replacement adds
those to the list and map it built, which costs what a flush's own publish
costs (the added parts' traces against the other parts). If the list is not
that (a part seen is gone, or a part to remove was added since), the
replacement is refused before its commit and changes nothing; this does not
happen while the rule above holds. The stage `ReplaceMapBuilt` marks the point
between the two steps, and a test publishes a flush there and compares the
resulting map with one built from scratch. The annotation list needs none of
this: a flush only adds to it, so the list under the gate is the list that
results from removing the ids and adding the new parts, whatever a flush
published meanwhile (a test flushes annotation parts into the window being
replaced while the replacement waits at `ReplaceMapBuilt`, and none is lost).

How the map is rebuilt:

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
(with the logarithm above). None of it is done under the gate except the parts
a flush added in the meantime.

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

The rows hold for both kinds: the directories of span parts and of annotation
parts, removed or new, are all "parts" to the manifest, and one edit changes
them together, so after a crash either every removed part of both kinds is
still live or none is.

After each crash the database reopens with exactly the surviving data (what
the manifest lists, plus the log's records after `wal_flushed_lsn`), and
nothing acknowledged is lost: a replacement does not touch `wal_flushed_lsn`
or the log.

## Retention

`EngineOptions::retention` is `None` (keep everything, the default) or a
duration. A part, of either kind (span part or annotation part), is expired
when its whole time window is older than the retention: `window_start_ns +
window_ns <= now - retention`, computed without overflow. A maintenance pass
removes all expired parts of both kinds with one replacement, in one manifest
version, so retention is atomic for queries: a snapshot sees the expired parts
or not, never some of them, and never a window's spans without its annotations
or the other way round (a test reads the manifest versions: the span parts and
the annotation parts of the expired windows go in one). The clock is `EngineOptions::clock` (Unix nanoseconds; a
test facility like the stage hook), by default the system clock. A retention
of zero and a `maintenance_interval` of zero are refused by `open`.

**A part, and so every trace in it, is kept until its window has ended and
the retention after that has passed, and is removed at most one maintenance
interval plus the length of one step of a pass after that.** The window of a
part is the window of the earliest span of each trace in the flush that wrote
it, so a span does not necessarily start inside its part's window: a trace
that is longer than a window, or has spans that arrive late, has spans that
start after the end of the window of the part that holds them. Retention goes
by the window, not by the start of each span: **a trace is removed with the
window of its earliest span in that flush**, and a span that starts much later
than that can be removed while it is younger than the retention.

The bound on how late a removal is: retention runs at the start of every pass,
and in a pass that merges, again before a merge whenever `maintenance_interval`
has passed since it last ran, together with the retry of deletions that
failed. Passes start at least every `maintenance_interval` (and after every
flush). So two runs of retention are never further apart than
`maintenance_interval` plus one step, where a step is the commit and publish of
a replacement, the deletion of one directory, or one merge (the longest, see
the bound on shutdown). A part that has expired is therefore removed no later
than `maintenance_interval` plus one step, plus its own commit and deletions,
after its window plus the retention has passed. A pass that keeps finding
merges (steady flushes into a busy window) does not hold retention back by
more than that. On top of that comes the age of data in the buffer: a part is
written when the data is flushed, up to `flush_interval` after it arrived.

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
  flush, each in the window of its earliest span in that flush's buffer; its
  other spans go with it, whenever they start. Retention removes parts, so a
  trace whose spans were flushed into different windows (late spans, a trace
  longer than a window) can lose its older part and keep the newer one;
  queries then return what is left.
- **The buffer is not touched.** Data in the buffer, expired or not, is
  removed only after a flush has written it into a part.
- **An annotation lives as long as the newest window that held its trace when
  it was flushed**, because that is the window of the annotation part it was
  written to (see The flush). It goes with that window, and nothing else
  decides: an annotation flushed into the window of a trace's older part
  expires with that window even if a later window still holds part of the
  trace (it is then gone while the trace is still readable), and an
  annotation flushed into the later window outlives the older one. The window
  rule is what keeps an overwritten value from coming back: of two records of
  one identity the newer is never in an older window than the other, so
  expiring the older window can take the older record and never the newer one
  alone. An annotation part whose window holds no span part any more (it
  happens when a retention pass removed the trace's part between a flush's
  window decision and its commit) is removed when its own window expires, like
  any other; until then `annotations` does not show it, because annotations are
  looked up through the windows of the trace's span parts.

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
kept once (the copy that arrived first, the one with the smallest `arrival`,
wins; the order of the inputs only breaks a tie) and text is shared across the inputs and
never rebuilt. Annotation parts are merged too, by
`nwdb_segment::merge_annotation_parts`: the annotation parts of one window
become one part holding, of each identity, the record with the largest
`arrival` (see below for what is kept). **The two kinds are never mixed**: a
merge has inputs of one kind and one window. `EngineOptions::compaction` (`CompactionOptions`, validated by
`open`) holds the settings; `compaction.rs` holds the policy.

**The policy** is a pure function of the live parts: for each part its id, kind,
window, size on disk (the sum of the files in its metadata), level and how long
ago it became live in this process (parts the manifest listed at start-up count
from start-up, and a merged part from the moment it was published). It returns
at most one merge, of parts of one kind in one window; a merge never crosses
windows or kinds. The parts are grouped by (kind, window), and every rule below
is applied to one such group: the span parts of a window and its annotation
parts are two groups, each with its own count, its own tiers and its own
"quiet". The size of an annotation part is its file size from `PartMeta.files`.

- **Tiered.** Within a window, sort the parts by size. If some run of at least
  `min_parts` (4) parts has its largest at most `size_ratio` (4) times its
  smallest, merge the first such run (the smallest parts), at most `max_parts`
  (32) of them, smallest first. Each byte is then rewritten once per tier it
  climbs, about `log(window size / flush size)` times, not once per later
  flush: a simulation of 4,096 flushes into one window (the unit test) rewrites
  5.9 bytes per byte ingested and never holds more than 13 parts.
- **Final.** If a window has at least two parts and none became live less than
  `quiet` (60 s) ago, merge the smallest `max_parts` (32) of them, smallest
  first. A window of many parts (after a period with compaction off there can
  be thousands) is then merged in steps of at most 32 inputs, each step taking
  the next smallest, until it is one part; a merge looks up every trace of
  every input, so one merge of thousands of inputs would be very slow and
  shutdown would wait for it. The output of a final merge counts as live
  since its youngest input became live, so as quiet as they were, and the
  window goes on to its next step at once instead of after another `quiet`
  (the output of a tiered merge counts from the moment it was published).
  This leaves one part per window once the window has stopped receiving data.
  It does not look at the event time of the window: data may be loaded long
  after it happened.
- **Byte cap.** No merge takes more than `max_input_bytes` (4 GiB) of inputs:
  the largest candidates are dropped until it fits, and a window with fewer
  than two left is skipped.
- **Piece ceiling.** A part holds at most `max_pieces` pieces of text (the
  writer's ceiling, 16,777,216); text past it is stored inline and cannot be
  searched. A merge whose inputs have more pieces between them than that would
  change what a search finds, so it is not chosen: the largest candidates are
  dropped until the pieces fit, and a window with fewer than two left is
  skipped, as for the byte cap. Such a window stays in more than one part
  (`windows_with_many_parts` counts it), and nothing is retried, because
  nothing is attempted. The piece count of a part is `meta.payload.pieces`; the
  the ceiling is the one the engine writes parts with
  (`EngineOptions::max_pieces`, a test facility). It does not concern
  annotation parts, which have no pieces.
- Among the groups that qualify, the one with the most parts goes first, across
  kinds (the oldest window on a tie, span parts before annotation parts within
  one window), so neither kind starves the other. Groups that are backing off
  or damaged are left out: **the failing and damaged groups are remembered per
  (kind, window)**, so an annotation part that cannot be merged does not stop
  the span merges of its window, and the other way round.

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
window. Then, until the policy returns nothing or shutdown asks maintenance to
stop (retention and the retry of deletions run again before a merge once
`maintenance_interval` has passed since they last ran; see Retention):

1. the policy chooses a merge; an id is reserved;
2. the stage `MergeStarting` is called, then, by the kind of the plan,
   `merge_parts` (span parts) or `merge_annotation_parts` (annotation parts)
   with the inputs oldest first (by part id; the order decides only between
   copies of a span, or records of an identity, with the same `arrival`), the
   window of the inputs and a level one above the highest input level; a span
   merge also gets the writer options a flush uses (`splitter`,
   `dedup_window_bytes`); both get the stop flag;
3. a reader is opened on the result (`PartReader::open` or
   `AnnotationReader::open`) and the stage `MergeWritten` is called;
4. `replace_parts` replaces the inputs by it (see Replacing parts), in the
   list of its kind.

The two kinds share everything around the merge call itself (the id, the level,
the stages, abandoning the output, the replacement, the statistics, the failure
handling below); only the input list, the merge function and the reader that
opens the result differ.

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
| `MergeStarting` | the live inputs | nothing was written |
| `MergeWritten`, `BeforeReplaceCommit` | the finished merged part, unreferenced; every input live | the merged part is deleted at start-up; nothing was replaced |
| the commit | the new manifest version exists or not | not there: as above. There: as below |
| `ReplaceCommitted`, `ReplacePublished` | the manifest lists the merged part; the inputs' directories remain, unreferenced | the manifest decides; the directories are deleted at start-up |
| `PartDirDeleted` | some input directories deleted | the rest are deleted at start-up |

The same table holds for a merge of annotation parts: the output is an
`annotations.nwa` part directory instead of a span part, and every row is the
same (the crash tests abort at each stage of an annotation merge and of a
retention replacement that holds annotation parts, and check that every
acknowledged annotation is there once as the winner, or gone with its expired
window, and that no directory is left over).

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
neither spins in a pass nor holds back the other windows or the other kind of
the same window. Damage found in an
input is `Corrupt` (`merge_parts` and `merge_annotation_parts` check every
input in full before they start; the engine names the damaged annotation input
and its directory in the error); the message also says which parts, the
window is not tried again until the next start, the inputs stay live and
readable as far as they are, and the engine is not put in the fatal state. A
failure to delete an input directory after the commit is not a failed merge:
the merge counts, and the pass records the deletion error and retries it.

**The stop flag.** The engine owns an `AtomicBool` (`Inner::merge_stop`) that
shutdown sets, together with the flag that refuses new requests, before it waits
for those requests or for the maintenance thread. The merge loop starts no
further merge once it is set (the flag is tested before each merge, and a merge
abandoned because of it ends the loop, so the loop cannot spin while the thread
exits), and a merge that returns an error while it is
set is not counted as a failure: no backoff, no `last_compaction_error`, and
the engine is not put in the fatal state (unless the error is `Fatal`). The
flag is also handed to `merge_parts` (`MergeOptions::stop`), which ends a
merge that is running with `Unavailable` at the next place it reads the flag:
before an input is verified, or before a chunk.

**Shutdown during a merge.** A merge in progress is abandoned at the next
place it reads the stop flag (before an input is verified, before a chunk): its temporary output is removed, no part is added or replaced, and the
inputs stay live; the next start merges them again. A merge that has written
its output when shutdown begins is finished, since only an open and a commit
are left. A merge that is abandoned for another reason (the stage hook returns
an error at `MergeWritten` or `BeforeReplaceCommit`) deletes its output and
leaves nothing behind; tests do both.

**Annotation merges.** The output holds, of each identity (trace, span, name,
source) in the inputs, the record with the largest `arrival`, in identity
order; the others are counted in `annotations_superseded`. **A delete record is
never dropped by a merge**, even when it is the winner and nothing older is in
the inputs: an older record of the same identity can be in an annotation part of
an older window (an annotation is in the newest window that held its trace
when it was flushed, so the older version can be in an earlier window), and
dropping the delete would show that record again as soon as nothing newer hides
it. A delete is dropped only when its window expires, with everything else in
it. What a merge changes for readers: before it `annotations` reads the trace's
records from several parts of the window and takes the largest `arrival`, after
it from one, with the same winners, the same deletes and the same filtered
listings; a snapshot taken before keeps reading the inputs.

**What a merge changes for queries.** A trace that was split over parts is one
run in one part and leaves the split map; a duplicate span (a sender's retry
that crossed a flush) is counted once by `list_traces` and `aggregate_tokens`
from then on; `get_trace` and `search` showed one copy all along, and it is the
same copy as after the merge. The spans of a trace are
grouped into a part by the window of the trace's earliest span in one flush, so
a trace whose spans reached the engine in different windows can have copies of
one span in different windows; those are not in one merge and stay duplicates
for the summaries, while `get_trace` and `search` still show one.

Statistics (`EngineStats`, `/api/v1/stats`): `merges_done`,
`parts_merged_away`, `merge_input_bytes` and `merge_output_bytes` (bytes on
disk) count merges of both kinds; `duplicate_spans_dropped` (span merges) and
`annotations_superseded` (annotation merges: records dropped because a newer
record of the identity was in the same window) since the engine opened;
`retention_parts_removed` counts parts of both kinds;
`windows_with_many_parts` (pairs of a window and a kind of part with more than
one live part, so `0` means every window is settled, for span parts and for
annotation parts alike: a script that waits for it waits for both); `merging` (a merge is being written or
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
- A flush: `FlushFrozen`, `PartWritten` (once per part of either kind, span parts first), `AllPartsWritten`
  (before the commit), `Committed` (before publishing), `Published`
  (before the log is rotated), `LogRotated` (before the trim), `Trimmed`.
- A merge: `MergeStarting` (before `merge_parts`; nothing is written; an error
  returned here is the merge's own error), `MergeWritten` (its output is written and open; nothing is
  replaced yet). An error returned here abandons the merge: its directory is
  deleted and nothing else changes (`Fatal` puts the engine in the fatal
  state and the directory is left for the next start).
- A replacement of parts: `ReplaceMapBuilt` (the list and split map are
  built, the publish gate is not held; an error returned here changes
  nothing, `Fatal` puts the engine in the fatal state), `BeforeReplaceCommit` (arguments checked, publish
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
  (the copy with the smallest `arrival` kept, then the first found), ordered by `(start_ns, span_id)`, returned in
  byte-bounded batches. A trace in a single part that is already in order is
  returned untouched.
- `annotations(trace, limit)`: the winning annotations of a trace, on the
  trace and on its spans, deletes applied (a deleted identity is absent), in
  identity order (trace-level first, then by span id, name, source), at most
  `limit`, and a flag that says more exist. The annotation parts are kept in
  their own list in the snapshot (`Visible.parts` and the split map hold span
  parts only, so listing, search, token sums and `known_traces` do not see
  them). The windows that hold the trace are those of the span parts that hold
  it (the split map, or the first part whose in-memory index has it); the
  records of the trace are read from every annotation part of those windows
  (a lookup reads only the blocks that can hold the trace, typically one per
  part) and merged with the trace's buffered records by `Latest`: per identity
  the largest `arrival`, whether it is in a part or the buffer, so a delete in
  a later part or in the buffer hides an older value. `annotations_with_stats`
  returns `AnnotationStats` (parts probed, blocks read). A trace that is only
  in the buffer has only buffered annotations.
- `known_traces(ids)`: for each id, whether the trace has rows in a live part
  or in the buffers. The ids are sorted once; each part's in-memory trace
  index is walked forward with the galloping search of the split map, for the
  ids still unknown, with no I/O; only if some are still unknown, one pass over
  the trace id column of every buffered batch.
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
- The annotation filter of `list_traces` (`ListQuery::annotation`): a trace
  matches if some winning, not deleted annotation on the trace or on any of its
  spans has the filter's `name`, and its `source` if one is given, a value of
  at least `min` and at most `max` if given (an annotation without a value
  matches neither bound) and its `label` if given. It is a per-trace test
  applied to the merged summary after the other filters, like `error`, so the
  early stop above holds. How it is evaluated, per candidate trace:
  - A candidate that is split (held by more than one span part), or has any
    buffered annotation, or exists only in the buffer, goes the exact way:
    `annotations` and the test on each winner (`annotation_exact_lookups`).
  - Every other candidate is in exactly one span part, so all its annotations
    are in the annotation parts of that part's window (an annotation goes to
    the largest window of its holders, see The flush). For that window the set
    of trace ids that match is built once, on first use, by one `Latest` scan
    over all its annotation parts (`annotation_windows_scanned`); a winner that
    is not a delete and passes the test puts its trace id in the set, and
    membership answers the filter. At most the two most recently used sets are
    kept. A set holds at most 1,048,576 trace ids (8 bytes of id plus hash
    table overhead each, so about 40 MB at most per set): past that the query
    fails with `InvalidArgument` saying more than that many traces of one
    window match and the filter must be narrowed.
  - Cost: a filter that most traces match stops with the newest windows
    (`limit` results come from one or two windows); a filter that few match
    reads each window that has annotation parts once, with no per-trace
    block read.
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

- `search` (see "Search" below): full-text search over the flushed parts.

A trace's summary is a function of its set of spans only: merging partial
summaries in any order gives the same result as summarising all the spans at
once. `session_id` and `user_id` are the smallest non-null value among the
spans, and the root fields come from the root with the smallest `(name,
service)`. A malformed trace with several roots therefore shows the smallest
root, not the earliest.

## Search

`Snapshot::search(&SearchQuery { query, from_ns, to_ns, limit })` returns the
newest `limit` spans whose text matches the query, with `start_ns` in
`[from_ns, to_ns)`, ordered by `(start_ns` descending, trace id, span id)`. A
hit is the span's trace id, span id, start, end, name, service name, operation
and request model. `Engine::search` takes a fresh snapshot and returns the hits
with `searched_through_lsn`; a snapshot has `searched_through_lsn()` too.
`query` is an `nwdb_textindex::Query`, built by `nwdb_engine::textindex::parse`
from the grammar documented in that crate (words are ANDed, `OR`, a leading `-`,
`"a phrase"`, `prefix*`, parentheses).

**Freshness: search covers flushed parts only.** Spans that are still in the
memory buffer (the active buffer, or the one being flushed) are not searched.
They become searchable at the next flush, at most `flush_interval` (30 seconds by
default) after they arrive, or sooner when the buffer reaches its size limit. The
log position returned with the hits is the highest position whose data is in
the parts that were searched: a caller that knows the position its own spans were
acknowledged at can tell whether they could be found yet. A snapshot never
changes: one taken before a flush does not see the part the flush writes.

What is searched is what the parts index: the text held in pieces (see the
segment README: text under 8 bytes, attribute values under 256 bytes that are
not under a content key, and text past a part's piece ceiling stay inline in the
span row and are not searched).

How the walk goes, and why it may stop early. Every span of a part starts no
later than the part's `U`, the largest maximum in the statistics of its
`start_ns` column (`PartReader::max_start_ns`). The parts are searched in
descending order of `U` (older part first among equal ones), the best `limit`
hits are held, and the walk stops when `limit` hits are held and the next
part's `U` is below the start of the worst held hit: a span enters the answer only
if it sorts before the worst held hit, the order starts with the start descending,
so no span of that part, nor of any later part (their `U` is no larger), can enter.
The test is strict, since a span that starts exactly at the worst held start can
win on trace id. The worst held hit only gets better as hits are added, so a part
that was ruled out stays ruled out. Parts whose time bounds cannot reach the
range (`min_start_ns >= to`, or `U < from`) are not looked at. `search_with_stats`
returns `SearchStats` (parts searched, skipped early and out of range, rows
matched and read, winner lookups, confirm bytes) and tests pin them.

Per part, `PartReader::search` gives the matching rows. They are read in ranges
(rows less than 64 apart are read together, at most 16,384 rows a call) with a
projection of nine columns that has none of the raw columns, so no piece is read
and no rope rebuilt (a test checks that a search of words reads no piece block;
only a phrase, which has to be confirmed against its text, reads pieces). Only the
`limit` best hits are held, so memory is one range of rows, the matching row
numbers of one part, the hits and the winner cache described next (at most
2^20 span entries, 8 bytes of span id and 16 of winner each plus map overhead;
one trace with more spans than that is still held whole).

The same span (trace id and span id) can be in two parts: a retry that crossed a
flush, until a merge. Only its winning copy is a hit: the one with the smallest
`arrival` (the log record the row arrived in), and among equal values the part
that is first in the list. This is the copy `get_trace` returns and a merge
keeps, so the answer does not change when parts are merged. A row that matches
is checked before it is held or counted toward the limit, whatever the limit,
the time range and the query: a losing copy is never returned, whether or not
the winner matches or lies in the time range, and the early stop above stays
valid. The check costs one read of the trace in each part that holds it (the span
ids and `arrival` of its rows are read, nothing else), once per search however
many parts have matching rows in it: a trace split over H parts costs H reads,
not H times H-1. The winner of every span id of the trace among all holders is
kept in a cache shared by the whole search, bounded at 2^20 span entries in
total (a single trace with more spans is still held whole); when adding a trace would pass the bound the cache is emptied first, which
costs reads again and never changes the answer. The check is only for a hit
whose trace is in the split map; a trace in one part is not checked. A search
reads `span_id` and `arrival` of a split trace from every part that holds it, so
damage in one of those parts fails a search whose hits are in other parts.
Spans in the buffers are not searched and always arrived later than the spans in
parts, so they need no check. The limit counts spans.

Limits of a search are those of `SearchLimits` (1,024 terms for a prefix, 256 MiB
of piece text read to confirm, 10 million rows a leaf); a query over one is
`Error::InvalidArgument` saying which. The terms and rows limits apply to each
part. The confirm limit is one budget for the whole search: each part is given
what the parts before it left, so a phrase of common words that occurs nowhere
reads at most 256 MiB in all, not in each part (the message then names the part's
remaining budget). The text index and the row
lists of the parts a flush writes have the budgets
`EngineOptions::text_index_budget_bytes` and `EngineOptions::row_list_budget_bytes`
(32 MiB each by default, per part being written).

## Known limits

- `list_traces` combines the per-trace summaries of every part (and of the
  buffers) that holds the trace (`TraceSummary::merge`, crates/core/src/summary.rs),
  whatever copy of a span each was built from. A span stored in two parts, or in
  a part and the buffers, is therefore counted twice (span count, token sums)
  until the parts are merged (the merge keeps one copy and writes the summary
  from the rows it kept) or, for the buffers, until the flush has happened. More
  than the counts is affected: the fields a listing shows and filters on (root
  name and root service, models, session id, user id, start, end, error) are the
  smallest, largest or union over all copies, so they can come from a copy that
  `get_trace` and `search` do not show, for example a later-arriving copy with
  another root name, model or session, an error, or an earlier start or later
  end. `aggregate_tokens` does not read summaries: it scans the span rows of the
  parts, so it counts every stored copy with that copy's own model, tokens and
  start (the buffers are de-duplicated by span id). This holds for any span
  stored twice, not only for identical copies. Copies of a span in parts of
  different windows (see Compaction) are never in one merge, so for those this
  lasts until retention removes the parts. `get_trace` and `search` always show
  exactly one copy of a span, the one with the smallest `arrival`, before and
  after any merge.
- An annotation lives as long as the newest window that held its trace when it
  was flushed (see Retention): it goes with that window even if a later window
  still holds part of the trace, so a trace spread over windows can be readable
  with its annotations gone. An annotation part whose window no live span part
  holds is not shown by `annotations` (it is removed when its window expires).
  Annotation parts of one window are merged into one, and a delete record
  stays in the part until the window expires, so a window of mostly deletes
  does not shrink.
- A span id of an annotation is not checked against the trace's spans (spans
  may arrive later), and an annotation for a trace the engine does not know
  yet is refused ("unknown trace"): annotate after the spans were
  acknowledged.
- A trace's annotations in the buffer are found by reading every buffered
  annotation, and `known_traces` reads the trace id column of every buffered
  batch for ids it did not find in a part; both cost time proportional to the
  buffer.
- The annotation filter of a list builds one set of matching trace ids per
  window it meets, up to 1,048,576 ids.
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
- A merge is stopped only between inputs being verified and between chunks,
  so shutdown waits for the one input being verified, the chunk that is running
  (32 MiB of rows by default) or the writer's `finish`, whichever is longest. A window that gets a
  small late part more rarely than `quiet` is rewritten whole each time.
- There is no check of free space before a merge. Its output, up to
  `max_input_bytes` of data, is written while its inputs are still on disk, so
  a merge needs that much space in addition. A full disk makes the merge fail
  (the window backs off and is tried again) and also fails log appends, which
  is `Fatal` for the log.
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

`tests/crash_annotations.rs` runs the child with `NWDB_ANNOTATE`: it annotates
the traces it ingests (or, alone, traces that are already stored), aborts at
each stage of a flush that carries spans and annotations, of a flush that
carries only annotations, and of a start-up that replays annotation records,
and checks that every acknowledged annotation is the winner exactly once after
the restart and that no part directory is left over.

`tests/startup_safety.rs` and `tests/regressions_second_pass.rs` check that
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
