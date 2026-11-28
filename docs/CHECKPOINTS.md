# Checkpoints

The engine is built in small steps. Each checkpoint is a tested increment:
its tests pass in a Linux container, it has been through an independent
review, and it is committed, tagged `checkpoint-NN` and pushed.

```
Progress  [████████░░░░░░░░]  8 of 16 checkpoints
```

| # | Checkpoint | State |
|---|---|---|
| 0 | Repository set-up: licence, design, experiments and measured results | done |
| 1 | Storage core as libraries: shared types, write-ahead log, span parts (segment files, trace index, summary), manifest | done |
| 2 | Engine: memory buffer, flush, recovery from the log, snapshot reads; open a trace, list traces, aggregate | done |
| 3 | Server: OTLP door for traces, HTTP query API, container image, end-to-end test with the generator | done |
| 4 | Large text out of line, each distinct piece stored once | done |
| 5 | Text index over distinct pieces; search | done |
| 6 | Compaction, retention, late spans | done |
| 7 | Scores and annotations | done |
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
- Span-only data takes more space than the baseline until parts are
  compacted. Token totals over a long range read every part on one thread.

## Checkpoint 5: full-text search

The text a part stores once is now indexed once, and
`GET /api/v1/search` returns the newest spans whose messages match a query.

- **An index of our own, over pieces instead of spans.** `nwdb-textindex` is
  an inverted index with its own file format and no search library under it.
  A part indexes each distinct piece of text once (`text.idx`) and keeps, per
  piece, the rows that use it (`text.rows`). A system prompt that ten thousand
  spans repeat is cut into words once, and a phrase in it is checked once.
- **The query language.** Words are ANDed; `OR`, a leading `-` for NOT,
  `"a phrase"`, `prefix*` and parentheses. ASCII case does not matter and any
  script is a word. The index holds no positions: a phrase finds its
  candidates by its words and is confirmed against the text of the piece, so
  a phrase must lie inside one message.
- **JSON is searched as the text it encodes.** A piece that is a complete JSON
  value is indexed with its escapes decoded, so a word after `\n` or one
  written as `\u00e9` is found. The stored bytes are not changed.
- **Every cost has a limit.** How many terms a prefix may expand to, how many
  rows one word may match in a part, and how much text one search may read to
  confirm phrases (one budget for the whole search). Over a limit is an error
  that says which, not a slow query.
- **Bounded memory on both sides.** The index is built within a byte budget
  whatever the input (one very large message is spread over several index
  segments), and a search reads each group of row lists once however many
  pieces match.
- **The part format is version 7.** The index files are bound to their part by
  checksum when the part is opened, like the text files of checkpoint 4.

Measured with the benchmark harness (2 CPUs, 4 GB; full results in
[benchmarks](benchmarks/README.md)): on 8.0 GB of agent-trace text, the newest
20 spans for a word, an AND, an OR or a prefix come back in 0.24 to 0.30 ms at
the median, a NOT in 0.57 ms and a phrase in 1.5 ms. The part files, text
index included, take 87.8 MB.

What is searched: the pieces, in flushed parts. Not searched: spans still in
the memory buffer (at most one flush interval, 30 seconds by default), values
too short to be pieces, and the promoted columns such as the span name and the
service, which the list filters cover.

How it was tested: searches over random text compared with a scan of the same
text, for every operator, through the index alone, through a part, through the
engine and through the server; index and row files truncated, swapped between
parts and damaged byte by byte; memory measured while building and searching;
and the end-to-end run now searches for a word and a phrase before a crash,
after `kill -9` and after a clean stop.

Two independent examinations of the search path and a second pass over the
fixes found these defects, each proven with a failing test first and kept as
a regression:

- Index and row files of another part, of equal length, were accepted, and a
  search then answered from the wrong part's text.
- A span stored twice could be returned as either copy depending on `limit`.
  (The rule that settles this is in checkpoint 6.)
- A word after a JSON escape was not found: `Hello\nError` was indexed as the
  word `nerror`.
- One large message made of distinct words took the index builder far past
  its memory budget.
- A common word read the row lists once per matching piece instead of once.
- An empty AND matched nothing instead of everything.
- Checking which copy of a duplicated span to show cost a number of reads that
  grew with the square of the number of parts holding the trace.

Known limits at this checkpoint:

- A value that is not complete, valid JSON (a message array cut off by a
  sender's length limit, for instance) is indexed as plain text. A word that
  directly follows an escape in it is not found.
- No ranking. Results are the newest matching spans.
- Of a span stored twice, only the copy that arrived first is ever returned,
  even when only the later copy matches.
- Damage in one part fails a search that has to read that part, including to
  decide which copy of a span in a split trace to show.

## Checkpoint 6: compaction, retention, late spans

A flush writes one small part per 15-minute window. A background thread now
merges them, removes data past its retention, and trims the log.

- **Merging.** Parts of similar size in one window are merged once there are
  four (so data that keeps arriving is rewritten a logarithmic number of
  times, about six bytes written per byte ingested in the simulation), and a
  window that has gone quiet becomes one part. A merge never crosses a window,
  holds no lock while it works, and runs in bounded memory: it reads a
  chunk of rows at a time (32 MiB by default) and carries the stored text over
  without rebuilding it.
- **Retention.** `--retention 7d` removes parts whose whole window is older
  than that, all expired parts in one step, so a query sees a part or does
  not. Retention is not held up by a busy merge.
- **Late and repeated spans.** Every span row now carries `arrival`, the
  number of the log record that brought it. Where one span is stored more than
  once (a sender's retry that crossed a flush), the copy that arrived first is
  the one shown: by a flush, by a merge, when a trace is opened and in search.
  The number comes from the log, so a crash, a restart or the order of merges
  cannot change which copy that is. A merge drops the later copies.
- **The log is rotated after each flush**, so it no longer keeps records that
  are already in parts.
- **Shutdown does not wait for a merge.** A running merge is abandoned and
  leaves nothing behind; the next start merges again.
- **Smaller span tables.** Column encodings were chosen per column (deltas for
  sorted values, bytes split by position for ids).

Measured with the benchmark harness (2 CPUs, 4 GB; full results in
[benchmarks](benchmarks/README.md)): ten million spans without text take
431 MB in 95 parts, one per window, down from 560 MB in 125 parts, and less
than the ClickHouse baseline's 469 MB. The saving came from the column
encodings, not from merging, which rewrote 143.2 MB into 142.8 MB; what
merging buys is one part per window for queries and, on text, sharing across
flushes. The log holds 4 KB after a load instead of 134 MB.

How it was tested: the process is aborted at every stage of a merge and of a
replacement, and each acknowledged record must be there exactly once after the
restart; ingest, flushes, queries, retention and merges run concurrently
against a model; merges with damaged inputs, at the piece ceiling, stopped
half way, and with resident memory measured; rotation with a file that cannot
be created and with a directory sync that fails.

Two independent examinations and a second pass over the fixes found these
defects, each proven with a failing test first:

- Which copy of a repeated span was kept depended on part order: a tiered
  merge could keep the later one, and the answer changed when another window
  was merged.
- A window that kept merging could hold retention back without bound.
- A failed log rotation left the log unusable until a restart; a failed
  directory sync during rotation could leave files that a restart refused.
- Very large option values overflowed and panicked a background thread.
- A merge whose inputs held more text pieces than one part may hold moved text
  out of the index, so searches lost hits. Such a merge is now not chosen.
- A row listed under the wrong trace by a damaged index was merged as data.
- Shutdown waited for a merge to finish, which could be minutes.

After the fixes, each behaviour the fixes of checkpoints 5 and 6 added was
taken out again, one at a time, to see a test fail: 53 removals. For 42 a test
failed as it should. Two were expected to change nothing and changed nothing
(the order of a merge's inputs and the order in which parts are read no longer
decide which copy of a span is shown). One could not be reached by any input.
For the remaining eight no test noticed, among them the comparison of a row's
trace id in a merge and the arrival term of a flush's sort key; a test was
added or tightened for each until the removal made it fail.

Known limits at this checkpoint:

- Until a merge has dropped the later copy of a repeated span, token totals
  count both, and a trace listing can show fields from either. Copies that
  were flushed into different windows are never in one merge, so for those
  this lasts until retention removes them. Opening a trace and searching show
  one copy all along.
- Late data for a window that was already merged into one part rewrites that
  window once it is quiet again.
- Retention goes by the window of a trace's earliest span in a flush, not by
  each span's own time.
- A merge does not check free disk space before it starts; it needs about the
  size of its inputs.
- Stopping a merge waits for the input being verified or for the final write
  of the output, whichever is in progress.
- The fixes of the last pass have had removal checks but no further
  independent examination.

## Checkpoint 7: scores and annotations

Evaluation scores, human labels and feedback arrive minutes or days after the
spans they are about, and span files are immutable. An annotation is now a
record of its own, attached to a trace or to one span of it.

- **The record.** A name (`accuracy`, `thumbs`), a source (who or what made
  it), a time, a number and/or a short label, an optional comment. Written
  again for the same trace, span, name and source, the latest write wins; a
  delete removes it.
- **Writing.** `POST /api/v1/annotations` takes up to 1,000 at a time. The
  answer comes after the write is in the log and synced, and from then on the
  annotation shows. An annotation for a trace the database does not hold is
  refused, by its position in the request; the others are written.
- **Storage.** Annotations are flushed into immutable annotation parts beside
  the span parts, in the same time windows, listed in the same manifest
  (format version 2; version 1 is still read) and committed in the same step.
  The file has its own small format: checksummed compressed blocks and a table
  that finds one trace's annotations by reading one block. Which version wins
  is decided by the number of the log record that carried it, so a crash, a
  restart or a merge cannot change it.
- **Reading.** A trace shows its annotations. The trace list filters on them
  (has a score named so, a value range, a label) and shows up to eight per
  trace. A filter scans a window's annotations once instead of looking up each
  trace.
- **Merging and expiry.** Annotation parts of a window are merged into one,
  and removed with their window by retention, in the same step as its spans.
  An annotation is stored in the window that ends last among those holding its
  trace, so removing an older window can never bring an overwritten value
  back. A delete is kept by merges for the same reason.
- **The first write endpoint, and what it needed.** The API had only ever
  answered reads. A write endpoint on an unauthenticated local port can be
  reached by any web page in the user's browser, so a write must be JSON by
  content type, must not come from another origin, is at most 1 MiB, and must
  arrive within a deadline. No cross-origin permission is ever granted.

Not built: deleting traces or spans, annotations arriving over OTLP, and a
summary of scores over time. The last waits for SQL.

How it was tested: the encodings and the part file against truncation at
every length and every single flipped bit (no flipped bit yields a wrong
record); random walks of annotate, overwrite, delete, flush, restart, merging
and expiry against a model; the process aborted at every stage of a flush, of
an annotation merge and of a retention step, with every acknowledged
annotation required exactly once afterwards; hand-written HTTP for the write
rules; and the end-to-end run, which now annotates, overwrites, deletes and
tries three cross-site writes, and checks the result from memory, after
`kill -9` and after a clean stop.

Three independent examinations found fourteen defects, each proven with a
failing test before it was fixed:

- **A web page could read traces, and now write annotations, in the container
  set-up.** The check of the `Host` header, the defence against DNS rebinding
  since checkpoint 3, switched itself off when the server listened on all
  interfaces, which the container image does. It is now always on: an address
  or `localhost` is accepted, any other name only when it is listed.
- An overwritten or deleted annotation could come back after retention when
  the window width had been changed between runs, and, through a rare race
  between a flush and retention, when the clock stepped back.
- One request for a trace list could hold a gigabyte, and a list with a
  filter took time that grew with the square of the annotations in memory.
- A filter that matched more than a million traces of one window failed
  instead of answering.
- A request written as a JSON array instead of an object was accepted; the
  reply to a malformed request repeated the request, three times larger.
- A planted annotation file made the reader allocate as much as the file was
  long before checking its length.
- A merge whose result could not be opened marked its window as damaged until
  the next restart.

A second, independent pass over the fixes found four of the fourteen only
partly closed, and new problems the fixes had brought. The check that was
meant to keep an annotation out of an expired window compared windows where
it should have compared traces, so it could still misplace an annotation and
could drop one whose trace was live; a flush now starts over when retention
removed parts while it was deciding. The trace list, made to stream so that it
would not hold a gigabyte, held the whole database snapshot for as long as a
slow client read it; it now reads its page, lets the snapshot go, and then
replies. The always-on `Host` check had also closed the health endpoint to
load balancers that address it by name; `/healthz`, which says only `ok`, is
exempt again. These are fixed with tests; each fix that changes behaviour was
then removed once more to see its test fail.

The benchmark found one thing the tests had not: listing the newest fifty
traces had become a third slower on a database with no annotations at all,
because each listed trace was looked up in every part before finding there was
nothing to read. Nothing is looked up now when there is nothing.

A pass over the whole code base for duplicated and unused code ran beside the
checkpoint: about a thousand lines went, among them nine copies of a counting
allocator and ten of a varint encoder, with no change in behaviour.

Measured with the benchmark harness (2 CPUs, 4 GB; full results in
[benchmarks](benchmarks/README.md)): on ten million spans in 545,163 traces,
60,000 annotations are written at 39,900 a second, each request answered
after its log sync, and take 29 bytes each on disk. Opening an annotated trace
takes 0.7 ms at the median. Listing the newest 50 traces with a score takes
1.8 ms when one trace in ten has one, 3.8 ms for a label one in a hundred
has, and 236 ms for a value one in ten thousand has, which has to look at
every trace in the range. The queries of the earlier checkpoints take what
they took. One did not: a phrase search on the
text-heavy data went from 1.50 ms to 2.44 ms. Old code on the same machine
and the benchmark at five commits in between showed that the engine's
annotation commit had made it slower without touching the search code; a
profile put the time in the decompression of the same text blocks. Reusing
the decompression context and buffers across a read brought it to 1.73 ms.
The last 0.2 ms is not explained, and the benchmark page says what was ruled
out.

Known limits at this checkpoint:

- An annotation is removed with the time window it was stored in: the window
  that ended last among those holding its trace when it was written to disk.
- An annotation cannot be written before its trace has arrived.
- Reading a trace's annotations costs time in proportion to the records stored
  for that trace, deleted ones included. There is no limit on annotations per
  trace.
- A span id in an annotation is not checked against the trace's spans.
- Every flush that carries annotations writes one small part per window it
  touches; scoring old traces across many windows writes many parts before
  merging catches up.
- No authentication. The write endpoint lets a local client attach
  annotations to existing traces; it cannot remove trace data.
- A phrase search is 15% slower than at checkpoint 6 for a reason that is
  only partly found (see the benchmark page).
- The fixes of the second pass had removal checks but no further independent
  examination.
