# nwdb-segment

Writes and reads one immutable **part**: the spans of the traces that started in
one time window, stored so that opening a trace is one index lookup and one
contiguous read.

- `PartWriter` builds a part in `<parts dir>/<id>.tmp` and renames it to
  `<parts dir>/<id>` once every file is synced.
- `PartReader` opens a finished part and keeps its files open. Reads are
  positioned reads (`read_at`) that never use a shared file offset, so one
  reader can be shared by many threads, and on Unix it keeps reading after the
  part directory has been deleted (by compaction or retention).
- Large text is not stored in the span rows. The raw columns `attrs`, `events`
  and `links` hold *ropes* that refer to a piece store (`pieces.blk`,
  `pieces.idx`, see `nwdb-payload`) in which each distinct piece of text is
  stored once per part. Reads rebuild the original bytes, so what comes back is
  exactly what was written.
- `merge_parts` writes one part from several, without rebuilding their text
  (see [Merging](#merging)).
- Every part carries a full-text index of its pieces and, for each piece, the
  rows that refer to it; `PartReader::search` answers a text query with the
  rows that match (see [Text search](#text-search)).

## Files of a part

All integers are little-endian. A part directory holds exactly these seven
files.

### `spans.parquet`

One row per span with exactly `nwdb_core::span_schema()`, in the order the
writer was given them. The columns `attrs`, `events` and `links` hold ropes
(see below) instead of the raw values. The spans of a trace are adjacent (the writer rejects
input that interleaves traces), so a trace is a contiguous row range and
repeated text between its spans compresses well.

Parquet settings:

| Setting | Value | Why |
|---|---|---|
| Compression | zstd level 3, all columns | |
| Statistics | page level (so also the column index), except off for `resource`, `scope`, `attrs`, `events` and `links` | row group pruning by `start_ns`; nothing prunes by the raw binary columns, so their statistics are wasted work |
| Dictionary encoding | on, except off for `attrs`, `events` and `links` | `resource` and `scope` repeat heavily across spans; the other three are close to unique per row |
| Offset index | on, for every column (also those without statistics) | lets a row selection skip pages; a test pins that `read_trace` does not fetch other traces' pages |
| Bloom filters | off | the trace index replaces them |
| Row group size | 16,384 rows or 64 MiB of stored value bytes, whichever first | rows range from 1 KB to several MB, so a row bound alone is unsafe; the row that crosses the byte bound stays in the group; the rope of a raw value, not the value, is what is counted |
| Data page size | 1 MiB | |
| Data page rows | 4,096 | a trace lookup should not decode a page of tens of thousands of unrelated small rows |
| Dictionary page | 1 MiB | |
| Write batch | 256 values | the Parquet writer checks page limits between batches of this many values |

Value bytes are the lengths of all string and binary values of a row, the
measure `nwdb_core::BatchLimits` uses; for the writer's row group bound the
raw columns count as stored, that is as ropes. The seven origin columns (`resource`,
`service_name`, `resource_schema_url`, `scope`, `scope_name`, `scope_version`,
`scope_schema_url`) are Arrow dictionary columns, `Dictionary(Int32, Utf8)` or
`Dictionary(Int32, LargeBinary)`. For the row group bound a row counts its
4-byte key in each of them, and the values of a batch's dictionaries are
counted once, on the first row of the batch.

### Ropes and the piece store

A raw value is stored as a *rope*: a sequence of segments, each inline bytes or
a reference to a piece of the store. The encoding is documented in
`nwdb-payload`: an empty value is zero bytes, a value with no cuts is `0x00`
and the value, and a value with cuts is `0x03`, the CRC-32C of the value
(4 bytes), the length of the value (a varint), then `varint(len << 1)` and inline bytes, or `varint(id << 1 | 1)`
for piece `id`. A null `events` or `links` stays null. The cuts are made in the
value's original bytes and nothing is re-encoded, so reading concatenates the
segments and gets the original back byte for byte. The checksum is there because
Parquet pages carry none and a read does not run `verify`: one flipped bit in a
reference would otherwise return a piece of another span, perhaps another
sender's text. The reader checks every rebuilt value that has references against
its checksum and returns `Corrupt` instead of the bytes on a mismatch; a value
with none is its own bytes and costs nothing extra. The length is there so
that a damaged run length cannot make a read allocate what the whole store
holds (see Reading).

Which bytes become pieces is decided by the `PayloadSplitter` in
`WriterOptions::splitter` (`nwdb_otlp::OtlpSplitter` for OTLP); with none every
value is stored inline, as a rope with no references. The writer checks the
splitter's ranges (ascending, not overlapping, not empty, inside the value, no
piece over 1 GiB) and stores a value whole if they break the contract. Two
more rules keep the number of pieces, which a sender decides, from deciding the
memory of the writer and of every open part:

- A cut shorter than `WriterOptions::min_piece_bytes` (default 8) is stored
  inline, whatever the splitter says: a reference and a table entry cost more
  than such a piece.
- Once the part holds `WriterOptions::max_pieces` pieces (default 16,777,216),
  further new text is stored inline; pieces that exist can still be referenced.
  Sharing is lost beyond the ceiling, nothing else.

`WriterOptions::piece_observer` is told about each piece the first time it is
stored (id and bytes, in the order of the ids, before the batch is written), for
something built on the pieces such as an index of their text. A piece that was
dropped from the window and is stored again is a new piece and is announced
again; text stored inline is not announced.

`pieces.blk` and `pieces.idx` are written by `nwdb_payload::PieceWriter` and
described in that crate's README: blocks of about 1 MiB of uncompressed pieces,
each compressed with zstd level 3, a block table and a piece table with its own
magic, version and checksum, and exact de-duplication within a window of recent
blocks. Both files exist in every part; a part with no pieces has an empty
`pieces.blk` and a header-only `pieces.idx`.

### `text.idx` and `text.rows`

The text of a part is searched through two files, both written by the part
writer while it writes the part, so that a flush and a merge produce them in
the same way (there is one implementation, in `src/text.rs`, fed from the one
path every piece and every rope of a part passes).

- `text.idx` is an `nwdb_textindex` index (format described in that crate's
  README) with **one document per piece** of the part's piece store, the
  document id being the piece id. A message that a thousand calls repeat is one
  piece and is tokenised once. Each new piece is added to an `IndexBuilder`
  when the piece is first stored (the same moment `WriterOptions::piece_observer`
  hears of it, in the order of the ids). The builder closes a segment when its
  open postings pass `WriterOptions::text_index_budget_bytes` (default 32 MiB,
  at least 1 MiB), so its memory does not grow with the part.
- `text.rows` is an `nwdb_textindex` id-list file with one key per piece id,
  dense from 0 to the number of pieces minus one: the key's list is the
  **ascending numbers of the rows** whose `attrs`, `events` or `links` refer to
  the piece (a row that refers to a piece twice, or from two columns, lists it
  once; a piece that no row refers to has an empty list). Row numbers are
  `u32`: a part of more than 4,294,967,295 rows is refused by the writer.

The `(piece, row)` pairs are collected from the ropes of every batch as it is
written. They are kept in memory up to `WriterOptions::row_list_budget_bytes`
(default 32 MiB, 8 bytes a pair), then sorted and written to a run file,
`text.rows.runN`, in the part's temporary directory; when the part is finished
the runs and the pairs still in memory are merged into `text.rows` and the run
files are deleted before the directory is renamed. The result does not depend
on the budget (a test builds a part with a 16 byte budget and a part with the
default and compares the two files byte for byte). Memory while merging is one
read buffer for each run (the budget divided between the runs, 4 to 64 KiB
each) and the list of the piece being written, which is as long as the number of
rows that refer to it (4 bytes a row: a system prompt shared by ten million
rows is a 40 MB list).

**What is not indexed, by design.** Only text that is in a piece is searchable.
Text that stays inline in a row is not: a cut shorter than
`WriterOptions::min_piece_bytes` (values under 8 bytes), a value the splitter
does not cut (for the OTLP splitter: attribute values under 256 bytes that are
not under a content key, and everything in a column the splitter does not
read), and text stored after the part held `WriterOptions::max_pieces` pieces
(the piece ceiling of a part; pieces that exist can still be referenced).
Keys, names, ids and the promoted columns (`name`, `service_name`, models and
the rest) are not text of pieces either; they are columns of the span row.

### Dictionary columns in Parquet

What the Parquet library (60.2.0) does with them. `tests/dictionaries.rs`
pins that parts written from batches with different dictionaries read back to
the same values and exactly the span schema; the rest was read from the
library and measured:

- Writing: `ArrowWriter` takes the dictionary arrays as they are and embeds
  the Arrow schema in the file. Two batches of one part may number the same
  value differently, or hold values the other lacks; each is written by value,
  so the file does not depend on the numbering. Within one dictionary array
  each distinct value is hashed once, not once per row (this holds when the
  array has at most as many values as the rows written together, and for every
  later write of the same array); a batch of rows sliced off a larger
  dictionary still hashes per row when the slice is shorter than the
  dictionary.
- Reading: because the Arrow schema is embedded, the reader gives back
  `Dictionary(Int32, Utf8)` and `Dictionary(Int32, LargeBinary)` columns, and
  every batch has exactly `span_schema()` (or the projection of it), so no
  conversion is done. Keys are numbered per batch and mean nothing outside it;
  use `SpanColumns`, which resolves them.
- The Parquet reader gives a batch the dictionary of the page being decoded
  (up to 4,096 rows), not only the values of the rows returned: at most about
  1 MiB per column, since the writer's dictionary page limit is 1 MiB. The
  point reads (`read_trace`, `read_rows`) therefore compact every batch with
  Arrow's dictionary garbage collection (`arrow-select`), for any projection:
  each dictionary column of a returned batch holds only the values its rows
  use. A trace that spans parts or row groups holds at most those values once
  per batch. `scan` reads whole row groups and keeps whole dictionaries, as
  compacting them would cost a pass over data the caller reads anyway.

The writer hands rows to Parquet in pieces that end exactly where a row group
ends, and closes the group itself. What the Parquet writer buffers is therefore
one row group of at most 64 MiB of values plus one row. Within a column, the
Parquet writer in the pinned version splits a batch of large values so that a
page holds one value past the page limit rather than a whole write batch of
them; a test pins this for 1 MiB values.

### `traces.idx`

```
offset  size   field
0       4      magic "NWTI"
4       4      version, u32 = 6, the part format version
8       8      trace count N, u64
16      8      length of spans.parquet in bytes, u64
24      4      CRC-32C of every byte of spans.parquet, u32
28      8      length of summary.parquet in bytes, u64
36      4      CRC-32C of every byte of summary.parquet, u32
40      8      length of pieces.blk in bytes, u64
48      4      CRC-32C of every byte of pieces.blk, u32
52      8      length of pieces.idx in bytes, u64
60      4      CRC-32C of every byte of pieces.idx but its last four, u32
64      8      length of text.idx in bytes, u64
72      4      CRC-32C of every byte of text.idx, u32
76      8      length of text.rows in bytes, u64
84      4      CRC-32C of every byte of text.rows, u32
88      44*N   records, sorted by trace_id (bytes compared in order)
88+44N  4      CRC-32C (Castagnoli) of every byte before it
```

The version is the first thing checked when a part is opened, before the
lengths, the checksum or the Parquet schema. An index with any other version
is refused with an error that gives the version the part has and the one this
build reads, and says that parts from development versions cannot be read.
Versions 2 and 3 had shorter headers, version 4 ropes have no checksum, and
version 5 ropes have no length and version 5 parts no text files; there is no
migration, since the project has had no release.

Parquet 60.2.0 writes no page checksums and zstd's frame checksum is off, so
damage that leaves the file structure intact would otherwise be returned as
data. The writer therefore keeps a running length and CRC-32C of everything
it writes to each Parquet file, to the two piece files and to the two text
files and records them here, once the files are complete (the piece store, the
text files and the summary are written before the index). `PartReader::open`
checks the lengths, and the CRC-32C of `pieces.idx`, which it reads in full
anyway and which holds the checksum of every block; `PartReader::verify` checks
the other CRCs, every block of the piece store, both text files and every rope,
and re-checks everything `open` checks (below).

The checksum recorded for `pieces.idx` leaves out the file's last four bytes.
They are the file's own CRC-32C, and the CRC-32C of a message with its CRC-32C
appended is the same constant (0x48674bc7) for every message, so a checksum of
the whole file would be the same for every part and bind nothing; over the rest
it is that trailing checksum, which depends on every block's checksum. The
other recorded CRCs are over every byte of their file. Since nothing re-reads
those four bytes after open, `verify` reads them again and compares them with
the CRC-32C of the rest of the file.

A record:

```
offset  size  field
0       16    trace_id
16      8     first_row, u64: row of the trace's first span in spans.parquet
24      4     row_count, u32
28      8     start_ns, i64: earliest span start in the trace
36      8     end_ns, i64: latest span end in the trace
```

The records' row ranges tile `0..rows` of `spans.parquet` exactly. A reader
loads the whole file, checks all of the above, and binary-searches the records.

### `summary.parquet`

One row per trace with `nwdb_core::summary_schema()`, sorted by
`(start_ns, trace_id)`, with the same compression, statistics and page index
settings, and row groups of 16,384 rows. Sorting by start time makes the
`start_ns` statistics of the row groups nearly disjoint, so a time-range
listing reads only the groups that overlap the range.

## Reading

`PartReader::open` fails with `Error::Corrupt`, naming the file, when a file is
missing, `traces.idx` fails a check, the length of a file (any of the seven)
differs from the one recorded in the index, `text.idx` or `text.rows` fails the
open checks of its own format (footer, directory, group index) or does not
cover exactly the part's pieces (a different number of documents or of lists
than `pieces.idx` has pieces), `pieces.idx` fails its own checks (magic, version,
checksum, tables that do not fill the blocks), a Parquet file does not have the
expected columns, the index's ranges do not match the number of rows, or the
summary's row count differs from the trace count. The two piece files are
compared with the record before anything is read: their lengths first (a file
extended to any size is not read), then, once `pieces.idx` has been read in
full (it is read anyway), its CRC-32C. `pieces.idx` holds the checksum of every
block, so this binds the blocks to the part: the piece files of another part are
refused at open, naming `pieces.idx`, because its checksum differs, whatever
the lengths. A `pieces.blk` of the same length alone, with this part's
`pieces.idx`, passes open: its blocks are caught by their checksums when they
are read and by `verify`. Open does not read the data pages or the blocks of
the piece store.

- **A part that passes `verify` opens.** `verify` checks again, against the
  files as they are now, everything `open` checks: it reads `traces.idx` again
  and requires it to decode to what was decoded at open (through the handle kept
  from open, so it works after the directory has been removed, like the other
  files), and it
  checks the trailing checksum of `pieces.idx` against the rest of that file.
  `merge_parts`'s input check (without the walk over the ropes) does the same.
  A test flips a bit at every offset of every file of a small part, and at a
  sample of offsets of a larger one, and requires that either `verify` on the
  open reader fails or a fresh `open` succeeds and its own `verify` passes.
- `PartReader::verify` re-reads all six data files and compares their length
  and CRC-32C with the recorded values (`Corrupt` naming the file on a
  mismatch), reads every block of the piece store, checks every segment of
  `text.idx` (the CRC-32C of `text.rows` covers its lists), and
  then walks every rope of
  every row against the store: each must be well formed, name pieces that exist,
  rebuild to the checksum it carries, and together the ropes must refer to
  exactly the bytes of pieces that `pieces.idx` records. It is the read path
  itself, run over the whole part, so a part that passes can be read. It is
  meant for compaction and a future scrubber and is not called on open; it costs
  a sequential read and a rebuild of every value.
  `spans_checksum()` and `summary_checksum()` return the recorded values.
  Without `verify`, a damaged Parquet data page can still be returned as data
  (damage in a row's own bytes changes only that row); a damaged reference
  cannot return another span's text, since a value that has references is
  checked against its checksum after it is rebuilt, and a damaged piece block
  cannot, since a block's checksum is checked every time it is decompressed.
- A read whose projection includes none of `attrs`, `events` and `links` does
  not touch the piece files at all (a test pins this: `payload_blocks_loaded()`
  stays zero, and damaged blocks are not noticed). A read that includes one
  rebuilds the original bytes of all the raw values it returns.
- A returned batch is rebuilt by block, not by reference. The reader collects
  every reference of the rows of the batch (all three raw columns), puts them in
  the order of the pieces, which is the order of the blocks, decompresses each
  block the batch needs once, and copies its pieces to their places in the
  output. The cost of a batch is the blocks it touches, however many references
  there are and in whatever order: a trace whose 1,000 references go round 40
  blocks decompresses 40 (before, a session that forgot everything past 32 MiB
  decompressed 1,000). Memory beyond the output itself is one decompressed block
  and 16 bytes for each reference of the batch (at most one for every 8 bytes of
  output for an honest part, since a piece is at least 8 bytes by default).
- A rope never decides how much is allocated. A rope with references records
  the length of the value it rebuilds to; before anything is allocated the
  reader adds up the segments and refuses (`Corrupt`) a value that does not add
  up to it, stopping at the first segment that goes past it, so one flipped bit
  in an inline run's length, which can turn text into hundreds of references to
  a large piece, costs a few steps and no allocation. A value whose references add up to more bytes than
  `pieces.idx` records for the whole part, and a batch whose references together
  do (no honest value or batch can exceed the total of all references of its
  part), and a total that cannot be represented; the output is then reserved
  with a fallible allocation, so a size that is absurd but consistent with the
  store is an error and not an abort. The checks name `spans.parquet`.
- Returned batches can exceed `BatchLimits` by the dictionaries of the origin
  columns: the dictionary values come with the batch, at most about 1 MiB for
  each of the seven origin columns (the size of a Parquet dictionary page), and
  the batches sliced off one Parquet batch share the same dictionary buffers (a
  `read_trace` or `read_rows` compacts each batch to the values its rows use;
  `scan` keeps whole dictionaries). The limits count the values of the rows.
- Because a rebuilt value can be thousands of times larger than its rope, the
  reader measures every batch it reads from Parquet in rebuilt bytes (from the
  piece lengths in the table, without reading any piece) and divides it so that
  every returned batch respects `BatchLimits::DEFAULT` in rebuilt value bytes.
  One row larger than the limit is a batch of its own. It then rebuilds one
  returned batch at a time. This holds for `read_trace`, `read_rows` and
  `scan`.
- Every call into the Parquet library, at open and on every read including
  each `Scan::next`, runs inside `std::panic::catch_unwind`. A panic (the
  library asserts and unwraps on damaged files) becomes `Error::Corrupt`
  naming the file, and a `Scan` that hit one ends. This relies on the build
  not using `panic = "abort"`. The reader holds no mutable state, so a caught
  panic leaves nothing half-updated.
- `read_rows_stored(first_row, row_count, projection)` returns the same rows with
  `attrs`, `events` and `links` as they are stored, ropes, and reads no piece;
  `nwdb_payload::rope::piece_ids(rope)` lists the pieces a rope refers to, and
  `piece_store()` gives the `PieceReader` that fetches a piece by id
  (`read_piece`, or `read_sorted` for many at once). This is the way to build
  something on the pieces, such as an index of their text, together with
  `WriterOptions::piece_observer` at write time. Ropes returned this way are not
  checked; a read that rebuilds them is.
- `read_trace` and `read_rows` read a row range with a Parquet row selection,
  so pages holding none of the rows are not fetched, and return batches whose
  dictionary columns hold only the values their rows use.
- Reads go one row group at a time, so a returned batch never spans row
  groups. A batch is also bounded by bytes: its row count for a group is
  `BatchLimits::DEFAULT.max_bytes` (32 MiB) divided by the group's average
  uncompressed row size, at least 1 and at most 8,192. A group with a few huge
  rows among many small ones can still exceed this, as the bound uses the
  average. This applies to `read_trace`, `read_rows`, `scan` and
  `scan_summary`.
- `scan` and `scan_summary` prune row groups by the `start_ns` statistics
  (found by Parquet column name, not by position in the Arrow schema) and do
  not filter rows; the caller filters. `scan` is lazy: it builds one group's
  reader at a time, so file errors surface from the iterator. `Scan` reports
  how many row groups were read and skipped.
- `scan_summary_rev` is the lazy, newest-first form of `scan_summary`: it
  yields one row group per call, starting with the group that holds the latest
  starts (the file is sorted by start, so the groups' order is the file's order
  reversed), and `next_upper_bound` gives the largest start the next group can
  hold, from the statistics, without reading it. A caller merging several
  parts uses it to leave a part unread until it can matter. The rows inside a
  group are still in ascending order, so a caller wanting descending order
  walks them backwards.
- A projection returns the requested columns in the requested order; an
  unknown or repeated column is `Error::InvalidArgument`.

## Text search

```rust
let rows = reader.search(&nwdb_textindex::parse("error \"rate limit\" -retry*")?,
                         &SearchLimits::default())?;
```

`PartReader::search(&Query, &SearchLimits) -> Result<Vec<u32>>` returns the
ascending rows whose text matches. A row's text is the union of the pieces its
raw columns refer to:

- A word (or a prefix) matches a row if **any** piece of the row has it.
- A phrase must lie **inside one piece**.
- `AND`, `OR` and `NOT` combine **sets of rows**, not of pieces: the two words
  of an `AND` may be in different messages of one span, and `-x` is every row
  of the part without a piece that has `x` (rows with no pieces included).

For each leaf of the query the index gives candidate pieces. Where the library
says the index alone cannot decide (a phrase; a word of more than 64 bytes),
each candidate piece is read once, in the order of its blocks
(`PieceReader::read_sorted`), and checked with `phrase_matches` /
`contains_token`. The confirmed pieces become rows through `text.rows` and are
united. Then `nwdb_textindex::evaluate` combines the leaves. The set of all
rows is built only when the query contains a `NOT`.

One query cannot hold a reader hostage. `SearchLimits` bounds, and a query over
a bound is `Error::InvalidArgument` saying which (the reader is unharmed):

| Limit | Default | What it bounds |
|---|---|---|
| `max_prefix_terms` | 1,024 | the terms a prefix may expand to |
| `max_confirm_bytes` | 256 MiB | the bytes of piece text read to confirm phrases and long words, over the whole query (a piece counts once per leaf that reads it) |
| `max_leaf_rows` | 10 million | the rows one leaf may produce; for a query with a `NOT`, also the rows the part may have, since the universe is built |

The rows of a leaf are collected and sorted when they pass twice the limit, so
memory is about twice the limit in row numbers (80 MB at the default) however
many pieces match. A test pins the memory of a query over 300,000 matching
pieces with a limit of 100,000 rows, in a child process (4 MiB of growth).

A reader is shared by threads and holds no mutable state for a search. A damaged
`text.idx` or `text.rows` is `Corrupt` naming the file: the segments of the index
are checksummed the first time they are used, each group of `text.rows` every time
it is read, and a row number outside the part is refused. A search never returns
a wrong answer because of damage: a test damages every byte of the ends of both
files and a sample of the rest (truncation, extension, flipped bits) and
requires `Corrupt` from `open`, `verify` or the search, and the right rows
from any search that succeeds. Text files of another part are caught by their
recorded lengths and, at open, by their counts of documents and lists; with equal
counts only `verify` catches them (its whole-file CRC-32C).

`PartReader::text_stats()` reports the sizes of the two files and what they
cover. `PartReader::max_start_ns()` is the largest maximum in the statistics of
the `start_ns` column of the row groups, an upper bound for the walk of
`nwdb-engine`'s search over parts.

## Writing

Traces must arrive contiguously, though a trace may continue across batches,
and a trace that has ended must not come back (`Error::InvalidArgument`; the
rejected batch is not written and the writer stays usable). `write` takes
batches of raw values; for each row it asks the splitter for cuts, stores the
pieces and writes the ropes. Memory is one row group of ropes (bounded in
stored bytes), the de-duplication window (`WriterOptions::dedup_window_bytes`,
default 64 MiB) with the open piece block and the map over its pieces, the
length of every distinct piece in the compact form it has on disk (one to three
bytes each, at most `max_pieces` of them: 16 to 32 MB at the default ceiling,
for pieces under 16 KiB), plus 44 bytes of index entry, a trace summary, and a
16-byte finished-id entry per trace. It does not grow with the amount of unique
text: a test writes 120 MiB of text that never repeats through a 4 MiB window
and shows the peak resident memory stays within a few MiB of what 24 MiB of such
text took (with the text index and the row lists at 1 MiB; at their defaults
each adds up to its budget, 32 MiB, to the peak of a part being written), and
another that 3.5 million pieces of 12 bytes raise it by 6 MiB over
what 0.7 million took (27 and 33 MiB; before, with 16 bytes a piece for the life
of the part, 37 and 101 MiB). A part with no rows is refused, `window_ns` must be positive
(`InvalidArgument`), and dropping an unfinished writer removes its temporary
directory.

`finish` syncs each file, the temporary directory, renames it, and syncs the
parts directory. When `create` has to create the parts directory (and any
missing parents), it also syncs the parent of each directory it created, since
a new directory is not durable until its parent is synced.

Failure rules for the caller:

- After any error from `write` or `finish` other than `Error::Fatal`, the
  writer is unusable or consumed and its temporary directory is removed;
  use a new part id for the next attempt.
- If anything fails after the rename (the sync of the parts directory),
  `finish` returns `Error::Fatal`: the part may or may not be durable. Do not
  reuse the id; restart.

## Merging

```rust
let (meta, stats) = merge_parts(&[&oldest, &newer], parts_dir, part_id,
                                window_start_ns, window_ns, MergeOptions::default())?;
```

`merge_parts(inputs, parts_dir, part_id, window_start_ns, window_ns, options)`
takes open readers, oldest first, and writes the part a flush would have written
had it received all the rows of its inputs at once. It is a function and not a
type: a merge has no state a caller could use between its start and its end, and
the engine needs one call that either returns a finished part or leaves nothing.
It returns the `PartMeta` (with `level` from `MergeOptions::level`, default 1)
and `MergeStats`.

What the output is:

- The spans of a trace are adjacent, ordered by `(start_ns, span_id)`. Traces
  are ordered by `(the earliest start of the trace in any input, trace_id)`. A
  trace with rows in several inputs is one run. The trace index and the summary
  are built by the writer a flush uses; the summary of a trace is computed from
  its merged rows.
- A `(trace_id, span_id)` that occurs more than once is kept once: the row of
  the earliest input, and within one input its first row. The others are
  `MergeStats::duplicates_dropped`. A row that differs in content but shares
  the ids of an earlier one loses to it.
- Every value of every kept row reads back byte for byte as the input returns
  it, including nulls, origins and ropes.
- The ordering of traces looks at the earliest start of any row of the trace in
  any input, also of a row that is dropped as a duplicate; the index entry and
  the summary describe the kept rows. A flush orders by the kept rows. The two
  differ only when a dropped duplicate started before every kept row of its
  trace, which a sender's retry (the same span again) never does, and the
  merge cannot know it without a pass over every row before it writes the first
  trace.

How it works:

- **Text is not rebuilt.** Rows are read as stored (`read_rows_stored`). A rope
  keeps its checksum (the CRC-32C of the value, which does not change) and has
  its piece references translated from the numbering of the input's store to the
  output's (`nwdb_payload::rope::translate`). A piece is read from an input store
  once per input, in the order of its blocks, and added to the output's store
  through the same path a flush uses, so text that two inputs hold is stored
  once and `unique_bytes` of the output is that of a flush of the same rows. The
  references are counted as the ropes are written, so `verify` finds the part
  consistent. Because the checksum is carried and not recomputed, a wrong
  translation is not trusted: a reader finds that the value does not rebuild
  to its checksum and returns `Corrupt` (`tests/merge.rs` swaps the pieces of a
  merged part and shows it).
- If the output cannot take a piece (`max_pieces` was reached and no equal piece
  exists), the piece's bytes go into the rope of every row that uses it, as
  inline bytes; the value is unchanged. Such a piece is read from its input
  again by each unit of work that needs it (see below), the one case where a
  piece is read more than once per input; the bytes of these pieces are held for
  the unit and are not counted in its size.
- The same writer. Rows go through the part writer's row groups, dictionary
  columns (`SpanBatchBuilder::append_from_with_raw`, the dictionaries of
  batches are merged by value), summary and index and file checksums. The
  writer takes batches whose raw columns already are ropes
  (`PartWriter::write_stored`, crate private); everything else is the code of a
  flush, and a flush's output did not change. The format version does not
  depend on how a part was made: a merged part is an ordinary part, its text
  index and row lists are built by the writer from the pieces and ropes it is
  given exactly as on a flush (a test merges parts and requires every search to
  give the rows, by trace id and span id, that the same search gives over the
  inputs), and `PieceObserver` hears of the output's pieces exactly as on a
  flush.
- **Chunks.** The distinct traces of all inputs are ordered first (24 bytes
  each; the inputs' indexes are sorted by trace id, so this is a merge of
  sorted lists). Then traces are taken in output order into a chunk until the
  estimated stored size of their rows reaches `MergeOptions::chunk_bytes`
  (default 32 MiB), or 524,288 rows; one trace is never split between chunks
  unless a chunk is only that trace. The size of a row is estimated from the
  average of its row group, so a group that mixes tiny and huge rows can make a
  chunk larger than the bound. For a chunk: (1) the keys (`span_id`, `start_ns`)
  of its rows are read, 32 bytes a row, each input's rows in coalesced runs;
  (2) duplicates are dropped and the rows are ordered as a flush orders them;
  (3) the rows are cut into units of about `chunk_bytes` (this is what bounds a
  trace larger than a chunk: it is processed in several units, in order) and for
  each unit the rows are read in full, again in runs (a gap of up to 32 rows is
  read through rather than skipped), their pieces are carried over and the rows
  are appended to the writer. The inputs are nearly in output order already
  (each is sorted by its own traces' starts), so the reading is close to
  sequential, with a few extra runs for traces that are split.

Durability and failure are those of any part: the part is built in
`<id>.tmp`, every file is synced, the directory is renamed and the parts
directory synced; any error removes the temporary directory (a merge that
returns `Fatal` after the rename is the writer's case, and the id must not be
reused). The inputs are never modified; a merge only reads them, with
positioned reads, so other threads keep reading them (a test runs readers
against merges).

Damage: unless `MergeOptions::verify_inputs` is false, every input is first
checked in full: the length and CRC-32C of its four data files against its
index and every block of its piece store (`PartReader::verify` without the walk
over the ropes, which the checksum of `spans.parquet` makes redundant); the
cost is a sequential read of each input. Then everything the merge reads is
checked as it is read, with the reader's own checks. Any damage is
`Error::Corrupt` naming `merge input N` and the directory; no output is
produced and nothing is left behind. A damaged part cannot panic the merge.
`traces.idx` is read when a reader is opened, so damage to it is refused there.
An input of another format version is refused there too (`open` checks the
version first); a merge is only given open readers.

Memory, besides the chunk: 24 bytes per distinct trace for the order, 4 bytes
per piece of every input for the translation table, 32 bytes per row of the
chunk for its keys (at most 16 MiB unless one trace has more rows than that), the
output batch (the chunk size, between 1 and 16 MiB), and what the writer
itself holds, which is what a flush holds: a row group, the de-duplication
window, one entry per trace written (index entry, summary, finished id) and one
or two bytes per piece. Nothing depends on the size of the inputs.
`tests/merge_memory.rs` merges eight inputs of 90 MiB in all (96 KiB of text left in
the row and 196 KiB cut into pieces per row, 40 rows per part) with a chunk and a
window of 4 MiB in a child process, once as eight traces stitched from all inputs
and once as a single trace, and compares the peak resident memory (`VmHWM`) with
that at the start of the merge. The peak rose by 31 MiB when merging the first four
inputs (45 MiB) and by 33 MiB when merging all eight, for eight traces; by 33 MiB for
both with a single trace of 320 spans. That is a few times the chunk, and it does not
grow with the inputs.

Cost: a merge reads each input once for the keys (two small columns), once for
the rows, once for the pieces it needs, and once more for the up-front check; it
writes the output once. In a debug build, which is what the tests run, it
merged about 16,000 rows per second (spans without text, 40 parts into one, with
the check of the inputs). The
size of what it makes is that of a flush of all the rows at once: 40 parts of 48
spans (1,710,200 bytes in all, 120 row groups) became one part of 97,933 bytes
(5.7%) with one row group; eight parts of agent-shaped text (the same system
prompt and tools in each part, conversations that grow across them) became
13.7% of their size, and their 765,952 unique bytes of text 160,256.

## Format version

`FORMAT_VERSION` (6) is recorded in `PartMeta.format_version`, which the
manifest stores for every part, and in the header of `traces.idx`. The engine
checks the manifest's value and `PartReader::open` the index's, both before
looking at any Parquet file. It covers the columns of `spans.parquet` and
`summary.parquet`, their types and order, the meaning of the raw encodings
inside them (`attrs`, `events`, `links`, `resource`, `scope`), and the layout of
`traces.idx`, and the formats of `text.idx` and `text.rows`. It must be raised
on any change to those. A part with another
version is refused; there is no migration. History: 2 added the file lengths
and checksums to the index; 3 made the seven origin columns dictionary
columns; 4 stores `attrs`, `events` and `links` as ropes, adds `pieces.blk` and
`pieces.idx`, and records their lengths and checksums in the index header; 5
adds the CRC-32C of the rebuilt value to every rope that has a reference (the
rope tag `0x03` is followed by 4 bytes of checksum), and makes `open` compare the
CRC-32C of `pieces.idx` with the one in the index; 6 adds the length of the
rebuilt value to every rope that has a reference, after its checksum, and adds
the text index and the piece-to-rows lists, `text.idx` and `text.rows`, and
records their lengths and checksums in the index header (the header grows from
64 to 88 bytes).
