# nwdb-segment

Writes and reads one immutable **part**: the spans of the traces that started in
one time window, stored so that opening a trace is one index lookup and one
contiguous read.

- `PartWriter` builds a part in `<parts dir>/<id>.tmp` and renames it to
  `<parts dir>/<id>` once every file is synced.
- `PartReader` opens a finished part and keeps both Parquet files open. Reads
  are positioned reads (`read_at`) that never use a shared file offset, so one
  reader can be shared by many threads, and on Unix it keeps reading after the
  part directory has been deleted (by compaction or retention).

## Files of a part

All integers are little-endian. A part directory holds exactly these three
files.

### `spans.parquet`

One row per span with exactly `nwdb_core::span_schema()`, in the order the
writer was given them. The spans of a trace are adjacent (the writer rejects
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
| Row group size | 16,384 rows or 64 MiB of value bytes, whichever first | rows range from 1 KB to several MB, so a row bound alone is unsafe; the row that crosses the byte bound stays in the group |
| Data page size | 1 MiB | |
| Data page rows | 4,096 | a trace lookup should not decode a page of tens of thousands of unrelated small rows |
| Dictionary page | 1 MiB | |
| Write batch | 256 values | the Parquet writer checks page limits between batches of this many values |

Value bytes are the lengths of all string and binary values of a row, the
measure `nwdb_core::BatchLimits` uses. The seven origin columns (`resource`,
`service_name`, `resource_schema_url`, `scope`, `scope_name`, `scope_version`,
`scope_schema_url`) are Arrow dictionary columns, `Dictionary(Int32, Utf8)` or
`Dictionary(Int32, LargeBinary)`. For the row group bound a row counts its
4-byte key in each of them, and the values of a batch's dictionaries are
counted once, on the first row of the batch.

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
- The dictionary of a batch read from a file holds the values of the page
  being decoded (up to 4,096 rows), not only the values of the rows returned.
  Reading one row therefore returns a batch whose dictionary can be as large as
  the dictionary of a page: at most about 1 MiB per column, because the
  writer's dictionary page limit is 1 MiB and a column that outgrows it
  continues without a dictionary.

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
4       4      version, u32 = 2
8       8      trace count N, u64
16      8      length of spans.parquet in bytes, u64
24      4      CRC-32C of every byte of spans.parquet, u32
28      8      length of summary.parquet in bytes, u64
36      4      CRC-32C of every byte of summary.parquet, u32
40      44*N   records, sorted by trace_id (bytes compared in order)
40+44N  4      CRC-32C (Castagnoli) of every byte before it
```

Version 1 files, which have no fields at offsets 16 to 39 (the records start
at 16), are still read; such a part has no Parquet checksums.

Parquet 60.2.0 writes no page checksums and zstd's frame checksum is off, so
damage that leaves the file structure intact would otherwise be returned as
data. The writer therefore keeps a running length and CRC-32C of everything
it writes to each Parquet file and records them here, once the files are
complete (the summary is written before the index). `PartReader::open`
checks the lengths; `PartReader::verify` checks the CRCs.

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
missing, `traces.idx` fails a check, a Parquet file's length differs from the
length recorded in a version 2 index, a file does not have the expected
columns, the index's ranges do not match the number of rows, or the summary's
row count differs from the trace count. It does not read the data pages.

- `PartReader::verify` re-reads both Parquet files and compares their length
  and CRC-32C with the recorded values (`Corrupt` naming the file on a
  mismatch). It is meant for compaction and a future scrubber and is not
  called on open. For a version 1 part there is nothing to compare and it
  returns `Ok`; `spans_checksum()` and `summary_checksum()` return the recorded
  values, or `None` for version 1. Without `verify`, a damaged data page can
  still be returned as data.
- Every call into the Parquet library, at open and on every read including
  each `Scan::next`, runs inside `std::panic::catch_unwind`. A panic (the
  library asserts and unwraps on damaged files) becomes `Error::Corrupt`
  naming the file, and a `Scan` that hit one ends. This relies on the build
  not using `panic = "abort"`. The reader holds no mutable state, so a caught
  panic leaves nothing half-updated.
- `read_trace` and `read_rows` read a row range with a Parquet row selection,
  so pages holding none of the rows are not fetched.
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
- A projection returns the requested columns in the requested order; an
  unknown or repeated column is `Error::InvalidArgument`.

## Writing

Traces must arrive contiguously, though a trace may continue across batches,
and a trace that has ended must not come back (`Error::InvalidArgument`; the
rejected batch is not written and the writer stays usable). Memory is one row
group plus 44 bytes of index entry, a trace summary, and a 16-byte finished-id
entry per trace. A part with no rows is refused, `window_ns` must be positive
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

`PartMeta.format_version` is 2 (`FORMAT_VERSION`).
