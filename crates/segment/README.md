# nwdb-segment

Writes and reads one immutable **part**: the spans of the traces that started in
one time window, stored so that opening a trace is one index lookup and one
contiguous read.

- `PartWriter` builds a part in `<parts dir>/<id>.tmp` and renames it to
  `<parts dir>/<id>` once every file is synced.
- `PartReader` opens a finished part. It holds no file position, so one reader
  can be shared by many threads; every read opens its own file handle.

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
| Statistics | page level (so also the column index) | row group pruning by `start_ns`; page pruning |
| Offset index | on | lets a row selection skip pages |
| Bloom filters | off | the trace index replaces them |
| Row group size | 16,384 rows or 64 MiB of value bytes, whichever first | rows range from 1 KB to several MB, so a row bound alone is unsafe; the row that crosses the byte bound stays in the group |
| Data page size | 1 MiB | |
| Data page rows | 4,096 | a trace lookup should not decode a page of tens of thousands of unrelated small rows |
| Dictionary page | 1 MiB | |
| Write batch | 256 values | the Parquet writer checks page limits between batches of this many values |

Value bytes are the lengths of all string and binary values of a row, the
measure `nwdb_core::BatchLimits` uses.

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
4       4      version, u32 = 1
8       8      trace count N, u64
16      44*N   records, sorted by trace_id (bytes compared in order)
16+44N  4      CRC-32C (Castagnoli) of every byte before it
```

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
missing, `traces.idx` fails a check, a file does not have the expected columns,
the index's ranges do not match the number of rows, or the summary's row count
differs from the trace count.

- `read_trace` and `read_rows` read a row range with a Parquet row selection,
  so pages holding none of the rows are not fetched.
- `scan` and `scan_summary` prune row groups by the `start_ns` statistics and
  do not filter rows; the caller filters. `Scan` reports how many row groups
  were read and skipped.
- A projection returns the requested columns in the requested order; an
  unknown or repeated column is `Error::InvalidArgument`.

## Writing

Traces must arrive contiguously, though a trace may continue across batches,
and a trace that has ended must not come back (`Error::InvalidArgument`; the
rejected batch is not written and the writer stays usable). Memory is one row
group plus 44 bytes of index entry, a trace summary, and a 16-byte finished-id
entry per trace. A part with no rows is refused, and dropping an unfinished
writer removes its temporary directory.
