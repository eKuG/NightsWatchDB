# payload: payload store experiment (stream 2)

Commands: `payload build|sample-ids|open|span|scan` (see ../SPEC.md). Variants: `parquet`,
`parquet-tuned`, `zstdlong`, `dedup`. Every command is single-threaded; `--threads` is accepted and
reported, and `threads_used` says 1. `build` also takes `--window-s` (dedup scope, default 900) and
`--tune key=value,...` (parquet variants only; keys: level, rg_rows, rg_mb, page_kb, page_rows,
dict_kb, dict, write_batch, feed_mb).

A trace's start time for scoping is the start time of its first span in the input. Scope
window = floor(start_ns / window), so scopes are aligned to the epoch.

`check.bin`: per trace in arrival order, trace id (16), SHA-256 of all its values in row and
attribute order (32), byte count (u64 LE); 56 bytes per trace. Not counted in `bytes_on_disk.total`.

## parquet / parquet-tuned
`payloads.parquet`, schema of the bake-off `payloads` table, page statistics (page index) on.
`parquet`: zstd 3, 2,048-row groups, everything else the parquet 60.2.0 default; values are handed to
the writer in batches of about 64 MB. Because the writer closes a data page only between write calls
and between chunks of `write_batch_size` values, the feed batch size changes the effective page size.
`parquet-tuned`: zstd 9, dictionary on, data page limit 32 MiB, write batch size 1 (so the page limit is
honoured per value), row groups closed at 64 MB of values (no row limit), feed 64 MB.
Locating a trace: the trace_id column is scanned once at open to learn each trace's row range (reported
as `open_setup_ms`); reads use a row selection, so only pages holding selected rows are read.

## zstdlong
`zl.data`: one zstd frame per trace, level 3, long-distance matching on, window log 27 (decoder limit set to 27).
`zl.idx`: "ZLX1", u64 traces, u64 spans; per trace {trace_id[16], frame_off u64, frame_len u64, raw_len u64,
first_row u64, spans u32}; then one u64 per span (global row order): start offset inside the uncompressed frame.
Frame content per span, six attributes in table order: `00` absent, or `01` u32 length, bytes.
A span read decodes the frame from its start up to the span.

## dedup
Pieces: whole value for system_instructions, tool_definitions, tool_call_arguments, tool_call_result;
top-level array elements (exact bytes) for input_messages and output_messages, rebuilt and compared
byte for byte at build time, else the value is stored whole (`whole_fallbacks`). Piece identity: xxh3-128, bytes
compared on a hash match. One scope per time window; scopes share nothing.
`dd.blocks`: zstd 3 frames, pieces packed in first-seen order into blocks of at most 1 MiB (a piece of
1 MiB or more has its own block). `dd.refs`: per row, six attributes, each `FFFFFFFF` absent,
`40000000` + u32 piece (whole value), or u32 n + n x u32 piece ids (array: `[` + pieces joined by `,` + `]`).
`dd.idx`: "DDX1", u64 traces/rows/pieces/blocks; traces {id[16], first_row u64, rows u32}; rows+1 u64
offsets into dd.refs; pieces {block u32, offset u32, length u32}; blocks {offset u64, compressed u32, raw u32}.
Reads decompress a block once per lookup (cache emptied before every `open`/`span` lookup; the scan keeps
up to 256 MB of decoded blocks, oldest first out).
Memory: the open scope holds every distinct piece (plus a hash map); released at each scope end.
`max_scope_state_bytes_est` reports the largest.
