# nwdb-payload

Large-text storage for the parts of NightsWatchDB. In agent traces every model
call re-sends the system prompt, the tool definitions and the conversation so
far, so 92 to 97% of the text in a part repeats earlier text of the same part.
This crate stores each distinct piece of text once per part; the span rows
refer to it.

It knows nothing about spans or OTLP and depends only on `nwdb-core`. The
decision of where to cut lies with a `nwdb_core::PayloadSplitter`
(implemented for OTLP by `nwdb_otlp::OtlpSplitter`); the part writer in
`nwdb-segment` applies it.

- [`rope`](#the-rope): how a value is stored when part of it lives in pieces.
- `json_array_elements`: the JSON array scanner.
- `PieceWriter`, `PieceReader`: the piece store, `pieces.blk` and
  `pieces.idx`.

```rust
let mut w = PieceWriter::create(dir, PieceWriterOptions::default())?;
let id = w.add(b"a piece of text")?;       // the same bytes give the same id
let summary = w.finish()?;                 // totals, and length and CRC-32C of both files

let r = PieceReader::open(dir)?;
let mut session = r.session();             // blocks decompressed during one read call
let mut out = Vec::new();
r.read_piece(&mut session, id, &mut out)?;
```

## The rope

A raw column value is stored in a part as a rope: a sequence of segments,
each either inline bytes or a reference to a piece. Reading concatenates them.
The cuts are made in the value's original bytes and nothing is re-encoded, so
the value is rebuilt byte for byte by construction, whatever the splitter
decided and whatever the bytes are.

```
empty value         ->  (no bytes)
value with no cuts  ->  0x00, the value
value with cuts     ->  0x01, then segments until the end:
                          varint(len << 1)       followed by len inline bytes
                          varint(id << 1 | 1)    a reference to piece `id`
```

Varints are little-endian base 128. A value without cuts costs one byte more
than the value; an empty value costs nothing; a reference costs one to five
bytes. Inline segments are never empty. A decoder refuses an unknown first
byte, a truncated or over-long number, an inline run longer than the rope, and
an empty inline run (`RopeError::Malformed`); a reference to a piece that does
not exist is an error from the store. A decoder never allocates from a length
it has not compared with the bytes it has.

`rope::cuts_are_valid` checks the contract of a splitter's ranges: ascending,
not overlapping, not empty, inside the value.

## The JSON array scanner

`json_array_elements(bytes, &mut ranges)` returns true and appends the range
of each top-level element when `bytes` is one complete JSON array (optionally
surrounded by whitespace); otherwise it returns false and appends nothing. It
understands strings, escapes and nesting, so `,`, `[` and `]` inside strings
or inner arrays do not confuse it. It does not validate numbers or literals:
an element is whatever lies between the separators. What lies between the
ranges is only whitespace, commas and the outer brackets, so the ranges and the
gaps tile the array. Empty elements (`[1,,2]`, `[1,]`), anything after the
closing bracket, a missing bracket and nesting deeper than 256 are refused. It
is iterative, uses a stack bounded by that depth, and never panics.

## The piece store

Two files, written next to the span rows of a part.

**`pieces.blk`** is the compressed blocks, one after the other. Pieces are
appended in first-seen order into a block until it holds about 1 MiB
(`block_bytes`); a piece larger than that gets a block of its own. Each block
is compressed on its own with zstd level 3. There is no header: the tables in
`pieces.idx` say where each block lies, and the whole file has a length and a
CRC-32C recorded by the part.

**`pieces.idx`** holds the block table and the piece table (all integers
little endian):

```
offset        size  field
0             4     magic "NWPI"
4             4     version, u32 = 1
8             8     block count B, u64
16            8     piece count P, u64
24            8     logical bytes, u64: what all references add up to
32            20*B  blocks: file offset u64, compressed length u32,
                    uncompressed length u32, CRC-32C of the compressed bytes u32
32+20B        ...   P piece lengths, each a varint, in piece id order
end-4         4     CRC-32C of every byte before it
```

Piece ids are dense `u32`, in first-seen order. A piece's block and offset are
not stored: the pieces fill the blocks in order, a block is full exactly when
its pieces add up to its uncompressed length, and the decoder derives the
position of each piece from the lengths and the block table. That makes the
piece table one or two bytes per piece. The decoder refuses a table that does
not fill the blocks exactly, a block that does not start where the previous one
ends, an empty piece, and a piece longer than 1 GiB.

`PieceReader::open` reads and checks `pieces.idx` completely and compares the
length of `pieces.blk` with what the table says. A block is read with a
positioned read, its CRC-32C is checked every time it is decompressed, and the
decompressed length must equal the recorded one: any damage is
`Error::Corrupt` naming `pieces.blk` or `pieces.idx`, never a panic and never
wrong bytes. The reader keeps no mutable state and serves many threads at
once. A `PieceSession` keeps the blocks one read call has decompressed (up to
32 MiB, then it starts over), so that many pieces of one block cost one
decompression. `verify` reads every block; `file_sums` gives the length and
CRC-32C of both files for comparison with the part's header.

Totals (`PayloadTotals` in `nwdb-core`): `pieces`; `logical_bytes`, the sum of
the lengths of every reference; `unique_bytes`, the sum of the lengths of the
distinct pieces; `stored_bytes`, the length of `pieces.blk`.

## De-duplication

Exact and bounded.

- A map from a keyed 64-bit hash of the bytes (`ahash`, seeded randomly for
  each writer, never stored) to the pieces with that hash. On a hash match
  the bytes are compared, and only equal bytes share a piece. A
  non-cryptographic hash alone is never trusted: because the seed is random, a
  sender cannot choose text whose hash collides with someone else's, and
  even if two pieces did collide the comparison keeps them apart. A test forces
  every piece into one bucket and shows that all read back as themselves.
- To compare, the writer needs the candidate's bytes. It keeps the
  uncompressed bytes of the open block and of recent closed blocks in memory,
  up to `dedup_window_bytes` (default 64 MiB). Each piece also costs 64 bytes
  of the window for the map entry and table row it keeps, so a window full of
  tiny pieces is bounded too. When the window is full it drops the closed
  block whose last match is oldest and removes its pieces from the map; a later
  repeat of one of them is stored again. That is a missed saving, never an
  error.

Memory of a writer: the open block, the window, 12 bytes of table and 4 bytes
of chain per piece stored (the table is the one thing that grows with the part,
in proportion to the number of distinct pieces, not their size) and a map
entry per piece in the window. A test writes 120 MiB of unique text through a
4 MiB window and shows the peak resident memory does not grow with the amount
of unique text (see `nwdb-segment`).
