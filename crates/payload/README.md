# nwdb-payload

Large-text storage for the parts of NightsWatchDB. In agent traces every model
call re-sends the system prompt, the tool definitions and the conversation so
far, so 92 to 97% of the text in a part repeats earlier text of the same part.
This crate stores each distinct piece of text once per part; the span rows
refer to it.

It knows nothing about spans or OTLP and depends only on `nwdb-core`. The
decision of where to cut lies with a `nwdb_core::PayloadSplitter`
(implemented for OTLP by `nwdb_otlp::OtlpSplitter`); the part writer in
`nwdb-segment` applies it, and decides which cuts are worth a piece (a floor on
their length, a ceiling on their number).

- [`rope`](#the-rope): how a value is stored when part of it lives in pieces.
- `json_array_elements`: the JSON array scanner.
- `PieceWriter`, `PieceReader`: the piece store, `pieces.blk` and
  `pieces.idx`.

```rust
let mut w = PieceWriter::create(dir, PieceWriterOptions::default())?;
let id = w.add(b"a piece of text")?;       // the same bytes give the same id
let summary = w.finish()?;                 // totals, and length and CRC-32C of both files

let r = PieceReader::open_recorded(dir, summary.blocks, summary.index)?;
let mut session = r.session();             // the block the last read decompressed
let mut out = Vec::new();
r.read_piece(&mut session, id, &mut out)?;

// Many pieces at once: ids in ascending order, each block decompressed once.
r.read_sorted([id], |n, bytes| { /* the nth id's bytes */ })?;
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
value with cuts     ->  0x03, CRC-32C of the value (u32 little endian),
                        varint(length of the value),
                        then segments until the end:
                          varint(len << 1)       followed by len inline bytes
                          varint(id << 1 | 1)    a reference to piece `id`
```

Varints are little-endian base 128. A value without cuts costs one byte more
than the value; an empty value costs nothing; a value with cuts costs five
bytes plus the varint of the value's length more than its inline bytes and
references, and a reference costs one to five bytes. Inline segments are never empty.

The checksum is the CRC-32C of the value the rope rebuilds to. Parquet pages
carry no checksum and a read does not run `verify`, so without it one flipped
bit in a reference would splice in a piece that belongs to another span, perhaps
another sender's text; with it the reader compares the rebuilt value and returns
`Corrupt` instead of the bytes. A rope without references holds its value
itself, so damage to it changes only that row, as it always did, and costs
nothing extra. The two tags differ in two bits, so one flipped bit in the tag
never turns a rope with references into one that is taken as its own bytes.

The length is the length of the rebuilt value. A reader adds up the segments
(the length of a piece is in the store's tables, no piece is read) and compares
the sum with it before it allocates anything, stopping at the first segment that
goes past it: one flipped bit in an inline run's length can turn text into
hundreds of references to a large piece, and without the length the reader would
reserve all of it before the checksum rejected the value. `rope::recorded_len`
gives it; `rope::decode` and `rope::logical_len` check it and return
`RopeError::Length` on a mismatch.

A decoder refuses an unknown first byte, a tag 3 rope too short for its
checksum and length, a truncated or over-long number, an inline run longer than the rope,
and an empty inline run (`RopeError::Malformed`); `rope::decode` returns
`RopeError::Checksum` when the rebuilt value does not match; a reference to a
piece that does not exist is an error from the store. A decoder never allocates
from a length it has not compared with the bytes it has.

`rope::translate(rope, map, out)` writes a rope for the same value with each
reference replaced as `map` says: a piece id of another store (`Mapped::Piece`),
or the piece's bytes (`Mapped::Inline`) when that store cannot take it. The
value is not rebuilt, so the checksum and the length are copied as they are, and a wrong `map` is
caught by the reader exactly as a damaged reference is; a rope whose references
all became inline bytes is written as the rope of a value with no cuts.

`rope::segments` walks a rope, `rope::piece_ids` lists the pieces it refers to,
`rope::checksum` gives the carried checksum, and `rope::cuts_are_valid` checks
the contract of a splitter's ranges: ascending, not overlapping, not empty,
inside the value.

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
little endian; the format is version 1 and did not change with the rope):

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
piece table one byte per piece up to 127 bytes, two up to 16 KiB, three up to
2 MiB. The decoder refuses a table that does not fill the blocks exactly, a
block that does not start where the previous one ends, an empty piece, and a
piece longer than 1 GiB.

`PieceReader::open_recorded(dir, blocks, index)` is how a part opens its store.
It compares the length of both files with the recorded ones before reading
anything, so a file that grew is never read in full; then it reads
`pieces.idx` and compares its CRC-32C with the recorded one (over every byte but
the last four, the file's own checksum: the CRC-32C of a message with its
CRC-32C appended is the same for every message, so over the whole file it would
tell one store from another not at all; `FileSum::crc32c` is defined that way for
`pieces.idx`). `pieces.idx` holds
the checksum of every block, so this binds the blocks to the part: piece index
taken from another part is refused at open (`Corrupt`, naming `pieces.idx`)
because their checksum differs. A `pieces.blk` of the same length alone, with
this store's `pieces.idx`, is not refused by open, which does not read the
blocks: its blocks are caught by their checksums when they are read and by
`verify`. `PieceReader::open(dir)` is the same
without a record to compare with. Either way `pieces.idx` is checked completely
and the length of `pieces.blk` is compared with what the table says. A block is
read with a positioned read, its CRC-32C is checked every time it is
decompressed, and the decompressed length must equal the recorded one: any
damage is `Error::Corrupt` naming `pieces.blk` or `pieces.idx`, never a panic
and never wrong bytes. The reader keeps no mutable state and serves many
threads at once. `verify` reads every block; `file_sums` gives the length and
CRC-32C of both files for comparison with the part's header, and also checks
the last four bytes of `pieces.idx` against the rest of the file, which the
recorded checksum leaves out and `open` checks: a store that passes `file_sums`
and `verify` is one that opens.

**What an open store costs in memory.** The reader keeps the piece table as it
is on disk (the varints) and finds a piece through a small index over it: the
first piece of each block, and a checkpoint (position in the table and offset in
the block, 8 bytes) for every 64th piece of a block. A lookup is a binary search
over the blocks and a scan of at most 63 lengths. That is the length of each
piece (one to three bytes) plus 8 bytes for every 64th piece and about 36 bytes
a block. Measured with an open part of 3.5 million pieces of 12 bytes: 3.9 MB
for the tables, 1.13 bytes a piece, and the process's resident memory grew
4.4 MB when it was opened; before, with a table of locations at 12 bytes a
piece, it grew 41 MB (12.0 bytes a piece). Pieces of 128 bytes to 16 KiB have
two-byte lengths, so they cost about 2.1 bytes a piece (the unit tests check a
mix of lengths against 3 bytes a piece, and the segment test checks the 12-byte
pieces against 2). `PieceReader::memory_bytes` gives the figure for an open
store.

**Reading.** `read_piece` appends one piece; its `PieceSession` keeps the last
block it decompressed, so reading consecutive pieces of one block costs one
decompression. `read_sorted(ids, f)` takes ids in ascending order, which is
the order of the blocks, and calls `f(n, bytes)` for each: every block is read
and decompressed once however many of its pieces are named and however often,
and one decompressed block is held at a time. The part reader rebuilds a batch
of rows with it: it collects every reference of the batch, sorts them by id,
and copies each piece to its places in the output, so reading a value costs the
blocks it touches, not its references.

Totals (`PayloadTotals` in `nwdb-core`): `pieces`; `logical_bytes`, the sum of
the lengths of every reference; `unique_bytes`, the sum of the lengths of the
distinct pieces; `stored_bytes`, the length of `pieces.blk`. No honest value
refers to more bytes than `logical_bytes`, and the references of a part add up
to exactly that, which is what a reader and `verify` check the ropes against.

## Writing

`add(bytes)` returns the id of the piece that holds `bytes`, storing it if it is
new; `add_piece` also says whether it was new (`Added { id, new }`), `find`
returns the id of a piece the writer still knows without ever storing one, and
`piece_count` says how many there are. Every `add` and every successful `find`
counts as a reference in `logical_bytes`. A `PieceObserver`, which the part
writer calls from `WriterOptions::piece_observer`, is told about each piece the
first time it is stored, in the order of the ids.

A caller that carries ropes written against another store over to this one
(a merge of parts) knows its references itself: `add_piece_unreferenced` and
`find_unreferenced` do what `add_piece` and `find` do without counting a
reference, and `add_references(bytes)` counts references. The total must equal
the length of every reference in the ropes written against the store, since
that is what `verify` compares the ropes with.

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
  up to `dedup_window_bytes` (default 64 MiB). A block entering the window is
  trimmed to the bytes it holds (its buffer grew by doubling, and would
  otherwise hold up to twice what it is charged), and the window is charged for
  what is held: the bytes, and 64 bytes for each piece in it for its offset,
  chain link and map entry. The offsets and chain links live with their block,
  so a block that leaves the window takes them with it. A window full of tiny
  pieces is therefore bounded too. When the window is full it drops the closed
  block whose last match is oldest and removes its pieces from the map; a later
  repeat of one of them is stored again, under a new id. That is a missed saving,
  never an error.

**Memory of a writer**: the open block, the window, and the length of every
piece stored, in the form it has on disk (one to three bytes a piece). That
last part is the only thing that grows with the part, in proportion to the
number of distinct pieces, not to their size, and the part writer bounds the
number (`max_pieces`) and keeps short cuts out (`min_piece_bytes`); before, a
piece cost the writer 16 bytes for the life of the part. Measured as the
kernel's peak resident memory (`VmHWM`) while writing 12-byte pieces through a
4 MiB window: 0.7 million pieces peaked at 27 MiB and 3.5 million at 33 MiB
(before: 37 and 101 MiB). `tests/window_memory.rs` writes 192 MiB of unique text
through a 16 MiB and through a 64 MiB window and shows that the 48 MiB of extra
window cost 46 MiB of resident memory (the peaks were 27 and 73 MiB), that is,
the window and no more.
