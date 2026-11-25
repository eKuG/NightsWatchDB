# textindex: inverted index over a `dedup` payload store

Commands: `build`, `queries`, `search`, `scan`, `check` (see ../SPEC.md). Extra optional flags:
`--budget-mb` (segment byte budget, default 512), `--cache-mb` (decoded store block cache, default 256).
Single-threaded; all integers little-endian.

## Index directory
- `seg-NNNNN.tix`: one file per index segment, covering the contiguous document ids [doc_lo, doc_hi).
  A segment closes at `--docs-per-segment` documents or when its in-memory postings pass the byte budget.
- `piece_rows.bin` (piece variant only).

## Segment file
    [postings area][dictionary][sparse index][footer, 60 bytes]

Document ids are piece ids (piece variant) or row ids (span variant). No positions, no frequencies.

**Posting list** (terms with more than 8 documents), at an offset inside the postings area:
`skip table` then `blocks`. Skip table: one entry per block of 128 ids (the last block may be shorter),
8 bytes each: `last id u32`, `block start u32` (relative to the end of the skip table). Block:
`width byte w`, then the block's gaps bit-packed LSB first, `w` bits each, `ceil(n*w/8)` bytes.
gap = id - previous id - 1, where the id before the first one is 0xFFFFFFFF (wrapping), so the first
gap is the first id and a run of consecutive documents has width 0. A block is decoded from its own
width byte and the previous block's last id (from the skip table), so a reader can jump to any block.

**Dictionary**: terms in ascending byte order, in blocks of 32 terms. Entry:
`varint shared-prefix length with the previous term (0 at the start of a block)`, `varint suffix length`,
suffix bytes, `varint df`, then either (df <= 8) df varints of gaps (same gap rule, inline), or (df > 8)
`varint offset` of the posting list in the postings area.

**Sparse index**: per dictionary block: `varint len, first term bytes, u64 block offset` (relative to the
start of the dictionary). Loaded into memory when the segment is opened; a lookup binary-searches it,
reads one block with a positioned read and scans it.

**Footer** (60 bytes): `dictionary offset u64`, `sparse index offset u64`, `terms u64`, `dictionary blocks u64`,
`postings u64`, `doc_lo u32`, `doc_hi u32`, `terms per block u32`, `version u8 (=1)`, 3 zero bytes,
magic `TXIX`. Positions and scoring will need a new version byte.

## piece_rows.bin
`"PRW1"`, `pieces u64`, `rows u64`, `(pieces+1) u64` offsets into the data, then per piece the ascending rows
that reference it, as varints: first row as is, then row - previous row. A row that uses a piece several times
is listed once. Read into memory when the searcher opens.

## Semantics
- Row text = the six attribute values in table order (absent = empty, arrays rebuilt as `[` pieces joined
  by `,` `]`), joined by one 0x00 byte (five per row). `text_bytes` counts exactly that.
- Boolean classes are evaluated over a row's token set. Piece variant: each term (or prefix) becomes a row bitmap
  through piece_rows, then bitmaps are combined (AND, OR, AND NOT). Span variant: lists of rows are combined
  with skip-based intersection, sorted merges for OR and NOT, sorted union for prefix.
- Phrase: adjacency is defined inside one piece (a whole value is one piece). Candidates are the documents whose
  index holds both tokens (skip intersection); each candidate is verified against the text read from the store.
  Piece variant verifies each candidate piece once; span variant rebuilds the row's pieces, in order, and stops at
  the first piece that contains the phrase. `verified_bytes` counts the piece bytes inspected. The store block
  cache is emptied before every run, so the decompression cost is inside the measured time.
- `scan` applies the same rules to a plain pass over the store.
