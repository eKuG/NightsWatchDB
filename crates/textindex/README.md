# nwdb-textindex

The full-text index of NightsWatchDB: an inverted index over documents
`(doc id: u32, bytes)`, written and read through its own file format. The crate
knows nothing about spans, traces or parts. The database indexes each distinct
piece of message text once, so a phrase is checked once however many spans
repeat it (experiment B measured this design: an index over pieces is about
five times smaller and seventeen times cheaper to build than one over spans).

It has no positions, frequencies or scores. A phrase is answered in two steps:
the index returns the documents that hold every token of the phrase, and the
caller checks each against its text with `phrase_matches`.

Depends only on `nwdb-core` and `crc32c`. No search, FST, roaring or
bit-packing crates.

## What is in the crate

| Item | Purpose |
|---|---|
| `tokens(&[u8])` | The tokeniser, an iterator over the bytes of a text. Allocates nothing. |
| `IndexBuilder`, `IndexOptions` | Builds one index file from documents added in ascending id order, in memory bounded by a byte budget. |
| `IndexReader`, `ReadAt` | Opens an index through positioned reads (`File`, `&[u8]`, `Vec<u8>`, or any `ReadAt`). `term`, `prefix`, `intersect`, `verify`, `doc_count`, `meta`. `Send + Sync`. |
| `DocIds`, `ids::{intersect, union, difference}` | Ascending id lists and the set operations on them. |
| `Query`, `parse`, `leaves`, `Leaf` | The query model and its parser. |
| `evaluate`, `phrase_candidates`, `phrase_matches` | What the caller needs to run a query over its own notion of "document" (see below). |
| `IdListWriter`, `IdListReader` | A compact "dense key to ascending list of `u32`" store. |

## Tokeniser

ASCII `A-Z` is folded to `a-z`. A token is a maximal run of bytes that are
`a-z`, `A-Z`, `0-9` or at least `0x80`; every other byte separates tokens. Bytes
at or above `0x80` are token bytes (UTF-8 in any script, and invalid bytes too),
and are never case folded. A token longer than 64 bytes is not indexed, but it
still counts as a token, so it takes a position in a phrase.

`Token` is a slice of the original text; `Token::lowercase` copies it into a
64-byte buffer on the caller's stack when it is short enough to index.

## Building

```rust
let mut builder = IndexBuilder::new(IndexOptions::default(), &mut file);
builder.add(doc_id, bytes)?;       // ids strictly ascending, gaps allowed
let meta = builder.finish()?;      // writes the directory and footer
```

Deviation from the first sketch of the API: the writer is passed to `new`, not
to `finish`, because an index segment is written as soon as it closes; passing
`&mut writer` keeps ownership with the caller.

A document becomes tokens in the postings of the **open segment**, held in
memory. When the memory held for them passes `segment_budget_bytes` (default
256 MiB), the segment is closed: its terms are sorted, its postings, dictionary
and sparse index are written, and its memory is freed. The next document opens
a new segment. A segment covers a contiguous range of document ids; segments
of one file never overlap.

### Memory bound

The open segment is the only thing that grows with the input. The budget is an
estimate of heap bytes (each term's bytes, its hash-table slot, its list header
and the capacity of its id list, with allocator rounding), checked after each
document. Closing a segment additionally needs the sorted order of the terms
(32 bytes per term) and the segment's dictionary and sparse index (about 14
bytes per term).

Measured with a counting allocator (`tests/build_memory.rs`), building about
13 MB of text with a Zipf-like mix of 50% common and 50% rare words:

| Budget | Peak heap above the start | Segments |
|---|---|---|
| 128 KiB | 0.36 MiB (2.9 x) | 1,781 |
| 512 KiB | 0.73 MiB (1.45 x) | 376 |
| 2 MiB | 2.6 MiB (1.30 x) | 69 |

The peak is about 1.3 times the budget plus a fixed 250 KiB or so (the output
buffer and the table's growth step). The test asserts `peak <= 1.5 x budget +
256 KiB`, and that a larger budget gives a larger peak. One large document can
exceed the budget by about 120 bytes for each distinct new token it holds; the
check happens between documents.

## Index file layout

All integers are little-endian. Checksums are CRC-32C (Castagnoli).

```text
file    = segment* directory footer

segment = postings area | dictionary | sparse index     (lengths in the directory)

directory = one 72-byte entry per segment
footer    = 64 bytes
```

### Footer (64 bytes, at the end of the file)

```text
  0  u32  format version (1)
  4  u32  number of segments
  8  u32  terms per dictionary block (32)
 12  u32  reserved, zero
 16  u64  documents (including documents with no tokens)
 24  u64  bytes of text tokenised
 32  u64  terms, summed over segments
 40  u64  postings, summed over segments
 48  u64  directory offset
 56  u32  CRC-32C of the directory bytes followed by footer bytes 0..56
 60  4    magic "NWTX"
```

The directory lies directly before the footer: its offset must equal
`file length - 64 - 72 x segments`.

### Directory entry (72 bytes)

```text
  0  u64  postings area length
  8  u64  dictionary length
 16  u64  sparse index length
 24  u64  terms
 32  u64  postings (entries over all terms)
 40  u32  documents in the segment
 44  u32  lowest document id
 48  u32  highest document id (inclusive)
 52  u32  dictionary blocks = ceil(terms / 32)
 56  u32  CRC-32C of the postings area followed by the dictionary
 60  u32  CRC-32C of the sparse index
 64  u64  offset of the segment in the file
```

Segments are contiguous from offset 0 and end where the directory starts;
their document ranges are ascending and disjoint.

### Dictionary

Terms in ascending byte order, in blocks of 32 terms (the last block may hold
fewer). Within a block terms are front coded. An entry is

```text
varint  length of the prefix shared with the previous term (0 at a block start)
varint  length of the suffix (at least 1)
bytes   suffix
varint  document count df (1 .. documents in the segment)
then    df <= 8:  df varints, the gaps of the ids (inline posting list)
        df >  8:  varint, offset of the posting list in the postings area
```

A term is at most 64 bytes. Gaps: `gap = id - previous id - 1`, taking the previous
id before the first as -1, so the first gap is the first id, and a run of
consecutive ids is all zeros.

### Sparse index

One entry per dictionary block: `varint length`, the block's first term,
`u64 offset of the block, relative to the start of the dictionary`. The whole
sparse index is loaded when the file is opened; a lookup binary-searches it,
reads one block, and scans it.

### Postings area

One posting list per term with more than 8 documents, at the offset the
dictionary gives:

```text
skip table   one entry per block of 128 ids:  u32 last id of the block,
             u32 start of the block relative to the end of the skip table
blocks       per block: one byte, the bit width w (0 to 32) of its gaps,
             then the gaps packed w bits each, least significant bit first,
             ceil(n x w / 8) bytes
```

The last block may hold fewer than 128 ids; its count follows from `df`.
A block is decoded from its own width byte and the previous block's last id
(from the skip table), so a reader can jump to any block.

## Reading and failure rules

`IndexReader::open` reads the footer, verifies its checksum (which covers the
directory), validates the directory, and loads and checks the sparse index of
each segment. After that, only positioned reads: a dictionary block (at most
about 4 KiB) and the posting blocks a call needs. Nothing is shared between
calls and there is no cursor, so one reader serves many threads.

**What is checked, and when.**

- At `open`: magic, version, reserved bytes, the footer checksum, that the
  directory fits the file, that segments are contiguous and fill the file,
  document ranges ascend, counts agree with each other and with the segment
  sizes (a term count that the dictionary could not hold is refused before
  anything is allocated from it), the sparse index checksum and its structure
  (offsets ascend inside the dictionary, first terms ascend).
- The first time a segment is used (any `term`, `prefix` or `intersect`, or
  `verify`): the checksum of its postings and dictionary is computed in
  64 KiB reads and compared. **The first search therefore reads the whole file
  once.** A caller that cannot afford that on the query path calls
  `IndexReader::verify` when it loads the file, off the query path.
- On every read: every varint (bounded to 10 bytes), term length, shared-prefix
  length, ordering of terms, df, inline ids against the segment's id range,
  every posting offset against the postings area, the skip table (ascending
  ids and starts, inside the list), every block's width, length and last id
  against its skip entry. A failure is `Error::Corrupt` naming what was wrong.

No length, count or offset from a file sizes an allocation until it has been
checked against the file's length or against a constant. A list of `n` ids is
decoded block by block and grows with what was actually read, never
`with_capacity(n)` from an unchecked `n`.

The checksum only guards against damage. A hostile file with valid checksums
meets the structural checks alone; `tests/damage.rs` exercises both.

### Memory of an open reader

The directory and each segment's sparse index: about **1.0 MB per million
terms** (summed over segments), measured with a counting allocator on an index
of 250,000 distinct terms in one segment and in 43 segments (about 32 bytes per
block of 32 terms: the first term, a length and an offset). Plus 72 bytes of
directory and a few hundred bytes of bookkeeping per segment. Terms of 64
bytes cost up to 2.5 times that.

A search holds, beyond its output, one dictionary block and, for each posting
list it opens, that list's skip table (8 bytes per 128 ids, so 1/64 of the
packed list or less) and one 128-id block.

### Searching

- `term(token)`: ascending ids across all segments. The token must be in the
  tokeniser's output form (lowercase, 1 to 64 bytes); anything else matches
  nothing.
- `prefix(prefix, limit_terms)`: the union over every term that starts with the
  prefix, merged from the posting lists with a heap (memory: the output plus a
  block and skip table per matching term). If more than `limit_terms` distinct
  terms match, nothing is read and the call fails with `InvalidArgument`.
- `intersect(tokens)`: documents that contain every token. The shortest list
  drives; the others jump with their skip tables, so a rare token next to a
  common one reads only the blocks it needs (tested by counting blocks read).
- `ids::intersect`, `union`, `difference` work on ascending slices; each
  allocates only its output.

## Query model

```text
query   = and
and     = or { or }                 operands side by side are ANDed
or      = unary { "OR" unary }      OR binds tighter than the implicit AND
unary   = "-" unary | primary
primary = "(" and ")" | phrase | word
phrase  = '"' any bytes but '"' '"'
word    = a run of bytes other than white space, '(', ')' and '"'
```

- `a b OR c` is `a AND (b OR c)`; `OR` must be upper case.
- A leading `-` negates the operand after it (`-foo`, `-"a b"`, `-(a OR b)`).
  A `-` followed by white space, `)` or the end is an ordinary byte.
- A word is cut into tokens with the tokeniser. One token is a `Term`, several
  (`foo-bar`) a `Phrase`, none and the word is ignored (so are an empty phrase
  and an empty group). A phrase in quotes is cut the same way.
- A word ending in `*` is a `Prefix`: what precedes the last `*` must be one
  token of 2 to 64 bytes.
- Errors (`InvalidArgument`): no usable operand; only negated operands; `OR`
  without an operand on both sides; an unterminated `"` or `(`; a stray `)`;
  nesting deeper than 32 (parentheses and `-` together); more than 64 operands;
  a phrase of more than 64 tokens; a query longer than 16 KiB.

The crate does not evaluate a whole `Query` over documents. A caller maps
documents to spans, and a boolean operator is defined over a span's whole
text, which is made of several documents; so combining must happen on the
caller's sets. The crate provides:

- `leaves(&Query)`: the `Term`, `Prefix` and `Phrase` leaves, in order.
- For a leaf: `IndexReader::term`, `IndexReader::prefix`, and for a phrase
  `phrase_candidates(reader, tokens)` (documents with every indexable token)
  then `phrase_matches(text, tokens)` on each candidate's text (the tokens occur
  consecutively in the token stream; a token longer than 64 bytes occupies a
  position and matches only an equal token).
- `evaluate(&Query, universe, leaf)`: combines the ascending id lists the
  caller returns for each leaf. `Not` is the universe minus the operand; an
  `And` of only negations starts from the universe. Lists must be strictly
  ascending (`InvalidArgument` otherwise); the nesting depth of a hand-built
  query is limited to 128.

## The id-list store

`IdListWriter::push(&[u32])` appends the list of the next key (keys are
`0..n`); `IdListReader::get(key)` returns it. Lists use the same gap,
bit-packing and block code as the postings, with the same bounds checks.

```text
file    = group* group index footer
group   = header | lists | crc
header  = one varint per key of the group: the byte length of that key's list
list    = varint n, then
          n <= 8:  n varints, the gaps of the ids
          n >  8:  blocks of 128 ids as in the postings (width byte, packed gaps),
                   without a skip table
crc     = u32 CRC-32C of the header and lists of the group
index   = per group: u32 first key, u64 offset of the group   (12 bytes)
footer  = 40 bytes:
   0  u32  format version (1)
   4  u32  groups
   8  u64  keys
  16  u64  ids
  24  u64  offset of the group index
  32  u32  CRC-32C of the group index followed by footer bytes 0..32
  36  4    magic "NWIL"
```

A group closes after 128 keys or about 32 KiB of lists, so a read fetches one
small group whose checksum is verified every time one of its keys is read.
The writer holds one group plus 12 bytes per group; the reader holds 12 bytes
per group (at most 12 bytes per 128 keys).

## Tests

- `src/tests.rs`: the tokeniser, varints, bit packing for widths 0, 1, 7, 8, 17
  and 32 and lengths 1, 8, 9, 128, 129 and 1000, front coding, set operations,
  skip data used by intersection.
- `tests/properties.rs`: seeded random documents (small shared vocabulary, large
  vocabulary, long lists across block boundaries, gaps in ids) with one segment
  and with many, against a brute-force scan: `term`, `prefix`, `intersect`,
  phrase candidates plus `phrase_matches`, and `evaluate`.
- `tests/query.rs`: every syntax form, precedence, errors and limits; `evaluate`
  against brute force over random sets; parsed text through the index.
- `tests/damage.rs`: truncation at every length, every bit flipped in a small
  index and a sample of a large one, random bytes, and damage with the
  checksums repaired, for both the index and the id-list store, under an
  allocator that aborts on any single allocation above 64 MiB.
- `tests/build_memory.rs`: the memory bound of the builder and of an open
  reader, with a counting allocator.
- `tests/size.rs`: the index over a few megabytes of English-like text.
- `tests/idlist.rs`: round trips and group boundaries of the id-list store.
