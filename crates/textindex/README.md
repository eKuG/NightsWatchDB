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
| `IndexBuilder`, `IndexOptions`, `MIN_SEGMENT_BUDGET` | Builds one index file from documents added in ascending id order, in memory bounded by a byte budget (at least 1 MiB). |
| `IndexReader`, `ReadAt`, `Matches` | Opens an index through positioned reads (`File`, `&[u8]`, `Vec<u8>`, or any `ReadAt`). `term` and `intersect` return `Matches { ids, exact }`; `prefix`, `verify`, `doc_count`, `meta`. `Send + Sync`. |
| `DocIds`, `ids::{intersect, union, difference}` | Ascending id lists and the set operations on them. |
| `Query`, `parse`, `leaves`, `Leaf` | The query model and its parser. `Leaf::needs_confirmation` says which leaves need the text. |
| `evaluate`, `phrase_candidates`, `phrase_matches`, `contains_token` | What the caller needs to run a query over its own notion of "document" (see below). |
| `IdListWriter`, `IdListReader` | A compact "dense key to ascending list of `u32`" store. |

## Tokeniser

ASCII `A-Z` is folded to `a-z`. A token is a maximal run of bytes that are
`a-z`, `A-Z`, `0-9` or at least `0x80`; every other byte separates tokens. Bytes
at or above `0x80` are token bytes (UTF-8 in any script, and invalid bytes too),
and are never case folded. A token longer than 64 bytes (a digest, a long key)
is not a term of its own: the index lists it under a key made of a `0x00`
byte (which can never be a token byte) followed by its first 64 bytes, case
folded. A search for it finds candidates, which the caller confirms against the
text (see "Words longer than 64 bytes"). It also counts as a token, so it takes
a position in a phrase.

`Token` is a slice of the original text; `Token::lowercase` copies it into a
64-byte buffer on the caller's stack when it is short enough to be a term.

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
256 MiB, **at least 1 MiB**: a smaller value is raised to `MIN_SEGMENT_BUDGET`),
the segment is closed: its terms are sorted, its postings, dictionary and
sparse index are written, and its memory is freed. The next document opens a
new segment. A segment also closes when it holds `u32::MAX` documents, so its
document counter never wraps. A segment covers a contiguous range of document
ids; segments of one file never overlap.

### Memory bound

The open segment grows with the input, up to the budget. Each **closed**
segment costs a 72 byte directory entry that stays in memory until `finish`
(144 bytes at most, counting the vector's spare room). That is why the budget
has a minimum: a segment closes only after more than 1 MiB of estimated heap,
and a new term costs about 100 bytes of that, so even text made only of new
words has at least about 10,000 tokens (some 10 KB) in every closed segment.
The directory therefore costs at most about 1.5% of the input in the worst
case and far less for ordinary text, where words repeat. With the default
256 MiB budget it is at most 144 bytes per 256 MiB of estimated heap.

The budget is an estimate of heap bytes (each term's bytes, its hash-table
slot, its list header and the capacity of its id list, with allocator
rounding), checked after each document. Closing a segment additionally needs
the sorted order of the terms (32 bytes per term) and the segment's dictionary
and sparse index (about 14 bytes per term for ordinary text; the sizes are in
`IndexMeta`).

Measured with a counting allocator (`tests/build_memory.rs`), building about
30 MB of text with a Zipf-like mix of 50% common and 50% rare words:

| Budget | Peak heap above the start | Segments |
|---|---|---|
| 1 MiB | 1.35 MiB (1.35 x) | 316 |
| 2 MiB | 2.60 MiB (1.30 x) | 138 |
| 4 MiB | 5.15 MiB (1.29 x) | 64 |

The peak is about 1.3 times the budget plus a fixed 250 KiB or so (the output
buffer and the table's growth step). The test asserts `peak <= 1.5 x budget +
256 KiB + 144 bytes x segments`, and that a larger budget gives a larger peak.
`tests/regressions_review_memory.rs` builds 60,000 small documents with a
requested budget of 1 KiB (raised to 1 MiB): the number of segments follows
the minimum budget, and growing the input twelvefold adds only the documented
144 bytes per extra segment to the peak. One large document can exceed the
budget by about 120 bytes for each distinct new token it holds; the check
happens between documents.

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
  0  u32  format version (2)
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

A term is a key of one of two kinds. An ordinary key is a token of 1 to 64
bytes, case folded. A **long-token key** is 65 bytes: a `0x00` byte, then the
first 64 bytes of a token that is longer than 64 bytes, case folded. A zero
byte never occurs in a token, so the two kinds cannot collide, and the long
keys sort before every ordinary key (they form one run at the start of the
dictionary). The builder writes both kinds; the documents listed under a long
key are those with a token that starts with those 64 bytes, so two different
long tokens can share a key. A key that is neither (empty, over 65 bytes, a
`0x00` start with another length, over 64 bytes without it) is corrupt.

Posting lists are written in dictionary order, one after another, so the offset
of a list is at least the end of the previous list. The reader checks this
wherever it walks entries in order (see "What is checked").

Gaps: `gap = id - previous id - 1`, taking the previous id before the first as
-1, so the first gap is the first id, and a run of consecutive ids is all
zeros.

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
about 4 KiB), pages of skip tables, and the posting blocks a call needs. Apart
from the verdict of each segment's checksum (below), nothing is shared between
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
  `IndexReader::verify` when it loads the file, off the query path. Each
  segment is verified **at most once per reader**: a once-only cell per
  segment holds the verdict. A thread that arrives while another is verifying
  the segment waits for it instead of reading the segment again, and a
  segment that failed keeps failing with the same `Corrupt` message without
  being read again. (A failed *read*, such as an I/O error, is not a verdict
  about the file and is not remembered; the next call tries again.)
- On every read: every varint (bounded to 10 bytes), term length and key kind,
  shared-prefix length, ordering of terms, df, inline ids against the
  segment's id range, every posting offset against the postings area, and,
  where entries are walked in order (a prefix search, and the scan of a block
  that a lookup makes), that a posting list starts at or after the end of the
  previous one (counting at least its skip table and one byte per block): many
  entries cannot point at the same list. The skip table is checked as it is
  read, page by page (ascending ids and starts, inside the list), and every
  block's width, length and last id against its skip entry. A failure is
  `Error::Corrupt` naming what was wrong.

No length, count or offset from a file sizes an allocation until it has been
checked against the file's length or against a constant. A list of `n` ids is
decoded block by block and grows with what was actually read, never
`with_capacity(n)` from an unchecked `n`.

The checksum only guards against damage. A hostile file with valid checksums
meets the structural checks alone; `tests/damage.rs` exercises both.

### Memory of an open reader

The directory and each segment's sparse index: about **0.8 MB per million
terms** (summed over segments), measured with a counting allocator on an index
of 230,000 distinct terms in one segment and 250,000 in 31 segments (about 32 bytes per
block of 32 terms: the first term, a length and an offset). Plus 72 bytes of
directory and a few hundred bytes of bookkeeping per segment. Terms of 64
bytes cost up to 2.5 times that.

A search holds, beyond its output, one dictionary block (at most about 4 KiB)
and, **for each posting list it has open, at most about 2 KiB however long the
list is**: a page of its skip table (64 entries and the one before and after,
528 bytes, read when needed; a seek binary-searches over pages, reading 4
bytes per probe, then inside one page), the decoded block (128 ids, 512 bytes)
and the block's bytes (at most 513). A search with a single list open (`term`,
a prefix with one matching term in a segment, an `intersect` of one token)
also reads ahead: an 8 KiB buffer, once. With several lists open (a prefix
with many matches, an `intersect` of several tokens) nothing reads ahead, so
the total is the number of open lists times 2 KiB and does not depend on the
length of any list. `tests/regressions_review_memory.rs` and
`tests/bounded_memory.rs` measure it (about 1.6 KB per list in a
prefix search over 1,000 terms; 2.7 KB in all for an intersection of two long
lists, over 48,000 or 1,200,000 documents alike).

### Words longer than 64 bytes

A SHA-512 digest, a long key or an id is one token of more than 64 bytes. It
is not lost: the builder lists it under the long-token key (see "Dictionary").
`IndexReader::term` for such a word returns the documents under that key as
`Matches { exact: false }`, because two different long tokens can share their
first 64 bytes. The caller confirms each candidate with
`contains_token(text, token)`. Prefix searches see these tokens and are exact.
Phrases that contain them work the same way, with `phrase_matches` as the
confirmation. Files written by version 1 of the format are refused: a search
in one would silently miss every long token.

### Searching

- `term(token) -> Matches { ids, exact }`: ascending ids across all segments.
  A token of 1 to 64 bytes must be in the tokeniser's output form (lowercase);
  the answer is exact (`exact: true`). A token of more than 64 bytes is looked
  up under its long-token key (its first 64 bytes, case folded) and the answer
  is **candidates** (`exact: false`): another token with the same first 64
  bytes is listed too. The caller confirms each with
  `contains_token(text, token)`. An empty token, or an ordinary token that
  starts with a zero byte, matches nothing.
- `prefix(prefix, limit_terms)`: the union over every term that starts with the
  prefix, long tokens included (the zero-byte namespace is walked with the same
  prefix; a prefix is at most 64 bytes, so these are exact), merged from the
  posting lists with a heap (memory: the output plus what each open list
  holds, above). A prefix of more than 64 bytes is `InvalidArgument`. If more
  than `limit_terms` distinct keys match (a long token counts once per
  distinct first 64 bytes), nothing is read and the call fails with
  `InvalidArgument`.
- `intersect(tokens) -> Matches`: documents that contain every token. The
  shortest list drives; the others jump with their skip tables, so a rare token
  next to a common one reads only the blocks it needs (tested by counting
  blocks read). `exact` is false if any token is longer than 64 bytes.
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
  `Leaf::needs_confirmation()` says which leaves the index alone does not
  answer exactly: a `Phrase`, and a `Term` of more than 64 bytes. The parser
  accepts such words.
- For a leaf: `IndexReader::term` (for a word over 64 bytes, then
  `contains_token(text, token)` on each candidate's text), `IndexReader::prefix`
  (exact), and for a phrase `phrase_candidates(reader, tokens)` (documents
  that hold every token, a long token contributing the documents that share
  its first 64 bytes; a phrase made only of long tokens works) then
  `phrase_matches(text, tokens)` on each candidate's text (the tokens occur
  consecutively in the token stream; a token longer than 64 bytes occupies a
  position and matches only an equal token). `phrase_candidates` reports
  `exact: true` only for a phrase of one token of up to 64 bytes.
- `evaluate(&Query, universe, leaf)`: combines the ascending id lists the
  caller returns for each leaf. `Not` is the universe minus the operand; an
  `And` of only negations starts from the universe. Lists must be strictly
  ascending (`InvalidArgument` otherwise). The nesting depth of a hand-built
  query is limited to 128 levels of nodes, except that an `And` and a `Not`
  that is its direct operand count as one level (the `And` evaluates the
  operand of the `Not` itself). A parsed query is at most about 66 levels.

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

A group holds at most 128 keys and 32 KiB of lists. The writer closes the
current group **before** adding a list that would take it past 32 KiB; that
list starts the next group, and a list that is over 32 KiB by itself is a group
of its own. So a read of a small list never fetches or checksums a large
neighbour; it fetches one group of at most 32 KiB plus its header (a large
list pays only for itself). The rule is the writer's alone: the layout and
what a reader accepts are unchanged, so the format version stays 1 and files
written before the rule are still read. The group's checksum is verified every
time one of its keys is read.

The writer holds one group (up to 32 KiB, or one encoded list if that is
larger) plus the list being added, and **16 bytes per group** already written
(a `u32` key and a `u64` offset, held until `finish`); the reader holds 12
bytes per group (a `u32` and a `u64` in two vectors, at most 12 bytes per 128
keys).

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
  allocator that aborts on any single allocation above 64 MiB. With the
  checksums repaired the answers must be `Corrupt` or internally consistent
  (ascending ids inside the segments' document range), and the total heap held
  during a call stays bounded.
- `tests/build_memory.rs`: the memory bound of the builder and of an open
  reader, with a counting allocator.
- `tests/bounded_memory.rs`: what a search holds per open list does not depend
  on the length of the list; the cap on read-ahead.
- `tests/reads.rs`: bytes read per search and per id-list `get`, through a
  counting `ReadAt`; a damaged segment is read once; many threads on a first
  search verify once.
- `tests/hostile.rs`: hand-built files with valid checksums.
- `tests/long_tokens.rs`: words longer than 64 bytes, byte-level terms that are
  not UTF-8, document ids near `u32::MAX`, a writer that fails midway.
- `tests/regressions_review_*.rs`: the failing tests of an independent review
  of this crate (long words, memory of a search and of the builder, bytes
  read).
- `tests/size.rs`: the index over a few megabytes of English-like text.
- `tests/idlist.rs`: round trips and group boundaries of the id-list store.

Tests that need many segments cheaply use the hidden
`IndexBuilder::with_max_segment_docs`, because the 1 MiB minimum budget would
need a lot of data.
