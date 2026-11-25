# Experiment B: our own text index, per span or per distinct message

Decision already made: the database builds its own inverted index for
full-text search over prompts, completions and tool input and output, with no
search library. This experiment answers what that costs and how to shape it:

1. How large is the index, how fast does it build, how much memory?
2. Experiment A showed that 92 to 97% of the text in an agent trace repeats
   earlier calls, and that a payload store which keeps each distinct piece of
   text once (the `dedup` store) is the best store. Should the index have one
   document per **span** (the obvious design) or one per **distinct piece**
   (index each message once, then map pieces back to spans)?
3. Is search within a factor of two of ClickHouse, with and without its text
   index?

Throwaway experiment code: small, direct, correct. The official measurements
are taken later by one neutral harness that runs each piece alone under fixed
container limits; you deliver correct code with exactly this command-line
surface plus a smoke run.

Two work streams, each in its own directory; do not edit the other's.

    experiments/expB/textindex/     Rust crate `textindex`
    experiments/expA/chbaseline/    ClickHouse search scripts (extends the existing baseline)

## Shared facts

- The text lives in a `dedup` payload store written by
  `experiments/expA/payload` (Rust). Its file formats (`dd.blocks`, `dd.refs`,
  `dd.idx`) are described in `experiments/expA/payload/README.md`, and its reader
  is in `experiments/expA/payload/src/dd.rs`. Rust code may copy from that crate
  (copy, do not depend on it, do not edit it). The six text attributes of a
  span, in table order: input_messages, output_messages, system_instructions,
  tool_definitions, tool_call_arguments, tool_call_result. A value is either
  absent, one whole piece, or an array rebuilt as `[` + pieces joined by `,` +
  `]`. Piece ids are global and dense. Rows are spans in arrival order.
- The official dataset is the 8 GB content dataset already stored in the
  Docker volume `expA-official` at `/data/d1-pay-dedup` (91,419 rows, 5,014
  traces, 169,843 pieces, 310 MB of distinct piece text, 8.0 GB of text when
  every span is rebuilt). Mount that volume read-only if you use it; never
  write to it.
- **Tokeniser, exactly** (same as the bake-off): map ASCII A-Z to a-z; a token
  is a maximal run of bytes that are a-z, 0-9, or >= 0x80; every other byte
  separates tokens; discard tokens longer than 64 bytes.
- **A span's text** is its six attribute values in table order, each rebuilt
  exactly, with one 0x00 byte between attributes. Because `[`, `,`, `]` and
  0x00 are separators, the tokens of a span are exactly the union of the
  tokens of its pieces. So "rows whose text contains token t" is the same set
  whether computed per span or per piece and mapped back.
- **Result unit.** Every query result is the ascending list of matching row
  ids (spans). Report `rows` (count), `traces` (distinct traces among them)
  and `rows_sha256` (SHA-256 of the row ids as u32 little-endian, ascending).
- Rust 1.100, release profile `lto = "thin"`, `codegen-units = 1`, no async
  runtime, no `unsafe` of your own. Allowed crates: `zstd` 0.14.5,
  `twox-hash`, `sha2`, `crossbeam-channel`; no search, FST, roaring or
  bit-packing crates: this experiment is about writing the index ourselves.
  Multi-stage `Dockerfile` with a dependency-caching layer
  (`rust:1.100.0-bookworm` then `debian:bookworm-slim`), binary as entry point.
- Every subcommand prints one JSON object per line on stdout; the last line is
  a summary with at least `task`, `variant`, `wall_s`, `user_cpu_s`,
  `sys_cpu_s`, `peak_rss_bytes` (VmHWM), `threads`. Diagnostics go to stderr.
  Latency lists are reported as `p50_ms`, `p90_ms`, `p99_ms`, `max_ms` by
  nearest rank. "MB" means 1e6 bytes.
- **Memory by bytes.** Never size a buffer by a count of rows or documents
  alone; single values reach 1 MiB and rebuilt span texts reach several MB.
  The official build runs under a 4 GB limit and should stay far below it.
- **Data discipline, strictly.** Generated data and outputs live only in
  Docker named volumes prefixed with your stream name (`expB-ti-`,
  `expB-ch-`). Never write generated data to the host; never bind-mount a host
  directory that holds large generated files. For your own smoke runs build a
  dedup store from at most 1 GB of generated content data (generator image:
  `experiments/bakeoff/tracegen.Dockerfile`, context `bench/tracegen`, corpus
  `.data/corpus` (in the repository root) mounted read-only;
  payload image: `experiments/expA/payload`). Containers run with
  `--cpus 2 --memory 4g`. Remove your volumes, containers and images when
  finished; touch no other Docker resource.
- No `git init`, commits or pushes. Plain code comments. Do not edit files
  outside your stream's directory.

## Stream 1: the index (`experiments/expB/textindex/`)

`textindex build --store DIR --out DIR --variant piece|span [--docs-per-segment N]`

- `piece`: one document per distinct piece; document id = piece id; text = the
  piece bytes. Also write `piece_rows`: for every piece, the ascending list of
  rows that reference it (from `dd.refs`), in a compact form of your choice.
  Its size counts toward the index.
- `span`: one document per row; document id = row id; text = the span's text
  as defined above (rebuilt from the store).
- The index is split into **index segments**, each covering a contiguous
  range of document ids, to model one index per data segment. Default
  `--docs-per-segment`: 32,768 for `piece`, 8,192 for `span`. A segment is
  also closed early when the memory held for it passes a byte budget
  (default 512 MB). Build segments one at a time (or a bounded few in
  parallel) so memory stays bounded.
- Index format, stage one. Per segment one file containing:
  - a term dictionary: terms in ascending byte order, in blocks of a fixed
    number of terms, front-coded within a block (shared-prefix length with the
    previous term, then the suffix), plus a sparse index holding the first
    term and file offset of every block;
  - for each term its document count and its posting list of ascending
    document ids: lists of at most 8 documents are stored inline in the
    dictionary entry; longer lists are stored in a postings area as blocks of
    128 ids, delta-encoded and bit-packed with one bit width per block, with
    skip data (last id and offset of every block) so a reader can jump;
  - no positions, no frequencies, no scores;
  - a footer with a magic value and a format version byte (positions and
    scoring are planned later), counts and offsets.
  The exact byte layout is yours; document it in a README.
- Fields: `docs`, `segments`, `text_bytes` (bytes tokenised), `distinct_tokens`
  (sum over segments and, separately, global distinct), `postings`,
  `index_bytes` (object per file kind, and total), `text_mb_per_s`.

`textindex queries --index DIR --store DIR --out FILE [--seed 1]`

Derive a fixed query set from the `piece` index, so both variants and
ClickHouse run the same queries. Candidate terms are tokens of 4 to 20 bytes
made only of a-z. `df` is the number of pieces containing the term, over the
whole dataset. To pick k terms near a target df: sort candidates by
(|df - target|, term) and take the first k.

| class | definition | how many |
|---|---|---|
| rare | one term, df target 2 | 10 |
| mid | one term, df target 0.1% of pieces | 10 |
| common | one term, df target 5% of pieces | 10 |
| and2 | mid[i] AND common[i] | 10 |
| and3 | mid[i] AND common[i] AND common[(i+1) mod 10] | 10 |
| or2 | rare[i] OR mid[i] | 10 |
| not | common[i] AND NOT mid[i] | 10 |
| prefix | the first 4 bytes of mid[i]; matches every token starting with them | 10 |
| phrase | two tokens that occur consecutively in some piece | 10 |

For `phrase`: with a seeded generator pick pieces and a position, take two
consecutive tokens that are both candidate terms, until 10 distinct pairs are
found. A document matches a phrase when the two tokens are consecutive in its
token stream (any separator bytes between them; tokens over 64 bytes still
count as tokens for adjacency even though they are not indexed).
Output: a JSON file `{"queries":[{"id":"rare-0","class":"rare","terms":[..]},
{"id":"prefix-0","class":"prefix","prefix":".."},
{"id":"phrase-0","class":"phrase","phrase":["a","b"]}, ...]}`. For and/or/not
classes `terms` are in the order written in the table.

`textindex search --index DIR --store DIR --variant V --queries FILE [--runs 5]`

Open everything once, then for every query and run: find the matching
documents in every index segment, and turn them into rows (for `piece`,
through `piece_rows`; a row matches if any of its pieces matches... but note
the boolean classes: AND, OR and NOT are defined over a **row's** token set,
not a piece's, so for `piece` evaluate each term to its row set first and
combine row sets). For `phrase`, find candidate documents containing both
tokens, then read their text from the store and check adjacency; in the
`piece` variant a piece is verified once however many rows reference it.
Print one line per query: `id`, `class`, `rows`, `traces`, `rows_sha256`,
`runs_ms` (per run, total time to the row list), `match_ms` (time to matching
document ids, median), and for phrase `candidates` and `verified_bytes`. The
summary line has `p50_ms` per class.

`textindex scan --store DIR --queries FILE`

Ground truth without any index and without `piece_rows`: one pass over all
rows, rebuilding each span's text, tokenising it, and evaluating every query
against that row. Print one line per query (`id`, `rows`, `traces`,
`rows_sha256`) and a summary with `scan_s` and `text_mb_per_s`. This is slow
by design (it tokenises all 8 GB); it is the answer key and the cost of having
no index.

Correctness: for every query, `rows_sha256` from `search` (both variants) must
equal `scan`. Provide a `check` subcommand or a small script that runs the
three and prints one line per query with `ok` or the mismatch, and a count.

Unit tests: tokeniser (the worked example in the bake-off spec, bytes >= 0x80,
64 and 65 byte tokens); dictionary front-coding round trip including shared
prefixes of 0 and of the full previous term; bit-packed posting blocks for
widths 0, 1, 7, 8, 17, 32 and list lengths 1, 8, 9, 128, 129, 1000; skip data
used by intersection; AND/OR/NOT/prefix/phrase against a brute-force answer on
a small synthetic store for both variants.

## Stream 2: ClickHouse search baseline (`experiments/expA/chbaseline/`, new files only)

Add `search.sh DATASET untuned|tuned --queries FILE [--runs 5]` and whatever
SQL it needs under `queries/search/`. It runs the same query file against the
content dataset already loaded by `load.sh` and prints one JSON line per query:
`id`, `class`, `rows`, `traces`, `server_runs_ms`, `server_p50_ms`, plus a
summary with p50 per class.

- `untuned`: the exporter's table as it is. A row matches when the query holds
  over the tokens of the six content attributes in `SpanAttributes`
  (`gen_ai.input.messages`, `gen_ai.output.messages`,
  `gen_ai.system_instructions`, `gen_ai.tool.definitions`,
  `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`), case-insensitively.
  Use the best plain functions for token, prefix and phrase matching and say
  what you chose.
- `tuned`: what a competent ClickHouse user would do for search. In a
  **separate table** `otel.otel_traces_text` (so the baseline table and its
  measured size stay untouched), keep the columns needed for the result plus
  one text column holding the six values, and put ClickHouse's text index on
  it with a tokeniser and case folding as close as possible to the tokeniser
  above. Read the documentation for the pinned server version for the index
  syntax and the functions that use it; they have changed across versions.
  Report the time and CPU to build it, the size of the table and of the index,
  and show with `EXPLAIN indexes = 1` that each query class uses the index (or
  say plainly which classes cannot).
- Query terms are all a-z, 4 to 20 bytes, so tokenisation differences should
  be rare. Where a ClickHouse count differs from the reference count supplied
  in the query file's companion results, report the query and the reason
  (for example bytes >= 0x80 treated differently, or phrase semantics).
- `tuned` must not change the baseline table; provide a way to drop the extra
  table.

## Report back (both streams)

Files with line counts; exact versions; commands run and their real output;
the file format you chose; anything in this document you could not follow and
what you did instead; anything you could not verify. State shortfalls plainly.
