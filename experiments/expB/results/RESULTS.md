# Experiment B result: index each distinct message once

Run 2026-11-24/25 with `official.sh ours` and `official.sh clickhouse` on the
8 GB content dataset (91,419 spans, 5,014 traces, 169,843 distinct pieces of
text, 310 MB distinct, 8.0 GB when every span is rebuilt). Docker Desktop on an
Apple M4 Pro, each step alone at 2 CPUs and 4 GB. ClickHouse 26.10.12.13.

## Correctness

90 fixed queries in 9 classes. Row counts and trace counts are identical
across five implementations: our brute-force scan, our index per span, our
index per piece (both also identical to the scan by SHA-256 of the row ids),
ClickHouse without an index, and ClickHouse with its text index (with and
without positions). Unit tests: 11 of 11 pass inside a Linux container.

## Size and build cost

| | Index size | Build CPU s | Build memory |
|---|---|---|---|
| Ours, one document per distinct piece | 24.0 MB | 1.9 | 306 MB |
| Ours, one document per span | 126.4 MB | 32.5 | 336 MB |
| ClickHouse text index, no positions | 118.9 MB | 59.2 | 471 MB (insert peak) |
| ClickHouse text index, with positions (experimental) | 1,448.8 MB | 90.4 | 467 MB |

The per-piece index is 7.7% of the distinct text and 0.3% of the raw text. It
includes 4.0 MB for the map from pieces back to spans. ClickHouse's figures
exclude the 512 MB second copy of the text its search table needs.

## Search latency, median across the 10 queries of each class (ms)

| Class | Ours, per piece | Ours, per span | ClickHouse, text index | ClickHouse, no index (first run) |
|---|---|---|---|---|
| rare word | 0.004 | 0.007 | 1.2 | 1,057 |
| mid-frequency word | 0.013 | 0.019 | 1.8 | 1,077 |
| common word | 0.28 | 0.07 | 2.2 | 982 |
| AND of 2 | 0.25 | 0.14 | 1.8 | 1,156 |
| AND of 3 | 0.51 | 0.21 | 1.9 | 1,192 |
| OR | 0.017 | 0.025 | 1.8 | 1,353 |
| AND NOT | 0.28 | 0.12 | 2.7 | 1,374 |
| prefix | 0.08 | 0.16 | 3.0 | 4,402 |

Ours is timed in-process with files open; ClickHouse is server-side time for a
full SQL query. Our brute-force scan with no index takes 21 s per pass.

## Phrase search

| | Median of 10 | Selective phrases (7 of 10) | Very common phrases (3 of 10) |
|---|---|---|---|
| Ours, per piece, no positions stored | 14 ms | 2.7 to 139 ms | 206 to 371 ms |
| Ours, per span, no positions stored | 1,163 ms | 204 to 6,037 ms | 492 to 4,174 ms |
| ClickHouse, text index without positions | 1,668 ms | 309 to 3,970 ms | 1,668 to 3,726 ms |
| ClickHouse, text index with positions | about 2 ms | 2 to 8 ms | 1.3 s, 2.3 s, and one query that raised an exception |

The last row was re-measured by hand with `hasPhrase` alone, three runs per
phrase, because the scripted query added a regular-expression check (to stop a
phrase matching across two attributes) that made common phrases look far
slower than the index really is. The exceptions were not investigated.

## Reading

- **Index pieces, not spans.** Five times smaller, seventeen times cheaper to
  build, and phrase search about eighty times faster, because a piece is
  verified once however many spans repeat it.
- **Fast phrase search without storing positions.** Skipping positions was
  chosen to keep the index small; indexing pieces removes most of the usual
  cost of that choice.
- **Against ClickHouse.** Every non-phrase class is faster than ClickHouse's
  text index, with an index a fifth the size. For phrases we beat its
  generally available configuration by about a hundred times. Its
  experimental positional index is faster than ours on selective phrases
  (about 2 ms against 14 ms), at sixty times the index size, and slower or
  failing on very common phrases.
- **Search results should be reported per message or trace.** A common word
  matched 41,000 of 91,000 spans but only 3,400 traces, because every later
  span repeats the earlier text.

## Pass marks

| Pass mark | Verdict |
|---|---|
| Every query returns the same rows as the scan | pass (90 of 90, both variants) |
| Index size and build speed known | pass |
| Search within 2x of ClickHouse | pass for 8 of 9 classes; phrase passes against the generally available index and misses against the experimental positional one on selective phrases |

## Caveats

Docker on a Mac, warm cache, one dataset, single-threaded code on our side,
queries timed one at a time. Sub-millisecond figures are in-process timings
and a real server will add overhead. Query terms are plain a-z words of 4 to
20 letters. No positions, no relevance ranking, no wildcard or fuzzy search
were built.
