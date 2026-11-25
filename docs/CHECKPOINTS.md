# Checkpoints

The engine is built in small steps. Each checkpoint is a tested increment:
its tests pass in a Linux container, it is committed, tagged
`checkpoint-NN`, and pushed.

| # | Checkpoint | State |
|---|---|---|
| 0 | Repository set-up: licence, design, experiments and measured results | done |
| 1 | Storage core as libraries: shared types, write-ahead log, span parts (segment files, trace index, summary), manifest | next |
| 2 | Engine: memory buffer, flush, recovery from the log, snapshot reads; open a trace, list traces, aggregate | |
| 3 | Server: OTLP door for traces, HTTP query API, container image, end-to-end test with the generator | |
| 4 | Large text out of line, each distinct piece stored once | |
| 5 | Text index over distinct pieces; search | |
| 6 | Compaction, retention, late spans | |
| 7 | Scores and annotations | |
| 8 | Logs | |
| 9 | Metrics | |
| 10 | SQL and a Postgres-compatible endpoint | |
| 11 | OTel-Arrow door | |
| 12 | Semantic-convention mapping | |
| 13 | Object-store backend | |
| 14 | Entity graph | |
| 15 | Profiles | |

Checkpoint 1 so far: the Cargo workspace and `nwdb-core` exist.

Benchmarks against the baseline are re-run when a checkpoint changes the
write or read path, and added to [benchmarks](benchmarks/README.md).
