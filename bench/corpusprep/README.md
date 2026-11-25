# corpusprep

Downloads public agent-trajectory datasets and extracts role-separated text for `../tracegen`. Temporary, like
the generator. Downloaded files are only parsed, never executed.

## Commands

    go build -o corpusprep .
    ./corpusprep fetch      # downloads into ../../.data/corpus-raw (default data dir: .data in the repository root)
    ./corpusprep extract    # writes ../../.data/corpus/{system,user,assistant,tool,tooldefs}/shard-NNNN.bin

An optional second argument overrides the data directory. `fetch` checks every file size against the Hub API
and refuses to download if free disk space would fall below 60 GB; `extract` refuses to start below 60 GB.
Requests use the generic User-Agent `bench-fetch/1.0` and no tokens.

## Output format

Each shard is at most 64 MB: a sequence of records, each a 4-byte big-endian length followed by that many bytes
of raw UTF-8 (entries may contain newlines). Identical entries are stored once per role (SHA-256).
`tooldefs` entries are `gen_ai.tool.definitions` JSON values; `tool` entries mix shell/file/test output and JSON
API results. Per role and source byte caps are in `caps` in `extract.go` (about 1.5 GB total).

## Sources and licences

| Dataset | Licence | Used for |
|---|---|---|
| nebius/SWE-rebench-openhands-trajectories | CC-BY-4.0 | system prompts, first user message, assistant text and tool-call arguments, tool output (every third trajectory) |
| Exgentic/agent-llm-traces | CDLA-permissive-2.0 | tool definitions, JSON tool results, system prompts |
| OpenAssistant/oasst1 (`2023-04-12_oasst_ready.messages.jsonl.gz`, English only) | Apache-2.0 | natural-language user and assistant text |

Licence and README files are downloaded next to each dataset in `.data/corpus-raw/`.

Attribution (CC-BY-4.0): the extracted text includes material from "SWE-rebench OpenHands Trajectories" by
Nebius (https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories), licensed under CC BY 4.0
(https://creativecommons.org/licenses/by/4.0/). The text was filtered, de-duplicated and re-packaged into
length-prefixed records; it contains no endorsement by the authors. Repository code that appears inside tool
output keeps its own licences. Do not redistribute the extracted corpus without carrying this attribution and
the Exgentic and oasst1 notices.
