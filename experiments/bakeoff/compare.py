"""Compare the two bake-off result files: first that both sides did the same
work, then how fast and how large. Usage: python3 -I compare.py RESULTS_DIR"""
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])


def load(lang):
    rows = {}
    for line in (out / f"{lang}.jsonl").read_text().splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        r = json.loads(line)
        rows.setdefault(r["task"] if "failed" in r else r.get("_step", r["task"]), []).append(r)
    return rows


go, rust = load("go"), load("rust")


def last(rows, task, pred=lambda r: True):
    c = [r for r in rows.get(task, []) if pred(r) and not r.get("failed")]
    return c[-1] if c else None


def best(rows, task, key, pred=lambda r: True):
    c = [r for r in rows.get(task, []) if pred(r) and not r.get("failed")]
    return min(c, key=lambda r: r[key]) if c else None


problems = []
for lang, rows in (("go", go), ("rust", rust)):
    for task, rs in rows.items():
        for r in rs:
            if r.get("failed"):
                problems.append(f"{lang}: step {task} FAILED (exit {r.get('exit')})")

# Same work?
full = lambda r: bool(r.get("spans_parquet_bytes"))  # one side prints null, the other 0, for decode-only runs
g, r = last(go, "convert", full), last(rust, "convert", full)
if g and r:
    for f in ("rows", "sum_input_tokens", "sum_output_tokens", "nonnull", "payload_bytes", "input_bytes"):
        if g[f] != r[f]:
            problems.append(f"convert.{f} differs: go={g[f]} rust={r[f]}")
g, r = last(go, "agg"), last(rust, "agg")
if g and r:
    if g["result"] != r["result"]:
        problems.append("agg.result differs")
    if g["window"] != r["window"]:
        problems.append(f"agg.window differs: go={g['window']} rust={r['window']}")
g, r = last(go, "trace"), last(rust, "trace")
if g and r:
    for f in ("lookups", "rows_returned", "payload_bytes_returned"):
        if g[f] != r[f]:
            problems.append(f"trace.{f} differs: go={g[f]} rust={r[f]}")
g, r = last(go, "index"), last(rust, "index")
if g and r:
    for f in ("docs", "distinct_tokens", "postings", "index_bytes", "index_sha256", "text_bytes"):
        if g[f] != r[f]:
            problems.append(f"index.{f} differs: go={g[f]} rust={r[f]}")
g, r = last(go, "sample-ids"), last(rust, "sample-ids")
if g and r and (g["distinct_traces"], g["written"]) != (r["distinct_traces"], r["written"]):
    problems.append("sample-ids counts differ")

print("SAME WORK CHECK:", "PASS" if not problems else "PROBLEMS")
for p in problems:
    print("  -", p)

MB = 1e6


def row(label, gv, rv, unit="", better="high"):
    if gv is None or rv is None:
        print(f"{label:38} go={gv} rust={rv}")
        return
    ratio = (gv / rv) if rv else float("nan")
    print(f"{label:38} go={gv:>12.2f}{unit:5} rust={rv:>12.2f}{unit:5} go/rust={ratio:5.2f}  ({better}er is better)")


def val(r, f, scale=1.0):
    return None if r is None or r.get(f) is None else r[f] / scale


print()
dec = lambda r: not r.get("spans_parquet_bytes")
gd, rd = best(go, "convert", "wall_s", dec), best(rust, "convert", "wall_s", dec)
gc_, rc = best(go, "convert", "wall_s", full), best(rust, "convert", "wall_s", full)
row("decode MB/s", val(gd, "input_mb_per_s"), val(rd, "input_mb_per_s"), "", "high")
row("decode peak RSS MB", val(gd, "peak_rss_bytes", MB), val(rd, "peak_rss_bytes", MB), "", "low")
row("convert MB/s", val(gc_, "input_mb_per_s"), val(rc, "input_mb_per_s"), "", "high")
row("convert CPU s (user+sys)", (gc_["user_cpu_s"] + gc_["sys_cpu_s"]) if gc_ else None,
    (rc["user_cpu_s"] + rc["sys_cpu_s"]) if rc else None, "", "low")
row("convert peak RSS MB", val(gc_, "peak_rss_bytes", MB), val(rc, "peak_rss_bytes", MB), "", "low")
row("spans.parquet MB", val(gc_, "spans_parquet_bytes", MB), val(rc, "spans_parquet_bytes", MB), "", "low")
row("payloads.parquet MB", val(gc_, "payloads_parquet_bytes", MB), val(rc, "payloads_parquet_bytes", MB), "", "low")
g, r = last(go, "agg"), last(rust, "agg")
med = lambda x: None if x is None else sorted(x["runs_ms"])[len(x["runs_ms"]) // 2]
row("agg median ms", med(g), med(r), "", "low")
g, r = last(go, "trace"), last(rust, "trace")
row("trace p50 ms", val(g, "p50_ms"), val(r, "p50_ms"), "", "low")
row("trace p99 ms", val(g, "p99_ms"), val(r, "p99_ms"), "", "low")
row("trace peak RSS MB", val(g, "peak_rss_bytes", MB), val(r, "peak_rss_bytes", MB), "", "low")
g, r = last(go, "index"), last(rust, "index")
row("index text MB/s", val(g, "text_mb_per_s"), val(r, "text_mb_per_s"), "", "high")
row("index peak RSS MB", val(g, "peak_rss_bytes", MB), val(r, "peak_rss_bytes", MB), "", "low")
g, r = last(go, "mixed"), last(rust, "mixed")
row("mixed ingest MB/s", val(g, "ingest_mb_per_s"), val(r, "ingest_mb_per_s"), "", "high")
row("mixed agg p99 ms", val(g, "agg_p99_ms"), val(r, "agg_p99_ms"), "", "low")
row("mixed trace p99 ms", val(g, "trace_p99_ms"), val(r, "trace_p99_ms"), "", "low")
row("mixed trace max ms", val(g, "trace_max_ms"), val(r, "trace_max_ms"), "", "low")
row("mixed peak RSS MB", val(g, "peak_rss_bytes", MB), val(r, "peak_rss_bytes", MB), "", "low")
if g:
    print(f"go GC during mixed: count={g.get('gc_count')} total_pause_ms={g.get('gc_pause_total_ms')} max_pause_ms={g.get('gc_pause_max_ms')}")
print()
print((out / "files.txt").read_text() if (out / "files.txt").exists() else "no files.txt")
