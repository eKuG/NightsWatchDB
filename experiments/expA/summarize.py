"""Collate the official experiment A results into plain tables.
Usage: python3 -I summarize.py results/d2-cpu2-mem4g results/d1-cpu2-mem4g"""
import json
import sys
from pathlib import Path

MB = 1e6


def jload(p):
    try:
        t = Path(p).read_text().strip()
        if not t:
            return None
        try:
            return json.loads(t)  # pretty-printed, one object
        except ValueError:
            return json.loads(t.splitlines()[-1])  # one object per line, take the last
    except (OSError, ValueError, IndexError):
        return None


def jlines(p):
    out = {}
    try:
        for line in Path(p).read_text().splitlines():
            line = line.strip()
            if line.startswith("{"):
                d = json.loads(line)
                out[d.get("task", "?")] = d
    except OSError:
        pass
    return out


def f(x, nd=2):
    if x is None:
        return "-"
    if isinstance(x, str):
        return x
    return f"{x:.{nd}f}"


def lat(d, kind="p50"):
    """p50/p99 for lookup tasks; for run-list tasks 'first' is the first run."""
    if not d or d.get("failed"):
        return None
    if kind == "first":
        r = d.get("runs_ms") or d.get("server_runs_ms")
        return r[0] if r else None
    return d.get(f"{kind}_ms")


def ch_lat(d, kind="p50"):
    if not d:
        return None
    if kind == "first":
        r = d.get("server_runs_ms")
        return r[0] if r else None
    return d.get(f"server_{kind}_ms")


def table(title, header, rows):
    print(f"\n### {title}\n")
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join(["---"] * len(header)) + "|")
    for r in rows:
        print("| " + " | ".join(r) + " |")


def d2(out):
    out = Path(out)
    variants = ("cotrace", "svc", "traceid")
    print(f"\n## Narrow dataset: {out.name}")
    inp = jload(out / "input-summary.json") or {}
    print(f"\ninput: {inp.get('spans')} spans, {inp.get('traces')} traces, {inp.get('encoded_bytes')} bytes")

    load = jload(out / "ch-load.json") or {}
    sizes = jload(out / "ch-sizes.json") or {}
    rows = []
    for v in variants:
        b = jload(out / f"build-{v}.json") or {}
        disk = sum((b.get("bytes_on_disk") or {}).values()) if isinstance(b.get("bytes_on_disk"), dict) else None
        cpu = (b.get("user_cpu_s") or 0) + (b.get("sys_cpu_s") or 0) if b else None
        rows.append([v, f(disk / MB if disk else None, 1), f(b.get("wall_s"), 1), f(cpu, 1),
                     f((b.get("peak_rss_bytes") or 0) / MB, 0), json.dumps(b.get("bytes_on_disk"))])
    chdisk = sum(t["bytes_on_disk"] for t in sizes.get("tables", [])) if sizes else None
    c = load.get("cpu_s", {})
    chcpu = sum(c.get(k, {}).get("user", 0) + c.get(k, {}).get("sys", 0) for k in ("clickhouse", "collector")) if c else None
    rows.append(["ClickHouse (+collector)", f(chdisk / MB if chdisk else None, 1), f(load.get("wall_s"), 1), f(chcpu, 1), "-",
                 json.dumps({t["table"]: t["bytes_on_disk"] for t in sizes.get("tables", [])})])
    table("Build / load and size", ["variant", "disk MB", "wall s", "CPU s", "peak RSS MB", "files"], rows)

    for temp in ("cold", "warm"):
        ch = jlines(out / f"ch-{temp}.jsonl")
        rows = []
        for q in ("open", "list1", "list2", "agg"):
            if q == "open":
                for kind in ("p50", "p99", "max"):
                    rows.append([f"{q} {kind} ms"] + [f(lat(jload(out / f"{temp}-{v}-{q}.json"), kind), 3) for v in variants]
                                + [f(ch_lat(ch.get("ch_open"), kind), 3)])
            else:
                kinds = ("first", "p50") if temp == "cold" else ("p50", "p99")
                for kind in kinds:
                    rows.append([f"{q} {kind} ms"] + [f(lat(jload(out / f"{temp}-{v}-{q}.json"), kind), 3) for v in variants]
                                + [f(ch_lat(ch.get(f"ch_{q}"), kind), 3)])
        table(f"Query latency, {temp} cache", ["query"] + list(variants) + ["ClickHouse (server)"], rows)

    # agreement
    ch = jlines(out / "ch-warm.jsonl")
    rows = []
    for q in ("list1", "list2"):
        vals = [(jload(out / f"warm-{v}-{q}.json") or {}).get("result_sha256") for v in variants]
        vals.append((ch.get(f"ch_{q}") or {}).get("result_sha256"))
        rows.append([q, "identical" if len(set(vals)) == 1 and vals[0] else f"DIFFER {vals}"])
    aggs = [json.dumps((jload(out / f"warm-{v}-agg.json") or {}).get("result"), sort_keys=True) for v in variants]
    aggs.append(json.dumps((ch.get("ch_agg") or {}).get("result"), sort_keys=True))
    rows.append(["agg", "identical" if len(set(aggs)) == 1 else "DIFFER"])
    opens = [(jload(out / f"warm-{v}-open.json") or {}).get("rows_returned") for v in variants]
    opens.append((ch.get("ch_open") or {}).get("rows_returned"))
    rows.append(["open rows", "identical" if len(set(opens)) == 1 else f"DIFFER {opens}"])
    table("Do all four agree?", ["query", "result"], rows)


def d1(out):
    out = Path(out)
    variants = ("parquet", "parquet-tuned", "zstdlong", "dedup")
    print(f"\n## Content dataset: {out.name}")
    inp = jload(out / "input-summary.json") or {}
    print(f"\ninput: {inp.get('spans')} spans, {inp.get('traces')} traces, {inp.get('encoded_bytes')} bytes")
    rows = []
    for v in variants:
        b = jload(out / f"build-{v}.json") or {}
        if b.get("failed"):
            rows.append([v, f"FAILED exit {b.get('exit')}"] + ["-"] * 6)
            continue
        bod = b.get("bytes_on_disk") or {}
        disk = bod.get("total") if isinstance(bod, dict) else None
        raw = sum((b.get("raw_payload_bytes") or {}).values()) if isinstance(b.get("raw_payload_bytes"), dict) else None
        cpu = (b.get("user_cpu_s") or 0) + (b.get("sys_cpu_s") or 0)
        rows.append([v, f(disk / MB if disk else None, 1), f(raw / disk if raw and disk else None, 1), f(b.get("input_mb_per_s"), 0),
                     f(b.get("wall_s"), 1), f(cpu, 1), f((b.get("peak_rss_bytes") or 0) / MB, 0),
                     json.dumps(b.get("scope_ratio")) if b.get("scope_ratio") else "-"])
    table("Payload stores: build and size", ["variant", "disk MB", "raw/disk", "build MB/s", "wall s", "CPU s", "peak RSS MB", "scope ratio"], rows)

    for temp in ("cold", "warm"):
        rows = []
        for q in ("open", "span"):
            for kind in ("p50", "p99", "max"):
                rows.append([f"{q} {kind} ms"] + [f(lat(jload(out / f"{temp}-pay-{v}-{q}.json"), kind), 3) for v in variants])
        rows.append(["scan MB/s"] + [f((jload(out / f"{temp}-pay-{v}-scan.json") or {}).get("mb_per_s"), 0) for v in variants])
        rows.append(["mismatches (open)"] + [str((jload(out / f"{temp}-pay-{v}-open.json") or {}).get("mismatches")) for v in variants])
        rows.append(["peak RSS MB (open)"] + [f(((jload(out / f"{temp}-pay-{v}-open.json") or {}).get("peak_rss_bytes") or 0) / MB, 0) for v in variants])
        table(f"Payload stores: read latency, {temp} cache", ["measure"] + list(variants), rows)

    load = jload(out / "ch-load.json") or {}
    sizes = jload(out / "ch-sizes.json") or {}
    chdisk = sum(t["bytes_on_disk"] for t in sizes.get("tables", [])) if sizes else None
    c = load.get("cpu_s", {})
    chcpu = sum(c.get(k, {}).get("user", 0) + c.get(k, {}).get("sys", 0) for k in ("clickhouse", "collector")) if c else None
    lb = jload(out / "build-cotrace.json") or {}
    ldisk = sum((lb.get("bytes_on_disk") or {}).values()) if isinstance(lb.get("bytes_on_disk"), dict) else None
    print(f"\nClickHouse content load: match={load.get('match')} rows={load.get('rows_in_clickhouse')} wall_s={load.get('wall_s')} "
          f"cpu_s(clickhouse+collector)={f(chcpu, 1)} disk_MB={f(chdisk / MB if chdisk else None, 1)}")
    print(f"our span tables on this data (cotrace): disk_MB={f(ldisk / MB if ldisk else None, 1)} build_wall_s={lb.get('wall_s')}")

    for temp in ("cold", "warm"):
        ch = jlines(out / f"ch-{temp}.jsonl")
        rows = []
        for q in ("open", "list1", "list2", "agg"):
            kinds = ("p50", "p99") if q == "open" or temp == "warm" else ("first", "p50")
            for kind in kinds:
                rows.append([f"{q} {kind} ms", f(lat(jload(out / f"{temp}-cotrace-{q}.json"), kind), 3), f(ch_lat(ch.get(f"ch_{q}"), kind), 3)])
        rows.append(["open: bytes returned", str((jload(out / f"{temp}-pay-dedup-open.json") or {}).get("bytes_returned")) + " (payload, dedup store)",
                     str((ch.get("ch_open") or {}).get("bytes_returned_uncompressed"))])
        table(f"Span queries on content data, {temp} cache (ours = span rows only; ClickHouse open returns text too)",
              ["query", "cotrace", "ClickHouse (server)"], rows)


for arg in sys.argv[1:]:
    (d2 if "/d2-" in arg or arg.startswith("d2-") or "d2-" in Path(arg).name else d1)(arg)
