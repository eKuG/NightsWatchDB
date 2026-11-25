#!/usr/bin/env python3
"""compare.py SMOKE_DIR REF_DIR : table of smoke runs, check block vs bake reference."""
import json, sys, os
out, ref = sys.argv[1], sys.argv[2]
refs = {d: json.load(open(f"{ref}/ref-{d}-bake.json")) for d in ("content", "small")}
gens = {d: json.load(open(f"{ref}/ref-{d}-gen.json")) for d in ("content", "small")}
print("dataset mode wal | wire_B spans wall_s cpu_s(u+s) cpu_s/GB peakRSS_MB fsyncs groups mean_grp | check")
for d in ("content", "small"):
    for m in ("full", "lazy", "none"):
        for w in ("off", "on"):
            p = f"{out}/{d}-{m}-{w}.stats.json"
            if not os.path.exists(p):
                print(d, m, w, "MISSING"); continue
            s = json.load(open(p)); r = refs[d]
            cpu = s["user_cpu_s"] + s["sys_cpu_s"]
            if m == "none":
                ok = f"n/a (nothing decoded); wire_bytes {'==' if s['wire_bytes'] == r['input_bytes'] - 4 * s['requests'] else '!='} file bytes minus frames"
            else:
                c = s["check"]
                same = (c["rows"] == r["rows"] and c["sum_input_tokens"] == r["sum_input_tokens"] and c["sum_output_tokens"] == r["sum_output_tokens"]
                        and c["nonnull"] == r["nonnull"] and c["payload_bytes"] == r["payload_bytes"] and s["spans"] == gens[d]["spans"] and s["rejected"] == 0)
                ok = "OK" if same else "MISMATCH"
            print(f"{d:8} {m:5} {w:3} | {s['wire_bytes']} {s['spans']} {s['wall_s']:.2f} {cpu:.2f}({s['user_cpu_s']:.2f}+{s['sys_cpu_s']:.2f}) {cpu / (s['wire_bytes'] / 1e9):.2f} {s['peak_rss_bytes'] / 2**20:.0f} {s['wal_fsyncs']} {s['wal_groups']} {s['wal_mean_group']} | {ok}")
