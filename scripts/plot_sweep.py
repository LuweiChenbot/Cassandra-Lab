#!/usr/bin/env python3
"""
Plot violation rate against injected delay from a latency sweep.

Usage: ./.venv/bin/python scripts/plot_sweep.py [model] [--csv PATH] [--out PATH]
  e.g. ./.venv/bin/python scripts/plot_sweep.py ryw --csv results/ryw_mac.csv

Reads the CSV (default results/<model>.csv), keeps the sweep rows (normal
scenario, natural mode, jitter 0), uses the last run of each (delay, W, R)
point, and writes a PNG (default results/<model>_delay_curves.png) with a
0-25 ms panel and a full-range panel.
"""
import argparse
import csv
import os
import sys
from collections import OrderedDict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# CJK-capable fonts (Windows names); only needed if labels contain Chinese
matplotlib.rcParams["font.sans-serif"] = [
    "Microsoft Yahei", "SimHei"]
matplotlib.rcParams["axes.unicode_minus"] = False

_ap = argparse.ArgumentParser(description="Plot violation rate vs injected delay.")
_ap.add_argument("model", nargs="?", default="ryw", help="ryw, mr, mw or wfr (default: ryw)")
_ap.add_argument("--csv", help="input CSV (default: results/<model>.csv)")
_ap.add_argument("--out", help="output PNG (default: results/<model>_delay_curves.png)")
_args = _ap.parse_args()

MODEL = _args.model
SRC = _args.csv or f"results/{MODEL}.csv"
OUT = _args.out or f"results/{MODEL}_delay_curves.png"

if not os.path.exists(SRC):
    sys.exit(f"找不到 {SRC} —— 先跑 ./scripts/sweep_latency.sh {MODEL}")

rows = list(csv.DictReader(open(SRC, newline="")))


def is_sweep_row(r):
    """Sweep rows: jitter 0, normal scenario, natural mode."""
    try:
        if float(r.get("jitter_ms") or -1) != 0.0:
            return False
        float(r["delay_ms"])
    except (ValueError, TypeError):
        return False
    return r.get("scenario") == "normal" and r.get("mode") in ("natural", "")


# if a (delay, W, R) point was run more than once, the last run wins
points = OrderedDict()
errors_seen = {}
for r in rows:
    if not is_sweep_row(r):
        continue
    key = (r["write_cl"], r["read_cl"])
    points.setdefault(key, {})[float(r["delay_ms"])] = float(r["violation_rate"]) * 100
    if int(r.get("errors") or 0) > 0:
        errors_seen[key] = errors_seen.get(key, 0) + int(r["errors"])

# n in the title comes from the last sweep row, matching the last-run-wins rule
_sweep_rows = [r for r in rows if is_sweep_row(r)]
N_ITER = _sweep_rows[-1]["iterations"] if _sweep_rows else "?"

if not points:
    sys.exit(f"{SRC} 里没有扫描数据(jitter=0 的行) —— 先跑 "
             f"./scripts/sweep_latency.sh {MODEL}")

# two panels: most of the change happens below ~20 ms, but the sweep runs to 800 ms
ZOOM_MAX = 25.0
fig, (ax_zoom, ax_full) = plt.subplots(1, 2, figsize=(13, 5.2))

for ax, xmax, title in (
        (ax_zoom, ZOOM_MAX, f"Focus on low latency areas (0–{int(ZOOM_MAX)} ms)"),
        (ax_full, None, "Full range (0–800 ms)")):
    for (w, r), series in points.items():
        xs = sorted(x for x in series if xmax is None or x <= xmax)
        if not xs:
            continue
        ys = [series[x] for x in xs]
        label = f"W={w}  R={r}"
        if (w, r) in errors_seen:
            label += f"  (errors: {errors_seen[(w, r)]})"
        ax.plot(xs, ys, marker="o", linewidth=2, markersize=5, label=label)
    ax.set_xlabel("Delay (ms)", fontsize=11)
    ax.set_ylim(-3, 103)
    ax.set_title(title, fontsize=12)
    ax.grid(True, alpha=0.3)

ax_zoom.set_ylabel(f"{MODEL.upper()} violation rate (%)", fontsize=11)
ax_full.legend(fontsize=9, loc="center right")
fig.suptitle(
    f"{MODEL.upper()} violation rate vs replication delay (3 nodes, RF=3, jitter=0, n={N_ITER})",
    fontsize=13.5)
fig.tight_layout(rect=[0, 0, 1, 0.94])
fig.savefig(OUT, dpi=150)

n_pts = sum(len(s) for s in points.values())
print(f"已保存 -> {OUT}  ({len(points)} 条曲线, {n_pts} 个数据点)")
for (w, r), series in points.items():
    pts = "  ".join(f"{int(x)}ms:{series[x]:.0f}%" for x in sorted(series))
    print(f"  W={w:7} R={r:7}  {pts}")
