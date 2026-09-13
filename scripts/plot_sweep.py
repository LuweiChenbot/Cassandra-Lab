#!/usr/bin/env python3
import csv
from collections import defaultdict
import matplotlib.pyplot as plt

rows = list(csv.DictReader(open("results/sweep_ryw.csv")))

series = defaultdict(list)
for row in rows:
    if not row["violation_rate"]:
        continue
    key = f"W={row['write_cl']} R={row['read_cl']}"
    series[key].append((float(row["delay_ms"]), float(row["violation_rate"])))

plt.figure(figsize=(8, 5))
for key, pts in series.items():
    pts.sort()
    xs, ys = zip(*pts)
    plt.plot(xs, ys, marker="o", label=key)

plt.xlabel("注入延迟 (ms)")
plt.ylabel("RYW 违例率 (%)")
plt.title("RYW 违例率 vs 复制延迟")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("results/ryw_delay_curves.png", dpi=150)
print("已保存 -> results/ryw_delay_curves.png")