#!/usr/bin/env python3
"""
plot_sweep.py -- 把延迟扫描结果画成违例率曲线。

    用法: ./.venv/bin/python scripts/plot_sweep.py [模型]
    例:   ./.venv/bin/python scripts/plot_sweep.py ryw

数据来源
--------
直接读 results/<模型>.csv, 不再解析实验脚本的终端输出。
CSV 里已有 delay_ms 列(取自 tc 实测值), 正则解析 stdout 那层耦合就没必要了 ——
输出格式一改, 正则就会静默匹配失败, 得到一批空值。

只取扫描产生的行
----------------
正式矩阵那轮跑的是 200ms ± 50ms(带抖动, 贴近真实网络), 扫描跑的是
jitter=0(确定性标量横轴)。两者目的不同, 混在一张图里会让 200ms 这个点
出现两个不同的 y 值。这里用 jitter_ms == 0 把扫描行筛出来。
"""
import csv
import os
import sys
from collections import OrderedDict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 中文标签需要 CJK 字体, 否则渲染成豆腐块。按 macOS -> Linux 顺序回退。
matplotlib.rcParams["font.sans-serif"] = [
    "PingFang SC", "Hiragino Sans GB", "Heiti SC", "Songti SC",
    "Arial Unicode MS", "Noto Sans CJK SC", "WenQuanYi Zen Hei", "DejaVu Sans",
]
matplotlib.rcParams["axes.unicode_minus"] = False

MODEL = sys.argv[1] if len(sys.argv) > 1 else "ryw"
SRC = f"results/{MODEL}.csv"
OUT = f"results/{MODEL}_delay_curves.png"

if not os.path.exists(SRC):
    sys.exit(f"找不到 {SRC} —— 先跑 ./scripts/sweep_latency.sh {MODEL}")

rows = list(csv.DictReader(open(SRC, newline="")))


def is_sweep_row(r):
    """扫描行: jitter=0、normal 场景、natural 模式。"""
    try:
        if float(r.get("jitter_ms") or -1) != 0.0:
            return False
        float(r["delay_ms"])
    except (ValueError, TypeError):
        return False
    return r.get("scenario") == "normal" and r.get("mode") in ("natural", "")


# 同一 (delay, W, R) 若跑过多次, 保留最后一次 —— CSV 是追加写入的,
# 所以重跑之后自然覆盖旧值, 行为可预期(取平均会把坏数据掺进来)
points = OrderedDict()
errors_seen = {}
for r in rows:
    if not is_sweep_row(r):
        continue
    key = (r["write_cl"], r["read_cl"])
    points.setdefault(key, {})[float(r["delay_ms"])] = float(r["violation_rate"]) * 100
    if int(r.get("errors") or 0) > 0:
        errors_seen[key] = errors_seen.get(key, 0) + int(r["errors"])

if not points:
    sys.exit(f"{SRC} 里没有扫描数据(jitter=0 的行) —— 先跑 "
             f"./scripts/sweep_latency.sh {MODEL}")

plt.figure(figsize=(9, 5.5))
for (w, r), series in points.items():
    xs = sorted(series)
    ys = [series[x] for x in xs]
    label = f"W={w}  R={r}"
    if (w, r) in errors_seen:
        label += f"  (异常 {errors_seen[(w, r)]})"
    plt.plot(xs, ys, marker="o", linewidth=2, markersize=6, label=label)

plt.xlabel("注入的单向复制延迟 (ms)", fontsize=11)
plt.ylabel(f"{MODEL.upper()} 违例率 (%)", fontsize=11)
plt.title(f"{MODEL.upper()} 违例率 vs 复制延迟（RF=3，jitter=0）", fontsize=13)
plt.ylim(-3, 103)
plt.legend(fontsize=10)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUT, dpi=150)

n_pts = sum(len(s) for s in points.values())
print(f"已保存 -> {OUT}  ({len(points)} 条曲线, {n_pts} 个数据点)")
for (w, r), series in points.items():
    pts = "  ".join(f"{int(x)}ms:{series[x]:.0f}%" for x in sorted(series))
    print(f"  W={w:7} R={r:7}  {pts}")
