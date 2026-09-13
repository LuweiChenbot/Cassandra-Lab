#!/usr/bin/env bash
# 延迟扫描: 固定 (write_cl, read_cl) 组合, 扫描复制延迟, 得到违例率曲线。
#
#   用法: ./scripts/sweep_latency.sh <模型> [迭代次数] [延迟点...]
#   例:   ./scripts/sweep_latency.sh ryw 200
#         ./scripts/sweep_latency.sh ryw 100 0 50 100 200
#
# 输出: 每个 (延迟, W, R) 一行, 写进 results/<模型>.csv 和 all_results.csv。
#       画图时按 (write_cl, read_cl) 分组、以 delay_ms 为横轴即可。
#
# 为什么 jitter 固定为 0
# ---------------------
# 正式矩阵用的是 200ms ± 50ms, 模拟真实网络的抖动。但扫描要的是一个
# 确定的标量横轴 —— 带抖动的话"延迟=100ms"这个点实际覆盖 50-150ms,
# 曲线的横坐标含义就模糊了。两者目的不同, CSV 里的 jitter_ms 列可区分。
#
# 为什么每个点都要验证实测延迟
# ----------------------------
# tc 规则可能在无人察觉的情况下消失(本项目就遇到过一次: 容器没重启,
# 但 qdisc 变回了 noqueue)。如果只按"打算设成多少"记录, 一整批数据会
# 带着错误的横坐标而看不出问题。这里在开跑前断言实测值等于目标值。

set -uo pipefail
cd "$(dirname "$0")/.."

MODEL="${1:-ryw}"
N="${2:-200}"
shift 2 2>/dev/null || shift $# 
DELAYS=("$@")
if [ ${#DELAYS[@]} -eq 0 ]; then
  DELAYS=(0 25 50 100 200 400 800)
fi

PY=./.venv/bin/python
COMBOS=("ONE ONE" "ONE QUORUM" "QUORUM QUORUM" "ALL ONE")

# 读某节点实际生效的 netem 延迟(毫秒, 整数)
observed_delay() {
  docker exec "$1" tc qdisc show dev eth0 2>/dev/null \
    | grep -o 'delay [0-9.]*ms' | head -1 | grep -o '[0-9.]*' | cut -d. -f1
}

echo "==================================================================="
echo " 延迟扫描  模型=$MODEL  n=$N  延迟点=${DELAYS[*]} ms  jitter=0"
echo "==================================================================="

for d in "${DELAYS[@]}"; do
  echo
  echo "###################################################################"
  echo "### 延迟 = ${d} ms"
  echo "###################################################################"

  if [ "$d" = "0" ]; then
    # 真基线: 完全移除 qdisc, 而不是设 delay 0ms (后者仍会过一遍 netem)
    ./scripts/clear_latency.sh >/dev/null 2>&1
  else
    DELAY="${d}ms" JITTER="0ms" ./scripts/inject_latency.sh >/dev/null 2>&1
  fi

  # 断言三个节点的实测值都等于目标, 不符就停 —— 宁可中断也不要产出
  # 横坐标错误的数据
  ok=1
  for c in cass1 cass2 cass3; do
    got=$(observed_delay "$c"); got="${got:-0}"
    if [ "$got" != "$d" ]; then
      echo "  ! $c 实测延迟 ${got}ms != 目标 ${d}ms" >&2
      ok=0
    fi
  done
  if [ "$ok" != "1" ]; then
    echo "  延迟注入未按预期生效, 中止扫描。" >&2
    exit 1
  fi
  echo "  [已验证] 三节点实测延迟均为 ${d}ms"

  for combo in "${COMBOS[@]}"; do
    w="${combo% *}"; r="${combo#* }"
    echo
    echo "--- $MODEL  W=$w  R=$r  delay=${d}ms ---"
    $PY "experiments/${MODEL}_test.py" \
        --scenario normal --write-cl "$w" --read-cl "$r" -n "$N" --no-trace \
      || echo "    [该组合非零退出, 继续]"
  done
done

echo
echo "==================================================================="
echo " 扫描完成。按 (write_cl, read_cl) 分组、delay_ms 为横轴出图。"
echo "==================================================================="
