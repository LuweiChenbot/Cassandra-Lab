#!/usr/bin/env bash
# 扫描不同延迟 x 不同 CL 组合, 测 RYW 违例率
set -uo pipefail
cd "$(dirname "$0")/.."

DELAYS=(0ms 50ms 100ms 200ms 400ms 800ms)
COMBOS=("ONE ONE" "ONE QUORUM" "QUORUM QUORUM" "ALL ONE")
N="${1:-200}"        # 扫描用小一点的 n, 图画好了再用 1000 跑正式数据

OUT=results/sweep_ryw.csv
mkdir -p results
echo "delay_ms,write_cl,read_cl,violation_rate,error_rate" > "$OUT"

for delay in "${DELAYS[@]}"; do
  if [ "$delay" = "0ms" ]; then
    ./scripts/clear_latency.sh >/dev/null
  else
    DELAY="$delay" JITTER=0ms ./scripts/inject_latency.sh >/dev/null
  fi

  for combo in "${COMBOS[@]}"; do
    w="${combo% *}"; r="${combo#* }"
    echo ">>> delay=$delay  W=$w  R=$r"

    parsed=$(./.venv/bin/python experiments/ryw_test.py \
        --write-cl "$w" --read-cl "$r" -n "$N" --no-trace | \
      python3 -c "
import sys, re
t = sys.stdin.read()
v = re.search(r'违例率=([\d.]+)%', t)
e = re.search(r'异常=\d+ \(([\d.]+)%\)', t)
print(f\"{v.group(1) if v else ''},{e.group(1) if e else ''}\")
")
    delay_ms="${delay%ms}"
    echo "${delay_ms},${w},${r},${parsed}" >> "$OUT"
  done
done

./scripts/clear_latency.sh >/dev/null
echo "扫描完成 -> $OUT"