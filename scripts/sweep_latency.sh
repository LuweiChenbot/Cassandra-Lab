#!/usr/bin/env bash
# Latency sweep for one model: for each delay (jitter 0), set netem on all
# nodes, check the applied delay, then run the four W/R combinations.
# Rows are appended to results/<model>.csv; plot them with scripts/plot_sweep.py.
#
# Usage: ./scripts/sweep_latency.sh <model> [iterations=200] [delay_ms ...]
#   e.g. ./scripts/sweep_latency.sh ryw 100 0 5 10 15 20 50 100 200 400 800
#   default delays: 0 25 50 100 200 400 800

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

# delay netem actually applies on a node (whole ms)
observed_delay() {
  docker exec "$1" tc qdisc show dev eth0 2>/dev/null \
    | grep -o 'delay [0-9.]*ms' | head -1 | grep -o '[0-9.]*' | cut -d. -f1
}

echo "==================================================================="
echo " delay sweep  model=$MODEL  n=$N  delays=${DELAYS[*]} ms  jitter=0"
echo "==================================================================="

for d in "${DELAYS[@]}"; do
  echo
  echo "###################################################################"
  echo "### delay = ${d} ms"
  echo "###################################################################"

  if [ "$d" = "0" ]; then
    # 0 ms: remove the qdisc instead of adding "netem delay 0ms"
    ./scripts/clear_latency.sh >/dev/null 2>&1
  else
    DELAY="${d}ms" JITTER="0ms" ./scripts/inject_latency.sh >/dev/null 2>&1
  fi

  # stop if any node's applied delay differs from the target
  ok=1
  for c in cass1 cass2 cass3; do
    got=$(observed_delay "$c"); got="${got:-0}"
    if [ "$got" != "$d" ]; then
      echo "  ! $c applied delay ${got}ms != target ${d}ms" >&2
      ok=0
    fi
  done
  if [ "$ok" != "1" ]; then
    echo "  delay injection did not take effect, stopping the sweep." >&2
    exit 1
  fi
  echo "  [checked] all three nodes apply ${d}ms"

  for combo in "${COMBOS[@]}"; do
    w="${combo% *}"; r="${combo#* }"
    echo
    echo "--- $MODEL  W=$w  R=$r  delay=${d}ms ---"
    $PY "experiments/${MODEL}_test.py" \
        --scenario normal --write-cl "$w" --read-cl "$r" -n "$N" --no-trace \
      || echo "    [exited non-zero, continuing]"
  done
done

echo
echo "==================================================================="
echo " sweep done. Plot with ./.venv/bin/python scripts/plot_sweep.py $MODEL"
echo "==================================================================="
