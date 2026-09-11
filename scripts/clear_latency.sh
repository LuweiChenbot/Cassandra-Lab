#!/usr/bin/env bash
# 移除 inject_latency.sh 注入的所有 netem 规则, 恢复基线。
set -uo pipefail
NODES="${NODES:-cass1 cass2 cass3}"
for c in $NODES; do
  docker exec "$c" tc qdisc del dev eth0 root 2>/dev/null \
    && echo "[$c] 已清除延迟" || echo "[$c] 无规则可清除"
done
