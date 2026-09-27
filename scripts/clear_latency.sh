#!/usr/bin/env bash
# Remove all tc/netem rules (injected delay and partitions) from the nodes.
# Usage: ./scripts/clear_latency.sh
set -uo pipefail
NODES="${NODES:-cass1 cass2 cass3}"
for c in $NODES; do
  docker exec "$c" tc qdisc del dev eth0 root 2>/dev/null \
    && echo "[$c] 已清除延迟" || echo "[$c] 无规则可清除"
done
