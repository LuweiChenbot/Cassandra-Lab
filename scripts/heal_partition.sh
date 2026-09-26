#!/usr/bin/env bash
# Remove the partition filters added by partition.sh; injected delay stays.
# Usage: ./scripts/heal_partition.sh
set -uo pipefail
NODES="${NODES:-cass1 cass2 cass3}"
for c in $NODES; do
  removed=0
  # one prio-1 filter per peer: delete until none are left
  while docker exec "$c" tc filter show dev eth0 2>/dev/null | grep -q 'pref 1 u32'; do
    docker exec "$c" tc filter del dev eth0 parent 1:0 prio 1 2>/dev/null || break
    removed=$((removed+1))
  done
  echo "[$c] 移除分区 filter ${removed} 组"
done
echo
echo "等待 gossip 恢复 (约 25 秒) ..."
sleep 25
docker exec cass1 nodetool status 2>&1 | grep -E '^[UD][NLJM]' | sed 's/^/    /'
