#!/usr/bin/env bash
# 移除 partition.sh 添加的丢包 filter (prio 1), 保留延迟规则 (prio 3)。
set -uo pipefail
NODES="${NODES:-cass1 cass2 cass3}"
for c in $NODES; do
  removed=0
  # 同一节点可能挂了多条 prio 1 filter (每个对端一条), 循环删到没有为止
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
