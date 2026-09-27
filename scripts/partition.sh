#!/usr/bin/env bash
# Partition one node from the others: drop inter-node traffic (port 7000) in
# both directions. Its client port stays reachable, so clients can use either
# side. Needs the qdisc set up by inject_latency.sh.
#
# Usage: ./scripts/partition.sh cass3      Undo: ./scripts/heal_partition.sh

set -euo pipefail

TARGET="${1:-cass3}"
ALL_NODES="${NODES:-cass1 cass2 cass3}"
STORAGE_PORT=7000

ip_of() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# requires the prio qdisc from inject_latency.sh
if ! docker exec "$TARGET" tc qdisc show dev eth0 | grep -q 'prio 1:'; then
  echo "错误: $TARGET 上没有 tc 队列结构。先跑 ./scripts/inject_latency.sh" >&2
  exit 1
fi

OTHERS=""
for c in $ALL_NODES; do [ "$c" = "$TARGET" ] || OTHERS="$OTHERS $c"; done

TARGET_IP=$(ip_of "$TARGET")
echo "隔离 $TARGET ($TARGET_IP)，切断其与[$OTHERS ] 的 :$STORAGE_PORT 通信"
echo

# target -> peers
for c in $OTHERS; do
  peer_ip=$(ip_of "$c")
  docker exec "$TARGET" tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
      match ip dst "$peer_ip"/32 match ip dport $STORAGE_PORT 0xffff flowid 1:3
  echo "  [$TARGET] 丢弃 -> $c ($peer_ip):$STORAGE_PORT"
done

# peers -> target (without this the partition would be one-way)
for c in $OTHERS; do
  docker exec "$c" tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
      match ip dst "$TARGET_IP"/32 match ip dport $STORAGE_PORT 0xffff flowid 1:3
  echo "  [$c] 丢弃 -> $TARGET ($TARGET_IP):$STORAGE_PORT"
done

echo
echo "等待 gossip 收敛 (约 20 秒) ..."
sleep 20
echo
for c in $ALL_NODES; do
  echo "--- $c 眼中的环 ---"
  docker exec "$c" nodetool status 2>/dev/null | grep -E '^[UD][NLJM]' | sed 's/^/    /' \
    || echo "    (取不到)"
done
echo
echo "多数派: [$OTHERS ]  ->  客户端用 --nodes 里不含 ${TARGET#cass} 的组合"
echo "少数派: $TARGET      ->  客户端用 --nodes ${TARGET#cass}"
