#!/usr/bin/env bash
# Delay inter-node traffic (destination port 7000) on every node with tc/netem.
# Client CQL traffic (port 9042) is not delayed, so no extra time is added
# between a client's write and its read.
#
# Usage: ./scripts/inject_latency.sh                        # 200ms ± 50ms
#        DELAY=100ms JITTER=0ms ./scripts/inject_latency.sh
#
# qdisc layout, shared with partition.sh (priomap all 0, so unmatched traffic
# stays in 1:1 untouched):
#   root prio 1:
#     1:1  default, no delay
#     1:2  netem delay       dport 7000 (filter prio 3)
#     1:3  netem loss 100%   partitions (filter prio 1, added by partition.sh)
#     1:4  unused

set -euo pipefail

DELAY="${DELAY:-200ms}"
JITTER="${JITTER:-50ms}"
NODES="${NODES:-cass1 cass2 cass3}"
STORAGE_PORT=7000

for c in $NODES; do
  if ! docker exec "$c" sh -c 'command -v tc' >/dev/null 2>&1; then
    echo "[$c] 安装 iproute2 ..."
    docker exec "$c" sh -c 'apt-get update -qq && apt-get install -y -qq iproute2' >/dev/null 2>&1 \
      || { echo "[$c] 装 iproute2 失败 (容器可能没有外网)"; exit 1; }
  fi

  docker exec "$c" tc qdisc del dev eth0 root 2>/dev/null || true

  docker exec "$c" tc qdisc add dev eth0 root handle 1: prio bands 4 \
      priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
  # netem only accepts "distribution normal" with a non-zero jitter, so drop
  # both when JITTER is 0
  case "$JITTER" in
    0|0ms|0s|"")
      docker exec "$c" tc qdisc add dev eth0 parent 1:2 handle 20: \
          netem delay "$DELAY"
      ;;
    *)
      docker exec "$c" tc qdisc add dev eth0 parent 1:2 handle 20: \
          netem delay "$DELAY" "$JITTER" distribution normal
      ;;
  esac
  docker exec "$c" tc qdisc add dev eth0 parent 1:3 handle 30: \
      netem loss 100%
  # traffic to storage_port -> delay band
  docker exec "$c" tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 \
      match ip dport $STORAGE_PORT 0xffff flowid 1:2

  case "$JITTER" in
    0|0ms|0s|"") echo "[$c] 已注入: dport ${STORAGE_PORT} 延迟 ${DELAY} (无抖动)" ;;
    *)           echo "[$c] 已注入: dport ${STORAGE_PORT} 延迟 ${DELAY} ± ${JITTER}" ;;
  esac
done

echo
echo "=== 验证 ==="
for c in $NODES; do
  echo "--- $c ---"
  docker exec "$c" tc qdisc show dev eth0 | sed 's/^/    /'
done
