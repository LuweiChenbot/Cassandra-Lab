#!/usr/bin/env bash
# 给节点间的*副本复制流量*注入延迟, 制造可观测的复制窗口。
#
# 只延迟目的端口 7000 (Cassandra storage_port, 节点间 gossip/复制),
# 不碰 9042 (客户端 CQL)。理由:
#   无差别延迟会把"写完立刻读"两步之间强行拉开几百毫秒, 反而给复制留出
#   时间窗口去完成, 违例率被系统性低估 —— 等于把要测的现象自己消掉了。
#   我们要模拟的是"副本之间同步慢", 不是"客户端网络慢"。
#
# tc 队列结构 (partition.sh 依赖同一套结构):
#
#   root prio 1: 四个 band, priomap 全部指向 band 0
#     ├─ 1:1  默认带, 无 qdisc     <- 未匹配的流量走这里, 完全不受影响
#     ├─ 1:2  netem delay          <- 复制延迟          (filter prio 3)
#     ├─ 1:3  netem loss 100%      <- 网络分区丢包      (filter prio 1)
#     └─ 1:4  预留
#
# priomap 全 0 很关键: prio qdisc 默认的 priomap 会按 TOS 位把流量散到
# 前三个 band, 那样未匹配的流量也可能落进 netem 带。全置 0 之后,
# 只有被 filter 显式选中的流量才会被处理, 行为完全确定。
#
# filter 优先级: 分区(prio 1) 高于 延迟(prio 3)。所以被分区的链路直接丢包,
# 不会先被延迟再丢。

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
  docker exec "$c" tc qdisc add dev eth0 parent 1:2 handle 20: \
      netem delay "$DELAY" "$JITTER" distribution normal
  docker exec "$c" tc qdisc add dev eth0 parent 1:3 handle 30: \
      netem loss 100%
  # 所有去往 storage_port 的流量 -> 延迟带
  docker exec "$c" tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 \
      match ip dport $STORAGE_PORT 0xffff flowid 1:2

  echo "[$c] 已注入: dport ${STORAGE_PORT} 延迟 ${DELAY} ± ${JITTER}"
done

echo
echo "=== 验证 ==="
for c in $NODES; do
  echo "--- $c ---"
  docker exec "$c" tc qdisc show dev eth0 | sed 's/^/    /'
done
