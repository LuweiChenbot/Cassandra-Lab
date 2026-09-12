#!/usr/bin/env bash
# 制造网络分区: 切断指定节点与其余节点之间的 Cassandra 内部通信(端口 7000),
# 但保留它的 9042 客户端通道。
#
#   用法: ./scripts/partition.sh cass3
#   恢复: ./scripts/heal_partition.sh
#
# 为什么不用 docker network disconnect
# -----------------------------------
# Cassandra 启动时把 listen_address 绑死在容器当时的 IP。把容器从网络摘掉
# 会连同这个网卡地址一起拿走 —— 结果不是"节点被隔离", 而是**节点的内部
# 监听器直接失效**, 重连后 IP 还可能变化, Cassandra 不会重新绑定, 只能靠
# 重启恢复。那是把节点弄坏, 不是制造分区。
#
# 而且摘掉网络后宿主机的 9044 端口映射也失效, 客户端**连不上少数派**,
# 于是只能观察多数派一侧 —— 无法回答"少数派还能不能用 CL=ONE 读写"
# "两侧是否产生不同版本""恢复后谁胜出"这些网络分区最核心的问题。
#
# 用 tc 丢包则: IP 和网卡全程不动, Cassandra 无感; 9042 完好, 多数派和
# 少数派两侧都能连; 一条 filter del 即可恢复。

set -euo pipefail

TARGET="${1:-cass3}"
ALL_NODES="${NODES:-cass1 cass2 cass3}"
STORAGE_PORT=7000

ip_of() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# 前置条件: inject_latency.sh 建好的 prio 队列结构
if ! docker exec "$TARGET" tc qdisc show dev eth0 | grep -q 'prio 1:'; then
  echo "错误: $TARGET 上没有 tc 队列结构。先跑 ./scripts/inject_latency.sh" >&2
  exit 1
fi

OTHERS=""
for c in $ALL_NODES; do [ "$c" = "$TARGET" ] || OTHERS="$OTHERS $c"; done

TARGET_IP=$(ip_of "$TARGET")
echo "隔离 $TARGET ($TARGET_IP)，切断其与[$OTHERS ] 的 :$STORAGE_PORT 通信"
echo

# 方向一: target -> 其余节点
for c in $OTHERS; do
  peer_ip=$(ip_of "$c")
  docker exec "$TARGET" tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
      match ip dst "$peer_ip"/32 match ip dport $STORAGE_PORT 0xffff flowid 1:3
  echo "  [$TARGET] 丢弃 -> $c ($peer_ip):$STORAGE_PORT"
done

# 方向二: 其余节点 -> target。两个方向都要断, 否则是单向分区
# (对方仍能把 mutation 推过来, 只是收不到回应), 那是另一种故障模型。
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
