#!/usr/bin/env bash
# 给三个节点之间的*副本复制流量*注入延迟, 制造可观测的复制窗口。
#
# 关键设计: 只延迟目的端口 7000 (Cassandra storage_port, 即节点间
# gossip/复制流量), 不延迟 9042 (客户端 CQL 流量)。
#
# 为什么不能像最初计划那样 `tc qdisc add dev eth0 root netem delay 200ms`:
#   那会把节点发给*客户端*的响应也延迟 200ms。后果有两个 ——
#   (a) 实验慢到不可用: 每次迭代多次往返, 1000 次循环要跑十几分钟,
#       完整矩阵 (4 模型 x 4 CL 组合 x 3 场景) 要好几个小时;
#   (b) 更严重的是它污染了因变量: 客户端"写完立刻读"之间本身就被迫
#       隔了几百毫秒, 反而给了复制额外的时间窗口去完成, 违例率被低估。
#
#   我们要模拟的是"副本之间同步慢", 不是"客户端网络慢"。只打 7000 端口
#   才精确对应前者。
#
# 三个节点都要打 —— 包括 cass1。因为 RYW 实验是"经 cass1 写、从 cass2 读",
# 制造延迟的是 cass1 的出向复制流量。只给 cass2/cass3 注入会漏掉这条路径。

set -euo pipefail

DELAY="${DELAY:-200ms}"
JITTER="${JITTER:-50ms}"
NODES="${NODES:-cass1 cass2 cass3}"
STORAGE_PORT=7000

for c in $NODES; do
  # cassandra:4.1 (Debian) 镜像默认不带 iproute2, 需要先装
  if ! docker exec "$c" sh -c 'command -v tc' >/dev/null 2>&1; then
    echo "[$c] 安装 iproute2 ..."
    docker exec "$c" sh -c 'apt-get update -qq && apt-get install -y -qq iproute2' >/dev/null 2>&1 \
      || { echo "[$c] 装 iproute2 失败 (可能无外网)。见脚本末尾的离线方案。"; exit 1; }
  fi

  # 清掉旧规则 (可能不存在, 忽略错误)
  docker exec "$c" tc qdisc del dev eth0 root 2>/dev/null || true

  # prio qdisc: band 1:1/1:2 走正常路径, 1:3 挂 netem
  docker exec "$c" tc qdisc add dev eth0 root handle 1: prio
  docker exec "$c" tc qdisc add dev eth0 parent 1:3 handle 30: \
      netem delay "$DELAY" "$JITTER" distribution normal
  # 只把去往 storage_port 的包分流到 band 1:3
  docker exec "$c" tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 \
      match ip dport $STORAGE_PORT 0xffff flowid 1:3

  echo "[$c] 已注入: dport ${STORAGE_PORT} 延迟 ${DELAY} ± ${JITTER}"
done

echo
echo "=== 验证 (应看到 netem 挂在 1:3 上) ==="
for c in $NODES; do
  echo "--- $c ---"
  docker exec "$c" tc qdisc show dev eth0
  docker exec "$c" tc filter show dev eth0 | head -4
done
