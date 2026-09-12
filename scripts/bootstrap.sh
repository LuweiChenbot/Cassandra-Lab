#!/usr/bin/env bash
# 一键把实验环境拉起来: 起集群 -> 建 keyspace -> 装 Python 驱动 -> 记录环境版本。
# 幂等, 可以重复跑。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> [1/5] 启动 3 节点集群 (compose 按 healthcheck 串行等待, 约 3-6 分钟)"
docker compose up -d --wait --wait-timeout 600

echo
echo "==> [2/5] 集群状态"
docker exec cass1 nodetool status
echo
docker exec cass1 nodetool describecluster | sed -n '1,8p'

echo
echo "==> [3/5] 建 keyspace 和表"
# healthcheck 通过之后 cqlsh 仍有短暂的不可用窗口 (statusbinary 已 running,
# 但本地 schema/连接尚未完全就绪), 所以要重试而不是一次失败就退出。
for attempt in 1 2 3 4 5 6; do
  if docker exec -i cass1 cqlsh < scripts/setup_keyspace.cql 2>/tmp/cqlsh_err; then
    echo "  keyspace 就绪 (第 $attempt 次尝试)"
    break
  fi
  if [ "$attempt" = 6 ]; then
    echo "  建 keyspace 失败, 最后一次错误:" >&2
    cat /tmp/cqlsh_err >&2
    exit 1
  fi
  echo "  第 $attempt 次未就绪, 8 秒后重试 ..."
  sleep 8
done
docker exec cass1 cqlsh -e "DESCRIBE TABLE consistency_lab.kv;" \
  | grep -E "read_repair|speculative_retry" || true

echo
echo "==> [4/5] Python 虚拟环境 + 固定版本的驱动"
if [ ! -d .venv ]; then python3 -m venv .venv; fi
./.venv/bin/pip install -q --upgrade pip
./.venv/bin/pip install -q -r requirements.txt

echo
echo "==> 验证节点钉定 (三个 session 必须落在三个不同 IP 上)"
./.venv/bin/python -c "
import sys; sys.path.insert(0,'experiments')
from common import connect_nodes
n = connect_nodes()
[x.shutdown() for x in n.values()]
"

echo
echo "==> [5/5] 记录环境版本 -> results/environment.txt"
mkdir -p results
{
  echo "# 实验环境快照 (由 scripts/bootstrap.sh 生成)"
  echo "生成时间: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo
  echo "Cassandra 镜像标签 : $(grep -m1 'image:' docker-compose.yml | awk '{print $2}')"
  echo "Cassandra 镜像摘要 : $(docker image inspect "$(grep -m1 'image:' docker-compose.yml | awk '{print $2}')" --format '{{.Id}}')"
  echo "Cassandra 版本     : $(docker exec cass1 cqlsh -e 'SELECT release_version FROM system.local;' 2>/dev/null | sed -n '4p' | tr -d ' ')"
  echo "Python             : $(./.venv/bin/python -V 2>&1)"
  echo "cassandra-driver   : $(./.venv/bin/python -c 'import cassandra; print(cassandra.__version__)')"
  echo "Docker             : $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  echo "宿主机             : $(uname -srm)"
} | tee results/environment.txt

echo
echo "环境就绪。下一步:"
echo "  ./scripts/inject_latency.sh                 # 注入复制延迟"
echo "  ./.venv/bin/python experiments/ryw_test.py -n 1000"
