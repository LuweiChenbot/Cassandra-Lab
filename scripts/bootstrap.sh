#!/usr/bin/env bash
# 一键把实验环境拉起来：起集群 -> 建 keyspace -> 装 Python 驱动。
# 幂等，可以重复跑。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> [1/4] 启动 3 节点集群（compose 会按 healthcheck 串行等待，约 3-5 分钟）"
docker compose up -d --wait

echo
echo "==> [2/4] 集群状态"
docker exec cass1 nodetool status
echo
docker exec cass1 nodetool describecluster | head -8

echo
echo "==> [3/4] 建 keyspace 和表"
docker exec -i cass1 cqlsh < scripts/setup_keyspace.cql
docker exec cass1 cqlsh -e "DESCRIBE TABLE consistency_lab.kv;" \
  | grep -E "read_repair|speculative_retry"

echo
echo "==> [4/4] Python 虚拟环境 + cassandra-driver"
if [ ! -d .venv ]; then python3 -m venv .venv; fi
./.venv/bin/pip install -q --upgrade pip
./.venv/bin/pip install -q cassandra-driver
./.venv/bin/python -c "import cassandra; print('  cassandra-driver', cassandra.__version__)"

echo
echo "==> 验证节点钉定（三个 session 必须落在三个不同 IP 上）"
./.venv/bin/python -c "
import sys; sys.path.insert(0,'experiments')
from common import connect_nodes
n = connect_nodes()
[x.shutdown() for x in n.values()]
"

echo
echo "环境就绪。下一步："
echo "  ./scripts/inject_latency.sh                 # 注入复制延迟"
echo "  ./.venv/bin/python experiments/ryw_test.py -n 200"
