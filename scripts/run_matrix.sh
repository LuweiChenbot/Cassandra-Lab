#!/usr/bin/env bash
# 跑完整实验矩阵: 4 个模型 × 4 个 R/W 组合, 按场景自动套用正确的节点参数。
#
#   用法: ./scripts/run_matrix.sh <场景> [迭代次数]
#   场景: normal | node_failure | partition_majority | partition_minority
#
# 为什么每个场景要单独配节点参数
# ------------------------------
# MW 默认用 node2 写 W1、node3 写 W2, WFR 默认要用满三个节点。
# 节点故障/分区时 node3 不可达, 沿用默认参数会直接报错。
# 而且 setup/verify 这类"实验装置"用的 CL 默认是 ALL, 三副本不全时
# 整轮迭代会全部作废 —— 必须降到 QUORUM(多数派) 或 ONE(少数派)。
#
# 前置条件:
#   normal              : 三节点全在, 已跑 inject_latency.sh
#   node_failure        : docker stop cass3
#   partition_majority  : ./scripts/partition.sh cass3, 客户端连 cass1/cass2
#   partition_minority  : ./scripts/partition.sh cass3, 客户端只连 cass3

set -uo pipefail
cd "$(dirname "$0")/.."

SCENARIO="${1:-normal}"
N="${2:-1000}"
PY=./.venv/bin/python

case "$SCENARIO" in
  normal)
    NODES="1,2,3"
    RYW="--write-node 1 --read-node 2"
    MR="--write-node 1 --read1-node 1 --read2-node 2 --seed-cl ALL"
    MW="--write1-node 2 --write2-node 3 --verify-node 1 --verify-cl ALL"
    WFR="--write-node 1 --read-node 2 --client-node 3 --verify-node 1 --setup-cl ALL --verify-cl ALL"
    ;;
  node_failure|partition_majority)
    # 只剩 cass1 + cass2。装置 CL 降到 QUORUM(2/3, 多数派仍可满足)
    NODES="1,2"
    RYW="--write-node 1 --read-node 2"
    MR="--write-node 1 --read1-node 1 --read2-node 2 --seed-cl QUORUM"
    # verify 降到 QUORUM 后必须给复制留收敛时间, 否则可见性滞后会被误判成定序违例
    MW="--write1-node 1 --write2-node 2 --verify-node 1 --verify-cl QUORUM --settle-ms 1000"
    WFR="--write-node 1 --read-node 2 --client-node 1 --verify-node 1 --setup-cl QUORUM --verify-cl QUORUM --settle-ms 1000"
    ;;
  partition_minority)
    # 只有 cass3 一个节点可达。装置 CL 只能用 ONE。
    # 注意: 单节点下 RYW/MR 的读写都在本地, 结构上不可能违例 ——
    # 这一组测的是*可用性*(哪些 CL 还能服务), 不是违例率。
    NODES="3"
    RYW="--write-node 3 --read-node 3"
    MR="--write-node 3 --read1-node 3 --read2-node 3 --seed-cl ONE"
    MW="--write1-node 3 --write2-node 3 --verify-node 3 --verify-cl ONE --settle-ms 1000"
    WFR="--write-node 3 --read-node 3 --client-node 3 --verify-node 3 --setup-cl ONE --verify-cl ONE --settle-ms 1000"
    ;;
  *)
    echo "未知场景: $SCENARIO" >&2
    echo "可选: normal | node_failure | partition_majority | partition_minority" >&2
    exit 1
    ;;
esac

# CSV 里的场景标签: partition_majority / partition_minority 都属于 partition 拓扑
case "$SCENARIO" in
  partition_*) TAG="partition" ;;
  *)           TAG="$SCENARIO" ;;
esac

# 实验矩阵的四个 R/W 组合
COMBOS=("ONE ONE" "ONE QUORUM" "QUORUM QUORUM" "ALL ONE")

echo "==================================================================="
echo " 实验矩阵  场景=$SCENARIO  标签=$TAG  节点=$NODES  n=$N"
echo "==================================================================="

run_one() {
  local model="$1" extra="$2" w="$3" r="$4"
  echo
  echo "-------------------------------------------------------------------"
  echo ">>> $model   W=$w  R=$r"
  echo "-------------------------------------------------------------------"
  # shellcheck disable=SC2086
  $PY "experiments/${model}_test.py" \
      --scenario "$TAG" --nodes "$NODES" \
      --write-cl "$w" --read-cl "$r" -n "$N" $extra \
    || echo "    [该组合以非零状态退出, 继续下一组]"
}

for combo in "${COMBOS[@]}"; do
  w="${combo% *}"; r="${combo#* }"
  run_one ryw "$RYW" "$w" "$r"
  run_one mr  "$MR"  "$w" "$r"
  run_one mw  "$MW"  "$w" "$r"
  run_one wfr "$WFR" "$w" "$r"
done

echo
echo "==================================================================="
echo " 矩阵跑完。汇总: results/all_results.csv"
echo " 逐次明细: results/traces/"
echo "==================================================================="
