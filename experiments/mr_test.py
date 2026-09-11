#!/usr/bin/env python3
"""
mr_test.py -- Monotonic Reads (单调读)

定义
----
客户端读到 x 之后, 它之后的任何读都不能返回比 x 更旧的值 —— 时间不能倒流。

实验构造
--------
每次迭代用一个全新的 key:

    1. [种子] 经 node1 以 CL=ALL 写 value=1
       => 三个副本都持有 v=1, 保证后面第一次读一定读得到东西
    2. 经 node1 以 CL=write_cl 写 value=2
       => write_cl=ONE 时只有 node1 确定有 v=2, node2/node3 还停在 v=1
    3. 客户端第一次读: node1, CL=read_cl   -> r1
    4. 客户端第二次读: node2, CL=read_cl   -> r2   (客户端"换了个节点")
    5. r2 < r1  =>  单调读违例 (读到了比刚才更旧的值)

为什么第一次读 node1、第二次读 node2
------------------------------------
这不是"凑结果"。MR 要防的就是客户端连接在节点间迁移时看到时光倒流:
负载均衡器重新路由、驱动故障转移重连、节点重启后客户端换节点。
node1 是刚才写 v=2 的协调者, 必然最新; node2 因为复制延迟还落后。
客户端先读到 2 再读到 1, 正是这条保证存在的理由。

为什么要有"种子写"
------------------
不种子的话, write_cl=ONE 下 node1 和 node2 大概率*都*还没有数据可读
(r1=r2=None), 迭代变成无效样本, 违例率会被系统性低估到接近 0 ——
看上去"没问题", 实际只是没测到。种子写用 CL=ALL 建立三副本共同初始状态,
把每次迭代都变成有效样本。种子写的 CL 是实验装置的一部分, 不是自变量。

预期
----
    W=ONE    R=ONE     违例 (node2 停在 v=1)
    W=ONE    R=QUORUM  部分违例 (node2 的法定人数是否包含 node1 是随机的)
    W=QUORUM R=QUORUM  不违例 (2+2 > 3)
    W=ALL    R=ONE     不违例 (3+1 > 3)
"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    connect_nodes, cl, summarize, build_parser, parse_nodes, TRANSIENT_ERRORS,
)

MODEL = "mr"
SEED_VALUE = 1
NEW_VALUE = 2


def rank(v):
    """None (行不存在) 排在所有实际值之前, 便于比较新旧。"""
    return -1 if v is None else v


def main():
    p = build_parser(MODEL)
    p.add_argument("--seed-cl", default="ALL",
                   help="种子写的一致性级别 (实验装置; 节点故障场景下降为 QUORUM)")
    p.add_argument("--write-node", type=int, default=1, help="两次写的协调者")
    p.add_argument("--read1-node", type=int, default=1, help="客户端第一次读的节点")
    p.add_argument("--read2-node", type=int, default=2, help="客户端第二次读的节点")
    args = p.parse_args()

    w_cl, r_cl, s_cl = cl(args.write_cl), cl(args.read_cl), cl(args.seed_cl)
    run_id = time.strftime("%H%M%S")

    print(f"\n=== MR: seed={args.seed_cl}  W={args.write_cl}@node{args.write_node}  "
          f"R={args.read_cl} (node{args.read1_node} -> node{args.read2_node})  "
          f"n={args.iterations}  场景={args.scenario} ===")

    nodes = connect_nodes(parse_nodes(args.nodes))
    wn = nodes[args.write_node]
    r1n, r2n = nodes[args.read1_node], nodes[args.read2_node]

    iterations = triggered = violations = errors = 0
    went_none = 0          # r1 有值但 r2 读不到行
    went_older = 0         # r2 读到了更小的值
    step = max(1, args.iterations // 10)

    try:
        for i in range(1, args.iterations + 1):
            iterations += 1
            key = f"{MODEL}_{run_id}_{i}"
            try:
                wn.write(key, SEED_VALUE, s_cl, writer="seed")
                wn.write(key, NEW_VALUE, w_cl, writer="update")
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000.0)
                r1 = r1n.read(key, r_cl)
                r2 = r2n.read(key, r_cl)
            except TRANSIENT_ERRORS as e:
                errors += 1
                if errors <= 3:
                    print(f"    [异常 {type(e).__name__}] {str(e)[:90]}")
                continue

            # 只有第一次读确实观测到了某个值, 单调性义务才成立
            if r1 is None:
                continue
            triggered += 1

            if rank(r2) < rank(r1):
                violations += 1
                if r2 is None:
                    went_none += 1
                else:
                    went_older += 1

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] 违例 {violations:>5} "
                      f"({rate:.1%})  异常 {errors}")
    finally:
        for nd in nodes.values():
            nd.shutdown()

    summarize(
        MODEL, args.scenario, "natural",
        args.write_cl, args.read_cl, "-",
        iterations, triggered, violations, errors,
        extra=f"seed_cl={args.seed_cl} back_to_none={went_none} "
              f"back_to_older={went_older} "
              f"path=node{args.read1_node}->node{args.read2_node}",
    )


if __name__ == "__main__":
    main()
