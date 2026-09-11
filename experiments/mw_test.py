#!/usr/bin/env python3
"""
mw_test.py -- Monotonic Writes (单调写)

定义
----
同一客户端先发 W1 后发 W2, 则所有副本上 W2 必须排在 W1 之后。

为什么 MW 在 Cassandra 里跟 R/W 一致性级别无关
----------------------------------------------
Cassandra 的冲突消解是 last-write-wins, 唯一的裁决依据是每个 cell 的
写时间戳 (WRITETIME), 而不是请求到达顺序。

  - CL 控制的是"等几个副本 ack"(写) 和"问几个副本"(读), 也就是*可见性*;
  - 谁覆盖谁, 完全由时间戳大小决定, 也就是*定序*。

所以只要 W2 的时间戳大于 W1, 不管 CL=ONE 还是 ALL, 最终态都是 W2;
只要 W2 的时间戳小于 W1, 不管 CL=ONE 还是 ALL, W2 都会被永久丢弃。
CL 这一维对 MW 完全正交 —— 这正是实验矩阵里 MW 一列全打 "?" 的原因,
而本脚本就是用来把这个 "?" 变成确定结论的。

两种模式
--------
natural (基线):
    用驱动默认时间戳正常写两次。Python 驱动每个 Cluster 有自己的
    MonotonicTimestampGenerator, 取系统时钟且保证单调; 两次写相隔毫秒级,
    必然 ts2 > ts1。预期: 所有 CL 组合下违例都是 0。
    这一组的作用是证明"正常情况下 MW 不会自己违例", 从而说明
    下一组的违例确实来自时间戳而非别的因素。

skew (构造违例):
    用 USING TIMESTAMP 手动传入乱序时间戳:
        W1 (value=1) 时间戳 = base + skew    <- 协调者时钟快
        W2 (value=2) 时间戳 = base           <- 协调者时钟正常
    客户端的发送顺序仍是 W1 -> W2, 但 LWW 会保留 W1。
    预期: 所有 CL 组合下违例率都是 100%。

    这不是人为刁难。它精确对应一个真实故障:
    客户端两次写经过了*不同的协调者*(重连/负载均衡), 而这两台机器之间
    存在时钟偏移。当客户端不自带时间戳时, 协调者会用自己的墙上时钟赋值,
    NTP 失步就会产生这种倒挂。USING TIMESTAMP 只是让这个偏移可控可复现,
    省去在容器里真的去改系统时钟。

判定
----
    最终值 = 经 node1 以 CL=verify_cl (默认 ALL) 读出的值。
    用 ALL 是因为 MW 判的是"收敛后的定序", 必须看全部副本;
    若用 ONE 读到一个还没收到任何写的副本, 那是可见性问题(RYW/MR),
    会和 MW 混为一谈。
    最终值 != 2  =>  MW 违例 (客户端后发的写没有胜出)
"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    connect_nodes, cl, summarize, build_parser, parse_nodes, now_us,
    TRANSIENT_ERRORS,
)

MODEL = "mw"
V1, V2 = 1, 2


def run_mode(mode, nodes, args, w_cl, v_cl):
    """跑一种模式 (natural / skew), 返回结果行。"""
    w1n = nodes[args.write1_node]
    w2n = nodes[args.write2_node]
    vn = nodes[args.verify_node]
    run_id = time.strftime("%H%M%S")

    print(f"\n--- 模式 {mode}: W1@node{args.write1_node} -> W2@node{args.write2_node}, "
          f"verify@node{args.verify_node} CL={args.verify_cl} ---")
    if mode == "skew":
        print(f"    构造时钟偏移 {args.skew_us} us "
              f"(W1 时间戳 = base+{args.skew_us}, W2 时间戳 = base)")

    iterations = triggered = violations = errors = 0
    kept_v1 = lost_both = 0
    step = max(1, args.iterations // 10)

    for i in range(1, args.iterations + 1):
        iterations += 1
        key = f"{MODEL}_{mode}_{run_id}_{i}"
        try:
            if mode == "natural":
                # 驱动自动生成单调递增时间戳
                w1n.write(key, V1, w_cl, writer="w1")
                w2n.write(key, V2, w_cl, writer="w2")
            else:
                base = now_us()
                # 客户端发送顺序 W1 -> W2, 但时间戳故意倒挂
                w1n.write(key, V1, w_cl, writer="w1", timestamp=base + args.skew_us)
                w2n.write(key, V2, w_cl, writer="w2", timestamp=base)

            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)
            final = vn.read(key, v_cl)
        except TRANSIENT_ERRORS as e:
            errors += 1
            if errors <= 3:
                print(f"    [异常 {type(e).__name__}] {str(e)[:90]}")
            continue

        triggered += 1
        if final != V2:
            violations += 1
            if final == V1:
                kept_v1 += 1      # W1 覆盖了 W2 —— 典型 MW 违例
            else:
                lost_both += 1    # 两次写都没看到 (可见性问题, 不是定序问题)

        if i % step == 0:
            rate = violations / triggered if triggered else 0
            print(f"    [{i:>5}/{args.iterations}] 违例 {violations:>5} "
                  f"({rate:.1%})  异常 {errors}")

    return summarize(
        MODEL, args.scenario, mode,
        args.write_cl, args.read_cl, args.verify_cl,
        iterations, triggered, violations, errors,
        extra=f"W1_wins={kept_v1} neither_visible={lost_both} "
              f"skew_us={args.skew_us if mode == 'skew' else 0} "
              f"path=node{args.write1_node}->node{args.write2_node}",
    )


def main():
    p = build_parser(MODEL)
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=500_000,
                   help="构造的时钟偏移(微秒), 默认 500000 = 500ms")
    p.add_argument("--write1-node", type=int, default=2, help="W1 的协调者")
    p.add_argument("--write2-node", type=int, default=3, help="W2 的协调者")
    p.add_argument("--verify-node", type=int, default=1, help="收敛校验读的节点")
    args = p.parse_args()

    w_cl, v_cl = cl(args.write_cl), cl(args.verify_cl)

    print(f"\n=== MW: W={args.write_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  场景={args.scenario} ===")
    print("    注意: MW 由写时间戳定序, read_cl 在本实验中不参与判定, "
          "仅记录在 CSV 中以对齐矩阵。")

    nodes = connect_nodes(parse_nodes(args.nodes))
    try:
        modes = ["natural", "skew"] if args.mode == "both" else [args.mode]
        for m in modes:
            run_mode(m, nodes, args, w_cl, v_cl)
    finally:
        for nd in nodes.values():
            nd.shutdown()


if __name__ == "__main__":
    main()
