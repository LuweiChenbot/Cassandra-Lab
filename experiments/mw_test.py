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

写时间戳到底由谁生成 (这一点必须说准)
-------------------------------------
Cassandra 的写时间戳有三个可能来源, 优先级从高到低:

  1. CQL 语句里的 USING TIMESTAMP —— 显式指定, 覆盖一切;
  2. 客户端驱动生成并随请求发送 —— **现代驱动的默认行为**。
     cassandra-driver 的 Cluster.timestamp_generator 默认是
     MonotonicTimestampGenerator, 时间戳在客户端机器上生成;
  3. 协调者用自己的墙上时钟赋值 —— 仅当客户端没提供时间戳时
     (老驱动, 或显式把 timestamp_generator 设为 None)。

本项目实测确认了第 2 条: 把驱动的生成器替换成一个固定的异常值
(1000000000000000, 即 2001 年) 后, 不带 USING TIMESTAMP 的普通写
落库的 WRITETIME 恰好等于该值, 而非写入时刻。

因此 skew 模式构造的是 **client-supplied timestamp inversion**
(客户端提供的时间戳倒挂), 不是"协调者之间的时钟偏移"。
报告里必须区分这两者, 不能混为一谈。

对应的真实故障是: 两个客户端进程跑在**不同机器**上, 机器之间 NTP 失步,
各自驱动生成的时间戳因而倒挂。由于时间戳在客户端生成, 客户端机器的
时钟偏移会直接决定写的定序 —— 这在生产环境里非常常见, 也正是
Cassandra 官方建议"要么全程严格授时, 要么由应用统一提供时间戳"的原因。
USING TIMESTAMP 只是让这个偏移可控可复现, 省去真的准备两台失步的机器。

两种模式
--------
natural (基线):
    用驱动默认时间戳正常写两次。两次写相隔毫秒级, 必然 ts2 > ts1。
    预期: 所有 CL 组合下违例都是 0。
    这一组的作用是证明"正常情况下 MW 不会自己违例", 从而说明
    下一组的违例确实来自时间戳倒挂而非别的因素。

skew (构造违例):
        W1 (value=1) 时间戳 = base + skew    <- 发出 W1 的客户端时钟快
        W2 (value=2) 时间戳 = base           <- 发出 W2 的客户端时钟正常
    客户端的发送顺序仍是 W1 -> W2, 但 LWW 会保留 W1。
    预期: 所有 CL 组合下违例率都是 100%。

判定
----
    最终值 = 经 verify_node 以 CL=verify_cl (默认 ALL) 读出的值。
    用 ALL 是因为 MW 判的是"收敛后的定序", 必须看全部副本;
    若用 ONE 读到一个还没收到任何写的副本, 那是可见性问题(RYW/MR),
    会和 MW 混为一谈。
    最终值 != 2  =>  MW 违例 (客户端后发的写没有胜出)
"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import topology
from common import (
    connect_nodes, cl, summarize, build_parser, parse_nodes, validate, now_us,
    TraceWriter, TRANSIENT_ERRORS,
)

MODEL = "mw"
V1, V2 = 1, 2


def run_mode(mode, nodes, args, w_cl, v_cl, topo):
    w1n = nodes[args.write1_node]
    w2n = nodes[args.write2_node]
    vn = nodes[args.verify_node]
    run_id = time.strftime("%H%M%S")

    print(f"\n--- 模式 {mode}: W1@node{args.write1_node} -> W2@node{args.write2_node}, "
          f"verify@node{args.verify_node} CL={args.verify_cl} ---")
    if mode == "skew":
        print(f"    构造客户端时间戳倒挂 {args.skew_us} us "
              f"(W1 时间戳 = base+{args.skew_us}, W2 时间戳 = base)")

    trace = TraceWriter(MODEL, args.scenario, mode,
                        args.write_cl, args.read_cl, run_id,
                        enabled=not args.no_trace)

    iterations = triggered = violations = errors = 0
    kept_v1 = lost_both = 0
    step = max(1, args.iterations // 10)

    try:
        for i in range(1, args.iterations + 1):
            iterations += 1
            key = f"{MODEL}_{mode}_{run_id}_{i}"
            base = now_us()
            ts1 = base + args.skew_us if mode == "skew" else None
            ts2 = base if mode == "skew" else None
            rec = {"run_id": run_id, "iteration": i, "model": MODEL,
                   "scenario": args.scenario, "mode": mode,
                   "write_cl": args.write_cl, "verify_cl": args.verify_cl,
                   "w1_node": w1n.name, "w1_addr": w1n.address,
                   "w2_node": w2n.name, "w2_addr": w2n.address,
                   "verify_node": vn.name,
                   "key": key, "w1_value": V1, "w2_value": V2,
                   "w1_timestamp": ts1, "w2_timestamp": ts2,
                   "expected_final": V2, "event_time": time.time()}
            try:
                w1n.write(key, V1, w_cl, writer="w1", timestamp=ts1)
                w2n.write(key, V2, w_cl, writer="w2", timestamp=ts2)
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000.0)
                # verify_cl=ALL 时不需要等: ALL 会问遍所有副本, 谁的时间戳最新谁胜出,
                # 与复制是否完成无关。但故障/分区场景下只能用 QUORUM ——
                # 此时若法定人数恰好没包含拿到 W2 的副本, 会读到 W1 而被误判成
                # 定序违例, 实际是可见性假阳性。settle_ms 给复制留出收敛时间。
                if args.settle_ms:
                    time.sleep(args.settle_ms / 1000.0)
                final, fwriter, fwt = vn.read_full(key, v_cl)
            except TRANSIENT_ERRORS as e:
                errors += 1
                rec.update(error_type=type(e).__name__, error_message=str(e)[:200],
                           triggered=False, violation=None)
                trace.write(**rec)
                if errors <= 3:
                    print(f"    [异常 {type(e).__name__}] {str(e)[:90]}")
                continue

            triggered += 1
            violated = (final != V2)
            if violated:
                violations += 1
                if final == V1:
                    kept_v1 += 1      # W1 覆盖了 W2 —— 典型 MW 违例
                else:
                    lost_both += 1    # 两次写都不可见 (可见性问题, 非定序问题)

            rec.update(observed_value=final, winning_writer=fwriter,
                       winning_timestamp=fwt, triggered=True, violation=violated,
                       error_type=None, error_message=None)
            trace.write(**rec)

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] 违例 {violations:>5} "
                      f"({rate:.1%})  异常 {errors}")
    finally:
        trace.close()

    return summarize(
        MODEL, args.scenario, mode,
        args.write_cl, args.read_cl, args.verify_cl,
        iterations, triggered, violations, errors,
        extra=f"W1_wins={kept_v1} neither_visible={lost_both} "
              f"skew_us={args.skew_us if mode == 'skew' else 0} "
              f"path=node{args.write1_node}->node{args.write2_node}",
        topology=topo,
    )


def main():
    p = build_parser(MODEL)
    p.add_argument("--verify-cl", default="ALL",
                   help="收敛后校验用的读级别 (判定最终定序需要看全副本)")
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=500_000,
                   help="构造的客户端时间戳偏移(微秒), 默认 500000 = 500ms。"
                        "MW 的两次写都用显式时间戳, 二者差值恒为 skew, "
                        "不受迭代耗时影响。")
    p.add_argument("--write1-node", type=int, default=2, help="W1 的协调者")
    p.add_argument("--write2-node", type=int, default=3, help="W2 的协调者")
    p.add_argument("--verify-node", type=int, default=1, help="收敛校验读的节点")
    p.add_argument("--settle-ms", type=float, default=0.0,
                   help="校验读之前的收敛等待(毫秒)。verify-cl=ALL 时无需设置; "
                        "降级到 QUORUM 的故障/分区场景建议 1000, "
                        "否则可见性滞后会被误判成定序违例。")
    args = p.parse_args()

    validate(args, {"--write1-node": args.write1_node,
                    "--write2-node": args.write2_node,
                    "--verify-node": args.verify_node})

    w_cl, v_cl = cl(args.write_cl), cl(args.verify_cl)

    print(f"\n=== MW: W={args.write_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  场景={args.scenario} ===")
    print("    注意: MW 由写时间戳定序, read_cl 不参与判定, 仅记录以对齐矩阵。")

    topo = topology.enforce(args.scenario, args.skip_topology_check)
    nodes = connect_nodes(parse_nodes(args.nodes))
    try:
        modes = ["natural", "skew"] if args.mode == "both" else [args.mode]
        for m in modes:
            run_mode(m, nodes, args, w_cl, v_cl, topo)
    finally:
        for nd in nodes.values():
            nd.shutdown()


if __name__ == "__main__":
    main()
