#!/usr/bin/env python3
"""
wfr_test.py -- Writes Follow Reads (写跟随读)

定义 (Terry et al., Bayou 会话保证)
----------------------------------
客户端在会话中读到了某个写 W1 的效果, 那么它之后发出的写 W2 必须在
*所有*副本上被排在 W1 之后。

注意这条保证约束的是"定序", 不是"可见性"。它只管客户端*确实看到过*的写;
客户端没看到的写不在它的约束范围内。这个区别决定了下面的判定口径。

实验构造
--------
每次迭代用一个全新的 key:

    1. [装置] 经 node1 以 CL=setup_cl(ALL) 写 value=1     -- 记为 W_a
    2. W_b:   经 node1 以 CL=... 写 value=2
    3. 客户端经 node2 以 CL=read_cl 读 -> r
    4. 客户端经 node3 以 CL=write_cl 写 value = r + 10     -- 读-改-写
    5. 经 node1 以 CL=ALL 读最终值 -> final

    严格 WFR 违例的判定口径:
        仅统计 r == 2 的迭代 (此时客户端确实观测到了 W_b),
        若 final != 12, 说明客户端读后发出的写没有排在 W_b 之后 => 违例。

    为什么不把 r == 1 也算违例:
        r == 1 时客户端只看到了 W_a, 它写出的 11 时间戳高于 W_a,
        在所有副本上都排在 W_a 之后 —— 严格 WFR 是满足的。
        这种情况属于"读到了陈旧值导致更新丢失", 是可见性问题
        (RYW/MR 的范畴), 单独记为 stale_trigger 指标, 不计入 WFR 违例。

两种模式
--------
natural:
    W_b 用 CL=write_cl 写, 时间戳由驱动生成。
    预期: 严格违例恒为 0 (客户端的写时间戳必然最新);
          但 stale_trigger 随 CL 变化 —— 这一维才是 CL 敏感的。

skew:
    W_a 时间戳 = base, W_b 时间戳 = base + skew (模拟 W_b 走的协调者时钟快),
    且 W_b 用 CL=ALL 写以保证客户端一定读得到 (把可见性这一维固定住,
    单独隔离时间戳的影响)。
    客户端读到 2, 随后写 12 —— 用驱动的自然时间戳, 约等于 base, 落后于 W_b。
    LWW 直接丢弃客户端的写, final 仍是 2。
    预期: 所有 CL 组合下严格违例率都接近 100%。

结论指向
--------
和 MW 一样, WFR 在 Cassandra 里是由*写时间戳*定序的, 与 R/W 一致性级别
正交。R+W>N 能修好 RYW 和 MR, 但修不了 MW 和 WFR。
"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    connect_nodes, cl, summarize, build_parser, parse_nodes, now_us,
    TRANSIENT_ERRORS,
)

MODEL = "wfr"
V_A, V_B = 1, 2
OFFSET = 10          # 客户端的读-改-写: new = r + OFFSET


def run_mode(mode, nodes, args, w_cl, r_cl, s_cl, v_cl):
    wn = nodes[args.write_node]      # W_a / W_b 的协调者
    rn = nodes[args.read_node]       # 客户端读的节点
    cn = nodes[args.client_node]     # 客户端写的节点
    vn = nodes[args.verify_node]     # 收敛校验
    run_id = time.strftime("%H%M%S")

    wb_cl = v_cl if mode == "skew" else w_cl      # skew 模式固定用 ALL 写 W_b
    wb_cl_name = args.verify_cl if mode == "skew" else args.write_cl

    print(f"\n--- 模式 {mode}: W_a@{args.setup_cl} -> W_b@{wb_cl_name} "
          f"-> read node{args.read_node}@{args.read_cl} "
          f"-> client write node{args.client_node}@{args.write_cl} ---")
    if mode == "skew":
        print(f"    构造时钟偏移 {args.skew_us} us (W_b 时间戳 = base+{args.skew_us})")

    iterations = triggered = violations = errors = 0
    stale_trigger = read_none = 0
    client_write_lost = 0
    skew_too_small = 0                 # 偏移被迭代耗时吃掉, 该次迭代不可能违例
    elapsed_sum = elapsed_n = 0        # base -> 客户端写 的实际间隔
    step = max(1, args.iterations // 10)

    for i in range(1, args.iterations + 1):
        iterations += 1
        key = f"{MODEL}_{mode}_{run_id}_{i}"
        try:
            base = now_us()
            if mode == "natural":
                wn.write(key, V_A, s_cl, writer="Wa")
                wn.write(key, V_B, wb_cl, writer="Wb")
            else:
                wn.write(key, V_A, s_cl, writer="Wa", timestamp=base)
                wn.write(key, V_B, wb_cl, writer="Wb", timestamp=base + args.skew_us)

            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)

            r = rn.read(key, r_cl)                         # 触发 WFR 义务的那次读
            new_val = (r if r is not None else 0) + OFFSET
            # 客户端这次写用驱动的自然时间戳(约等于此刻墙上时钟)。
            # 记录它是为了判定: 偏移 skew 是否大于 "W_b 到本次写" 的实际间隔 ——
            # 小于的话客户端时间戳反而更高, 违例根本无法成立。
            t_client = now_us()
            cn.write(key, new_val, w_cl, writer="client")  # 读-改-写
            final = vn.read(key, v_cl)                     # 收敛后的定序结果
            elapsed_sum += t_client - base
            elapsed_n += 1
            if mode == "skew" and t_client >= base + args.skew_us:
                skew_too_small += 1
        except TRANSIENT_ERRORS as e:
            errors += 1
            if errors <= 3:
                print(f"    [异常 {type(e).__name__}] {str(e)[:90]}")
            continue

        if r is None:
            read_none += 1
            continue
        if r == V_A:
            # 只看到了 W_a: 严格 WFR 不受约束, 但这是实际危险的"陈旧读"
            stale_trigger += 1
            continue

        # r == V_B: 客户端确实观测到了 W_b, WFR 义务成立
        triggered += 1
        if final != new_val:
            violations += 1
            if final == V_B:
                client_write_lost += 1   # 客户端的写被 W_b 的高时间戳吃掉了

        if i % step == 0:
            rate = violations / triggered if triggered else 0
            print(f"    [{i:>5}/{args.iterations}] 有效 {triggered:>5} "
                  f"违例 {violations:>5} ({rate:.1%})  "
                  f"陈旧读 {stale_trigger}  异常 {errors}")

    return summarize(
        MODEL, args.scenario, mode,
        args.write_cl, args.read_cl, args.verify_cl,
        iterations, triggered, violations, errors,
        extra=f"stale_trigger={stale_trigger} read_none={read_none} "
              f"client_write_lost={client_write_lost} "
              f"skew_us={args.skew_us if mode == 'skew' else 0} "
              f"mean_gap_us={int(elapsed_sum / elapsed_n) if elapsed_n else 0} "
              f"skew_too_small={skew_too_small} "
              f"setup_cl={args.setup_cl}",
    )


def main():
    p = build_parser(MODEL)
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=5_000_000,
                   help="构造的时钟偏移(微秒), 默认 5000000 = 5s。"
                        "必须大于 W_b 到客户端后续写之间的实际间隔, "
                        "否则客户端的自然时间戳反而更高, 违例无法成立。"
                        "注入 200ms 复制延迟后该间隔实测约 1.1-1.4s。")
    p.add_argument("--setup-cl", default="ALL", help="W_a 装置写的一致性级别")
    p.add_argument("--write-node", type=int, default=1, help="W_a / W_b 的协调者")
    p.add_argument("--read-node", type=int, default=2, help="客户端读的节点")
    p.add_argument("--client-node", type=int, default=3, help="客户端写的节点")
    p.add_argument("--verify-node", type=int, default=1, help="收敛校验读的节点")
    args = p.parse_args()

    w_cl, r_cl = cl(args.write_cl), cl(args.read_cl)
    s_cl, v_cl = cl(args.setup_cl), cl(args.verify_cl)

    print(f"\n=== WFR: W={args.write_cl}  R={args.read_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  场景={args.scenario} ===")

    nodes = connect_nodes(parse_nodes(args.nodes))
    try:
        modes = ["natural", "skew"] if args.mode == "both" else [args.mode]
        for m in modes:
            run_mode(m, nodes, args, w_cl, r_cl, s_cl, v_cl)
    finally:
        for nd in nodes.values():
            nd.shutdown()


if __name__ == "__main__":
    main()
