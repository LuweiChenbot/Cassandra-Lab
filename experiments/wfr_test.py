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

    1. [装置] 经 write_node 以 CL=setup_cl 写 value=1        -- 记为 W_a
    2. W_b:   经 write_node 写 value=2
    3. 客户端经 read_node 以 CL=read_cl 读 -> r
    4. 客户端经 client_node 以 CL=write_cl 写 value = r + 10  -- 读-改-写
    5. 经 verify_node 以 CL=verify_cl 读最终值 -> final

    严格 WFR 违例的判定口径:
        仅统计 r == 2 的迭代 (此时客户端确实观测到了 W_b),
        若 final != 12, 说明客户端读后发出的写没有排在 W_b 之后 => 违例。

    为什么不把 r == 1 也算违例:
        r == 1 时客户端只看到了 W_a, 它写出的 11 时间戳高于 W_a,
        在所有副本上都排在 W_a 之后 —— 严格 WFR 是满足的。
        这种情况属于"读到陈旧值导致更新丢失", 是可见性问题(RYW/MR 范畴),
        单独记为 stale_trigger 指标, 不计入 WFR 违例。

时间戳来源 (与 mw_test.py 同一前提, 必须说准)
---------------------------------------------
cassandra-driver 默认挂着 MonotonicTimestampGenerator, 普通写的时间戳
由**客户端**生成并随请求发送, 协调者的系统时钟不参与 (本项目已实测确认,
详见 mw_test.py 的说明)。所以下面 skew 模式构造的是
**client-supplied timestamp inversion**, 对应"不同机器上的客户端进程
因 NTP 失步而产生倒挂的时间戳", 不是"协调者之间的时钟偏移"。

两种模式
--------
natural:
    W_b 用 CL=write_cl 写, 时间戳由驱动生成。
    预期: 严格违例恒为 0 (客户端后发的写时间戳必然最新);
          但 stale_trigger 随 CL 变化 —— 这一维才是 CL 敏感的。

skew:
    W_a 时间戳 = base, W_b 时间戳 = base + skew (写出 W_b 的客户端时钟快),
    且 W_b 用 CL=verify_cl(ALL) 写以保证客户端一定读得到 —— 把可见性这一维
    固定住, 单独隔离时间戳的影响。
    客户端读到 2, 随后写 12 —— 用驱动的自然时间戳, 落后于 W_b。
    LWW 直接丢弃客户端的写, final 仍是 2。
    预期: 所有 CL 组合下严格违例率都接近 100%。

    !! 关键前提: skew 必须大于 "W_b 到客户端后续写" 的实际间隔。
       否则客户端的自然时间戳反而更高, 违例根本无法成立。
       注入 200ms 复制延迟后该间隔实测约 1.1-1.4 秒, 所以默认 skew 取 5 秒。
       脚本会记录 mean_gap_us 和 skew_too_small, 让这个前提可验证。
       换句话说, WFR 违例成立当且仅当:
           客户端间时钟偏移 > 被观测的写到后续写之间的实际间隔

结论指向
--------
和 MW 一样, WFR 在 Cassandra 里是由*写时间戳*定序的, 与 R/W 一致性级别
正交。R+W>N 能修好 RYW 和 MR, 但修不了 MW 和 WFR。
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

MODEL = "wfr"
V_A, V_B = 1, 2
OFFSET = 10          # 客户端的读-改-写: new = r + OFFSET


def run_mode(mode, nodes, args, w_cl, r_cl, s_cl, v_cl, topo):
    wn = nodes[args.write_node]      # W_a / W_b 的协调者
    rn = nodes[args.read_node]       # 客户端读的节点
    cn = nodes[args.client_node]     # 客户端写的节点
    vn = nodes[args.verify_node]     # 收敛校验
    run_id = time.strftime("%H%M%S")

    wb_cl = v_cl if mode == "skew" else w_cl      # skew 模式固定用强 CL 写 W_b
    wb_cl_name = args.verify_cl if mode == "skew" else args.write_cl

    print(f"\n--- 模式 {mode}: W_a@{args.setup_cl} -> W_b@{wb_cl_name} "
          f"-> read node{args.read_node}@{args.read_cl} "
          f"-> client write node{args.client_node}@{args.write_cl} ---")
    if mode == "skew":
        print(f"    构造客户端时间戳倒挂 {args.skew_us} us "
              f"(W_b 时间戳 = base+{args.skew_us})")

    trace = TraceWriter(MODEL, args.scenario, mode,
                        args.write_cl, args.read_cl, run_id,
                        enabled=not args.no_trace)

    iterations = triggered = violations = errors = 0
    stale_trigger = read_none = 0
    client_write_lost = 0
    skew_too_small = 0                 # 偏移被迭代耗时吃掉, 该次迭代不可能违例
    elapsed_sum = elapsed_n = 0        # base -> 客户端写 的实际间隔
    step = max(1, args.iterations // 10)

    try:
        for i in range(1, args.iterations + 1):
            iterations += 1
            key = f"{MODEL}_{mode}_{run_id}_{i}"
            base = now_us()
            ts_a = base if mode == "skew" else None
            ts_b = base + args.skew_us if mode == "skew" else None
            rec = {"run_id": run_id, "iteration": i, "model": MODEL,
                   "scenario": args.scenario, "mode": mode,
                   "write_cl": args.write_cl, "read_cl": args.read_cl,
                   "setup_cl": args.setup_cl, "verify_cl": args.verify_cl,
                   "write_node": wn.name, "read_node": rn.name,
                   "read_addr": rn.address, "client_node": cn.name,
                   "client_addr": cn.address, "verify_node": vn.name,
                   "key": key, "wa_value": V_A, "wb_value": V_B,
                   "wa_timestamp": ts_a, "wb_timestamp": ts_b,
                   "event_time": time.time()}
            try:
                wn.write(key, V_A, s_cl, writer="Wa", timestamp=ts_a)
                wn.write(key, V_B, wb_cl, writer="Wb", timestamp=ts_b)
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000.0)

                r = rn.read(key, r_cl)                        # 触发 WFR 义务的那次读
                new_val = (r if r is not None else 0) + OFFSET
                # 客户端这次写用驱动的自然时间戳(约等于此刻墙上时钟)。
                # 记录它是为了判定 skew 是否大于 "W_b -> 本次写" 的实际间隔。
                t_client = now_us()
                cn.write(key, new_val, w_cl, writer="client")
                # 收敛等待只放在校验读之前, 不影响上面那次"触发 WFR 义务的读" ——
                # 后者必须立刻读才能测出陈旧读率。理由同 mw_test.py。
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

            elapsed_sum += t_client - base
            elapsed_n += 1
            too_small = (mode == "skew" and t_client >= base + args.skew_us)
            if too_small:
                skew_too_small += 1

            rec.update(observed_read=r, client_wrote=new_val,
                       client_timestamp_approx=t_client,
                       gap_us=t_client - base, skew_too_small=too_small,
                       observed_final=final, winning_writer=fwriter,
                       winning_timestamp=fwt,
                       error_type=None, error_message=None)

            if r is None:
                read_none += 1
                rec.update(triggered=False, violation=None,
                           note="read saw nothing; WFR obligation not triggered")
                trace.write(**rec)
                continue
            if r == V_A:
                # 只看到 W_a: 严格 WFR 不受约束, 但这是实际危险的"陈旧读"
                stale_trigger += 1
                rec.update(triggered=False, violation=None,
                           note="read saw only W_a (stale); counted as stale_trigger")
                trace.write(**rec)
                continue

            # r == V_B: 客户端确实观测到了 W_b, WFR 义务成立
            triggered += 1
            violated = (final != new_val)
            if violated:
                violations += 1
                if final == V_B:
                    client_write_lost += 1   # 客户端的写被 W_b 的高时间戳吃掉
            rec.update(triggered=True, violation=violated)
            trace.write(**rec)

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] 有效 {triggered:>5} "
                      f"违例 {violations:>5} ({rate:.1%})  "
                      f"陈旧读 {stale_trigger}  异常 {errors}")
    finally:
        trace.close()

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
        topology=topo,
    )


def main():
    p = build_parser(MODEL)
    p.add_argument("--verify-cl", default="ALL",
                   help="收敛后校验用的读级别; skew 模式也用它写 W_b")
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=5_000_000,
                   help="构造的客户端时间戳偏移(微秒), 默认 5000000 = 5s。"
                        "必须大于 W_b 到客户端后续写之间的实际间隔, "
                        "否则客户端的自然时间戳反而更高, 违例无法成立。"
                        "注入 200ms 复制延迟后该间隔实测约 1.1-1.4s。")
    p.add_argument("--setup-cl", default="ALL", help="W_a 装置写的一致性级别")
    p.add_argument("--write-node", type=int, default=1, help="W_a / W_b 的协调者")
    p.add_argument("--read-node", type=int, default=2, help="客户端读的节点")
    p.add_argument("--client-node", type=int, default=3, help="客户端写的节点")
    p.add_argument("--verify-node", type=int, default=1, help="收敛校验读的节点")
    p.add_argument("--settle-ms", type=float, default=0.0,
                   help="校验读之前的收敛等待(毫秒), 不影响触发 WFR 义务的那次读。"
                        "verify-cl 降级到 QUORUM 的故障/分区场景建议 1000。")
    args = p.parse_args()

    validate(args, {"--write-node": args.write_node,
                    "--read-node": args.read_node,
                    "--client-node": args.client_node,
                    "--verify-node": args.verify_node})

    w_cl, r_cl = cl(args.write_cl), cl(args.read_cl)
    s_cl, v_cl = cl(args.setup_cl), cl(args.verify_cl)

    print(f"\n=== WFR: W={args.write_cl}  R={args.read_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  场景={args.scenario} ===")

    topo = topology.enforce(args.scenario, args.skip_topology_check)
    nodes = connect_nodes(parse_nodes(args.nodes))
    try:
        modes = ["natural", "skew"] if args.mode == "both" else [args.mode]
        for m in modes:
            run_mode(m, nodes, args, w_cl, r_cl, s_cl, v_cl, topo)
    finally:
        for nd in nodes.values():
            nd.shutdown()


if __name__ == "__main__":
    main()
