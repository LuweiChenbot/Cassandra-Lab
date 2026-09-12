#!/usr/bin/env python3
"""
ryw_test.py -- Read-Your-Writes (读己之写)

定义
----
同一个客户端写入 x 后, 它之后的任何读都必须看到 x 或更新的值。

实验构造
--------
每次迭代用一个全新的 key:

    1. 客户端经 write_node 写 value=i          (CL = write_cl)
    2. 同一客户端立刻经 read_node 读同一个 key  (CL = read_cl)
    3. 读回值 != i  =>  RYW 违例

为什么写和读要走不同节点
------------------------
如果走同一个协调者, 由于 RF=3 时协调者自己就是副本, CL=ONE 的写会先落本地,
紧接着的本地读必然命中 —— 违例被掩盖, 实验失去意义。
换节点读模拟的是真实场景: 客户端重连、负载均衡器切换、驱动故障转移。
RYW 这条会话保证要防的正是这种情况。

为什么用全新 key
----------------
新 key 下"没复制到"表现为读回 None, 判定绝对无歧义 —— 不需要区分
"读到旧值"还是"读到新值", 只要不是 i 就是违例。

预期
----
    W=ONE    R=ONE     违例 (1+1 <= 3, 法定人数不相交)
    W=ONE    R=QUORUM  部分违例 (1+2 <= 3, 恰在边界)
    W=QUORUM R=QUORUM  不违例 (2+2 > 3, 必然相交)
    W=ALL    R=ONE     不违例 (3+1 > 3)
"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import topology
from common import (
    connect_nodes, cl, summarize, build_parser, parse_nodes, validate,
    TraceWriter, TRANSIENT_ERRORS,
)

MODEL = "ryw"


def main():
    p = build_parser(MODEL)
    p.add_argument("--write-node", type=int, default=1, help="执行写的节点")
    p.add_argument("--read-node", type=int, default=2, help="执行读的节点")
    args = p.parse_args()

    validate(args, {"--write-node": args.write_node,
                    "--read-node": args.read_node})

    w_cl, r_cl = cl(args.write_cl), cl(args.read_cl)
    run_id = time.strftime("%H%M%S")

    print(f"\n=== RYW: W={args.write_cl}@node{args.write_node}  "
          f"R={args.read_cl}@node{args.read_node}  "
          f"n={args.iterations}  场景={args.scenario} ===")

    topo = topology.enforce(args.scenario, args.skip_topology_check)
    nodes = connect_nodes(parse_nodes(args.nodes))
    wn, rn = nodes[args.write_node], nodes[args.read_node]

    trace = TraceWriter(MODEL, args.scenario, "natural",
                        args.write_cl, args.read_cl, run_id,
                        enabled=not args.no_trace)

    iterations = triggered = violations = errors = 0
    stale_none = stale_old = 0
    step = max(1, args.iterations // 10)

    try:
        for i in range(1, args.iterations + 1):
            iterations += 1
            key = f"{MODEL}_{run_id}_{i}"
            rec = {"run_id": run_id, "iteration": i, "model": MODEL,
                   "scenario": args.scenario, "mode": "natural",
                   "write_cl": args.write_cl, "read_cl": args.read_cl,
                   "write_node": wn.name, "write_addr": wn.address,
                   "read_node": rn.name, "read_addr": rn.address,
                   "key": key, "written_value": i,
                   "event_time": time.time()}
            try:
                wn.write(key, i, w_cl)
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000.0)
                got, writer, wt = rn.read_full(key, r_cl)
            except TRANSIENT_ERRORS as e:
                errors += 1
                rec.update(error_type=type(e).__name__, error_message=str(e)[:200],
                           triggered=False, violation=None)
                trace.write(**rec)
                if errors <= 3:
                    print(f"    [异常 {type(e).__name__}] {str(e)[:90]}")
                continue

            triggered += 1
            violated = (got != i)
            if violated:
                violations += 1
                if got is None:
                    stale_none += 1
                else:
                    stale_old += 1

            rec.update(observed_value=got, write_timestamp=wt,
                       triggered=True, violation=violated,
                       error_type=None, error_message=None)
            trace.write(**rec)

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] 违例 {violations:>5} "
                      f"({rate:.1%})  异常 {errors}")
    finally:
        trace.close()
        for nd in nodes.values():
            nd.shutdown()

    summarize(
        MODEL, args.scenario, "natural",
        args.write_cl, args.read_cl, "-",
        iterations, triggered, violations, errors,
        extra=f"read_none={stale_none} read_stale={stale_old} "
              f"w_node={args.write_node} r_node={args.read_node}",
        topology=topo,
    )


if __name__ == "__main__":
    main()
