#!/usr/bin/env python3
"""
Read-your-writes (RYW) test.

Each iteration uses a fresh key: write value i through --write-node at
--write-cl, then immediately read it through --read-node at --read-cl.
Reading anything other than i (usually None) is a violation. Writing and
reading through different coordinators models a client that reconnects to
another node.

Expected with RF=3: ONE/ONE and ONE/QUORUM can violate (W+R <= 3);
QUORUM/QUORUM and ALL/ONE cannot (W+R > 3).
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
    p.add_argument("--write-node", type=int, default=1, help="node for the write")
    p.add_argument("--read-node", type=int, default=2, help="node for the read")
    args = p.parse_args()

    validate(args, {"--write-node": args.write_node,
                    "--read-node": args.read_node})

    w_cl, r_cl = cl(args.write_cl), cl(args.read_cl)
    run_id = time.strftime("%H%M%S")

    print(f"\n=== RYW: W={args.write_cl}@node{args.write_node}  "
          f"R={args.read_cl}@node{args.read_node}  "
          f"n={args.iterations}  scenario={args.scenario} ===")

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
                    print(f"    [error {type(e).__name__}] {str(e)[:90]}")
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
                print(f"    [{i:>5}/{args.iterations}] violations {violations:>5} "
                      f"({rate:.1%})  errors {errors}")
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
