#!/usr/bin/env python3
"""
Monotonic-reads (MR) test.

Each iteration uses a fresh key:
  1. seed write value=1 at --seed-cl (ALL by default, so every replica has it)
  2. write value=2 through --write-node at --write-cl
  3. read through --read1-node, then through --read2-node, both at --read-cl
A violation is the second read returning an older value than the first
(e.g. 2 then 1). The seed CL is test setup, not a variable under test.

Expected with RF=3: ONE/ONE can violate; QUORUM/QUORUM and ALL/ONE cannot.
ONE/QUORUM (W+R = 3) is not guaranteed.
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

MODEL = "mr"
SEED_VALUE = 1
NEW_VALUE = 2


def rank(v):
    """Order values for comparison; a missing row (None) is the oldest."""
    return -1 if v is None else v


def main():
    p = build_parser(MODEL)
    p.add_argument("--seed-cl", default="ALL",
                   help="seed write consistency level (test setup; QUORUM with a node down)")
    p.add_argument("--write-node", type=int, default=1, help="coordinator for both writes")
    p.add_argument("--read1-node", type=int, default=1, help="node for the first read")
    p.add_argument("--read2-node", type=int, default=2, help="node for the second read")
    args = p.parse_args()

    validate(args, {"--write-node": args.write_node,
                    "--read1-node": args.read1_node,
                    "--read2-node": args.read2_node})

    w_cl, r_cl, s_cl = cl(args.write_cl), cl(args.read_cl), cl(args.seed_cl)
    run_id = time.strftime("%H%M%S")

    print(f"\n=== MR: seed={args.seed_cl}  W={args.write_cl}@node{args.write_node}  "
          f"R={args.read_cl} (node{args.read1_node} -> node{args.read2_node})  "
          f"n={args.iterations}  scenario={args.scenario} ===")

    topo = topology.enforce(args.scenario, args.skip_topology_check)
    nodes = connect_nodes(parse_nodes(args.nodes))
    wn = nodes[args.write_node]
    r1n, r2n = nodes[args.read1_node], nodes[args.read2_node]

    trace = TraceWriter(MODEL, args.scenario, "natural",
                        args.write_cl, args.read_cl, run_id,
                        enabled=not args.no_trace)

    iterations = triggered = violations = errors = 0
    went_none = went_older = 0
    step = max(1, args.iterations // 10)

    try:
        for i in range(1, args.iterations + 1):
            iterations += 1
            key = f"{MODEL}_{run_id}_{i}"
            rec = {"run_id": run_id, "iteration": i, "model": MODEL,
                   "scenario": args.scenario, "mode": "natural",
                   "write_cl": args.write_cl, "read_cl": args.read_cl,
                   "seed_cl": args.seed_cl,
                   "write_node": wn.name, "read1_node": r1n.name,
                   "read1_addr": r1n.address, "read2_node": r2n.name,
                   "read2_addr": r2n.address,
                   "key": key, "written_value": NEW_VALUE,
                   "event_time": time.time()}
            try:
                wn.write(key, SEED_VALUE, s_cl, writer="seed")
                wn.write(key, NEW_VALUE, w_cl, writer="update")
                if args.sleep_ms:
                    time.sleep(args.sleep_ms / 1000.0)
                r1 = r1n.read(key, r_cl)
                r2 = r2n.read(key, r_cl)
            except TRANSIENT_ERRORS as e:
                errors += 1
                rec.update(error_type=type(e).__name__, error_message=str(e)[:200],
                           triggered=False, violation=None)
                trace.write(**rec)
                if errors <= 3:
                    print(f"    [error {type(e).__name__}] {str(e)[:90]}")
                continue

            rec.update(read1_value=r1, read2_value=r2,
                       error_type=None, error_message=None)

            # MR only applies once the first read has returned a value
            if r1 is None:
                rec.update(triggered=False, violation=None,
                           note="first read saw nothing; obligation not triggered")
                trace.write(**rec)
                continue
            triggered += 1

            violated = rank(r2) < rank(r1)
            if violated:
                violations += 1
                if r2 is None:
                    went_none += 1
                else:
                    went_older += 1
            rec.update(triggered=True, violation=violated)
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
        extra=f"seed_cl={args.seed_cl} back_to_none={went_none} "
              f"back_to_older={went_older} "
              f"path=node{args.read1_node}->node{args.read2_node}",
        topology=topo,
    )


if __name__ == "__main__":
    main()
