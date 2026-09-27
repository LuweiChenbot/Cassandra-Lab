#!/usr/bin/env python3
"""
Monotonic-writes (MW) test.

Each iteration uses a fresh key: write value=1 (W1) through --write1-node, then
value=2 (W2) through --write2-node, both at --write-cl, and read the final value
through --verify-node at --verify-cl (ALL by default). A final value other than
2 is a violation. --read-cl is recorded in the CSV but not used.

Cassandra keeps the write with the highest timestamp, and the Python driver
assigns timestamps on the client, so the outcome depends on timestamps, not on
consistency levels.

Modes:
  natural  driver timestamps (W2 is later); expected 0% violations
  skew     W1 = base + --skew-us, W2 = base, as if two clients' clocks
           disagreed; expected 100% violations at every consistency level

When --verify-cl is below ALL (node failure, partition), set --settle-ms so a
replica that hasn't received W2 yet isn't counted as a violation.
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

    print(f"\n--- mode {mode}: W1@node{args.write1_node} -> W2@node{args.write2_node}, "
          f"verify@node{args.verify_node} CL={args.verify_cl} ---")
    if mode == "skew":
        print(f"    client timestamp inversion of {args.skew_us} us "
              f"(W1 timestamp = base+{args.skew_us}, W2 timestamp = base)")

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
                # Below ALL, a replica still missing W2 would look like an ordering
                # violation, so --settle-ms lets replication finish first.
                if args.settle_ms:
                    time.sleep(args.settle_ms / 1000.0)
                final, fwriter, fwt = vn.read_full(key, v_cl)
            except TRANSIENT_ERRORS as e:
                errors += 1
                rec.update(error_type=type(e).__name__, error_message=str(e)[:200],
                           triggered=False, violation=None)
                trace.write(**rec)
                if errors <= 3:
                    print(f"    [error {type(e).__name__}] {str(e)[:90]}")
                continue

            triggered += 1
            violated = (final != V2)
            if violated:
                violations += 1
                if final == V1:
                    kept_v1 += 1      # W1 overwrote W2: an MW violation
                else:
                    lost_both += 1    # neither write visible (visibility, not order)

            rec.update(observed_value=final, winning_writer=fwriter,
                       winning_timestamp=fwt, triggered=True, violation=violated,
                       error_type=None, error_message=None)
            trace.write(**rec)

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] violations {violations:>5} "
                      f"({rate:.1%})  errors {errors}")
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
                   help="read level for the final check (ALL sees every replica)")
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=500_000,
                   help="client timestamp skew in microseconds, default 500000 = 500 ms. "
                        "Both MW writes use explicit timestamps, so their gap is always the skew "
                        "regardless of iteration time.")
    p.add_argument("--write1-node", type=int, default=2, help="coordinator for W1")
    p.add_argument("--write2-node", type=int, default=3, help="coordinator for W2")
    p.add_argument("--verify-node", type=int, default=1, help="node for the final check")
    p.add_argument("--settle-ms", type=float, default=0.0,
                   help="wait before the final check in ms. Not needed with verify-cl=ALL; "
                        "use 1000 with QUORUM (node failure, partition), "
                        "or replication lag is counted as an ordering violation.")
    args = p.parse_args()

    validate(args, {"--write1-node": args.write1_node,
                    "--write2-node": args.write2_node,
                    "--verify-node": args.verify_node})

    w_cl, v_cl = cl(args.write_cl), cl(args.verify_cl)

    print(f"\n=== MW: W={args.write_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  scenario={args.scenario} ===")
    print("    note: MW is ordered by write timestamps; read_cl is recorded but not used.")

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
