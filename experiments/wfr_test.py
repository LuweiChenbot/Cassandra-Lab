#!/usr/bin/env python3
"""
Writes-follow-reads (WFR) test.

Each iteration uses a fresh key:
  1. setup write value=1 (W_a) through --write-node at --setup-cl
  2. write value=2 (W_b) through --write-node at --write-cl
     (--verify-cl in skew mode)
  3. the client reads through --read-node at --read-cl -> r
  4. the client writes r + 10 through --client-node at --write-cl
  5. read the final value through --verify-node at --verify-cl
Only iterations where the client read 2 (it saw W_b) are checked; a final
value other than 12 is a violation. Reads of 1 are counted as stale_trigger.

Modes:
  natural  driver timestamps; expected 0% violations
  skew     W_b gets timestamp base + --skew-us, so the client's later write
           loses; expected ~100% violations at every consistency level

The skew must exceed the time from W_b to the client's write (about 1.1-1.4 s
with 200 ms injected delay, hence the 5 s default). mean_gap_us and
skew_too_small in the CSV show whether it did.
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
OFFSET = 10          # client's read-modify-write: new = r + OFFSET


def run_mode(mode, nodes, args, w_cl, r_cl, s_cl, v_cl, topo):
    wn = nodes[args.write_node]      # coordinator for W_a and W_b
    rn = nodes[args.read_node]       # client's read
    cn = nodes[args.client_node]     # client's write
    vn = nodes[args.verify_node]     # final verification read
    run_id = time.strftime("%H%M%S")

    wb_cl = v_cl if mode == "skew" else w_cl  # skew: W_b written at verify-cl
    wb_cl_name = args.verify_cl if mode == "skew" else args.write_cl

    print(f"\n--- mode {mode}: W_a@{args.setup_cl} -> W_b@{wb_cl_name} "
          f"-> read node{args.read_node}@{args.read_cl} "
          f"-> client write node{args.client_node}@{args.write_cl} ---")
    if mode == "skew":
        print(f"    client timestamp inversion of {args.skew_us} us "
              f"(W_b timestamp = base+{args.skew_us})")

    trace = TraceWriter(MODEL, args.scenario, mode,
                        args.write_cl, args.read_cl, run_id,
                        enabled=not args.no_trace)

    iterations = triggered = violations = errors = 0
    stale_trigger = read_none = 0
    client_write_lost = 0
    skew_too_small = 0                 # iterations where elapsed time exceeded the skew
    elapsed_sum = elapsed_n = 0        # time from base to the client's write
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

                r = rn.read(key, r_cl)  # the read the client's write depends on
                new_val = (r if r is not None else 0) + OFFSET
                # the client write gets a driver timestamp (~now); noted to check the skew
                t_client = now_us()
                cn.write(key, new_val, w_cl, writer="client")
                # settle only before the verification read, never before the client's read
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
                # saw only W_a: no WFR obligation, counted as a stale read
                stale_trigger += 1
                rec.update(triggered=False, violation=None,
                           note="read saw only W_a (stale); counted as stale_trigger")
                trace.write(**rec)
                continue

            # r == V_B: the client saw W_b, so the WFR obligation applies
            triggered += 1
            violated = (final != new_val)
            if violated:
                violations += 1
                if final == V_B:
                    client_write_lost += 1   # client write lost to W_b's higher timestamp
            rec.update(triggered=True, violation=violated)
            trace.write(**rec)

            if i % step == 0:
                rate = violations / triggered if triggered else 0
                print(f"    [{i:>5}/{args.iterations}] valid {triggered:>5} "
                      f"violations {violations:>5} ({rate:.1%})  "
                      f"stale reads {stale_trigger}  errors {errors}")
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
                   help="read level for the final check; skew mode also writes W_b at this level")
    p.add_argument("--mode", default="both", choices=["natural", "skew", "both"])
    p.add_argument("--skew-us", type=int, default=5_000_000,
                   help="client timestamp skew in microseconds, default 5000000 = 5 s. "
                        "Must exceed the time from W_b to the client's write, "
                        "otherwise the client's own timestamp is later and no violation can occur "
                        "(about 1.1-1.4 s with 200 ms injected delay).")
    p.add_argument("--setup-cl", default="ALL", help="consistency level for the W_a setup write")
    p.add_argument("--write-node", type=int, default=1, help="coordinator for W_a and W_b")
    p.add_argument("--read-node", type=int, default=2, help="node for the client's read")
    p.add_argument("--client-node", type=int, default=3, help="node for the client's write")
    p.add_argument("--verify-node", type=int, default=1, help="node for the final check")
    p.add_argument("--settle-ms", type=float, default=0.0,
                   help="wait before the final check in ms; the client's read is not delayed. "
                        "Use 1000 when verify-cl drops to QUORUM (node failure, partition).")
    args = p.parse_args()

    validate(args, {"--write-node": args.write_node,
                    "--read-node": args.read_node,
                    "--client-node": args.client_node,
                    "--verify-node": args.verify_node})

    w_cl, r_cl = cl(args.write_cl), cl(args.read_cl)
    s_cl, v_cl = cl(args.setup_cl), cl(args.verify_cl)

    print(f"\n=== WFR: W={args.write_cl}  R={args.read_cl}  verify={args.verify_cl}  "
          f"n={args.iterations}  scenario={args.scenario} ===")

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
