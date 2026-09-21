#!/usr/bin/env python3
"""
ryw_diagnostic.py -- Diagnostic companion for ryw_test.py.

Purpose:
  * Keep WRITE and READ coordinators pinned exactly as in the normal RYW test.
  * Add client-side high-resolution timing.
  * Enable Cassandra query tracing for a SMALL number of trials.
  * Print and save the coordinator and trace events for WRITE and READ.

IMPORTANT:
  Query tracing perturbs timing. Use this script for diagnosis only, not for
  measuring the official violation rate.
"""

import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import topology
from common import connect_nodes, cl, parse_nodes, validate, TRANSIENT_ERRORS


def execute_write_traced(node, key, value, consistency):
    """Execute one write with Cassandra query tracing enabled."""
    bs = node._ins.bind((key, value, node.name))
    bs.consistency_level = consistency

    t0 = time.perf_counter_ns()
    result = node.session.execute(bs, trace=True)
    t1 = time.perf_counter_ns()

    return result, t0, t1


def execute_read_traced(node, key, consistency):
    """Execute one read with Cassandra query tracing enabled."""
    bs = node._sel.bind((key,))
    bs.consistency_level = consistency

    t2 = time.perf_counter_ns()
    result = node.session.execute(bs, trace=True)
    row = result.one()
    t3 = time.perf_counter_ns()


    if row is None:
        value, writer, wt = None, None, None
    else:
        value, writer, wt = row.value, row.writer, row.wt

    return value, writer, wt, result, t2, t3


def trace_to_dict(trace):
    if trace is None:
        return {"coordinator": None, "events": []}

    events = []
    for event in trace.events:
        events.append({
            "datetime": str(event.datetime),
            "source": str(event.source),
            "source_elapsed_us": event.source_elapsed,
            "description": event.description,
            "thread_name": event.thread_name,
        })

    return {
        "coordinator": str(trace.coordinator),
        "duration_us": trace.duration,
        "request_type": trace.request_type,
        "events": events,
    }


def print_trace(label, trace):
    print(f"\n      {label}")
    if trace is None:
        print("        <no query trace>")
        return

    print(f"        coordinator = {trace.coordinator}")
    print(f"        duration    = {trace.duration}")
    for event in trace.events:
        print(
            f"        {str(event.source):>15}  "
            f"{str(event.source_elapsed):>10} us  "
            f"{event.description}"
        )


def main():
    p = argparse.ArgumentParser(
        description="Small traced RYW diagnostic; do not use for official rates."
    )
    p.add_argument("--write-cl", default="ONE")
    p.add_argument("--read-cl", default="QUORUM")
    p.add_argument("-n", "--iterations", type=int, default=20)
    p.add_argument("--write-node", type=int, default=1)
    p.add_argument("--read-node", type=int, default=2)
    p.add_argument("--nodes", default="1,2,3")
    p.add_argument("--scenario", default="normal")
    p.add_argument("--sleep-ms", type=float, default=0.0)
    p.add_argument("--skip-topology-check", action="store_true")
    p.add_argument(
        "--output",
        default=None,
        help="JSONL output path (default: results/diagnostics/...)",
    )
    args = p.parse_args()

    validate(args, {
        "--write-node": args.write_node,
        "--read-node": args.read_node,
    })

    w_cl = cl(args.write_cl)
    r_cl = cl(args.read_cl)

    run_id = time.strftime("%Y%m%d_%H%M%S")
    topology_label = topology.enforce(
        args.scenario, args.skip_topology_check
    )

    nodes = connect_nodes(parse_nodes(args.nodes))
    wn = nodes[args.write_node]
    rn = nodes[args.read_node]

    if args.output:
        output_path = args.output
    else:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out_dir = os.path.join(root, "results", "diagnostics")
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(
            out_dir,
            f"ryw_diag_W{args.write_cl}_R{args.read_cl}_"
            f"w{args.write_node}_r{args.read_node}_{run_id}.jsonl",
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    print("\n=== RYW DIAGNOSTIC (Cassandra tracing ON) ===")
    print("Do NOT use this run to estimate the official violation rate.")
    print(f"platform       = {platform.platform()}")
    print(f"python         = {platform.python_version()}")
    print(f"topology       = {topology_label}")
    print(f"WRITE          = {args.write_cl} @ {wn.name} ({wn.address})")
    print(f"READ           = {args.read_cl} @ {rn.name} ({rn.address})")
    print(f"iterations     = {args.iterations}")
    print(f"sleep_ms       = {args.sleep_ms}")
    print(f"output         = {output_path}")

    violations = 0

    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            for i in range(1, args.iterations + 1):
                key = f"ryw_diag_{run_id}_{i}"
                rec = {
                    "run_id": run_id,
                    "iteration": i,
                    "utc_time": datetime.now(timezone.utc).isoformat(),
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "scenario": args.scenario,
                    "topology": topology_label,
                    "write_cl": args.write_cl,
                    "read_cl": args.read_cl,
                    "write_node": wn.name,
                    "write_addr": wn.address,
                    "read_node": rn.name,
                    "read_addr": rn.address,
                    "key": key,
                    "written_value": i,
                    "sleep_ms": args.sleep_ms,
                }

                try:
                    wresult, t0, t1 = execute_write_traced(
                        wn, key, i, w_cl
                    )

                    if args.sleep_ms:
                        time.sleep(args.sleep_ms / 1000.0)

                    got, writer, wt, rresult, t2, t3 = execute_read_traced(
                        rn, key, r_cl
                    )

                    wtrace = wresult.get_query_trace(max_wait_sec=10.0)
                    rtrace = rresult.get_query_trace(max_wait_sec=10.0)
                    violated = got != i
                    violations += int(violated)

                    rec.update({
                        "observed_value": got,
                        "observed_writer": writer,
                        "write_timestamp": wt,
                        "violation": violated,
                        "write_ms": (t1 - t0) / 1_000_000.0,
                        "gap_ms": (t2 - t1) / 1_000_000.0,
                        "read_ms": (t3 - t2) / 1_000_000.0,
                        "total_ms": (t3 - t0) / 1_000_000.0,
                        "write_trace": trace_to_dict(wtrace),
                        "read_trace": trace_to_dict(rtrace),
                        "error_type": None,
                        "error_message": None,
                    })

                    print(
                        f"\n[{i:02d}/{args.iterations}] "
                        f"written={i} observed={got} "
                        f"violation={violated}"
                    )
                    print(
                        f"      timing: write={rec['write_ms']:.3f} ms  "
                        f"gap={rec['gap_ms']:.3f} ms  "
                        f"read={rec['read_ms']:.3f} ms"
                    )
                    print_trace("WRITE TRACE", wtrace)
                    print_trace("READ TRACE", rtrace)

                except TRANSIENT_ERRORS as e:
                    rec.update({
                        "violation": None,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    })
                    print(
                        f"\n[{i:02d}/{args.iterations}] "
                        f"ERROR {type(e).__name__}: {e}"
                    )

                fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                fh.flush()

    finally:
        for node in nodes.values():
            node.shutdown()

    print("\n=== diagnostic summary ===")
    print(f"violations = {violations}/{args.iterations}")
    print(f"saved      = {output_path}")


if __name__ == "__main__":
    main()
