#!/usr/bin/env python3
"""
Pre-run topology check.

Before each run, look at the actual cluster (running containers, each node's
view of the ring, tc rules) and exit if it doesn't match --scenario. The
observed snapshot is saved in the CSV's topology column.
"""
import re
import subprocess

CONTAINERS = {1: "cass1", 2: "cass2", 3: "cass3"}
VALID_SCENARIOS = ("normal", "node_failure", "partition", "smoke", "probe")


def _run(cmd, timeout=30):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:                      # docker missing or timed out
        return -1, "", str(e)


def running_containers():
    rc, out, _ = _run(["docker", "ps", "--format", "{{.Names}}"])
    if rc != 0:
        return set()
    return {l.strip() for l in out.splitlines() if l.strip()}


def nodetool_status(container):
    """This node's view of the ring as {ip: 'UN'|'DN'|...}, or None."""
    rc, out, _ = _run(["docker", "exec", container, "nodetool", "status"])
    if rc != 0:
        return None
    ring = {}
    for line in out.splitlines():
        m = re.match(r"^([UD][NLJM])\s+(\d+\.\d+\.\d+\.\d+)", line.strip())
        if m:
            ring[m.group(2)] = m.group(1)
    return ring


_UNIT_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0}
_DELAY_RE = re.compile(
    r"delay\s+([\d.]+)(us|ms|s)\b(?:\s+([\d.]+)(us|ms|s)\b)?")


def parse_netem_delay(qdisc_text):
    """
    Return (delay_ms, jitter_ms) as applied by netem, parsed from `tc qdisc show`,
    so results record the observed delay rather than the requested one.
    """
    for line in qdisc_text.splitlines():
        if "delay" not in line:
            continue
        m = _DELAY_RE.search(line)
        if not m:
            continue
        delay = float(m.group(1)) * _UNIT_MS[m.group(2)]
        jitter = float(m.group(3)) * _UNIT_MS[m.group(4)] if m.group(3) else 0.0
        return round(delay, 3), round(jitter, 3)
    return 0.0, 0.0


def tc_state(container):
    """netem state: delay and jitter, loss band present, partition filter count."""
    _, qdisc, _ = _run(["docker", "exec", container, "tc", "qdisc", "show", "dev", "eth0"])
    _, filt, _ = _run(["docker", "exec", container, "tc", "filter", "show", "dev", "eth0"])
    delay_ms, jitter_ms = parse_netem_delay(qdisc)
    return {
        "delay": delay_ms > 0,
        "delay_ms": delay_ms,
        "jitter_ms": jitter_ms,
        "loss": "loss" in qdisc,
        # band 1:3 is the partition drop band (see scripts/partition.sh)
        "partition_filters": filt.count("flowid 1:3"),
        "qdisc": " | ".join(l.strip() for l in qdisc.splitlines() if l.strip()),
    }


def snapshot():
    """Collect the state of all three containers."""
    running = running_containers()
    snap = {"running": sorted(c for c in CONTAINERS.values() if c in running),
            "nodes": {}}
    for c in CONTAINERS.values():
        if c not in running:
            snap["nodes"][c] = {"running": False}
            continue
        snap["nodes"][c] = {
            "running": True,
            "ring": nodetool_status(c),
            "tc": tc_state(c),
        }
    return snap


def un_counts(snap):
    """Number of UN nodes seen by each running container."""
    out = {}
    for c, d in snap["nodes"].items():
        if d.get("running") and d.get("ring"):
            out[c] = sum(1 for v in d["ring"].values() if v == "UN")
    return out


def format_snapshot(snap):
    lines = []
    for c in sorted(snap["nodes"]):
        d = snap["nodes"][c]
        if not d.get("running"):
            lines.append(f"    {c}: not running")
            continue
        ring = d.get("ring") or {}
        seen = ", ".join(f"{ip}={st}" for ip, st in sorted(ring.items())) or "(unavailable)"
        tc = d["tc"]
        flags = []
        if tc["delay"]:
            flags.append("delay")
        if tc["partition_filters"]:
            flags.append(f"partition filters x{tc['partition_filters']}")
        lines.append(f"    {c}: ring view [{seen}]  netem[{'+'.join(flags) or 'none'}]")
    return "\n".join(lines)


def check_scenario(scenario, strict=True):
    """Compare the live topology with the declared scenario; returns (snapshot, problems)."""
    snap = snapshot()
    running = set(snap["running"])
    uns = un_counts(snap)
    part = sum(d.get("tc", {}).get("partition_filters", 0)
               for d in snap["nodes"].values() if d.get("running"))
    problems = []

    if not running:
        problems.append("no Cassandra container is running; run docker compose up -d --wait first")
        return snap, problems

    if scenario == "normal":
        if len(running) != 3:
            problems.append(
                f"scenario is normal but only {len(running)} containers are running ({sorted(running)}). "
                "All three should be up.")
        bad = {c: n for c, n in uns.items() if n != 3}
        if bad:
            problems.append(f"scenario is normal but these nodes do not see 3 UN: {bad}")
        if part:
            problems.append(
                f"scenario is normal but {part} partition filters are present; "
                "run ./scripts/heal_partition.sh first")

    elif scenario == "node_failure":
        if len(running) == 3:
            problems.append(
                "scenario is node_failure but all three containers are running. "
                "Run docker stop cass3 (or another node) first.")
        elif len(running) < 2:
            problems.append(
                f"scenario is node_failure but only {len(running)} containers are left; "
                "with two nodes down even QUORUM fails at RF=3, which this test does not cover.")
        alive = {c: n for c, n in uns.items() if n >= 3}
        if alive:
            problems.append(
                f"these nodes still see 3 UN: {alive}; "
                "gossip may not have marked the failure yet, retry in 15 s.")

    elif scenario == "partition":
        if len(running) != 3:
            problems.append(
                f"scenario is partition but only {len(running)} containers are running. "
                "In a partition all processes stay up; only their traffic is cut.")
        if part == 0:
            problems.append(
                "scenario is partition but no partition filter was found; "
                "run ./scripts/partition.sh cass3 first")

    elif scenario in ("smoke", "probe"):
        pass                                     # debugging scenarios, not checked

    else:
        problems.append(
            f"unknown scenario {scenario!r}; choose from {', '.join(VALID_SCENARIOS)}")

    return snap, problems


# netem values seen by the last enforce(); common.summarize() writes them to the CSV
LAST_NETEM = {"delay_ms": "", "jitter_ms": ""}


def enforce(scenario, skip=False):
    """Print the topology and exit on a mismatch; returns a one-line summary for the CSV."""
    snap, problems = check_scenario(scenario)
    print(f"  [topology check] scenario={scenario}")
    print(format_snapshot(snap))
    if problems:
        if skip:
            print("  [topology check] mismatch, continuing because of --skip-topology-check:")
            for p in problems:
                print(f"      ! {p}")
        else:
            print("\n  [topology check] live topology does not match the scenario, stopping:")
            for p in problems:
                print(f"      ! {p}")
            print("\n  To run anyway, add --skip-topology-check\n")
            raise SystemExit(2)
    # every node should report the same netem values; otherwise record MIXED[...]
    delays = sorted({d["tc"]["delay_ms"] for d in snap["nodes"].values()
                     if d.get("running") and d.get("tc")})
    jitters = sorted({d["tc"]["jitter_ms"] for d in snap["nodes"].values()
                      if d.get("running") and d.get("tc")})
    LAST_NETEM["delay_ms"] = delays[0] if len(delays) == 1 else f"MIXED{delays}"
    LAST_NETEM["jitter_ms"] = jitters[0] if len(jitters) == 1 else f"MIXED{jitters}"
    if len(delays) > 1:
        print(f"  [topology check] warning: nodes report different netem delays {delays}; "
              f"re-run ./scripts/inject_latency.sh")

    uns = un_counts(snap)
    return (f"running={'/'.join(snap['running'])} "
            f"un={','.join(f'{c}:{n}' for c, n in sorted(uns.items()))} "
            f"delay={LAST_NETEM['delay_ms']}ms")
