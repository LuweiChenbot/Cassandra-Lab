#!/usr/bin/env python3
"""
Shared infrastructure for the consistency experiments.

Each Node is a CQL session pinned to one Cassandra node. The containers publish
CQL on host ports 9042/9043/9044, and WhiteListRoundRobinPolicy(["127.0.0.1"])
stops the driver from routing requests through any other coordinator.
connect_nodes() checks the pinning through system.local.

summarize() appends one row per run to results/<model>.csv and
results/all_results.csv; TraceWriter writes one JSON line per iteration to
results/traces/.
"""
import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone

import topology as topology_mod
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy

# ---------------------------------------------------------------- constants

KEYSPACE = "consistency_lab"
TABLE = "kv"

# node number -> host port (see ports in docker-compose.yml)
NODE_PORTS = {1: 9042, 2: 9043, 3: 9044}
NODE_NAMES = {1: "cass1", 2: "cass2", 3: "cass3"}

CL_BY_NAME = {
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")

CSV_FIELDS = [
    "run_ts", "model", "scenario", "mode",
    "write_cl", "read_cl", "verify_cl",
    "iterations", "triggered", "violations", "violation_rate",
    "errors", "error_rate", "delay_ms", "jitter_ms", "topology", "extra",
]

# Driver errors counted in the "errors" column instead of stopping the run
# (e.g. every W=ALL write while a node is down).
from cassandra import (
    Unavailable, WriteTimeout, ReadTimeout, WriteFailure, ReadFailure,
    OperationTimedOut, InvalidRequest,
)
from cassandra.cluster import NoHostAvailable

TRANSIENT_ERRORS = (
    Unavailable, WriteTimeout, ReadTimeout, WriteFailure, ReadFailure,
    OperationTimedOut, NoHostAvailable,
)


def cl(name):
    """'QUORUM' -> ConsistencyLevel.QUORUM"""
    key = name.strip().upper()
    if key not in CL_BY_NAME:
        raise ValueError(f"未知一致性级别 {name!r}, 可选: {list(CL_BY_NAME)}")
    return CL_BY_NAME[key]


def cl_name(level):
    for k, v in CL_BY_NAME.items():
        if v == level:
            return k
    return str(level)


# ---------------------------------------------------------------- node sessions

class Node:
    """A CQL session pinned to a single Cassandra node."""

    def __init__(self, number, request_timeout=30.0):
        if number not in NODE_PORTS:
            raise ValueError(f"节点号必须是 1/2/3, 收到 {number}")
        self.number = number
        self.name = NODE_NAMES[number]
        self.port = NODE_PORTS[number]

        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            consistency_level=ConsistencyLevel.ONE,   # overridden per statement
            request_timeout=request_timeout,
        )
        self.cluster = Cluster(
            contact_points=["127.0.0.1"],
            port=self.port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            # keep retrying a stopped node every 2 s
            reconnection_policy=__import__(
                "cassandra.policies", fromlist=["ConstantReconnectionPolicy"]
            ).ConstantReconnectionPolicy(2.0, max_attempts=None),
        )
        self.session = self.cluster.connect(KEYSPACE)

        # system.local reports the coordinator's own address; used to check pinning
        row = self.session.execute("SELECT broadcast_address FROM system.local").one()
        self.address = str(row.broadcast_address)

        self._ins = self.session.prepare(
            f"INSERT INTO {TABLE} (key, value, writer) VALUES (?, ?, ?)")
        self._ins_ts = self.session.prepare(
            f"INSERT INTO {TABLE} (key, value, writer) VALUES (?, ?, ?) USING TIMESTAMP ?")
        self._sel = self.session.prepare(
            f"SELECT value, writer, WRITETIME(value) AS wt FROM {TABLE} WHERE key = ?")

    # -- read/write primitives -------------------------------------

    def write(self, key, value, consistency, writer=None, timestamp=None):
        """Write one row; a timestamp (microseconds) is sent as USING TIMESTAMP."""
        writer = writer if writer is not None else self.name
        if timestamp is None:
            bs = self._ins.bind((key, value, writer))
        else:
            bs = self._ins_ts.bind((key, value, writer, int(timestamp)))
        bs.consistency_level = consistency
        self.session.execute(bs)

    def read(self, key, consistency):
        """Return the value, or None if the replicas read have no row."""
        bs = self._sel.bind((key,))
        bs.consistency_level = consistency
        row = self.session.execute(bs).one()
        return None if row is None else row.value

    def read_full(self, key, consistency):
        """Return (value, writer, writetime), or three Nones if there is no row."""
        bs = self._sel.bind((key,))
        bs.consistency_level = consistency
        row = self.session.execute(bs).one()
        if row is None:
            return (None, None, None)
        return (row.value, row.writer, row.wt)

    def shutdown(self):
        try:
            self.cluster.shutdown()
        except Exception:
            pass


def connect_nodes(numbers=(1, 2, 3), verbose=True):
    """Connect to the given nodes; fail if two sessions reach the same node."""
    nodes = {}
    for n in numbers:
        nodes[n] = Node(n)
    addrs = {n: nd.address for n, nd in nodes.items()}
    if len(set(addrs.values())) != len(addrs):
        for nd in nodes.values():
            nd.shutdown()
        raise RuntimeError(
            f"pinning 失效: 多个 session 落到了同一节点 {addrs}。"
            "检查 WhiteListRoundRobinPolicy 与端口映射。")
    if verbose:
        for n in sorted(nodes):
            print(f"  [pin] node{n} ({nodes[n].name}) -> 127.0.0.1:{nodes[n].port} "
                  f"=> broadcast_address {addrs[n]}")
    return nodes


# ---------------------------------------------------------------- timestamps

def now_us():
    """Current time in microseconds, the unit of Cassandra write timestamps."""
    return int(time.time() * 1_000_000)


# ---------------------------------------------------------------- result files

def _migrate_header(path):
    """
    Rewrite a CSV whose header differs from CSV_FIELDS (missing columns left
    empty, extra ones dropped) so newly appended rows line up with the header.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return
        if header == CSV_FIELDS:
            return
        old_rows = [dict(zip(header, r)) for r in reader]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in old_rows:
            w.writerow({k: r.get(k, "") for k in CSV_FIELDS})
    print(f"  [CSV 迁移] {os.path.relpath(path)}: "
          f"{len(header)} 列 -> {len(CSV_FIELDS)} 列, {len(old_rows)} 行已保留")


def record(row):
    """Append one row to results/<model>.csv and results/all_results.csv."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    row = {k: row.get(k, "") for k in CSV_FIELDS}
    for path in (os.path.join(RESULTS_DIR, f"{row['model']}.csv"),
                 os.path.join(RESULTS_DIR, "all_results.csv")):
        _migrate_header(path)
        is_new = not os.path.exists(path)
        with open(path, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            if is_new:
                w.writeheader()
            w.writerow(row)


def summarize(model, scenario, mode, write_cl, read_cl, verify_cl,
              iterations, triggered, violations, errors, extra="", topology=""):
    """Compute the rates, print a summary and record the row. Returns the row."""
    denom = triggered if triggered else 0
    v_rate = (violations / denom) if denom else 0.0
    e_rate = (errors / iterations) if iterations else 0.0
    row = {
        "run_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model, "scenario": scenario, "mode": mode,
        "write_cl": write_cl, "read_cl": read_cl, "verify_cl": verify_cl,
        "iterations": iterations, "triggered": triggered,
        "violations": violations, "violation_rate": f"{v_rate:.4f}",
        "errors": errors, "error_rate": f"{e_rate:.4f}",
        # netem values read back from tc, not the requested ones
        "delay_ms": topology_mod.LAST_NETEM["delay_ms"],
        "jitter_ms": topology_mod.LAST_NETEM["jitter_ms"],
        "topology": topology,
        "extra": extra,
    }
    print(f"\n  {'-'*66}")
    print(f"  模型={model}  场景={scenario}  模式={mode}")
    print(f"  W={write_cl}  R={read_cl}  verify={verify_cl}")
    print(f"  迭代={iterations}  有效样本={triggered}  违例={violations}  "
          f"违例率={v_rate:.2%}")
    print(f"  异常={errors} ({e_rate:.2%})   {extra}")
    print(f"  {'-'*66}")
    record(row)
    return row


# ---------------------------------------------------------------- command line

def build_parser(model, default_write="ONE", default_read="ONE"):
    p = argparse.ArgumentParser(description=f"{model} 一致性实验")
    p.add_argument("--write-cl", default=default_write, help="写一致性级别 ONE/QUORUM/ALL")
    p.add_argument("--read-cl", default=default_read, help="读一致性级别 ONE/QUORUM/ALL")
    p.add_argument("-n", "--iterations", type=int, default=1000, help="循环次数")
    p.add_argument("--scenario", default="normal",
                   help="场景标签: normal / node_failure / partition")
    p.add_argument("--nodes", default="1,2,3",
                   help="本次实验需要连接的节点, 逗号分隔 (节点故障时用 1,2)")
    p.add_argument("--sleep-ms", type=float, default=0.0,
                   help="写与读之间插入的等待毫秒数 (默认 0, 即尽快读)")
    p.add_argument("--skip-topology-check", action="store_true",
                   help="跳过开跑前的拓扑自检 (不推荐; 会产生标签可能不实的数据)")
    p.add_argument("--no-trace", action="store_true",
                   help="不写逐次迭代的 JSONL 明细")
    return p


# ---------------------------------------------------------------- argument checks

def validate(args, roles):
    """
    Exit with a readable message on invalid argument combinations.

    roles maps each node-role flag to its node, e.g. {"--write-node": 1}.
    """
    errs, warns = [], []

    if args.iterations <= 0:
        errs.append(f"--iterations 必须为正整数, 收到 {args.iterations}")

    try:
        nodes = parse_nodes(args.nodes)
    except Exception:
        errs.append(f"--nodes 无法解析: {args.nodes!r} (期望形如 1,2,3)")
        nodes = ()

    unknown = [n for n in nodes if n not in NODE_PORTS]
    if unknown:
        errs.append(f"--nodes 含未知节点号 {unknown}, 只能是 {sorted(NODE_PORTS)}")
    if len(set(nodes)) != len(nodes):
        errs.append(f"--nodes 有重复: {args.nodes!r}")

    # every role node must be in --nodes
    for flag, n in roles.items():
        if nodes and n not in nodes:
            errs.append(
                f"{flag}={n} 不在 --nodes={','.join(map(str, nodes))} 之内。"
                f"故障/分区场景下请显式指定该角色, 可选 {sorted(nodes)}")

    # consistency level names
    cl_flags = [("--write-cl", args.write_cl), ("--read-cl", args.read_cl)]
    for name in ("verify_cl", "setup_cl", "seed_cl"):
        if hasattr(args, name):
            cl_flags.append((f"--{name.replace('_', '-')}", getattr(args, name)))
    for flag, val in cl_flags:
        if str(val).strip().upper() not in CL_BY_NAME:
            errs.append(f"{flag}={val!r} 不是合法一致性级别, 可选 {list(CL_BY_NAME)}")

    # With fewer than 3 nodes, ALL cannot succeed: an error for the setup/verify
    # CLs (every iteration would be void), only a warning for the tested W/R CLs.
    if nodes and len(nodes) < 3:
        for name, flag in (("setup_cl", "--setup-cl"), ("verify_cl", "--verify-cl"),
                           ("seed_cl", "--seed-cl")):
            if hasattr(args, name) and str(getattr(args, name)).upper() == "ALL":
                errs.append(
                    f"{flag}=ALL 但只连接了 {len(nodes)} 个节点。"
                    f"{flag} 是实验装置而非自变量, 不可用会让整轮迭代全部作废。"
                    f"故障/分区场景请改用 {flag} QUORUM。")
        for flag, val in (("--write-cl", args.write_cl), ("--read-cl", args.read_cl)):
            if str(val).upper() == "ALL":
                warns.append(
                    f"{flag}=ALL 且只连接了 {len(nodes)} 个节点 —— "
                    "预期会持续抛 Unavailable。这本身就是要记录的数据, 继续执行。")

    for w in warns:
        print(f"  [告警] {w}")
    if errs:
        print()
        print("  [参数校验] 以下参数不合法, 已中止:")
        for e in errs:
            print(f"      ! {e}")
        print()
        raise SystemExit(2)


# ---------------------------------------------------------------- per-iteration traces

class TraceWriter:
    """Write one JSON line per iteration to results/traces/<run>.jsonl."""

    def __init__(self, model, scenario, mode, write_cl, read_cl, run_id, enabled=True):
        self.enabled = bool(enabled)
        self.path = None
        self.fh = None
        if not self.enabled:
            return
        d = os.path.join(RESULTS_DIR, "traces")
        os.makedirs(d, exist_ok=True)
        self.path = os.path.join(
            d, f"{model}_{scenario}_{mode}_W{write_cl}_R{read_cl}_{run_id}.jsonl")
        self.fh = open(self.path, "w", encoding="utf-8")

    def write(self, **rec):
        if self.fh is not None:
            self.fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    def close(self):
        if self.fh is not None:
            self.fh.close()
            self.fh = None
            print(f"  逐次明细 -> {os.path.relpath(self.path)}")


def parse_nodes(s):
    return tuple(int(x) for x in s.split(",") if x.strip())
