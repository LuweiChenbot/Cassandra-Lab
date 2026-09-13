#!/usr/bin/env python3
"""
common.py -- Cassandra client-centric consistency lab 的共享基础设施。

核心职责:
  1. 把每个 CQL session "钉死" 在指定的物理节点上 (pinning)
  2. 提供带 CL / USING TIMESTAMP 的读写原语
  3. 统一的 CSV 结果记录格式

为什么必须 pinning
------------------
Python 驱动默认用 TokenAwarePolicy(DCAwareRoundRobinPolicy) 做负载均衡,
它会在集群所有节点间自动轮转协调者。如果不禁用, "从 node1 写、从 node2 读"
这个实验前提根本不成立 —— 驱动可能两次都路由到同一个节点, 违例就消失了。

做法: WhiteListRoundRobinPolicy(['127.0.0.1'])。
三个容器分别把 9042 映射到宿主机 9042/9043/9044, 所以
  Cluster(contact_points=['127.0.0.1'], port=9043)
就唯一确定了 cass2。而集群里其他节点在 system.peers 里的地址是
172.18.0.x, 不在白名单内 -> distance() 返回 IGNORED -> 驱动根本不会
对它们建连接池。connect() 里会用 system.local (节点本地表) 验证钉对了没有。
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

# ---------------------------------------------------------------- 常量

KEYSPACE = "consistency_lab"
TABLE = "kv"

# 节点号 -> 宿主机端口 (见 docker-compose.yml 的 ports 映射)
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

# 驱动在不可用 / 超时 时抛出的异常, 统一捕获并计入 errors 而不是让脚本崩掉。
# 节点故障场景下 W=ALL 必然走到这里 —— 这本身就是要记录的数据。
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


# ---------------------------------------------------------------- 节点封装

class Node:
    """一个钉死在单个 Cassandra 物理节点上的 CQL session。"""

    def __init__(self, number, request_timeout=30.0):
        if number not in NODE_PORTS:
            raise ValueError(f"节点号必须是 1/2/3, 收到 {number}")
        self.number = number
        self.name = NODE_NAMES[number]
        self.port = NODE_PORTS[number]

        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            consistency_level=ConsistencyLevel.ONE,   # 每条语句会单独覆盖
            request_timeout=request_timeout,
        )
        self.cluster = Cluster(
            contact_points=["127.0.0.1"],
            port=self.port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            # 节点故障场景下不要让驱动后台无限重连刷屏
            reconnection_policy=__import__(
                "cassandra.policies", fromlist=["ConstantReconnectionPolicy"]
            ).ConstantReconnectionPolicy(2.0, max_attempts=None),
        )
        self.session = self.cluster.connect(KEYSPACE)

        # system.local 是节点本地表: 谁做协调者就返回谁的身份。
        # 用它验证 pinning 是否真的生效。
        row = self.session.execute("SELECT broadcast_address FROM system.local").one()
        self.address = str(row.broadcast_address)

        self._ins = self.session.prepare(
            f"INSERT INTO {TABLE} (key, value, writer) VALUES (?, ?, ?)")
        self._ins_ts = self.session.prepare(
            f"INSERT INTO {TABLE} (key, value, writer) VALUES (?, ?, ?) USING TIMESTAMP ?")
        self._sel = self.session.prepare(
            f"SELECT value, writer, WRITETIME(value) AS wt FROM {TABLE} WHERE key = ?")

    # -- 读写原语 -------------------------------------------------

    def write(self, key, value, consistency, writer=None, timestamp=None):
        """写一行。timestamp 非 None 时走 USING TIMESTAMP (单位: 微秒)。"""
        writer = writer if writer is not None else self.name
        if timestamp is None:
            bs = self._ins.bind((key, value, writer))
        else:
            bs = self._ins_ts.bind((key, value, writer, int(timestamp)))
        bs.consistency_level = consistency
        self.session.execute(bs)

    def read(self, key, consistency):
        """返回 value (int) 或 None (行不存在 / 该副本还没收到)。"""
        bs = self._sel.bind((key,))
        bs.consistency_level = consistency
        row = self.session.execute(bs).one()
        return None if row is None else row.value

    def read_full(self, key, consistency):
        """返回 (value, writer, writetime), 用于需要看时间戳的场景。"""
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
    """连接若干节点并验证 pinning 生效 (三个 session 必须落在三个不同 IP 上)。"""
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


# ---------------------------------------------------------------- 时间戳

def now_us():
    """当前时间的微秒时间戳 —— Cassandra 写时间戳的单位。"""
    return int(time.time() * 1_000_000)


# ---------------------------------------------------------------- 结果记录

def _migrate_header(path):
    """
    已有 CSV 的表头与当前 CSV_FIELDS 不一致时, 就地重写成新表头,
    缺失的列留空, 多余的列丢弃。

    为什么需要: 给 CSV 加列之后, 老文件的表头还是旧的。DictWriter 不会
    校验文件里已有的表头, 会直接按新字段数追加 —— 结果是表头 N 列、
    新行 N+2 列的错位文件, 而且不会报错。组员拉到新代码但本地留着旧结果
    时必然踩这个坑, 所以放在写入路径上自动处理。
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
    """把一行结果追加到 results/<model>.csv 和 results/all_results.csv。"""
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
    """算出比率、打印到终端、写 CSV。返回该行 dict。"""
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
        # 从 tc 读回来的实际生效值, 不是命令行声称的值
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


# ---------------------------------------------------------------- 参数

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


# ---------------------------------------------------------------- 参数校验

def validate(args, roles):
    """
    开跑前把不合法的参数组合拦下来, 给出说明性错误而不是运行中 KeyError。

    roles: {"--write-node": args.write_node, ...} —— 本实验用到的节点角色。
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

    # 角色节点必须在连接列表里 —— 否则运行到一半 KeyError
    for flag, n in roles.items():
        if nodes and n not in nodes:
            errs.append(
                f"{flag}={n} 不在 --nodes={','.join(map(str, nodes))} 之内。"
                f"故障/分区场景下请显式指定该角色, 可选 {sorted(nodes)}")

    # 一致性级别拼写
    cl_flags = [("--write-cl", args.write_cl), ("--read-cl", args.read_cl)]
    for name in ("verify_cl", "setup_cl", "seed_cl"):
        if hasattr(args, name):
            cl_flags.append((f"--{name.replace('_', '-')}", getattr(args, name)))
    for flag, val in cl_flags:
        if str(val).strip().upper() not in CL_BY_NAME:
            errs.append(f"{flag}={val!r} 不是合法一致性级别, 可选 {list(CL_BY_NAME)}")

    # RF=3 下 ALL 需要三个副本都在。
    #   装置用的 CL 不可用 -> 整轮数据作废, 按错误拦下;
    #   自变量 CL 不可用 -> 那正是要测的现象(记录 Unavailable), 只告警。
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


# ---------------------------------------------------------------- 逐次明细

class TraceWriter:
    """
    每次迭代写一行 JSON, 让汇总 CSV 里的每个数字都能追溯到原始证据。

    汇总表回答"违例率是多少", 明细回答"具体哪一次、写了什么、读到什么、
    经过哪个协调者、时间戳多少、异常是哪一类"。出了反常结果时,
    没有明细就只能重跑并祈祷复现。
    """

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
