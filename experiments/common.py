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
import os
import time
from datetime import datetime, timezone

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
    "errors", "error_rate", "extra",
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

def record(row):
    """把一行结果追加到 results/<model>.csv 和 results/all_results.csv。"""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    row = {k: row.get(k, "") for k in CSV_FIELDS}
    for path in (os.path.join(RESULTS_DIR, f"{row['model']}.csv"),
                 os.path.join(RESULTS_DIR, "all_results.csv")):
        is_new = not os.path.exists(path)
        with open(path, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            if is_new:
                w.writeheader()
            w.writerow(row)


def summarize(model, scenario, mode, write_cl, read_cl, verify_cl,
              iterations, triggered, violations, errors, extra=""):
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
    p.add_argument("--verify-cl", default="ALL",
                   help="收敛后校验用的读级别 (MW/WFR 判定最终序需要看全副本)")
    p.add_argument("-n", "--iterations", type=int, default=1000, help="循环次数")
    p.add_argument("--scenario", default="normal",
                   help="场景标签: normal / node_failure / partition")
    p.add_argument("--nodes", default="1,2,3",
                   help="本次实验需要连接的节点, 逗号分隔 (节点故障时用 1,2)")
    p.add_argument("--sleep-ms", type=float, default=0.0,
                   help="写与读之间插入的等待毫秒数 (默认 0, 即尽快读)")
    return p


def parse_nodes(s):
    return tuple(int(x) for x in s.split(",") if x.strip())
