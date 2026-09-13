#!/usr/bin/env python3
"""
topology.py -- 实验开始前的运行时拓扑自检。

解决的问题
----------
--scenario 原本只是一个写进 CSV 的字符串。这意味着:
在集群完全正常的情况下跑一遍 --scenario node_failure, 会得到一份
标着"节点故障"但其实是正常场景的数据, 而且事后无法分辨。

本模块在每次实验开始时真的去看一眼集群长什么样(哪些容器在跑、
每个节点眼里有几个 UN、tc 规则装没装), 和声明的场景对不上就直接退出。
同时把观测到的拓扑快照写进结果, 让 CSV 里每一行都有据可查。
"""
import re
import subprocess

CONTAINERS = {1: "cass1", 2: "cass2", 3: "cass3"}
VALID_SCENARIOS = ("normal", "node_failure", "partition", "smoke", "probe")


def _run(cmd, timeout=30):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:                      # docker 不在 PATH / 超时
        return -1, "", str(e)


def running_containers():
    rc, out, _ = _run(["docker", "ps", "--format", "{{.Names}}"])
    if rc != 0:
        return set()
    return {l.strip() for l in out.splitlines() if l.strip()}


def nodetool_status(container):
    """该节点眼中的整个环: {ip: 'UN'|'DN'|'UJ'|...}。取不到返回 None。"""
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
    从 tc qdisc 输出里读出实际生效的 delay/jitter (毫秒)。

    为什么不直接信命令行参数: 扫描实验里延迟是自变量, 如果只把"打算设成多少"
    写进结果, 一旦某个节点的 tc 没生效(例如 qdisc 被意外清掉), 数据会带着
    错误的横坐标而无法察觉。从 tc 读回来才是观测值。
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
    """netem 规则现状: 延迟/抖动实际值、是否有丢包带、分区 filter 有几条。"""
    _, qdisc, _ = _run(["docker", "exec", container, "tc", "qdisc", "show", "dev", "eth0"])
    _, filt, _ = _run(["docker", "exec", container, "tc", "filter", "show", "dev", "eth0"])
    delay_ms, jitter_ms = parse_netem_delay(qdisc)
    return {
        "delay": delay_ms > 0,
        "delay_ms": delay_ms,
        "jitter_ms": jitter_ms,
        "loss": "loss" in qdisc,
        # band 1:3 专门用于分区丢包(见 scripts/partition.sh)
        "partition_filters": filt.count("flowid 1:3"),
        "qdisc": " | ".join(l.strip() for l in qdisc.splitlines() if l.strip()),
    }


def snapshot():
    """采集全部三个容器的状态。"""
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
    """每个在跑的容器眼中有几个节点是 UN。"""
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
            lines.append(f"    {c}: 容器未运行")
            continue
        ring = d.get("ring") or {}
        seen = ", ".join(f"{ip}={st}" for ip, st in sorted(ring.items())) or "(取不到)"
        tc = d["tc"]
        flags = []
        if tc["delay"]:
            flags.append("延迟")
        if tc["partition_filters"]:
            flags.append(f"分区filter×{tc['partition_filters']}")
        lines.append(f"    {c}: 环内所见 [{seen}]  netem[{'+'.join(flags) or '无'}]")
    return "\n".join(lines)


def check_scenario(scenario, strict=True):
    """
    校验实际拓扑与声明的 --scenario 是否一致。
    返回 (snapshot, [问题列表])。strict=True 且有问题时由调用方退出。
    """
    snap = snapshot()
    running = set(snap["running"])
    uns = un_counts(snap)
    part = sum(d.get("tc", {}).get("partition_filters", 0)
               for d in snap["nodes"].values() if d.get("running"))
    problems = []

    if not running:
        problems.append("一个 Cassandra 容器都没在跑 —— 先执行 docker compose up -d --wait")
        return snap, problems

    if scenario == "normal":
        if len(running) != 3:
            problems.append(
                f"声明 normal 但只有 {len(running)} 个容器在跑 ({sorted(running)})。"
                "正常场景应当三个都在。")
        bad = {c: n for c, n in uns.items() if n != 3}
        if bad:
            problems.append(f"声明 normal 但这些节点没看到 3 个 UN: {bad}")
        if part:
            problems.append(
                f"声明 normal 但检测到 {part} 条分区 filter —— "
                "先跑 ./scripts/heal_partition.sh")

    elif scenario == "node_failure":
        if len(running) == 3:
            problems.append(
                "声明 node_failure 但三个容器都在跑。"
                "请先 docker stop cass3 (或其他节点)。")
        elif len(running) < 2:
            problems.append(
                f"声明 node_failure 但只剩 {len(running)} 个容器 —— "
                "RF=3 下停掉两个节点连 QUORUM 都不可用, 不是本实验预期的场景。")
        alive = {c: n for c, n in uns.items() if n >= 3}
        if alive:
            problems.append(
                f"这些节点仍看到 3 个 UN: {alive} —— "
                "gossip 可能还没标记故障, 等 15 秒再试。")

    elif scenario == "partition":
        if len(running) != 3:
            problems.append(
                f"声明 partition 但只有 {len(running)} 个容器在跑。"
                "网络分区场景下进程应当全部存活, 只是互相通信被切断。")
        if part == 0:
            problems.append(
                "声明 partition 但没检测到任何分区 filter —— "
                "先跑 ./scripts/partition.sh cass3")

    elif scenario in ("smoke", "probe"):
        pass                                     # 调试用, 不校验

    else:
        problems.append(
            f"未知场景 {scenario!r}。可选: {', '.join(VALID_SCENARIOS)}")

    return snap, problems


# enforce() 观测到的 netem 实际值, 由 common.summarize() 读取写入 CSV。
# 这样 4 个实验脚本都不用改签名。
LAST_NETEM = {"delay_ms": "", "jitter_ms": ""}


def enforce(scenario, skip=False):
    """打印拓扑; 不一致则退出。返回快照的一行摘要, 供写入结果。"""
    snap, problems = check_scenario(scenario)
    print(f"  [拓扑自检] 场景={scenario}")
    print(format_snapshot(snap))
    if problems:
        if skip:
            print("  [拓扑自检] 检测到不一致, 但 --skip-topology-check 已指定, 继续:")
            for p in problems:
                print(f"      ! {p}")
        else:
            print("\n  [拓扑自检] 实际拓扑与声明的场景不符, 已中止:")
            for p in problems:
                print(f"      ! {p}")
            print("\n  确认无误想强制继续, 加 --skip-topology-check\n")
            raise SystemExit(2)
    # 取各节点 netem 的众数作为本轮的延迟观测值; 三个节点应当一致,
    # 不一致说明注入没铺全, 在摘要里显式标出来
    delays = sorted({d["tc"]["delay_ms"] for d in snap["nodes"].values()
                     if d.get("running") and d.get("tc")})
    jitters = sorted({d["tc"]["jitter_ms"] for d in snap["nodes"].values()
                      if d.get("running") and d.get("tc")})
    LAST_NETEM["delay_ms"] = delays[0] if len(delays) == 1 else f"MIXED{delays}"
    LAST_NETEM["jitter_ms"] = jitters[0] if len(jitters) == 1 else f"MIXED{jitters}"
    if len(delays) > 1:
        print(f"  [拓扑自检] 警告: 各节点 netem 延迟不一致 {delays} —— "
              f"重跑 ./scripts/inject_latency.sh")

    uns = un_counts(snap)
    return (f"running={'/'.join(snap['running'])} "
            f"un={','.join(f'{c}:{n}' for c, n in sorted(uns.items()))} "
            f"delay={LAST_NETEM['delay_ms']}ms")
