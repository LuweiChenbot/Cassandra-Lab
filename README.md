# Cassandra Client-Centric Consistency Lab

用 3 节点 Cassandra 集群（RF=3）实验验证四种 client-centric consistency（会话一致性）模型：

| 模型 | 含义 |
|---|---|
| **RYW** — Read-Your-Writes | 客户端写完之后，自己一定读得到 |
| **MR** — Monotonic Reads | 读过新值之后，不会再读到旧值 |
| **MW** — Monotonic Writes | 同一客户端先发的写，一定排在后发的写之前 |
| **WFR** — Writes-Follow-Reads | 读到某个写之后发出的写，必须排在那个写之后 |

---

## 1. 环境要求

- Docker Desktop（运行中，建议给到 4 GB+ 内存 —— 三个 JVM 各 512 MB 堆）
- Python 3.9+
- 约 3 GB 磁盘

## 2. 一键启动

```bash
git clone <repo 地址>
cd cassandra-lab
./scripts/bootstrap.sh
```

`bootstrap.sh` 会依次完成：起集群 → 建 keyspace → 装 Python 驱动 → 验证节点钉定。
全程约 3–5 分钟（三个节点是**串行** bootstrap 的）。

### 手动启动（想看清每一步时用）

```bash
docker compose up -d --wait
```

> **不需要手动 `sleep`。** `docker-compose.yml` 里每个节点都有 healthcheck，
> 且 `cass2` 依赖 `cass1` healthy、`cass3` 依赖 `cass2` healthy。
> `--wait` 会让 compose 自己按顺序等，避免三个节点同时 bootstrap 抢 token 导致集群裂开。
>
> 如果确实想一个个起（例如想观察日志）：
> ```bash
> docker compose up -d cass1    # compose 会阻塞到 cass1 healthy 才返回
> docker compose up -d cass2
> docker compose up -d cass3
> ```
> 这三条同样不需要 sleep —— `depends_on: condition: service_healthy` 已经保证了顺序。

## 3. 验证集群（其他人复现时请贴这一段的输出）

```bash
docker exec cass1 nodetool status
```

期望看到**三行 `UN`**（Up / Normal），且 `Owns (effective)` 均为 100%：

```
--  Address     Load       Tokens  Owns (effective)  Host ID    Rack
UN  172.18.0.2  ...        16      100.0%            ...        rack1
UN  172.18.0.3  ...        16      100.0%            ...        rack1
UN  172.18.0.4  ...        16      100.0%            ...        rack1
```

确认三个节点在**同一个集群**（而不是各自成环）：

```bash
docker exec cass1 nodetool describecluster
```

关键是 `Schema versions:` 下只有**一个** UUID，且三个 IP 都列在它下面。
不同集群的节点不会出现在彼此的 schema 列表里，这是最硬的判据。

> 三个节点共享 `CASSANDRA_SEEDS: cass1`（只有 cass1 是 seed，cass2/cass3 作为
> 非 seed 节点向它 bootstrap），集群名统一为 `consistency_lab`。

## 4. 注入复制延迟

不注入延迟的话，本机三个容器之间复制只要几毫秒，违例窗口太窄，基本观测不到。

```bash
./scripts/inject_latency.sh        # 默认 200ms ± 50ms
DELAY=300ms JITTER=80ms ./scripts/inject_latency.sh   # 自定义
./scripts/clear_latency.sh         # 恢复
```

**这里有一个重要的设计选择**：脚本只延迟**目的端口 7000**（Cassandra 的
`storage_port`，即节点间 gossip/复制流量），**不延迟 9042**（客户端 CQL 流量）。

原因是，如果像常见做法那样 `tc qdisc add dev eth0 root netem delay 200ms`
无差别地延迟所有出向流量，会有两个问题：

1. **实验慢到不可用**：每次迭代多次客户端往返，完整矩阵（4 模型 × 4 CL 组合 × 3 场景）要跑好几个小时；
2. **更严重：污染因变量**。客户端"写完立刻读"这两步之间会被强行插入几百毫秒，
   反而给了复制额外的时间窗口去完成，违例率被系统性**低估**。

我们要模拟的是"副本之间同步慢"，不是"客户端网络慢"。只打 7000 端口才精确对应前者。

三个节点**都要注入**（包括 cass1）—— RYW 实验是"经 cass1 写、从 cass2 读"，
制造延迟窗口的正是 cass1 的**出向复制流量**，只给 cass2/cass3 注入会漏掉这条路径。

## 5. 跑实验

```bash
source .venv/bin/activate     # 或者直接用 ./.venv/bin/python

python experiments/ryw_test.py --write-cl ONE    --read-cl ONE    -n 1000
python experiments/mr_test.py  --write-cl QUORUM --read-cl QUORUM -n 1000
python experiments/mw_test.py  --write-cl ONE    --read-cl ONE    -n 1000
python experiments/wfr_test.py --write-cl ALL    --read-cl ONE    -n 1000
```

结果自动追加到 `results/<model>.csv` 和 `results/all_results.csv`。

常用参数：

| 参数 | 说明 |
|---|---|
| `--write-cl` / `--read-cl` | `ONE` / `QUORUM` / `ALL` |
| `-n` | 迭代次数 |
| `--scenario` | 结果里的场景标签：`normal` / `node_failure` / `partition` |
| `--nodes` | 本次要连接的节点，如节点故障时用 `--nodes 1,2` |
| `--mode` | 仅 MW/WFR：`natural` / `skew` / `both` |
| `--skew-us` | 仅 MW/WFR：构造的时钟偏移，默认 500000（=500ms） |

## 6. 项目结构

```
cassandra-lab/
├── docker-compose.yml       3 节点，RF=3，端口 9042/9043/9044
├── scripts/
│   ├── bootstrap.sh         一键拉起环境
│   ├── setup_keyspace.cql   keyspace + kv 表（含设计理由注释）
│   ├── inject_latency.sh    只延迟 storage_port 的 netem 规则
│   └── clear_latency.sh
├── experiments/
│   ├── common.py            节点钉定 / 读写原语 / CSV 记录
│   ├── ryw_test.py
│   ├── mr_test.py
│   ├── mw_test.py
│   └── wfr_test.py
└── results/                 CSV 结果
```

## 7. 两个关键的实验设计决定（写报告时要解释）

### 7.1 为什么关掉 `read_repair` 和 `speculative_retry`

见 `scripts/setup_keyspace.cql` 里的完整注释，摘要：

- **`read_repair`（默认 `BLOCKING`）**：读操作发现副本间摘要不一致时，会在返回结果
  *之前*把最新值同步写回落后的副本。结果是第一次读也许还能观测到违例，但它同时把
  发散**修好了** —— 后续读再也复现不出来，违例率被系统性低估。
- **`speculative_retry`（默认 `99p`）**：协调者在某副本响应慢时会额外向其他副本发请求。
  我们注入了 200ms 延迟，这会**持续**触发投机重试，等于偷偷提高了实际参与的副本数，
  让 `CL=ONE` 表现得像 `CL=TWO`。

两者都是 Cassandra 的自愈机制。实验目的恰恰是观测**未自愈状态下**的副本发散，所以必须关闭。

> 附注：`additional_write_policy`（写侧的投机重试，4.0+ 新增，默认 `99p`）在
> Cassandra 4.1 上无法通过 `ALTER TABLE` 修改（语句被静默忽略）。在 RF=3 全副本、
> 无 transient replica 的拓扑下它没有"额外副本"可投机，预期是空操作；
> 这一点由 `W=ONE/R=ONE` 下 RYW 的实测违例率反证 —— 若写真的被拔高到等 2 个 ack，
> 违例率会趋近 0。

### 7.2 为什么必须把 session 钉死在指定节点上

Python 驱动默认用 `TokenAwarePolicy(DCAwareRoundRobinPolicy)` 做负载均衡，会在集群
所有节点间自动轮转协调者。不禁用的话，"从 node1 写、从 node2 读"这个实验前提根本不成立。

`common.py` 用 `WhiteListRoundRobinPolicy(['127.0.0.1'])` 解决：三个容器分别把 9042
映射到宿主机 9042/9043/9044，所以 `Cluster(contact_points=['127.0.0.1'], port=9043)`
唯一确定 cass2；而其他节点在 `system.peers` 里的地址是 `172.18.0.x`，不在白名单内，
驱动对它们返回 `HostDistance.IGNORED`，根本不建连接池。

`connect_nodes()` 每次都会查 `system.local`（节点本地表，谁做协调者就返回谁的身份）
来验证钉定是否真的生效，三个 session 落在同一 IP 上会直接报错退出。

## 8. 故障与分区场景

```bash
# 节点故障
docker stop cass3
python experiments/ryw_test.py --scenario node_failure --nodes 1,2 --write-cl ALL --read-cl ONE
docker start cass3

# 网络分区（cass3 从网络摘除，但进程还活着）
docker network disconnect cassandra-lab_cassnet cass3
python experiments/ryw_test.py --scenario partition --nodes 1,2 ...
docker network connect cassandra-lab_cassnet cass3
```

节点故障下 `W=ALL` 必然抛 `Unavailable` —— 脚本会把它计入 `errors` 列而不是崩掉，
**异常数本身就是要记录的数据**（说明该配置在故障下不可用）。
