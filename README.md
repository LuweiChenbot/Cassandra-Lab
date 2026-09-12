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

- Docker Desktop（运行中，建议分配 4 GB+ 内存 —— 三个 JVM 各 512 MB 堆）
- Python 3.9+
- 约 3 GB 磁盘

版本已固定，保证复现：`cassandra:4.1.12`（compose）、`cassandra-driver==3.30.1`（`requirements.txt`）。
`bootstrap.sh` 会把实际跑起来的版本写进 `results/environment.txt`。

## 2. 一键启动

```bash
git clone https://github.com/LuweiChenbot/Cassandra-Lab.git
cd Cassandra-Lab
./scripts/bootstrap.sh
```

`bootstrap.sh` 依次完成：起集群 → 建 keyspace → 装固定版本驱动 → 验证节点钉定 → 记录环境快照。
全程约 4–6 分钟（三个节点是**串行** bootstrap 的）。幂等，可以重复跑。

### 手动启动

```bash
docker compose up -d --wait --wait-timeout 600
```

> **`--wait` 不能省。** `-d` 只保证容器**启动**了，不代表 Cassandra 通过了 healthcheck。
> `--wait` 才会阻塞到所有服务 healthy。
>
> 想一个个起（例如要观察日志）：
> ```bash
> docker compose up -d --wait cass1
> docker compose up -d --wait cass2
> docker compose up -d --wait cass3
> ```
> 串行是由 `depends_on: condition: service_healthy` 保证的（cass2 等 cass1，cass3 等 cass2），
> 不需要手动 `sleep`。但每条都要带 `--wait`，否则最后一个节点返回时可能还没就绪。

## 3. 验证集群（复现时请贴这一段的输出）

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

关键是 `Schema versions:` 下只有**一个** UUID，且三个 IP 都列在它下面 ——
不同集群的节点不会出现在彼此的 schema 列表里，这是最硬的判据。

> 三个节点共享 `CASSANDRA_SEEDS: cass1`（只有 cass1 是 seed，cass2/cass3 作为
> 非 seed 节点向它 bootstrap），集群名统一为 `consistency_lab`。

## 4. 注入复制延迟

不注入延迟的话，本机三个容器之间复制只要几毫秒，违例窗口太窄，基本观测不到。

```bash
./scripts/inject_latency.sh                            # 默认 200ms ± 50ms
DELAY=300ms JITTER=80ms ./scripts/inject_latency.sh    # 自定义
./scripts/clear_latency.sh                             # 全部清除
```

**只延迟目的端口 7000**（`storage_port`，节点间 gossip/复制），**不碰 9042**（客户端 CQL）。

如果像常见做法那样 `tc qdisc add dev eth0 root netem delay 200ms` 无差别延迟所有出向流量：

1. **实验慢到不可用**：完整矩阵要跑好几个小时；
2. **更严重：污染因变量**。客户端"写完立刻读"这两步之间会被强行插入几百毫秒，
   反而给复制留出额外时间去完成，违例率被系统性**低估** —— 等于把要测的现象自己消掉了。

我们要模拟的是"副本之间同步慢"，不是"客户端网络慢"。三个节点**都要注入**（包括 cass1）——
RYW 是"经 cass1 写、从 cass2 读"，制造延迟窗口的正是 cass1 的**出向复制流量**。

### tc 队列结构

延迟和分区共用一套结构，`partition.sh` 依赖它：

```
root prio 1: 四个 band，priomap 全部指向 band 0
  ├─ 1:1  默认带，无 qdisc      ← 未匹配流量走这里，完全不受影响
  ├─ 1:2  netem delay           ← 复制延迟      (filter prio 3)
  ├─ 1:3  netem loss 100%       ← 网络分区丢包  (filter prio 1)
  └─ 1:4  预留
```

`priomap` 全 0 是关键：`prio` qdisc 默认的 priomap 会按 TOS 位把流量散到前三个 band，
那样未匹配的流量也可能落进 netem 带。全置 0 之后只有被 filter 显式选中的流量才会被处理，
行为完全确定。分区 filter 优先级（prio 1）高于延迟（prio 3），所以被分区的链路直接丢包。

## 5. 跑实验

单个实验：

```bash
./.venv/bin/python experiments/ryw_test.py --write-cl ONE --read-cl ONE -n 1000
```

整个矩阵（4 模型 × 4 个 R/W 组合，自动套用该场景正确的节点参数）：

```bash
./scripts/run_matrix.sh normal 1000
```

结果追加到 `results/<model>.csv` 和 `results/all_results.csv`；
逐次迭代明细写到 `results/traces/*.jsonl`。

### 常用参数

| 参数 | 说明 |
|---|---|
| `--write-cl` / `--read-cl` | `ONE` / `TWO` / `THREE` / `QUORUM` / `ALL` |
| `-n` | 迭代次数 |
| `--scenario` | `normal` / `node_failure` / `partition`（会被拓扑自检校验，见 §7） |
| `--nodes` | 本次要连接的节点，如故障场景用 `--nodes 1,2` |
| `--mode` | 仅 MW/WFR：`natural` / `skew` / `both` |
| `--skew-us` | 仅 MW/WFR：构造的客户端时间戳偏移 |
| `--seed-cl` / `--setup-cl` / `--verify-cl` | **实验装置**用的 CL，不是自变量（见 §8.3） |
| `--settle-ms` | 仅 MW/WFR：校验读之前的收敛等待，降级到 `QUORUM` 时必须设（见 §8.3） |
| `--no-trace` | 不写逐次 JSONL 明细 |
| `--skip-topology-check` | 跳过拓扑自检（不推荐） |

## 6. 三个场景

每个场景需要不同的节点参数 —— MW 默认用 node2/node3，WFR 默认要用满三个节点，
node3 不可达时沿用默认会直接报错。`run_matrix.sh` 已经把这些配好了，手动跑时请照下表指定。

| 场景 | 前置操作 | `--nodes` | 装置 CL |
|---|---|---|---|
| `normal` | `./scripts/inject_latency.sh` | `1,2,3` | `ALL` |
| `node_failure` | `docker stop cass3` | `1,2` | `QUORUM` |
| `partition`（多数派） | `./scripts/partition.sh cass3` | `1,2` | `QUORUM` |
| `partition`（少数派） | 同上 | `3` | `ONE` |

### 节点故障

```bash
docker stop cass3
./scripts/run_matrix.sh node_failure 1000
docker start cass3
```

`W=ALL` 在故障下必然抛 `Unavailable` —— 脚本把它计入 `errors` 列而不是崩掉。
**异常数本身就是数据**（说明该配置在故障下不可用）。

### 网络分区

```bash
./scripts/partition.sh cass3          # 切断 cass3 与其余节点的 :7000
./scripts/run_matrix.sh partition_majority 1000
./scripts/run_matrix.sh partition_minority 1000
./scripts/heal_partition.sh           # 恢复
```

**为什么不用 `docker network disconnect`**

Cassandra 启动时把 `listen_address` 绑死在容器当时的 IP。把容器从网络摘掉会连同这个
网卡地址一起拿走 —— 结果不是"节点被隔离"，而是**节点的内部监听器直接失效**，
重连后 IP 还可能变化，Cassandra 不会重新绑定，只能靠重启恢复。那是把节点弄坏，
不是制造分区。

而且摘掉网络后宿主机的 9044 端口映射也失效，客户端**连不上少数派**，于是只能观察
多数派一侧 —— 无法回答网络分区最核心的几个问题：少数派还能不能用 `CL=ONE` 读写？
两侧是否产生不同版本？恢复后谁胜出？

用 tc 丢包则：IP 和网卡全程不动，Cassandra 无感；9042 完好，**两侧都能连**；
一条 `tc filter del` 即可恢复。实测结果：

| 客户端位置 | CL | 结果 |
|---|---|---|
| 少数派 cass3 | `ONE` | 全部成功 —— 可用，但与世隔绝 |
| 少数派 cass3 | `QUORUM` | 全部 `Unavailable` —— 不可用 |
| 多数派 cass1+2 | `QUORUM` | 全部成功 —— 既可用又一致 |

## 7. 拓扑自检

`--scenario` 不只是一个写进 CSV 的标签。每次实验开跑前，`experiments/topology.py`
会真的去看一眼集群：哪些容器在跑、每个节点眼里有几个 `UN`、tc 规则装没装，
和声明的场景对不上就**直接退出**：

```
  [拓扑自检] 实际拓扑与声明的场景不符, 已中止:
      ! 声明 node_failure 但三个容器都在跑。请先 docker stop cass3 (或其他节点)。
```

观测到的拓扑快照也会写进 CSV 的 `topology` 列，让每一行数据都有据可查。
确认无误要强制继续，加 `--skip-topology-check`。

## 8. 实验设计理由（写报告时要解释）

### 8.1 为什么关掉 `read_repair` 和 `speculative_retry`

完整注释见 `scripts/setup_keyspace.cql`，摘要：

- **`read_repair`（默认 `BLOCKING`）**：读操作发现副本间摘要不一致时，会在返回结果
  *之前*把最新值同步写回落后的副本。结果是第一次读也许还能观测到违例，但它同时把
  发散**修好了** —— 后续读再也复现不出来，违例率被系统性低估。
- **`speculative_retry`（默认 `99p`）**：协调者在某副本响应慢时会额外向其他副本发请求。
  我们注入了 200ms 延迟，这会**持续**触发投机重试，等于偷偷提高了实际参与的副本数，
  让 `CL=ONE` 表现得像 `CL=TWO`。

两者都是 Cassandra 的自愈机制。实验目的恰恰是观测**未自愈状态下**的副本发散。

> `additional_write_policy`（写侧投机重试，4.0+ 新增，默认 `99p`）在 Cassandra 4.1 上
> 无法通过 `ALTER TABLE` 修改（语句被静默忽略）。在 RF=3 全副本、无 transient replica 的
> 拓扑下它没有"额外副本"可投机，预期是空操作。这一点由实测反证：`W=ONE/R=ONE` 下
> RYW 违例率约 92%，若写真的被拔高到等 2 个 ack，违例率会趋近 0。

### 8.2 为什么用 `SimpleStrategy` 而不是 `NetworkTopologyStrategy`

单 DC / N=3 / RF=3 的拓扑下两者的副本放置**完全等价**，`SimpleStrategy` 更直白。

要用 `NetworkTopologyStrategy` 必须写**真实的 DC 名**。本集群用的是默认的
`SimpleSnitch`，它硬编码 DC 名为 `datacenter1`；写 `{'dc1': 3}` 会直接失败：

```
ConfigurationException: Unrecognized strategy option {dc1} passed to NetworkTopologyStrategy
```

要让 `dc1` 这个名字真的生效，得把 snitch 换成 `GossipingPropertyFileSnitch` 并重建全部三个
节点 —— 为了一个实验上没有任何差别的名字付这个代价不划算。compose 里原有的
`CASSANDRA_DC: dc1` 已删除，因为它在 `SimpleSnitch` 下从未生效，留着只会误导。

### 8.3 装置 CL 与自变量 CL 的区别

`--write-cl` / `--read-cl` 是**自变量** —— 实验就是要看它们怎么影响违例率。

`--seed-cl` / `--setup-cl` / `--verify-cl` 是**实验装置**：

- MR/WFR 的种子写用 `CL=ALL` 建立三副本共同初始状态。不这样做的话，`W=ONE` 下
  读节点大概率还没有任何数据，迭代变成无效样本，违例率被低估到接近 0 ——
  看上去"没问题"，实际只是没测到。
- MW/WFR 的收敛校验读用 `CL=ALL`，因为判的是"收敛后的定序"，必须看全部副本。
  用 `ONE` 读到一个还没收到任何写的副本，那是可见性问题，会和定序问题混为一谈。

副本不全时装置 CL 不可用会让**整轮迭代全部作废**，所以参数校验会直接拦下
（而自变量 CL 不可用只告警 —— 那正是要测的现象）。

**降级校验必须配收敛等待。** `verify-cl=ALL` 会问遍所有副本，谁的时间戳最新谁胜出，
与复制是否完成无关 —— 天然免疫可见性干扰。但故障/分区场景下只能降到 `QUORUM`，
此时若法定人数恰好没包含拿到 W2 的副本，会读到 W1 而被**误判成定序违例**，
实际是可见性假阳性（实测 `verify-cl=QUORUM` 无等待时 MW natural 模式出现约 33% 假违例）。

`--settle-ms 1000` 在校验读之前给复制留出收敛时间，假阳性即消失。
注意它只作用于**校验读**，不影响 WFR 里那次"触发义务的读"——后者必须立刻读才能测出陈旧读率。
`run_matrix.sh` 在降级场景下已自动带上。

### 8.4 为什么必须把 session 钉死在指定节点上

Python 驱动默认用 `TokenAwarePolicy(DCAwareRoundRobinPolicy)` 负载均衡，会在集群所有节点
间自动轮转协调者。不禁用的话，"从 node1 写、从 node2 读"这个实验前提根本不成立。

`common.py` 用 `WhiteListRoundRobinPolicy(['127.0.0.1'])` 解决：三个容器分别把 9042 映射到
宿主机 9042/9043/9044，所以 `Cluster(contact_points=['127.0.0.1'], port=9043)` 唯一确定 cass2；
而其他节点在 `system.peers` 里的地址是 `172.18.0.x`，不在白名单内，驱动对它们返回
`HostDistance.IGNORED`，根本不建连接池。

`connect_nodes()` 每次都查 `system.local`（节点本地表，谁做协调者就返回谁的身份）验证钉定
是否真的生效，三个 session 落在同一 IP 上会直接报错退出。

### 8.5 写时间戳由谁生成（MW/WFR 的关键前提）

Cassandra 写时间戳有三个来源，优先级从高到低：

1. CQL 里的 `USING TIMESTAMP` —— 显式指定，覆盖一切；
2. **客户端驱动生成并随请求发送 —— 现代驱动的默认行为**；
3. 协调者用自己的墙上时钟赋值 —— 仅当客户端没提供时间戳时。

本项目实测确认了第 2 条：把驱动的 `timestamp_generator` 换成固定异常值
（`1000000000000000`，即 2001 年）后，不带 `USING TIMESTAMP` 的普通写落库的
`WRITETIME` 恰好等于该值，而非写入时刻。

**所以 MW/WFR 的 skew 模式构造的是 client-supplied timestamp inversion（客户端提供的
时间戳倒挂），不是"协调者之间的时钟偏移"。** 报告里必须区分这两者。

对应的真实故障是：两个客户端进程跑在**不同机器**上，机器间 NTP 失步，各自驱动生成的
时间戳因而倒挂。由于时间戳在客户端生成，客户端机器的时钟偏移会直接决定写的定序。

### 8.6 WFR 违例的阈值条件

WFR 的 skew 模式有个容易踩的坑：**偏移必须大于"被观测的写到后续写之间的实际间隔"**，
否则客户端的自然时间戳反而更高，违例根本无法成立。

注入 200ms 复制延迟后该间隔实测约 1.1–1.4 秒（两次 `CL=ALL` 写 + 一次 `CL=ALL` 读，
每次都要跨双向延迟往返），所以默认 `--skew-us` 取 5 秒。脚本会把 `mean_gap_us` 和
`skew_too_small` 记进 CSV，让这个前提可验证而不是隐含假设。

换句话说，这本身就是一条结论：

> WFR 违例成立，当且仅当 客户端间时钟偏移 > 被观测的写到后续写之间的实际间隔

## 9. 结果文件

| 文件 | 内容 |
|---|---|
| `results/all_results.csv` | 所有模型的汇总，一行一次运行 |
| `results/<model>.csv` | 按模型分开的同样数据 |
| `results/traces/*.jsonl` | **逐次迭代**明细，一行一次迭代 |
| `results/environment.txt` | 环境版本快照 |

汇总表回答"违例率是多少"，明细回答"具体哪一次、写了什么、读到什么、经过哪个协调者、
时间戳多少、异常是哪一类"。汇总表里每个数字都能追溯到原始证据。

明细字段：`run_id` `iteration` `model` `scenario` `mode` `write_cl` `read_cl`
`write_node`/`read_node`（含实际 IP）`key` `written_value` `observed_value`
`write_timestamp` `triggered` `violation` `error_type` `error_message` `event_time`。

## 10. 项目结构

```
Cassandra-Lab/
├── docker-compose.yml        3 节点，RF=3，端口 9042/9043/9044
├── requirements.txt          固定 Python 驱动版本
├── scripts/
│   ├── bootstrap.sh          一键拉起环境 + 记录版本快照
│   ├── setup_keyspace.cql    keyspace + kv 表（含设计理由注释）
│   ├── inject_latency.sh     只延迟 storage_port 的 netem 规则
│   ├── clear_latency.sh
│   ├── partition.sh          tc 丢包制造网络分区（两侧都可访问）
│   ├── heal_partition.sh
│   └── run_matrix.sh         按场景跑完整实验矩阵
├── experiments/
│   ├── common.py             节点钉定 / 读写原语 / 参数校验 / CSV / JSONL
│   ├── topology.py           开跑前的拓扑自检
│   ├── ryw_test.py
│   ├── mr_test.py
│   ├── mw_test.py
│   └── wfr_test.py
└── results/
    ├── all_results.csv
    ├── environment.txt
    └── traces/
```
