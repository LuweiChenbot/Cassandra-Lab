# 终端操作手册（快速上手）

这份手册只讲**最常用的操作**：启动集群 → 跑一条 RYW 实验 → 看结果 → 收工。
设计原理、另外三种一致性模型、故障/分区场景见 [README.md](README.md)。

- 所有命令都在**项目根目录**下执行
- 文中的输出都是实际运行截取的（key 名、时间戳会和你的不一样）
- 命令块里**没有行尾注释**，可以整块直接粘贴。自己写命令时也别在行尾加 `# 注释`：
  macOS 的 zsh 默认不把交互输入里的 `#` 当注释，它会被当成一个参数甚至一条命令

---

## 0. 开工前

1. 打开 **Docker Desktop**，等菜单栏的鲸鱼图标不再跳动。
2. 进入项目目录，拉最新代码：

```bash
cd Cassandra-Lab
git pull
```

## 1. 启动集群

先看你是哪种情况：

```bash
docker ps -a --format '{{.Names}}\t{{.Status}}'
```

| 看到的输出 | 你的情况 | 该跑什么 | 耗时 |
|---|---|---|---|
| 什么都没有 | 第一次用，或删过容器 | `./scripts/bootstrap.sh` | 4–6 分钟 |
| `cass1  Exited (...)` | 电脑或 Docker 重启过 | `docker compose up -d --wait` | 约 1 分钟 |
| `cass1  Up ... (healthy)` | 已经在跑 | 什么都不用做 | — |

**第一次用：**

```bash
./scripts/bootstrap.sh
```

它会依次起三个节点、建表、装 Python 依赖、检查连接。最后看到 `Environment ready.` 就成功了。

**电脑重启过：**

```bash
docker compose up -d --wait
```

数据都还在，**不需要**再跑 bootstrap。

**确认集群健康：**

```bash
docker exec cass1 nodetool status
```

要看到**三行 `UN`**（Up + Normal）：

```
UN  172.18.0.4  ...  16  100.0%  ...  rack1
UN  172.18.0.3  ...  16  100.0%  ...  rack1
UN  172.18.0.2  ...  16  100.0%  ...  rack1
```

## 2. 注入复制延迟 ⚠️ 每次启动集群后都要做

```bash
./scripts/inject_latency.sh
```

```
[cass1] 已注入: dport 7000 延迟 200ms ± 50ms
[cass2] 已注入: dport 7000 延迟 200ms ± 50ms
[cass3] 已注入: dport 7000 延迟 200ms ± 50ms
```

它让三个节点之间的**数据复制**慢 200ms（客户端读写本身不受影响）。
不注入的话，本机容器之间复制只要一两毫秒，几乎看不到违例。

> **容器一重启，延迟规则就没了** —— 它存在于容器的网络命名空间里，不是写在磁盘上的配置。
> 忘了这一步，实验会跑出全是 0 的结果。那不是 Cassandra 很一致，是你没注入。

随时检查当前延迟：

```bash
docker exec cass1 tc qdisc show dev eth0 | grep delay
```

有输出（含 `delay 200ms`）= 已注入；没有输出 = 没注入。

## 3. 手动体验一次 RYW 违例（可选，约 1 分钟）

> 只想跑实验的话，可以直接跳到第 4 节。

**RYW（Read-Your-Writes）**：你刚写进去的东西，你自己马上就该读得到。
下面用 `cqlsh` 手动看一次它被打破的样子。

先把延迟**临时调到 800ms**，再给这次演示起一个新 key（用当前时间戳）：

```bash
DELAY=800ms JITTER=0ms ./scripts/inject_latency.sh
K=demo_$(date +%s)
```

每次演示都要换新 key —— 用过的 key 早就复制完了，再读当然读得到。

为什么要调大：`docker exec ... cqlsh` 每调用一次本身就要 ~0.4 秒。
复制只慢 200ms 的话，等你的读命令发出时复制早已完成，什么都看不到。

### 实验 A：`ONE` 写 + `ONE` 读

经 cass1 写，**立刻**经 cass2 读。两条命令**必须用 `&&` 连成一行**一起执行 ——
分两次粘贴的话，你手动的间隔有好几秒，复制早就到了。

```bash
docker exec cass1 cqlsh -e "CONSISTENCY ONE; INSERT INTO consistency_lab.kv (key, value, writer) VALUES ('$K', 1, 'manual');" && docker exec cass2 cqlsh -e "CONSISTENCY ONE; SELECT * FROM consistency_lab.kv WHERE key='$K';"
```

```
Consistency level set to ONE.
Consistency level set to ONE.

 key | value | writer
-----+-------+--------


(0 rows)
```

**刚写进去，却读不到** —— 这就是 RYW 违例。
cass1 已经确认写入成功，但数据还在去 cass2 的路上。

等两秒，再读一次：

```bash
docker exec cass2 cqlsh -e "CONSISTENCY ONE; SELECT * FROM consistency_lab.kv WHERE key='$K';"
```

```
 key             | value | writer
-----------------+-------+--------
 demo_1789713472 |     1 | manual

(1 rows)
```

数据到了。这叫**最终一致**：迟早会一致，但不保证马上。

### 实验 B：`QUORUM` 写 + `QUORUM` 读

同样 800ms 延迟，只把一致性级别换成 `QUORUM`（3 个副本中的 2 个）：

```bash
K=demo_$(date +%s)
docker exec cass1 cqlsh -e "CONSISTENCY QUORUM; INSERT INTO consistency_lab.kv (key, value, writer) VALUES ('$K', 1, 'manual');" && docker exec cass2 cqlsh -e "CONSISTENCY QUORUM; SELECT * FROM consistency_lab.kv WHERE key='$K';"
```

```
 key              | value | writer
------------------+-------+--------
 demo_1789713556  |     1 | manual

(1 rows)
```

**立刻就读到了。** 写要 2 个副本确认、读要问 2 个副本，2 + 2 > 3，两组副本必然有重叠，
所以一定能读到最新值。

代价是慢：这一写一读**各花了约 2 秒**，因为要等远端副本回应。

> 别把演示延迟调到 1 秒以上 —— `QUORUM` 写要等一个来回，会超过 Cassandra 默认 2 秒的写超时而报错。

### 演示完，恢复标准延迟

```bash
./scripts/inject_latency.sh
```

## 4. 用脚本跑一条 RYW 实验

手动演示只能看一次。脚本把"写一次、立刻换个节点读一次"重复 N 遍，统计违例比例：

```bash
./.venv/bin/python experiments/ryw_test.py --write-cl ONE --read-cl ONE -n 100
```

输出（节选）：

```
=== RYW: W=ONE@node1  R=ONE@node2  n=100  场景=normal ===
  [拓扑自检] 场景=normal
    cass1: 环内所见 [172.18.0.2=UN, 172.18.0.3=UN, 172.18.0.4=UN]  netem[延迟]
    cass2: 环内所见 [172.18.0.2=UN, 172.18.0.3=UN, 172.18.0.4=UN]  netem[延迟]
    cass3: 环内所见 [172.18.0.2=UN, 172.18.0.3=UN, 172.18.0.4=UN]  netem[延迟]
  [pin] node1 (cass1) -> 127.0.0.1:9042 => broadcast_address 172.18.0.2
  [pin] node2 (cass2) -> 127.0.0.1:9043 => broadcast_address 172.18.0.3
  [pin] node3 (cass3) -> 127.0.0.1:9044 => broadcast_address 172.18.0.4
    [   10/100] 违例    10 (100.0%)  异常 0
    ...
    [  100/100] 违例   100 (100.0%)  异常 0
  逐次明细 -> results/traces/ryw_normal_natural_WONE_RONE_144002.jsonl

  ------------------------------------------------------------------
  模型=ryw  场景=normal  模式=natural
  W=ONE  R=ONE  verify=-
  迭代=100  有效样本=100  违例=100  违例率=100.00%
  异常=0 (0.00%)   read_none=100 read_stale=0 w_node=1 r_node=2
  ------------------------------------------------------------------
```

怎么看：

| 输出 | 意思 |
|---|---|
| `[拓扑自检] ... netem[延迟]` | 开跑前自动检查集群；`netem[延迟]` 表示延迟已注入。**看到 `netem[无]` 就该停下来，先注入延迟** |
| `[pin]` | 写固定走 cass1、读固定走 cass2。换节点读才能暴露违例 |
| `违例率=100.00%` | 100 次里有 100 次没读到自己刚写的值 |
| `read_none=100` | 违例的具体形式：读到"不存在"，数据还没复制过来 |

换成 `QUORUM` 对照：

```bash
./.venv/bin/python experiments/ryw_test.py --write-cl QUORUM --read-cl QUORUM -n 100
```

```
  迭代=100  有效样本=100  违例=0  违例率=0.00%
```

> 嫌 `./.venv/bin/python` 太长：先执行 `source .venv/bin/activate`，之后直接写 `python` 即可。

## 5. 常用参数

| 参数 | 作用 | 例子 |
|---|---|---|
| `--write-cl` | 写一致性级别 | `ONE` / `QUORUM` / `ALL` |
| `--read-cl` | 读一致性级别 | 同上 |
| `-n` | 重复次数 | `-n 200` |
| `--write-node` / `--read-node` | 写 / 读走哪个节点（1、2、3） | `--write-node 1 --read-node 3` |
| `--no-trace` | 不保存逐次明细 | 跑很多组时省空间 |

四种典型组合在标准延迟（200ms ± 50ms）下的实测结果：

| `--write-cl` | `--read-cl` | 违例率 | 为什么 |
|---|---|---|---|
| `ONE` | `ONE` | ≈ 100% | 1 + 1 ≤ 3，写和读问到的副本可能不重叠 |
| `ONE` | `QUORUM` | ≈ 10–30%，会波动 | 1 + 2 = 3，刚好压线，看读到哪个副本 |
| `QUORUM` | `QUORUM` | 0% | 2 + 2 > 3，必然重叠 |
| `ALL` | `ONE` | 0% | 3 + 1 > 3，写已经到了所有副本 |

规律一句话：**写确认的副本数 + 读询问的副本数 > 3，就不会违例。**

## 6. 看结果

每跑一次，结果自动追加到：

| 文件 | 内容 |
|---|---|
| `results/ryw.csv` | 每次运行一行汇总：一致性级别、违例率、实测延迟…… |
| `results/traces/ryw_*.jsonl` | 每次**迭代**一行明细，排查奇怪结果时用 |

用表格软件打开最方便：

```bash
open results/ryw.csv
```

> CSV 里的 `delay_ms` 是从节点上**实际读到**的延迟，而不是你以为设了多少。
> 拿到奇怪的结果时，先看这一列。

## 7. 收工

**停止集群，数据保留**（平时用这个）。下次用 `docker compose up -d --wait` 恢复：

```bash
docker compose stop
```

**删除容器，数据一起删掉**。下次要重新跑 `./scripts/bootstrap.sh`：

```bash
docker compose down
```

无论哪种，**下次启动后都要重新注入延迟**（第 2 节）。

## 8. 常见问题

**`Cannot connect to the Docker daemon`**
Docker Desktop 没打开。打开它，等鲸鱼图标稳定后再试。

**违例率全是 0**
十有八九是没注入延迟，尤其是电脑或 Docker 刚重启过。
跑 `./scripts/inject_latency.sh`，再确认实验输出里是 `netem[延迟]`。

**`[拓扑自检] 实际拓扑与声明的场景不符, 已中止`**
集群状态不对，比如有节点没起来。先 `docker exec cass1 nodetool status`，确认三行都是 `UN`。

**第 3 节手动演示，第一次没看到违例**
集群刚启动时，第一次调用 `cqlsh` 比平时慢（要预热），读命令发出前复制就完成了。
重新执行 `K=demo_$(date +%s)` 换个 key，再试一次。

**`WriteTimeout` 报错**
延迟调得太大，`QUORUM` / `ALL` 等不到远端确认。恢复标准设置：`./scripts/inject_latency.sh`。

---

**下一步**：MR / MW / WFR 三种模型、节点故障和网络分区场景、延迟扫描出图 → [README.md](README.md)
