-- consistency_lab keyspace + kv 表
--
-- RF=3 且只有 3 个节点 => 每个节点都是每个 key 的副本。
-- 这样"从哪个节点读/写"完全由客户端决定, 不受 token 分布干扰,
-- 是做 client-centric consistency 实验最干净的拓扑。

CREATE KEYSPACE IF NOT EXISTS consistency_lab
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE consistency_lab;

-- read_repair 和 speculative_retry 必须关掉, 否则:
--
-- 1) read_repair (默认 BLOCKING): 读操作发现副本间摘要不一致时, 会在返回
--    结果*之前*把最新值同步写回落后的副本。结果是第一次读可能还能观测到
--    违例, 但它同时把发散修好了 —— 后续读再也复现不出来, 违例率被系统性低估。
--    更糟的是 BLOCKING read repair 会让 R=QUORUM 的读产生"意外的"单调性,
--    掩盖 R+W<=N 本该暴露的问题。
--
-- 2) speculative_retry (默认 99p): 协调者在某副本响应慢时会额外向其他副本
--    发请求。我们注入了 200ms 网络延迟, 这会持续触发投机重试, 等于偷偷提高
--    了实际参与的副本数, 让 CL=ONE 表现得像 CL=TWO。
--
-- 两者都是 Cassandra 的"自愈"机制。实验目的恰恰是观测未自愈状态下的副本
-- 发散, 所以必须关闭 —— 这是 experiment design rationale 的一部分。
CREATE TABLE IF NOT EXISTS kv (
    key    text PRIMARY KEY,
    value  int,
    writer text
) WITH read_repair = 'NONE'
  AND speculative_retry = 'NONE'
  AND comment = 'client-centric consistency lab: self-healing disabled on purpose';
