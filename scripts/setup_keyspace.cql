CREATE KEYSPACE IF NOT EXISTS consistency_lab
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE consistency_lab;

CREATE TABLE IF NOT EXISTS kv (
    key    text PRIMARY KEY,
    value  int,
    writer text
) WITH read_repair = 'NONE'
  AND speculative_retry = 'NONE'
  AND comment = 'client-centric consistency lab: self-healing disabled on purpose';
