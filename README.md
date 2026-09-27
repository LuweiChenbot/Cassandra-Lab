# Cassandra Client-Centric Consistency Lab

Experiments on a 3-node Apache Cassandra cluster (RF=3) testing four session guarantees under different write/read consistency levels (CL), replication delays, node failure and network partition:

| Model | Guarantee |
|---|---|
| RYW, read-your-writes | A client sees its own completed writes |
| MR, monotonic reads | A client never reads an older value after a newer one |
| MW, monotonic writes | A client's writes are applied in the order issued |
| WFR, writes-follow-reads | A write issued after a read is ordered after the write that was read |

The report is in `report/` (LaTeX source and `main.pdf`).

## Requirements

- Docker with ~4 GB memory (three JVMs with 512 MB heap each)
- Python 3.9+ and bash (macOS, Linux or WSL)
- Pinned versions: `cassandra:4.1.12` (`docker-compose.yml`), `cassandra-driver==3.30.1` (`requirements.txt`)

## Setup

```bash
./scripts/bootstrap.sh
```

This starts the cluster (nodes join one at a time, about 5 minutes), creates the keyspace from `scripts/setup_keyspace.cql`, installs the driver into `.venv`, checks that each client session is pinned to a different node, and writes versions to `results/environment.txt`. Check the cluster with `docker exec cass1 nodetool status`: three `UN` rows.

## Reproducing the report

All reported runs use 100 iterations per configuration. Results are appended to `results/<model>.csv` and `results/all_results.csv`.

**Delay sweeps** (zero jitter; used for the delay figures):

```bash
./scripts/sweep_latency.sh ryw 100 0 5 10 15 20 50 100 200 400 800   # same points for mr, mw
./scripts/sweep_latency.sh wfr 100 0 5 10 20 50
./.venv/bin/python scripts/plot_sweep.py ryw                          # -> results/ryw_delay_curves.png
./.venv/bin/python scripts/plot_sweep.py ryw --csv results/ryw_mac.csv --out ryw_mac.png   # committed data
```

**Scenario matrices** (4 models × 4 W/R combinations), each run at 0 ms and at 100 ms ± 50 ms delay:

```bash
./scripts/clear_latency.sh                              # 0 ms; or: DELAY=100ms JITTER=50ms ./scripts/inject_latency.sh
./scripts/run_matrix.sh normal 100

docker stop cass3                                       # node failure
./scripts/run_matrix.sh node_failure 100
docker compose up -d --wait cass3

DELAY=100ms JITTER=50ms ./scripts/inject_latency.sh     # partition needs the qdisc this creates
./scripts/partition.sh cass3
./scripts/run_matrix.sh partition_majority 100
./scripts/run_matrix.sh partition_minority 100
./scripts/heal_partition.sh
```

`tc` rules are lost when a container restarts, so re-inject latency after restarting a node. Each experiment checks the live topology against `--scenario` and exits on a mismatch.

**RYW tracing diagnostic** (ONE/QUORUM at 100 ms delay, 20 traced iterations):

```bash
DELAY=100ms JITTER=0ms ./scripts/inject_latency.sh
./.venv/bin/python experiments/ryw_diagnostic.py --write-cl ONE --read-cl QUORUM -n 20
```

Single experiments can be run directly, e.g. `./.venv/bin/python experiments/ryw_test.py --write-cl ONE --read-cl QUORUM -n 100`; `--help` lists the options.

## Result files

Columns: CL settings, iterations, valid samples (`triggered`), violations, errors, the delay and jitter read back from `tc`, and the observed topology.

| File | Contents | Used in |
|---|---|---|
| `ryw_mac.csv`, `ryw_windows.csv` | RYW sweeps on each host; the Windows file also has jitter probes and partition runs | RYW figures and text |
| `mr.csv` | MR sweep, matrices (normal, partition) and jitter/read-path probes | MR figure and tables |
| `mw.csv` | MW sweep and matrices (normal, partition), natural and skew modes | MW tables |
| `wfr.csv` | WFR matrices, including node failure | WFR scenario table |
| `wfr_windows.csv` | WFR sweep (0–50 ms) | WFR figures |
| `all_results_windows.csv` | Earlier combined log from the Windows host; every run is also in the per-model files | not used |
| `diagnostics/*.jsonl` | Traced RYW runs: two macOS, one Windows | RYW diagnostic discussion |
| `environment*.txt` | Versions for the macOS and Windows hosts | Setup |

The committed CSVs are named by host, while a fresh run writes `results/<model>.csv`. The MR/MW data come from the Windows host. Node failure was run for WFR only.

One value was corrected by hand: in the MR ONE/ONE row at 20 ms, `back_to_older` was changed from 0 to 69 (commits `fa6d7e1`, `a9fe359`) to match its 69 violations. No reported figure depends on it.

## Design notes

- **Latency only on inter-node traffic.** `tc`/`netem` delays destination port 7000 on every node; client CQL (9042) is not delayed, so no extra time is added between a client's write and its read.
- **Self-healing off.** The table sets `read_repair = 'NONE'` and `speculative_retry = 'NONE'`, which would otherwise repair or hide replica divergence.
- **Node pinning.** Each session uses `WhiteListRoundRobinPolicy` on its own host port (9042/9043/9044) so writes and reads go through the intended coordinators; this is verified via `system.local`.
- **Partitions via packet loss.** `partition.sh` drops port-7000 traffic between one node and the rest while its client port stays reachable, so both sides can be tested.
- **Setup CL vs tested CL.** Seed, setup and verify reads use ALL (QUORUM or ONE when nodes are unreachable, with `--settle-ms 1000`), separately from the W/R levels under test.
- **Timestamps.** The driver assigns write timestamps on the client, so the MW/WFR skew modes model client clock skew (`USING TIMESTAMP`), not coordinator clock skew.

## Layout

```
docker-compose.yml   3-node cluster, CQL on host ports 9042/9043/9044
experiments/         common.py, topology.py, {ryw,mr,mw,wfr}_test.py, ryw_diagnostic.py
scripts/             bootstrap, latency/partition control, run_matrix, sweep_latency, plot_sweep
results/             data files above
report/              LaTeX source, figures, main.pdf
```
