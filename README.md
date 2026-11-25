# PostgreSQL Contention Lab

Measure how client concurrency and transaction duration affect throughput and tail latency when transactions compete to update one row.

Requires Python 3 and Docker with access to the official `postgres:17` image.

```sh
python3 run.py --clients 1 4 16 32 --hold-ms 0 5 --seconds 10 > results.csv
```

Each point starts with a fresh table. All clients update the same row, then sleep inside the transaction before committing. This extends the time the row lock is held. The script reports pgbench throughput and nearest-rank transaction latency percentiles from pgbench logs. It starts a disposable Docker container without publishing PostgreSQL ports and removes it on exit.

Compare throughput and p99 at each hold time. Look for the first concurrency level at which throughput plateaus while p99 keeps rising. The lab uses persistent pgbench connections and one client worker. Its results describe a synthetic hot-row workload. Record image digest, Docker limits, machine and trial duration, and repeat trials before drawing conclusions. This repository ships no measured results.
