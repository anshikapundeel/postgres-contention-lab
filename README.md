# PostgreSQL Contention Lab

Measure how client concurrency and transaction duration affect throughput and tail latency when transactions compete to update one row.

Requires Python 3 and Docker with access to the official `postgres:17` image.

```sh
python3 run.py --clients 1 4 16 32 --hold-ms 0 5 --seconds 10 > results.csv
```

Each point starts with a fresh table. In the hot-row workload, all clients update one shared row. In the control workload, each client updates its own row. Both workloads sleep inside the transaction before committing, extending the lock hold. The runner randomizes trial order with a recorded seed and runs three repeats by default. The script reports pgbench throughput and nearest-rank transaction latency percentiles from pgbench logs. It starts a disposable Docker container with no external network access and removes it on exit.

Compare throughput and p99 between the shared-row workload and its control at each hold time. Report the trial-to-trial range. Look for the first concurrency level at which throughput plateaus while p99 keeps rising. The lab uses persistent pgbench connections and one client worker. Its results describe a synthetic hot-row workload. Record image digest, Docker limits, machine and trial duration, and repeat trials before drawing conclusions. This repository ships no measured results.

Add `--report comparison.md` to generate paired throughput and tail-latency ratios. The CSV includes counts from pg_stat_activity, sampled throughout each trial. A zero observation count means insufficient sampling coverage. The observer runs in both layouts.
