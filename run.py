#!/usr/bin/env python3
"""Run a hot-row PostgreSQL workload at several concurrency levels."""
import argparse
import csv
import math
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from statistics import median
from pathlib import Path
import random
import re
import secrets
import subprocess
import sys
import time


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True)


def summarize(output, log):
    samples = sorted(int(line.split()[2]) for line in log.splitlines() if line)
    if not samples:
        raise ValueError("pgbench produced no latency samples")
    throughput = re.search(r"^tps = ([\d.]+)", output, re.MULTILINE)
    if not throughput:
        raise ValueError("pgbench produced no throughput result")
    return [len(samples), float(throughput[1]),
            *(samples[math.ceil(len(samples) * p) - 1] / 1000 for p in (.50, .95, .99))]


def trial_plan(clients, hold_times, layouts, repeats, seed):
    rng = random.Random(seed)
    for repeat in range(1, repeats + 1):
        points = [(client, hold, layout) for client in clients
                  for hold in hold_times for layout in layouts]
        rng.shuffle(points)
        for client, hold, layout in points:
            yield repeat, client, hold, layout


def observe_locks(name, stop):
    blocked = observed = snapshots = 0
    query = ("SELECT count(*) FILTER (WHERE wait_event_type='Lock'), count(*) "
             "FROM pg_stat_activity WHERE application_name='contention_trial'")
    while not stop.is_set():
        values = docker("exec", name, "psql", "-XAt", "-U", "postgres", "-c", query)
        waiting, sessions = map(int, values.strip().split("|"))
        if sessions:
            snapshots += 1
            blocked += waiting
            observed += sessions
        stop.wait(0.25)
    return snapshots, blocked, observed


def report(rows):
    lines = ["# PostgreSQL contention experiment", "",
             "Each row pairs a shared-row trial with its per-client control.",
             "A ratio above one means the shared-row value was higher.", "",
             "| Clients | Hold (ms) | Pairs | Median TPS ratio | p99 ratio (min / median / max) | Lock-wait share (shared / control) |",
             "|---:|---:|---:|---:|---:|---:|"]
    for clients, hold in sorted({(key[1], key[2]) for key in rows}):
        pairs = [(group["hot-row"], group["per-client"]) for (_, c, h), group in rows.items()
                 if (c, h) == (clients, hold) and {"hot-row", "per-client"} <= group.keys()]
        if not pairs:
            continue
        ratios = [hot[4] / control[4] for hot, control in pairs if control[4] > 0]
        tail = " / ".join(f"{v:.2f}" for v in (min(ratios), median(ratios), max(ratios))) if ratios else "unavailable"
        shares = []
        for side in (0, 1):
            observations = sum(pair[side][7] for pair in pairs)
            waits = sum(pair[side][6] for pair in pairs)
            shares.append(f"{waits / observations:.1%}" if observations else "unobserved")
        throughput = median(hot[1] / control[1] for hot, control in pairs)
        lines.append(f"| {clients} | {hold} | {len(pairs)} | {throughput:.2f} | {tail} | {' / '.join(shares)} |")
    lines.extend(["", "Lock-wait share counts sampled client sessions waiting on a PostgreSQL lock. "
                  "Sampling can miss short waits and includes all lock types. Compare both layouts "
                  "at the same hold time and concurrency before attributing a latency change to contention."])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32])
    parser.add_argument("--hold-ms", type=int, nargs="+", default=[0, 5])
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--layouts", nargs="+", choices=["hot-row", "per-client"],
                        default=["hot-row", "per-client"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--report", type=Path, help="write paired comparisons as Markdown")
    args = parser.parse_args()
    if min(args.clients) < 1 or max(args.clients) > 64 or min(args.hold_ms) < 0 or min(args.seconds, args.repeats) < 1:
        parser.error("use 1–64 clients, nonnegative hold times, and positive duration and repeat count")

    name = f"postgres-contention-{secrets.token_hex(5)}"
    docker("run", "-d", "--network", "none", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17")
    try:
        for _ in range(60):
            ready = subprocess.run(
                ["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("PostgreSQL did not become ready")

        rows = {}
        writer = csv.writer(sys.stdout)
        writer.writerow(["repeat", "layout", "clients", "hold_ms", "samples",
                         "tps", "p50_ms", "p95_ms", "p99_ms", "snapshots", "lock_wait_observations", "session_observations"])
        for repeat, clients, hold, layout in trial_plan(
                args.clients, args.hold_ms, args.layouts, args.repeats, args.seed):
            docker("exec", name, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1",
                   "-c", "DROP TABLE IF EXISTS counter; CREATE TABLE counter "
                   "(id int PRIMARY KEY, n bigint); INSERT INTO counter "
                   "SELECT id, 0 FROM generate_series(1, 64) AS id")
            row = "1" if layout == "hot-row" else ":client_id + 1"
            sql = (f"BEGIN;\nUPDATE counter SET n=n+1 WHERE id={row};\n"
                   f"SELECT pg_sleep({hold}/1000.0);\nCOMMIT;\n")
            subprocess.run(["docker", "exec", "-i", name, "tee", "/tmp/workload.sql"],
                           input=sql, text=True, stdout=subprocess.DEVNULL, check=True)
            prefix = f"/tmp/bench-{secrets.token_hex(5)}"
            stop = Event()
            with ThreadPoolExecutor(max_workers=1) as executor:
                observer = executor.submit(observe_locks, name, stop)
                try:
                    output = docker("exec", "-e", "PGAPPNAME=contention_trial", name,
                                    "pgbench", "-n", "-U", "postgres", "-c", str(clients),
                                    "-j", "1", "-T", str(args.seconds), "-f", "/tmp/workload.sql",
                                    "--log", "--exit-on-abort", f"--log-prefix={prefix}", "postgres")
                finally:
                    stop.set()
                observations = observer.result()
            listing = docker("exec", name, "ls", "-1", "/tmp").splitlines()
            logs = [f"/tmp/{file}" for file in listing if file.startswith(prefix.rsplit("/", 1)[1] + ".")]
            if not logs:
                raise RuntimeError("pgbench produced no transaction log files")
            measurements = summarize(output, docker("exec", name, "cat", *logs)) + list(observations)
            writer.writerow([repeat, layout, clients, hold, *measurements])
            rows.setdefault((repeat, clients, hold), {})[layout] = measurements
            sys.stdout.flush()
        if args.report:
            args.report.write_text(report(rows))
    finally:
        docker("rm", "-f", name)


if __name__ == "__main__":
    main()
