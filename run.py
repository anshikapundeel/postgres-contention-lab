#!/usr/bin/env python3
"""Run a hot-row PostgreSQL workload at several concurrency levels."""
import argparse
import csv
import math
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32])
    parser.add_argument("--hold-ms", type=int, nargs="+", default=[0, 5])
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--layouts", nargs="+", choices=["hot-row", "per-client"],
                        default=["hot-row", "per-client"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
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

        writer = csv.writer(sys.stdout)
        writer.writerow(["repeat", "layout", "clients", "hold_ms", "samples",
                         "tps", "p50_ms", "p95_ms", "p99_ms"])
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
            output = docker("exec", name, "pgbench", "-n", "-U", "postgres", "-c", str(clients),
                            "-j", "1", "-T", str(args.seconds), "-f", "/tmp/workload.sql",
                            "--log", "--exit-on-abort", f"--log-prefix={prefix}", "postgres")
            listing = docker("exec", name, "ls", "-1", "/tmp").splitlines()
            logs = [f"/tmp/{file}" for file in listing if file.startswith(prefix.rsplit("/", 1)[1] + ".")]
            if not logs:
                raise RuntimeError("pgbench produced no transaction log files")
            writer.writerow([repeat, layout, clients, hold,
                             *summarize(output, docker("exec", name, "cat", *logs))])
            sys.stdout.flush()
    finally:
        docker("rm", "-f", name)


if __name__ == "__main__":
    main()
