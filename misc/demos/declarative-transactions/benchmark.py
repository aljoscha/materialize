# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


"""Closed-loop benchmark of ordinary or declarative DDL transactions. Emits JSONL."""

import argparse
import json
import math
import os
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import psycopg


def cell(url, mode, clients, renames, writes, count, warmup):
    prefix = "bench_" + uuid.uuid4().hex[:12]
    begin = "BEGIN DECLARATIVE TRANSACTION" if mode == "declarative" else "BEGIN"
    barrier = threading.Barrier(clients)
    clock = time.perf_counter
    with psycopg.connect(url, autocommit=True) as admin:
        for i in range(clients):
            admin.execute(f"CREATE SCHEMA {prefix}_{i}_a")
            admin.execute(f"CREATE TABLE {prefix}_{i}_a.data (n int)")

    def worker(i):
        root = f"{prefix}_{i}_"
        name = root + "a"
        inserted = 0
        completed = []
        failures = Counter()
        examples = {}
        try:
            with psycopg.connect(url, autocommit=True, prepare_threshold=None) as c:
                c.execute("SET statement_timeout = '120s'")
                for j in range(warmup + count):
                    if j == warmup:
                        barrier.wait(timeout=300)
                        start = clock()
                    t = clock()
                    try:
                        c.execute(begin)
                        source = name
                        dest = root + ("b" if name.endswith("_a") else "a")
                        for k in range(renames):
                            target = dest if k == renames - 1 else root + f"tmp{k}"
                            c.execute(f"ALTER SCHEMA {source} RENAME TO {target}")
                            source = target
                        for _ in range(writes):
                            c.execute(f"INSERT INTO {source}.data VALUES (1)")
                        c.execute("COMMIT")
                        name = source
                        inserted += writes
                        if j >= warmup:
                            completed.append(1000 * (clock() - t))
                    except psycopg.Error as e:
                        # Never retry unknown outcomes on a new connection.
                        if not e.sqlstate or e.sqlstate.startswith("08"):
                            raise
                        c.execute("ROLLBACK")
                        if j >= warmup:
                            failures[e.sqlstate] += 1
                            examples[e.sqlstate] = e.diag.message_primary
                end = clock()
                barrier.wait(timeout=600)
                actual = c.execute(f"SELECT count(*) FROM {name}.data").fetchone()[0]
                if actual != inserted:
                    raise RuntimeError(
                        f"expected {inserted} committed rows, found {actual}"
                    )
            return start, end, completed, failures, examples, name
        except BaseException:
            barrier.abort()
            raise

    with ThreadPoolExecutor(max_workers=clients) as pool:
        runs = list(pool.map(worker, range(clients)))
    elapsed = max(r[1] for r in runs) - min(r[0] for r in runs)
    times = sorted(t for r in runs for t in r[2])
    with psycopg.connect(url, autocommit=True) as admin:
        for *_, name in runs:
            admin.execute(f"DROP SCHEMA {name} CASCADE")
    return dict(
        mode=mode,
        clients=clients,
        renames=renames,
        writes=writes,
        attempts=count * clients,
        commits=len(times),
        elapsed_s=elapsed,
        tps=len(times) / elapsed,
        latency_ms={
            f"p{p}": times[math.ceil(p * len(times) / 100) - 1] if times else None
            for p in (50, 95, 99)
        },
        failures=dict(sum((r[3] for r in runs), Counter())),
        errors={k: v for r in runs for k, v in r[4].items()},
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--url",
        default=os.environ.get(
            "MZ_URL", "postgresql://materialize@localhost:6875/materialize"
        ),
    )
    p.add_argument("--mode", choices=["ordinary", "declarative"], required=True)
    p.add_argument("--clients", type=int, nargs="+", default=[1, 4])
    p.add_argument("--renames", type=int, nargs="+", default=[1, 2])
    p.add_argument("--writes", type=int, nargs="+", default=[0, 1])
    p.add_argument(
        "--count", type=int, default=12, help="measured attempts per client per cell"
    )
    p.add_argument(
        "--warmup", type=int, default=1, help="unmeasured attempts per client per cell"
    )
    args = p.parse_args()
    if (
        min([args.count, *args.clients, *args.renames]) < 1
        or min([args.warmup, *args.writes]) < 0
    ):
        p.error(
            "count, clients and renames must be positive; warmup and writes nonnegative"
        )
    for n in args.renames:
        for w in args.writes:
            for c in args.clients:
                print(
                    json.dumps(
                        cell(args.url, args.mode, c, n, w, args.count, args.warmup)
                    ),
                    flush=True,
                )


if __name__ == "__main__":
    main()
