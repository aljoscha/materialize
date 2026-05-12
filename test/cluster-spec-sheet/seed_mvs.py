"""Seed a local envd with N materialized views, sharded across pad clusters.

Mirrors the EnvdScalabilityMvsScenario layout (10k MVs per cluster, on a
trivial 1-row base table). Connects to a local envd on the standard
external SQL port (6875). Run as:

    python3 test/cluster-spec-sheet/seed_mvs.py --target 50000

Idempotent: it queries `mz_materialized_views` to find the highest
existing `pad_mv_N` and resumes from there. Re-running is a no-op if the
target has already been hit.
"""

import argparse
import sys
import time
from typing import Optional

import psycopg


PAD_SCHEMA = "pad_schema"
PAD_BASE = "pad_base"
MVS_PER_CLUSTER = 10_000


def connect(dsn: str) -> psycopg.Connection:
    conn = psycopg.connect(dsn, autocommit=True)
    return conn


def exec_sql(conn: psycopg.Connection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.encode())


def query_one(conn: psycopg.Connection, sql: str) -> Optional[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql.encode())
        return cur.fetchone()


def current_n(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            b"SELECT name FROM mz_materialized_views WHERE name LIKE 'pad_mv_%'"
        )
        best = 0
        for (name,) in cur.fetchall():
            try:
                n = int(name.split("pad_mv_", 1)[1])
                if n > best:
                    best = n
            except (ValueError, IndexError):
                continue
        return best


def ensure_base(conn: psycopg.Connection) -> None:
    exec_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {PAD_SCHEMA}")
    # CREATE TABLE IF NOT EXISTS so resume is idempotent.
    exec_sql(conn, f"CREATE TABLE IF NOT EXISTS {PAD_SCHEMA}.{PAD_BASE} (id int, val text)")
    row = query_one(conn, f"SELECT count(*) FROM {PAD_SCHEMA}.{PAD_BASE}")
    if row and row[0] == 0:
        exec_sql(conn, f"INSERT INTO {PAD_SCHEMA}.{PAD_BASE} VALUES (1, 'x')")


def ensure_cluster(conn: psycopg.Connection, cluster_idx: int, replica_size: str) -> None:
    cname = f"pad_c_{cluster_idx}"
    row = query_one(conn, f"SELECT count(*) FROM mz_clusters WHERE name = '{cname}'")
    if row and row[0] == 0:
        print(f"  creating cluster {cname} (size {replica_size})", flush=True)
        exec_sql(conn, f"CREATE CLUSTER {cname} SIZE '{replica_size}'")


def seed(
    conn: psycopg.Connection,
    target: int,
    replica_size: str,
    log_every: int = 500,
) -> None:
    ensure_base(conn)
    start_n = current_n(conn)
    if start_n >= target:
        print(f"already at N={start_n} ≥ target {target}; nothing to do", flush=True)
        return

    print(f"starting at N={start_n}, target N={target}", flush=True)
    next_i = start_n + 1
    started = time.time()
    last_log = started
    created_since_log = 0

    while next_i <= target:
        cluster_idx = (next_i - 1) // MVS_PER_CLUSTER
        ensure_cluster(conn, cluster_idx, replica_size)
        cluster_end = min(target, (cluster_idx + 1) * MVS_PER_CLUSTER)

        for i in range(next_i, cluster_end + 1):
            exec_sql(
                conn,
                f"CREATE MATERIALIZED VIEW {PAD_SCHEMA}.pad_mv_{i} "
                f"IN CLUSTER pad_c_{cluster_idx} "
                f"AS SELECT id, val FROM {PAD_SCHEMA}.{PAD_BASE} WHERE id < {i}",
            )
            created_since_log += 1
            if created_since_log >= log_every or i == target:
                now = time.time()
                rate = created_since_log / (now - last_log) if now > last_log else 0
                overall = (i - start_n) / (now - started) if now > started else 0
                print(
                    f"  created up to pad_mv_{i} "
                    f"({rate:.1f}/s recent, {overall:.1f}/s overall, "
                    f"{i}/{target} total)",
                    flush=True,
                )
                last_log = now
                created_since_log = 0

        next_i = cluster_end + 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", type=int, default=50000, help="Final N (default 50000)")
    ap.add_argument(
        "--dsn",
        default="postgres://materialize@localhost:6875/materialize?sslmode=disable",
        help="psycopg DSN",
    )
    ap.add_argument(
        "--replica-size",
        default="scale=1,workers=4",
        help="SIZE clause for each pad cluster",
    )
    ap.add_argument("--log-every", type=int, default=500)
    args = ap.parse_args()

    conn = connect(args.dsn)
    seed(conn, args.target, args.replica_size, args.log_every)
    return 0


if __name__ == "__main__":
    sys.exit(main())
