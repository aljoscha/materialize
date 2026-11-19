# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


"""Race a paused cluster candidate against a committed table or catalog write."""

import argparse
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
from demo import check, schemas

HOOK = "SET unsafe_failpoints = 'deterministic_candidate_before_cas={}'"
EVENT = re.compile(
    rb"deterministic candidate read subscribe=(t[0-9]+) attempt=([0-9]+) write_ts=([0-9]+)\b"
)


def read_event(log, attempt, commit=None):
    deadline = time.monotonic() + 60
    line = b""
    while time.monotonic() < deadline:
        chunk = log.readline()
        if not chunk:
            if commit is not None and commit.done():
                commit.result()
                raise RuntimeError("COMMIT returned before the candidate barrier")
            time.sleep(0.05)
            continue
        line += chunk
        if not line.endswith(b"\n"):
            continue
        match = EVENT.search(line)
        line = b""
        if match:
            subscription, n, ts = match.groups()
            check("attempt", int(n), attempt)
            print(f"subscribe={subscription.decode('ascii')} write_ts={int(ts)}")
            return subscription, int(ts)
    raise TimeoutError(f"No candidate read for attempt {attempt} within 60s")


def compete(writer, observer, log, mutation, claimed=None):
    log.seek(0, os.SEEK_END)
    with ThreadPoolExecutor(max_workers=1) as pool:
        observer.execute(HOOK.format("return"))
        try:
            commit = pool.submit(writer.execute, "COMMIT")
            subscription, ts = read_event(log, 1, commit)
            if commit.done():
                commit.result()
                raise RuntimeError("COMMIT did not pause")
            observer.execute(mutation)
            claimant_id = schemas(observer, claimed)[claimed] if claimed else None
        finally:
            observer.execute(HOOK.format("off"))
        try:
            commit.result()
        except psycopg.Error as err:
            if claimed is None or f"schema '{claimed}' already exists" not in (
                err.diag.message_primary or ""
            ):
                raise
            print(f"COMMIT rejected: schema '{claimed}' already exists")
        else:
            if claimed is not None:
                raise RuntimeError("COMMIT accepted an occupied destination")
        retry, new_ts = read_event(log, 2)
        check("retry subscription", retry, subscription)
        check("write timestamp advanced", new_ts > ts, True)
        return claimant_id


def verify_data(conn, p, schema, src_values, dst_values):
    outside = p + "_outside"
    for relation, values in (
        (f"{schema}.src", src_values),
        (f"{schema}.dst", dst_values),
        (f"{schema}.v", dst_values),
        (f"{outside}.v", dst_values),
    ):
        rows = conn.execute(f"SELECT n FROM {relation} ORDER BY n").fetchall()
        check(relation, rows, [(n,) for n in values])
    # Read persisted CREATE SQL, not just query results from already-bound IDs.
    (definition,) = conn.execute(
        """SELECT v.create_sql FROM mz_catalog.mz_views v
           JOIN mz_catalog.mz_schemas s ON v.schema_id = s.id
           JOIN mz_catalog.mz_databases d ON s.database_id = d.id
           WHERE d.name = current_database() AND s.name = %s AND v.name = 'v'""",
        (outside,),
    ).fetchone()
    check(
        "outside view references renamed view",
        f"{schema}.v" in definition.replace('"', ""),
        True,
    )
    check("outside view has no old reference", p + "_old" in definition, False)


def inspect(conn, p, sid, outside_id, claimant_id):
    check(
        "schema IDs",
        schemas(conn, p),
        {p + "_final": sid, p + "_outside": outside_id, p + "_claim": claimant_id},
    )
    verify_data(conn, p, p + "_final", [1, 2, 3], [1, 2])


def run(writer, observer, log, show_plan):
    p = "dt_" + uuid.uuid4().hex
    old, final, outside, claim = (
        p + "_" + s for s in ("old", "final", "outside", "claim")
    )
    w = writer.execute
    if show_plan:

        def print_plan(diag):
            message = diag.message_primary or ""
            if message.startswith(
                "Deterministic transaction physical plan (subscribe "
            ):
                print(message, flush=True)

        writer.add_notice_handler(print_plan)
        w("SET emit_plan_insights_notice = on")
    for c in (observer, writer):
        c.execute("SET statement_timeout = '120s'")
    w(f"CREATE SCHEMA {old}")
    w(f"CREATE SCHEMA {outside}")
    w(f"CREATE TABLE {old}.src (n int)")
    w(f"INSERT INTO {old}.src VALUES (0), (1)")
    w(f"CREATE TABLE {old}.dst (n int)")
    w(f"CREATE VIEW {old}.v AS SELECT n FROM {old}.dst")
    w(f"CREATE VIEW {outside}.v AS SELECT n FROM {old}.v")
    ids = schemas(observer, p)

    w("BEGIN DETERMINISTIC TRANSACTION")
    w(f"ALTER SCHEMA {old} RENAME TO {final}")
    w(f"INSERT INTO {final}.dst SELECT n FROM {final}.src")
    compete(writer, observer, log, f"UPDATE {old}.src SET n = 2 WHERE n = 0")
    check("after retry", schemas(observer, p), {final: ids[old], outside: ids[outside]})
    verify_data(observer, p, final, [1, 2], [1, 2])
    observer.execute(f"INSERT INTO {final}.src VALUES (3)")
    verify_data(observer, p, final, [1, 2, 3], [1, 2])

    w("BEGIN DETERMINISTIC TRANSACTION")
    w(f"ALTER SCHEMA {final} RENAME TO {claim}")
    w(f"INSERT INTO {claim}.dst VALUES (99)")
    claimant_id = compete(
        writer,
        observer,
        log,
        f"CREATE SCHEMA {claim}",
        claimed=claim,
    )
    inspect(observer, p, ids[old], ids[outside], claimant_id)
    print(
        f"After restart: python3 competition.py inspect {p} {ids[old]} {ids[outside]} {claimant_id}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.environ.get(
            "MZ_URL", "postgresql://materialize@localhost:6875/materialize"
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--server-log", required=True)
    runner.add_argument(
        "--show-plan",
        action="store_true",
        help="print the installed LIR for each paused commit",
    )
    reader = commands.add_parser("inspect", help="read a completed run after restart")
    for arg in ("prefix", "schema_id", "outside_id", "claimant_id"):
        reader.add_argument(arg)
    args = parser.parse_args()
    if args.command == "inspect" and not re.fullmatch(r"dt_[0-9a-f]{32}", args.prefix):
        parser.error("expected a run prefix printed by competition.py")
    with psycopg.connect(args.url, autocommit=True) as observer:
        if args.command == "inspect":
            inspect(
                observer, args.prefix, args.schema_id, args.outside_id, args.claimant_id
            )
        else:
            with (
                open(args.server_log, "rb") as log,
                psycopg.connect(args.url, autocommit=True) as writer,
            ):
                run(writer, observer, log, args.show_plan)


if __name__ == "__main__":
    main()
