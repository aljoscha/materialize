# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Observe declarative commit, rollback, and a conflicting schema rename."""

import argparse
import os
import re
import uuid

import psycopg


def check(label, actual, expected):
    if actual != expected:
        raise RuntimeError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"{label}: {actual}")


def schemas(conn, prefix):
    return dict(
        conn.execute(
            """SELECT s.name, s.id FROM mz_catalog.mz_schemas s
               JOIN mz_catalog.mz_databases d ON d.id = s.database_id
               WHERE d.name = current_database() AND left(s.name, %s) = %s""",
            (len(prefix), prefix),
        ).fetchall()
    )


def observe(conn, prefix, names, visible, values):
    check("observer schemas", schemas(conn, prefix), names)
    rows = conn.execute(
        f"SELECT 'numbers' AS source, n FROM {visible}.numbers "
        f"UNION ALL SELECT 'archive', n FROM {visible}.archive ORDER BY source, n"
    ).fetchall()
    check(
        "observer rows (one snapshot)",
        rows,
        [(source, n) for source in ("archive", "numbers") for n in values],
    )


def run(writer, observer):
    p = "dt_" + uuid.uuid4().hex
    old, mid, final, claimed = (f"{p}_{s}" for s in ("old", "mid", "final", "claim"))
    print("Run prefix:", p, flush=True)
    w = writer.execute
    w(f"CREATE SCHEMA {old}")
    w(f"CREATE TABLE {old}.numbers (n int)")
    w(f"CREATE TABLE {old}.archive (n int)")
    sid = schemas(observer, p)[old]

    w("BEGIN DECLARATIVE TRANSACTION")
    w(f"ALTER SCHEMA {old} RENAME TO {mid}")
    w(f"INSERT INTO {mid}.numbers VALUES (1)")
    w(f"ALTER SCHEMA {mid} RENAME TO {final}")
    w(f"INSERT INTO {final}.numbers SELECT n + 1 FROM {final}.numbers")
    w(f"INSERT INTO {final}.archive SELECT n FROM {final}.numbers")
    print("Before commit:")
    observe(observer, p, {old: sid}, old, [])
    w("COMMIT")
    print("After commit:")
    observe(observer, p, {final: sid}, final, [1, 2])

    w("BEGIN DECLARATIVE TRANSACTION")
    w("COMMIT")
    print("After an empty commit (no reapplication):")
    observe(observer, p, {final: sid}, final, [1, 2])

    w("BEGIN DECLARATIVE TRANSACTION")
    w(f"UPDATE {final}.numbers SET n = n + 10")
    w(f"ALTER SCHEMA {final} RENAME TO {mid}")
    w("ROLLBACK")
    print("After rollback:")
    observe(observer, p, {final: sid}, final, [1, 2])

    w("BEGIN DECLARATIVE TRANSACTION")
    w(f"INSERT INTO {final}.numbers VALUES (99)")
    w(f"INSERT INTO {final}.archive VALUES (100)")
    w(f"ALTER SCHEMA {final} RENAME TO {claimed}")
    observer.execute(f"CREATE SCHEMA {claimed}")
    claimant_id = schemas(observer, p)[claimed]
    try:
        w("COMMIT")
    except psycopg.Error as err:
        msg = err.diag.message_primary or ""
        if not (
            (err.sqlstate == "40001" and "different result at commit" in msg)
            or (claimed in msg and "already exists" in msg)
        ):
            raise
        print(f"Commit rejected [{err.sqlstate}]: {msg}")
    else:
        raise RuntimeError("Commit unexpectedly succeeded with an occupied destination")

    inspect(observer, p, sid, claimant_id)
    print(f"After restart, run: python3 demo.py inspect {p} {sid} {claimant_id}")


def inspect(conn, prefix, sid, claimant_id):
    final = prefix + "_final"
    observe(conn, prefix, {final: sid, prefix + "_claim": claimant_id}, final, [1, 2])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("MZ_URL"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="create a fresh run and verify all three outcomes")
    reader = commands.add_parser(
        "inspect", help="verify a completed run without writes"
    )
    reader.add_argument("prefix")
    reader.add_argument("schema_id")
    reader.add_argument("claimant_id")
    args = parser.parse_args()
    if args.command == "inspect" and not re.fullmatch(r"dt_[0-9a-f]{32}", args.prefix):
        parser.error("expected a run prefix printed by the demo")
    url = args.url or "postgresql://materialize@localhost:6875/materialize"
    with psycopg.connect(url, autocommit=True) as observer:
        if args.command == "inspect":
            inspect(observer, args.prefix, args.schema_id, args.claimant_id)
        else:
            with psycopg.connect(url, autocommit=True) as writer:
                run(writer, observer)


if __name__ == "__main__":
    main()
