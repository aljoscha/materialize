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
import hashlib
import json
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


def compete(writer, observer, log, *mutations, claimed=None):
    log.seek(0, os.SEEK_END)
    with ThreadPoolExecutor(max_workers=1) as pool:
        observer.execute(HOOK.format("return"))
        try:
            commit = pool.submit(writer.execute, "COMMIT")
            subscription, ts = read_event(log, 1, commit)
            if commit.done():
                commit.result()
                raise RuntimeError("COMMIT did not pause")
            for mutation in mutations:
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
        return claimant_id, ts


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


def catalog_rows(raw, sid, outside_id):
    # Query by stable IDs, not by the expected new name or SQL. Keep duplicate
    # rows so an insertion without its matching retraction fails comparison.
    return [
        row
        for (row,) in raw.execute(
            """SELECT data FROM mz_internal.mz_catalog_raw
           WHERE CASE data->>'kind'
             WHEN 'Schema' THEN mz_internal.parse_catalog_id(data->'key'->'id')
             WHEN 'Item' THEN mz_internal.parse_catalog_id(data->'value'->'schema_id')
           END IN (%s, %s)
           ORDER BY data->>'kind', (data->'key')::text""",
            (sid, outside_id),
        )
    ]


def expected_rename(rows, old, final):
    check("fixture catalog rows", len(rows), 6)
    for row in rows:
        v = row["value"]
        if row["kind"] == "Schema":
            if v["name"] == old:
                v["name"] = final
        else:
            definition = v["definition"]["V1"]
            sql = definition["create_sql"]
            check(f'{v["name"]} has old qualifier', f'"{old}".' in sql, True)
            definition["create_sql"] = sql.replace(f'"{old}".', f'"{final}".')
    return rows


def allocator(raw):
    ((row,),) = raw.execute(
        """SELECT data FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'IdAlloc'
           AND data->'key'->>'name' = 'auditlog'"""
    ).fetchall()
    return row


def verify_audits(raw, p, sid, claimant_id, user, database):
    events = [
        e
        for (e,) in raw.execute(
            """WITH events AS (
               SELECT data->'key'->'event'->'V1' AS e FROM mz_internal.mz_catalog_raw
               WHERE data->>'kind' = 'AuditLog')
           SELECT e FROM events WHERE e->'details'->'SchemaV2'->>'name' IN (%s, %s)
             OR e->'details'->'RenameSchemaV1'->>'id' = %s
           ORDER BY (e->>'id')::uint8""",
            ("race_" + p, p + "_claim", sid),
        )
    ]
    check("race, rename and claimant audit count", len(events), 4)
    first, mid, rename, claim = events
    check(
        "allocator retry rename",
        mid["details"]["RenameSchemaV1"],
        {
            "database_name": database,
            "id": sid,
            "old_name": p + "_old",
            "new_name": p + "_mid",
        },
    )
    for e, name in ((first, "race_" + p), (claim, p + "_claim")):
        check(
            "competing CREATE SCHEMA",
            (e["event_type"], e["object_type"], e["details"]["SchemaV2"]["name"]),
            (1, 10, name),
        )
    check("claimant audit id", claim["details"]["SchemaV2"]["id"], claimant_id)
    check(
        "rename audit",
        (
            rename["event_type"],
            rename["object_type"],
            rename["user"],
            rename["details"],
        ),
        (
            3,
            10,
            {"inner": user},
            {
                "RenameSchemaV1": {
                    "database_name": database,
                    "id": sid,
                    "old_name": p + "_mid",
                    "new_name": p + "_final",
                }
            },
        ),
    )
    # Check all events using these IDs, including unrelated payloads.
    ids = [e["id"] for e in events]
    check("successive audit IDs", ids, list(range(first["id"], first["id"] + 4)))
    ((count,),) = raw.execute(
        """SELECT count(*) FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'AuditLog'
           AND (data->'key'->'event'->'V1'->>'id')::uint8 BETWEEN %s AND %s""",
        (ids[0], ids[-1]),
    ).fetchall()
    check("no duplicate durable audit IDs", count, 4)
    check("allocator past claimant", allocator(raw)["value"]["next_id"] > ids[-1], True)
    return events


def inspect(conn, raw, p, sid, outside_id, claimant_id):
    check(
        "schema IDs",
        schemas(conn, p),
        {p + "_final": sid, p + "_outside": outside_id, p + "_claim": claimant_id},
    )
    verify_data(conn, p, p + "_final", [1, 2, 3], [1, 2])
    user, database = conn.execute("SELECT current_user, current_database()").fetchone()
    events = verify_audits(raw, p, sid, claimant_id, user, database)
    rows = catalog_rows(raw, sid, outside_id) + catalog_rows(
        raw, claimant_id, claimant_id
    )
    return (
        events,
        hashlib.sha256(json.dumps([rows, events], sort_keys=True).encode()).hexdigest(),
    )


def run(writer, observer, raw, log, show_plan):
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
    expected = expected_rename(catalog_rows(raw, ids[old], ids[outside]), old, final)
    alloc = allocator(raw)
    first_audit_id = alloc["value"]["next_id"]
    (mock_ts,) = raw.execute("SHOW unsafe_mock_audit_event_timestamp").fetchone()

    # Bound table plans retain the catalog revision used for authorization.
    # Race the allocator in a DDL-only transaction, then race the source in
    # the mixed transaction without invalidating those table bindings.
    mid = p + "_mid"
    w("BEGIN DETERMINISTIC TRANSACTION")
    w(f"ALTER SCHEMA {old} RENAME TO {mid}")
    _, alloc_ts = compete(writer, observer, log, f"CREATE SCHEMA race_{p}")

    w("BEGIN DETERMINISTIC TRANSACTION")
    w(f"ALTER SCHEMA {mid} RENAME TO {final}")
    w(f"INSERT INTO {final}.dst SELECT n FROM {final}.src")
    _, ts = compete(writer, observer, log, f"UPDATE {mid}.src SET n = 2 WHERE n = 0")
    check("retry durable rows", catalog_rows(raw, ids[old], ids[outside]), expected)
    alloc["value"]["next_id"] += 3
    check("retry allocator", allocator(raw), alloc)
    check("after retry", schemas(observer, p), {final: ids[old], outside: ids[outside]})
    verify_data(observer, p, final, [1, 2], [1, 2])
    observer.execute(f"INSERT INTO {final}.src VALUES (3)")
    verify_data(observer, p, final, [1, 2, 3], [1, 2])

    w("BEGIN DETERMINISTIC TRANSACTION")
    w(f"ALTER SCHEMA {final} RENAME TO {claim}")
    w(f"INSERT INTO {claim}.dst VALUES (99)")
    claimant_id, _ = compete(
        writer,
        observer,
        log,
        f"CREATE SCHEMA {claim}",
        claimed=claim,
    )
    check(
        "rejected rename durable rows",
        catalog_rows(raw, ids[old], ids[outside]),
        expected,
    )
    alloc["value"]["next_id"] += 1
    check("only claimant allocated", allocator(raw), alloc)
    events, digest = inspect(observer, raw, p, ids[old], ids[outside], claimant_id)
    check("competing DDL used original allocator", events[0]["id"], first_audit_id)
    check(
        "allocator retry audit timestamp",
        events[1]["occurred_at"],
        {"millis": int(mock_ts or alloc_ts)},
    )
    # The subscription keeps its audit time from installation across retries.
    check(
        "rename audit timestamp",
        events[2]["occurred_at"],
        {"millis": int(mock_ts or ts)},
    )
    print(
        f"After restart: python3 competition.py inspect {p} {ids[old]} {ids[outside]} {claimant_id} --digest {digest}"
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
    reader.add_argument(
        "--digest", help="compare full durable rows with the completed run"
    )
    args = parser.parse_args()
    if args.command == "inspect" and not re.fullmatch(r"dt_[0-9a-f]{32}", args.prefix):
        parser.error("expected a run prefix printed by competition.py")
    # The public SQL role cannot read mz_catalog_raw. Only SELECT/SHOW use
    # the internal listener. MZ_SYSTEM_URL can point at a nonstandard port.
    raw_url = os.environ.get("MZ_SYSTEM_URL") or psycopg.conninfo.make_conninfo(
        args.url, user="mz_system", port=6877
    )
    with (
        psycopg.connect(args.url, autocommit=True) as observer,
        psycopg.connect(raw_url, autocommit=True) as raw,
    ):
        if args.command == "inspect":
            _, digest = inspect(
                observer,
                raw,
                args.prefix,
                args.schema_id,
                args.outside_id,
                args.claimant_id,
            )
            if args.digest:
                check("recovered durable rows", digest, args.digest)
        else:
            with (
                open(args.server_log, "rb") as log,
                psycopg.connect(args.url, autocommit=True) as writer,
            ):
                run(writer, observer, raw, log, args.show_plan)


if __name__ == "__main__":
    main()
