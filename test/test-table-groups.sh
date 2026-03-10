#!/usr/bin/env bash
#
# Manual test script for PG Table Groups.
#
# Prerequisites:
#   - PostgreSQL running on localhost:5432 (user: postgres, password: postgres)
#   - Materialize running:
#       bin/environmentd --reset -- --all-features --unsafe-mode
#
# Usage:
#   chmod +x test/test-table-groups.sh
#   test/test-table-groups.sh

set -euo pipefail

PG_CONN="postgresql://postgres:postgres@localhost:5432/postgres"
MZ_CONN="postgresql://materialize@localhost:6875/materialize"
MZ_SYSTEM_CONN="postgresql://mz_system@localhost:6877/materialize"

pg()         { psql "$PG_CONN" -q -X "$@"; }
mz()         { psql "$MZ_CONN" -q -X "$@"; }
mz_system()  { psql "$MZ_SYSTEM_CONN" -q -X "$@"; }
mz_query()   { psql "$MZ_CONN" -X "$@"; }

echo "=== Step 1: Set up PostgreSQL tables ==="

pg <<'SQL'
DROP PUBLICATION IF EXISTS mz_source;

DROP TABLE IF EXISTS public.events_2024_01 CASCADE;
DROP TABLE IF EXISTS public.events_2024_02 CASCADE;
DROP TABLE IF EXISTS public.events_2024_03 CASCADE;
DROP TABLE IF EXISTS public.other_table CASCADE;

DROP SCHEMA IF EXISTS tenant_a CASCADE;
DROP SCHEMA IF EXISTS tenant_b CASCADE;

-- Scenario A: Time-partitioned tables in public schema
CREATE TABLE public.events_2024_01 (
    id INTEGER PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    payload TEXT
);
CREATE TABLE public.events_2024_02 (
    id INTEGER PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    payload TEXT
);
CREATE TABLE public.events_2024_03 (
    id INTEGER PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    payload TEXT
);
-- A table that should NOT be matched by the pattern
CREATE TABLE public.other_table (
    id INTEGER PRIMARY KEY,
    value TEXT
);

ALTER TABLE public.events_2024_01 REPLICA IDENTITY FULL;
ALTER TABLE public.events_2024_02 REPLICA IDENTITY FULL;
ALTER TABLE public.events_2024_03 REPLICA IDENTITY FULL;
ALTER TABLE public.other_table REPLICA IDENTITY FULL;

INSERT INTO public.events_2024_01 VALUES
    (1, '2024-01-15 10:00:00+00', 'jan event A'),
    (2, '2024-01-20 14:30:00+00', 'jan event B');
INSERT INTO public.events_2024_02 VALUES
    (1, '2024-02-10 09:00:00+00', 'feb event A'),
    (2, '2024-02-25 16:00:00+00', 'feb event B');
INSERT INTO public.events_2024_03 VALUES
    (1, '2024-03-05 11:00:00+00', 'mar event A');
INSERT INTO public.other_table VALUES (1, 'should not appear');

-- Scenario B: Multi-tenant tables in separate schemas
CREATE SCHEMA tenant_a;
CREATE SCHEMA tenant_b;

CREATE TABLE tenant_a.orders (
    order_id INTEGER PRIMARY KEY,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE tenant_b.orders (
    order_id INTEGER PRIMARY KEY,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE tenant_a.orders REPLICA IDENTITY FULL;
ALTER TABLE tenant_b.orders REPLICA IDENTITY FULL;

INSERT INTO tenant_a.orders VALUES
    (1, 99.99, '2024-06-01 08:00:00+00'),
    (2, 149.50, '2024-06-02 12:00:00+00');
INSERT INTO tenant_b.orders VALUES
    (1, 200.00, '2024-06-01 09:00:00+00'),
    (2, 50.00, '2024-06-03 15:00:00+00');

CREATE PUBLICATION mz_source FOR ALL TABLES;
ALTER USER postgres WITH REPLICATION;
SQL

echo "  Done. Created events_2024_{01,02,03}, other_table, tenant_{a,b}.orders"

echo ""
echo "=== Step 2: Set up Materialize source ==="

mz_system -c "ALTER SYSTEM SET enable_create_table_from_source = true;"

mz <<'SQL'
CREATE SECRET IF NOT EXISTS pgpass AS 'postgres';

CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
    HOST 'localhost',
    PORT 5432,
    DATABASE 'postgres',
    USER 'postgres',
    PASSWORD SECRET pgpass
);

CREATE SOURCE IF NOT EXISTS pg_src
    IN CLUSTER quickstart
    FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
SQL

echo "  Done. Source pg_src created."

echo ""
echo "=== Step 3: Create table groups ==="

mz <<'SQL'
-- Time-partitioned: merge all events_* tables, TABLE PATTERN excludes other_table
CREATE TABLE GROUP all_events
    FROM SOURCE pg_src
    (SCHEMAS ('public'), TABLE PATTERN '^events_');

-- Multi-tenant: merge orders from both tenant schemas
CREATE TABLE GROUP all_orders
    FROM SOURCE pg_src
    (SCHEMAS ('tenant_a', 'tenant_b'));
SQL

echo "  Done. Created table groups all_events and all_orders."

echo ""
echo "=== Step 4: Query snapshot data ==="

echo "--- all_events (5 rows expected) ---"
mz_query -c "
SELECT mz_source_schema, mz_source_table, id, ts::text, payload
FROM all_events
ORDER BY mz_source_table, id;
"

echo "--- all_orders (4 rows expected) ---"
mz_query -c "
SELECT mz_source_schema, mz_source_table, order_id, amount, created_at::text
FROM all_orders
ORDER BY mz_source_schema, order_id;
"

echo "--- filter: only events_2024_02 ---"
mz_query -c "
SELECT mz_source_table, id, payload
FROM all_events
WHERE mz_source_table = 'events_2024_02'
ORDER BY id;
"

echo "--- aggregate: row count per source table ---"
mz_query -c "
SELECT mz_source_table, count(*)
FROM all_events
GROUP BY 1
ORDER BY 1;
"

echo ""
echo "=== Step 5: Test replication (insert new rows in PG) ==="

pg <<'SQL'
INSERT INTO public.events_2024_01 VALUES (3, '2024-01-30 18:00:00+00', 'jan event C');
INSERT INTO public.events_2024_03 VALUES (2, '2024-03-20 07:00:00+00', 'mar event B');
INSERT INTO tenant_b.orders VALUES (3, 75.25, '2024-06-04 10:00:00+00');
-- This should NOT appear in all_events:
INSERT INTO public.other_table VALUES (2, 'still should not appear');
SQL

echo "  Inserted rows. Waiting 3s for replication..."
sleep 3

echo "--- all_events after insert (7 rows expected) ---"
mz_query -c "
SELECT mz_source_schema, mz_source_table, id, ts::text, payload
FROM all_events
ORDER BY mz_source_table, id;
"

echo "--- all_orders after insert (5 rows expected) ---"
mz_query -c "
SELECT mz_source_schema, mz_source_table, order_id, amount, created_at::text
FROM all_orders
ORDER BY mz_source_schema, order_id;
"

echo ""
echo "=== Step 6: Cleanup ==="
read -p "Drop test objects? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    mz <<'SQL'
DROP TABLE GROUP all_events CASCADE;
DROP TABLE GROUP all_orders CASCADE;
DROP SOURCE pg_src CASCADE;
DROP CONNECTION pg CASCADE;
DROP SECRET pgpass CASCADE;
SQL
    pg <<'SQL'
DROP PUBLICATION mz_source;
DROP TABLE public.events_2024_01, public.events_2024_02, public.events_2024_03, public.other_table;
DROP TABLE tenant_a.orders, tenant_b.orders;
DROP SCHEMA tenant_a, tenant_b;
SQL
    echo "  Cleaned up."
else
    echo "  Skipped cleanup. Drop manually when done."
fi

echo ""
echo "=== Done ==="
