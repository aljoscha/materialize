# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test PostgreSQL table groups: CREATE TABLE GROUP merges multiple upstream
# tables matching a schema/pattern into a single Materialize relation with
# mz_source_schema and mz_source_table identification columns prepended.
#

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}'

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

# ---------------------------------------------------------
# Set up PostgreSQL: time-partitioned tables + multi-tenant
# ---------------------------------------------------------

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
DROP SCHEMA IF EXISTS tenant_a CASCADE;
DROP SCHEMA IF EXISTS tenant_b CASCADE;

DROP PUBLICATION IF EXISTS mz_source;

# Scenario A: time-partitioned tables in public schema
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
# A table that should NOT be matched by the events_ pattern
CREATE TABLE public.other_table (
    id INTEGER PRIMARY KEY,
    value TEXT
);

ALTER TABLE public.events_2024_01 REPLICA IDENTITY FULL;
ALTER TABLE public.events_2024_02 REPLICA IDENTITY FULL;
ALTER TABLE public.events_2024_03 REPLICA IDENTITY FULL;
ALTER TABLE public.other_table REPLICA IDENTITY FULL;

INSERT INTO public.events_2024_01 VALUES (1, '2024-01-15 10:00:00+00', 'jan_A');
INSERT INTO public.events_2024_01 VALUES (2, '2024-01-20 14:30:00+00', 'jan_B');
INSERT INTO public.events_2024_02 VALUES (1, '2024-02-10 09:00:00+00', 'feb_A');
INSERT INTO public.events_2024_02 VALUES (2, '2024-02-25 16:00:00+00', 'feb_B');
INSERT INTO public.events_2024_03 VALUES (1, '2024-03-05 11:00:00+00', 'mar_A');

INSERT INTO public.other_table VALUES (1, 'should_not_appear');

# Scenario B: multi-tenant tables in separate schemas
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

INSERT INTO tenant_a.orders VALUES (1, 99.99, '2024-06-01 08:00:00+00');
INSERT INTO tenant_a.orders VALUES (2, 149.50, '2024-06-02 12:00:00+00');
INSERT INTO tenant_b.orders VALUES (1, 200.00, '2024-06-01 09:00:00+00');
INSERT INTO tenant_b.orders VALUES (2, 50.00, '2024-06-03 15:00:00+00');

# Scenario C: a table with mismatched schema for error testing
CREATE TABLE public.events_bad_schema (
    id INTEGER PRIMARY KEY,
    extra_col TEXT,
    ts TIMESTAMPTZ NOT NULL,
    payload TEXT
);
ALTER TABLE public.events_bad_schema REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

# ---------------------------------------------------------
# Create source
# ---------------------------------------------------------

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

# ---------------------------------------------------------
# Error cases
# ---------------------------------------------------------

# Non-existent source
! CREATE TABLE GROUP bad_group
  FROM SOURCE no_such_source
  (SCHEMAS ('public'), TABLE PATTERN '^events_')
contains:unknown catalog item

# Pattern that matches nothing
! CREATE TABLE GROUP bad_group
  FROM SOURCE mz_source
  (SCHEMAS ('nonexistent_schema'), TABLE PATTERN '^events_')
contains:no tables match

# Pattern matching tables with different schemas (events_bad_schema has extra column)
! CREATE TABLE GROUP bad_group
  FROM SOURCE mz_source
  (SCHEMAS ('public'), TABLE PATTERN '^events_')
contains:columns

# ---------------------------------------------------------
# Create table groups (after removing the bad-schema table)
# ---------------------------------------------------------

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER PUBLICATION mz_source DROP TABLE public.events_bad_schema;

# Time-partitioned: merge all events_* tables
> CREATE TABLE GROUP all_events
  FROM SOURCE mz_source
  (SCHEMAS ('public'), TABLE PATTERN '^events_2024');

# Multi-tenant: merge orders from both tenant schemas
> CREATE TABLE GROUP all_orders
  FROM SOURCE mz_source
  (SCHEMAS ('tenant_a', 'tenant_b'));

# ---------------------------------------------------------
# Snapshot: verify merged data with identification columns
# ---------------------------------------------------------

# all_events should have 5 rows from three tables; other_table excluded by pattern
> SELECT mz_source_schema, mz_source_table, id, payload FROM all_events ORDER BY mz_source_table, id
mz_source_schema mz_source_table id payload
---
public events_2024_01 1 jan_A
public events_2024_01 2 jan_B
public events_2024_02 1 feb_A
public events_2024_02 2 feb_B
public events_2024_03 1 mar_A

# all_orders should have 4 rows from two schemas
> SELECT mz_source_schema, mz_source_table, order_id, amount FROM all_orders ORDER BY mz_source_schema, order_id
mz_source_schema mz_source_table order_id amount
---
tenant_a orders 1 99.99
tenant_a orders 2 149.50
tenant_b orders 1 200.00
tenant_b orders 2 50.00

# ---------------------------------------------------------
# Filter by identification columns
# ---------------------------------------------------------

> SELECT mz_source_table, id, payload FROM all_events WHERE mz_source_table = 'events_2024_02' ORDER BY id
mz_source_table id payload
---
events_2024_02 1 feb_A
events_2024_02 2 feb_B

> SELECT mz_source_schema, order_id, amount FROM all_orders WHERE mz_source_schema = 'tenant_b' ORDER BY order_id
mz_source_schema order_id amount
---
tenant_b 1 200.00
tenant_b 2 50.00

# ---------------------------------------------------------
# Aggregation over identification columns
# ---------------------------------------------------------

> SELECT mz_source_table, count(*) FROM all_events GROUP BY 1 ORDER BY 1
mz_source_table count
---
events_2024_01 2
events_2024_02 2
events_2024_03 1

# ---------------------------------------------------------
# Replication: insert new rows upstream and verify they appear
# ---------------------------------------------------------

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO public.events_2024_01 VALUES (3, '2024-01-30 18:00:00+00', 'jan_C');
INSERT INTO public.events_2024_03 VALUES (2, '2024-03-20 07:00:00+00', 'mar_B');
INSERT INTO tenant_b.orders VALUES (3, 75.25, '2024-06-04 10:00:00+00');
INSERT INTO public.other_table VALUES (2, 'still_excluded');

> SELECT mz_source_schema, mz_source_table, id, payload FROM all_events ORDER BY mz_source_table, id
mz_source_schema mz_source_table id payload
---
public events_2024_01 1 jan_A
public events_2024_01 2 jan_B
public events_2024_01 3 jan_C
public events_2024_02 1 feb_A
public events_2024_02 2 feb_B
public events_2024_03 1 mar_A
public events_2024_03 2 mar_B

> SELECT mz_source_schema, mz_source_table, order_id, amount FROM all_orders ORDER BY mz_source_schema, order_id
mz_source_schema mz_source_table order_id amount
---
tenant_a orders 1 99.99
tenant_a orders 2 149.50
tenant_b orders 1 200.00
tenant_b orders 2 50.00
tenant_b orders 3 75.25

# ---------------------------------------------------------
# Replication: updates
# ---------------------------------------------------------

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE public.events_2024_01 SET payload = 'jan_A_updated' WHERE id = 1;
UPDATE tenant_a.orders SET amount = 999.99 WHERE order_id = 1;

> SELECT mz_source_table, id, payload FROM all_events WHERE mz_source_table = 'events_2024_01' ORDER BY id
mz_source_table id payload
---
events_2024_01 1 jan_A_updated
events_2024_01 2 jan_B
events_2024_01 3 jan_C

> SELECT mz_source_schema, order_id, amount FROM all_orders WHERE mz_source_schema = 'tenant_a' ORDER BY order_id
mz_source_schema order_id amount
---
tenant_a 1 999.99
tenant_a 2 149.50

# ---------------------------------------------------------
# Replication: deletes
# ---------------------------------------------------------

$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM public.events_2024_02 WHERE id = 2;
DELETE FROM tenant_b.orders WHERE order_id = 2;

> SELECT mz_source_table, id, payload FROM all_events WHERE mz_source_table = 'events_2024_02' ORDER BY id
mz_source_table id payload
---
events_2024_02 1 feb_A

> SELECT mz_source_schema, order_id, amount FROM all_orders WHERE mz_source_schema = 'tenant_b' ORDER BY order_id
mz_source_schema order_id amount
---
tenant_b 1 200.00
tenant_b 3 75.25

# ---------------------------------------------------------
# Materialized view and index on top of table group
# ---------------------------------------------------------

> CREATE MATERIALIZED VIEW event_counts AS
  SELECT mz_source_table, count(*) AS cnt FROM all_events GROUP BY 1;

> SELECT * FROM event_counts ORDER BY mz_source_table
mz_source_table cnt
---
events_2024_01 3
events_2024_02 1
events_2024_03 2

> CREATE INDEX all_events_idx ON all_events (mz_source_table, id);

> SELECT mz_source_table, id, payload FROM all_events WHERE mz_source_table = 'events_2024_03' ORDER BY id
mz_source_table id payload
---
events_2024_03 1 mar_A
events_2024_03 2 mar_B

# ---------------------------------------------------------
# IF NOT EXISTS: creating same table group should be idempotent
# ---------------------------------------------------------

> CREATE TABLE GROUP IF NOT EXISTS all_events
  FROM SOURCE mz_source
  (SCHEMAS ('public'), TABLE PATTERN '^events_2024');

# ---------------------------------------------------------
# DROP TABLE GROUP
# ---------------------------------------------------------

> DROP TABLE GROUP all_events CASCADE

# Verify the materialized view is gone (CASCADE)
! SELECT * FROM event_counts
contains:unknown catalog item

# The other table group should still work
> SELECT count(*) FROM all_orders
5

> DROP TABLE GROUP all_orders

# ---------------------------------------------------------
# A standalone table coexists with a table group on the same upstream table
# ---------------------------------------------------------

> CREATE TABLE GROUP events_group
  FROM SOURCE mz_source
  (SCHEMAS ('public'), TABLE PATTERN '^events_2024');

> CREATE TABLE standalone_events_01 FROM SOURCE mz_source (REFERENCE public.events_2024_01);

# Both should have data independently
> SELECT count(*) FROM events_group WHERE mz_source_table = 'events_2024_01'
3

> SELECT count(*) FROM standalone_events_01
3

# Insert and verify both get the new row
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO public.events_2024_01 VALUES (4, '2024-01-31 12:00:00+00', 'jan_D');

> SELECT count(*) FROM events_group WHERE mz_source_table = 'events_2024_01'
4

> SELECT count(*) FROM standalone_events_01
4

> DROP TABLE standalone_events_01
> DROP TABLE GROUP events_group CASCADE

# ---------------------------------------------------------
# Cleanup
# ---------------------------------------------------------

> DROP SOURCE mz_source CASCADE
> DROP CONNECTION pg CASCADE
> DROP SECRET pgpass CASCADE
