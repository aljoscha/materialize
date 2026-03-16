\set ON_ERROR_STOP on
\pset pager off

SET cluster = quickstart;
ALTER SYSTEM SET max_tables = 50000;
ALTER SYSTEM SET max_objects_per_schema = 100000;
DROP TABLE IF EXISTS gc_target;
DROP TABLE IF EXISTS gc_probe;
DROP TABLE IF EXISTS gc_results;

SELECT 'DROP TABLE IF EXISTS gc_bystander_' || i
FROM generate_series(1, 20000) AS i;
\gexec

CREATE TABLE gc_target (v INT);
CREATE TABLE gc_probe (id INT, ts TIMESTAMPTZ);
CREATE TABLE gc_results (
  stage TEXT,
  total_tables INT,
  probe_count INT,
  avg_ms NUMERIC(10,2),
  min_ms NUMERIC(10,2),
  max_ms NUMERIC(10,2),
  spikes_gt_20ms INT,
  spikes_gt_50ms INT
);

\echo '=== BASELINE ==='

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'baseline',
  0,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== Creating bystander tables 1-1000 ==='
\timing on

SELECT 'CREATE TABLE gc_bystander_' || i || ' (x INT)'
FROM generate_series(1, 1000) AS i;
\gexec

\timing off

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'after_1000',
  1000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== Creating bystander tables 1001-3000 ==='
\timing on

SELECT 'CREATE TABLE gc_bystander_' || i || ' (x INT)'
FROM generate_series(1001, 3000) AS i;
\gexec

\timing off

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'after_3000',
  3000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== Creating bystander tables 3001-5000 ==='
\timing on

SELECT 'CREATE TABLE gc_bystander_' || i || ' (x INT)'
FROM generate_series(3001, 5000) AS i;
\gexec

\timing off

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'after_5000',
  5000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== Creating bystander tables 5001-10000 ==='
\timing on

SELECT 'CREATE TABLE gc_bystander_' || i || ' (x INT)'
FROM generate_series(5001, 10000) AS i;
\gexec

\timing off

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'after_10000',
  10000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== Creating bystander tables 10001-20000 ==='
\timing on

SELECT 'CREATE TABLE gc_bystander_' || i || ' (x INT)'
FROM generate_series(10001, 20000) AS i;
\gexec

\timing off

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'after_20000',
  20000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo ''
\echo '=== CONTROL: empty transactions at 20000 tables ==='

DELETE FROM gc_probe;
SELECT unnest(ARRAY[
  'INSERT INTO gc_probe VALUES (' || i || ', now())',
  'BEGIN',
  'COMMIT'
])
FROM generate_series(1, 50) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'empty_txn_20k',
  20000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

\echo '=== CONTROL: SELECT 1 at 20000 tables ==='

DELETE FROM gc_probe;
SELECT unnest(ARRAY[
  'INSERT INTO gc_probe VALUES (' || i || ', now())',
  'SELECT 1'
])
FROM generate_series(1, 100) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'select1_20k',
  20000,
  count(*)::int,
  round(avg(gap_ms)::numeric, 2),
  round(min(gap_ms)::numeric, 2),
  round(max(gap_ms)::numeric, 2),
  count(*) FILTER (WHERE gap_ms > 20)::int,
  count(*) FILTER (WHERE gap_ms > 50)::int
FROM (
  SELECT extract(epoch from lead(ts) OVER (ORDER BY id) - ts) * 1000 AS gap_ms
  FROM gc_probe
) sub WHERE gap_ms IS NOT NULL;

SELECT stage, total_tables, probe_count, avg_ms, min_ms, max_ms, spikes_gt_20ms, spikes_gt_50ms
FROM gc_results
ORDER BY total_tables, stage;
