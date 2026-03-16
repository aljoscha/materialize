\set ON_ERROR_STOP on
\pset pager off

SET cluster = quickstart;

DROP TABLE IF EXISTS gc_probe;
DROP TABLE IF EXISTS gc_results;

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

\echo '=== INSERT latency at 20k tables ==='

DELETE FROM gc_probe;
SELECT 'INSERT INTO gc_probe VALUES (' || i || ', now())'
FROM generate_series(1, 200) AS i;
\gexec

INSERT INTO gc_results
SELECT
  'insert_20k',
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

\echo '=== Empty transactions at 20k tables ==='

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

\echo '=== SELECT 1 at 20k tables ==='

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
ORDER BY stage;
