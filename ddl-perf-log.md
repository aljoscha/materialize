# DDL Performance Optimization Log

## Current Setup

Problem: individual DDL statements (CREATE TABLE, CREATE VIEW, CREATE INDEX,
DROP, ALTER) become slow when thousands of objects already exist. A statement
that takes milliseconds on a fresh environment can take multiple seconds at
scale. We measure per-statement latency at various object counts.

```bash
# Debug build (faster to compile, makes O(n) scaling more visible)
bin/environmentd --build-only
bin/environmentd --reset -- --system-parameter-default=log_filter=info

# Optimized build (for confirming fixes against realistic absolute numbers)
bin/environmentd --optimized --build-only
bin/environmentd --reset --optimized -- --system-parameter-default=log_filter=info

# Connect as materialize user (external port)
psql -U materialize -h localhost -p 6875 materialize

# Connect as mz_system (internal port — needed for ALTER SYSTEM SET, etc.)
psql -U mz_system -h localhost -p 6877 materialize
```

**Important:** After `--reset`, you must raise object limits before creating
large numbers of objects:
```bash
psql -U mz_system -h localhost -p 6877 materialize -c "
  ALTER SYSTEM SET max_tables = 100000;
  ALTER SYSTEM SET max_materialized_views = 100000;
  ALTER SYSTEM SET max_objects_per_schema = 100000;
"
```

To bulk-create objects, write a SQL file and use `psql -f` (much faster than
one psql invocation per statement):
```bash
for i in $(seq 1 1000); do
  echo "CREATE TABLE t_$i (a int, b text);"
done > /tmp/bulk_create.sql
psql -U materialize -h localhost -p 6875 materialize -f /tmp/bulk_create.sql
```

## Profiling Setup

Use `perf record` with frame pointers:

```bash
# Record (after creating objects — run DDL batch during perf recording)
perf record -F 9000 --call-graph fp -p <environmentd-pid> -- sleep <duration>
# (start DDL batch shortly after perf starts)

# Process
perf script | inferno-collapse-perf | rustfilt > /tmp/collapsed.txt

# View as flamegraph
cat /tmp/collapsed.txt | inferno-flamegraph > /tmp/flame.svg

# Or analyze with grep
grep -E 'some_function' /tmp/collapsed.txt | awk '{sum += $NF} END {print sum}'
```

Key profiling notes:
- Use `--call-graph fp` (frame pointers) — the `--profile optimized` build has frame pointers enabled
- 9000 Hz sampling rate gives good resolution without excessive overhead
- Coordinator is single-threaded — DDL goes through the coordinator, so look for
  per-statement CPU time there
- Look for O(n) or O(n^2) patterns: functions whose per-call time grows with object count

## Optimization Log

### Session 1: Baseline measurements (2026-02-18)

**Goal:** Establish baseline DDL latency at increasing object counts.

**Setup:** Debug build, `bin/environmentd` (no `--optimized`). Created tables
with default indexes using `psql -f` batch files. Each table is
`CREATE TABLE bt_N (a int, b text)` which also creates a default index, so
each table contributes ~2 user objects.

**Measurement method:** 5 individual `psql -c` invocations per DDL type at each
scale, measuring wall-clock time with `date +%s%N`. Each sample is a fresh psql
connection → execute DDL → close.

**Results (median of 5 samples, debug build):**

| User Objects | CREATE TABLE | CREATE VIEW | CREATE INDEX | DROP TABLE |
|-------------|-------------|-------------|-------------|------------|
| 0           | 225 ms      | 182 ms      | 218 ms      | 176 ms     |
| ~116        | 250 ms      | 207 ms      | 229 ms      | 193 ms     |
| ~531        | 337 ms      | 287 ms      | 299 ms      | 281 ms     |
| ~1046       | 426 ms      | 374 ms      | 414 ms      | 362 ms     |
| ~2061       | 700 ms      | 630 ms      | 700 ms      | 610 ms     |
| ~3076       | 1034 ms     | 966 ms      | 1006 ms     | 927 ms     |

**Key observations:**
- All DDL types show roughly linear O(n) scaling: ~250-270ms additional latency
  per 1000 user objects.
- All DDL types scale at approximately the same rate, suggesting a shared
  bottleneck in the DDL path rather than type-specific overhead.
- Even the baseline (~225ms for CREATE TABLE) is substantial for a debug build.
  Some of this is likely fixed overhead (persist, cockroach writes, etc).
- At 3000 tables (~6000 user objects counting indexes), a single CREATE TABLE
  takes ~1 second in a debug build.
- The linear scaling across all DDL types points to something in the common DDL
  path that iterates over all existing objects — likely in catalog operations,
  dependency tracking, or coordinator bookkeeping.

**Raw data:** See /tmp/ddl-bench-results.txt for full 5-sample measurements.

**Next step:** Profile the slow path with `perf record` at ~3000 tables to
identify which functions dominate the per-statement cost and where the O(n)
scaling lives.
