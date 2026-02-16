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

To bulk-create objects, use a bash loop or script, e.g.:
```bash
for i in $(seq 1 1000); do
  psql -U materialize -h localhost -p 6875 materialize -c "CREATE TABLE t_$i (a int);"
done
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
