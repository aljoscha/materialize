We're investigating database-issues#11251: coordinator per-query latency
scales linearly with the number of catalog objects (tables). At 20k tables,
even `SELECT 1` takes ~25ms (vs ~3ms at 1k tables). INSERT/COMMIT/DDL are
similarly affected.

Design doc: @misc/coord-linear-scaling.md
Session log: @coord-scaling-log.md

## Workflow

* Each solid progress (baseline measurement, diagnosis finding, optimization)
  gets committed as a git commit with a good description, then continue to the
  next step.
* Before committing, verify that what you produced is high quality and works.
* Code should be simple and clean, well-commented explaining what/how/why.
* Minimal changes — if we iterate and try multiple things, clean up to the
  minimum required fix at the end.
* **Read this file again after each context compaction.**
* Update @coord-scaling-log.md after each milestone with a new session entry.
* Update this file's "Current status" and "Immediate next steps" after each
  milestone.
* Update @misc/coord-linear-scaling.md if experimental findings contradict
  or refine it.

## Steps

1. Reproduce/measure the problem (establish baselines at various table counts)
2. Profile and diagnose where time is spent on the coordinator thread per query
3. Fix the bottleneck(s) (following design doc proposals or better)
4. Re-measure to confirm improvement
5. Repeat — there may be multiple layers of bottlenecks

## Setup

```bash
# Build — use --optimized for measurements. Debug builds have different
# hot-path characteristics. Do NOT use --release (much slower to compile);
# --optimized is sufficient.
bin/environmentd --optimized

# Connect
psql -U materialize -h localhost -p 6875 materialize
psql -U mz_system -h localhost -p 6877 materialize  # for ALTER SYSTEM SET

# Prometheus metrics
curl -s http://localhost:6878/metrics > /tmp/metrics.txt
```

The reproducer script is at @misc/coord-scaling-repro.sql. Run it with:
```bash
psql -U mz_system -h localhost -p 6877 materialize -f misc/coord-scaling-repro.sql
```

## Key Metrics

| Metric | What it measures |
|--------|-----------------|
| `mz_message_handling{kind="..."}` | Wall-clock time per message type on the coord thread |
| `mz_message_batch` | Number of messages batched per select-loop iteration |
| `mz_catalog_transact_seconds` | Catalog transaction time |

## Key Code Paths

| Component | Location |
|-----------|----------|
| Coordinator main loop | `src/adapter/src/coord.rs:3506` (fn serve) |
| Message handling | `src/adapter/src/coord/message_handler.rs:53` |
| Group commit | `src/adapter/src/coord/appends.rs:328` |
| advance_timelines | `src/adapter/src/coord/timeline.rs:299` |
| ids_in_timeline (iterates ALL entries) | `src/adapter/src/catalog/timeline.rs:70` |
| ReadHolds::downgrade (iterates all holds) | `src/adapter/src/coord/read_policy.rs:87` |
| least_valid_write / storage_frontiers | `src/adapter/src/coord/timestamp_selection.rs:544` |
| timedomain_for (iterates schema items) | `src/adapter/src/coord/timeline.rs:363` |
| initialize_read_policies (adds per-obj hold) | `src/adapter/src/coord/read_policy.rs:273` |
| sequence_peek (SELECT path) | `src/adapter/src/coord/sequencer/inner/peek.rs:122` |
| sequence_insert (INSERT path) | `src/adapter/src/coord/sequencer/inner.rs:2476` |
| sequence_peek_timestamp | `src/adapter/src/coord/sequencer/inner/peek.rs:1065` |
| controller.process() | `src/controller/src/lib.rs:520` |
| PlanValidity::check | `src/adapter/src/coord/validity.rs:78` |

## Hypothesized Bottlenecks (unverified — need profiling to confirm)

### B1 (HIGH): Table advancement in group_commit — ON COORD THREAD
   `src/adapter/src/coord/appends.rs:515-517`
   Every group commit iterates ALL catalog entries, filters for tables, and
   adds empty advancement entries. With 20k tables → 20k map entries that
   are then consolidated, mapped to GlobalIds, and sent to persist.
   Group commits happen on every INSERT, every DDL, and every ~1s (timer).
   This is why even SELECT 1 gets slower: a concurrent/recent group commit
   blocks the coord thread for O(N) time.

### B2 (MEDIUM): advance_timelines → ReadHolds::downgrade
   `src/adapter/src/coord/timeline.rs:299`, `read_policy.rs:87`
   After every group commit, iterates all read holds (one per table/source/
   MV/index in each timeline) calling try_downgrade → ChangeBatch → channel
   send for each. 20k tables = 20k iterations.

### B3 (LOW): ids_in_timeline iterates all catalog entries
   `src/adapter/src/catalog/timeline.rs:70`
   Only for non-EpochMilliseconds timelines. Probably not a factor.

### B4 (OFF-THREAD): Persist table worker processes all N empty entries
   `src/storage-controller/src/persist_handles.rs:396`
   Not on coord thread but adds end-to-end latency.

## Current Status

B1 fixed: removed O(N) table advancement loop from group_commit.
`group_commit_initiate` reduced from 19.2ms to 5.3ms at 20k tables.

B2 investigated: `advance_timelines` is 6ms/call, but **the cost is NOT
ReadHolds::downgrade** — it's `oracle.read_ts().await` (~6-7ms). Disabling
the downgrade entirely still shows 6.9ms. The per-hold downgrade adds <1ms.

New bottleneck identified: `controller_ready(storage)` = **29ms/call** at
20k tables. This is the actual dominant O(N) bottleneck on the coord thread.

## Immediate Next Steps

1. Investigate the ~30ms `controller_ready(storage)` overhead. Disabling
   ALL of `maintain()` still shows 30ms, so the cost is from the storage
   controller's `ready()`/`process()` loop itself — possibly tracing
   overhead, instance response processing, or async task coordination.
   This needs profiling (samply/perf) to identify the specific cost.
2. Consider the remaining latency floor: oracle.read_ts() (~6ms) +
   group_commit coord work (~5ms) + persist write (~5ms) = ~16ms
   minimum. The B1 fix brought the average from ~35ms toward this floor.
   Further improvements may require architectural changes (e.g., async
   group commit, cached oracle reads).
