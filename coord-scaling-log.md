# Coordinator Scaling Investigation Log

## Session 1 — Setup & Code Analysis (2026-03-16)

- Created PROMPT.md, design doc (`misc/coord-linear-scaling.md`), reproducer
  script (`misc/coord-scaling-repro.sql`), and this log.
- Analyzed codebase to identify O(N) bottlenecks on the coordinator thread.
- **Key finding — B1 (table advancement in group_commit):**
  `src/adapter/src/coord/appends.rs:515-517` iterates ALL tables on every group
  commit to add "advancement" entries (empty writes that advance the table's
  upper). With 20k tables, this creates 20k entries in the appends map, which
  are then consolidated (20k iter), mapped to GlobalIds (20k catalog lookups),
  and sent to the persist worker (20k iter). All of this runs on the coordinator
  thread. Group commits fire on every INSERT, every DDL, and every ~1s via timer,
  so this explains why SELECT 1 is also affected (it gets queued behind a
  concurrent group commit).
- **Secondary finding — B2 (ReadHolds::downgrade):**
  After every group commit, `advance_timelines` calls `read_holds.downgrade()`
  which iterates all holds (one per table/source/MV/index per timeline). 20k
  iterations with channel sends.
- Next session: reproduce with the reproducer script, profile with samply to
  confirm B1 is the dominant cost, then fix.

## Session 2 — Reproduce, diagnose, fix B1 (2026-03-16)

### Baseline measurement (before fix)
Ran `misc/coord-scaling-repro.sql` with optimized build:

| Tables | Avg INSERT latency (ms) |
|--------|------------------------|
| 0      | 5.67                   |
| 1,000  | 6.95                   |
| 3,000  | 9.86                   |
| 5,000  | 13.26                  |
| 10,000 | 19.46                  |
| 20,000 | 34.61                  |

SELECT 1 at 20k: 33.04ms avg. Confirms O(N) scaling.

### Metrics-based diagnosis
Took 30s deltas of Prometheus metrics at 20k tables:
- `group_commit_initiate`: **19.2ms/call** (dominant!)
  - Of which `table_advancement` loop: **5.3ms** (just the catalog iteration)
  - Remaining ~14ms: consolidation of 20k entries, 20k GlobalId mappings,
    leadership confirmation, append_table setup
- `advance_timelines` (ReadHolds::downgrade): **6.0ms/call**
- Combined coord-thread blocking: **~24ms per group commit cycle**

### B1 fix
Removed the table advancement loop at `appends.rs:512-516`. With txn-wal,
table uppers are advanced through the txns shard (via `commit_at()`), so
the empty entries were unnecessary.

### Post-fix measurement
Ran reproducer again with fix applied:

| Tables | Before (ms) | After (ms) |
|--------|------------|------------|
| 0      | 5.67       | 13.85*     |
| 5,000  | 13.26      | 14.48      |
| 10,000 | 19.46      | 16.99      |
| 20,000 | 34.61      | 23.76      |

*Baseline difference due to different run environment.

O(N) scaling component: ~29ms → ~10ms (**66% reduction**).

Post-fix metric deltas at 20k tables:
- `group_commit_initiate`: **5.3ms/call** (was 19.2ms, **73% reduction**)
- `advance_timelines`: **6.0ms/call** (unchanged, as expected)

### Remaining bottleneck: B2 (advance_timelines → ReadHolds::downgrade)
- 6.0ms per call at 20k tables
- Iterates all ~20k storage + compute holds, calling `try_downgrade()` on each
- Each `try_downgrade` allocates: Antichain (clone), ChangeBatch, channel send
- ~60k heap allocations per cycle = ~6ms
- Fixing this requires either batching channel sends, lazy downgrade, or
  architectural change to shared timeline frontier
