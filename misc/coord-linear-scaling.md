# Coordinator Per-Query Latency Scales Linearly with Catalog Size

**Issue:** database-issues#11251

## Problem Statement

After creating N tables, per-query latency (SELECT 1, INSERT, COMMIT, DDL) grows
linearly with N. At 20k tables, ~25ms per query vs ~3ms at 1k tables.

This is a coordinator-thread bottleneck: something on the per-query hot path
iterates all catalog objects, so cost grows with O(N).

## Architecture Background

The coordinator runs a single-threaded async event loop (`Coordinator::serve`).
All SQL sequencing happens on this thread. The main select-loop receives messages
from various sources (client commands, controller events, group commit
notifications, etc.) and processes them sequentially.

Key per-query flows:
- **SELECT 1**: `Command → sequence_plan → sequence_peek → [linearize_ts →
  timestamp_read_hold → optimize → finish]`
- **INSERT (constant)**: `Command → sequence_plan → sequence_insert →
  insert_constant (off-thread) → group_commit → advance_timelines`
- **BEGIN/COMMIT**: `Command → sequence_plan → sequence_end_transaction →
  group_commit → advance_timelines`

After every group commit (i.e., after every write), the coordinator:
1. Runs `group_commit()` on the coord thread — includes O(N) table advancement
2. Spawns a task that does the persist write and applies the write at the
   timestamp oracle
3. That task sends `Message::AdvanceTimelines` back to the coord loop
4. `advance_timelines` iterates all global timelines and downgrades read holds

## Identified O(N) Bottlenecks

### B1: Table advancement in group_commit (ON COORD THREAD) — HIGH impact

**Location:** `src/adapter/src/coord/appends.rs:515-517`

```rust
// Add table advancements for all tables.
for table in self.catalog().entries().filter(|entry| entry.is_table()) {
    appends.entry(table.id()).or_default();
}
```

On EVERY group commit, the coordinator iterates ALL catalog entries, filters for
tables, and adds an empty entry to the appends map for each one. This ensures all
tables get advanced to the new timestamp even if nothing was written to them.

With 20k tables, this creates 20k entries, which are then:
- Consolidated (20k iterations)
- Mapped from CatalogItemId → GlobalId (20k catalog lookups)
- Sent to the persist table worker (which iterates all 20k entries)

This runs on the coordinator thread and blocks all other messages.

Group commits happen:
- On every INSERT (user writes trigger group commit)
- On every DDL (catalog transactions trigger group commit)
- Every 1 second via `advance_timelines_interval`

So even `SELECT 1` is affected: a concurrent group commit (from another client
or from the periodic timer) will block the coord thread for O(N) time, delaying
the SELECT response.

### B2: `advance_timelines` → `ReadHolds::downgrade` — MEDIUM impact

**Location:** `src/adapter/src/coord/timeline.rs:299`

After every group commit, `advance_timelines` is called. For each global
timeline, it calls `read_holds.downgrade(read_ts)`. The `downgrade` method
iterates every storage and compute hold (`src/adapter/src/coord/read_policy.rs:87`)
and calls `try_downgrade()` on each. Each `try_downgrade` creates a ChangeBatch
and sends it through a channel.

With 20k tables, that's 20k+ iterations. However, for `Timeline::EpochMilliseconds`
(which all user tables belong to), the `ids_in_timeline` call and `least_valid_write`
computation are skipped — only the `read_holds.downgrade` runs.

### B3: `ids_in_timeline` iterates all catalog entries — LOW impact (for EpochMs)

**Location:** `src/adapter/src/catalog/timeline.rs:70`

Iterates ALL catalog entries to build a `CollectionIdBundle` for a given timeline.
Only called for non-EpochMilliseconds timelines in `advance_timelines`, so unlikely
to be a factor for normal tables. Could matter if there are many objects in
non-standard timelines.

### B4: Persist table worker processes all N tables — OFF-THREAD but adds latency

**Location:** `src/storage-controller/src/persist_handles.rs:396`

The `append` function in the persist table worker iterates all `(GlobalId, updates)`
entries. For the 20k empty table entries from B1, it still loops through each,
calls `self.write_handles.get(&id)` and checks `updates.iter().all(|u| u.is_empty())`.
Since this runs off-thread, it doesn't directly block the coordinator, but it adds
to the end-to-end latency of writes.

## Proposed Fixes

### Fix for B1: Don't advance tables with no writes in group_commit

The simplest fix: instead of adding all tables to the appends map, only advance
tables that actually have pending writes. Table advancement can be done separately
or lazily.

**Risk:** Tables that never get written to will never advance their upper, which
could prevent compaction. Need to understand if table advancement is required for
correctness or just an optimization. The `advance_timelines_interval` already
handles timeline advancement, so table-specific advancement in group_commit may
be redundant.

### Fix for B2: Batch read hold downgrades

Instead of individually downgrading each hold, batch the downgrade into a single
operation that sends one ChangeBatch covering all holds.

### Fix for B4: Filter empty appends before sending to persist worker

Skip tables with no actual writes when constructing the appends vector.

## Findings

(Not yet started — investigation not started)
