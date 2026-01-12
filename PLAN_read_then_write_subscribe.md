# Plan: Refactor `sequence_read_then_write` to Use Subscribe with Persist-Batches

## Overview

Change `sequence_read_then_write` from its current peek-then-write approach to use a subscribe with persist-batches output format. This enables optimistic concurrency control where we continuously listen to changes and retry writes until success.

**POC Scope:** Focus on DELETE first. UPDATE support (which requires emitting both old and new rows) can be added later.

## Current Architecture

**Current Flow (`sequence_read_then_write` in `inner.rs:2575-3036`):**
1. Acquire write locks for the table (pessimistic concurrency control)
2. Execute a peek with `QueryWhen::FreshestTableWrite`
3. In spawned async task:
   - Receive peek results
   - Apply `assignments` (for UPDATE) to compute new row values
   - Create diffs based on `MutationKind` (Insert/Update/Delete)
   - Linearize the read timestamp
   - Call `send_diffs` to sequence the write

**New Approach:** OCC (optimistic concurrency control) - no locks needed, retry on conflict.

**Key Data Structures:**
- `ReadThenWritePlan`: Contains `id`, `selection` (HirRelationExpr), `assignments`, `kind` (MutationKind), `returning`, `finishing`
- Peek produces rows, adapter computes diffs (retractions/additions)

## Proposed New Architecture

### Phase 1: Plan Changes (SQL Layer) - DELETE POC

**Goal:** For DELETE, the subscribe should directly produce rows with diff=-1.

For DELETE, the transformation is straightforward:
- The `selection` already identifies rows to delete
- We need the subscribe to emit these rows with `diff = -1`
- This can be done by negating the diff in MIR

**MIR Approach:**
- Wrap the selection in a `Negate` operator (or equivalent)
- The subscribe will then naturally produce `(row, time, -1)` tuples

**Future UPDATE Support:**
- For UPDATE, need to emit `(old_row, -1)` and `(new_row, +1)`
- Could use `FlatMap` or `Union` at MIR level to produce both
- Leave for later iteration

### Phase 2: Subscribe with Persist-Batches Output

**Goal:** Use `SubscribeOutputFormat::PersistBatches` to get batches that can be directly applied.

**Key Insight:** `TableData::Batches(SmallVec<[ProtoBatch; 1]>)` already exists in `storage-client/src/client.rs:544-551`. The existing `submit_timestamped_write` can accept batches directly.

1. **Configure Subscribe:**
   - Use `SubscribeOutputFormat::PersistBatches(SubscribePersistBatchesConfig { shard_id, persist_location, relation_desc })`
   - **Use the target table's shard directly** - no intermediate shard needed
   - Get shard info from catalog/storage controller for the target table
   - The `relation_desc` must match the target table's schema

2. **Response Handling:**
   - Receive `StashedSubscribeBatch` with `Vec<ProtoBatch>`
   - Batches are already in the target shard, ready for `compare_and_append`
   - Convert to `TableData::Batches` for the write path

### Phase 3: Optimistic Concurrency Control Loop

**Goal:** Implement retry loop that submits timestamped writes on frontier advances.

```rust
// Pseudocode for the async task
async fn run_read_then_write_subscribe(...) {
    let mut all_batches: Vec<ProtoBatch> = vec![];

    loop {
        match subscribe_response_rx.recv().await {
            SubscribeResponse::StashedBatch(batch) => {
                // Accumulate ALL batches from the beginning
                all_batches.extend(batch.batches?);

                // On frontier advance, try to write
                if let Some(upper_ts) = batch.upper.as_option() {
                    let write_ts = upper_ts.step_forward(); // Next timestamp

                    let table_data = TableData::Batches(all_batches.clone().into());
                    let writes = BTreeMap::from([(table_id, smallvec![table_data])]);

                    match submit_timestamped_write(write_ts, writes, ...).await {
                        TimestampedWriteResult::Success { .. } => {
                            // Done! Return success to user
                            return Ok(ExecuteResponse::Deleted(count));
                        }
                        TimestampedWriteResult::TimestampPassed { .. } => {
                            // Keep listening, retry on next frontier
                            continue;
                        }
                        TimestampedWriteResult::Cancelled => {
                            return Err(AdapterError::Canceled);
                        }
                    }
                }
            }
            SubscribeResponse::DroppedAt(_) => {
                return Err(AdapterError::Internal("subscribe dropped"));
            }
        }
    }
}
```

**Key Points:**
- On retry, use ALL accumulated batches (not just new ones)
- The batches represent the complete set of changes needed
- `submit_timestamped_write` already handles `TableData::Batches`

### Phase 4: Integration

1. **Internal Subscribe Setup:**
   - Create an internal subscribe (not user-facing)
   - Use `ActiveComputeSink::Subscribe` but with different response handling
   - Or create a new variant for internal use

2. **No Write Locks Needed:**
   - OCC handles conflicts via retry - no pessimistic locking required
   - Simplifies the flow significantly

3. **Channel/Message Flow:**
   - Subscribe responses come via `SubscribeResponse` from compute
   - Need to send timestamped write requests back to coordinator
   - The async task needs `internal_cmd_tx` to communicate

## Key Files to Modify

1. **`src/sql/src/plan/statement/dml.rs`** - Add MIR transformation for DELETE (negate diffs)
2. **`src/adapter/src/coord/sequencer/inner.rs`** - New `sequence_read_then_write` implementation
3. **`src/adapter/src/coord/sequencer/inner/subscribe.rs`** - May need internal subscribe variant
4. **`src/adapter/src/active_compute_sink.rs`** - Possibly new sink type for internal subscribes

## Implementation Steps

1. **Step 1:** Modify DELETE planning to produce negated diffs at MIR level
2. **Step 2:** Create internal subscribe setup that uses `PersistBatches` output format
3. **Step 3:** Implement the OCC loop in the async task
4. **Step 4:** Wire up the channels for timestamped write submission
5. **Step 5:** Handle cleanup (drop subscribe on success/failure)
6. **Step 6:** Add tests

## Concurrency Semantics

**Multiple OCC Writers**: When multiple read-then-write subscribes attempt to write at the same timestamp:

1. **Only ONE `InternalTimestamped` write is processed per group commit round**
2. If multiple writes target the same timestamp, one is selected (arbitrary but deterministic) and others receive `TimestampPassed`
3. The failing writes will retry with a new timestamp on the next frontier advance

This is necessary because:
- Each timestamped write may not have resolved dependencies with other concurrent timestamped writes
- They were computed independently and could be inconsistent if applied together
- After committing at timestamp T, the oracle advances past T, so other writes at T would fail anyway

The implementation ensures correctness by failing conflicting writes early rather than waiting for the timestamp oracle to reject them.

## Open Questions

1. **Row Count:** How do we count affected rows for the response? Need to track from batches or count during subscribe.

2. **RETURNING Clause:** Deferred for POC - need to read batch contents to compute return values.
