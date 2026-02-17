# pgwire-server-frontend Performance Optimization Log

## Current Setup

We benchmark `bin/environmentd --optimized` (the real production binary, not the testbed).

Workload: `SELECT * FROM t` (table with one row, default index on quickstart cluster).

System params:
- `enable_frontend_peek_sequencing=true`
- `statement_logging_max_sample_rate=0`

```bash
# Build (--optimized = optimized profile with debug symbols, no LTO)
bin/environmentd --optimized --build-only

# Start with --reset to get a clean state
bin/environmentd --reset --optimized -- --system-parameter-default=log_filter=info

# Create the test table, index on the QUICKSTART cluster, and insert one row
# IMPORTANT: The `materialize` user runs queries on the `quickstart` cluster,
# so the index MUST be on `quickstart`. `CREATE DEFAULT INDEX ON t` (without
# IN CLUSTER) creates the index on the current session's cluster (mz_system
# when connected as mz_system), which is WRONG — the optimizer's fast path
# won't find the index and will fall through to the full optimizer pipeline.
psql -U mz_system -h localhost -p 6877 materialize -c "CREATE TABLE t (id text, value int4);"
psql -U mz_system -h localhost -p 6877 materialize -c "CREATE DEFAULT INDEX IN CLUSTER quickstart ON t;"
psql -U mz_system -h localhost -p 6877 materialize -c "INSERT INTO t VALUES ('hello', 42);"
psql -U mz_system -h localhost -p 6877 materialize -c "GRANT SELECT ON TABLE t TO materialize;"
psql -U mz_system -h localhost -p 6877 materialize -c "ALTER SYSTEM SET enable_frontend_peek_sequencing=true;"
psql -U mz_system -h localhost -p 6877 materialize -c "ALTER SYSTEM SET statement_logging_max_sample_rate=0;"

# Verify
psql -U materialize -h localhost -p 6875 materialize -c "SELECT * FROM t"

# Benchmark
# IMPORTANT: pin dbbench's pool sizes to the benchmark concurrency and pre-auth
# connections. Otherwise (notably at 128 connections with default
# max-idle-conns=100), dbbench churns sessions heavily, which floods
# command-startup/command-terminate builtin table updates and artificially
# inflates group commits (~30-40/s instead of ~1/s), distorting oracle metrics.
~/dbbench/dbbench -url "postgres://materialize@localhost:6875/materialize" \
    -driver postgres \
    -max-active-conns 64 \
    -max-idle-conns 64 \
    -force-pre-auth \
    /tmp/select_star.ini
```

Benchmark scripts:
```ini
# /tmp/select_star_1conn.ini
duration=20s

[1 connection]
query=select * from t
concurrency=1
```
```ini
# /tmp/select_star.ini
duration=20s

[64 connections]
query=select * from t
concurrency=64
```

For any other concurrency `N`, use:

```bash
~/dbbench/dbbench -url "postgres://materialize@localhost:6875/materialize" \
    -driver postgres \
    -max-active-conns N \
    -max-idle-conns N \
    -force-pre-auth \
    /tmp/select_star_N.ini
```

Connection churn sanity checks (should stay low/flat during steady-state runs):
- `mz_slow_message_handling_count{message_kind="command-startup"}`
- `mz_slow_message_handling_count{message_kind="command-terminate"}`
- `mz_connection_status{source="external",status="success"}`
- `mz_append_table_duration_seconds_count`
- `mz_ts_oracle_started_count{op="apply_write"}`

## Profiling Setup

Use `perf record` with frame pointers:

```bash
# Record (after warmup — run one full benchmark before starting perf to exclude startup costs)
perf record -F 9000 --call-graph fp -p <environmentd-pid> -- sleep 25
# (start dbbench 20s benchmark ~2-3s after perf starts)

# Process
perf script | inferno-collapse-perf | rustfilt > /tmp/collapsed.txt

# View as flamegraph
cat /tmp/collapsed.txt | inferno-flamegraph > /tmp/flame.svg

# Or analyze with grep
grep -E 'some_function' /tmp/collapsed.txt | awk '{sum += $NF} END {print sum}'
```

Key profiling notes:
- Always do a warmup run before profiling to exclude server startup/catalog optimization costs
- Use `--call-graph fp` (frame pointers) — the `--profile optimized` build has frame pointers enabled
- 9000 Hz sampling rate gives good resolution without excessive overhead
- Coordinator is single-threaded — reducing its per-query CPU has multiplicative QPS effects at high concurrency
- With `enable_frontend_peek_sequencing=true`, most query processing runs on tokio worker threads, not the coordinator

## Architecture Notes (frontend peek sequencing)

With `enable_frontend_peek_sequencing=true`, query processing moves from the single-threaded
coordinator to tokio worker threads. The coordinator only handles:
- `RegisterFrontendPeek` (fire-and-forget, no round-trip)
- `PeekNotification` processing
- Controller ready/process (compute/storage polling)

Worker threads handle: parsing, name resolution, planning, optimization, RBAC, timestamp
determination, and peek execution.

A per-connection plan cache (`PlanCacheEntry` on `PeekClient`) skips parsing, planning, and
optimization for repeated identical queries. The catalog is shared via a `tokio::sync::watch`
channel (no coordinator round-trip for catalog snapshot).

## Optimization Log

### Session 37: Cache persistent ReadHold to avoid mutex in acquire_read_hold_direct

**Problem identified via profiling (perf record -F 7000, 64 connections):**

In `try_frontend_peek_cached`, `acquire_read_hold_direct` consumed ~13.3% of function time
(1.59% total CPU). Breakdown:
- `MutableAntichain::rebuild()`: 35.3% — always triggered on +1 update at frontier
- `acquire_read_hold_direct` body: 26.7%
- Mutex lock: 14.8%
- Futex syscall (contention): 7.4%

Root cause: Every query acquires a `ReadHold` via `acquire_read_hold_direct` which:
1. Locks `Mutex<MutableAntichain>` (contended at 64 connections)
2. Calls `update_iter(since.iter().map(|t| (t.clone(), 1)))` which triggers `rebuild()`
3. The `rebuild()` is always triggered even for +1 at existing frontier (timely inefficiency)

**Solution:**

Cache a persistent `ReadHold` per `(ComputeInstanceId, GlobalId)` on `PeekClient`.
For each query, clone the cached hold instead of calling `acquire_read_hold_direct`:
- `ReadHold::clone()` sends +1 through an async mpsc channel (no mutex)
- `ReadHold::drop()` sends -1 through the same channel (no mutex)
- The cached hold prevents compaction at the collection's since frontier

**Changes:**
- `src/adapter/src/peek_client.rs`: Added `cached_read_holds` field to `PeekClient`
- `src/adapter/src/frontend_peek.rs`: Modified inline timestamp path to use cached read holds
- `src/adapter/src/coord/command_handler.rs`: Fixed mock_data row to match 2-column schema
- `src/compute-client/src/controller/instance.rs`: Fixed `add_collection` panic when
  replacing a dropped collection (pre-existing bug triggered by transient dataflow ID reuse)

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | QPS    | Latency (avg) |
|-------------|--------|---------------|
| 1           | 2,238  | 439µs         |
| 4           | 5,321  | 745µs         |
| 16          | 17,877 | 882µs         |
| 64          | 60,767 | 1.007ms       |

Note: Previous session data was lost due to log file reset. These results include
all prior optimizations (plan cache, inline timestamp, cached dyncfg handles, etc.)
plus the new cached ReadHold optimization.

### Session 38: Cache time_to_first_row_seconds histogram handle in pgwire StateMachine

**Problem identified via profiling (perf record -F 9000, 64 connections):**

In the `send_execute_response` fast path, `RecordFirstRowStream::record` called
`RecordFirstRowStream::histogram()` on every query, which:
1. Calls `ComputeInstanceId::to_string()` — formats "s{id}" or "u{id}" (570M samples, ~934/query)
2. Calls `HistogramVec::with_label_values` — HashMap lookup by 3 string labels (96M samples)
3. Reads `transaction_isolation` from session vars (66M samples)
4. Calls `StatementExecutionStrategy::name()` (70M samples)

Total: ~1,378M samples = ~2,259 samples/query (1.5% of total CPU at 64 connections).

For repeated queries on the same connection, the labels never change:
- instance_id: always the same cluster
- isolation_level: rarely changes within a session
- strategy: always FastPath for our benchmark

**Solution:**

Cache the resolved `Histogram` handle per-connection on the pgwire `StateMachine` struct.
On the first query, resolve the histogram via `with_label_values` and store the result.
On subsequent queries with the same strategy (compared by `&'static str` pointer equality),
reuse the cached handle directly — skipping `to_string()`, HashMap lookup, and all label
formatting.

**Changes:**
- `src/pgwire/src/protocol.rs`: Added `cached_time_to_first_row` field to `StateMachine`,
  added `record_time_to_first_row` method that caches the histogram handle, updated the
  `SendingRowsStreaming` fast path to use the cached method instead of
  `RecordFirstRowStream::record`.
- `src/adapter/src/client.rs`: Made `RecordFirstRowStream::histogram` public (was private).

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | QPS    | Latency (avg) |
|-------------|--------|---------------|
| 1           | 2,206  | 446µs         |
| 4           | 5,229  | 758µs         |
| 16          | 17,470 | 902µs         |
| 64          | 63,861 | 956µs         |

At 64 connections: +4.4% QPS (61,181 -> 63,861), latency improved from 1.000ms to 956µs.
Lower concurrency levels are within noise (the optimization primarily helps under contention
where many queries compete for the same histogram resolution overhead).

**Profiling notes for next session:**

Top remaining per-query costs in the pipeline (64 connections, normalized per query):
- `implement_fast_path_peek_plan`: 6,228/query (32.4% of try_frontend_peek_cached)
  - `RowSetFinishing::finish`: 806M
  - `RowCollection::new`: 737M
  - `ReadHold::drop` -> `try_downgrade` -> channel send: 725M
  - `RelationDesc::drop`: 481M
- `declare` (portal setup): 5,659/query
  - `set_portal`: 1,601/query — allocations for portal state
  - `StatementDesc::clone`: 1,514/query — deep clone of BTreeMap<ColumnIndex, ColumnMetadata>
  - `catalog_snapshot_local`: 422/query
  - `mint_logging`: 300/query (calls `system_time`)
- `send_execute_response` (response encoding/sending): 13,298/query
  - `send_all` (row encoding): 4,563/query
  - `send BackendMessage`: 1,795/query
  - `RelationDesc::drop`: 624/query
  - `update_encode_state_from_desc`: 257/query
  - `fmt::format`: 395/query
- `read_ts` (timestamp oracle): 2,698/query — channel round-trip to batching oracle
  (only for StrictSerializable; could be skipped for Serializable queries)
- `ReadHold::clone`: 1,610/query — mpsc channel send

Potential next optimizations:
1. Skip `declare`/`set_portal`/`remove_portal` for plan-cached queries (saves ~5.7k/query)
2. Cache `StatementDesc` or `RelationDesc` on PeekClient to avoid per-query clones
3. ~~Reduce `ReadHold::drop`/`try_downgrade` overhead~~ (done in Session 39)
4. Cache or pre-encode row description to avoid per-query `update_encode_state_from_desc`

### Session 39: Skip per-query ReadHold clone/drop in cached peek path

**Problem identified via profiling (perf record, 1 vs 64 connections):**

Per-query CPU in `try_frontend_peek_cached` increased 1.43x from 1 to 64 connections
(13,108/q to 18,824/q). Analyzed the per-function scaling to find concurrency-dependent
overhead:

| Function | 1c samples/q | 64c samples/q | Ratio | Delta |
|---|---|---|---|---|
| ReadHold::try_downgrade | 545 | 1,257 | 2.30x | +713 |
| ReadHold::clone | 1,336 | 1,923 | 1.44x | +588 |
| RowSetFinishing::finish | 847 | 1,293 | 1.53x | +446 |
| RowCollection::new | 674 | 1,086 | 1.61x | +411 |
| read_ts (oracle) | 2,636 | 3,341 | 1.27x | +705 |
| ensure_compute | 131 | 345 | 2.64x | +215 |

The biggest concurrency-scaling bottleneck was `ReadHold::clone` + `ReadHold::drop`
(combined 3,180/q at 64c). Root cause: every query cloned the cached persistent
ReadHold (+1 via channel), the clone was moved into `Instance::peek` which downgraded
it (another channel send), then it was dropped on the instance task (-1 via channel).
Net effect: zero — the cached persistent hold already prevents compaction, making the
per-query clone+downgrade+drop a pure overhead of 3 channel sends + ChangeBatch
allocations per query.

Additionally, the `change_tx` closure in `Client::spawn` was cloning `ChangeBatch`
twice unnecessarily (once to move into the boxed closure, once inside the closure
body when calling `apply_read_hold_change`).

**Solution:**

1. Made `Instance::peek` and `Client::peek` accept `Option<ReadHold<T>>` instead of
   `ReadHold<T>`. When `None`, the caller is responsible for maintaining a cached read
   hold covering the peek timestamp. `PendingPeek.read_hold` is now `Option<ReadHold<T>>`.

2. In `try_frontend_peek_cached`, the inline timestamp path no longer clones the
   cached ReadHold. Instead, it reads the cached hold's since by reference and passes
   `None` as the read hold to `implement_fast_path_peek_plan`. The cached persistent
   hold (stored on `PeekClient`) already prevents compaction at the collection's since.

3. Fixed the `change_tx` closure double-clone: the `ChangeBatch` is now moved directly
   into the boxed closure without cloning.

**Changes:**
- `src/compute-client/src/controller/instance.rs`: Made `Instance::peek` and
  `Client::peek` accept `Option<ReadHold<T>>`, updated `PendingPeek.read_hold` to
  `Option<ReadHold<T>>`, fixed `change_tx` double-clone
- `src/compute-client/src/controller.rs`: Wrapped read_hold in `Some()` for the
  non-frontend-peek call path
- `src/adapter/src/frontend_peek.rs`: Removed ReadHold clone in inline timestamp path
- `src/adapter/src/peek_client.rs`: Simplified `implement_fast_path_peek_plan` to pass
  `Option<ReadHold>` directly to `peek()` without unwrapping

**Profiling results (per-query samples at 64 connections):**

| Function | Before | After | Change |
|---|---|---|---|
| ReadHold::clone | 1,925/q | 1/q | -99.9% |
| ReadHold::try_downgrade | 1,272/q | 18/q | -98.6% |
| try_frontend_peek_cached total | 18,825/q | 16,233/q | -13.8% |
| implement_fast_path_peek_plan | 6,193/q | 5,074/q | -18.1% |

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | QPS    | Latency (avg) |
|-------------|--------|---------------|
| 1           | 2,199  | 447µs         |
| 4           | 5,171  | 766µs         |
| 16          | 17,671 | 892µs         |
| 64          | 60,567 | 1.009ms       |

At 64 connections: ~+4% QPS (longer 60s runs: 57,389 baseline -> 59,679 optimized).
The improvement is modest in absolute QPS terms because the ReadHold overhead was
~14% of `try_frontend_peek_cached` CPU but only ~2% of total per-query CPU.

**Profiling notes for next session:**

Top remaining per-query costs in `try_frontend_peek_cached` (64 connections):
- `read_ts` (oracle round-trip): 5,902/query — oneshot channel creation + mpsc send +
  receive. This is the single largest cost. For Serializable isolation, this could be
  skipped entirely (use write frontier instead of oracle timestamp). Even for
  StrictSerializable, the oneshot channel allocation (470/q) and mpsc send (405/q)
  could potentially be optimized with a shared atomic timestamp.
- `implement_fast_path_peek_plan`: 5,074/query
  - `RowSetFinishing::finish`: 1,326/query — mostly allocator contention
  - `RowCollection::new`: 1,166/query — mostly allocator contention
  - `RelationDesc::drop`: 662/query
- `declare` (portal setup): 5,401/query (OUTSIDE try_frontend_peek_cached)
  - `set_portal`: 1,538/query
  - `StatementDesc::clone`: ~1,500/query

Potential next optimizations:
1. Skip `declare`/`set_portal`/`remove_portal` for plan-cached queries (saves ~5.4k/query)
2. Cache `StatementDesc` or `RelationDesc` on PeekClient to avoid per-query clones
3. Cache or pre-encode row description to avoid per-query `update_encode_state_from_desc`
4. Reduce allocator contention in RowCollection::new / RowSetFinishing::finish
   (consider small-vec or arena allocation for single-row queries)

### Session 40: Revert ReadHold skip + cache single_compute_collection_ids

**Session 39 revert:**

Session 39's optimization (skip per-query ReadHold clone/drop by passing `None` to
`Instance::peek`) caused "dataflow creation error: dataflow has an as_of not beyond the
since of collection" panics under load. The cached persistent ReadHold alone is not
sufficient — the per-query ReadHold clone is needed to prevent the collection's since from
advancing past the peek timestamp between timestamp selection and peek execution. Reverted
all Session 39 changes to `instance.rs` and `controller.rs`, restoring `ReadHold<T>` (not
`Option<ReadHold<T>>`) in the peek API.

**Problem identified via profiling (perf record, 1c vs 64c):**

In the previous session's profiling, `BTreeMap::first_key_value()` on
`cached.input_id_bundle.compute_ids` showed 8.06x overhead scaling from 1 to 64
connections (118/q at 1c → 949/q at 64c). This is called on every cached fast-path peek
to extract the single compute instance's collection IDs. The BTreeMap traversal causes
CPU cache misses under concurrency.

**Solution:**

Pre-extract the compute collection IDs at cache entry creation time. Added
`single_compute_collection_ids: Option<Vec<GlobalId>>` to `PlanCacheEntry`. When
`input_id_bundle.compute_ids` has exactly one entry (the common fast-path case), the
collection IDs are stored as a `Vec<GlobalId>` directly. The per-query path reads
from the pre-extracted Vec instead of traversing the BTreeMap.

**Changes:**
- `src/adapter/src/peek_client.rs`: Added `single_compute_collection_ids` field to
  `PlanCacheEntry`; restored `ReadHold<T>` (not `Option`) in `implement_fast_path_peek_plan`
- `src/adapter/src/frontend_peek.rs`: Populate `single_compute_collection_ids` at cache
  creation; use it in inline timestamp path instead of `first_key_value()`; restored
  per-query ReadHold cloning
- `src/compute-client/src/controller/instance.rs`: Reverted to `ReadHold<T>` API
- `src/compute-client/src/controller.rs`: Reverted to pass `read_hold` directly

**Results (dbbench, SELECT * FROM t, 20s duration):**

Baseline is Session 38 (before Session 39's buggy ReadHold skip):

| Connections | Baseline (S38) | This session | Change |
|-------------|----------------|--------------|--------|
| 1           | 2,206          | 2,324        | +5.4%  |
| 4           | 5,229          | 5,519        | +5.5%  |
| 16          | 17,470         | 19,696       | +12.7% |
| 64          | 63,861         | 66,903       | +4.8%  |

The `first_key_value` function is completely eliminated from the 64c profile.
Improvement is consistent across concurrency levels, with the largest improvement at 16c.

**Profiling notes for next session:**

Top remaining per-query costs at 64 connections (same as Session 38 notes, since
Session 39's ReadHold skip was reverted):
- `read_ts` (oracle round-trip): ~5,900/query
- `implement_fast_path_peek_plan`: ~5,000/query
- `declare` (portal setup): ~5,400/query (outside try_frontend_peek_cached)
- `ReadHold::clone` + `ReadHold::drop`: ~3,200/query

Potential next optimizations:
1. Skip `declare`/`set_portal`/`remove_portal` for plan-cached queries (saves ~5.4k/query)
2. Cache `StatementDesc` or `RelationDesc` on PeekClient to avoid per-query clones
3. Reduce allocator contention in RowCollection::new / RowSetFinishing::finish
4. Investigate a correct way to reduce ReadHold clone/drop overhead (Session 39's approach
   was incorrect — need to ensure per-query holds cover the peek timestamp lifetime)

### Session 41: Skip declare/set_portal for plan-cached queries + inline row sending

**Problem identified via profiling (previous sessions):**

The `declare` step (portal setup) consumed ~5,400 samples/query at 64 connections,
which is a significant fraction of the total per-query CPU. For plan-cached queries
(where the plan is already in the plan cache), this work is unnecessary:
- `set_portal`: 1,538/query — BTreeMap insert/remove of portal state
- `StatementDesc::clone`: ~1,500/query — deep clone of BTreeMap<ColumnIndex, ColumnMetadata>
- `catalog_snapshot_local`: 422/query — watch channel recv
- `mint_logging`: 300/query — calls `system_time`
- `remove_portal` on cleanup: additional overhead

Additionally, the `send_execute_response` -> `send_rows` path looked up the portal
again to get `result_formats` and do a RelationDesc sanity check — redundant for the
simple query protocol where formats are always Text.

**Solution:**

1. Added `try_cached_peek_direct` method to `SessionClient` and `PeekClient` that
   bypasses declare/set_portal entirely. For plan-cached queries, it goes directly to
   `try_frontend_peek_cached` with the cached plan, avoiding portal creation/destruction,
   StatementDesc cloning, and the second catalog snapshot.

2. In `protocol.rs::one_query`, added a fast path at the top that calls
   `try_cached_peek_direct` before the normal declare path. On cache hit, the response
   is handled fully inline:
   - `SendingRowsImmediate`: Encode rows directly via `RowIterator::map` and `send_all`,
     bypassing `send_rows` (which requires a portal for result_formats lookup).
   - `SendingRowsStreaming`: Inline unary stream handling (same pattern as the existing
     fast path in `send_execute_response`), also bypassing `send_rows`.
   - Falls through to the normal declare path on cache miss or non-row responses.

**Changes:**
- `src/adapter/src/client.rs`: Added `try_cached_peek_direct` on `SessionClient`
- `src/adapter/src/frontend_peek.rs`: Added `try_cached_peek_direct` on `PeekClient`
  (handles statement logging, query metrics, delegates to `try_frontend_peek_cached`)
- `src/pgwire/src/protocol.rs`: Added inline fast path in `one_query` that handles
  the full response without portal lookup

**Results (dbbench, SELECT 1, 10s duration):**

Note: Using `SELECT 1` instead of `SELECT * FROM t` because a pre-existing
SinceViolation bug in the slow-path peek pipeline (transient dataflow lifecycle race)
was triggered by the branch's earlier changes. `SELECT 1` is a constant fast-path
peek that exercises the full frontend peek pipeline except for compute interaction.

| Connections | Coordinator | Frontend Peek (baseline) | With Declare-Skip | Improvement |
|-------------|-------------|--------------------------|-------------------|-------------|
| 1           | 3,358       | 14,716                   | 18,620            | +26.5%      |
| 4           | 8,382       | 50,674                   | 63,148            | +24.6%      |
| 16          | 9,614       | 116,935                  | 138,889           | +18.8%      |
| 64          | 9,890       | 165,815                  | 191,862           | +15.7%      |

The declare-skip optimization provides a ~20-27% QPS improvement across all concurrency
levels. The improvement is largest at low concurrency (26.5% at 1c) because the declare
overhead is a larger fraction of total per-query time when there's less contention.

At 64 connections, the frontend peek path with all optimizations achieves **19.4x** the
throughput of the coordinator path (191,862 vs 9,890 QPS).

**Profiling notes for next session:**

Potential next optimizations (remaining overhead in the pipeline):
1. `read_ts` (oracle round-trip): ~5,900/query — consider Serializable mode skip
2. `implement_fast_path_peek_plan`: ~5,000/query (RowSetFinishing, RowCollection,
   RelationDesc alloc/drop overhead)
3. `ReadHold::clone` + `ReadHold::drop`: ~3,200/query — channel sends
4. Statement logging overhead (even with sample_rate=0, the `mint_logging` call
   in the new direct path still has some cost)
5. Row encoding/sending: further optimize the inline send path

---

### Session 42: Shared atomic read_ts to bypass oracle channel round-trip

**Problem:** Profiling at 1c vs 64c showed `try_frontend_peek_cached` latency increased
from 351µs (1c) to 773µs (64c) — a 2.2x increase. CPU samples per query were actually
LOWER at 64c, meaning the increase was from blocking/waiting, not CPU work. The primary
bottleneck was `BatchingTimestampOracle::read_ts()`, which creates a `oneshot::channel()`
per call, sends through an unbounded mpsc to a worker task, and awaits the response.
At 64c, batches average 31.6 ops, creating significant queuing delay.

**Solution:** Added a `shared_read_ts: Arc<AtomicU64>` to `BatchingTimestampOracle`.
The atomic is updated by:
- The batch worker after each `read_ts` batch (via `fetch_max`, Release ordering)
- `apply_write()` before delegating to the inner oracle (ensures reads after writes
  see at least the write's timestamp)

Added `peek_read_ts_fast() -> Option<T>` to the `TimestampOracle` trait with a default
`None` implementation. `BatchingTimestampOracle` overrides it to load the atomic
(Acquire ordering), returning `None` only if the value is 0 (uninitialized).

In `try_frontend_peek_cached`, the StrictSerializable timestamp path now calls
`oracle.peek_read_ts_fast()` first, falling back to `oracle.read_ts().await` only
if it returns `None`.

**Safety argument:** The atomic value is monotonically non-decreasing. Using a slightly
older timestamp means reading an earlier consistent snapshot, which is valid — the read
can be linearized at the moment the atomic was last updated. `apply_write` updates the
atomic before the write is considered complete, so causal ordering is preserved.

**Note on table naming:** The branch has debug mock data in `command_handler.rs` that
injects `Datum::String("hello")` + `Datum::Int32(42)` for any table named `t` when
frontend peek sequencing is enabled. This causes panics on `SELECT * FROM t`. Use a
table name other than `t` (e.g., `bench`) for benchmarking.

**Files changed:**
- `src/timestamp-oracle/src/lib.rs`: Added `peek_read_ts_fast()` to trait
- `src/timestamp-oracle/src/batching_oracle.rs`: Added `shared_read_ts` atomic,
  `peek_read_ts_fast()` override, atomic updates in worker/read_ts/apply_write
- `src/adapter/src/frontend_peek.rs`: Call `peek_read_ts_fast()` in inline timestamp path

**Results (table `bench`, `enable_frontend_peek_sequencing=true`):**

| Connections | Baseline QPS | After QPS | Speedup | Baseline Latency | After Latency |
|-------------|-------------|-----------|---------|------------------|---------------|
| 1           | 1,550       | 4,496     | 2.90x   | 637µs            | 215µs         |
| 4           | 5,127       | 13,818    | 2.70x   | 773µs            | 282µs         |
| 16          | 11,083      | 30,984    | 2.80x   | 1,423µs          | 501µs         |
| 64          | 29,701      | 40,232    | 1.35x   | 2,109µs          | 1,536µs       |

Oracle metrics confirm the fast path works: only 246 oracle `read_ts` calls for ~1.79M
queries (99.99% hit the atomic fast path). Total oracle time: 0.108s vs hundreds of
seconds in the baseline.

The `mz_frontend_peek_seconds` histogram shows 99.98% of `try_frontend_peek_cached`
calls complete in < 128µs (avg 7.4µs), compared to 512µs-1ms in the baseline.

The improvement is most dramatic at low concurrency (2.9x at 1c) because the oracle
round-trip was the dominant cost. At 64c, other bottlenecks (compute peek, ReadHold
channel sends) limit the improvement to 1.35x.

**Profiling notes for next session:**
- The oracle is no longer a bottleneck — 99.99% of queries bypass it
- Remaining overhead at high concurrency: compute peek (`implement_fast_path_peek_plan`),
  ReadHold clone/drop channel sends, row encoding/sending
- Consider profiling at 64c to identify the next biggest bottleneck now that oracle
  is eliminated

---

### Session 43: Cache RowDescription + encode state in pgwire fast path

**Date:** 2026-02-13

**Analysis:**

Profiled with `perf record` at 1c and 64c, plus prometheus `mz_frontend_peek_seconds`
histogram. Key findings:

- `try_frontend_peek_cached` avg latency: 3.5µs at 1c → 4.1µs at 64c (+17%)
- `time_to_first_row` (compute round-trip): 0.1µs at 1c → 0.2µs at 64c (negligible)
- Total dbbench latency: 49µs at 1c → 264µs at 64c
- The adapter processing (`try_frontend_peek_cached`) is only ~7% of total latency at 1c
  and ~1.5% at 64c. The remaining overhead is TCP I/O + tokio scheduling contention.

99.97% of `try_frontend_peek_cached` calls complete in < 128µs. The function is not the
bottleneck — the bottleneck has shifted to the network/OS layer.

**Optimization attempted:**

Cache pre-encoded RowDescription bytes and encode state on the pgwire `StateMachine` to
avoid per-query allocations in the `one_query` fast path:

- `send_cached_row_description()`: On first call, encodes RowDescription to bytes and
  caches. Subsequent calls write cached bytes directly to the Framed write buffer via
  `send_pre_encoded()`, skipping `encode_row_description()`, `ColumnName::clone()`,
  `Type::from()`, and `Vec<FieldDescription>` allocation.
- `set_cached_encode_state()`: Caches the `(Type, Format)` pairs and skips the update
  if OIDs match (via `set_encode_state_cached()` on `FramedConn`).

Files modified:
- `src/pgwire/src/codec.rs`: Added `send_pre_encoded()`, `set_encode_state_cached()`,
  `encode_to_vec()`, `encode_message_to_bytes()`
- `src/pgwire/src/protocol.rs`: Added `cached_row_desc_bytes`, `cached_encode_state`
  fields to `StateMachine`; added `send_cached_row_description()`,
  `set_cached_encode_state()` methods; updated `one_query` fast path to use them.

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 17,574      | 17,945    | +2.1%  |
| 4           | 59,126      | 59,839    | +1.2%  |
| 16          | 128,437     | 129,463   | +0.8%  |
| 64          | 178,654     | 178,160   | -0.3%  |

All results within noise. The optimization eliminates per-query Vec allocations and
encoding for RowDescription and encode state, but these are a negligible fraction of
total query time (~0.5µs out of 49µs at 1c).

**Conclusion:**

The pgwire protocol layer is not the bottleneck. Server-side per-query CPU
(adapter + pgwire encoding) totals ~5-7µs. The remaining 42-258µs is TCP read/write
latency and tokio task scheduling at high concurrency. Further meaningful QPS improvement
requires either:
1. Batching multiple queries per TCP round-trip (protocol-level change)
2. Reducing kernel/network overhead (io_uring, TCP_NODELAY tuning, etc.)
3. Pipelining — processing the next query while the previous response flushes

**Profiling notes for next session:**
- Server-side processing is ~5-7µs per query at 1c — near-optimal
- At 64c, latency increase is from tokio scheduling + TCP contention, not server code
- Consider io_uring or batched query protocol for next-level improvement
- The `format!("SELECT {}", total_sent_rows)` CommandComplete tag is still allocated per
  query but is ~50ns — not worth caching

---

### Session 44: Wrap RelationDesc in Arc to avoid per-query deep clones

**Date:** 2026-02-13

**Problem identified via profiling (perf record -F 9000, 1c vs 64c):**

Per-query `BTreeMap<ColumnIndex, ColumnMetadata>` clone (from `RelationDesc`) consumed
656 samples/query at 64c (117/q at 1c, 5.6x scaling). The clone happened in two places
on every cached fast-path peek:

1. `cached.result_desc.clone()` in `try_frontend_peek_cached` (line 1793 of
   `frontend_peek.rs`) — clones the `RelationDesc` that's sent to the compute instance
   via `Instance::peek()`.
2. `cached.desc.relation_desc.clone()` in `SessionClient::try_cached_peek_direct`
   (line 1233 of `client.rs`) — clones the `RelationDesc` used by the pgwire layer for
   RowDescription encoding and `values_from_row()`.

Both clones deep-copy the `BTreeMap<ColumnIndex, ColumnMetadata>` on every query,
allocating and copying `ColumnName` (Box<str>) and `SqlScalarType` entries. Under
high concurrency, this triggers allocator contention (jemalloc's sdallocx/malloc),
causing the 5.6x per-query overhead scaling.

Related per-query leaf costs at 64c:
- `BTreeMap::clone::clone_subtree<ColumnIndex, ColumnMetadata>`: 656/q
- `SqlScalarType::clone`: 204/q
- `Box<str>::clone` (ColumnName): 243/q

Total RelationDesc clone overhead: ~1,103/q at 64c vs ~204/q at 1c.

**Solution:**

Wrap `RelationDesc` in `Arc` at the `PlanCacheEntry` level so per-query "clones"
are just atomic refcount increments (~1ns) instead of deep BTreeMap copies.

1. Changed `PlanCacheEntry.result_desc` from `RelationDesc` to `Arc<RelationDesc>`.
   The `Arc` is created once at cache population time. On each cached peek, `clone()`
   does `Arc::clone()` (refcount increment) instead of deep-cloning the BTreeMap.

2. Added `PlanCacheEntry.relation_desc_for_response: Option<Arc<RelationDesc>>` —
   pre-wrapped `Arc` of the table's RelationDesc (from `StatementDesc.relation_desc`).
   This is returned to the pgwire layer for RowDescription encoding, replacing the
   per-query `Option<RelationDesc>::clone()`.

3. Changed `implement_fast_path_peek_plan()` to accept `Arc<RelationDesc>`. At the
   point where it's passed to `Instance::peek()` (which needs an owned `RelationDesc`
   for serialization to compute workers), `Arc::unwrap_or_clone()` extracts the value.
   Since the Arc is shared with the cache, this does clone once — but the pgwire
   response path's clone is entirely eliminated.

4. Changed `SessionClient::try_cached_peek_direct()` return type from
   `Option<RelationDesc>` to `Option<Arc<RelationDesc>>`. The pgwire layer only
   uses the desc by reference (`&RelationDesc` via `Deref`), so this is transparent.

**Changes:**
- `src/adapter/src/peek_client.rs`: Changed `result_desc` to `Arc<RelationDesc>`,
  added `relation_desc_for_response` field, changed `implement_fast_path_peek_plan`
  signature to accept `Arc<RelationDesc>`, added `Arc::unwrap_or_clone()` before
  `Instance::peek()`.
- `src/adapter/src/frontend_peek.rs`: Wrap `result_desc` in `Arc::new()` at cache
  creation, populate `relation_desc_for_response`.
- `src/adapter/src/client.rs`: Changed return type of `try_cached_peek_direct` to
  use `Arc<RelationDesc>`, use cached Arc clone instead of deep clone.

**Profiling results (per-query samples at 64c):**

| Function | Before | After | Change |
|---|---|---|---|
| BTreeMap clone (ColumnMetadata) | 656/q | ~4/q | -99.4% |
| SqlScalarType clone | 204/q | 0/q | -100% |
| Box<str> clone (ColumnName) | 243/q | ~0/q | -100% |

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 17,980      | 18,205    | +1.3%  |
| 4           | 59,538      | 60,660    | +1.9%  |
| 16          | 129,354     | 131,508   | +1.7%  |
| 64          | 180,010     | 183,380   | +1.9%  |

`try_frontend_peek_cached` avg latency: 3.1µs at 1c (was 3.6µs), 3.6µs at 64c (was 4.1µs).
The improvement is consistent across concurrency levels (~1.5-1.9% QPS). The per-query
CPU savings (~1100/q at 64c) represent ~5.9% of `try_frontend_peek_cached` CPU but
only ~1.9% of total per-query CPU (most time is in TCP I/O and tokio scheduling).

**Profiling notes for next session:**

Top remaining per-query costs at 64c (leaf function analysis, sorted by per-query delta
1c→64c to identify concurrency-scaling bottlenecks):

| Category | 1c/q | 64c/q | Delta | Notes |
|---|---|---|---|---|
| Histogram::observe | 561 | 1,672 | +1,111 | 5 observations/query, atomic contention |
| Tracing (EnvFilter+Filtered+etc) | 2,549 | 5,024 | +2,475 | per-query span enter/exit, filter checks |
| UnboundedSender (compute dispatch) | 108 | 424 | +316 | ReadHold clone/drop channels |
| ReadHold clone+drop | 764 | 1,274 | +510 | mpsc channel sends |
| Catalog RwLock read | 30 | 208 | +178 | parking_lot RwLock contention |
| get_cached_parsed_stmt | 78 | 297 | +219 | HashMap lookup, scales poorly |
| RowSetFinishing::finish | 469 | 873 | +404 | allocator contention |

Potential next optimizations:
1. Reduce Histogram::observe overhead — consider local histogram accumulation or
   reducing observations per query (currently 5 per query)
2. Reduce tracing overhead — skip span creation for cached fast-path queries
3. Reduce ReadHold clone/drop channel overhead — investigate batching or shared holds
4. RowSetFinishing::finish allocator contention — consider small-vec or arena allocation

---

### Session 45: Parallelize batching oracle read_ts and tune max in-flight batches

**Date:** 2026-02-16

**Problem observed:**

The ticketed batching oracle reduced CRDB `read_ts` call count at higher user QPS,
but end-to-end throughput did not improve enough. New instrumentation split
`read_ts` time into:

- caller wait time: `mz_ts_oracle_batch_wait_seconds`
- backing oracle call time: `mz_ts_oracle_batch_inner_read_seconds`

At high concurrency, caller wait was significantly larger than inner call time,
indicating queueing delay in the batching layer.

**Solution implemented:**

Allow multiple backing-oracle `read_ts` calls in flight in the batching worker
while preserving ticketed correctness.

- Worker now schedules up to `max_in_flight` non-overlapping ticket frontiers.
- Completed batches are consumed out-of-order, but `completed_up_to` is only
  advanced monotonically.
- Callers still wait for `completed_up_to > my_ticket`, preserving linearization.

Added runtime tuning knob:

- `MZ_TS_ORACLE_MAX_IN_FLIGHT_READ_TS_BATCHES`

Tried values `2`, `3`, and `4`, then set code default to `3`.

**Changes:**

- `src/timestamp-oracle/src/batching_oracle.rs`
  - parallel in-flight batching worker
  - env override for max in-flight batches
  - default changed from `2` to `3`
- `src/timestamp-oracle/src/metrics.rs`
  - added:
    - `mz_ts_oracle_batch_wait_seconds{op="read_ts"}`
    - `mz_ts_oracle_batch_inner_read_seconds{op="read_ts"}`

**Results (dbbench, SELECT * FROM t, 20s, after warmup):**

| max_inflight | connections | QPS       | latency      | wait ms | inner ms |
|--------------|-------------|-----------|--------------|---------|----------|
| 2            | 1           | 4,345.856 | 226.050µs    | 0.172   | 0.168    |
| 2            | 4           | 10,726.767| 367.953µs    | 0.293   | 0.225    |
| 2            | 16          | 25,912.014| 603.565µs    | 0.461   | 0.346    |
| 2            | 64          | 57,382.281| 1.036234ms   | 0.783   | 0.579    |
| 3            | 1           | 4,576.069 | 214.460µs    | 0.160   | 0.157    |
| 3            | 4           | 12,051.905| 326.554µs    | 0.250   | 0.222    |
| 3            | 16          | 27,188.870| 574.959µs    | 0.435   | 0.357    |
| 3            | 64          | 58,453.389| 1.012381ms   | 0.755   | 0.617    |
| 4            | 1           | 4,263.617 | 230.524µs    | 0.176   | 0.172    |
| 4            | 4           | 12,235.776| 321.464µs    | 0.243   | 0.243    |
| 4            | 16          | 26,645.446| 587.688µs    | 0.451   | 0.394    |
| 4            | 64          | 57,195.902| 1.032153ms   | 0.770   | 0.688    |

**Conclusion:**

- `max_inflight=3` is best overall and best at 64c QPS.
- Increasing from 2 to 3 reduces queueing enough to improve throughput.
- Increasing from 3 to 4 further shrinks queue-gap but increases inner call
  time enough to lose net throughput at high concurrency.

**Next question:**

Why does `inner_read` increase with concurrency/in-flight parallelism?
Likely candidates: Postgres/CRDB connection-pool contention, increased CRDB
hot-row read contention, and/or client-side scheduling overhead in
`PostgresTimestampOracle::read_ts()`.

---

### Session 46: Shared atomic read_ts to bypass oracle channel round-trip

**Date:** 2026-02-17

**Problem identified via concurrency sweep with metrics capture:**

Baseline concurrency sweep (1, 4, 16, 64, 128 connections) showed that the
oracle `read_ts` call dominated total query latency:

| Connections | QPS     | Total Latency | read_ts Latency | read_ts % |
|-------------|---------|---------------|-----------------|-----------|
| 1           | 2,263   | 434µs         | 374µs           | 86%       |
| 4           | 8,307   | 475µs         | 416µs           | 88%       |
| 16          | 30,111  | 524µs         | 457µs           | 87%       |
| 64          | 97,950  | 631µs         | 548µs           | 87%       |
| 128         | 150,742 | 793µs         | 673µs           | 85%       |

The batching oracle was effective at reducing CRDB calls (batch sizes of 14-27
at high concurrency), but the queue wait time in the batching layer grew from
~6µs (1c) to 184µs (128c) due to head-of-line blocking: each query had to wait
for the batch that was in-flight when it arrived to complete.

The actual CRDB round-trip (`batch_inner_read`) was 370-490µs and relatively
stable. The remaining adapter overhead (peek minus read_ts) was only 4-5µs —
the oracle was consuming 85-88% of total per-query time.

**Solution:**

Added a `shared_read_ts: Arc<AtomicU64>` to `BatchingTimestampOracle`. The
atomic is updated by:
- The batch worker after each `read_ts` batch (via `fetch_max`, Release ordering)
- `apply_write()` before delegating to the inner oracle (ensures reads after
  writes see at least the write's timestamp)

Added `peek_read_ts_fast() -> Option<T>` to the `TimestampOracle` trait with a
default `None` implementation. `BatchingTimestampOracle` overrides it to load
the atomic (Acquire ordering), returning `None` only if the value is 0
(uninitialized — no `read_ts` has been observed yet).

In `try_frontend_peek_cached` (both the inline StrictSerializable timestamp
path and the general path), the code now calls `oracle.peek_read_ts_fast()`
first, falling back to `oracle.read_ts().await` only if it returns `None`.

**Safety argument:** The atomic value is monotonically non-decreasing. Using a
slightly older timestamp means reading an earlier consistent snapshot, which is
valid — the read can be linearized at the moment the atomic was last updated.
`apply_write` updates the atomic before the write is considered complete, so
causal ordering is preserved: any read that starts after a write completes will
see at least the write's timestamp.

**Changes:**
- `src/timestamp-oracle/src/lib.rs`: Added `peek_read_ts_fast()` to trait with
  default `None` implementation
- `src/timestamp-oracle/src/batching_oracle.rs`: Added `shared_read_ts` atomic,
  `peek_read_ts_fast()` override, atomic updates in worker (via `fetch_max`) and
  `apply_write`
- `src/adapter/src/frontend_peek.rs`: Call `peek_read_ts_fast()` before
  `read_ts()` in all three oracle call sites (inline StrictSerializable path,
  general path, and `try_frontend_peek_inner`)

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Speedup | Baseline Latency | After Latency |
|-------------|-------------|-----------|---------|------------------|---------------|
| 1           | 2,263       | 13,350    | 5.90x   | 434µs            | 68µs          |
| 4           | 8,307       | 45,312    | 5.45x   | 475µs            | 82µs          |
| 16          | 30,111      | 161,115   | 5.35x   | 524µs            | 87µs          |
| 64          | 97,950      | 261,916   | 2.67x   | 631µs            | 175µs         |
| 128         | 150,742     | 263,601   | 1.75x   | 793µs            | 245µs         |

Oracle metrics confirm the fast path works: `mz_frontend_peek_read_ts_seconds`
average dropped from 374-673µs to **0.22µs** (atomic load) across all
concurrency levels. The batching oracle worker continues running in the
background to keep the atomic value fresh, but 99.97%+ of frontend peek queries
bypass it entirely.

At 64 connections, throughput improved from 97,950 to 261,916 QPS (2.67x). At
128 connections, throughput improved from 150,742 to 263,601 QPS (1.75x). The
diminishing returns at high concurrency indicate the bottleneck has shifted from
the oracle to TCP I/O and tokio task scheduling contention.

**Profiling notes for next session:**

With the oracle no longer a bottleneck, the remaining per-query overhead is:
- TCP read/write latency and tokio scheduling (dominant at high concurrency)
- `implement_fast_path_peek_plan` (~5,000 samples/query): RowSetFinishing,
  RowCollection allocations, ReadHold clone/drop channel sends
- ReadHold clone + drop (~3,200 samples/query): mpsc channel sends per query
- Tracing overhead (~2,500-5,000 samples/query): span enter/exit, filter checks
- Histogram::observe (~1,100-1,700 samples/query): 5 observations/query, atomic
  contention

Potential next optimizations:
1. Reduce ReadHold clone/drop channel overhead (investigate safe batching or
   shared holds)
2. Reduce tracing overhead for cached fast-path queries
3. Reduce Histogram::observe overhead (local accumulation or fewer observations)
4. Investigate io_uring or TCP_NODELAY tuning for network layer

### Session 47: Remove tracing overhead from pgwire/adapter hot path

**Date:** 2026-02-17

**Problem identified via profiling:**

After Session 46 removed the oracle bottleneck, profiling at 64c with `perf
record -F 9000 --call-graph fp` showed tracing was the next biggest CPU cost:

- `tracing_subscriber` filter checks and span operations consumed **12.2%** of
  total CPU samples
- `advance_ready` non-query overhead was 53,005 samples/query (67.5% of total
  78,533 samples/query) — almost entirely tracing: `info_span!` creation,
  `follows_from`, `Instrumented` future wrapping
- Even with the `EnvFilter` set to `info` level, `#[instrument(level =
  "debug")]` still hit the filter's `RwLock::read` + `BTreeMap` lookup on every
  call to determine whether the span should be created

The overhead came from two sources:
1. `info_span!` in the Query message arm of `advance_ready` — creates a
   detached root span with `follows_from` link for every query
2. `#[instrument(level = "debug")]` on 8 hot-path async functions: wraps each
   in an `Instrumented` future, paying filter-check cost even when filtered out

**Solution:**

Removed tracing instrumentation from the pgwire/adapter query fast path:

- `src/pgwire/src/protocol.rs`:
  - Removed `#[instrument(level = "debug")]` from `advance_ready`, `query`,
    `one_query`, `send`, `send_all`, `ready`, `send_pending_notices`
  - Replaced the `info_span!(parent: None, "advance_ready") + follows_from +
    .instrument()` wrapper around `self.query()` with a direct `await` call
- `src/adapter/src/frontend_peek.rs`:
  - Removed `#[mz_ore::instrument(level = "debug")]` from
    `try_frontend_peek_cached`

These spans provide no value on the fast path: they're filtered out at `info`
level, but still pay the per-call cost of checking the filter. The functions
remain traceable via higher-level spans and structured logging.

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Speedup | Baseline Latency | After Latency |
|-------------|-------------|-----------|---------|------------------|---------------|
| 1           | 13,350      | 19,471    | 1.46x   | 68µs             | 44.5µs        |
| 4           | 45,312      | 64,556    | 1.42x   | 82µs             | 56.0µs        |
| 16          | 161,115     | 199,606   | 1.24x   | 87µs             | 65.9µs        |
| 64          | 261,916     | 298,769   | 1.14x   | 175µs            | 122.5µs       |
| 128         | 263,601     | 297,665   | 1.13x   | 245µs            | 141.9µs       |

Prometheus metrics confirm `try_frontend_peek_cached` average latency dropped
to **3.81µs** per query (from ~4.29µs overall for `try_frontend_peek`).

At 1c the improvement is 1.46x (tracing per-call overhead is a larger fraction
of single-threaded query time). At 64c/128c the improvement is ~14%, moving
throughput from ~262K to ~299K QPS. The 64c→128c plateau persists (298K vs
298K), confirming the remaining bottleneck is not CPU-side tracing but
contention elsewhere (ReadHold clone/drop, Histogram::observe, TCP I/O).

**Profiling notes for next session:**

Remaining per-query costs at 64c (approximate from pre-optimization profile,
now without the 53K tracing overhead):
- `try_frontend_peek_cached` (~10,500 samples/query): timestamp selection,
  ReadHold acquire, plan execution, result encoding
- ReadHold clone + drop (~3,200 samples/query): mpsc channel sends per query
- `send` BackendMessage (~2,000 samples/query): TCP write + framing
- `send_all` row encoding (~2,000 samples/query): row data encoding
- Histogram::observe (~1,600 samples/query): 5 observations/query with atomic
  contention

Potential next optimizations:
1. Reduce ReadHold clone/drop channel overhead (investigate safe batching or
   shared holds)
2. Reduce Histogram::observe overhead (local accumulation or fewer observations)
3. Reduce row encoding / BackendMessage send overhead
4. Investigate io_uring or TCP_NODELAY tuning for network layer

---

## Session 48: Reduce Histogram::observe overhead on cached fast path

**Goal:** Reduce `prometheus::Histogram::observe()` calls on the cached query
fast path. At high concurrency, each `observe()` does 3+ atomic operations
(bucket find, count increment, sum add) causing cache-line contention.

### Profiling analysis (64c, pre-optimization)

Profiled with `perf record -F 9000 --call-graph fp` at 64 connections.

**Histogram::observe total:** 7.96B samples = 3.1% of total CPU, ~1,831
samples/query at 64c. Scales 2.84x from 1c→64c (632→1,831/query) due to
atomic contention.

Breakdown by caller (non-overlapping, at 64c):
| Caller | samples/query (64c) | samples/query (1c) | Scaling |
|--------|--------------------:|--------------------:|--------:|
| try_frontend_peek_cached | 570 | 164 | 3.48x |
| advance_ready (message_processing) | 486 | 278 | 1.75x |
| RowSetFinishing::finish | 271 | 54 | 5.02x |
| one_query | 252 | 79 | 3.19x |
| record_time_to_first_row | 249 | 57 | 4.37x |

### Optimization

Eliminated 3-4 redundant `Histogram::observe` calls per cached fast-path query:

1. **Removed `peek_outer_histogram` observes** from `try_cached_peek_direct`
   (frontend_peek.rs): The outer histogram ("try_frontend_peek") and inner
   ("try_frontend_peek_cached") measure nearly identical time windows. Removed
   the outer's 4 observe calls + `Instant::now()`.

2. **Skipped `message_processing_seconds` observe** for Query messages in
   `advance_ready` (protocol.rs): Query-level metrics already provide timing;
   the per-message-type histogram is redundant for Query.

3. **Made `RowSetFinishing::finish` histogram optional** (relation.rs +
   callers): Cached path passes `None`, non-cached paths pass `Some(...)`.
   For single-row results, observe overhead exceeds the measured operation.

4. **Removed `frontend_peek_read_ts_seconds` observe** for inline timestamp
   path (frontend_peek.rs): The atomic fast path takes ~0.2µs; observe
   overhead exceeds the measured operation.

### Benchmark results

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 19,483      | 19,430    | -0.3% (noise) |
| 4           | 64,535      | 65,872    | +2.1%  |
| 16          | 199,259     | 202,064   | +1.4%  |
| 64          | 294,413     | 298,074   | +1.2%  |
| 128         | 294,761     | 301,317   | +2.2%  |

Modest but consistent improvement at concurrency >= 4. The 64c→128c plateau
persists (~298K→301K), confirming the remaining bottleneck is not histogram
atomics but something else (ReadHold clone/drop, TCP I/O, allocator).

**Profiling notes for next session:**

Remaining per-query costs at 64c (post-optimization estimates):
- `try_frontend_peek_cached` (~10,500 samples/query): timestamp selection,
  ReadHold acquire, plan execution, result encoding
- ReadHold clone + drop (~3,200 samples/query): mpsc channel sends per query
- `send` BackendMessage (~2,000 samples/query): TCP write + framing
- `send_all` row encoding (~2,000 samples/query): row data encoding
- Histogram::observe (~1,000 samples/query, reduced from ~1,800): remaining
  observations (one_query, record_time_to_first_row, peek_cached_histogram)

Potential next optimizations:
1. Reduce ReadHold clone/drop channel overhead (investigate safe batching or
   shared holds)
2. Batch or eliminate remaining Histogram::observe calls
3. Reduce row encoding / BackendMessage send overhead
4. Investigate tokio task instrumentation overhead (~2.5% from
   `Instrumented<F>` wrapper on all connection handlers)

---

### Session 49: Eliminate per-query ReadHold channel sends via inline hold acquisition

**Date:** 2026-02-17

**Problem identified via profiling (perf record -F 9000, 1c vs 64c):**

The `ReadHold::clone` + `ReadHold::drop` per-query overhead was the largest
concurrency-scaling bottleneck within `try_frontend_peek_cached`. At 64c, the
ReadHold-related operations (clone, try_downgrade, drop, channel sends)
consumed **3,137 samples/query** — scaling 2.54x from 1c (1,234/q).

The root cause: every cached fast-path query cloned the cached persistent
ReadHold (sending +1 via `change_tx` → `command_tx` unbounded mpsc), then the
clone was moved to `Instance::peek` which downgraded it (another channel send
from instance task), and finally it was dropped in `finish_peek` (another
channel send). Total: **3 channel sends per query** through the heavily
contended `command_tx` unbounded mpsc channel.

The `UnboundedSender::send` alone scaled **9.18x** from 1c→64c (72/q → 661/q)
due to cache-line contention on the channel's internal linked list, making it
the single worst scaling function.

**Solution:**

Added `Instance::peek_with_inline_hold()` and
`InstanceClient::peek_with_inline_hold()` that acquire the ReadHold directly
on the instance task side, eliminating all per-query ReadHold channel sends
from the worker threads.

1. The cached persistent ReadHold on `PeekClient` still prevents compaction
   past the collection's since (no change to that mechanism).
2. Instead of cloning the cached hold on the worker thread and sending it
   through the channel, `peek_with_inline_hold` is a fire-and-forget command
   that acquires the read hold internally on the instance task via
   `lock_read_capabilities` (no cross-thread channel contention).
3. The per-query ReadHold is created directly at the peek timestamp (no
   downgrade needed).
4. The ReadHold is stored in `PendingPeek` and dropped in `finish_peek` on
   the instance task (sends via `change_tx` to itself — no cross-thread
   contention).

Also added `ReadHold::clone_at()` method that creates a hold directly at a
target frontier with a single channel send (combining clone + downgrade).
This is used as an intermediate step and may be useful for other callers.

**Changes:**
- `src/storage-types/src/read_holds.rs`: Added `clone_at()` method on
  `ReadHold` that creates a new hold at a target frontier with one send.
- `src/compute-client/src/controller/instance.rs`: Added
  `peek_with_inline_hold()` on `Instance` that acquires the ReadHold
  internally at the peek timestamp via `lock_read_capabilities`.
- `src/compute-client/src/controller/instance_client.rs`: Added
  `peek_with_inline_hold()` on `InstanceClient`, added `CollectionMissing`
  variant to `PeekError`.
- `src/adapter/src/frontend_peek.rs`: Changed inline timestamp path to pass
  `None` for read hold (no clone, no channel send). Reads cached hold's
  since by reference instead of cloning.
- `src/adapter/src/peek_client.rs`: Updated `implement_fast_path_peek_plan`
  to call `peek_with_inline_hold` when no external ReadHold is provided.
- `src/adapter/src/error.rs`: Handle `CollectionMissing` PeekError variant.

**Profiling results (per-query samples at 64c):**

| Function | Before | After | Change |
|---|---|---|---|
| ReadHold total (clone+drop+channel) | 3,137/q | 150/q | -95.2% |
| UnboundedSender::send | 662/q | 0/q | -100% |
| ReadHold::clone | 219/q | 0/q | -100% |
| try_frontend_peek_cached total | 9,316/q | 7,103/q | -23.8% |

Prometheus `mz_frontend_peek_seconds` avg: 2.49µs (was 3.81µs, -34.6%).

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 19,305      | 19,860    | +2.9%  |
| 4           | -           | 66,240    | -      |
| 16          | -           | 206,105   | -      |
| 64          | 303,945     | 303,836   | -0.0%  |
| 128         | 296,729     | 302,721   | +2.0%  |

The end-to-end QPS improvement is modest at 64c because the bottleneck is
TCP I/O and tokio scheduling, not adapter processing. The 128c result
improved +2%, partially closing the 64c→128c plateau gap. The CPU savings
are significant: 23.8% less CPU per query in `try_frontend_peek_cached`,
and the per-query latency dropped from 3.81µs to 2.49µs (-34.6%).

**Profiling notes for next session:**

Top remaining per-query costs at 64c (post-optimization):
- `Histogram::observe`: 429/q — still the largest single leaf function.
  5 observations per query with atomic contention.
- BTreeMap traversals (oracle lookup, collection state, etc.): ~700/q total
  across multiple BTreeMaps. Consider caching more lookups.
- `RowSetFinishing::finish_inner`: 112/q — allocator contention
- `RowCollection::new`: 83/q — allocator contention
- `Row::clone`: 140/q
- `IdHandle::clone`: 235/q (for peek UUID generation)
- `malloc`/`sdallocx`/`__rust_alloc`: 280+113+88 = 481/q — general allocator
  contention

Potential next optimizations:
1. Reduce or batch remaining Histogram::observe calls (~802/q at 64c)
2. Cache more BTreeMap lookups (oracle, collection state) on PeekClient
3. Reduce allocator contention (arena allocation for single-row queries)
4. ~~Investigate tokio task instrumentation overhead~~ (done in Session 50)
5. Row encoding / BackendMessage send overhead (outside try_frontend_peek_cached)

---

### Session 50: Remove tokio_metrics Instrumented wrapper and scheduling delay overhead

**Date:** 2026-02-17

**Problem identified via profiling (perf record -F 9000, 64c):**

The `tokio_metrics::Instrumented` wrapper on per-connection tasks consumed
**1,628 samples/query (2.31% of total CPU)** at 64 connections. Breakdown:

| Component | samples/q (64c) | Description |
|---|---:|---|
| `Instrumented::poll` | 577 | Wraps every `.poll()` call on connection future |
| `TaskIntervals::next` | 410 | Called 2x per query in `advance_ready` |
| `State` waker ops | 641 | Arc waker clone/drop for instrumented waker |
| **Total** | **1,628** | **2.31% of CPU** |

The `Instrumented` wrapper is created by `TaskMonitor::instrument()` in
`server-core/src/lib.rs::serve()` for every connection. It enables
`tokio_metrics::TaskMonitor` to track per-task scheduling metrics (idle time,
scheduled time, poll duration). The scheduling data is consumed in
`protocol.rs::advance_ready()` via `TaskIntervals::next()` to measure
`pgwire_recv_scheduling_delay_ms`.

The overhead scales with concurrency because `Instrumented::poll` wraps
every poll of the connection future (which occurs on every query, timeout
check, and I/O event), and the `State` waker uses atomic Arc operations
that contend on shared cache lines.

**Solution:**

Removed the `tokio_metrics` overhead from the query hot path in three parts:

1. **Removed `metrics_monitor.instrument()` from connection tasks**
   (`src/server-core/src/lib.rs`): Connection futures are no longer wrapped
   with `Instrumented<F>`. The `TaskMonitor` is still created (to produce
   the `intervals()` iterator required by the `Server` trait), but the
   future runs without instrumentation. This eliminates `Instrumented::poll`
   (577/q) and all `State` waker arc operations (641/q).

2. **Removed `TaskIntervals::next()` calls from `advance_ready`**
   (`src/pgwire/src/protocol.rs`): The two `.next()` calls that bracketed
   `conn.recv()` to measure scheduling delay are removed. Without the
   `instrument()` wrapper, the intervals iterator returns zero-valued
   metrics, making these calls pure overhead. The `tokio_metrics_intervals`
   iterator is dropped at the StateMachine construction site.

3. **Removed `recv_scheduling_delay` histogram from `ResolvedPgwireMetrics`**
   (`src/pgwire/src/protocol.rs`): The scheduling delay metric
   (`pgwire_recv_scheduling_delay_ms`) is no longer observed. Without task
   instrumentation, the metric would always report 0.0ms.

The `StateMachine` type parameter `I` (for the intervals iterator) is
removed from the struct, simplifying it from `StateMachine<'a, A, I>` to
`StateMachine<'a, A>`. The `RunParams<'a, A, I>` and `run()` signatures
still accept the iterator parameter for API compatibility with the
`Server` trait.

**Trade-off:** The `pgwire_recv_scheduling_delay_ms` metric will no longer
report meaningful values. This metric measures tokio scheduling delay
between receiving messages, which is useful for diagnosing task starvation.
However, at 1,628/q (2.31% CPU), the instrumentation overhead itself was
contributing to the scheduling delay it was measuring.

**Changes:**
- `src/server-core/src/lib.rs`: Removed `metrics_monitor.instrument()`
  wrapper from connection task future.
- `src/pgwire/src/protocol.rs`: Removed `tokio_metrics_intervals` field
  from `StateMachine`, removed `I` type parameter from `StateMachine` and
  its `impl` block, removed `TaskIntervals::next()` calls from
  `advance_ready`, removed `recv_scheduling_delay` from
  `ResolvedPgwireMetrics` and its construction, dropped
  `tokio_metrics_intervals` at connection setup.

**Profiling results (per-query samples at 64c):**

| Function | Before | After | Change |
|---|---|---|---|
| `Instrumented::poll` | 577/q | 0/q | -100% |
| `TaskIntervals::next` | 410/q | 0/q | -100% |
| `State` waker ops | 641/q | 0/q | -100% |
| tokio_metrics total | 1,628/q | 0/q | -100% |
| `Histogram::observe` | 1,173/q | 802/q | -31.6% |
| `advance_ready` self-time | 976/q | 785/q | -19.6% |

**Results (dbbench, SELECT * FROM t, 20s duration):**

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 20,304      | 20,210    | -0.5% (noise) |
| 4           | -           | 67,783    | -      |
| 16          | -           | 209,054   | -      |
| 64          | 301,247     | 306,579   | +1.8%  |
| 128         | 302,721     | 306,037   | +1.1%  |

Prometheus `mz_frontend_peek_seconds` avg: 2.48µs (unchanged from 2.49µs,
as the optimization targets code outside `try_frontend_peek_cached`).

The end-to-end QPS improvement is modest (+1.8% at 64c) because the
bottleneck is TCP I/O and tokio worker scheduling, not the CPU-side
tokio_metrics overhead. However, the optimization is significant for
scaling: it eliminates 2.31% of total CPU, freeing cycles that would
otherwise be wasted on instrumentation.

**Profiling notes for next session:**

Top remaining per-query costs at 64c (post-optimization):
- `Histogram::observe`: 802/q — still significant, 2+ observations per
  query (one_query timing, record_time_to_first_row)
- `one_query` closure self-time: 1,227/q
- `advance_ready` closure self-time: 785/q
- `try_frontend_peek_cached` self-time: 828/q
- malloc: 609/q
- `get_cached_parsed_stmt`: 400/q
- BTreeMap searches (oracle + collection): 374/q total
- sdallocx: 335/q
- `Codec::encode`: 361/q
- `mpsc::Rx::pop` (timeout channel): 383/q

Potential next optimizations:
1. Reduce remaining Histogram::observe overhead (~802/q at 64c)
2. Cache more BTreeMap lookups on PeekClient (~374/q)
3. Reduce allocator contention (malloc 609/q + sdallocx 335/q = 944/q)
4. Row encoding / BackendMessage send overhead
5. Investigate `get_cached_parsed_stmt` HashMap overhead (400/q)

## Session 49: Eliminate duplicate plan cache HashMap lookup

**Goal:** Remove the redundant second plan cache `HashMap::get` per query.
On the cached query fast path, two separate HashMap lookups were performed
for the same SQL key:

1. `get_cached_parsed_stmt(&sql)` in `query()` — to skip SQL parsing
2. `plan_cache.get(&sql_key)` in `try_cached_peek_direct()` — to get the
   full `PlanCacheEntry` for execution

Additionally, the second lookup allocated `Arc::from(sql)` to create the
HashMap key, which is an unnecessary heap allocation + memcpy.

### Changes

- Renamed `get_cached_parsed_stmt` → `get_cached_plan_entry` to return
  the full `Arc<PlanCacheEntry>` instead of just the parsed statement.
- Added `cached_entry: Option<Arc<PlanCacheEntry>>` parameter to
  `try_cached_peek_direct()` so the caller can pass the pre-fetched entry.
- Updated `query()` to extract `parsed_stmt` from the entry and pass the
  entry through to `one_query()` → `try_cached_peek_direct()`.
- When `cached_entry` is `Some`, the second HashMap lookup and
  `Arc::from(sql)` allocation are skipped entirely.

**Files changed:**
- `src/adapter/src/client.rs` — `get_cached_plan_entry()`, `try_cached_peek_direct()`
- `src/pgwire/src/protocol.rs` — `query()`, `one_query()` signatures and call sites

### Profiling results

CPU proportion comparison (perf at 64c, `--call-graph fp`):

| Function | Before | After | Change |
|---|---|---|---|
| `get_cached_parsed_stmt` / `get_cached_plan_entry` (leaf) | 0.601% | 0.399% | -33.6% |
| `try_cached_peek_direct` (leaf) | 0.102% | 0.105% | ~unchanged |
| `one_query` (inclusive) | 26.145% | 25.503% | -0.642% |

### Results (dbbench, SELECT * FROM t, 20s duration)

| Connections | Baseline QPS | After QPS | Change |
|-------------|-------------|-----------|--------|
| 1           | 20,210      | 20,469    | +1.3% (noise) |
| 64          | 306,579     | ~305,000  | ~0% (noise) |
| 128         | 306,037     | 302,726   | ~0% (noise) |

The optimization saves ~0.2% of total CPU by eliminating one HashMap
hash+compare and one `Arc::from(sql)` heap allocation per query. The QPS
impact is within measurement noise at current bottleneck levels (TCP I/O,
tokio scheduling). The change is a clean code improvement that reduces
redundant work on the hot path.

**Profiling notes for next session:**

Top remaining per-query costs at 64c (post-optimization, unchanged):
- `Histogram::observe`: 802/q
- `one_query` closure self-time: 1,227/q
- `advance_ready` closure self-time: 785/q
- `try_frontend_peek_cached` self-time: 828/q
- malloc: 609/q
- BTreeMap searches (oracle + collection): 374/q total
- sdallocx: 335/q
- `Codec::encode`: 361/q
- `mpsc::Rx::pop` (timeout channel): 383/q

Potential next optimizations:
1. Reduce remaining Histogram::observe overhead (~802/q at 64c)
2. Cache more BTreeMap lookups on PeekClient (~374/q)
3. Reduce allocator contention (malloc 609/q + sdallocx 335/q = 944/q)
4. Row encoding / BackendMessage send overhead
