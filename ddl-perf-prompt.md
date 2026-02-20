I'm working on diagnosing and fixing DDL performance degradation when Materialize
has a large number of objects (tables, views, indexes, etc.). We have a log file
in @ddl-perf-log.md where we keep recording findings for future sessions.

The problem is per-statement latency: a single CREATE TABLE or CREATE VIEW that
takes milliseconds on a fresh environment can take multiple seconds when there
are thousands of existing objects. We don't care about DDL throughput (nobody
runs DDL in a tight loop) — what matters is that each individual DDL statement
completes in a reasonable time regardless of how many objects already exist.

Figure out where DDL time is going and fix it. We need to use profiling, metrics,
code analysis, and potentially custom logging to understand the bottlenecks.

We want to try one thing per session — either a diagnostic step or an
optimization — then note down our results in the log file and stop. I'll then
start a new session to tackle the next thing. The workflow is:

1. Reproduce/measure the problem (establish baselines at various object counts)
2. Profile and diagnose where time is spent
3. Fix the bottleneck
4. Re-measure to confirm improvement
5. Repeat — there may be multiple layers of bottlenecks

After we complete a step and note results in our log, add a good description to
the jj change and start a new one with `jj new` for the next session.

Don't run tests, right now this will only slow us down. We're happy as long as
we can run our DDL statements.

For reproducing and diagnosing, prefer debug builds (`bin/environmentd` without
`--optimized`) — they're faster to compile and make O(n)/O(n^2) scaling more
visible since everything is slower. We don't care about absolute performance,
only how latency degrades as object count grows. Use `--optimized` only when
you need to confirm a fix against realistic absolute numbers.

**Don't use `--reset` between runs.** Creating 50k tables takes a very long
time, so we want to reuse existing state across runs and even across switching
between debug and optimized builds. Instead of resetting, start environmentd
without `--reset`, inspect current state (e.g. count objects with
`SELECT count(*) FROM mz_objects`), verify you have the expected number of
objects, and then proceed with your measurements. Only use `--reset` if the
state is actually corrupt or you explicitly need a fresh start.

You'll likely need hundreds or thousands of objects before degradation is
visible. Use a script (bash loop with psql, or a SQL DO block) to bulk-create
objects rather than doing it by hand.

To change system variables, connect as mz_system on the internal port:
```
psql -U mz_system -h localhost -p 6877 materialize
```

Focus initially on the "large number of objects" case. We can expand to "large
number of schemas" later if needed.

You can look at prometheus metrics for environmentd at
http://localhost:6878/metrics.

Below here, I have some immediate next steps to explore. Once you feel you have
resolved one of them, please update this prompt so that we don't consider them
anymore in our next sessions. Update the prompt in a separate git commit with a
good description.

Current status (after Session 19): At ~37.5k objects (optimized build, Docker
CockroachDB), DDL latency is CREATE TABLE ~136ms (individual psql),
catalog_transact avg ~95ms, apply_catalog_implications avg ~46ms. That's down
from 444ms at the Session 6 baseline. CPU work per DDL is now ~0.6ms — DDL is
I/O bound.

**Important: DDL is now I/O bound, not CPU bound.** Use Prometheus wall-clock
histograms for profiling, not perf/flamegraphs. **Always verify proportions
with optimized builds** — debug builds grossly distort CPU-heavy vs I/O-heavy
ratios (Session 19 showed table_register as 95% in debug but only 20% in
optimized).

**Session 19 create_collections breakdown (optimized, 33ms total):**
- `storage_collections`: **~24ms (73%)** — 3 sequential persist ops:
  upgrade_version, open_critical_handle, compare_and_downgrade_since
- `table_register`: ~6.5ms (20%) — txn-wal registration
- `open_data_handles`: ~0.7ms (2%) — write handle open
- `sequential_loop`: ~0.1ms (0%) — CPU work

Completed optimizations (Sessions 7-9, 11, 13, 15-16, 18):
- Cached Snapshot in PersistHandle (Session 7)
- Lightweight allocate_id bypassing full Transaction (Session 8)
- Persistent CatalogState with imbl::OrdMap + Arc (Session 9) — eliminated the
  ~30% Cow\<CatalogState\> clone+drop cost
- Removed O(n) table advancement loop from group_commit (Session 11) — eliminated
  the 23% cost of iterating all tables on every DDL. Txn-wal protocol already
  handles logical frontier advancement for all registered shards.
- Incremental consolidation in apply_updates (Session 13) — replaced O(n log n)
  sort_unstable on the full trace with O(n) merge of sorted old + new entries.
  Consolidation was 24% of catalog_transact (~19ms) in Session 12 profiling.
- BTreeMap snapshot trace (Session 15) — replaced Vec<(T, Timestamp, Diff)> with
  BTreeMap<T, Diff>, eliminating the O(n) consolidation entirely. Consolidation
  was 15% of total CPU (~31ms) in Session 14 profiling. Now O(m log n) per commit.
- Lazy validate_resource_limits counting (Session 16) — guard each user_*().count()
  call with a check on whether the corresponding delta is positive. For CREATE TABLE,
  skips ~10 unnecessary O(n) catalog scans. Was 7.8% of total CPU in Session 14.
- Eliminated redundant WriteHandle in storage_collections for tables (Session 18)
  — tables had two separate WriteHandle opens for the same shard (one in
  storage_collections, one in storage_controller). Not measurable with local
  filesystem persist but saves ~20-50ms per DDL with S3-backed persist in
  production.

Completed diagnostics (Sessions 10, 12, 13, 14, 17, 18, 19):
- Profiled post-Session 9 optimized build at ~10k objects (Session 10)
- Profiled post-Session 11 optimized build at ~28k objects (Session 12)
- Profiled post-Session 13 optimized build at ~36.5k objects (Session 14)
- Wall-clock profiling via Prometheus at ~37k objects (Session 17) — revealed
  I/O dominance: apply_catalog_implications=42ms, create_collections=20-25ms,
  oracle operations=5ms, initialize_read_policies=10ms
- Session 18 A/B tested redundant WriteHandle elimination: no measurable
  improvement with local filesystem persist. Key insight: persist blob location
  (local vs S3) determines whether persist operation elimination is impactful.
- Session 19 instrumented create_collections internal breakdown (optimized build):
  storage_collections=24ms (73%), table_register=6.5ms (20%),
  open_data_handles=0.7ms (2%). Key: debug build proportions are misleading
  (table_register appeared as 95% in debug but is only 20% in optimized).
- Full cost breakdowns in ddl-perf-log.md
- Session 13 investigated lazy Transaction::new: found that CREATE TABLE accesses
  both expensive collections (items 16%, storage_collection_metadata 10%) plus
  reads from databases/schemas/roles/introspection_sources for OID allocation.
  Lazy init would save very little for CREATE TABLE specifically.

Profiling notes:
- **Use Prometheus metrics for wall-clock profiling**, not perf/flamegraphs.
  CPU sampling misses async I/O waits which now dominate DDL time. Key metrics:
  `mz_catalog_transact_seconds`, `mz_apply_catalog_implications_seconds`,
  `mz_catalog_allocate_id_seconds`, `mz_catalog_transaction_commit_latency_seconds`,
  `mz_slow_message_handling` (by message_kind), `mz_ts_oracle_seconds` (by op),
  `mz_create_table_collections_seconds` (by step),
  `mz_storage_create_collections_seconds` (by step),
  `mz_initialize_storage_collections_seconds`.
- Capture metrics before/after a DDL batch and compute deltas for accurate
  per-DDL averages. Use `curl -s http://localhost:6878/metrics > /tmp/before.txt`
  then compare after.

Immediate next steps (ranked by Session 19 optimized-build wall-clock profiling):

- **Reduce storage_collections persist ops (~24ms, 73% of create_collections,
  ~25% of DDL).** Three sequential persist operations for every table:
  `upgrade_version` (~8ms), `open_critical_handle` (~8ms),
  `compare_and_downgrade_since` (~8ms). Code path:
  `src/storage-client/src/storage_collections.rs` → `open_since_handle` +
  concurrent block. Options:
  - Eliminate `upgrade_version` for newly-created shards (schema is already correct)
  - Overlap these persist ops with open_data_handles/table_register
  - Defer `compare_and_downgrade_since` to background
  - Batch multiple persist ops into fewer round-trips

- **Reduce table_register latency (~6.5ms, 20% of create_collections).** txn-wal
  registration: `try_register_schema`, `txns_cache.update_ge`, `small_caa`,
  `apply_le`. Code path: `src/storage-controller/src/persist_handles.rs`
  `TxnsTableWorker::register` → `src/txn-wal/src/txns.rs` `Txns::register`.
  Could overlap with post-create_collections work.

- **Merge allocate_id into catalog_transact (~3ms, 3% of DDL).** Saves one
  persist round-trip by allocating IDs within the main transaction instead of a
  separate one.

- **Reduce DurableCatalogState::transaction CPU cost (~12-15ms).** Snapshot clone
  + Transaction::new (proto→Rust conversion). Arc-wrapping Snapshot BTreeMaps
  saves ~2-3ms of clone overhead. Lazy Transaction::new saves little for CREATE
  TABLE (accesses most collections) but helps other DDL types.

Note: `initialize_read_policies` is now only ~0.5ms (down from Session 17's
~10ms estimate). No longer a significant optimization target.
