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

Current status (after Session 15): At ~36.5k objects (optimized build, Docker
CockroachDB), DDL latency is CREATE TABLE median ~134ms, catalog_transact avg
97.7ms. That's down from 444ms at the Session 6 baseline (~70% cumulative
improvement, or ~75% accounting for ~30-40ms Docker CockroachDB overhead).

Completed optimizations (Sessions 7-9, 11, 13, 15):
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

Completed diagnostics (Sessions 10, 12, 13, 14):
- Profiled post-Session 9 optimized build at ~10k objects (Session 10)
- Profiled post-Session 11 optimized build at ~28k objects (Session 12)
- Profiled post-Session 13 optimized build at ~36.5k objects (Session 14)
- Full cost breakdowns in ddl-perf-log.md
- Session 13 investigated lazy Transaction::new: found that CREATE TABLE accesses
  both expensive collections (items 16%, storage_collection_metadata 10%) plus
  reads from databases/schemas/roles/introspection_sources for OID allocation.
  Lazy init would save very little for CREATE TABLE specifically.

Profiling notes: `perf record` works on this machine using the older perf binary
at `/usr/lib/linux-tools/6.8.0-100-generic/perf` after setting
`perf_event_paranoid` to 1. Use `rustfilt` for symbol demangling.

Immediate next steps (ranked by estimated impact from Session 14 profiling,
with consolidation now eliminated):

- **Cache validate_resource_limits counts (~7.8% of total, ~16ms).** This
  function calls 7+ `user_*()` methods (user_tables, user_sources, etc.), each
  iterating ALL entries in the imbl::OrdMap with `is_*` type checks. Combined
  `is_*` self-time is ~4.5%. Maintaining a `ResourceCounts` struct in
  CatalogState, updated incrementally during catalog transactions, would
  eliminate these scans entirely.

- **Arc-wrap Snapshot BTreeMaps (~5.7% of total, ~12ms).** The durable catalog's
  `Snapshot` (21 BTreeMaps) is fully cloned for each transaction. Wrapping in
  `Arc` makes clone O(1) with copy-on-write semantics in `apply_update`.

- **Lazy Transaction::new (~14% of total, ~29ms, but limited savings for CREATE
  TABLE).** Items deserialization alone is 9.5%. CREATE TABLE accesses both
  `items` and `storage_collection_metadata` plus OID allocation collections,
  limiting savings to ~2% from skipping the other 18 collections. More
  beneficial for other DDL types.

- **Optimize ReadHold::try_downgrade (~5.5% of total, ~11ms).** Part of
  `apply_catalog_implications` (6.7% total). Iterates a BTreeMap of all storage
  collection read holds. May be batchable or optimizable.
