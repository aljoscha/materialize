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

Current status (after Session 13): At ~28k objects (optimized build), DDL
latency is CREATE TABLE ~131ms (Session 11 baseline, native CockroachDB).
Session 13 measured ~169ms with Docker CockroachDB (~30-40ms overhead from
Docker networking). That's down from 444ms at the Session 6 baseline (~70%
cumulative improvement).

Completed optimizations (Sessions 7-9, 11, 13):
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

Completed diagnostics (Sessions 10, 12, 13):
- Profiled post-Session 9 optimized build at ~10k objects (Session 10)
- Profiled post-Session 11 optimized build at ~28k objects (Session 12)
- Full cost breakdowns in ddl-perf-log.md
- Session 13 investigated lazy Transaction::new: found that CREATE TABLE accesses
  both expensive collections (items 16%, storage_collection_metadata 10%) plus
  reads from databases/schemas/roles/introspection_sources for OID allocation.
  Lazy init would save very little for CREATE TABLE specifically.

Immediate next steps (ranked by impact from Session 12 profiling):

- **Make Transaction::new lazy (~26% of catalog_transact, ~20ms).** The main
  DDL transaction calls `TableTransaction::new` for ALL 20 collections,
  converting every entry from proto to Rust types. The `items` collection alone
  costs 16% of catalog_transact. Session 13 found that CREATE TABLE accesses
  both `items` and `storage_collection_metadata` (together 26%), plus reads from
  databases/schemas/roles/introspection_sources for OID allocation. So for
  CREATE TABLE the savings would be limited. However, could still help for
  CREATE VIEW (which may skip `storage_collection_metadata`), ALTER, and other
  DDL types that touch fewer collections. Also saves Transaction drop time (6%).

- **Reduce Snapshot clone cost (~12% of catalog_transact, ~9ms).** The cached
  Snapshot's BTreeMaps (especially `items` at 8% and `storage_collection_metadata`
  at 3%) are fully cloned each transaction. Wrap in `Arc` so clone is O(1) and
  use clone-on-write semantics. If lazy Transaction::new is implemented, the
  Snapshot clone may become partly unnecessary too.

- **Reduce apply_catalog_implications (~13% of catalog_transact, ~10ms).**
  Includes `create_table_collections` and read hold management. May be harder
  to optimize as it's real work for each DDL.

- **Reduce OID allocation cost.** During CREATE TABLE, `allocate_oids` scans
  `databases`, `schemas`, `roles`, `items`, and `introspection_sources` to
  collect all allocated OIDs. This forces initialization of those collections in
  Transaction::new. Maintaining a cached set of allocated OIDs could avoid this
  scan and make more collections eligible for lazy initialization.
