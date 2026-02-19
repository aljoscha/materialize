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

Current status (after Session 11): At ~28k objects (optimized build), DDL
latency is CREATE TABLE ~131ms, CREATE VIEW ~96ms, DROP TABLE ~97ms, DROP VIEW
~86ms. That's down from 444ms/436ms/311ms at the Session 6 baseline (70-78%
improvement for creates, 59-69% for drops).

Completed optimizations (Sessions 7-9, 11):
- Cached Snapshot in PersistHandle (Session 7)
- Lightweight allocate_id bypassing full Transaction (Session 8)
- Persistent CatalogState with imbl::OrdMap + Arc (Session 9) — eliminated the
  ~30% Cow\<CatalogState\> clone+drop cost
- Removed O(n) table advancement loop from group_commit (Session 11) — eliminated
  the 23% cost of iterating all tables on every DDL. Txn-wal protocol already
  handles logical frontier advancement for all registered shards.

Completed diagnostics (Session 10):
- Profiled post-Session 9 optimized build at ~10k objects
- Full cost breakdown in ddl-perf-log.md Session 10

Immediate next steps:

- **Profile post-Session 11 to identify next bottleneck.** The cost breakdown
  has shifted significantly again. Profile to get the new breakdown.

- **Make Transaction::new lazy for catalog_transact (~20-25%).** The main DDL
  transaction still deserializes all 20 collections from proto to Rust types.
  Most DDL operations only access a few collections. Could lazily deserialize
  only accessed collections.

- **Reduce Snapshot clone cost (~20-25%).** The cached Snapshot still needs to be
  fully cloned for each transaction. Could use `Arc<BTreeMap>` for the large
  maps (items, comments) so clone shares data.

- **Reduce apply_updates consolidation cost (~12-15%).** `apply_updates` is
  dominated by sort_unstable on the full StateUpdate trace (O(n log n)).
  Could maintain the trace in sorted order or use incremental consolidation.
