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

Current status (after Session 8): At 50k objects (optimized build), DDL latency
is CREATE TABLE ~320ms, CREATE VIEW ~295ms, DROP TABLE ~282ms. That's down from
444ms/436ms/311ms at the Session 6 baseline (28-32% improvement for creates).

Immediate next steps (handoff):

- **Eliminate Cow\<CatalogState\> clone+drop (~30% of DDL time).** This is the
  largest remaining O(n) cost. `transact_inner` in
  `src/adapter/src/catalog/transact.rs` calls `Cow::to_mut()` which deep-clones
  the entire CatalogState (50k+ entries in `entry_by_id` BTreeMap). Options:
  - Use `Arc` for the large maps so clone is O(1)
  - Use persistent/immutable data structures (e.g., `im` crate)
  - Restructure `transact_inner` for in-place mutation with rollback
  Profile first to confirm this is still the dominant cost after Session 8.

- **Make Transaction::new lazy for catalog_transact (~8%).** The main DDL
  transaction still deserializes all 20 collections from proto to Rust types.
  Most DDL operations only access a few collections. Could lazily deserialize
  only accessed collections.

- **Reduce Snapshot clone cost (~12%).** The cached Snapshot still needs to be
  fully cloned for each transaction. Could use `Arc<BTreeMap>` for the large
  maps (items, comments) so clone shares data.
