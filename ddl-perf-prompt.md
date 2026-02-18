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
anymore in our next sessions. Update the prompt in a separate jj change with a
good description.

Immediate next steps (handoff):

- **Use --optimized after all.** If we think that debug build might be to
  different to an optimized build, we might have to update our instructions to
  use --optimized builds and continue with that. The consistency check thing
  was already a red herring.

- **Reproduce the problem.** Start environmentd with `--reset`, create
  increasing numbers of objects (tables with default indexes, views, etc.) and
  measure how long DDL statements take at each scale. Find the threshold where
  DDL becomes meaningfully slow (e.g., >1s per statement). Log the baseline
  numbers. Try different DDL types: CREATE TABLE, CREATE VIEW, CREATE INDEX,
  DROP, ALTER. Some may degrade faster than others.

- **Profile the slow path.** Once we have a reproduction at a scale where DDL is
  slow, use `perf record` to capture a flamegraph during a batch of DDL
  statements. Identify which functions dominate. Look at coordinator message
  handling, catalog operations, persist operations, compute/storage controller
  interactions, and any O(n) or O(n^2) loops over existing objects.
