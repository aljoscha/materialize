I'm working on a testbed for testing how far we can push throughput with our
pgwire implementation and adapter frontend. We have a log file in
@pgwire-server-perf-log.md where we keep recording findings for future
sessions.

Figure out how we can improve latency and throughput. For this, we need to use
sampling to figure out where our time in processing these simple queries is
going and fix it.

We want to try one optimization, then note down our results in the log file and
then stop. I'll then start a new session to try and tackle the next thing. We
don't want to add optimizations that cache the result of a query and then
return that straightaway, we want to truly improve the whole processing
pipeline, remove bottlenecks.

After we tried an optimization and noted down results in our log, add a good
description to the jj change and start a new one with jj new for the next
session.

Don't run tests, right now this will only slow us down. We're happy as long as
we can run our queries.

We have dbbench at ~/dbbench/dbbench, and you can use this script for testing:
duration=20s

[64 connections]
query=select * from t
concurrency=64

We are currently benchmarking bin/environmentd --optimized.

When benchmarking, it's important to first login as mz_system and enable
frontend peek sequencing via `alter system set
enable_frontend_peek_sequencing=true;`.

Please focus on where we spend time in try_frontend_peek_inner, or rather
try_frontend_peek_cached, which is really hit in our benchmark. It looks like
the time spent in that function goes up as we increase the number of concurrent
clients. Run benchmarks and profiling to figure out why that time goes up as
concurrency of clients goes up.

You can look at prometheus metrics for environmentd at
http://localhost:6878/metrics. Interesting metrics for us are
mz_frontend_peek_seconds, with the try_frontend_peek_cached label. This covers
the runtime of try_frontend_peek_cached mentioned above. These are histograms,
so you have to look at the one with the _bucket suffix and figure out the
histogram by yourself. Another important metric is
mz_frontend_peek_read_ts_seconds, which covers the read_ts calls we make to the
oracle from the frontend. Looks like this is the main culprit right now for why
QPS doesn't scale and latency goes up as we add more concurrent clients.

We recently changed the batching timestamp oracle to a different
implementation, but looks like that hasn't resolved the scaling issues. Looks
like oracle calls to the backing crdb oracle are actually going down, with
higher concurrency, but still latency seems to go up. You can use the
mz_ts_oracle_* metrics, with the read_ts label to look into how the timestamp
oracle on the backend is doing. Maybe you can spot something there.

Below here, I have some immediate next steps to explore. Once you feel you have
resolved on of them, please update this prompt so that we don't consider them
anymore in our next sessions. Update the prompt in a separate jj change with a
good description.

Immediate next steps (handoff):

**Critical context from Session 46:** The `peek_read_ts_fast` optimization
(Session 42) was removed for correctness reasons. Without it, `oracle.read_ts()`
is called on every StrictSerializable query and dominates latency at ~483µs
(~97% of per-query time). The true baseline is ~2,500 QPS at 1c and ~103K at
64c. BTreeMap lookup caching was attempted and found to save only ~2-3µs per
query — negligible. Any future optimization must reduce oracle call frequency
or latency.

- **Explore correctness-preserving oracle call batching/sharing.** The batching
  oracle already batches calls from the backing store side, but each frontend
  query still does its own `read_ts().await` through the batching channel. Could
  we coalesce multiple frontend queries into a single oracle round-trip? The key
  correctness constraint is that each query must get a timestamp >= the latest
  write_ts at the time the query arrived (strict serializability). A shared
  "current read_ts" that is refreshed periodically (e.g., every write_ts bump)
  might work if queries only use it when it's known to be >= their arrival time.
  Read GUIDE.md carefully before attempting this.

- **Benchmark Serializable vs StrictSerializable.** Serializable queries skip
  the oracle entirely and use the write frontier. This would confirm the oracle
  is truly the bottleneck and establish an upper bound for optimization.

- **Break down oracle read_ts latency.** Use phase histograms
  (`mz_ts_oracle_postgres_read_ts_phase_seconds`) to identify which part of
  the oracle round-trip is slowest: connection acquisition, prepared statement,
  or CRDB query execution. Compare against CRDB's own statement stats.

- **Fix statement_logging crash.** `end_statement_execution` panics when
  frontend peek sequencing is active with statement logging enabled. The
  coordinator's `executions_begun` map doesn't have the UUID. Workaround:
  `statement_logging_max_sample_rate=0`.
