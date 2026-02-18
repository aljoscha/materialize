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

**Critical context from Sessions 46-47:** The `peek_read_ts_fast` optimization
(Session 42) was removed for correctness reasons. The system is I/O-bound on
CRDB round-trips (~362µs per `read_ts` call). CPU-side optimizations (spin
polling, parallelism tuning, metrics/tracing stripping) have no measurable
effect on throughput. The true baseline is ~2,500 QPS at 1c and ~103K at 64c.

**Completed (remove from consideration):**
- ~~Break down oracle read_ts latency~~: Done in Session 47. CRDB query_one
  dominates at >95%. get_connection ~4µs, prepare_cached ~0.5µs.
- ~~CPU-side oracle optimizations~~: Exhaustively tested (yield_now spin,
  max_in_flight tuning, metrics/tracing stripping). All neutral or regressive.

**Remaining directions:**

- **Explore correctness-preserving oracle call batching/sharing.** The batching
  oracle already coalesces calls from the backing store side (8.67x ratio at
  64c), but the CRDB round-trip is irreducible. Could we reduce the number of
  CRDB calls further? The key correctness constraint is strict serializability:
  each query must get a timestamp >= the latest write_ts at the time the query
  arrived. Read GUIDE.md carefully before attempting any caching. The batching
  oracle's ticketed design is correct; the question is whether CRDB can be
  called less often while still respecting real-time bounds.

- **Benchmark Serializable vs StrictSerializable.** Serializable queries skip
  the oracle entirely and use the write frontier. This would confirm the oracle
  is truly the bottleneck and establish an upper bound for non-oracle
  optimization.

- **Reduce CRDB round-trip latency.** The ~362µs per CRDB call is the
  bottleneck. Options: colocate the timestamp oracle in-process (avoid network
  hop), use a lighter backing store, optimize the CRDB query, or use CRDB
  follower reads with bounded staleness (correctness implications — see
  GUIDE.md).

- **Fix statement_logging crash.** `end_statement_execution` panics when
  frontend peek sequencing is active with statement logging enabled. The
  coordinator's `executions_begun` map doesn't have the UUID. Workaround:
  `statement_logging_max_sample_rate=0`.
