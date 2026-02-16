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
 - Run all dbbench runs with fixed pool settings to avoid churn artifacts:
   `-max-active-conns N -max-idle-conns N -force-pre-auth` (with `N=concurrency`).
 - Sanity-check each run for churn:
   `command-startup ~= N`, `command-terminate ~= N`, and `mz_append_table_duration_seconds_count` / `apply_write` should stay near the read-ts tick rate (not spike).
 - Run a concurrency sweep on the bigger machine (`1,4,16,64,128` and optionally `256`) and capture:
   `mz_frontend_peek_seconds{kind="try_frontend_peek_cached"}`,
   `mz_frontend_peek_read_ts_seconds`,
   `mz_ts_oracle_batch_wait_seconds{op="read_ts"}`,
   `mz_ts_oracle_batch_inner_read_seconds{op="read_ts"}`,
   `mz_ts_oracle_batched_op_count{op="read_ts"}`,
   `mz_ts_oracle_batches_count{op="read_ts"}`.
 - Use the new phase histograms to break down postgres-backed `read_ts`:
   `mz_ts_oracle_postgres_read_ts_phase_seconds{phase="get_connection|prepare_cached|query_one"}`.
   We already observed that 64->128 inner-read growth is almost entirely in `phase="query_one"`.
 - For each case, reset CRDB SQL stats before the run (`SELECT crdb_internal.reset_sql_stats();`) and compare CRDB statement stats for
   `SELECT read_ts FROM timestamp_oracle WHERE timeline = $1` (`svcLat`, `runLat`, `cpuSQLNanos`, `contentionTime`)
   against the `query_one` phase metric. This separates DB server time from envd/runtime overhead.
 - Check CPU/runtime pressure on the bigger machine using Tokio runtime metrics:
   `mz_tokio_worker_total_busy_duration{runtime="main"}`,
   `mz_tokio_budget_forced_yield_count{runtime="main"}`,
   `mz_tokio_worker_poll_count{runtime="main"}`.
   Goal: determine if we are scheduler/CPU bound vs DB-latency bound.
 - Decision after data:
   if `query_one` rises mainly because CRDB `svcLat` rises, focus on CRDB-side causes (storage/compaction/hot-row effects);
   if `query_one` rises much more than CRDB `svcLat`, focus on envd/runtime scheduling and oracle call path isolation.
