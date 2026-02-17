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
oracle from the frontend.

The oracle read_ts bottleneck has been resolved: Session 46 added a shared
atomic `peek_read_ts_fast()` that bypasses the CRDB round-trip for 99.97%+ of
queries. The read_ts cost is now ~0.2µs (was 374-673µs). The bottleneck has
shifted to TCP I/O, tokio scheduling, and per-query adapter overhead.

Below here, I have some immediate next steps to explore. Once you feel you have
resolved one of them, please update this prompt so that we don't consider them
anymore in our next sessions. Update the prompt in a separate jj change with a
good description.

Immediate next steps (handoff):
 - Remaining Histogram::observe calls (~802 samples/query at 64c): one_query
   timing and record_time_to_first_row are the main remaining callers.
   Consider local accumulation or batched flushes.
 - BTreeMap traversals (~374 samples/query total at 64c): oracle lookup,
   collection state, etc. Consider caching more lookups on PeekClient.
 - Row encoding / BackendMessage send overhead (Codec::encode ~361/q,
   send/send_all ~500/q at 64c). Investigate vectored writes or pre-encoded
   responses.
 - Allocator contention (malloc 609/q + sdallocx 335/q = 944/q at 64c).
   Consider arena allocation for single-row queries.
 - get_cached_parsed_stmt HashMap overhead (~400/q at 64c). Consider
   pointer-equality fast path or per-connection LRU cache.
 - Scaling plateau at 64c→128c (~307K→306K QPS). Remaining gap is TCP
   socket contention or tokio worker saturation.
