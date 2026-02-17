// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle that wraps a `TimestampOracle` and batches calls
//! to it.
//!
//! Uses a ticketed approach: callers take a monotonically increasing ticket
//! number (atomic fetch_add), and a worker task batches oracle calls. Results
//! are broadcast via a `tokio::sync::watch` channel. This eliminates per-call
//! `oneshot::channel()` allocations and `mpsc` sends from the hot path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use mz_ore::metrics::Histogram;
use tokio::pin;
use tokio::sync::{Notify, watch};
use tracing::debug;

use crate::metrics::Metrics;
use crate::{TimestampOracle, WriteTimestamp};

/// Maximum number of backing-oracle `read_ts` calls we allow in flight at once.
///
/// Allowing a small amount of parallelism reduces queueing delay under load
/// while still keeping pressure on the backing oracle bounded.
const MAX_IN_FLIGHT_READ_TS_BATCHES: usize = 3;
const MAX_IN_FLIGHT_READ_TS_BATCHES_ENV: &str = "MZ_TS_ORACLE_MAX_IN_FLIGHT_READ_TS_BATCHES";

fn max_in_flight_read_ts_batches() -> usize {
    match std::env::var(MAX_IN_FLIGHT_READ_TS_BATCHES_ENV) {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(0) => {
                tracing::warn!(
                    env = %MAX_IN_FLIGHT_READ_TS_BATCHES_ENV,
                    value = %raw,
                    fallback = MAX_IN_FLIGHT_READ_TS_BATCHES,
                    "max in-flight read_ts batches must be >= 1; using fallback"
                );
                MAX_IN_FLIGHT_READ_TS_BATCHES
            }
            Ok(parsed) => parsed,
            Err(_) => {
                tracing::warn!(
                    env = %MAX_IN_FLIGHT_READ_TS_BATCHES_ENV,
                    value = %raw,
                    fallback = MAX_IN_FLIGHT_READ_TS_BATCHES,
                    "failed to parse max in-flight read_ts batches; using fallback"
                );
                MAX_IN_FLIGHT_READ_TS_BATCHES
            }
        },
        Err(_) => MAX_IN_FLIGHT_READ_TS_BATCHES,
    }
}

/// The state broadcast by the worker task to waiting callers.
///
/// `completed_up_to` means all tickets with number < `completed_up_to` have
/// been served. `ts` is the timestamp returned by the backing oracle for the
/// most recent batch.
#[derive(Clone, Copy, Debug)]
struct BatchResult {
    completed_up_to: u64,
    ts: u64,
}

/// A batching [`TimestampOracle`] backed by a [`TimestampOracle`]
///
/// This will only batch calls to `read_ts` because the rest of the system
/// already naturally does batching of write-related calls via the group commit
/// mechanism. Write-related calls are passed straight through to the backing
/// oracle.
///
/// For `read_ts` calls, we have to be careful to never cache results from the
/// backing oracle: for the timestamp to be linearized we can never return a
/// result as of an earlier moment, but batching them up is correct because this
/// can only make it so that we return later timestamps. Those later timestamps
/// still fall within the duration of the `read_ts` call and so are linearized.
///
/// The ticketed approach works as follows:
/// 1. Each caller atomically increments `next_ticket` to take a ticket number.
/// 2. The worker task snapshots `next_ticket`, calls `inner.read_ts()` in up
///    to `MAX_IN_FLIGHT_READ_TS_BATCHES` in-flight batches, and broadcasts the
///    highest completed snapshot via a `watch` channel.
/// 3. Callers subscribe to the watch and wait until `completed_up_to > my_ticket`.
///
/// Correctness: the worker snapshots `next_ticket = N` after tickets 0..N-1
/// were taken (i.e., after those callers arrived). The oracle call happens after
/// the snapshot, so the returned timestamp is within the real-time bounds of
/// every caller with ticket < N. Even with multiple in-flight batches,
/// `completed_up_to` only advances monotonically, so callers only ever observe a
/// result from a snapshot taken after they arrived. This is the same argument as
/// the channel-based batching — we never cache or reuse a timestamp from before
/// a caller arrived.
pub struct BatchingTimestampOracle<T> {
    inner: Arc<dyn TimestampOracle<T> + Send + Sync>,
    next_ticket: Arc<AtomicU64>,
    worker_notify: Arc<Notify>,
    result_tx: watch::Sender<BatchResult>,
    read_ts_wait_seconds: Histogram,
    /// Shared atomic holding the most recently observed `read_ts` value.
    ///
    /// Updated by the worker after each backing oracle `read_ts` batch and by
    /// `apply_write` before delegating to the inner oracle (to ensure reads
    /// after writes see at least the write's timestamp).
    ///
    /// A value of 0 means "uninitialized" — `peek_read_ts_fast` returns `None`.
    shared_read_ts: Arc<AtomicU64>,
}

impl<T> std::fmt::Debug for BatchingTimestampOracle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchingTimestampOracle").finish()
    }
}

impl<T> BatchingTimestampOracle<T>
where
    T: Into<u64> + From<u64> + Copy + Clone + Send + Sync + 'static,
{
    /// Creates a [`BatchingTimestampOracle`] that uses the given inner oracle.
    pub fn new(metrics: Arc<Metrics>, oracle: Arc<dyn TimestampOracle<T> + Send + Sync>) -> Self {
        let max_in_flight = max_in_flight_read_ts_batches();
        let next_ticket = Arc::new(AtomicU64::new(0));
        let worker_notify = Arc::new(Notify::new());
        let (result_tx, _) = watch::channel(BatchResult {
            completed_up_to: 0,
            ts: 0,
        });
        let read_ts_wait_seconds = metrics.batching.read_ts.wait_seconds.clone();
        let shared_read_ts = Arc::new(AtomicU64::new(0));

        let task_oracle = Arc::clone(&oracle);
        let task_next_ticket = Arc::clone(&next_ticket);
        let task_notify = Arc::clone(&worker_notify);
        let task_result_tx = result_tx.clone();
        let task_shared_read_ts = Arc::clone(&shared_read_ts);

        mz_ore::task::spawn(|| "BatchingTimestampOracle Worker Task", async move {
            let read_ts_metrics = &metrics.batching.read_ts;
            let mut completed_up_to = 0u64;
            let mut scheduled_up_to = 0u64;
            let mut in_flight = FuturesUnordered::new();

            loop {
                while in_flight.len() < max_in_flight {
                    let target = task_next_ticket.load(Ordering::Acquire);
                    if target <= scheduled_up_to {
                        break;
                    }

                    read_ts_metrics.ops_count.inc_by(target - scheduled_up_to);
                    read_ts_metrics.batches_count.inc();

                    let batch_oracle = Arc::clone(&task_oracle);
                    in_flight.push(async move {
                        let inner_start = Instant::now();
                        let ts = batch_oracle.read_ts().await;
                        (target, ts, inner_start.elapsed().as_secs_f64())
                    });
                    scheduled_up_to = target;
                }

                if in_flight.is_empty() {
                    // Register for wake-up BEFORE checking for pending tickets.
                    // This ensures we don't miss a notify_one() that arrives
                    // between our check and our sleep.
                    let notified = task_notify.notified();
                    pin!(notified);
                    notified.as_mut().enable();
                    if task_next_ticket.load(Ordering::Acquire) <= scheduled_up_to {
                        notified.await;
                    }
                    continue;
                }

                tokio::select! {
                    // A batch finished; update metrics and publish the highest
                    // completed ticket boundary seen so far.
                    completed = in_flight.next() => {
                        let Some((target, ts, inner_seconds)) = completed else {
                            continue;
                        };
                        read_ts_metrics.inner_read_seconds.observe(inner_seconds);
                        let ts_u64: u64 = ts.into();
                        // Update the shared atomic with the latest timestamp.
                        // fetch_max ensures monotonically non-decreasing even if
                        // batches complete out of order.
                        task_shared_read_ts.fetch_max(ts_u64, Ordering::Release);
                        if target > completed_up_to {
                            completed_up_to = target;
                            // Broadcast result. It's okay if there are no receivers.
                            let _ = task_result_tx.send(BatchResult {
                                completed_up_to,
                                ts: ts_u64,
                            });
                        }
                    }
                    // New callers arrived; loop around so we can schedule
                    // additional batches (if below the in-flight limit).
                    _ = task_notify.notified(), if in_flight.len() < max_in_flight => {}
                }
            }
        });

        Self {
            inner: oracle,
            next_ticket,
            worker_notify,
            result_tx,
            read_ts_wait_seconds,
            shared_read_ts,
        }
    }
}

#[async_trait]
impl<T> TimestampOracle<T> for BatchingTimestampOracle<T>
where
    T: Into<u64> + From<u64> + Copy + Send + Sync,
{
    async fn write_ts(&self) -> WriteTimestamp<T> {
        self.inner.write_ts().await
    }

    async fn peek_write_ts(&self) -> T {
        self.inner.peek_write_ts().await
    }

    async fn read_ts(&self) -> T {
        let wait_start = Instant::now();
        let my_ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.worker_notify.notify_one();

        // Subscribe to the watch channel and wait for our ticket to be served.
        let mut rx = self.result_tx.subscribe();
        loop {
            {
                let state = rx.borrow_and_update();
                if state.completed_up_to > my_ticket {
                    self.read_ts_wait_seconds
                        .observe(wait_start.elapsed().as_secs_f64());
                    return T::from(state.ts);
                }
            }
            rx.changed()
                .await
                .expect("worker task cannot stop while there are outstanding callers");
        }
    }

    async fn apply_write(&self, write_ts: T) {
        // Update the shared atomic BEFORE delegating to the inner oracle.
        // This ensures that any `peek_read_ts_fast()` call that starts after
        // this `apply_write()` returns will see at least `write_ts`, preserving
        // causal ordering: reads after writes see at least the write's timestamp.
        self.shared_read_ts
            .fetch_max(write_ts.into(), Ordering::Release);
        self.inner.apply_write(write_ts).await
    }

    async fn peek_read_ts_fast(&self) -> Option<T> {
        let val = self.shared_read_ts.load(Ordering::Acquire);
        if val == 0 {
            // Uninitialized — no read_ts has been observed yet.
            debug!("peek_read_ts_fast: uninitialized, falling back to read_ts");
            None
        } else {
            Some(T::from(val))
        }
    }
}

#[cfg(test)]
mod tests {

    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::Timestamp;
    use tracing::info;

    use crate::postgres_oracle::{PostgresTimestampOracle, PostgresTimestampOracleConfig};

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_batching_timestamp_oracle() -> Result<(), anyhow::Error> {
        let config = match PostgresTimestampOracleConfig::new_for_test() {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresTimestampOracleConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));

        crate::tests::timestamp_oracle_impl_test(|timeline, now_fn, initial_ts| {
            // We use the postgres oracle as the backing oracle.
            let pg_oracle = PostgresTimestampOracle::open(
                config.clone(),
                timeline,
                initial_ts,
                now_fn,
                false, /* read-only */
            );

            async {
                let arced_pg_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(pg_oracle.await);

                let batching_oracle =
                    BatchingTimestampOracle::new(Arc::clone(&metrics), arced_pg_oracle);

                let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(batching_oracle);

                arced_oracle
            }
        })
        .await?;

        Ok(())
    }
}
