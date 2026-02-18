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
//! are published via shared atomics and a `Notify` broadcast.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::pin;
use tokio::sync::Notify;

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

/// Shared state between the worker task and callers.
///
/// The worker writes `latest_ts` first (Release), then `completed_up_to`
/// (Release). Callers read `completed_up_to` first (Acquire), then
/// `latest_ts` (Acquire). The Release on `completed_up_to` ensures that
/// any thread observing the new `completed_up_to` also sees the preceding
/// `latest_ts` write. This is guaranteed by the Release/Acquire pairing
/// on `completed_up_to`.
struct SharedState {
    /// All tickets with number < `completed_up_to` have been served.
    completed_up_to: AtomicU64,
    /// The timestamp returned by the backing oracle for the most recent batch.
    latest_ts: AtomicU64,
    /// Notifies all waiting callers when a batch completes.
    caller_notify: Notify,
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
///    to `MAX_IN_FLIGHT_READ_TS_BATCHES` in-flight batches, and publishes the
///    result via shared atomics + `Notify`.
/// 3. Callers check the atomic `completed_up_to` and wait on the `Notify` if
///    their ticket hasn't been served yet.
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
    shared: Arc<SharedState>,
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
        let shared = Arc::new(SharedState {
            completed_up_to: AtomicU64::new(0),
            latest_ts: AtomicU64::new(0),
            caller_notify: Notify::new(),
        });

        let task_oracle = Arc::clone(&oracle);
        let task_next_ticket = Arc::clone(&next_ticket);
        let task_notify = Arc::clone(&worker_notify);
        let task_shared = Arc::clone(&shared);

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
                        if target > completed_up_to {
                            completed_up_to = target;
                            // Publish result via atomics. Write `latest_ts`
                            // first (Release), then `completed_up_to`
                            // (Release). The Release on `completed_up_to`
                            // ensures callers who Acquire-load it also see
                            // the `latest_ts` write.
                            task_shared.latest_ts.store(ts.into(), Ordering::Release);
                            task_shared
                                .completed_up_to
                                .store(completed_up_to, Ordering::Release);
                            task_shared.caller_notify.notify_waiters();
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
            shared,
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
        let my_ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.worker_notify.notify_one();

        // Fast path: check if already completed before registering for notification.
        if self.shared.completed_up_to.load(Ordering::Acquire) > my_ticket {
            return T::from(self.shared.latest_ts.load(Ordering::Acquire));
        }

        loop {
            // Register for notification BEFORE re-checking the condition.
            // This ensures we don't miss a notify_waiters() that fires
            // between our check and our sleep.
            let notified = self.shared.caller_notify.notified();
            pin!(notified);
            notified.as_mut().enable();

            // Re-check after registration to avoid missed-wakeup race.
            if self.shared.completed_up_to.load(Ordering::Acquire) > my_ticket {
                return T::from(self.shared.latest_ts.load(Ordering::Acquire));
            }

            notified.await;
        }
    }

    async fn apply_write(&self, write_ts: T) {
        self.inner.apply_write(write_ts).await
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
