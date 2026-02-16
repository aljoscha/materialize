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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::pin;
use tokio::sync::{watch, Notify};

use crate::metrics::Metrics;
use crate::{TimestampOracle, WriteTimestamp};

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
/// 2. The worker task snapshots `next_ticket`, calls `inner.read_ts()`, and
///    broadcasts the result via a `watch` channel.
/// 3. Callers subscribe to the watch and wait until `completed_up_to > my_ticket`.
///
/// Correctness: the worker snapshots `next_ticket = N` after tickets 0..N-1
/// were taken (i.e., after those callers arrived). The oracle call happens after
/// the snapshot, so the returned timestamp is within the real-time bounds of
/// every caller with ticket < N. Callers with ticket >= N wait for the next
/// batch. This is the same argument as the channel-based batching — we never
/// cache or reuse a timestamp from before a caller arrived.
pub struct BatchingTimestampOracle<T> {
    inner: Arc<dyn TimestampOracle<T> + Send + Sync>,
    next_ticket: Arc<AtomicU64>,
    worker_notify: Arc<Notify>,
    result_tx: watch::Sender<BatchResult>,
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
        let next_ticket = Arc::new(AtomicU64::new(0));
        let worker_notify = Arc::new(Notify::new());
        let (result_tx, _) = watch::channel(BatchResult {
            completed_up_to: 0,
            ts: 0,
        });

        let task_oracle = Arc::clone(&oracle);
        let task_next_ticket = Arc::clone(&next_ticket);
        let task_notify = Arc::clone(&worker_notify);
        let task_result_tx = result_tx.clone();

        mz_ore::task::spawn(|| "BatchingTimestampOracle Worker Task", async move {
            let read_ts_metrics = &metrics.batching.read_ts;
            let mut completed_up_to = 0u64;

            loop {
                // Register for wake-up BEFORE checking for pending tickets.
                // This ensures we don't miss a notify_one() that arrives
                // between our check and our sleep.
                let notified = task_notify.notified();
                pin!(notified);
                notified.as_mut().enable();

                // Inner loop: process batches as long as there are pending
                // tickets.
                loop {
                    let target = task_next_ticket.load(Ordering::Acquire);
                    if target <= completed_up_to {
                        break;
                    }

                    read_ts_metrics
                        .ops_count
                        .inc_by(target - completed_up_to);
                    read_ts_metrics.batches_count.inc();

                    let ts = task_oracle.read_ts().await;
                    completed_up_to = target;

                    // Broadcast result. It's okay if there are no receivers.
                    let _ = task_result_tx.send(BatchResult {
                        completed_up_to,
                        ts: ts.into(),
                    });
                }

                // Sleep until a caller takes a new ticket and notifies us.
                notified.await;
            }
        });

        Self {
            inner: oracle,
            next_ticket,
            worker_notify,
            result_tx,
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

        // Subscribe to the watch channel and wait for our ticket to be served.
        let mut rx = self.result_tx.subscribe();
        loop {
            {
                let state = rx.borrow_and_update();
                if state.completed_up_to > my_ticket {
                    return T::from(state.ts);
                }
            }
            rx.changed()
                .await
                .expect("worker task cannot stop while there are outstanding callers");
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
