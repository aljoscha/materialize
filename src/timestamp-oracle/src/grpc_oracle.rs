// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`TimestampOracle`] client that delegates to a `tsoracled` gRPC service.
//!
//! This client connects to a remote `tsoracled` server and forwards all
//! oracle operations over gRPC. The `read_ts` hot path is kept minimal
//! (no tracing span) matching the pattern in `postgres_oracle.rs`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_repr::Timestamp;
use tonic::transport::Endpoint;
use tracing::{info, warn};

use crate::WriteTimestamp;
use crate::metrics::Metrics;
use crate::service::proto_ts_oracle_client::ProtoTsOracleClient;
use crate::service::{
    ProtoApplyWriteRequest, ProtoGetAllTimelinesRequest, ProtoOpenTimelineRequest, ProtoTimeline,
};

/// Configuration for a gRPC-backed timestamp oracle.
#[derive(Clone, Debug)]
pub struct GrpcTimestampOracleConfig {
    /// URL of the tsoracled gRPC service (e.g., `http://127.0.0.1:6880`).
    pub url: String,
    /// Shared metrics.
    pub metrics: Arc<Metrics>,
}

impl GrpcTimestampOracleConfig {
    /// Create a new `GrpcTimestampOracleConfig`.
    pub fn new(url: &str, metrics_registry: &MetricsRegistry) -> Self {
        GrpcTimestampOracleConfig {
            url: url.to_string(),
            metrics: Arc::new(Metrics::new(metrics_registry)),
        }
    }

    /// Returns the metrics associated with this config.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// A [`TimestampOracle`](crate::TimestampOracle) backed by a gRPC connection
/// to a `tsoracled` server.
#[derive(Debug)]
pub struct GrpcTimestampOracle {
    timeline: String,
    client: ProtoTsOracleClient<tonic::transport::Channel>,
}

impl GrpcTimestampOracle {
    /// Open a new gRPC timestamp oracle for the given timeline.
    ///
    /// Connects to the tsoracled server and opens the timeline.
    pub async fn open(
        config: GrpcTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        _now_fn: NowFn,
        read_only: bool,
    ) -> Self {
        info!(
            url = config.url,
            timeline = timeline,
            initially = ?initially,
            read_only = read_only,
            "opening GrpcTimestampOracle"
        );

        let channel = loop {
            match Endpoint::from_shared(config.url.clone())
                .expect("valid URL")
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .http2_keep_alive_interval(Duration::from_secs(10))
                .connect()
                .await
            {
                Ok(channel) => break channel,
                Err(err) => {
                    warn!("failed to connect to tsoracled at {}, retrying: {err}", config.url);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };

        let mut client = ProtoTsOracleClient::new(channel);

        // Open the timeline on the server. Retry on transient failures.
        loop {
            match client
                .open_timeline(ProtoOpenTimelineRequest {
                    timeline: timeline.clone(),
                    initially: u64::from(initially),
                    read_only,
                })
                .await
            {
                Ok(_) => break,
                Err(err) => {
                    warn!(
                        "failed to open timeline '{}' on tsoracled, retrying: {err}",
                        timeline
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        GrpcTimestampOracle { timeline, client }
    }

    /// Returns all known timelines and their current timestamps from the
    /// tsoracled server.
    pub async fn get_all_timelines(
        config: GrpcTimestampOracleConfig,
    ) -> Result<Vec<(String, Timestamp)>, anyhow::Error> {
        let channel = Endpoint::from_shared(config.url.clone())
            .expect("valid URL")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .connect()
            .await?;

        let mut client = ProtoTsOracleClient::new(channel);
        let response = client
            .get_all_timelines(ProtoGetAllTimelinesRequest {})
            .await?;

        let timelines = response
            .into_inner()
            .timelines
            .into_iter()
            .map(|tt| (tt.timeline, Timestamp::from(tt.timestamp)))
            .collect();

        Ok(timelines)
    }
}

#[async_trait]
impl crate::TimestampOracle<Timestamp> for GrpcTimestampOracle {
    async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
        let mut client = self.client.clone();
        loop {
            match client
                .write_ts(ProtoTimeline {
                    timeline: self.timeline.clone(),
                })
                .await
            {
                Ok(response) => {
                    let wt = response.into_inner();
                    return WriteTimestamp {
                        timestamp: Timestamp::from(wt.timestamp),
                        advance_to: Timestamp::from(wt.advance_to),
                    };
                }
                Err(err) => {
                    warn!("grpc write_ts failed, retrying: {err}");
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        let mut client = self.client.clone();
        loop {
            match client
                .peek_write_ts(ProtoTimeline {
                    timeline: self.timeline.clone(),
                })
                .await
            {
                Ok(response) => {
                    return Timestamp::from(response.into_inner().timestamp);
                }
                Err(err) => {
                    warn!("grpc peek_write_ts failed, retrying: {err}");
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    // Hot path: no tracing span, matching the postgres oracle optimization.
    async fn read_ts(&self) -> Timestamp {
        let mut client = self.client.clone();
        loop {
            match client
                .read_ts(ProtoTimeline {
                    timeline: self.timeline.clone(),
                })
                .await
            {
                Ok(response) => {
                    return Timestamp::from(response.into_inner().timestamp);
                }
                Err(err) => {
                    warn!("grpc read_ts failed, retrying: {err}");
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn apply_write(&self, lower_bound: Timestamp) {
        let mut client = self.client.clone();
        loop {
            match client
                .apply_write(ProtoApplyWriteRequest {
                    timeline: self.timeline.clone(),
                    lower_bound: u64::from(lower_bound),
                })
                .await
            {
                Ok(_) => return,
                Err(err) => {
                    warn!("grpc apply_write failed, retrying: {err}");
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}
