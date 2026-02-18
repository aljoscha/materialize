// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A gRPC-based timestamp oracle server (`tsoracled`).
//!
//! This server serves timestamps from memory, only persisting to CRDB
//! periodically via a pre-allocation window. This eliminates most CRDB
//! round-trips for read_ts and peek_write_ts operations.

use std::collections::HashMap;
use std::sync::Arc;

use mz_ore::now::NowFn;
use mz_postgres_client::PostgresClient;
use mz_repr::Timestamp;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::GenericNowFn;

use crate::postgres_oracle::PostgresTimestampOracleConfig;
use crate::service::proto_ts_oracle_server::ProtoTsOracle;
use crate::service::{
    ProtoApplyWriteRequest, ProtoEmpty, ProtoGetAllTimelinesRequest, ProtoGetAllTimelinesResponse,
    ProtoOpenTimelineRequest, ProtoOpenTimelineResponse, ProtoTimeline, ProtoTimelineTimestamp,
    ProtoTimestamp, ProtoWriteTimestamp,
};

/// The CRDB schema for the timestamp oracle table.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline text NOT NULL,
    read_ts DECIMAL(20,0) NOT NULL,
    write_ts DECIMAL(20,0) NOT NULL,
    PRIMARY KEY(timeline)
)
";

const CRDB_SCHEMA_OPTIONS: &str = "WITH (sql_stats_automatic_collection_enabled = false)";

const CRDB_CONFIGURE_ZONE: &str =
    "ALTER TABLE timestamp_oracle CONFIGURE ZONE USING gc.ttlseconds = 600;";

/// In-memory state for a single timeline.
struct TimelineInner {
    read_ts: u64,
    write_ts: u64,
    /// The upper bound that has been persisted to CRDB.
    /// Invariant: persisted_upper >= write_ts
    persisted_upper: u64,
    read_only: bool,
}

/// Per-timeline state protected by a mutex.
struct TimelineState {
    inner: Mutex<TimelineInner>,
}

/// The tsoracled gRPC server.
///
/// Manages timestamp state for multiple timelines, serving most operations
/// from memory and only persisting to CRDB when the pre-allocation window
/// is exhausted.
pub struct TsoracledServer {
    timelines: RwLock<HashMap<String, Arc<TimelineState>>>,
    postgres_client: Arc<PostgresClient>,
    now_fn: NowFn,
    window_size: u64,
}

impl std::fmt::Debug for TsoracledServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsoracledServer")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
}

impl TsoracledServer {
    /// Create a new `TsoracledServer`.
    ///
    /// `pg_config` provides the CRDB connection configuration.
    /// `window_size` is the pre-allocation window in timestamp units
    /// (milliseconds for EpochMilliseconds timelines).
    pub async fn new(
        pg_config: PostgresTimestampOracleConfig,
        now_fn: NowFn,
        window_size: u64,
    ) -> Result<Self, anyhow::Error> {
        let postgres_client = PostgresClient::open(pg_config.into())?;

        // Ensure the schema exists.
        let client = postgres_client.get_connection().await?;
        match client
            .batch_execute(&format!(
                "{}{}; {}",
                SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ))
            .await
        {
            Ok(()) => {}
            Err(e) => {
                // Fall back to plain Postgres schema if CRDB options fail.
                let code = e.code();
                if code == Some(&deadpool_postgres::tokio_postgres::error::SqlState::INVALID_PARAMETER_VALUE)
                    || code == Some(&deadpool_postgres::tokio_postgres::error::SqlState::SYNTAX_ERROR)
                {
                    info!("CRDB-specific options not supported, using plain schema");
                    client.batch_execute(SCHEMA).await?;
                } else {
                    return Err(e.into());
                }
            }
        }

        Ok(TsoracledServer {
            timelines: RwLock::new(HashMap::new()),
            postgres_client: Arc::new(postgres_client),
            now_fn,
            window_size,
        })
    }

    /// Open a timeline, reading existing state from CRDB and initializing
    /// the pre-allocation window.
    async fn open_timeline_inner(
        &self,
        timeline: &str,
        initially: u64,
        read_only: bool,
    ) -> Result<(), Status> {
        // Read existing state from CRDB.
        let client = self
            .postgres_client
            .get_connection()
            .await
            .map_err(|e| Status::internal(format!("CRDB connection error: {e}")))?;

        // Ensure the row exists.
        let q = r#"
            INSERT INTO timestamp_oracle (timeline, read_ts, write_ts)
                VALUES ($1, $2, $3)
                ON CONFLICT (timeline) DO NOTHING;
        "#;
        let initially_dec = Self::u64_to_decimal(initially);
        let stmt = client
            .prepare_cached(q)
            .await
            .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
        client
            .execute(&stmt, &[&timeline, &initially_dec, &initially_dec])
            .await
            .map_err(|e| Status::internal(format!("insert error: {e}")))?;

        // Read current state.
        let q = r#"
            SELECT read_ts, write_ts FROM timestamp_oracle WHERE timeline = $1;
        "#;
        let stmt = client
            .prepare_cached(q)
            .await
            .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
        let row = client
            .query_one(&stmt, &[&timeline])
            .await
            .map_err(|e| Status::internal(format!("query error: {e}")))?;

        let crdb_read_ts: mz_pgrepr::Numeric = row.try_get("read_ts").expect("missing read_ts");
        let crdb_write_ts: mz_pgrepr::Numeric =
            row.try_get("write_ts").expect("missing write_ts");
        let crdb_read_ts = Self::decimal_to_u64(crdb_read_ts);
        let crdb_write_ts = Self::decimal_to_u64(crdb_write_ts);

        let read_ts = std::cmp::max(initially, crdb_read_ts);
        let write_ts = std::cmp::max(initially, crdb_write_ts);

        // Apply the initial timestamp (like PostgresTimestampOracle::open does).
        if !read_only {
            let apply_q = r#"
                UPDATE timestamp_oracle
                    SET write_ts = GREATEST(write_ts, $2), read_ts = GREATEST(read_ts, $2)
                    WHERE timeline = $1;
            "#;
            let stmt = client
                .prepare_cached(apply_q)
                .await
                .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
            let initially_dec = Self::u64_to_decimal(initially);
            client
                .execute(&stmt, &[&timeline, &initially_dec])
                .await
                .map_err(|e| Status::internal(format!("apply_write error: {e}")))?;
        }

        let persisted_upper = if read_only {
            // In read-only mode, don't persist anything, just track what CRDB has.
            write_ts
        } else {
            // Persist the pre-allocation window.
            let upper = write_ts.saturating_add(self.window_size);
            let upper_dec = Self::u64_to_decimal(upper);
            let q = r#"
                UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts, $2)
                    WHERE timeline = $1;
            "#;
            let stmt = client
                .prepare_cached(q)
                .await
                .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
            client
                .execute(&stmt, &[&timeline, &upper_dec])
                .await
                .map_err(|e| Status::internal(format!("persist window error: {e}")))?;
            upper
        };

        let state = Arc::new(TimelineState {
            inner: Mutex::new(TimelineInner {
                read_ts,
                write_ts,
                persisted_upper,
                read_only,
            }),
        });

        let mut timelines = self.timelines.write().await;
        timelines.insert(timeline.to_string(), state);

        info!(
            timeline = timeline,
            read_ts = read_ts,
            write_ts = write_ts,
            persisted_upper = persisted_upper,
            read_only = read_only,
            "opened timeline"
        );

        Ok(())
    }

    /// Get the timeline state, returning an error if it hasn't been opened.
    async fn get_timeline(&self, timeline: &str) -> Result<Arc<TimelineState>, Status> {
        let timelines = self.timelines.read().await;
        timelines.get(timeline).cloned().ok_or_else(|| {
            Status::not_found(format!(
                "timeline '{}' not opened; call OpenTimeline first",
                timeline
            ))
        })
    }

    /// Extend the pre-allocation window by persisting a new upper to CRDB.
    async fn extend_window(
        &self,
        timeline: &str,
        new_upper: u64,
    ) -> Result<(), Status> {
        let upper_dec = Self::u64_to_decimal(new_upper);
        let client = self
            .postgres_client
            .get_connection()
            .await
            .map_err(|e| Status::internal(format!("CRDB connection error: {e}")))?;
        let q = r#"
            UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts, $2)
                WHERE timeline = $1;
        "#;
        let stmt = client
            .prepare_cached(q)
            .await
            .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
        client
            .execute(&stmt, &[&timeline, &upper_dec])
            .await
            .map_err(|e| Status::internal(format!("extend window error: {e}")))?;
        Ok(())
    }

    /// Persist `read_ts` to CRDB.
    async fn persist_read_ts(
        &self,
        timeline: &str,
        read_ts: u64,
    ) -> Result<(), Status> {
        let read_ts_dec = Self::u64_to_decimal(read_ts);
        let client = self
            .postgres_client
            .get_connection()
            .await
            .map_err(|e| Status::internal(format!("CRDB connection error: {e}")))?;
        let q = r#"
            UPDATE timestamp_oracle SET read_ts = GREATEST(read_ts, $2)
                WHERE timeline = $1;
        "#;
        let stmt = client
            .prepare_cached(q)
            .await
            .map_err(|e| Status::internal(format!("prepare error: {e}")))?;
        client
            .execute(&stmt, &[&timeline, &read_ts_dec])
            .await
            .map_err(|e| Status::internal(format!("persist read_ts error: {e}")))?;
        Ok(())
    }

    fn u64_to_decimal(ts: u64) -> mz_pgrepr::Numeric {
        let decimal = dec::Decimal::from(Timestamp::from(ts));
        mz_pgrepr::Numeric::from(decimal)
    }

    fn decimal_to_u64(ts: mz_pgrepr::Numeric) -> u64 {
        let timestamp: Timestamp = ts.0 .0.try_into().expect("we only use u64 timestamps");
        u64::from(timestamp)
    }
}

#[tonic::async_trait]
impl ProtoTsOracle for TsoracledServer {
    async fn write_ts(
        &self,
        request: Request<ProtoTimeline>,
    ) -> Result<Response<ProtoWriteTimestamp>, Status> {
        let timeline = request.into_inner().timeline;
        let state = self.get_timeline(&timeline).await?;
        let mut inner = state.inner.lock().await;

        if inner.read_only {
            return Err(Status::failed_precondition(
                "cannot write_ts on read-only timeline",
            ));
        }

        let now: u64 = GenericNowFn::<Timestamp>::now(&self.now_fn).into();
        let proposed = std::cmp::max(inner.write_ts.saturating_add(1), now);

        if proposed >= inner.persisted_upper {
            // Need to extend the window.
            let new_upper = proposed.saturating_add(self.window_size);
            self.extend_window(&timeline, new_upper).await?;
            inner.persisted_upper = new_upper;
        }

        inner.write_ts = proposed;
        let advance_to = proposed.saturating_add(1);

        debug!(
            timeline = timeline,
            write_ts = proposed,
            advance_to = advance_to,
            "write_ts"
        );

        Ok(Response::new(ProtoWriteTimestamp {
            timestamp: proposed,
            advance_to,
        }))
    }

    async fn peek_write_ts(
        &self,
        request: Request<ProtoTimeline>,
    ) -> Result<Response<ProtoTimestamp>, Status> {
        let timeline = request.into_inner().timeline;
        let state = self.get_timeline(&timeline).await?;
        let inner = state.inner.lock().await;
        Ok(Response::new(ProtoTimestamp {
            timestamp: inner.write_ts,
        }))
    }

    async fn read_ts(
        &self,
        request: Request<ProtoTimeline>,
    ) -> Result<Response<ProtoTimestamp>, Status> {
        let timeline = request.into_inner().timeline;
        let state = self.get_timeline(&timeline).await?;
        let inner = state.inner.lock().await;
        Ok(Response::new(ProtoTimestamp {
            timestamp: inner.read_ts,
        }))
    }

    async fn apply_write(
        &self,
        request: Request<ProtoApplyWriteRequest>,
    ) -> Result<Response<ProtoEmpty>, Status> {
        let req = request.into_inner();
        let timeline = req.timeline;
        let lower_bound = req.lower_bound;

        let state = self.get_timeline(&timeline).await?;
        let mut inner = state.inner.lock().await;

        if inner.read_only {
            return Err(Status::failed_precondition(
                "cannot apply_write on read-only timeline",
            ));
        }

        inner.read_ts = std::cmp::max(inner.read_ts, lower_bound);
        inner.write_ts = std::cmp::max(inner.write_ts, lower_bound);

        if inner.write_ts >= inner.persisted_upper {
            let new_upper = inner.write_ts.saturating_add(self.window_size);
            self.extend_window(&timeline, new_upper).await?;
            inner.persisted_upper = new_upper;
        }

        // Also persist the read_ts to CRDB for crash recovery.
        // We do this outside the lock to avoid holding it during I/O,
        // but since apply_write is serialized per-timeline, this is safe.
        let read_ts = inner.read_ts;
        drop(inner);

        self.persist_read_ts(&timeline, read_ts).await?;

        debug!(
            timeline = timeline,
            lower_bound = lower_bound,
            "apply_write"
        );

        Ok(Response::new(ProtoEmpty {}))
    }

    async fn open_timeline(
        &self,
        request: Request<ProtoOpenTimelineRequest>,
    ) -> Result<Response<ProtoOpenTimelineResponse>, Status> {
        let req = request.into_inner();
        self.open_timeline_inner(&req.timeline, req.initially, req.read_only)
            .await?;
        Ok(Response::new(ProtoOpenTimelineResponse {}))
    }

    async fn get_all_timelines(
        &self,
        _request: Request<ProtoGetAllTimelinesRequest>,
    ) -> Result<Response<ProtoGetAllTimelinesResponse>, Status> {
        let timelines = self.timelines.read().await;
        let mut result = Vec::with_capacity(timelines.len());
        for (name, state) in timelines.iter() {
            let inner = state.inner.lock().await;
            let ts = std::cmp::max(inner.read_ts, inner.write_ts);
            result.push(ProtoTimelineTimestamp {
                timeline: name.clone(),
                timestamp: ts,
            });
        }
        Ok(Response::new(ProtoGetAllTimelinesResponse {
            timelines: result,
        }))
    }
}
