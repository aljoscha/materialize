// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Signal collection for auto-scaling decisions.
//!
//! This module provides functionality to collect signals from system tables
//! that inform scaling decisions, including activity, hydration status, and
//! crash information.
//!
//! Signals are collected from:
//! - `mz_statement_execution_history_redacted` - for activity (last query timestamp)
//! - `mz_hydration_statuses` - for hydration status per replica
//! - `mz_cluster_replica_status_history` - for crash/OOM information

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use mz_controller_types::ClusterId;
use mz_repr::Datum;
use tracing::{debug, warn};

use super::models::{
    ClusterInfo, CrashInfo, HydrationStatus, Signals, StrategyConfig, StrategyState,
};
use crate::widget::{WidgetContext, WidgetError};

/// Collects signals from catalog and system state.
///
/// This struct is created by the coordinator and passed to the widget
/// to provide read-only access to cluster state.
#[derive(Debug, Clone, Default)]
pub struct CatalogSnapshot {
    /// Information about all clusters.
    pub clusters: BTreeMap<ClusterId, ClusterInfo>,
    /// Signals for each cluster.
    pub signals: BTreeMap<ClusterId, Signals>,
    /// Strategy configurations per cluster.
    pub strategy_configs: BTreeMap<ClusterId, Vec<StrategyConfig>>,
    /// Strategy state keyed by (strategy_id, cluster_id).
    pub strategy_states: BTreeMap<(String, ClusterId), StrategyState>,
    /// Available replica sizes.
    pub available_sizes: Vec<String>,
    /// Current timestamp.
    pub now: DateTime<Utc>,
}

impl CatalogSnapshot {
    /// Create a new empty snapshot.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            clusters: BTreeMap::new(),
            signals: BTreeMap::new(),
            strategy_configs: BTreeMap::new(),
            strategy_states: BTreeMap::new(),
            available_sizes: Vec::new(),
            now,
        }
    }

    /// Add a cluster to the snapshot.
    pub fn add_cluster(&mut self, info: ClusterInfo) {
        let cluster_id = info.id;
        self.clusters.insert(cluster_id, info);
        // Initialize empty signals if not present
        self.signals
            .entry(cluster_id)
            .or_insert_with(|| Signals::new(cluster_id));
    }

    /// Get cluster info by ID.
    pub fn get_cluster(&self, cluster_id: &ClusterId) -> Option<&ClusterInfo> {
        self.clusters.get(cluster_id)
    }

    /// Get signals for a cluster.
    pub fn get_signals(&self, cluster_id: &ClusterId) -> Option<&Signals> {
        self.signals.get(cluster_id)
    }

    /// Get mutable signals for a cluster.
    pub fn get_signals_mut(&mut self, cluster_id: &ClusterId) -> Option<&mut Signals> {
        self.signals.get_mut(cluster_id)
    }

    /// Set hydration status for a replica.
    pub fn set_hydration_status(
        &mut self,
        cluster_id: ClusterId,
        replica_name: String,
        status: HydrationStatus,
    ) {
        if let Some(signals) = self.signals.get_mut(&cluster_id) {
            signals.hydration_status.insert(replica_name, status);
        }
    }

    /// Set last activity timestamp for a cluster.
    pub fn set_last_activity(&mut self, cluster_id: ClusterId, ts: DateTime<Utc>) {
        if let Some(signals) = self.signals.get_mut(&cluster_id) {
            signals.last_activity_ts = Some(ts);
        }
    }

    /// Add crash info for a replica.
    pub fn add_crash_info(&mut self, cluster_id: ClusterId, replica_name: String, info: CrashInfo) {
        if let Some(signals) = self.signals.get_mut(&cluster_id) {
            signals.crash_info.insert(replica_name, info);
        }
    }
}

/// Collector for signals from system tables.
///
/// This reads from builtin tables to gather activity, hydration, and crash
/// information for scaling decisions.
pub struct SignalCollector;

impl SignalCollector {
    /// Collect all signals for the given clusters.
    ///
    /// This reads from multiple system tables to gather:
    /// - Last activity timestamp per cluster
    /// - Hydration status per replica
    /// - Crash information per replica
    pub async fn collect_signals(
        ctx: &WidgetContext,
        snapshot: &mut CatalogSnapshot,
        lookback_hours: i64,
    ) -> Result<(), WidgetError> {
        // Collect activity signals
        if let Err(e) = Self::collect_activity_signals(ctx, snapshot).await {
            warn!(error = %e, "Failed to collect activity signals");
        }

        // Collect hydration signals
        if let Err(e) = Self::collect_hydration_signals(ctx, snapshot).await {
            warn!(error = %e, "Failed to collect hydration signals");
        }

        // Collect crash signals
        if let Err(e) = Self::collect_crash_signals(ctx, snapshot, lookback_hours).await {
            warn!(error = %e, "Failed to collect crash signals");
        }

        Ok(())
    }

    /// Collect activity signals (last query timestamp) for clusters.
    ///
    /// Reads from `mz_statement_execution_history` and finds the maximum
    /// `finished_at` timestamp for each cluster.
    async fn collect_activity_signals(
        ctx: &WidgetContext,
        snapshot: &mut CatalogSnapshot,
    ) -> Result<(), WidgetError> {
        let sql = "\
SELECT cluster_id, max(finished_at) \
FROM mz_internal.mz_statement_execution_history_redacted \
WHERE cluster_id IS NOT NULL AND finished_at IS NOT NULL \
GROUP BY cluster_id";

        let rows = ctx.execute_sql(sql).await?;
        debug!(num_rows = rows.len(), "Read activity aggregates for widget");

        let mut updated_clusters = 0usize;
        for row in rows {
            let datums: Vec<Datum> = row.iter().collect();
            let cluster_id_str = match datums.get(0) {
                Some(Datum::String(s)) => *s,
                _ => continue,
            };
            let finished_at = match datums.get(1) {
                Some(Datum::TimestampTz(ts)) => ts.to_naive().and_utc(),
                _ => continue,
            };

            let cluster_id = match ClusterId::from_str(cluster_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };

            snapshot.set_last_activity(cluster_id, finished_at);
            updated_clusters += 1;
            debug!(
                cluster_id = %cluster_id,
                last_activity_ts = %finished_at,
                "Collected activity signal"
            );
        }

        debug!(
            updated_clusters,
            total_tracked_clusters = snapshot.clusters.len(),
            "Finished collecting activity signals"
        );
        Ok(())
    }

    /// Collect hydration signals for replicas.
    ///
    /// Reads from `mz_hydration_statuses` and aggregates hydration status
    /// per replica. A replica is considered hydrated if all its objects
    /// are hydrated.
    async fn collect_hydration_signals(
        ctx: &WidgetContext,
        snapshot: &mut CatalogSnapshot,
    ) -> Result<(), WidgetError> {
        // IMPORTANT: We must count objects that are missing from
        // `mz_internal.mz_hydration_statuses` as *not hydrated*. To get that
        // LEFT JOIN semantics we enumerate the objects expected to hydrate per
        // cluster, cross them with replicas of that cluster, and then left join
        // to hydration statuses on (replica_id, object_id).
        let sql = "\
WITH objects AS (
    SELECT id AS object_id, cluster_id
    FROM mz_catalog.mz_indexes
    UNION ALL
    SELECT id AS object_id, cluster_id
    FROM mz_catalog.mz_materialized_views
    UNION ALL
    SELECT id AS object_id, cluster_id
    FROM mz_catalog.mz_sources
    WHERE cluster_id IS NOT NULL AND type != 'webhook'
    UNION ALL
    SELECT id AS object_id, cluster_id
    FROM mz_catalog.mz_sinks
    WHERE cluster_id IS NOT NULL
)
SELECT r.cluster_id, r.name, count(*)::bigint,
       sum(CASE WHEN COALESCE(h.hydrated, false) THEN 1 ELSE 0 END)::bigint
FROM mz_catalog.mz_cluster_replicas r
JOIN objects o ON o.cluster_id = r.cluster_id
LEFT JOIN mz_internal.mz_hydration_statuses h
    ON h.replica_id = r.id AND h.object_id = o.object_id
GROUP BY r.cluster_id, r.name";

        let rows = ctx.execute_sql(sql).await?;
        debug!(
            num_rows = rows.len(),
            "Read hydration aggregates for widget"
        );

        let mut updated_replicas = 0usize;
        for row in rows {
            let datums: Vec<Datum> = row.iter().collect();
            let cluster_id_str = match datums.get(0) {
                Some(Datum::String(s)) => *s,
                _ => continue,
            };
            let replica_name = match datums.get(1) {
                Some(Datum::String(s)) => (*s).to_string(),
                _ => continue,
            };
            let total_objects = match datums.get(2) {
                Some(Datum::Int64(v)) => u64::try_from(*v).unwrap_or(0),
                _ => continue,
            };
            let hydrated_objects = match datums.get(3) {
                Some(Datum::Int64(v)) => u64::try_from(*v).unwrap_or(0),
                _ => continue,
            };

            let cluster_id = match ClusterId::from_str(cluster_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };
            if !snapshot.clusters.contains_key(&cluster_id) {
                continue;
            }

            debug!(
                cluster_id = %cluster_id,
                replica_name = %replica_name,
                total_objects,
                hydrated_objects,
                is_hydrated = (total_objects == 0 || hydrated_objects >= total_objects),
                "Collected hydration signal"
            );
            updated_replicas += 1;
            snapshot.set_hydration_status(
                cluster_id,
                replica_name,
                HydrationStatus {
                    total_objects,
                    hydrated_objects,
                },
            );
        }

        // For replicas with no hydration entries, they are considered hydrated
        // (no objects to hydrate)
        for (cluster_id, cluster_info) in &snapshot.clusters {
            for replica in &cluster_info.replicas {
                if let Some(signals) = snapshot.signals.get_mut(cluster_id) {
                    if !signals.hydration_status.contains_key(&replica.name) {
                        signals.hydration_status.insert(
                            replica.name.clone(),
                            HydrationStatus {
                                total_objects: 0,
                                hydrated_objects: 0,
                            },
                        );
                    }
                }
            }
        }

        debug!(
            updated_replicas,
            total_tracked_clusters = snapshot.clusters.len(),
            "Finished collecting hydration signals"
        );
        for (cluster_id, cluster_info) in &snapshot.clusters {
            let Some(signals) = snapshot.signals.get(cluster_id) else {
                continue;
            };
            let hydrated = signals
                .hydration_status
                .values()
                .filter(|s| s.is_hydrated())
                .count();
            debug!(
                cluster_id = %cluster_id,
                cluster_name = %cluster_info.name,
                replicas_total = cluster_info.replicas.len(),
                replicas_with_status = signals.hydration_status.len(),
                replicas_hydrated = hydrated,
                cluster_hydrated = signals.is_cluster_hydrated(),
                "Hydration summary"
            );
        }

        Ok(())
    }

    /// Collect crash signals for replicas.
    ///
    /// Reads from `mz_cluster_replica_status_history` and counts offline
    /// events within the lookback window.
    async fn collect_crash_signals(
        ctx: &WidgetContext,
        snapshot: &mut CatalogSnapshot,
        lookback_hours: i64,
    ) -> Result<(), WidgetError> {
        let sql = format!(
            "\
SELECT r.cluster_id, r.name, count(*)::bigint, \
       sum(CASE WHEN lower(coalesce(h.reason, '')) LIKE '%oom%' THEN 1 ELSE 0 END)::bigint, \
       max(h.occurred_at) \
FROM mz_internal.mz_cluster_replica_status_history h \
JOIN mz_catalog.mz_cluster_replicas r ON r.id = h.replica_id \
WHERE h.process_id = 0 \
  AND h.status = 'offline' \
  AND h.occurred_at >= now() - interval '{} hours' \
GROUP BY r.cluster_id, r.name",
            lookback_hours
        );

        let rows = ctx.execute_sql(&sql).await?;
        debug!(
            num_rows = rows.len(),
            lookback_hours, "Read crash aggregates for widget"
        );

        let mut updated_replicas = 0usize;
        for row in rows {
            let datums: Vec<Datum> = row.iter().collect();
            let cluster_id_str = match datums.get(0) {
                Some(Datum::String(s)) => *s,
                _ => continue,
            };
            let replica_name = match datums.get(1) {
                Some(Datum::String(s)) => (*s).to_string(),
                _ => continue,
            };
            let total_crashes = match datums.get(2) {
                Some(Datum::Int64(v)) => u64::try_from(*v).unwrap_or(0),
                _ => continue,
            };
            let oom_count = match datums.get(3) {
                Some(Datum::Int64(v)) => u64::try_from(*v).unwrap_or(0),
                _ => continue,
            };
            let latest_crash_time = match datums.get(4) {
                Some(Datum::TimestampTz(ts)) => Some(ts.to_naive().and_utc()),
                _ => None,
            };

            let cluster_id = match ClusterId::from_str(cluster_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };
            if !snapshot.clusters.contains_key(&cluster_id) {
                continue;
            }

            debug!(
                cluster_id = %cluster_id,
                replica_name = %replica_name,
                total_crashes,
                oom_count,
                latest_crash_time = ?latest_crash_time,
                "Collected crash signal"
            );
            updated_replicas += 1;
            snapshot.add_crash_info(
                cluster_id,
                replica_name,
                CrashInfo {
                    total_crashes,
                    oom_count,
                    latest_crash_time,
                },
            );
        }

        debug!(
            updated_replicas,
            total_tracked_clusters = snapshot.clusters.len(),
            "Finished collecting crash signals"
        );
        Ok(())
    }

    /// Collect cluster information from the catalog.
    ///
    /// This iterates over all clusters in the catalog and builds ClusterInfo
    /// structs for each one.
    pub fn collect_cluster_info<F, I>(iter_clusters: F) -> Vec<ClusterInfo>
    where
        F: FnOnce() -> I,
        I: Iterator<Item = ClusterInfo>,
    {
        iter_clusters().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::widget::auto_scaling::models::ReplicaInfo;
    use mz_repr::GlobalId;

    #[test]
    fn test_catalog_snapshot() {
        let now = Utc::now();
        let mut snapshot = CatalogSnapshot::new(now);

        let cluster_id = ClusterId::User(1);
        let cluster_info = ClusterInfo {
            id: cluster_id,
            name: "test_cluster".to_string(),
            replicas: vec![ReplicaInfo {
                name: "r1".to_string(),
                size: "100cc".to_string(),
                id: GlobalId::User(1),
            }],
            managed: false,
        };

        snapshot.add_cluster(cluster_info);

        assert!(snapshot.get_cluster(&cluster_id).is_some());
        assert!(snapshot.get_signals(&cluster_id).is_some());

        // Set hydration status
        snapshot.set_hydration_status(
            cluster_id,
            "r1".to_string(),
            HydrationStatus {
                total_objects: 10,
                hydrated_objects: 10,
            },
        );

        let signals = snapshot.get_signals(&cluster_id).unwrap();
        assert!(signals.is_replica_hydrated("r1"));
    }

    #[test]
    fn test_catalog_snapshot_activity() {
        let now = Utc::now();
        let mut snapshot = CatalogSnapshot::new(now);

        let cluster_id = ClusterId::User(1);
        let cluster_info = ClusterInfo {
            id: cluster_id,
            name: "test_cluster".to_string(),
            replicas: vec![],
            managed: false,
        };

        snapshot.add_cluster(cluster_info);

        // Set last activity
        let activity_ts = now - chrono::Duration::minutes(5);
        snapshot.set_last_activity(cluster_id, activity_ts);

        let signals = snapshot.get_signals(&cluster_id).unwrap();
        assert_eq!(signals.last_activity_ts, Some(activity_ts));

        // Check seconds since activity
        let seconds = signals.seconds_since_activity(now).unwrap();
        assert!((seconds - 300.0).abs() < 1.0); // ~5 minutes
    }

    #[test]
    fn test_catalog_snapshot_crash_info() {
        let now = Utc::now();
        let mut snapshot = CatalogSnapshot::new(now);

        let cluster_id = ClusterId::User(1);
        let cluster_info = ClusterInfo {
            id: cluster_id,
            name: "test_cluster".to_string(),
            replicas: vec![ReplicaInfo {
                name: "r1".to_string(),
                size: "100cc".to_string(),
                id: GlobalId::User(1),
            }],
            managed: false,
        };

        snapshot.add_cluster(cluster_info);

        // Add crash info
        snapshot.add_crash_info(
            cluster_id,
            "r1".to_string(),
            CrashInfo {
                total_crashes: 3,
                oom_count: 2,
                latest_crash_time: Some(now - chrono::Duration::minutes(10)),
            },
        );

        let signals = snapshot.get_signals(&cluster_id).unwrap();
        assert!(signals.is_replica_oom_looping("r1", 2));
        assert!(signals.is_replica_crash_looping("r1", 3));
    }
}
