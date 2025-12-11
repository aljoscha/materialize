// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data models for the auto-scaling widget.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use mz_controller_types::ClusterId;
use mz_repr::GlobalId;
use serde::{Deserialize, Serialize};

/// Which replicas a cluster's auto-scaling configuration is allowed to manage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaScope {
    /// Manage all replicas in the cluster.
    All,
    /// Only manage replicas created by the widget (identified by `_auto` suffix).
    ManagedOnly,
}

impl ReplicaScope {
    pub fn from_config(config: &serde_json::Value) -> Result<Self, String> {
        let scope = config
            .get("replica_scope")
            .and_then(|v| v.as_str())
            .unwrap_or("ALL");
        match scope.to_uppercase().as_str() {
            "ALL" => Ok(ReplicaScope::All),
            "MANAGED_ONLY" => Ok(ReplicaScope::ManagedOnly),
            other => Err(format!(
                "invalid replica_scope '{}', expected ALL or MANAGED_ONLY",
                other
            )),
        }
    }

    pub fn is_managed_replica_name(name: &str) -> bool {
        name.ends_with("_auto")
    }

    pub fn ensure_managed_name(name: &str) -> String {
        if Self::is_managed_replica_name(name) {
            name.to_string()
        } else {
            format!("{}_auto", name)
        }
    }
}

/// Specification for a cluster replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSpec {
    /// Name of the replica.
    pub name: String,
    /// Size of the replica (e.g., "200cc").
    pub size: String,
    /// Whether the replica uses disk.
    pub disk: bool,
    /// Whether the replica is internal.
    pub internal: bool,
}

impl ReplicaSpec {
    /// Create a new replica spec with just name and size.
    pub fn new(name: impl Into<String>, size: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            size: size.into(),
            disk: false,
            internal: false,
        }
    }

    /// Generate CREATE CLUSTER REPLICA SQL.
    pub fn to_create_sql(&self, cluster_name: &str) -> String {
        let mut options = vec![format!("SIZE = '{}'", self.size)];

        if self.disk {
            options.push("DISK = true".to_string());
        }
        if self.internal {
            options.push("INTERNAL = true".to_string());
        }
        // Enable introspection so we can see hydration status
        options.push("INTROSPECTION INTERVAL = '1s'".to_string());

        format!(
            "CREATE CLUSTER REPLICA \"{}\".\"{}\" ({})",
            cluster_name,
            self.name,
            options.join(", ")
        )
    }
}

/// Information about a cluster replica from the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInfo {
    /// Name of the replica.
    pub name: String,
    /// Size of the replica.
    pub size: String,
    /// Replica ID.
    pub id: GlobalId,
}

/// Information about a cluster from the catalog.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Cluster ID.
    pub id: ClusterId,
    /// Cluster name.
    pub name: String,
    /// Current replicas in the cluster.
    pub replicas: Vec<ReplicaInfo>,
    /// Whether the cluster is managed.
    pub managed: bool,
}

impl ClusterInfo {
    /// Get a replica by name.
    pub fn get_replica(&self, name: &str) -> Option<&ReplicaInfo> {
        self.replicas.iter().find(|r| r.name == name)
    }

    /// Get replicas with a specific size.
    pub fn get_replicas_by_size(&self, size: &str) -> Vec<&ReplicaInfo> {
        self.replicas.iter().filter(|r| r.size == size).collect()
    }
}

/// State maintained by a strategy between ticks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StrategyState {
    /// Version of the state schema.
    pub state_version: i32,
    /// Strategy-specific payload.
    pub payload: serde_json::Value,
}

impl StrategyState {
    /// Current state version for all strategies.
    pub const CURRENT_VERSION: i32 = 1;

    /// Create a new empty state.
    pub fn new() -> Self {
        Self {
            state_version: Self::CURRENT_VERSION,
            payload: serde_json::json!({}),
        }
    }

    /// Check if the state version is compatible.
    pub fn is_version_compatible(&self) -> bool {
        self.state_version == Self::CURRENT_VERSION
    }
}

/// Configuration for a scaling strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Unique identifier for the strategy (from mz_scaling_strategies.id).
    pub id: String,
    /// Strategy type (e.g., "target_size", "idle_suspend").
    pub strategy_type: String,
    /// Configuration parameters.
    pub config: serde_json::Value,
    /// Whether the strategy is enabled.
    pub enabled: bool,
}

/// Represents the desired state of a cluster as determined by strategies.
#[derive(Debug, Clone)]
pub struct DesiredState {
    /// Cluster ID.
    pub cluster_id: ClusterId,
    /// Cluster name.
    pub cluster_name: String,
    /// Target replicas that should exist.
    pub target_replicas: BTreeMap<String, ReplicaSpec>,
    /// Human-readable reasons for changes.
    pub reasons: Vec<String>,
    /// Per-replica origin strategy ID for replicas present in `target_replicas`.
    pub replica_origins: BTreeMap<String, String>,
    /// Per-replica origin strategy ID for removals.
    pub removal_origins: BTreeMap<String, String>,
}

impl DesiredState {
    /// Create a new desired state initialized with current replicas.
    pub fn from_cluster_info(cluster_info: &ClusterInfo) -> Self {
        Self::from_cluster_info_with_scope(cluster_info, ReplicaScope::All)
    }

    /// Create a new desired state initialized with current replicas, filtered by scope.
    pub fn from_cluster_info_with_scope(cluster_info: &ClusterInfo, scope: ReplicaScope) -> Self {
        let mut target_replicas = BTreeMap::new();
        for replica in &cluster_info.replicas {
            if scope == ReplicaScope::ManagedOnly
                && !ReplicaScope::is_managed_replica_name(&replica.name)
            {
                continue;
            }
            target_replicas.insert(
                replica.name.clone(),
                ReplicaSpec::new(&replica.name, &replica.size),
            );
        }
        Self {
            cluster_id: cluster_info.id,
            cluster_name: cluster_info.name.clone(),
            target_replicas,
            reasons: Vec::new(),
            replica_origins: BTreeMap::new(),
            removal_origins: BTreeMap::new(),
        }
    }

    /// Add a replica to the desired state.
    pub fn add_replica(
        &mut self,
        strategy_id: &str,
        replica: ReplicaSpec,
        reason: impl Into<String>,
    ) {
        let reason_str = reason.into();
        if !reason_str.is_empty() {
            self.reasons
                .push(format!("Adding replica {}: {}", replica.name, reason_str));
        }
        self.replica_origins
            .insert(replica.name.clone(), strategy_id.to_string());
        self.removal_origins.remove(&replica.name);
        self.target_replicas.insert(replica.name.clone(), replica);
    }

    /// Remove a replica from the desired state.
    pub fn remove_replica(
        &mut self,
        strategy_id: &str,
        replica_name: &str,
        reason: impl Into<String>,
    ) {
        if self.target_replicas.remove(replica_name).is_some() {
            let reason_str = reason.into();
            if !reason_str.is_empty() {
                self.reasons
                    .push(format!("Removing replica {}: {}", replica_name, reason_str));
            }
            self.removal_origins
                .insert(replica_name.to_string(), strategy_id.to_string());
            self.replica_origins.remove(replica_name);
        }
    }

    /// Get all replica names in the desired state.
    pub fn get_replica_names(&self) -> std::collections::BTreeSet<String> {
        self.target_replicas.keys().cloned().collect()
    }
}

/// An action to be taken on a cluster.
#[derive(Debug, Clone)]
pub struct ScalingAction {
    /// Cluster ID affected by this action.
    pub cluster_id: ClusterId,
    /// Strategy ID that produced this action.
    pub strategy_id: String,
    /// The DDL SQL to execute.
    pub sql: String,
    /// Action type for logging/metrics.
    pub action_type: ScalingActionType,
    /// Human-readable reasons for this action.
    pub reasons: Vec<String>,
}

/// Types of scaling actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingActionType {
    /// Create a new replica.
    CreateReplica,
    /// Drop an existing replica.
    DropReplica,
    /// Resize an existing replica (implemented as drop+create).
    ResizeReplica,
}

impl ScalingActionType {
    /// Get string representation for storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScalingActionType::CreateReplica => "create_replica",
            ScalingActionType::DropReplica => "drop_replica",
            ScalingActionType::ResizeReplica => "resize_replica",
        }
    }
}

/// Signals/metrics used by strategies to make decisions.
#[derive(Debug, Clone)]
pub struct Signals {
    /// Cluster ID.
    pub cluster_id: ClusterId,
    /// Last activity timestamp (None if no activity recorded).
    pub last_activity_ts: Option<DateTime<Utc>>,
    /// Per-replica hydration status (replica_name -> is_hydrated).
    pub hydration_status: BTreeMap<String, HydrationStatus>,
    /// Per-replica crash information.
    pub crash_info: BTreeMap<String, CrashInfo>,
}

/// Hydration status for a replica.
#[derive(Debug, Clone, Default)]
pub struct HydrationStatus {
    /// Total number of objects that need hydration.
    pub total_objects: u64,
    /// Number of objects that are hydrated.
    pub hydrated_objects: u64,
}

impl HydrationStatus {
    /// Whether the replica is fully hydrated.
    pub fn is_hydrated(&self) -> bool {
        // If there are no objects, consider it hydrated
        self.total_objects == 0 || self.hydrated_objects >= self.total_objects
    }
}

/// Crash information for a replica.
#[derive(Debug, Clone, Default)]
pub struct CrashInfo {
    /// Total number of crashes in the lookback window.
    pub total_crashes: u64,
    /// Number of OOM crashes.
    pub oom_count: u64,
    /// Latest crash time.
    pub latest_crash_time: Option<DateTime<Utc>>,
}

impl Signals {
    /// Create new signals for a cluster.
    pub fn new(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            last_activity_ts: None,
            hydration_status: BTreeMap::new(),
            crash_info: BTreeMap::new(),
        }
    }

    /// Seconds since last activity, or None if no activity recorded.
    #[allow(clippy::as_conversions)] // i64 to f64 conversion is safe for milliseconds
    pub fn seconds_since_activity(&self, now: DateTime<Utc>) -> Option<f64> {
        self.last_activity_ts
            .map(|ts| (now - ts).num_milliseconds() as f64 / 1000.0)
    }

    /// Whether the cluster is fully hydrated (all replicas are hydrated).
    pub fn is_cluster_hydrated(&self) -> bool {
        if self.hydration_status.is_empty() {
            return true;
        }
        self.hydration_status.values().all(|s| s.is_hydrated())
    }

    /// Whether a specific replica is hydrated.
    pub fn is_replica_hydrated(&self, replica_name: &str) -> bool {
        self.hydration_status
            .get(replica_name)
            .map(|s| s.is_hydrated())
            .unwrap_or(false) // If no status, assume not hydrated
    }

    /// Whether a replica is experiencing OOM loops.
    pub fn is_replica_oom_looping(&self, replica_name: &str, min_oom_count: u64) -> bool {
        self.crash_info
            .get(replica_name)
            .map(|info| info.oom_count >= min_oom_count)
            .unwrap_or(false)
    }

    /// Whether a replica is experiencing crash loops.
    pub fn is_replica_crash_looping(&self, replica_name: &str, min_crash_count: u64) -> bool {
        self.crash_info
            .get(replica_name)
            .map(|info| info.total_crashes >= min_crash_count)
            .unwrap_or(false)
    }
}
