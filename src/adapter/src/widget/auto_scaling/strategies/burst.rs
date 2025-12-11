// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Burst scaling strategy.
//!
//! This strategy adds a large "burst" replica during hydration to speed up
//! the process, then removes it when other replicas are hydrated.

use chrono::{DateTime, Utc};
use tracing::{debug, info};

use super::{ScalingStrategy, StrategyDecision, StrategyPriority};
use crate::widget::auto_scaling::models::{
    ClusterInfo, DesiredState, ReplicaScope, ReplicaSpec, Signals, StrategyState,
};
use crate::widget::auto_scaling::signals::CatalogSnapshot;

/// Burst strategy configuration.
#[derive(Debug, Clone)]
pub struct BurstConfig {
    /// Size of the burst replica (e.g., "3200cc").
    pub burst_size: String,
    /// Cooldown period in seconds.
    pub cooldown_s: i64,
    /// Which replicas are managed by the widget.
    pub replica_scope: ReplicaScope,
}

impl BurstConfig {
    /// Parse configuration from JSON.
    pub fn from_json(config: &serde_json::Value) -> Result<Self, String> {
        let replica_scope = ReplicaScope::from_config(config)?;

        let burst_size = config
            .get("burst_size")
            .and_then(|v| v.as_str())
            .ok_or("Missing required config key: burst_size")?
            .to_string();

        if burst_size.is_empty() {
            return Err("burst_size must be a non-empty string".to_string());
        }

        let cooldown_s = config
            .get("cooldown_s")
            .and_then(|v| v.as_i64())
            .unwrap_or(60);

        Ok(Self {
            burst_size,
            cooldown_s,
            replica_scope,
        })
    }
}

/// Burst strategy implementation.
///
/// This strategy:
/// 1. Creates a large "burst" replica when no replicas are hydrated
/// 2. Drops the burst replica when any other replica becomes hydrated
/// 3. Respects cooldown periods to avoid thrashing
pub struct BurstStrategy;

impl ScalingStrategy for BurstStrategy {
    fn name(&self) -> &'static str {
        "burst"
    }

    fn priority(&self) -> StrategyPriority {
        StrategyPriority::Burst
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), String> {
        BurstConfig::from_json(config)?;
        Ok(())
    }

    fn decide(
        &self,
        strategy_id: &str,
        current_state: &StrategyState,
        config: &serde_json::Value,
        signals: &Signals,
        cluster_info: &ClusterInfo,
        _catalog_snapshot: &CatalogSnapshot,
        current_desired_state: Option<DesiredState>,
        now: DateTime<Utc>,
    ) -> Result<StrategyDecision, String> {
        let config = BurstConfig::from_json(config)?;

        // Initialize desired state from previous strategy or current replicas
        let mut desired =
            current_desired_state.unwrap_or_else(|| DesiredState::from_cluster_info(cluster_info));

        // Check cooldown
        if self.check_cooldown(
            current_state,
            &serde_json::json!({"cooldown_s": config.cooldown_s}),
            now,
        ) {
            debug!(
                cluster_id = %cluster_info.id,
                strategy = "burst",
                "Skipping decision due to cooldown"
            );
            return Ok(StrategyDecision {
                desired_state: desired,
                next_state: current_state.clone(),
                changed: false,
            });
        }

        let burst_replica_name = match config.replica_scope {
            ReplicaScope::All => format!("{}_burst", cluster_info.name),
            ReplicaScope::ManagedOnly => {
                ReplicaScope::ensure_managed_name(&format!("{}_burst", cluster_info.name))
            }
        };
        let has_burst_replica = cluster_info
            .replicas
            .iter()
            .any(|r| r.name == burst_replica_name);

        // Check if any non-burst replicas are hydrated (using desired state)
        let other_replicas_hydrated = desired
            .target_replicas
            .keys()
            .filter(|name| *name != &burst_replica_name)
            .any(|name| signals.is_replica_hydrated(name));

        // Check if any non-burst replicas exist (using desired state)
        let has_other_replicas = desired
            .target_replicas
            .keys()
            .any(|name| name != &burst_replica_name);

        // Check if there's already a desired replica with the same size as the burst replica
        let has_replica_with_burst_size = desired
            .target_replicas
            .values()
            .any(|spec| spec.size == config.burst_size);

        let mut new_payload = current_state.payload.clone();
        let mut changed = false;

        if has_other_replicas
            && !other_replicas_hydrated
            && !has_burst_replica
            && !has_replica_with_burst_size
        {
            // Add burst replica when other replicas exist but none are hydrated
            let burst_spec = ReplicaSpec::new(&burst_replica_name, &config.burst_size);
            desired.add_replica(
                strategy_id,
                burst_spec,
                "Creating burst replica - no hydrated or burst-sized replicas",
            );
            changed = true;

            info!(
                cluster_id = %cluster_info.id,
                burst_size = %config.burst_size,
                other_replicas_count = desired.target_replicas.len() - 1,
                "Adding burst replica to desired state"
            );
        } else if has_burst_replica && other_replicas_hydrated {
            // Remove burst replica when other replicas become hydrated
            desired.remove_replica(
                strategy_id,
                &burst_replica_name,
                "Dropping burst replica - other replicas are now hydrated",
            );
            changed = true;

            info!(
                cluster_id = %cluster_info.id,
                "Removing burst replica from desired state - other replicas hydrated"
            );
        }

        // Update state based on changes
        if changed {
            new_payload["last_decision_ts"] = serde_json::json!(now.to_rfc3339());
        }

        let next_state = StrategyState {
            state_version: StrategyState::CURRENT_VERSION,
            payload: new_payload,
        };

        Ok(StrategyDecision {
            desired_state: desired,
            next_state,
            changed,
        })
    }

    fn initial_state(&self) -> StrategyState {
        StrategyState {
            state_version: StrategyState::CURRENT_VERSION,
            payload: serde_json::json!({
                "last_decision_ts": null,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::widget::auto_scaling::models::{HydrationStatus, ReplicaInfo};
    use mz_controller_types::ClusterId;
    use mz_repr::GlobalId;

    fn make_cluster_info(name: &str, replicas: Vec<(&str, &str)>) -> ClusterInfo {
        ClusterInfo {
            id: ClusterId::User(1),
            name: name.to_string(),
            replicas: replicas
                .into_iter()
                .enumerate()
                .map(|(i, (name, size))| ReplicaInfo {
                    name: name.to_string(),
                    size: size.to_string(),
                    id: GlobalId::User(i as u64),
                })
                .collect(),
            managed: false,
        }
    }

    fn make_signals(cluster_id: ClusterId, hydrated_replicas: Vec<&str>) -> Signals {
        let mut signals = Signals::new(cluster_id);
        for name in hydrated_replicas {
            signals.hydration_status.insert(
                name.to_string(),
                HydrationStatus {
                    total_objects: 10,
                    hydrated_objects: 10,
                },
            );
        }
        signals
    }

    #[mz_ore::test]
    fn test_burst_creates_replica_when_not_hydrated() {
        let strategy = BurstStrategy;
        let config = serde_json::json!({
            "burst_size": "3200cc"
        });

        // Cluster has a replica that is not hydrated
        let cluster_info = make_cluster_info("test_cluster", vec![("main", "200cc")]);
        let signals = make_signals(ClusterId::User(1), vec![]); // Not hydrated
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(Utc::now());

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                Utc::now(),
            )
            .unwrap();

        assert!(result.changed);
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_burst")
        );
        assert_eq!(
            result.desired_state.target_replicas["test_cluster_burst"].size,
            "3200cc"
        );
    }

    #[mz_ore::test]
    fn test_burst_drops_replica_when_hydrated() {
        let strategy = BurstStrategy;
        let config = serde_json::json!({
            "burst_size": "3200cc"
        });

        // Cluster has burst replica and main replica that is now hydrated
        let cluster_info = make_cluster_info(
            "test_cluster",
            vec![("main", "200cc"), ("test_cluster_burst", "3200cc")],
        );
        let signals = make_signals(ClusterId::User(1), vec!["main"]); // main is hydrated
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(Utc::now());

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                Utc::now(),
            )
            .unwrap();

        assert!(result.changed);
        assert!(result.desired_state.target_replicas.contains_key("main"));
        assert!(
            !result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_burst")
        );
    }

    #[mz_ore::test]
    fn test_burst_no_change_when_no_replicas() {
        let strategy = BurstStrategy;
        let config = serde_json::json!({
            "burst_size": "3200cc"
        });

        // Cluster has no replicas - burst shouldn't create anything
        let cluster_info = make_cluster_info("test_cluster", vec![]);
        let signals = make_signals(ClusterId::User(1), vec![]);
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(Utc::now());

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                Utc::now(),
            )
            .unwrap();

        assert!(!result.changed);
        assert!(result.desired_state.target_replicas.is_empty());
    }

    #[mz_ore::test]
    fn test_burst_no_change_when_already_hydrated() {
        let strategy = BurstStrategy;
        let config = serde_json::json!({
            "burst_size": "3200cc"
        });

        // Cluster has a replica that is already hydrated - no burst needed
        let cluster_info = make_cluster_info("test_cluster", vec![("main", "200cc")]);
        let signals = make_signals(ClusterId::User(1), vec!["main"]); // Already hydrated
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(Utc::now());

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                Utc::now(),
            )
            .unwrap();

        assert!(!result.changed);
        assert!(result.desired_state.target_replicas.contains_key("main"));
        assert!(
            !result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_burst")
        );
    }
}
