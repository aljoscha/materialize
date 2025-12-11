// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Target size scaling strategy.
//!
//! This strategy ensures a cluster has exactly one replica of a specified target size.
//! If the target size replica doesn't exist, it creates one.
//! If other size replicas exist when the target size replica is hydrated, it drops them.

use chrono::{DateTime, Utc};
use tracing::{debug, info};

use super::{ScalingStrategy, StrategyDecision, StrategyPriority};
use crate::widget::auto_scaling::models::{
    ClusterInfo, DesiredState, ReplicaScope, ReplicaSpec, Signals, StrategyState,
};
use crate::widget::auto_scaling::signals::CatalogSnapshot;

/// Target size strategy configuration.
#[derive(Debug, Clone)]
pub struct TargetSizeConfig {
    /// Target replica size (e.g., "200cc").
    pub target_size: String,
    /// Name for the target size replica.
    pub replica_name: String,
    /// Cooldown period in seconds.
    pub cooldown_s: i64,
    /// Which replicas are managed by the widget.
    pub replica_scope: ReplicaScope,
}

impl TargetSizeConfig {
    /// Parse configuration from JSON.
    pub fn from_json(config: &serde_json::Value) -> Result<Self, String> {
        let replica_scope = ReplicaScope::from_config(config)?;

        let target_size = config
            .get("size")
            .and_then(|v| v.as_str())
            .ok_or("Missing required config key: size")?
            .to_string();

        if target_size.is_empty() {
            return Err("target_size must be a non-empty string".to_string());
        }

        let replica_name = config
            .get("replica_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("r_{}", target_size));
        let replica_name = match replica_scope {
            ReplicaScope::All => replica_name,
            ReplicaScope::ManagedOnly => ReplicaScope::ensure_managed_name(&replica_name),
        };

        let cooldown_s = config
            .get("cooldown_s")
            .and_then(|v| v.as_i64())
            .unwrap_or(60);

        Ok(Self {
            target_size,
            replica_name,
            cooldown_s,
            replica_scope,
        })
    }
}

/// Target size strategy implementation.
///
/// This strategy:
/// 1. Ensures a cluster has a replica of a specific target size
/// 2. Creates a target size replica if it doesn't exist
/// 3. Drops other size replicas when the target size replica is hydrated
pub struct TargetSizeStrategy;

impl ScalingStrategy for TargetSizeStrategy {
    fn name(&self) -> &'static str {
        "target_size"
    }

    fn priority(&self) -> StrategyPriority {
        StrategyPriority::TargetSize
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), String> {
        TargetSizeConfig::from_json(config)?;
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
        let config = TargetSizeConfig::from_json(config)?;

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
                strategy = "target_size",
                "Skipping decision due to cooldown"
            );
            return Ok(StrategyDecision {
                desired_state: desired,
                next_state: current_state.clone(),
                changed: false,
            });
        }

        // Find current replicas by size
        let target_size_replicas: Vec<_> = cluster_info
            .replicas
            .iter()
            .filter(|r| r.size == config.target_size)
            .collect();

        let other_size_replicas: Vec<_> = cluster_info
            .replicas
            .iter()
            .filter(|r| r.size != config.target_size)
            .collect();

        // Track pending replica from state
        let pending_target_replica = current_state
            .payload
            .get("pending_target_replica")
            .and_then(|v| v.as_object());

        debug!(
            cluster_id = %cluster_info.id,
            target_size = %config.target_size,
            replica_name = %config.replica_name,
            target_size_replicas = target_size_replicas.len(),
            other_size_replicas = other_size_replicas.len(),
            pending_target_replica = ?pending_target_replica,
            is_hydrated = signals.is_cluster_hydrated(),
            "Evaluating target size requirements"
        );

        let mut new_payload = current_state.payload.clone();
        let mut changed = false;

        // Case 1: No target size replica exists and none is pending
        if target_size_replicas.is_empty() && pending_target_replica.is_none() {
            let replica_spec = ReplicaSpec::new(&config.replica_name, &config.target_size);
            desired.add_replica(
                strategy_id,
                replica_spec,
                format!("Creating target size replica ({})", config.target_size),
            );
            changed = true;

            info!(
                cluster_id = %cluster_info.id,
                target_size = %config.target_size,
                replica_name = %config.replica_name,
                "Adding target size replica to desired state"
            );
        }
        // Case 2: Target size replica exists and is hydrated, drop other replicas
        else if !target_size_replicas.is_empty() && !other_size_replicas.is_empty() {
            // Check if any target size replica is hydrated
            let target_replica_hydrated = target_size_replicas
                .iter()
                .any(|r| signals.is_replica_hydrated(&r.name));

            if target_replica_hydrated {
                for replica in &other_size_replicas {
                    desired.remove_replica(
                        strategy_id,
                        &replica.name,
                        format!(
                            "Dropping non-target size replica ({}) - target size replica is hydrated",
                            replica.size
                        ),
                    );
                    changed = true;
                }

                info!(
                    cluster_id = %cluster_info.id,
                    target_size = %config.target_size,
                    replicas_to_drop = other_size_replicas.len(),
                    "Removing non-target size replicas from desired state"
                );
            }
        }
        // Case 3: We have a pending target replica, check if it now exists
        else if pending_target_replica.is_some() && !target_size_replicas.is_empty() {
            // The pending replica now exists, clear the pending state
            info!(
                cluster_id = %cluster_info.id,
                target_size = %config.target_size,
                replica_name = %config.replica_name,
                "Target size replica creation completed"
            );
        }
        // Case 4: We have a pending target replica but it doesn't exist - need to recreate
        else if pending_target_replica.is_some() && target_size_replicas.is_empty() {
            let replica_spec = ReplicaSpec::new(&config.replica_name, &config.target_size);
            desired.add_replica(
                strategy_id,
                replica_spec,
                format!(
                    "Recreating target size replica ({}) - pending replica not found",
                    config.target_size
                ),
            );
            changed = true;

            info!(
                cluster_id = %cluster_info.id,
                target_size = %config.target_size,
                replica_name = %config.replica_name,
                "Adding target size replica to desired state (pending replica not found)"
            );
        }

        // Update state based on changes
        if changed {
            new_payload["last_decision_ts"] = serde_json::json!(now.to_rfc3339());

            // Track pending target replica creation
            let current_replica_names: std::collections::BTreeSet<String> = cluster_info
                .replicas
                .iter()
                .map(|r| r.name.clone())
                .collect();
            let desired_replica_names = desired.get_replica_names();

            let replicas_to_add: Vec<_> = desired_replica_names
                .iter()
                .filter(|n| !current_replica_names.contains(*n))
                .collect();

            if replicas_to_add.contains(&&config.replica_name) {
                new_payload["pending_target_replica"] = serde_json::json!({
                    "name": config.replica_name,
                    "size": config.target_size,
                    "created_at": now.to_rfc3339()
                });
            }

            // Clear pending state when target replica is found
            if !target_size_replicas.is_empty() && pending_target_replica.is_some() {
                new_payload["pending_target_replica"] = serde_json::Value::Null;
            }
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
                "pending_target_replica": null
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::widget::auto_scaling::models::ReplicaInfo;
    use mz_controller_types::ClusterId;
    use mz_repr::GlobalId;

    fn make_cluster_info(replicas: Vec<(&str, &str)>) -> ClusterInfo {
        ClusterInfo {
            id: ClusterId::User(1),
            name: "test_cluster".to_string(),
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
                crate::widget::auto_scaling::models::HydrationStatus {
                    total_objects: 10,
                    hydrated_objects: 10,
                },
            );
        }
        signals
    }

    #[mz_ore::test]
    fn test_target_size_creates_replica() {
        let strategy = TargetSizeStrategy;
        let config = serde_json::json!({
            "size": "200cc",
            "replica_name": "main"
        });

        let cluster_info = make_cluster_info(vec![]);
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

        assert!(result.changed);
        assert!(result.desired_state.target_replicas.contains_key("main"));
        assert_eq!(result.desired_state.target_replicas["main"].size, "200cc");
    }

    #[mz_ore::test]
    fn test_target_size_drops_other_replicas_when_hydrated() {
        let strategy = TargetSizeStrategy;
        let config = serde_json::json!({
            "size": "200cc",
            "replica_name": "main"
        });

        let cluster_info = make_cluster_info(vec![("main", "200cc"), ("old", "100cc")]);
        let signals = make_signals(ClusterId::User(1), vec!["main"]);
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
        assert!(!result.desired_state.target_replicas.contains_key("old"));
    }

    #[mz_ore::test]
    fn test_target_size_no_change_when_already_correct() {
        let strategy = TargetSizeStrategy;
        let config = serde_json::json!({
            "size": "200cc",
            "replica_name": "main"
        });

        let cluster_info = make_cluster_info(vec![("main", "200cc")]);
        let signals = make_signals(ClusterId::User(1), vec!["main"]);
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
    }
}
