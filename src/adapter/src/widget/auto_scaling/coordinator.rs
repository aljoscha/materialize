// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Strategy coordinator for auto-scaling.
//!
//! The coordinator runs multiple scaling strategies in priority order and
//! combines their decisions into a single desired state.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use mz_controller_types::ClusterId;
use tracing::{debug, warn};

use super::models::{ClusterInfo, DesiredState, Signals, StrategyConfig, StrategyState};
use super::signals::CatalogSnapshot;
use super::strategies::StrategyRegistry;

/// Coordinates multiple scaling strategies for a cluster.
///
/// The coordinator:
/// 1. Loads strategy state from builtin tables
/// 2. Runs strategies in priority order (lowest first)
/// 3. Each strategy receives the accumulated desired state from previous strategies
/// 4. Higher priority strategies can override or modify previous decisions
pub struct StrategyCoordinator {
    /// Registry of available strategy implementations.
    registry: StrategyRegistry,
}

impl StrategyCoordinator {
    /// Create a new strategy coordinator.
    pub fn new() -> Self {
        Self {
            registry: StrategyRegistry::new(),
        }
    }

    /// Run all configured strategies for a cluster and return the final desired state.
    ///
    /// Strategies are run in priority order (lowest first). Each strategy receives
    /// the accumulated desired state from previous strategies and can modify it.
    pub fn run_strategies(
        &mut self,
        cluster_info: &ClusterInfo,
        signals: &Signals,
        strategy_configs: &[StrategyConfig],
        strategy_states: &BTreeMap<(String, ClusterId), StrategyState>,
        catalog_snapshot: &CatalogSnapshot,
        initial_desired_state: DesiredState,
        now: DateTime<Utc>,
    ) -> Result<(DesiredState, BTreeMap<String, StrategyState>), String> {
        // Sort configs by strategy priority
        let mut sorted_configs: Vec<_> = strategy_configs
            .iter()
            .filter_map(|config| {
                self.registry
                    .get(&config.strategy_type)
                    .map(|strategy| (strategy.priority(), config, strategy))
            })
            .collect();
        sorted_configs.sort_by_key(|(priority, _, _)| *priority);

        let mut desired_state: Option<DesiredState> = Some(initial_desired_state);
        let mut new_states: BTreeMap<String, StrategyState> = BTreeMap::new();

        for (priority, config, strategy) in sorted_configs {
            if !config.enabled {
                debug!(
                    cluster = %cluster_info.name,
                    strategy = strategy.name(),
                    "Skipping disabled strategy"
                );
                continue;
            }

            let state_key = (config.id.clone(), cluster_info.id);
            let mut current_state = strategy_states
                .get(&state_key)
                .cloned()
                .unwrap_or_else(|| strategy.initial_state());

            // Check state version compatibility
            if !current_state.is_version_compatible() {
                warn!(
                    cluster = %cluster_info.name,
                    strategy = strategy.name(),
                    state_version = current_state.state_version,
                    expected_version = StrategyState::CURRENT_VERSION,
                    "Strategy state version mismatch, resetting state"
                );
                current_state = strategy.initial_state();
            }

            debug!(
                cluster = %cluster_info.name,
                strategy = strategy.name(),
                priority = ?priority,
                "Running scaling strategy"
            );

            // Run the strategy (take the desired_state to pass to strategy)
            let current_desired = desired_state.take();
            match strategy.decide(
                &config.id,
                &current_state,
                &config.config,
                signals,
                cluster_info,
                catalog_snapshot,
                current_desired,
                now,
            ) {
                Ok(decision) => {
                    // Record updated state for persistence only if it changed.
                    if decision.changed
                        || decision.next_state.state_version != current_state.state_version
                        || decision.next_state.payload != current_state.payload
                    {
                        new_states.insert(config.id.clone(), decision.next_state.clone());
                    }

                    // Update desired state
                    desired_state = Some(decision.desired_state);

                    if decision.changed {
                        debug!(
                            cluster = %cluster_info.name,
                            strategy = strategy.name(),
                            "Strategy made changes to desired state"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        cluster = %cluster_info.name,
                        strategy = strategy.name(),
                        error = %e,
                        "Strategy decision failed"
                    );
                    // Continue with other strategies
                }
            }
        }

        // If no strategies ran, return current state as desired
        Ok((
            desired_state.unwrap_or_else(|| DesiredState::from_cluster_info(cluster_info)),
            new_states,
        ))
    }
}

impl Default for StrategyCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::widget::auto_scaling::models::ReplicaInfo;
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

    #[mz_ore::test]
    fn test_coordinator_runs_strategies() {
        let mut coordinator = StrategyCoordinator::new();
        let cluster_info = make_cluster_info("test_cluster", vec![]);
        let signals = Signals::new(ClusterId::User(1));
        let snapshot = CatalogSnapshot::new(Utc::now());

        let configs = vec![StrategyConfig {
            id: "strategy-1".to_string(),
            strategy_type: "target_size".to_string(),
            config: serde_json::json!({
                "size": "200cc",
                "replica_name": "main"
            }),
            enabled: true,
        }];

        let (desired, _states) = coordinator
            .run_strategies(
                &cluster_info,
                &signals,
                &configs,
                &BTreeMap::new(),
                &snapshot,
                DesiredState::from_cluster_info(&cluster_info),
                Utc::now(),
            )
            .unwrap();

        // Should have created a replica
        assert!(desired.target_replicas.contains_key("main"));
        assert_eq!(desired.target_replicas["main"].size, "200cc");
    }

    #[mz_ore::test]
    fn test_coordinator_skips_disabled_strategies() {
        let mut coordinator = StrategyCoordinator::new();
        let cluster_info = make_cluster_info("test_cluster", vec![]);
        let signals = Signals::new(ClusterId::User(1));
        let snapshot = CatalogSnapshot::new(Utc::now());

        let configs = vec![StrategyConfig {
            id: "strategy-1".to_string(),
            strategy_type: "target_size".to_string(),
            config: serde_json::json!({
                "size": "200cc",
                "replica_name": "main"
            }),
            enabled: false,
        }];

        let (desired, _states) = coordinator
            .run_strategies(
                &cluster_info,
                &signals,
                &configs,
                &BTreeMap::new(),
                &snapshot,
                DesiredState::from_cluster_info(&cluster_info),
                Utc::now(),
            )
            .unwrap();

        // Should not have created a replica (strategy disabled)
        assert!(desired.target_replicas.is_empty());
    }
}
