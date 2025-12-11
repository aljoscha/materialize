// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Idle suspend scaling strategy.
//!
//! This strategy suspends all replicas in a cluster after a period of inactivity.
//! Replica recreation is handled by the target_size strategy when combined.

use chrono::{DateTime, Utc};
use tracing::{debug, info};

use super::{ScalingStrategy, StrategyDecision, StrategyPriority};
use crate::widget::auto_scaling::models::{ClusterInfo, DesiredState, Signals, StrategyState};
use crate::widget::auto_scaling::signals::CatalogSnapshot;

/// Idle suspend strategy configuration.
#[derive(Debug, Clone)]
pub struct IdleSuspendConfig {
    /// Seconds of inactivity before suspending.
    pub idle_after_s: i64,
    /// Cooldown period in seconds.
    pub cooldown_s: i64,
}

impl IdleSuspendConfig {
    /// Parse configuration from JSON.
    pub fn from_json(config: &serde_json::Value) -> Result<Self, String> {
        let idle_after_s = config
            .get("idle_after_s")
            .and_then(|v| v.as_i64())
            .ok_or("Missing required config key: idle_after_s")?;

        if idle_after_s <= 0 {
            return Err("idle_after_s must be > 0".to_string());
        }

        let cooldown_s = config
            .get("cooldown_s")
            .and_then(|v| v.as_i64())
            .unwrap_or(300); // Default 5 minute cooldown for suspend

        Ok(Self {
            idle_after_s,
            cooldown_s,
        })
    }
}

/// Idle suspend strategy implementation.
///
/// This strategy:
/// 1. Monitors cluster activity
/// 2. Suspends all replicas after a configured idle period
/// 3. Has the highest priority so it can override other strategies
///
/// Note: Replica recreation is handled by the target_size strategy when combined.
/// When a query arrives on a suspended cluster, the target_size strategy will
/// create a new replica.
pub struct IdleSuspendStrategy;

impl ScalingStrategy for IdleSuspendStrategy {
    fn name(&self) -> &'static str {
        "idle_suspend"
    }

    fn priority(&self) -> StrategyPriority {
        StrategyPriority::IdleSuspend
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), String> {
        IdleSuspendConfig::from_json(config)?;
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
        let config = IdleSuspendConfig::from_json(config)?;

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
                strategy = "idle_suspend",
                "Skipping decision due to cooldown"
            );
            return Ok(StrategyDecision {
                desired_state: desired,
                next_state: current_state.clone(),
                changed: false,
            });
        }

        let current_replicas = desired.target_replicas.len();
        let seconds_since_activity = signals.seconds_since_activity(now);

        debug!(
            cluster_id = %cluster_info.id,
            seconds_since_activity = ?seconds_since_activity,
            threshold = config.idle_after_s,
            current_replicas = current_replicas,
            "Evaluating idle suspend"
        );

        let mut new_payload = current_state.payload.clone();
        let mut changed = false;

        // Check if cluster is idle and has replicas to suspend
        if current_replicas > 0 {
            let should_suspend;
            let reason;

            if seconds_since_activity.is_none() {
                // No activity recorded - suspend as it indicates no recent activity
                should_suspend = true;
                reason = "No activity data available - suspending cluster".to_string();

                info!(
                    cluster_id = %cluster_info.id,
                    "No activity data available, suspending cluster"
                );
            } else if let Some(secs) = seconds_since_activity {
                #[allow(clippy::as_conversions)] // i64 to f64 conversion is safe for seconds
                if secs > config.idle_after_s as f64 {
                    should_suspend = true;
                    reason = format!(
                        "Idle for {:.0}s (threshold: {}s)",
                        secs, config.idle_after_s
                    );
                } else {
                    should_suspend = false;
                    reason = String::new();
                }
            } else {
                should_suspend = false;
                reason = String::new();
            }

            if should_suspend {
                // Remove all replicas from desired state
                let replica_names: Vec<_> = desired.target_replicas.keys().cloned().collect();
                for replica_name in replica_names {
                    desired.remove_replica(strategy_id, &replica_name, &reason);
                }
                changed = true;

                info!(
                    cluster_id = %cluster_info.id,
                    idle_seconds = ?seconds_since_activity,
                    threshold = config.idle_after_s,
                    replicas_to_remove = current_replicas,
                    "Removing all replicas from desired state (suspending idle cluster)"
                );
            }
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
    use crate::widget::auto_scaling::models::ReplicaInfo;
    use chrono::Duration;
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

    fn make_signals_with_activity(
        cluster_id: ClusterId,
        last_activity: Option<DateTime<Utc>>,
    ) -> Signals {
        let mut signals = Signals::new(cluster_id);
        signals.last_activity_ts = last_activity;
        signals
    }

    #[mz_ore::test]
    fn test_idle_suspend_suspends_after_threshold() {
        let strategy = IdleSuspendStrategy;
        let config = serde_json::json!({
            "idle_after_s": 300  // 5 minutes
        });

        let cluster_info = make_cluster_info(vec![("main", "200cc")]);
        let now = Utc::now();
        // Last activity was 10 minutes ago
        let signals =
            make_signals_with_activity(ClusterId::User(1), Some(now - Duration::seconds(600)));
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(now);

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                now,
            )
            .unwrap();

        assert!(result.changed);
        assert!(result.desired_state.target_replicas.is_empty());
    }

    #[mz_ore::test]
    fn test_idle_suspend_no_change_when_active() {
        let strategy = IdleSuspendStrategy;
        let config = serde_json::json!({
            "idle_after_s": 300  // 5 minutes
        });

        let cluster_info = make_cluster_info(vec![("main", "200cc")]);
        let now = Utc::now();
        // Last activity was 1 minute ago
        let signals =
            make_signals_with_activity(ClusterId::User(1), Some(now - Duration::seconds(60)));
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(now);

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                now,
            )
            .unwrap();

        assert!(!result.changed);
        assert!(result.desired_state.target_replicas.contains_key("main"));
    }

    #[mz_ore::test]
    fn test_idle_suspend_suspends_when_no_activity_data() {
        let strategy = IdleSuspendStrategy;
        let config = serde_json::json!({
            "idle_after_s": 300
        });

        let cluster_info = make_cluster_info(vec![("main", "200cc")]);
        let now = Utc::now();
        // No activity data
        let signals = make_signals_with_activity(ClusterId::User(1), None);
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(now);

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                now,
            )
            .unwrap();

        assert!(result.changed);
        assert!(result.desired_state.target_replicas.is_empty());
    }

    #[mz_ore::test]
    fn test_idle_suspend_no_change_when_no_replicas() {
        let strategy = IdleSuspendStrategy;
        let config = serde_json::json!({
            "idle_after_s": 300
        });

        // No replicas to suspend
        let cluster_info = make_cluster_info(vec![]);
        let now = Utc::now();
        let signals = make_signals_with_activity(ClusterId::User(1), None);
        let state = strategy.initial_state();
        let snapshot = CatalogSnapshot::new(now);

        let result = strategy
            .decide(
                "strategy-1",
                &state,
                &config,
                &signals,
                &cluster_info,
                &snapshot,
                None,
                now,
            )
            .unwrap();

        assert!(!result.changed);
    }
}
