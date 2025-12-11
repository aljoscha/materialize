// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shrink to fit scaling strategy.
//!
//! This strategy creates replicas of all possible sizes up to a configurable max,
//! then drops larger replicas when smaller ones become healthy (hydrated and not
//! crash-looping), arriving at the smallest viable replica size.

use chrono::{DateTime, Utc};
use tracing::{debug, info};

use super::{ScalingStrategy, StrategyDecision, StrategyPriority};
use crate::widget::auto_scaling::models::{
    ClusterInfo, DesiredState, ReplicaScope, ReplicaSpec, Signals, StrategyState,
};
use crate::widget::auto_scaling::signals::CatalogSnapshot;

/// Shrink to fit strategy configuration.
#[derive(Debug, Clone)]
pub struct ShrinkToFitConfig {
    /// Maximum replica size to try.
    pub max_size: String,
    /// Cooldown period in seconds.
    pub cooldown_s: i64,
    /// Minimum OOM count to consider a replica as OOM-looping.
    pub min_oom_count: u64,
    /// Minimum crash count to consider a replica as crash-looping.
    pub min_crash_count: u64,
    /// Which replicas are managed by the widget.
    pub replica_scope: ReplicaScope,
}

impl ShrinkToFitConfig {
    /// Parse configuration from JSON.
    pub fn from_json(config: &serde_json::Value) -> Result<Self, String> {
        let replica_scope = ReplicaScope::from_config(config)?;

        let max_size = config
            .get("max_size")
            .and_then(|v| v.as_str())
            .ok_or("Missing required config key: max_size")?
            .to_string();

        if max_size.is_empty() {
            return Err("max_size must be a non-empty string".to_string());
        }

        let cooldown_s = config
            .get("cooldown_s")
            .and_then(|v| v.as_i64())
            .unwrap_or(120);

        let min_oom_count = config
            .get("min_oom_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);

        let min_crash_count = config
            .get("min_crash_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);

        Ok(Self {
            max_size,
            cooldown_s,
            min_oom_count,
            min_crash_count,
            replica_scope,
        })
    }
}

/// Shrink to fit strategy implementation.
///
/// This strategy:
/// 1. Creates replicas of all possible sizes up to max_size (if no replicas exist)
/// 2. Removes crash-looping replicas (they're definitively too small)
/// 3. Drops larger replicas when smaller ones become healthy
pub struct ShrinkToFitStrategy;

impl ShrinkToFitStrategy {
    /// Get the index of a replica size in the ordered list.
    fn get_size_index(size: &str, available_sizes: &[String]) -> Option<usize> {
        available_sizes.iter().position(|s| s == size)
    }

    /// Get all sizes up to and including max_size.
    fn get_sizes_up_to_max<'a>(
        max_size: &str,
        available_sizes: &'a [String],
    ) -> Option<&'a [String]> {
        let max_index = available_sizes.iter().position(|s| s == max_size)?;
        Some(&available_sizes[..=max_index])
    }
}

impl ScalingStrategy for ShrinkToFitStrategy {
    fn name(&self) -> &'static str {
        "shrink_to_fit"
    }

    fn priority(&self) -> StrategyPriority {
        StrategyPriority::ShrinkToFit
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), String> {
        ShrinkToFitConfig::from_json(config)?;
        Ok(())
    }

    fn decide(
        &self,
        strategy_id: &str,
        current_state: &StrategyState,
        config: &serde_json::Value,
        signals: &Signals,
        cluster_info: &ClusterInfo,
        catalog_snapshot: &CatalogSnapshot,
        current_desired_state: Option<DesiredState>,
        now: DateTime<Utc>,
    ) -> Result<StrategyDecision, String> {
        let config = ShrinkToFitConfig::from_json(config)?;

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
                strategy = "shrink_to_fit",
                "Skipping decision due to cooldown"
            );
            return Ok(StrategyDecision {
                desired_state: desired,
                next_state: current_state.clone(),
                changed: false,
            });
        }

        let available_sizes = &catalog_snapshot.available_sizes;

        // Validate max_size is in available sizes
        let valid_sizes = match Self::get_sizes_up_to_max(&config.max_size, available_sizes) {
            Some(sizes) => sizes,
            None => {
                debug!(
                    cluster_id = %cluster_info.id,
                    max_size = %config.max_size,
                    available_sizes = ?available_sizes,
                    "max_size not found in available sizes, skipping"
                );
                return Ok(StrategyDecision {
                    desired_state: desired,
                    next_state: current_state.clone(),
                    changed: false,
                });
            }
        };

        debug!(
            cluster_id = %cluster_info.id,
            max_size = %config.max_size,
            valid_sizes = ?valid_sizes,
            current_replicas = cluster_info.replicas.len(),
            desired_replicas = desired.target_replicas.len(),
            "Evaluating shrink to fit requirements"
        );

        let mut new_payload = current_state.payload.clone();
        let mut changed = false;

        // Phase 1: Ensure we have at least one replica of each valid size
        // (if we don't have any replicas at all)
        if desired.target_replicas.is_empty() {
            for size in valid_sizes {
                let base_name = format!("{}_{}", cluster_info.name, size);
                let replica_name = match config.replica_scope {
                    ReplicaScope::All => base_name,
                    ReplicaScope::ManagedOnly => ReplicaScope::ensure_managed_name(&base_name),
                };
                let replica_spec = ReplicaSpec::new(&replica_name, size);
                desired.add_replica(
                    strategy_id,
                    replica_spec,
                    format!("Creating replica size {} for shrink-to-fit strategy", size),
                );
                changed = true;
            }

            if changed {
                info!(
                    cluster_id = %cluster_info.id,
                    sizes_added = ?valid_sizes,
                    "Adding all valid size replicas to desired state"
                );
            }
        }

        // Phase 2: Remove crash-looping replicas (they're definitively too small)
        if !desired.target_replicas.is_empty() {
            let mut crash_looping_replicas = Vec::new();

            for (replica_name, replica_spec) in &desired.target_replicas {
                if signals.is_replica_oom_looping(replica_name, config.min_oom_count) {
                    let crash_info = signals.crash_info.get(replica_name);
                    let oom_count = crash_info.map(|c| c.oom_count).unwrap_or(0);
                    crash_looping_replicas.push((
                        replica_name.clone(),
                        replica_spec.size.clone(),
                        format!("OOM loops ({} OOMs)", oom_count),
                    ));
                } else if signals.is_replica_crash_looping(replica_name, config.min_crash_count) {
                    let crash_info = signals.crash_info.get(replica_name);
                    let total_crashes = crash_info.map(|c| c.total_crashes).unwrap_or(0);
                    crash_looping_replicas.push((
                        replica_name.clone(),
                        replica_spec.size.clone(),
                        format!("crash loops ({} crashes)", total_crashes),
                    ));
                }
            }

            for (replica_name, replica_size, reason) in &crash_looping_replicas {
                desired.remove_replica(
                    strategy_id,
                    replica_name,
                    format!("Removing replica ({}) - {}", replica_size, reason),
                );
                changed = true;
            }

            if !crash_looping_replicas.is_empty() {
                info!(
                    cluster_id = %cluster_info.id,
                    replicas_removed = crash_looping_replicas.len(),
                    "Removed crash-looping replicas"
                );
            }
        }

        // Phase 3: Drop larger replicas when smaller ones are healthy
        if !desired.target_replicas.is_empty() {
            // Find the smallest healthy replica size (hydrated AND not crash-looping)
            let mut smallest_healthy_index: Option<usize> = None;
            let mut smallest_healthy_size: Option<String> = None;

            for (replica_name, replica_spec) in &desired.target_replicas {
                let is_hydrated = signals.is_replica_hydrated(replica_name);
                let is_oom_looping =
                    signals.is_replica_oom_looping(replica_name, config.min_oom_count);
                let is_crash_looping =
                    signals.is_replica_crash_looping(replica_name, config.min_crash_count);

                if is_hydrated && !is_oom_looping && !is_crash_looping {
                    if let Some(size_index) =
                        Self::get_size_index(&replica_spec.size, available_sizes)
                    {
                        if smallest_healthy_index.is_none()
                            || smallest_healthy_index.map_or(true, |existing| size_index < existing)
                        {
                            smallest_healthy_index = Some(size_index);
                            smallest_healthy_size = Some(replica_spec.size.clone());
                        }
                    }
                }
            }

            if let (Some(smallest_index), Some(smallest_size)) =
                (smallest_healthy_index, smallest_healthy_size)
            {
                // Drop all replicas larger than the smallest healthy one
                let replicas_to_drop: Vec<_> = desired
                    .target_replicas
                    .iter()
                    .filter_map(|(name, spec)| {
                        Self::get_size_index(&spec.size, available_sizes).and_then(|idx| {
                            if idx > smallest_index {
                                Some((name.clone(), spec.size.clone()))
                            } else {
                                None
                            }
                        })
                    })
                    .collect();

                for (replica_name, replica_size) in &replicas_to_drop {
                    desired.remove_replica(
                        strategy_id,
                        replica_name,
                        format!(
                            "Dropping larger replica ({}) - smaller replica ({}) is healthy",
                            replica_size, smallest_size
                        ),
                    );
                    changed = true;
                }

                if !replicas_to_drop.is_empty() {
                    info!(
                        cluster_id = %cluster_info.id,
                        smallest_healthy_size = %smallest_size,
                        replicas_dropped = replicas_to_drop.len(),
                        "Dropping larger replicas - smaller replica is healthy"
                    );
                }
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
    use crate::widget::auto_scaling::models::{CrashInfo, HydrationStatus, ReplicaInfo};
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

    fn make_snapshot_with_sizes(sizes: Vec<&str>) -> CatalogSnapshot {
        let mut snapshot = CatalogSnapshot::new(Utc::now());
        snapshot.available_sizes = sizes.into_iter().map(|s| s.to_string()).collect();
        snapshot
    }

    fn make_signals(
        cluster_id: ClusterId,
        hydrated: Vec<&str>,
        crash_info: Vec<(&str, u64, u64)>,
    ) -> Signals {
        let mut signals = Signals::new(cluster_id);
        for name in hydrated {
            signals.hydration_status.insert(
                name.to_string(),
                HydrationStatus {
                    total_objects: 10,
                    hydrated_objects: 10,
                },
            );
        }
        for (name, oom_count, total_crashes) in crash_info {
            signals.crash_info.insert(
                name.to_string(),
                CrashInfo {
                    oom_count,
                    total_crashes,
                    latest_crash_time: None,
                },
            );
        }
        signals
    }

    #[mz_ore::test]
    fn test_shrink_creates_all_sizes_when_empty() {
        let strategy = ShrinkToFitStrategy;
        let config = serde_json::json!({
            "max_size": "200cc"
        });

        let cluster_info = make_cluster_info(vec![]);
        let signals = make_signals(ClusterId::User(1), vec![], vec![]);
        let state = strategy.initial_state();
        let snapshot = make_snapshot_with_sizes(vec!["50cc", "100cc", "200cc", "400cc"]);

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
        // Should create 50cc, 100cc, 200cc (not 400cc which is above max)
        assert_eq!(result.desired_state.target_replicas.len(), 3);
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_50cc")
        );
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_100cc")
        );
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_200cc")
        );
    }

    #[mz_ore::test]
    fn test_shrink_drops_larger_when_smaller_hydrated() {
        let strategy = ShrinkToFitStrategy;
        let config = serde_json::json!({
            "max_size": "200cc"
        });

        let cluster_info = make_cluster_info(vec![
            ("test_cluster_50cc", "50cc"),
            ("test_cluster_100cc", "100cc"),
            ("test_cluster_200cc", "200cc"),
        ]);
        // 100cc is hydrated
        let signals = make_signals(ClusterId::User(1), vec!["test_cluster_100cc"], vec![]);
        let state = strategy.initial_state();
        let snapshot = make_snapshot_with_sizes(vec!["50cc", "100cc", "200cc", "400cc"]);

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
        // Should keep 50cc and 100cc, drop 200cc (larger than smallest healthy)
        assert_eq!(result.desired_state.target_replicas.len(), 2);
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_50cc")
        );
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_100cc")
        );
        assert!(
            !result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_200cc")
        );
    }

    #[mz_ore::test]
    fn test_shrink_removes_crash_looping_replicas() {
        let strategy = ShrinkToFitStrategy;
        let config = serde_json::json!({
            "max_size": "200cc",
            "min_oom_count": 2
        });

        let cluster_info = make_cluster_info(vec![
            ("test_cluster_50cc", "50cc"),
            ("test_cluster_100cc", "100cc"),
        ]);
        // 50cc has 2 OOMs (crash looping)
        let signals = make_signals(
            ClusterId::User(1),
            vec![],
            vec![("test_cluster_50cc", 2, 2)],
        );
        let state = strategy.initial_state();
        let snapshot = make_snapshot_with_sizes(vec!["50cc", "100cc", "200cc"]);

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
        // Should remove the crash-looping 50cc replica
        assert!(
            !result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_50cc")
        );
        assert!(
            result
                .desired_state
                .target_replicas
                .contains_key("test_cluster_100cc")
        );
    }

    #[mz_ore::test]
    fn test_shrink_no_change_when_max_size_not_available() {
        let strategy = ShrinkToFitStrategy;
        let config = serde_json::json!({
            "max_size": "invalid_size"
        });

        let cluster_info = make_cluster_info(vec![]);
        let signals = make_signals(ClusterId::User(1), vec![], vec![]);
        let state = strategy.initial_state();
        let snapshot = make_snapshot_with_sizes(vec!["50cc", "100cc", "200cc"]);

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
    }
}
