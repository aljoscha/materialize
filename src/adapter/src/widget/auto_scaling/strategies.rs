// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scaling strategies for the auto-scaling widget.
//!
//! Strategies implement different scaling policies:
//! - `target_size`: Maintain exactly one replica of a specified size
//! - `burst`: Add temporary large replicas during hydration
//! - `idle_suspend`: Suspend replicas after inactivity
//! - `shrink_to_fit`: Find smallest viable replica size

mod burst;
mod idle_suspend;
mod shrink_to_fit;
mod target_size;

pub use burst::BurstStrategy;
pub use idle_suspend::IdleSuspendStrategy;
pub use shrink_to_fit::ShrinkToFitStrategy;
pub use target_size::TargetSizeStrategy;

use chrono::{DateTime, Utc};

use super::models::{ClusterInfo, DesiredState, Signals, StrategyState};
use super::signals::CatalogSnapshot;

/// Priority levels for strategies.
///
/// Lower numbers = lower priority (processed first).
/// Higher priority strategies can override decisions from lower priority ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StrategyPriority {
    /// Target size strategy (priority 0).
    TargetSize = 0,
    /// Shrink to fit strategy (priority 1).
    ShrinkToFit = 1,
    /// Burst strategy (priority 2).
    Burst = 2,
    /// Idle suspend strategy (priority 3).
    IdleSuspend = 3,
}

/// Result of a strategy decision.
#[derive(Debug)]
pub struct StrategyDecision {
    /// The desired state after this strategy's decisions.
    pub desired_state: DesiredState,
    /// Updated strategy state to persist.
    pub next_state: StrategyState,
    /// Whether any changes were made.
    pub changed: bool,
}

/// Trait for scaling strategies.
///
/// Strategies implement decision logic for cluster scaling. Each strategy
/// receives the current state, signals, and optionally the desired state
/// from previous strategies, then returns a (possibly modified) desired state.
pub trait ScalingStrategy: Send + Sync {
    /// Unique name for this strategy type.
    fn name(&self) -> &'static str;

    /// Priority of this strategy (lower = processed first).
    fn priority(&self) -> StrategyPriority;

    /// Validate strategy configuration.
    fn validate_config(&self, config: &serde_json::Value) -> Result<(), String>;

    /// Make scaling decision.
    ///
    /// # Arguments
    /// * `current_state` - Current state for this strategy
    /// * `config` - Strategy configuration
    /// * `signals` - Activity and hydration signals for the cluster
    /// * `cluster_info` - Information about the cluster
    /// * `catalog_snapshot` - Snapshot of catalog state
    /// * `current_desired_state` - Desired state from previous strategies (if any)
    /// * `now` - Current timestamp
    ///
    /// # Returns
    /// The strategy decision including desired state and updated strategy state.
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
    ) -> Result<StrategyDecision, String>;

    /// Create initial state for this strategy.
    fn initial_state(&self) -> StrategyState {
        StrategyState::new()
    }

    /// Check if strategy is in cooldown.
    fn check_cooldown(
        &self,
        state: &StrategyState,
        config: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> bool {
        let cooldown_s = config
            .get("cooldown_s")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if cooldown_s <= 0 {
            return false;
        }

        if let Some(last_ts) = state
            .payload
            .get("last_decision_ts")
            .and_then(|v| v.as_str())
        {
            if let Ok(last_decision) = DateTime::parse_from_rfc3339(last_ts) {
                let elapsed = (now - last_decision.with_timezone(&Utc)).num_seconds();
                return elapsed < cooldown_s;
            }
        }

        false
    }
}

/// Registry of available strategy implementations.
pub struct StrategyRegistry {
    strategies: Vec<Box<dyn ScalingStrategy>>,
}

impl StrategyRegistry {
    /// Create a new registry with all built-in strategies.
    pub fn new() -> Self {
        let mut strategies: Vec<Box<dyn ScalingStrategy>> = vec![
            Box::new(TargetSizeStrategy),
            Box::new(ShrinkToFitStrategy),
            Box::new(BurstStrategy),
            Box::new(IdleSuspendStrategy),
        ];
        // Sort by priority (lowest first)
        strategies.sort_by_key(|s| s.priority());
        Self { strategies }
    }

    /// Get a strategy by name.
    pub fn get(&self, name: &str) -> Option<&dyn ScalingStrategy> {
        self.strategies
            .iter()
            .find(|s| s.name() == name)
            .map(|s| s.as_ref())
    }

    /// Get all strategies in priority order.
    pub fn all(&self) -> impl Iterator<Item = &dyn ScalingStrategy> {
        self.strategies.iter().map(|s| s.as_ref())
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::new()
    }
}
