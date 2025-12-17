// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Auto-scaling widget for managing cluster replicas.
//!
//! This module implements the auto-scaling widget that manages cluster replicas
//! based on configured scaling strategies. The widget runs in its own async task
//! and communicates with the coordinator through channels.
//!
//! # Architecture
//!
//! The auto-scaling widget consists of:
//!
//! - **Models** (`models`): Data structures for replicas, clusters, signals, and actions.
//! - **Signals** (`signals`): Collection of signals from system state.
//! - **Strategies** (`strategies`): Scaling decision logic (target_size, burst, etc.).
//! - **Coordinator** (`coordinator`): Orchestration of multiple strategies.
//!
//! # Usage
//!
//! The widget is spawned by the coordinator and receives periodic ticks. On each tick:
//!
//! 1. Read current configuration from `mz_scaling_strategies` (via catalog)
//! 2. Collect cluster and replica information from the catalog
//! 3. Run strategies in priority order to compute desired state
//! 4. Compare desired state with current state to compute actions
//! 5. Return actions to be executed by the coordinator

pub mod coordinator;
pub mod models;
pub mod signals;
pub mod strategies;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use mz_catalog::builtin::{MZ_SCALING_STRATEGIES, MZ_SCALING_STRATEGY_STATE};
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::ClusterId;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::{Datum, GlobalId, Row};
use tracing::{debug, info, warn};

use self::coordinator::StrategyCoordinator;
use self::models::{
    ClusterInfo, DesiredState, ReplicaInfo, ReplicaScope, ScalingAction, ScalingActionType,
    Signals, StrategyConfig,
};
use self::signals::{CatalogSnapshot, SignalCollector};
use super::{Widget, WidgetAction, WidgetContext, WidgetError, WidgetEvent};

// ============================================================================
// Strategy Parsing (widget-local types)
// ============================================================================

/// A parsed strategy from the mz_scaling_strategies table.
///
/// This is the widget's internal representation of a strategy row. It contains
/// just enough information to determine which clusters the strategy applies to
/// and what configuration to use.
#[derive(Debug, Clone)]
struct ParsedStrategy {
    /// Unique identifier for this strategy.
    #[allow(dead_code)]
    id: String,
    /// The cluster ID if this strategy targets a specific cluster.
    cluster_id: Option<ClusterId>,
    /// The pattern if this is an "ALL CLUSTERS LIKE" strategy.
    cluster_pattern: Option<String>,
    /// The type of strategy (target_size, burst, idle_suspend, shrink_to_fit).
    strategy_type: String,
    /// The JSON configuration for the strategy.
    config: serde_json::Value,
    /// Whether this strategy is currently enabled.
    enabled: bool,
}

impl ParsedStrategy {
    /// Returns true if this strategy applies to the given cluster.
    fn applies_to_cluster(&self, cluster_id: ClusterId, cluster_name: &str) -> bool {
        if let Some(target_id) = self.cluster_id {
            // Specific cluster strategy
            target_id == cluster_id
        } else if let Some(ref pattern) = self.cluster_pattern {
            // Pattern-based strategy
            Self::matches_pattern(cluster_name, pattern)
        } else {
            // ALL CLUSTERS strategy
            true
        }
    }

    /// Simple LIKE pattern matching where % matches any sequence of characters.
    fn matches_pattern(name: &str, pattern: &str) -> bool {
        let pattern_parts: Vec<&str> = pattern.split('%').collect();
        if pattern_parts.is_empty() {
            return name.is_empty();
        }

        let mut pos = 0;
        for (i, part) in pattern_parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            match name[pos..].find(part) {
                Some(found_pos) => {
                    // First part must match at start if pattern doesn't start with %
                    if i == 0 && !pattern.starts_with('%') && found_pos != 0 {
                        return false;
                    }
                    pos += found_pos + part.len();
                }
                None => return false,
            }
        }

        // Last part must match at end if pattern doesn't end with %
        if !pattern.ends_with('%') {
            if let Some(last) = pattern_parts.last() {
                if !last.is_empty() {
                    return name.ends_with(last);
                }
            }
        }

        true
    }
}

/// Auto-scaling widget for managing cluster replicas.
///
/// This widget implements scaling strategies to automatically manage cluster
/// replicas based on configured policies.
pub struct AutoScalingWidget {
    /// Strategy coordinator for running scaling strategies.
    coordinator: StrategyCoordinator,
    /// Tick count for debugging.
    tick_count: u64,
}

impl AutoScalingWidget {
    /// Create a new auto-scaling widget.
    pub fn new() -> Self {
        Self {
            coordinator: StrategyCoordinator::new(),
            tick_count: 0,
        }
    }

    /// Build a catalog snapshot from the context's catalog.
    ///
    /// This reads cluster and replica information directly from the catalog,
    /// collects signals from system tables, and loads strategy configurations.
    async fn build_snapshot(&self, ctx: &WidgetContext) -> CatalogSnapshot {
        let now = Utc
            .timestamp_millis_opt(i64::try_from(ctx.now_millis).unwrap_or(i64::MAX))
            .single()
            .unwrap_or_else(Utc::now);

        let mut snapshot = CatalogSnapshot::new(now);

        // Collect available replica sizes from the catalog
        // These are needed for strategies like shrink_to_fit
        snapshot.available_sizes = ctx
            .catalog
            .cluster_replica_sizes()
            .enabled_allocations()
            .map(|(name, _)| name.clone())
            .collect();

        // Collect cluster information from the catalog
        for cluster in ctx.catalog.clusters() {
            // Skip system clusters (those starting with mz_ or system)
            if cluster.name.starts_with("mz_") || cluster.name == "system" {
                continue;
            }

            let replicas: Vec<ReplicaInfo> = cluster
                .replicas()
                .map(|replica| {
                    let size = match &replica.config.location {
                        ReplicaLocation::Managed(managed) => managed.size_for_billing().to_string(),
                        ReplicaLocation::Unmanaged { .. } => "unmanaged".to_string(),
                    };

                    ReplicaInfo {
                        name: replica.name.clone(),
                        size,
                        id: GlobalId::User(replica.replica_id.inner_id()),
                    }
                })
                .collect();

            let cluster_info = ClusterInfo {
                id: cluster.id,
                name: cluster.name.clone(),
                replicas,
                managed: cluster.is_managed(),
            };

            snapshot.add_cluster(cluster_info);
        }

        // Collect signals from system tables (activity, hydration, crashes)
        // Default lookback for crash signals is 1 hour
        const CRASH_LOOKBACK_HOURS: i64 = 1;
        if let Err(e) =
            SignalCollector::collect_signals(ctx, &mut snapshot, CRASH_LOOKBACK_HOURS).await
        {
            warn!(error = %e, "Failed to collect signals from system tables");
        }

        // Load strategy configurations
        self.load_strategy_configs(ctx, &mut snapshot).await;

        // Load persisted strategy state (per (strategy_id, cluster_id)).
        self.load_strategy_state(ctx, &mut snapshot).await;

        snapshot
    }

    /// Load strategy configurations by reading from the mz_scaling_strategies table.
    ///
    /// This uses the generic table reading interface to get raw rows, then parses
    /// them into strategy configurations.
    async fn load_strategy_configs(&self, ctx: &WidgetContext, snapshot: &mut CatalogSnapshot) {
        // Get the table ID for mz_scaling_strategies
        let table_id = ctx.catalog.resolve_builtin_table(&MZ_SCALING_STRATEGIES);

        // Read raw rows from the table
        let rows = match ctx.read_builtin_table(table_id).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "Failed to read scaling strategies table, using empty list");
                return;
            }
        };

        if rows.is_empty() {
            debug!("No scaling strategies configured");
            return;
        }

        // Parse the raw rows into strategy data
        let parsed_strategies: Vec<ParsedStrategy> =
            rows.iter().filter_map(Self::parse_strategy_row).collect();

        info!(
            num_strategies = parsed_strategies.len(),
            "Loaded scaling strategies from table"
        );

        // Choose an effective strategy per (cluster, strategy_type) using deterministic precedence:
        // 1) specific cluster strategy
        // 2) most-specific matching pattern
        // 3) ALL CLUSTERS
        for (cluster_id, cluster) in &snapshot.clusters {
            let mut by_type: BTreeMap<String, Vec<&ParsedStrategy>> = BTreeMap::new();
            for strategy in &parsed_strategies {
                if !strategy.enabled {
                    continue;
                }
                if strategy.applies_to_cluster(*cluster_id, &cluster.name) {
                    by_type
                        .entry(strategy.strategy_type.clone())
                        .or_default()
                        .push(strategy);
                }
            }

            for (strategy_type, candidates) in by_type {
                let selected =
                    select_effective_strategy(&snapshot.clusters, *cluster_id, &candidates);
                let Some(selected) = selected else {
                    continue;
                };

                snapshot
                    .strategy_configs
                    .entry(*cluster_id)
                    .or_insert_with(Vec::new)
                    .push(StrategyConfig {
                        id: selected.id.clone(),
                        strategy_type: strategy_type.clone(),
                        config: selected.config.clone(),
                        enabled: selected.enabled,
                    });

                debug!(
                    cluster_name = %cluster.name,
                    strategy_type = %strategy_type,
                    strategy_id = %selected.id,
                    "Selected effective scaling strategy for cluster"
                );
            }
        }
    }

    /// Load strategy state rows from mz_scaling_strategy_state.
    async fn load_strategy_state(&self, ctx: &WidgetContext, snapshot: &mut CatalogSnapshot) {
        let table_id = ctx
            .catalog
            .resolve_builtin_table(&MZ_SCALING_STRATEGY_STATE);

        let rows = match ctx.read_builtin_table(table_id).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "Failed to read scaling strategy state table");
                return;
            }
        };

        if rows.is_empty() {
            return;
        }

        for row in rows {
            let mut datums = row.iter();
            let strategy_id = match datums.next() {
                Some(Datum::String(s)) => s.to_string(),
                _ => continue,
            };
            let cluster_id_str = match datums.next() {
                Some(Datum::String(s)) => s.to_string(),
                _ => continue,
            };
            let state_version = match datums.next() {
                Some(Datum::Int32(v)) => v,
                _ => continue,
            };
            let payload_datum = match datums.next() {
                Some(d) => d,
                None => continue,
            };
            let payload = match payload_datum {
                Datum::JsonNull => serde_json::Value::Null,
                datum => JsonbRef::from_datum(datum).to_serde_json(),
            };

            let cluster_id = match ClusterId::from_str(&cluster_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };

            // Only keep state for clusters we currently track (user clusters).
            if !snapshot.clusters.contains_key(&cluster_id) {
                continue;
            }

            snapshot.strategy_states.insert(
                (strategy_id, cluster_id),
                self::models::StrategyState {
                    state_version,
                    payload,
                },
            );
        }
    }

    /// Parse a raw row from mz_scaling_strategies into a ParsedStrategy.
    ///
    /// The schema is:
    /// - id: String
    /// - cluster_id: String (nullable)
    /// - cluster_pattern: String (nullable)
    /// - strategy_type: String
    /// - config: Jsonb
    /// - enabled: Bool
    /// - created_at: TimestampTz
    /// - updated_at: TimestampTz
    fn parse_strategy_row(row: &Row) -> Option<ParsedStrategy> {
        let mut datums = row.iter();

        let id = datums.next()?.unwrap_str().to_string();
        let cluster_id_str = match datums.next()? {
            Datum::String(s) => Some(s.to_string()),
            Datum::Null => None,
            _ => return None,
        };
        let cluster_pattern = match datums.next()? {
            Datum::String(s) => Some(s.to_string()),
            Datum::Null => None,
            _ => return None,
        };
        let strategy_type = datums.next()?.unwrap_str().to_string();

        // Extract JSONB config - it's stored as JsonbRef in the datum
        let config_datum = datums.next()?;
        let config = match config_datum {
            Datum::JsonNull => serde_json::Value::Null,
            datum => {
                // Try to convert to JsonbRef and then to serde_json::Value
                let jsonb_ref = JsonbRef::from_datum(datum);
                jsonb_ref.to_serde_json()
            }
        };

        let enabled = datums.next()?.unwrap_bool();
        // Skip timestamps for now - we don't need them for strategy matching
        let _created_at = datums.next()?;
        let _updated_at = datums.next()?;

        // Parse cluster_id from string
        let cluster_id = cluster_id_str
            .as_ref()
            .and_then(|s| ClusterId::from_str(s).ok());

        Some(ParsedStrategy {
            id,
            cluster_id,
            cluster_pattern,
            strategy_type,
            config,
            enabled,
        })
    }

    /// Compute scaling actions by comparing desired state with current state.
    fn compute_actions(
        cluster_info: &ClusterInfo,
        desired_state: &DesiredState,
        replica_scope: ReplicaScope,
    ) -> Vec<ScalingAction> {
        let mut actions = Vec::new();

        let current_by_name: std::collections::BTreeMap<&str, &str> = cluster_info
            .replicas
            .iter()
            .map(|r| (r.name.as_str(), r.size.as_str()))
            .collect();
        let desired_names = desired_state.get_replica_names();

        // Replicas to create
        for name in &desired_names {
            if !current_by_name.contains_key(name.as_str()) {
                if let Some(spec) = desired_state.target_replicas.get(name) {
                    let sql = spec.to_create_sql(&cluster_info.name);
                    actions.push(ScalingAction {
                        cluster_id: cluster_info.id,
                        strategy_id: desired_state
                            .replica_origins
                            .get(name)
                            .cloned()
                            .unwrap_or_else(|| "<unknown>".to_string()),
                        sql,
                        action_type: ScalingActionType::CreateReplica,
                        reasons: desired_state.reasons.clone(),
                    });
                }
            }
        }

        // Replicas to resize (drop + create) when name exists but size differs.
        for (name, desired_spec) in &desired_state.target_replicas {
            if let Some(current_size) = current_by_name.get(name.as_str()) {
                if *current_size != desired_spec.size.as_str() {
                    let strategy_id = desired_state
                        .replica_origins
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".to_string());

                    let drop_sql = format!(
                        "DROP CLUSTER REPLICA \"{}\".\"{}\"",
                        cluster_info.name, name
                    );
                    actions.push(ScalingAction {
                        cluster_id: cluster_info.id,
                        strategy_id: strategy_id.clone(),
                        sql: drop_sql,
                        action_type: ScalingActionType::ResizeReplica,
                        reasons: desired_state.reasons.clone(),
                    });

                    let create_sql = desired_spec.to_create_sql(&cluster_info.name);
                    actions.push(ScalingAction {
                        cluster_id: cluster_info.id,
                        strategy_id,
                        sql: create_sql,
                        action_type: ScalingActionType::ResizeReplica,
                        reasons: desired_state.reasons.clone(),
                    });
                }
            }
        }

        // Replicas to drop
        for replica in &cluster_info.replicas {
            if replica_scope == ReplicaScope::ManagedOnly
                && !ReplicaScope::is_managed_replica_name(&replica.name)
            {
                continue;
            }
            if !desired_names.contains(&replica.name) {
                let sql = format!(
                    "DROP CLUSTER REPLICA \"{}\".\"{}\"",
                    cluster_info.name, replica.name
                );
                actions.push(ScalingAction {
                    cluster_id: cluster_info.id,
                    strategy_id: desired_state
                        .removal_origins
                        .get(&replica.name)
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".to_string()),
                    sql,
                    action_type: ScalingActionType::DropReplica,
                    reasons: desired_state.reasons.clone(),
                });
            }
        }

        actions
    }
}

impl Default for AutoScalingWidget {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Widget for AutoScalingWidget {
    fn name(&self) -> &'static str {
        "auto_scaling"
    }

    fn tick_interval(&self) -> Duration {
        // Tick every 30 seconds by default
        Duration::from_secs(30)
    }

    async fn initialize(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError> {
        info!(
            widget = self.name(),
            now_millis = ctx.now_millis,
            read_only = ctx.read_only,
            "AutoScalingWidget initialized"
        );
        Ok(())
    }

    async fn tick(&mut self, ctx: &WidgetContext) -> Result<Vec<WidgetAction>, WidgetError> {
        self.tick_count += 1;

        let now = Utc
            .timestamp_millis_opt(i64::try_from(ctx.now_millis).unwrap_or(i64::MAX))
            .single()
            .unwrap_or_else(Utc::now);

        // Build snapshot from catalog
        let snapshot = self.build_snapshot(ctx).await;

        info!(
            widget = self.name(),
            tick_count = self.tick_count,
            now = %now,
            num_clusters = snapshot.clusters.len(),
            num_strategy_configs = snapshot.strategy_configs.values().map(|v| v.len()).sum::<usize>(),
            "AutoScalingWidget tick"
        );

        // If in read-only mode, don't take any actions
        if ctx.read_only {
            info!(widget = self.name(), "Skipping tick in read-only mode");
            return Ok(vec![]);
        }

        let mut all_actions = Vec::new();

        // Process each cluster with strategies
        for (cluster_id, cluster_info) in &snapshot.clusters {
            let signals = snapshot
                .signals
                .get(cluster_id)
                .cloned()
                .unwrap_or_else(|| Signals::new(*cluster_id));

            // Get strategy configs for this cluster from the snapshot
            let strategy_configs = snapshot
                .strategy_configs
                .get(cluster_id)
                .cloned()
                .unwrap_or_default();

            if strategy_configs.is_empty() {
                continue;
            }

            let hydrated_replicas = signals
                .hydration_status
                .values()
                .filter(|s| s.is_hydrated())
                .count();
            let total_crashes: u64 = signals.crash_info.values().map(|c| c.total_crashes).sum();
            let total_ooms: u64 = signals.crash_info.values().map(|c| c.oom_count).sum();

            info!(
                widget = self.name(),
                cluster = %cluster_info.name,
                num_strategies = strategy_configs.len(),
                "Processing cluster with strategies"
            );

            debug!(
                widget = self.name(),
                cluster = %cluster_info.name,
                cluster_id = %cluster_info.id,
                current_replicas = ?cluster_info
                    .replicas
                    .iter()
                    .map(|r| format!("{}:{}", r.name, r.size))
                    .collect::<Vec<_>>(),
                last_activity_ts = ?signals.last_activity_ts,
                seconds_since_activity = ?signals.seconds_since_activity(snapshot.now),
                hydration_replicas = signals.hydration_status.len(),
                hydrated_replicas,
                cluster_hydrated = signals.is_cluster_hydrated(),
                crash_replicas = signals.crash_info.len(),
                total_crashes,
                total_ooms,
                strategies = ?strategy_configs
                    .iter()
                    .map(|c| format!("{}({})", c.strategy_type, c.id))
                    .collect::<Vec<_>>(),
                "Auto-scaling inputs"
            );

            // Run strategies to get desired state
            let replica_scope = match effective_replica_scope(&strategy_configs) {
                Ok(scope) => scope,
                Err(e) => {
                    warn!(
                        widget = self.name(),
                        cluster = %cluster_info.name,
                        error = %e,
                        "Invalid replica_scope configuration; skipping cluster"
                    );
                    continue;
                }
            };

            let initial_desired =
                DesiredState::from_cluster_info_with_scope(cluster_info, replica_scope);

            match self.coordinator.run_strategies(
                cluster_info,
                &signals,
                &strategy_configs,
                &snapshot.strategy_states,
                &snapshot,
                initial_desired,
                now,
            ) {
                Ok((desired_state, new_states)) => {
                    debug!(
                        widget = self.name(),
                        cluster = %cluster_info.name,
                        desired_replicas = ?desired_state
                            .target_replicas
                            .iter()
                            .map(|(name, spec)| format!("{}:{}", name, spec.size))
                            .collect::<Vec<_>>(),
                        reasons = ?desired_state.reasons,
                        "Auto-scaling decision"
                    );
                    // Persist updated per-strategy state.
                    for (strategy_id, state) in new_states {
                        let delete_sql = format!(
                            "DELETE FROM mz_internal.mz_scaling_strategy_state \
                             WHERE strategy_id = '{}' AND cluster_id = '{}'",
                            escape_single_quotes(&strategy_id),
                            escape_single_quotes(&cluster_info.id.to_string()),
                        );
                        all_actions.push(WidgetAction::ExecuteDml {
                            sql: delete_sql,
                            reason: format!(
                                "Persist auto-scaling state: delete prior state (strategy_id={}, cluster_id={})",
                                strategy_id, cluster_info.id
                            ),
                        });

                        let payload_json = serde_json::to_string(&state.payload)
                            .unwrap_or_else(|_| "null".to_string());
                        let insert_sql = format!(
                            "INSERT INTO mz_internal.mz_scaling_strategy_state \
                             (strategy_id, cluster_id, state_version, payload, updated_at) \
                             VALUES ('{}', '{}', {}, '{}'::jsonb, now())",
                            escape_single_quotes(&strategy_id),
                            escape_single_quotes(&cluster_info.id.to_string()),
                            state.state_version,
                            escape_single_quotes(&payload_json),
                        );
                        all_actions.push(WidgetAction::ExecuteDml {
                            sql: insert_sql,
                            reason: format!(
                                "Persist auto-scaling state: insert new state (strategy_id={}, cluster_id={})",
                                strategy_id, cluster_info.id
                            ),
                        });
                    }

                    // Compute actions from desired state
                    let actions =
                        Self::compute_actions(cluster_info, &desired_state, replica_scope);

                    // Convert to WidgetActions
                    for action in actions {
                        info!(
                            widget = self.name(),
                            cluster = %cluster_info.name,
                            action_type = action.action_type.as_str(),
                            sql = %action.sql,
                            reasons = ?action.reasons,
                            "Generated scaling action"
                        );
                        all_actions.push(WidgetAction::ExecuteDdl {
                            cluster_id: action.cluster_id,
                            strategy_id: Some(action.strategy_id),
                            action_type: Some(action.action_type.as_str().to_string()),
                            sql: action.sql,
                            reason: action.reasons.join("; "),
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        widget = self.name(),
                        cluster = %cluster_info.name,
                        error = %e,
                        "Strategy execution failed"
                    );
                }
            }
        }

        Ok(all_actions)
    }

    async fn on_event(
        &mut self,
        event: WidgetEvent,
        _ctx: &WidgetContext,
    ) -> Result<Vec<WidgetAction>, WidgetError> {
        debug!(
            widget = self.name(),
            event = ?event,
            "AutoScalingWidget received event"
        );
        // Events could trigger early evaluation, but for now we just log them
        Ok(vec![])
    }

    async fn shutdown(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError> {
        info!(
            widget = self.name(),
            tick_count = self.tick_count,
            now_millis = ctx.now_millis,
            "AutoScalingWidget shutting down"
        );
        Ok(())
    }
}

fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

fn effective_replica_scope(configs: &[StrategyConfig]) -> Result<ReplicaScope, String> {
    let mut scope: Option<ReplicaScope> = None;
    for config in configs {
        let s = ReplicaScope::from_config(&config.config)?;
        if let Some(existing) = scope {
            if existing != s {
                return Err(format!(
                    "conflicting replica_scope values for cluster: {:?} vs {:?}",
                    existing, s
                ));
            }
        } else {
            scope = Some(s);
        }
    }
    Ok(scope.unwrap_or(ReplicaScope::All))
}

fn select_effective_strategy<'a>(
    clusters: &'a BTreeMap<ClusterId, ClusterInfo>,
    cluster_id: ClusterId,
    candidates: &[&'a ParsedStrategy],
) -> Option<&'a ParsedStrategy> {
    // 1) Specific cluster config wins.
    let mut cluster_specific: Vec<&ParsedStrategy> = candidates
        .iter()
        .copied()
        .filter(|s| s.cluster_id == Some(cluster_id))
        .collect();
    if !cluster_specific.is_empty() {
        cluster_specific.sort_by(|a, b| a.id.cmp(&b.id));
        if cluster_specific.len() > 1 {
            warn!(
                cluster_id = %cluster_id,
                strategy_type = %cluster_specific[0].strategy_type,
                num_candidates = cluster_specific.len(),
                "Multiple cluster-specific strategies match; choosing deterministically"
            );
        }
        return Some(cluster_specific[0]);
    }

    // 2) Most-specific matching pattern wins (defined as matching fewer current clusters).
    let mut patterns: Vec<(usize, &ParsedStrategy)> = candidates
        .iter()
        .copied()
        .filter_map(|s| {
            s.cluster_pattern.as_ref().map(|pattern| {
                let count = clusters
                    .values()
                    .filter(|c| ParsedStrategy::matches_pattern(&c.name, pattern))
                    .count();
                (count, s)
            })
        })
        .collect();
    if !patterns.is_empty() {
        patterns.sort_by(|(count_a, a), (count_b, b)| {
            count_a.cmp(count_b).then_with(|| a.id.cmp(&b.id))
        });
        if patterns.len() > 1 && patterns[0].0 == patterns[1].0 {
            warn!(
                cluster_id = %cluster_id,
                strategy_type = %patterns[0].1.strategy_type,
                match_count = patterns[0].0,
                "Ambiguous pattern overlap at runtime; choosing deterministically"
            );
        }
        return Some(patterns[0].1);
    }

    // 3) ALL CLUSTERS default.
    let mut all_clusters: Vec<&ParsedStrategy> = candidates
        .iter()
        .copied()
        .filter(|s| s.cluster_id.is_none() && s.cluster_pattern.is_none())
        .collect();
    if !all_clusters.is_empty() {
        all_clusters.sort_by(|a, b| a.id.cmp(&b.id));
        if all_clusters.len() > 1 {
            warn!(
                cluster_id = %cluster_id,
                strategy_type = %all_clusters[0].strategy_type,
                num_candidates = all_clusters.len(),
                "Multiple ALL CLUSTERS strategies match; choosing deterministically"
            );
        }
        return Some(all_clusters[0]);
    }

    None
}
