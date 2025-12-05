# Auto-Scaling Widget Integration

We want to bring `mz-clusterctl` functionality directly into Materialize. The
external tool implements auto-scaling strategies for clusters (idle suspend,
burst scaling, etc.) but requires separate deployment and management. By
integrating this into `environmentd`, we get faster reaction times, operational
simplicity, and a unified SQL interface for configuration.

This document proposes a "widget" system for hosting semi-autonomous extensions
within `environmentd`, with auto-scaling as the first widget. Widgets run in
their own async tasks, isolated from the coordinator main loop, and communicate
through well-defined interfaces.

## Context

### mz-clusterctl

The external [mz-clusterctl](https://github.com/MaterializeInc/mz-clusterctl)
tool implements four scaling strategies:

| Strategy | Priority | Purpose |
|----------|----------|---------|
| `target_size` | 0 (lowest) | Maintain exactly one replica of specified size |
| `shrink_to_fit` | 1 | Find smallest viable replica size (experimental) |
| `burst` | 2 | Add temporary large replicas during hydration |
| `idle_suspend` | 3 (highest) | Suspend replicas after inactivity period |

The tool monitors activity via `mz_statement_execution_history_redacted`,
hydration status via `mz_hydration_statuses`, and crash information via
`mz_cluster_replica_status_history`. Configuration lives in user tables
(`mz_cluster_strategies`, `mz_cluster_strategy_state`,
`mz_cluster_strategy_actions`).

The main limitation is that it runs externally, typically via cron or GitHub
Actions, polling every few minutes. This means slow reaction times and
operational overhead.

### Materialize Adapter Architecture

We identified these extension points in the adapter:

1. **Message Loop** (`coord.rs`): Multi-channel select with priorities. We can
   add new message types for widget communication.

2. **Background Tasks** (in `serve()`): Pattern for spawning autonomous tasks.
   Examples include the watchdog, storage usage collection, and statement
   logging.

3. **Builtin Tables**: System tables with standard write paths. Widgets will
   use these for all state persistence.

## Goals

- Integrate auto-scaling as a built-in feature with SQL configuration
- Create a reusable widget system for future semi-autonomous extensions
- Zero external dependencies (no separate processes, cron jobs, or GitHub
  Actions)
- Faster reaction time through in-process execution and event-driven reactions

## Non-Goals

- Full plugin system with dynamic loading (widgets are compiled in)
- External API for third-party widget development (internal only, initially)
- Invasive changes to Coordinator core logic; integrations are via existing
  message-loop and background-task patterns

## Implementation

### Widget System

A widget is a semi-autonomous component that:
- Runs in its own async task, isolated from the coordinator main loop
- Receives periodic ticks and system events
- Can observe system state (read catalog, query builtin tables)
- Proposes actions through a channel/an interface (DDL operations, state
  updates)
- Persists all state in builtin tables (no other coordinator state)

The isolation is important: widget bugs or slow operations should not block
critical coordinator functions.

```
┌─────────────────────────────────────────────────────────────────┐
│                         environmentd                            │
│                                                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    Coordinator Task                        │ │
│  │  ┌─────────────────┐    ┌─────────────────────────────┐    │ │
│  │  │  Message Loop   │    │  Widget Command Handler     │    │ │
│  │  └────────┬────────┘    └─────────────┬───────────────┘    │ │
│  └───────────┼───────────────────────────┼────────────────────┘ │
│              │ WidgetToCoord             │ CoordToWidget        │
│  ┌───────────┼───────────────────────────┼────────────────────┐ │
│  │           ▼                           ▼                    │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │              Widget Task (isolated)                  │  │ │
│  │  │  ┌───────────┐  ┌───────────┐  ┌──────────────────┐  │  │ │
│  │  │  │Tick Timer │  │Event Recv │  │Auto-Scaling Logic│  │  │ │
│  │  │  └───────────┘  └───────────┘  └──────────────────┘  │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │                      Widget Runtime                        │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

The `Widget` trait defines the interface (see [Appendix
A](#appendix-a-widget-trait-definition) for full definition):

```rust
#[async_trait]
pub trait Widget: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn tick_interval(&self) -> Duration;
    async fn initialize(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;
    async fn tick(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;
    async fn on_event(&mut self, event: WidgetEvent, env: &WidgetEnv) -> Result<(), WidgetError>;
    async fn shutdown(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;
}
```

Widgets receive a `WidgetEnv` that provides both read-only context and write
capabilities:

```rust
pub struct WidgetEnv {
    pub ctx: WidgetContext,          // Read-only: catalog, table_reader, now
    pub coord: CoordinatorHandle,    // Write: execute_ddl, write_to_table
}
```

Widgets communicate with the coordinator through `WidgetRequest` messages:
execute DDL, write to builtin tables, delete from builtin tables. All writes go
through the same code paths as user write queries to maintain internal
consistency of tables.

### Widget Lifecycle

1. **Coordinator starts**: Builtin tables for widgets are created/loaded.
2. **Widget construction**: `WidgetRuntime` constructs each registered widget.
3. **Initialization**: `widget.initialize(env)` is called. The widget validates
   its configuration and prepares for operation. If `initialize` fails, the
   widget is disabled but `environmentd` continues running.
4. **Main loop**: Runtime runs a single async task per widget:
   ```rust
   loop {
       select! {
           _ = tick_timer => { widget.tick(&env).await?; }
           event = event_rx.recv() => { widget.on_event(event, &env).await?; }
           _ = shutdown_rx => { widget.shutdown(&env).await?; break; }
       }
   }
   ```
5. **Shutdown**: On coordinator shutdown, `widget.shutdown(env)` is called with
   a timeout to allow graceful cleanup.

**Concurrency guarantee**: Because all widget methods are called from a single
async task, `initialize`, `tick`, `on_event`, and `shutdown` are never executed
concurrently for a given widget instance.

### Widget State Persistence

Widgets store all their state in builtin tables. There are no other new data
structures in the coordinator (neither in-memory nor in the durable catalog).

**Builtin tables are the source of truth.** Widgets must not cache state
in-memory across ticks. On every tick (and on every event), the widget reads
its current state from builtin tables to ensure it has an up-to-date view. This
is critical because:

- State may have been modified by user SQL (e.g., `ALTER SCALING STRATEGY`)
- Another `environmentd` instance may have modified state (in HA scenarios)
- Restart recovery is automatic—no special "reload from disk" logic needed

The pattern for each tick is:
1. Read current configuration from `mz_scaling_strategies`
2. Read current state from `mz_scaling_strategy_state`
3. Collect signals from system tables
4. Make decisions
5. Execute actions
6. Write updated state back to `mz_scaling_strategy_state`

For example, with the auto-scaling widget, state is stored in
`mz_internal.mz_scaling_strategy_state` with a composite primary key of
`(strategy_id, cluster_id)`:

```sql
CREATE TABLE mz_internal.mz_scaling_strategy_state (
    strategy_id text NOT NULL,
    cluster_id text NOT NULL,          -- Per-cluster state, even for ALL CLUSTERS strategies
    state_version int4 NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (strategy_id, cluster_id)
);
```

For `ALL CLUSTERS` or pattern-based strategies, each affected cluster gets its
own state row. This enables:
- Per-cluster cooldown tracking
- Per-cluster pending replica tracking
- Clean deletion when a cluster is dropped
- Easy querying of state per cluster

### Auto-Scaling Widget

The auto-scaling widget implements the four strategies from `mz-clusterctl`.
Each strategy has a priority, and they run in priority order with
higher-priority strategies able to override earlier decisions.

**Replica Management Modes**: The widget supports two modes via the
`REPLICA_SCOPE` option:

- `REPLICA_SCOPE = 'ALL'` (default): The widget manages all replicas in the
  cluster. It will create, drop, and resize any replica as needed. Users should
  not manually manage replicas in this mode.

- `REPLICA_SCOPE = 'MANAGED_ONLY'`: The widget only manages replicas it creates,
  identified by a `_auto` suffix in the replica name (e.g., `r1_auto`,
  `burst_auto`). User-created replicas are left untouched. This mode is safer
  for initial rollout and allows coexistence of manual and automated replicas.

**Strategy Coordination**: Strategies are sorted by priority ascending and
executed in that order. Each strategy sees and may modify the
`DesiredReplicaState` constructed by previous strategies. For example,
`idle_suspend` (priority 3) can suspend all replicas regardless of what
`target_size` decided.

**Single Config Per Strategy Type**: Only one configuration of each strategy
type is allowed per cluster. Attempting to create a second `TARGET_SIZE`
strategy for the same cluster will error.

**Strategy Resolution Precedence**: When multiple strategies could apply to a
cluster (specific cluster vs ALL CLUSTERS vs pattern), we use deterministic
precedence:

1. Specific cluster strategy (highest priority)
2. Pattern-based strategy (`ALL CLUSTERS LIKE 'prod_%'`)
3. `ALL CLUSTERS` strategy (lowest priority)

For overlapping patterns, the more specific pattern wins (defined as the pattern
that matches fewer clusters). If truly ambiguous, `CREATE SCALING STRATEGY`
errors with "ambiguous pattern overlap".

### Signal Collection

The widget collects signals to inform scaling decisions:

- **Activity**: Seconds since last query on the cluster (from
  `mz_statement_execution_history_redacted`)
- **Hydration**: Per-replica hydration status for all objects (indexes, MVs,
  sources)
- **Crashes**: Recent crash/OOM information within a configurable window

For hydration, we must use LEFT JOINs from object catalog tables to
`mz_hydration_statuses`. This ensures objects without a hydration status entry
are counted as not hydrated. The external tool only tracked indexes and Kafka
sources, which is incomplete.

### Idempotence and Failure Handling

Scaling decisions must be idempotent. The decision pipeline works as follows:

1. `SignalCollector` builds `ClusterSignals` from system tables
2. `StrategyCoordinator` runs strategies (priority order) to build `DesiredReplicaState`
3. Compare `DesiredReplicaState` with actual cluster replicas
4. Compute a set of actions (DDL statements)
5. For each action:
   - Insert a row into `mz_scaling_actions` with `executed = false`
   - Execute the DDL via coordinator (same code path as user DDL)
   - Update `executed = true` or set `error_message`

The state update logic produces zero actions when the current cluster state
already matches desired. Action execution re-validates against the current
catalog before executing.

On restart, the widget reconstructs state from builtin tables and resumes
operation. Partially-executed actions are handled by idempotent DDL semantics:
if the widget decides to create a replica but crashes before logging success,
on restart it will attempt to create the replica again, which either succeeds
(replica didn't exist) or is a no-op (replica already exists).

**Source of truth**: `mz_scaling_strategy_state` is the single source of truth
for widget state between ticks. `mz_scaling_actions` is purely an audit log—the
widget never reads from it except for informational `SHOW` queries.

Widgets must not cause `environmentd` to crash. Errors are logged, metrics
incremented, and operation continues.

### Safety Mechanisms

**Global kill switch**: A system variable allows disabling all auto-scaling:

```sql
ALTER SYSTEM SET auto_scaling_enabled = false;
```

When disabled, the widget:
- Still updates internal state and computes desired actions
- Writes actions to `mz_scaling_actions` with `executed = false`
- Does **not** send DDL to the coordinator
- Logs "auto-scaling disabled, would have executed: ..."

This gives us a fast way to stop a misbehaving widget without dropping
configurations.

**Per-cluster cooldown**: Each strategy has a configurable `COOLDOWN`. The
widget also enforces a global per-cluster cooldown (default 30s) between any
DDL actions on a given cluster, preventing rapid oscillation.

**Action rate limiting**: No more than 10 actions per 5 minutes per cluster.
This is a hard safety limit to prevent runaway scaling.

### Permissions

**Strategy management**: `CREATE/ALTER/DROP SCALING STRATEGY` requires `USAGE`
privilege on the target cluster(s). For `ALL CLUSTERS` strategies, the user
must have `USAGE` on all existing clusters, or be a superuser.

**Widget DDL execution**: When the widget executes DDL (CREATE/DROP REPLICA),
it runs with system privileges (similar to cluster scheduling). This is safe
because:
- Users explicitly opt into auto-scaling by creating strategies
- Users can only create strategies for clusters they have `USAGE` on
- The widget only affects clusters with configured strategies

### Feature Flag

The auto-scaling widget and all associated SQL commands are gated behind a
feature flag. When the flag is disabled (the default), the SQL commands return
an error and the widget does not run.

The feature flag is defined using the `feature_flags!` macro in
`src/sql/src/session/vars/definitions.rs`:

```rust
feature_flags!(
    // ... existing flags ...
    {
        name: enable_auto_scaling,
        desc: "auto-scaling strategies for clusters",
        default: false,
        enable_for_item_parsing: true,
    },
);
```

This generates:
- `ENABLE_AUTO_SCALING_VAR`: The underlying `VarDefinition`
- `ENABLE_AUTO_SCALING`: A `FeatureFlag` for use with `scx.require_feature_flag()`

**SQL command gating**: Each scaling strategy SQL command checks the feature
flag during planning:

```rust
// In plan_create_scaling_strategy()
scx.require_feature_flag(&vars::ENABLE_AUTO_SCALING)?;
```

**Widget gating**: The widget runtime checks the flag before starting:

```rust
// In WidgetRuntime::start()
if !ENABLE_AUTO_SCALING.get(&dyncfg) {
    tracing::info!("auto-scaling widget disabled by feature flag");
    return;
}
```

**Enabling the feature**: Operators enable auto-scaling via:

```sql
ALTER SYSTEM SET enable_auto_scaling = true;
```

This can also be set via LaunchDarkly for gradual rollout.

## SQL Syntax

Scaling strategies are managed through dedicated SQL commands, separate from
cluster DDL:

```sql
-- Create a scaling strategy for a specific cluster
CREATE SCALING STRATEGY FOR CLUSTER my_cluster
    TYPE TARGET_SIZE
    WITH (SIZE = '200cc', REPLICA_NAME = 'main');

CREATE SCALING STRATEGY FOR CLUSTER my_cluster
    TYPE IDLE_SUSPEND
    WITH (IDLE_AFTER = '30m', COOLDOWN = '5m');

-- Create a scaling strategy for ALL clusters
CREATE SCALING STRATEGY FOR ALL CLUSTERS
    TYPE IDLE_SUSPEND
    WITH (IDLE_AFTER = '1h');

-- Use MANAGED_ONLY mode for safer rollout
CREATE SCALING STRATEGY FOR CLUSTER my_cluster
    TYPE TARGET_SIZE
    WITH (SIZE = '200cc', REPLICA_SCOPE = 'MANAGED_ONLY');

-- Modify strategy options (partial update, only specified keys change)
ALTER SCALING STRATEGY FOR CLUSTER my_cluster
    TYPE TARGET_SIZE
    UPDATE (SIZE = '400cc');

-- Enable/disable without removing
ALTER SCALING STRATEGY FOR CLUSTER my_cluster
    TYPE IDLE_SUSPEND
    UPDATE (ENABLED = false);

-- Remove a strategy
DROP SCALING STRATEGY FOR CLUSTER my_cluster TYPE TARGET_SIZE;

-- Remove ALL scaling strategies from a cluster
DROP ALL SCALING STRATEGIES FOR CLUSTER my_cluster;

-- Inspect configuration
SHOW SCALING STRATEGIES;
SHOW SCALING STRATEGIES FOR CLUSTER my_cluster;
SHOW SCALING ACTIONS FOR CLUSTER my_cluster LIMIT 10;
```

`ALTER SCALING STRATEGY ... UPDATE` performs a partial update: only specified
options are changed, existing options retain their values.

See [Appendix B](#appendix-b-ast-definitions) for AST definitions and [Appendix
C](#appendix-c-parser-additions) for parser implementation.

## Writable Builtin Tables

Builtin tables in Materialize are typically read-only system tables managed
internally via `BuiltinTableUpdate` operations. However, the scaling strategy
tables need to support direct SQL writes (INSERT/UPDATE/DELETE) so that:

1. Users can directly manipulate strategy configurations if needed
2. The DROP/ALTER SCALING STRATEGY commands can be implemented using standard
   SQL DML rather than custom retraction logic
3. Future debugging and operational tasks are simplified

### Implementation

We extended the `BuiltinTable` struct with an optional `writable` flag:

```rust
pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    // ... other fields ...
    /// Whether this builtin table supports writes via INSERT/UPDATE/DELETE.
    /// Most builtin tables are read-only and managed internally, but some
    /// (like mz_scaling_strategies) can be modified by users.
    pub writable: bool,
}
```

For writable builtin tables, we:

1. **Track writability in the catalog**: Added `is_writable_builtin: bool` to
   the `Table` struct in `CatalogItem`. This is set from the `BuiltinTable`
   definition when the catalog is initialized.

2. **Added catalog trait method**: `is_writable_builtin_table()` on
   `CatalogItem` allows the SQL planner to check if a table allows writes.

3. **Modified SQL planning**: The INSERT, UPDATE, DELETE, and COPY planning
   code checks `is_writable_builtin_table()` and allows writes to system tables
   that are marked writable:

   ```rust
   // Block writes to system tables, except for writable builtin tables
   if table.id().is_system() && !table.is_writable_builtin_table() {
       sql_bail!("cannot insert into system table '{}'", table_name);
   }
   ```

### Writable Tables

The following builtin tables are marked as writable:

| Table | Purpose |
|-------|---------|
| `mz_scaling_strategies` | Strategy configuration (user-managed) |
| `mz_scaling_strategy_state` | Per-cluster strategy state (widget-managed) |
| `mz_scaling_actions` | Audit log of scaling actions |

### Usage

With writable builtin tables, users can directly interact with the tables:

```sql
-- Direct manipulation (alternative to SQL commands)
INSERT INTO mz_internal.mz_scaling_strategies (id, cluster_id, strategy_type, ...)
VALUES (...);

DELETE FROM mz_internal.mz_scaling_strategies
WHERE cluster_id = 'u1' AND strategy_type = 'target_size';

UPDATE mz_internal.mz_scaling_strategies
SET config = config || '{"size": "400cc"}'::jsonb
WHERE id = 'strategy-123';
```

The dedicated SQL commands (CREATE/ALTER/DROP SCALING STRATEGY) are preferred
for normal use, but direct SQL provides flexibility for:
- Bulk operations
- Complex conditional updates
- Debugging and recovery scenarios

### Considerations

- **No schema enforcement**: Unlike the SQL commands which validate options,
  direct SQL writes bypass validation. Users must ensure JSON config is valid.
- **Differential semantics**: Builtin tables use differential dataflow under
  the hood. Direct DELETE/UPDATE operations work correctly via the standard
  `ReadThenWrite` execution path.
- **Permissions**: Users still need appropriate privileges. The system table
  check is bypassed, but other permission checks (schema access, etc.) apply.

### Internal Implementation Warning

**Internal code (including widgets) must NOT directly manipulate
`BuiltinTableUpdate` or write raw differential updates to builtin tables.**

Direct manipulation bypasses transactional consistency checks and can lead to
negative accumulations in tables.

Instead, all internal table modifications should go through the standard SQL
execution paths (INSERT/UPDATE/DELETE), which use the `ReadThenWrite`
infrastructure to ensure proper transactional semantics. See [Appendix
D](#appendix-d-widget-coordinator-communication) for details on how widgets
should handle table modifications.

## Builtin Tables

```sql
-- Scaling strategy configuration (user-facing)
CREATE TABLE mz_internal.mz_scaling_strategies (
    id text NOT NULL PRIMARY KEY,
    cluster_id text,                    -- NULL for "all clusters" strategies
    cluster_pattern text,               -- Pattern for LIKE-based strategies
    strategy_type text NOT NULL,
    config jsonb NOT NULL,              -- Versioned: {"config_version": 1, ...}
    enabled bool NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

-- Strategy state (internal, per-cluster)
CREATE TABLE mz_internal.mz_scaling_strategy_state (
    strategy_id text NOT NULL,
    cluster_id text NOT NULL,           -- Per-cluster state
    state_version int4 NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (strategy_id, cluster_id)
);

-- Action log (audit trail)
CREATE TABLE mz_internal.mz_scaling_actions (
    action_id bigint NOT NULL PRIMARY KEY,
    strategy_id text NOT NULL,
    cluster_id text NOT NULL,
    action_type text NOT NULL,          -- 'create_replica', 'drop_replica', 'resize_replica'
    action_sql text NOT NULL,
    reason text NOT NULL,
    executed bool NOT NULL,
    error_message text,
    created_at timestamptz NOT NULL
);
```

**Config JSON schema**: The `config` column uses a versioned JSON format:

```json
{
  "config_version": 1,
  "size": "200cc",
  "cooldown_s": 60,
  "replica_name": "main",
  "replica_scope": "ALL"
}
```

The `config_version` field enables safe schema evolution. On version mismatch,
the widget logs a warning and attempts migration.

**Retention policy**: The `mz_scaling_actions` table implements retention from
day 1:
- Default: 7 days or 10,000 rows per cluster (whichever is smaller)
- Configurable via `ALTER SYSTEM SET auto_scaling_action_retention = '14 days'`
- GC runs periodically as part of widget tick

When a cluster is dropped, associated strategy and state rows are deleted.
Action rows are retained for audit purposes (subject to retention policy).

**Convenience view**: For easier debugging:

```sql
CREATE VIEW mz_internal.mz_scaling_strategies_effective AS
SELECT
    s.id,
    s.strategy_type,
    COALESCE(s.cluster_id, st.cluster_id) as effective_cluster_id,
    s.config,
    s.enabled,
    st.payload as state
FROM mz_internal.mz_scaling_strategies s
LEFT JOIN mz_internal.mz_scaling_strategy_state st
    ON s.id = st.strategy_id;
```

## Implementation Phases

### Phase 1: SQL Syntax

1. Add `mz_scaling_strategies` builtin table
2. Add `mz_scaling_actions` audit table and action execution
3. Add `mz_scaling_strategy_state` builtin table
4. Add keywords and AST types to parser
5. Implement planning and execution for CREATE/ALTER/DROP SCALING STRATEGY
6. Implement SHOW SCALING STRATEGIES/ACTIONS

### Phase 2: Widget System Foundation

1. Define `Widget` trait, `WidgetContext`, `WidgetEvent`, `WidgetError` types
2. Implement `WidgetRuntime` for spawning widget tasks
3. Add widget request handling to coordinator message loop

### Phase 3: Auto-Scaling Widget Core

1. Implement `SignalCollector` for querying system tables
2. Implement `TargetSizeStrategy` with `StrategyCoordinator`
3. Implement remaining strategies: `BurstStrategy`, `IdleSuspendStrategy`, `ShrinkToFitStrategy`

### Phase 4: Event-Driven Scaling

1. Hook into replica status, hydration, and crash events
2. Implement `on_event` handlers for immediate reaction

### Phase 5: Polish

1. Add metrics and structured logging
2. Add integration tests (testdrive, sqllogictest)
3. Implement retention policy for `mz_scaling_actions`

## Testing Strategy

We will add:
- Unit tests for strategy decision logic
- Integration tests with testdrive (using a test-only command to trigger widget
  ticks)
- SQL logic tests for syntax validation
- Event-driven tests simulating hydration events

### Required Test Scenarios

**1. Basic functionality:**
```
> CREATE CLUSTER scaling_test SIZE = '1'

> CREATE SCALING STRATEGY FOR CLUSTER scaling_test
    TYPE TARGET_SIZE
    WITH (SIZE = '2')

# Trigger widget tick (test-only command)
$ widget-tick auto_scaling

# Verify target size replica was created
> SELECT name, size FROM mz_cluster_replicas
  WHERE cluster_id = (SELECT id FROM mz_clusters WHERE name = 'scaling_test')
name  size
----------
r1    2
```

**2. User DDL conflict:** Verify system converges to deterministic state when
user creates/drops replicas while widget is active. In `REPLICA_SCOPE = 'ALL'`
mode, widget should eventually restore desired state. In `MANAGED_ONLY` mode,
user replicas should be untouched.

**3. Crash/restart mid-action:** Using failpoints, inject failures after
writing `mz_scaling_actions` but before DDL, and after DDL but before updating
state. Verify on restart:
- No duplicate replicas
- Widget resumes correctly
- Idempotent recovery

**4. Cooldown correctness:** Verify strategies skip actions when within cooldown
window and increment `auto_scaling_cooldown_skips_total`.

**5. Pattern strategies:** For `ALL CLUSTERS LIKE 'analytics_%'`:
- Newly created `analytics_foo` cluster automatically picks up strategies
- Dropping the cluster cleans up state rows
- Cluster rename behavior (new name may not match pattern)

**6. Event flood:** Simulate many hydration/crash events. Verify CPU usage is
reasonable and widget doesn't starve ticks indefinitely.

**7. Idempotence property tests:** For random sequences of user DDL, widget
decisions, and failures/restarts, verify invariants:
- No negative replica counts
- No duplicate replica names
- No actions on dropped clusters

## Alternatives

### Keep mz-clusterctl external

We could continue improving the external tool. This doesn't help with reaction
time (still limited by polling interval) and adds operational overhead for
users.

### Full plugin system

We could build a more general plugin system with dynamic loading and external
APIs. This is significantly more complex and not needed for our current use
cases. The widget system can evolve toward this if needed.

## Open Questions

### Resolved

- ~~Should we add a `MANAGED_REPLICAS_ONLY` option?~~ **Yes.** Implemented as
  `REPLICA_SCOPE = 'MANAGED_ONLY'` (see Replica Management Modes).

### Open

1. **Time-of-day profiles**: Should strategies support different configurations
   for different times (e.g., `TARGET_SIZE` for weekdays vs weekends)? This
   could be a future extension.

2. **Dry-run mode**: Should we support `WITH (DRY_RUN = true)` at the strategy
   level? This would compute and log actions without executing DDL, useful for
   validation before enabling.

3. **Cluster rename behavior**: For pattern-based strategies (`ALL CLUSTERS
   LIKE 'prod_%'`), what happens when a cluster is renamed and no longer matches
   the pattern? Options:
   - Strategy stops applying (cluster is now unmanaged)
   - Strategy continues applying (bound by cluster_id, not name)

   Currently leaning toward the first option for predictability.

4. **Interaction with managed clusters**: How do scaling strategies interact
   with clusters that already have `MANAGED` replication? These clusters have
   their own replica management semantics via `ALTER CLUSTER ... SET (SIZE = ...)`.

---

## Appendix A: Widget Trait Definition

```rust
// src/adapter/src/widget/mod.rs

use async_trait::async_trait;

/// A semi-autonomous extension that runs within environmentd.
///
/// Each widget runs in its own async task. The runtime guarantees that
/// `initialize`, `tick`, `on_event`, and `shutdown` are never executed
/// concurrently for a given widget instance—all are called from a single
/// async task using a select! loop.
#[async_trait]
pub trait Widget: Send + Sync + 'static {
    /// Unique identifier for this widget type.
    fn name(&self) -> &'static str;

    /// The interval between periodic ticks for this widget.
    /// This is static for the lifetime of the widget.
    fn tick_interval(&self) -> Duration;

    /// Called during coordinator startup to initialize the widget.
    /// Validates configuration and prepares for operation. Should NOT cache
    /// state in memory—state is read fresh from builtin tables on each tick.
    /// If this fails, the widget is disabled but environmentd continues.
    async fn initialize(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;

    /// Called periodically to perform widget logic.
    /// Each tick reads current state from builtin tables (source of truth),
    /// collects signals, makes decisions, executes actions, and writes state.
    /// Events may trigger earlier evaluation but must respect cooldowns.
    async fn tick(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;

    /// Called when relevant system events occur.
    async fn on_event(&mut self, event: WidgetEvent, env: &WidgetEnv)
        -> Result<(), WidgetError>;

    /// Called during coordinator shutdown with a timeout for graceful cleanup.
    async fn shutdown(&mut self, env: &WidgetEnv) -> Result<(), WidgetError>;
}

/// Environment provided to widgets, combining read and write capabilities.
pub struct WidgetEnv {
    /// Read-only context for observing system state.
    pub ctx: WidgetContext,
    /// Handle for sending requests to the coordinator.
    pub coord: CoordinatorHandle,
}

/// Read-only context provided to widgets for observing the system.
pub struct WidgetContext {
    pub catalog: Arc<dyn SessionCatalog>,
    pub table_reader: BuiltinTableReader,
    pub now: DateTime<Utc>,
}

/// Events that widgets can react to.
pub enum WidgetEvent {
    ClusterReplicasChanged { cluster_id: ClusterId },
    ReplicaHydrationChanged { cluster_id: ClusterId, replica_id: ReplicaId },
    ReplicaCrashed { cluster_id: ClusterId, replica_id: ReplicaId, reason: CrashReason },
    ClusterActivity { cluster_id: ClusterId },
    ConfigurationChanged { widget_name: String, cluster_id: Option<ClusterId> },
}

/// Error types for widget operations.
#[derive(Debug, thiserror::Error)]
pub enum WidgetError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Signal collection failed: {0}")]
    SignalCollection(String),
    #[error("DDL execution failed: {0}")]
    DdlExecution(#[from] AdapterError),
    #[error("State persistence failed: {0}")]
    StatePersistence(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
```

## Appendix B: AST Definitions

```rust
// src/sql-parser/src/ast/defs/statement.rs

/// Target for scaling strategy commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalingStrategyTarget<T: AstInfo> {
    Cluster(T::ClusterName),
    AllClusters,
    AllClustersLike(String),
}

/// CREATE SCALING STRATEGY statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateScalingStrategyStatement<T: AstInfo> {
    pub if_not_exists: bool,
    pub target: ScalingStrategyTarget<T>,
    pub strategy_type: ScalingStrategyType,
    pub with_options: Vec<ScalingStrategyOption<T>>,
}

/// ALTER SCALING STRATEGY statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterScalingStrategyStatement<T: AstInfo> {
    pub target: ScalingStrategyTarget<T>,
    pub strategy_type: ScalingStrategyType,
    pub action: AlterScalingStrategyAction<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterScalingStrategyAction<T: AstInfo> {
    /// UPDATE (option = value, ...) - partial update, merges with existing config
    UpdateOptions(Vec<ScalingStrategyOption<T>>),
}

/// DROP SCALING STRATEGY statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropScalingStrategyStatement<T: AstInfo> {
    pub if_exists: bool,
    pub target: ScalingStrategyTarget<T>,
    pub strategy_type: Option<ScalingStrategyType>, // None = drop all
}

/// Scaling strategy types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalingStrategyType {
    TargetSize,
    ShrinkToFit,
    Burst,
    IdleSuspend,
}

/// Options for scaling strategies.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalingStrategyOption<T: AstInfo> {
    pub name: ScalingStrategyOptionName,
    pub value: WithOptionValue<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalingStrategyOptionName {
    // Common
    Cooldown,
    Enabled,
    ReplicaScope,  // 'ALL' or 'MANAGED_ONLY'
    // Target Size
    Size,
    ReplicaName,
    // Burst
    BurstSize,
    // Idle Suspend
    IdleAfter,
    // Shrink to Fit
    MaxSize,
    MinOomCount,
    MinCrashCount,
    CrashWindow,
}

// Add to Statement enum
pub enum Statement<T: AstInfo> {
    // ... existing variants ...
    CreateScalingStrategy(CreateScalingStrategyStatement<T>),
    AlterScalingStrategy(AlterScalingStrategyStatement<T>),
    DropScalingStrategy(DropScalingStrategyStatement<T>),
    ShowScalingStrategies(ShowScalingStrategiesStatement<T>),
    ShowScalingActions(ShowScalingActionsStatement<T>),
}
```

## Appendix C: Parser Additions

```rust
// src/sql-parser/src/parser.rs

impl<'a> Parser<'a> {
    fn parse_create(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        // ... existing dispatch ...
        } else if self.parse_keywords(&[SCALING, STRATEGY]) {
            self.parse_create_scaling_strategy()
                .map_parser_err(StatementKind::CreateScalingStrategy)
        } else {
        // ...
    }

    fn parse_create_scaling_strategy(&mut self) -> Result<Statement<Raw>, ParserError> {
        let if_not_exists = self.parse_if_not_exists()?;

        self.expect_keyword(FOR)?;
        let target = self.parse_scaling_strategy_target()?;

        self.expect_keyword(TYPE)?;
        let strategy_type = self.parse_scaling_strategy_type()?;

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_scaling_strategy_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        Ok(Statement::CreateScalingStrategy(CreateScalingStrategyStatement {
            if_not_exists,
            target,
            strategy_type,
            with_options,
        }))
    }

    // ALTER SCALING STRATEGY FOR CLUSTER x TYPE y UPDATE (...)
    fn parse_alter_scaling_strategy(&mut self) -> Result<Statement<Raw>, ParserError> {
        self.expect_keyword(FOR)?;
        let target = self.parse_scaling_strategy_target()?;

        self.expect_keyword(TYPE)?;
        let strategy_type = self.parse_scaling_strategy_type()?;

        self.expect_keyword(UPDATE)?;
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_scaling_strategy_option)?;
        self.expect_token(&Token::RParen)?;

        Ok(Statement::AlterScalingStrategy(AlterScalingStrategyStatement {
            target,
            strategy_type,
            action: AlterScalingStrategyAction::UpdateOptions(options),
        }))
    }

    fn parse_scaling_strategy_target(&mut self) -> Result<ScalingStrategyTarget<Raw>, ParserError> {
        if self.parse_keywords(&[ALL, CLUSTERS]) {
            if self.parse_keyword(LIKE) {
                let pattern = self.parse_literal_string()?;
                Ok(ScalingStrategyTarget::AllClustersLike(pattern))
            } else {
                Ok(ScalingStrategyTarget::AllClusters)
            }
        } else {
            self.expect_keyword(CLUSTER)?;
            let name = self.parse_cluster_name()?;
            Ok(ScalingStrategyTarget::Cluster(name))
        }
    }

    fn parse_scaling_strategy_type(&mut self) -> Result<ScalingStrategyType, ParserError> {
        if self.parse_keywords(&[TARGET, SIZE]) {
            Ok(ScalingStrategyType::TargetSize)
        } else if self.parse_keywords(&[SHRINK, TO, FIT]) {
            Ok(ScalingStrategyType::ShrinkToFit)
        } else if self.parse_keyword(BURST) {
            Ok(ScalingStrategyType::Burst)
        } else if self.parse_keywords(&[IDLE, SUSPEND]) {
            Ok(ScalingStrategyType::IdleSuspend)
        } else {
            self.expected("scaling strategy type (TARGET SIZE, SHRINK TO FIT, BURST, IDLE SUSPEND)")
        }
    }

    fn parse_scaling_strategy_option(&mut self) -> Result<ScalingStrategyOption<Raw>, ParserError> {
        let name = if self.parse_keyword(SIZE) {
            ScalingStrategyOptionName::Size
        } else if self.parse_keywords(&[BURST, SIZE]) {
            ScalingStrategyOptionName::BurstSize
        } else if self.parse_keywords(&[MAX, SIZE]) {
            ScalingStrategyOptionName::MaxSize
        } else if self.parse_keywords(&[REPLICA, NAME]) {
            ScalingStrategyOptionName::ReplicaName
        } else if self.parse_keywords(&[REPLICA, SCOPE]) {
            ScalingStrategyOptionName::ReplicaScope
        } else if self.parse_keywords(&[IDLE, AFTER]) {
            ScalingStrategyOptionName::IdleAfter
        } else if self.parse_keyword(COOLDOWN) {
            ScalingStrategyOptionName::Cooldown
        } else if self.parse_keywords(&[MIN, OOM, COUNT]) {
            ScalingStrategyOptionName::MinOomCount
        } else if self.parse_keywords(&[MIN, CRASH, COUNT]) {
            ScalingStrategyOptionName::MinCrashCount
        } else if self.parse_keywords(&[CRASH, WINDOW]) {
            ScalingStrategyOptionName::CrashWindow
        } else if self.parse_keyword(ENABLED) {
            ScalingStrategyOptionName::Enabled
        } else {
            return self.expected("scaling strategy option");
        };

        self.expect_token(&Token::Eq)?;
        let value = self.parse_with_option_value()?;

        Ok(ScalingStrategyOption { name, value })
    }
}
```

New keywords to add to `src/sql-lexer/src/keywords.rs`:

```rust
SCALING,
STRATEGY,
STRATEGIES,
TARGET,
SHRINK,
FIT,
BURST,
IDLE,
SUSPEND,
SCOPE,
WINDOW,
```

## Appendix D: Widget-Coordinator Communication

### Important: Safe Table Modification

**Widgets must NOT directly manipulate `BuiltinTableUpdate` or write raw diffs to
builtin tables.** Direct manipulation bypasses transactional consistency checks
and can lead to inconsistent table state (e.g., negative row counts).

Instead, widgets should use one of these approaches for table modifications:

1. **SQL DML via `ExecuteDml`**: Execute INSERT/UPDATE/DELETE statements through
   the coordinator, which uses the standard SQL execution path with proper
   transactional semantics.

2. **DDL via `ExecuteDdl`**: For operations like creating/dropping replicas,
   use the DDL execution path which handles catalog consistency.

The coordinator ensures all modifications go through the same code paths as user
SQL queries, maintaining internal consistency of tables.

```rust
// src/adapter/src/widget.rs

/// Actions that widgets request from the coordinator.
///
/// All SQL execution goes through the standard planning and execution paths
/// to ensure transactional consistency.
pub enum WidgetAction {
    /// Execute a DDL statement (CREATE/DROP CLUSTER REPLICA).
    ExecuteDdl {
        sql: String,
        reason: String,
    },
    /// Execute a DML statement (INSERT/UPDATE/DELETE) on a writable builtin table.
    ///
    /// This goes through the standard SQL execution path to ensure proper
    /// transactional semantics and table consistency.
    ExecuteDml {
        sql: String,
        reason: String,
    },
}
```

### Data Requests

Widgets can also request data from the coordinator for signal collection:

```rust
/// Requests from widget to coordinator for data.
pub enum WidgetDataRequest {
    /// Read all rows from a builtin table.
    ReadBuiltinTable {
        table_id: CatalogItemId,
        response_tx: oneshot::Sender<Result<Vec<Row>, String>>,
    },
    /// Execute a SQL query (SELECT only).
    ExecuteSql {
        sql: String,
        response_tx: oneshot::Sender<Result<Vec<Row>, String>>,
    },
}
```

## Appendix E: Strategy Definitions

```rust
// src/adapter/src/widget/auto_scaling/strategies.rs

pub trait ScalingStrategy: Send + Sync {
    fn name(&self) -> &'static str;
    fn priority(&self) -> u8;
    fn validate_config(&self, config: &StrategyConfig) -> Result<(), ConfigError>;

    /// Make scaling decision. Modifies `desired_state` in place.
    fn decide(
        &self,
        cluster: &ClusterInfo,
        signals: &ClusterSignals,
        current_state: &mut StrategyState,
        desired_state: &mut DesiredReplicaState,
    ) -> Result<StrategyDecision, StrategyError>;

    fn initial_state(&self) -> StrategyState;
}

pub struct TargetSizeStrategy;   // Priority 0
pub struct ShrinkToFitStrategy;  // Priority 1
pub struct BurstStrategy;        // Priority 2
pub struct IdleSuspendStrategy;  // Priority 3
```

## Appendix F: Signal Types

```rust
// src/adapter/src/widget/auto_scaling/signals.rs

pub struct ClusterSignals {
    pub seconds_since_activity: Option<u64>,
    pub replica_hydration: BTreeMap<ReplicaId, HydrationStatus>,
    pub crash_info: CrashInfo,
    pub available_sizes: Vec<ReplicaSize>,
}

pub struct HydrationStatus {
    pub total_objects: u64,
    pub hydrated_objects: u64,
    pub is_hydrated: bool,
}

pub struct CrashInfo {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub total_crashes: u64,
    pub oom_crashes: u64,
    pub crash_reasons: Vec<String>,
    pub is_crash_looping: bool,
    pub is_oom_looping: bool,
}
```

## Appendix G: Configuration Reference

### Common Options (All Strategies)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `COOLDOWN` | interval | No | varies | Time between decisions |
| `ENABLED` | bool | No | `true` | Whether strategy is active |
| `REPLICA_SCOPE` | string | No | `'ALL'` | `'ALL'` or `'MANAGED_ONLY'` |

### Target Size Strategy

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `SIZE` | string | Yes | - | Target replica size (e.g., '200cc') |
| `REPLICA_NAME` | string | No | `r1_auto` | Name for the managed replica |
| `COOLDOWN` | interval | No | '60s' | Time between decisions |

### Burst Strategy

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `BURST_SIZE` | string | Yes | - | Size of burst replica |
| `COOLDOWN` | interval | No | '60s' | Time between burst decisions |

### Idle Suspend Strategy

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `IDLE_AFTER` | interval | Yes | - | Time before suspending |
| `COOLDOWN` | interval | No | '300s' | Time between suspend decisions |

### Shrink to Fit Strategy (Experimental)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MAX_SIZE` | string | Yes | - | Maximum replica size to try |
| `COOLDOWN` | interval | No | '120s' | Time between shrink decisions |
| `MIN_OOM_COUNT` | int | No | 1 | OOMs before considered looping |
| `MIN_CRASH_COUNT` | int | No | 1 | Crashes before considered looping |
| `CRASH_WINDOW` | interval | No | '1h' | Time window for crash detection |

### Interval Storage

Intervals in SQL (e.g., `IDLE_AFTER = '30m'`) are stored in JSON config as seconds:
- `'30s'` → `{"idle_after_s": 30}`
- `'5m'` → `{"idle_after_s": 300}`
- `'1h'` → `{"idle_after_s": 3600}`

### REPLICA_SCOPE Values

- `'ALL'`: Widget manages all replicas in the cluster. User-created replicas
  may be modified or dropped.
- `'MANAGED_ONLY'`: Widget only manages replicas it creates (identified by
  `_auto` suffix). User-created replicas are untouched.

## Appendix H: File Locations

| Component | Path |
|-----------|------|
| Widget trait and types | `src/adapter/src/widget/mod.rs` |
| Widget runtime | `src/adapter/src/widget/runtime.rs` |
| Widget-coordinator channel | `src/adapter/src/widget/channel.rs` |
| Widget registry | `src/adapter/src/widget/registry.rs` |
| Auto-scaling widget | `src/adapter/src/widget/auto_scaling/mod.rs` |
| Strategies | `src/adapter/src/widget/auto_scaling/strategies/*.rs` |
| Signals | `src/adapter/src/widget/auto_scaling/signals.rs` |
| SQL AST additions | `src/sql-parser/src/ast/defs/statement.rs` |
| Parser additions | `src/sql-parser/src/parser.rs` |
| Builtin table definitions | `src/adapter/src/catalog/builtin.rs` |

## Appendix I: Metrics Reference

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `widget_tick_duration_seconds` | Histogram | `widget` | Time spent in widget tick |
| `widget_tick_errors_total` | Counter | `widget` | Number of tick errors |
| `widget_event_handling_duration_seconds` | Histogram | `widget`, `event_type` | Time spent handling events |
| `auto_scaling_actions_total` | Counter | `strategy`, `action_type`, `status` | Scaling actions taken |
| `auto_scaling_cooldown_skips_total` | Counter | `strategy` | Actions skipped due to cooldown |
| `auto_scaling_signal_collection_duration_seconds` | Histogram | - | Time to collect signals |
| `auto_scaling_rate_limit_hits_total` | Counter | `cluster_id` | Rate limit triggered |

## Appendix J: Structured Logging

For each scaling decision, log a structured event:

```json
{
  "event": "auto_scaling_decision",
  "cluster_id": "u1",
  "strategy_type": "target_size",
  "old_replica_state": {"r1": "100cc"},
  "desired_replica_state": {"r1_auto": "200cc"},
  "actions": [
    {"type": "create_replica", "name": "r1_auto", "size": "200cc"}
  ],
  "reason": "target size not present"
}
```

For failures:

```json
{
  "event": "auto_scaling_action_failed",
  "cluster_id": "u1",
  "strategy_type": "target_size",
  "ddl": "CREATE CLUSTER REPLICA ...",
  "error_message": "size not available in region"
}
```

For global state changes:

```json
{
  "event": "auto_scaling_disabled",
  "reason": "system variable auto_scaling_enabled set to false"
}
```
