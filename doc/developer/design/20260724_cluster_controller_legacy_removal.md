# Make the cluster controller the sole owner of managed clusters

- Associated (controller work, merged): #37214, #37452, #37671, #37767
- Associated (latent bug context): #31452, #32188, commit `76ba9e793f`

## The Problem

The cluster controller is merged and, in production, owns the replica set of
every *user* managed cluster. But it lives behind temporary flags and next to
the paths it was meant to replace. Three things still coexist with it:

1. **The legacy REFRESH scheduler** (`src/adapter/src/coord/cluster_scheduling.rs`),
   driven by a coordinator timer, superseded by the controller's
   `OnRefreshStrategy`. It is a no-op whenever the controller is on.
2. **The legacy staged reconfiguration machine** (the `WaitForHydrated` and
   `Finalize` cluster stages, the `-pending` overlap replicas, and their
   connection-lifecycle cleanup), superseded by the controller's durable
   reconfiguration record plus the `AwaitReconfiguration` wait-shim.
3. **A system-cluster exception.** The controller deliberately excludes
   system/builtin clusters (`ManagedClusterIds` filters `is_user()`). Their
   replica set is instead reconciled at catalog-open by
   `add_new_remove_old_builtin_cluster_replicas_migration`, from the hardcoded
   `BUILTIN_CLUSTER_REPLICAS` list.

The whole arrangement is gated by `ENABLE_CLUSTER_CONTROLLER` (a break-glass
dyncfg, default on), so we carry two implementations of the same behavior.

The system-cluster exception also hides a latent correctness bug. When builtin
clusters were switched from unmanaged to managed (`76ba9e793f`), they gained a
`replication_factor` field, but the replica migration was not rewired to it. It
still reconciles the actual replicas to the hardcoded single-`r1`-per-cluster
list, gated only on `replication_factor > 0` as an on/off switch. Nothing
reconciles the two notions of "desired replicas":

- `ALTER CLUSTER mz_system SET (REPLICATION FACTOR 2)` is accepted (shared
  managed path, no `is_system` guard) and creates `r2` at runtime, which the
  next reboot reaps.
- the bootstrap flags accept `range(0..=2)`, and #32188 sets the builtin system
  and probe factors to 2 in the mzcompose harness, intending multiple replicas.
- the user default cluster honors its factor at init
  (`initialize.rs`, `for i in 0..default_cluster_replication_factor`), while the
  builtin migration ignores factors above one.

So a builtin cluster can durably carry `replication_factor = 2` while running a
single replica, with no consistency check and no repair. This has been live in
CI since #32188 without anyone noticing, because no test asserts a builtin
cluster's replica count against its factor for the `> 1` case.

## Success Criteria

- The cluster controller is the single owner of the replica set of *all*
  managed clusters, user and system alike.
- The legacy REFRESH scheduler, the legacy staged reconfiguration machine, and
  the system-cluster special case are deleted, not fenced off.
- `ENABLE_CLUSTER_CONTROLLER` and the legacy scheduling interval var are
  removed.
- A managed cluster's stored config (size and `replication_factor`) is the
  single source of truth for its replica set. The config-vs-replica desync is
  gone by construction, and `replication_factor > 1` works for builtin clusters.
- Boot ordering and 0dt read-only behavior are preserved: system clusters have
  their replicas up and hydrated before the serve loop starts and before a 0dt
  cutover.

## Out of Scope

- Removing the controller sub-behavior kill-switches `ENABLE_BACKGROUND_ALTER_CLUSTER`
  and `ENABLE_HYDRATION_BURST`. They gate controller behaviors, not legacy
  paths, and should be retired separately after burn-in.
- Removing the feature-acceptance flags `enable_cluster_schedule_refresh` and
  `enable_auto_scaling_strategy`. Those gate user-facing SQL surface under a
  staged rollout.
- Removing the durable `pending` field on `ReplicaLocation::Managed`. It becomes
  vestigial (always false) once the staged machine is gone, but dropping it is a
  catalog/proto migration and belongs in its own change.

## Solution Proposal

The core move is a reframing: the managed config is authoritative for a
cluster's replica set, and materializing replicas is convergence toward it. The
controller does that convergence at runtime. A catalog-open step does it eagerly
at boot, for the windows where the controller cannot run. Both derive the target
from the same config, so they agree by construction and there is no dual writer.

The change has four parts. They are separable, but the payoff (deleting the
special case and fixing the desync) needs Part A.

### Part A: config as the single source of truth for system clusters

Rework `add_new_remove_old_builtin_cluster_replicas_migration` so that, per
builtin cluster, it converges the durable replica set to the target implied by
the managed config (`replication_factor` replicas of the configured size),
rather than to the hardcoded `BUILTIN_CLUSTER_REPLICAS` set. The target
computation should be shared with the controller's baseline strategy so the two
cannot drift. `replication_factor = 0` (support and analytics by default) yields
zero replicas, subsuming the "conditionally create" feature. Replica names are
derived (`r1..rN`) exactly as user clusters derive them.

This migration stays because it is load-bearing in two windows the controller
cannot cover:

- **Boot.** `Coordinator::bootstrap` brings up only replicas already present in
  the durable catalog, and it runs before the controller task is spawned. System
  replicas must exist durably at open or the system boots with no served
  introspection.
- **0dt read-only.** The controller is inactive in read-only mode. The
  pre-promotion environmentd must have system clusters up and hydrated before
  cutover. The migration, running in savepoint mode, provides that.

Because the migration and the controller now compute the same target, the
controller has nothing to do for a system cluster whose replicas already match,
so read-only inactivity is fine and reboot no longer reaps a config-implied
replica.

### Part B: controller owns system clusters at runtime

Drop the `is_user()` conjunct from `ManagedClusterIds` and from the two
`controller_owns` computations in the sequencer. Runtime ALTERs of system
clusters then flow through the controller reshape path like any other managed
cluster.

### Part C: remove `ENABLE_CLUSTER_CONTROLLER` and the legacy scheduler

Delete the dyncfg, the whole legacy REFRESH scheduler and its plumbing
(`Message::CheckSchedulingPolicies` / `Message::SchedulingDecisions`, the
coordinator timer and select-loop tick, the two scheduling metrics), the legacy
interval var `cluster_check_scheduling_policies_interval`, and the audit-only
leftovers `ReplicaCreateDropReason::ClusterScheduling` and
`SchedulingDecision`/`RefreshDecision`. The controller's `OnRefresh` audit path
and the persisted `SchedulingDecisionsWithReasonsV2` proto stay.

### Part D: delete the legacy staged reconfiguration machine

With both user and system clusters on the controller, no ALTER reaches the
staged machine. Delete the `WaitForHydrated` and `Finalize` stages and their
structs and handlers, `NeedsFinalization`, `PENDING_REPLICA_SUFFIX`, the
`pending_cluster_alters` connection state and its retire path
(`drop_reconfiguration_replicas`, `retire/cancel_cluster_reconfigurations_for_conn`),
and the `AlterClusterWhilePendingReplicas` error. `AlterClusterPlanStrategy`
stays, it is consumed by the controller reshape path. `remove_pending_cluster_replicas_migration`
likely becomes removable too.

## Behavior changes

- **`replication_factor > 1` on builtin clusters now takes effect and persists.**
  This is the desync fix, and it gives real HA for mz_system, which #32188 and
  the `0..=2` flag range were already reaching for.
- **`ALTER CLUSTER <system> SET (SIZE ...)` becomes graceful / zero-downtime**
  via the controller reshape path, instead of today's immediate recreate. This
  is an improvement for the catalog server, but it is a semantics change, and it
  means system clusters start using reconfiguration records.
- **Audit reason and timing for boot-created builtin replicas.** The migration
  should keep emitting a `System` create reason at boot so existing audit
  expectations hold, rather than the controller's `Manual` at first tick.

## Alternatives

- **Keep the system-cluster exception, only reject `WITH (WAIT ...)` on system
  clusters, and delete the staged machine.** Smaller. But it keeps the special
  case and the latent desync, and it rejects a currently-accepted statement. The
  staged machine would be gone but the two-source-of-truth split for builtin
  replicas would remain.
- **Keep the system-cluster exception unchanged, delete only the legacy
  scheduler.** Smallest. Leaves both the staged machine (reachable for system
  clusters) and the desync in place.

Both were rejected because they leave the exception, and with it the latent bug,
in the tree.

## Open questions

- The exact mechanism for sharing the target computation between the catalog-open
  migration and the controller baseline strategy. The migration runs against a
  durable `Transaction` with no coordinator, so the shared piece must be a pure
  function over cluster config, not a controller method.
- Whether ALTER of a system cluster should really become graceful, or whether we
  want to keep an immediate-recreate path for builtins specifically.
- Semantics of the ephemeral support and analytics clusters (default
  `replication_factor = 0`) under controller ownership, including spin-up on
  demand.
- Whether to fold the `pending` durable-field removal and
  `remove_pending_cluster_replicas_migration` into this change or a follow-up.
