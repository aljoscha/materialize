# Spec 1: Components and state ownership

Part of the controllerd design. Companion specs: the adapter and controllerd
protocol (spec 2), the cluster data plane (spec 3), multi-controllerd
cooperation (spec 4). The design doc gives context and rationale. This spec
defines the processes, their responsibilities, identities, and state
ownership. It is normative for implementation but not prescriptive about
internals.

## Processes

- **environmentd** hosts the adapter: SQL frontends, sessions, the catalog,
  planning, and the write stack. There is exactly one read-write adapter per
  environment per deploy generation. Process overlap during restarts and
  failovers is handled by the incarnation ordering below, not assumed away.
- **controllerd** is a new binary hosting the storage controller, the compute
  controller, and storage collections management (critical since handles).
  One instance per environment initially. The design admits multiple
  cooperating instances, for example one per cluster (spec 4).
- **clusterd** is unchanged in role. It gains a data-plane endpoint and a
  peek admission state machine (spec 3).
- **orchestratord** (cloud) or equivalent deployment machinery provisions
  environmentd and controllerd processes, selects the deployment topology,
  and drives promotion. It is outside the scope of these specs except where
  named.

Environmentd and controllerd of the same deploy generation are the same
build version. They parse the same catalog contents and share plan encodings
through the expression cache, which is keyed on build version. This
constraint is enforced, not assumed: session establishment (spec 2) carries
deploy generation and build version, and mismatches are rejected.

### Instance identity and incarnations

Every adapter and controllerd process has a two-part identity:

- A **stable name** (for the adapter: its role in the environment, for
  controllerd instances: the identity under which roles are assigned in
  spec 4). The name survives restarts.
- An **incarnation token**, totally ordered across restarts of the same
  name. For the adapter this is the catalog fence token (deploy generation,
  catalog epoch) it obtained when opening the catalog read-write, which by
  construction fenced its predecessor. For controllerd instances it is
  (deploy generation, per-boot token), where the per-boot token is a
  monotone value ordered across restarts, supplied by deployment machinery.
  Role assignment epochs (spec 4) are per-role fence tokens carried on
  role-scoped actions, they are not process identity: an instance holding
  several roles has one incarnation and several role tokens. The
  incarnation orders sessions and connections only. Critical-handle
  capability is deliberately shared across restarts of the same instance
  name (the successor reuses the same handles, which is what makes
  controllerd restarts cheap), so a lingering predecessor process of the
  same name is not fenced at persist. Its window of stale activity is
  bounded by process supervision and the role-holding freshness bound
  (spec 4), and anything it does in that window degrades to query errors
  by the layering contract (spec 2).

Displacement anywhere in the system is ordered by incarnation: a request or
connection carrying a lower incarnation than the highest seen for the same
name is rejected, a higher one displaces. Nothing in the design uses
last-writer-wins displacement.

Transient GlobalIds are partitioned per (name, incarnation), so a restarted
process never collides with its predecessor's still-draining transient
objects. Transient ids must not appear in durable state or user-visible
relations.

## Responsibilities

### Adapter (environmentd)

- Terminates client connections, runs sessions, plans and sequences SQL.
- Sole writer of the durable catalog. All DDL is a catalog commit. DDL
  success means the catalog commit succeeded. Enactment on clusters is
  asynchronous and converges (spec 2).
- Runs the optimizer at DDL time for indexes and materialized views, and
  writes their optimized plans and metainfo to the expression cache before
  the catalog commit. At boot it re-optimizes existing indexes and
  materialized views exactly as today, which regenerates optimizer notices
  and warms the expression cache for its build version. Sink dataflows are
  not cached and are always optimized by controllerd. Optimizer notices and
  `mz_notices` content come exclusively from the adapter's own optimization
  runs (DDL-time and boot-time). Controllerd re-optimization produces no
  notices.
- Owns the timestamp oracle client. Only the adapter talks to the oracle.
- Owns the write stack: group commit, txn-wal table registration and forget,
  webhook appends, statement logging appends, storage usage collection, and
  all other adapter-originated appends. The write stack also performs
  persist bring-up (opening write handles, registering table shards, schema
  evolution via `ALTER TABLE`) for the collections it writes: tables and
  webhook collections are writable by the adapter as soon as the catalog
  commit lands, independent of controllerd enactment. Controllerd learns
  their descriptions and schema changes from its catalog subscription and
  never sits on the table write path.
- Performs timestamp selection against holds granted by controllerd (spec 2)
  and the frontier mirror, and enforces linearizability via the oracle.
- Issues fast-path index peeks directly to cluster processes and receives
  peek rows, subscribe batches, and COPY TO completions directly (spec 3).
- Executes persist fast-path peeks locally against persist using short-lived
  leased persist read handles. These handles are a bounded, self-expiring
  form of read capability that exists in addition to lease-session holds.
  The adapter must not hold long-lived leased persist handles: their since
  holdback must be bounded by the persist lease timeout, so a dead adapter
  cannot pin compaction beyond that timeout.
- Reads stashed (spilled) peek results from persist and deletes them after
  reading. Stash batches orphaned by an adapter crash are swept by
  controllerd's environment role by age (spec 4).
- Runs the real-time-recency probe library and INSPECT SHARD as local
  libraries against external systems and persist.
- Runs the cluster reconciler: a catalog-level controller that reads
  hydration and replica-status signals from controller-owned persist
  collections and writes durable catalog state (replica create and drop,
  reconfiguration record resolution). The storage and compute controllers in
  controllerd enact what the catalog says.
- Performs pre-commit validation for DDL: resource limits, connection
  validation, description compatibility checks. All user-visible DDL errors
  originate before the catalog commit.

### controllerd

- Follows the durable catalog as a read-only subscriber (a catalog
  follower). Derives all deterministic controller actions from catalog
  diffs: collection creation and drop (except adapter-written collections'
  persist bring-up, above), dataflow installation and removal, cluster and
  replica lifecycle, read policy changes, configuration updates, and
  ingestion or export reconfiguration.
- Resolves catalog items to dataflow plans cache-first: reads the expression
  cache, re-optimizes on miss (always, for sinks). Its plans are
  authoritative for what runs on clusters. It writes every re-optimized
  plan back to the cache, so the cache converges to the running plan, and
  cache entries record their writer, build version, and optimizer
  configuration fingerprint. That recorded metadata is the provenance
  EXPLAIN reports (design doc, user-visible changes).
- Chooses as-ofs for catalog-derived dataflows at apply time. Validates
  adapter-chosen as-ofs for transient dataflows against session holds.
- Owns storage collections management: critical persist since handles, read
  policy enactment, since downgrades, frontier tracking.
- Serves lease sessions and the frontier mirror to adapters (spec 2). These
  are served early in boot, before controller state rebuild completes (see
  boot sequence below).
- Owns the controller-to-cluster control plane: CTP (Cluster Transport
  Protocol) connections to cluster
  processes, command histories, replica reconciliation and rehydration.
- Provisions cluster processes through the orchestrator, including orphan
  and past-generation cleanup, gated on the destructive-action rule of
  spec 4.
- Writes the controller-owned collections through its collection management
  machinery: frontier introspection, replica frontiers, statistics, status
  histories, wallclock lag, replica metrics, compute introspection,
  hydration status, and the replica data-plane endpoint collection (spec 3).
- Runs introspection subscribes against replicas as a data-plane client of
  its own clusters (spec 3) and routes their data into the controller-owned
  collections. The arrangement-size history pipeline moves with them.
- Runs VPC endpoint reconciliation and writes PrivateLink status history.
  The adapter keeps pre-commit connection validation.
- Performs shard finalization under the rules of spec 4.
- Evaluates per-cluster caught-up state for zero-downtime deployments using
  the same criteria as today's implementation (replica write frontiers
  within configured lag of live-collection targets, exclusion of collections
  that cannot advance read-only, and a replica-health stability window) and
  exposes a readiness signal for its deploy generation on its operational
  status endpoint. Deployment machinery aggregates readiness across the
  adapter and all controllerd instances of a generation and drives
  promotion, including any operator-forced skip of the caught-up wait.
- Exposes its own internal HTTP endpoint for state dumps, metrics, and
  profiling, gated by the same authorization posture as environmentd's
  internal routes (not clusterd's ungated model). Replica HTTP proxying
  moves here with the replica connections.

### clusterd

Responsibilities unchanged, plus the data-plane endpoint and peek admission
state machine of spec 3. Spec 3's admission validation is correctness
critical: the availability and "errors, not wrong answers" claims of this
design depend on it, it is not best-effort hardening.

## Network surfaces

| Port | Server | Intended clients | A connection grants |
| --- | --- | --- | --- |
| Adapter protocol (spec 2) | controllerd | the environment's adapter(s) | full-environment read capability (holds), transient dataflow installation, compaction holdback. Authenticated adapter principals only. |
| Control (ctl) ports | clusterd | the owning controllerd | full controller authority over the replica. Authenticated controllerd principals only, ordered by fence token (spec 4). |
| Data plane (spec 3) | clusterd | adapters, controllerd (introspection subscribes) | peek issuance against any installed object, result delivery for owned dataflows. Authenticated, role-gated. |
| Internal HTTP | controllerd | operators, deployment machinery (readiness aggregation) | state dumps, metrics, profiling, replica proxy. Authorization-gated. |

Identity on all of these is authenticated, never client-asserted. The
concrete mechanism (mTLS or platform tokens) is a phase 2 decision, the
requirement is not.

## Durable state ownership

Persist is the shared fabric. Every durable statement below is a persist
shard or a set of persist shards.

| State | Writer | Readers | Notes |
| --- | --- | --- | --- |
| Durable catalog | adapter only | adapter, controllerd (follower) | Items, clusters, replicas, system configuration, shard mappings, unfinalized-shard WAL, txn-wal shard id, role assignments (spec 4). |
| Expression cache | adapter (DDL-time and boot-time), controllerd (re-optimization write-back) | both | Compare-and-append multi-writer safe: entries are keyed by (build version, object, expression kind) and equal keys carry plans for the same object under possibly different optimizer configuration, last write wins and readers re-validate configuration at use. A cache, not a source of truth. |
| User collection shards | cluster processes, adapter write stack (tables, webhooks) | all | Unchanged. |
| txn-wal txns shard | adapter write stack | adapter, controllerd (registration observation), cluster processes (table reads) | Registration membership is deterministic from the catalog: a table is registered iff it exists. Timing of register and forget is adapter-internal. |
| Controller-owned collections | controllerd | all, including adapter-side consumers (cluster reconciler, caught-up aggregation, session notices) and the adapter's data-plane endpoint discovery | Self-correcting differential or writer-scoped append-only (spec 4). |
| Critical since handles | owning controllerd instance | enforced by persist (compare-and-swap on the fencing opaque) | Durable, survive the owning process. v1 uses the existing well-known handle per collection with a widened opaque (spec 4). |
| Timestamp oracle state | oracle service (Postgres/CRDB) | adapter only | Unchanged. |
| Peek stash shards | cluster processes | adapter | Adapter deletes after reading, environment role sweeps orphans by age. |
| Secrets | secrets controller (adapter-side DDL) | adapter (validation, RTR probes), cluster processes (ingestion, export) | **controllerd holds no customer secrets.** It handles secret references only. It holds cloud-provider credentials for orchestration and VPC reconciliation. This is an invariant, not an accident. |

Shard allocation for new collections happens inside adapter catalog
transactions, as today. Controllerd learns mappings from its catalog
subscription and never allocates shards.

## Ephemeral state ownership

| State | Owner | Recovery on owner restart |
| --- | --- | --- |
| Sessions, transactions, portals | adapter | Lost, clients reconnect. Unchanged. |
| Local read hold tokens (multiplexing) | adapter | Rebuilt by fresh lease acquisition (spec 2). |
| Lease sessions | controllerd | Adapters re-register. Freeze rule protects sinces meanwhile (spec 2). |
| Frontier mirror (consumer copy) | adapter | Retained and served from across controllerd restarts, re-synced when the new stream catches up (spec 2). |
| Controller command histories, instance state | controllerd | Re-derived from catalog plus persist frontiers, replica-side reconciliation preserves compatible dataflows. |
| Transient dataflows (slow-path peeks, subscribes, COPY TO) | the issuing lease session (controllerd-side bookkeeping) | Lost on controllerd restart, affected operations fail with retryable errors. |
| Pending data-plane peeks and delivery buffers | (client name, incarnation, data-plane connection) on the replica | Survive controllerd restarts and control-plane reconciliation (spec 3). Torn down by data-plane connection loss or target removal, always with an error response. |
| Watch-set equivalents, DDL waits | adapter | Local waits against the mirror, rebuilt from catalog plus mirror. |

## Boot sequences

### Adapter boot

1. Open the durable catalog read-write (obtains the fence token that is its
   incarnation, fences the predecessor).
2. Start the oracle, write stack, and frontends. Table writes and
   persist-backed reads are servable from here on.
3. Connect to controllerd, establish a lease session (displacing any
   predecessor session immediately), install standing holds as one batched
   request, and begin consuming the mirror.
4. Boot-time re-optimization of indexes and materialized views (notices and
   cache warming) proceeds in the background.
5. Serve queries. Index fast-path peeks additionally require data-plane
   endpoints and holds, see failure semantics in spec 2 for the degraded
   mode when controllerd is unavailable during adapter boot.

The adapter boot never blocks on controllerd availability or catch-up as a
whole. Gating is per-object (mirror appearance, per-request catalog_ts),
though command gating waits on controllerd's instance-global applied
frontier reaching the request's catalog_ts (spec 2).

### controllerd boot

1. Open the catalog as a follower, snapshot, begin streaming.
2. Acquire critical since handles for its roles (spec 4). Enact no since
   downgrades yet (freeze rule, spec 2).
3. **Serve lease sessions and the mirror now.** Hold grants require only
   handle state. Mirror entries stream incrementally as state rebuilds.
4. Rebuild controller state from the catalog snapshot: collections,
   dataflow plans (cache-first), cluster and replica lifecycle, as-of
   selection with existing outputs as hard constraints (spec 4).
5. Connect to cluster processes. Replica-side reconciliation preserves
   compatible running dataflows and does not disturb pending data-plane
   peeks (spec 3).

The gap between controllerd downtime and step 3 is the quantity the lease
TTL must cover (spec 2). Keeping step 3 early and cheap is a normative
requirement, not an optimization.

### clusterd boot

Unchanged, plus the data-plane endpoint accepts connections independently of
the control connection (spec 3).

## Observability requirements

The design makes enactment asynchronous, so observability of enactment is a
correctness requirement of the operational model, not polish. Normative
requirements:

- Both protocols carry trace context (the existing OpenTelemetry propagation
  on peeks must survive the split) and the following correlation ids where
  applicable: peek id, lease session (name, incarnation), transient
  GlobalId, `catalog_ts` (spec 2), statement logging id.
- The adapter exposes: applied-catalog-ts lag (its own catalog upper versus
  the mirrored applied timestamp), per-collection mirror staleness age,
  lease time-to-expiry and renewal round-trip histograms, counts of
  mirror-gating waits, admission-deadline errors, and ephemeral command
  rejections by reason.
- controllerd exposes: applied-catalog-ts and its lag versus the catalog
  upper, per-DDL enactment latency (catalog commit to mirror appearance),
  lease session counts and hold volumes, freeze state and its duration,
  fence events (role, epoch), and catalog-follower reopen and resync counts.
- Cluster processes expose: held-peek counts and ages, admission deadline
  expiries, data-plane connection counts by client, delivery buffer
  occupancy and terminations.

Alert conditions and thresholds derive from these and ship with phase 2
(design doc, rollout).

## Testing topology

Controllerd is embeddable: library mode runs the controllerd stack
in-process, speaking the real spec 2 protocol over in-memory transport, so
protocol code is exercised even in single-process harnesses. Library mode
(embedded topology) is also a supported production configuration and the
topology rollback path (design doc, rollout), not a test-only artifact.

## Prerequisite workstreams

These are prerequisites, tracked as phase 0 of the rollout, not re-specified
here:

1. Eliminate the remaining sequencer side-effect closures. Index, sink, and
   materialized view creation must flow through the catalog-implications
   pipeline. The DDL-transaction commit replay of closures must be gone.
2. Report subscribe and COPY TO progress through frontier responses on the
   control protocol so the data plane can carry data only.
3. Factor catalog state resolution (apply, parse) and the optimizer out of
   the adapter crate so controllerd can depend on them.
4. A production catalog-follower abstraction: read-only open, continuous
   sync, and transparent handling of same-generation epoch fences with two
   resume modes: continue from the follower's frontier when the catalog
   shard's since permits, otherwise full snapshot plus diff against applied
   state. Neither mode disturbs enacted controller state, resync produces
   the same implications a continuous stream would have.
