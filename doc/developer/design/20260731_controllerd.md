# controllerd: Splitting the Controllers out of environmentd

## Summary

We split the storage controller, the compute controller, and storage
collections management out of `environmentd` into a new binary,
`controllerd`. The adapter and the controllers get independent restart and
failure domains and independently sizable processes (their versions deploy
in lockstep within a generation), query serving is decoupled from
controller availability, and the architecture gains a path to multiple
cooperating controllerd instances, for example one per cluster.

This document gives context, rationale, user-visible changes, and the
rollout plan. The normative content lives in four companion specs:

1. [Components and state ownership](controllerd/01-components-and-state.md)
2. [The adapter and controllerd protocol](controllerd/02-adapter-controllerd-protocol.md)
3. [The cluster data plane](controllerd/03-cluster-data-plane.md)
4. [Multi-controllerd cooperation](controllerd/04-multi-controllerd.md)

## Context and problem

`environmentd` today hosts both the adapter (SQL frontends, sessions,
catalog, planning, write path) and the controllers (what runs on clusters,
cluster exhaust processing, since management, controller-owned collections).
The two are entangled through in-process interfaces: synchronous frontier
reads during timestamp selection, RAII read holds over in-process channels,
controller calls inline in DDL processing, and controller responses pumped
through the coordinator loop.

Consequences:

- One failure domain. A crash or stall in either half takes down the other.
- One scaling domain. Adapter load (connections, planning) and controller
  load (frontier churn, introspection, replica management) compete in one
  process.
- No path to distributing the controllers, which prior designs identified
  as the route to physical use-case isolation and high availability.

We are honest about the strength of these motivations. We do not have
incident statistics showing controller-side faults as a leading cause of
downtime, and the small-coordinator work already removed the coordinator
loop as the SELECT bottleneck in-process. The case for the split is
strategic: the roadmap items that require multiple cooperating control
processes (zero-downtime upgrades v2, high availability, physical use-case
isolation) all need the boundaries this design builds, and the failure
domain and restart-independence benefits are real, if unquantified. The
rollout below is therefore measurement-gated: each phase produces the
numbers that justify, or veto, the next.

This design continues a documented lineage: the platform v2 logical
architecture (decoupling via definite collections and explicit timestamps),
the decoupled storage controller (StorageCollections as a separable
component), the small coordinator work (moving SELECT processing off the
coordinator loop, `PeekClient` talking to controller internals directly),
and the zero-downtime upgrade designs (read-only controller stacks,
concurrent instances, catalog-tracked capability). See:

- [Platform v2 logical architecture](20231127_pv2_uci_logical_architecture.md)
- [Decoupled storage controller](20240117_decoupled_storage_controller.md)
- [A small coordinator](20250717_a_small_coordinator_more_scalable_isolated_materialize.md)
- [Zero-downtime upgrades, physical isolation, HA](20251219_zero_downtime_upgrades_physical_isolation_high_availability.md)

## Goals

- `controllerd` as a separate binary with an independent restart and
  failure domain from the adapter.
- The adapter keeps serving fast-path reads and table writes across
  controllerd restarts, and DDL keeps committing while controllerd is
  unavailable.
- Clean, explicitly specified boundaries: who owns which durable and
  ephemeral state, what travels on which channel, which invariants hold.
- Design (not yet deploy) multi-controllerd: all protocols and durable
  token formats admit multiple cooperating instances, sharded by cluster,
  with a centralized single instance as the first deployment and the
  multi-instance machinery itself deferred to when it can be exercised
  (spec 4's "what ships when").

## Non-goals

- Multiple concurrent adapters. The forward-looking constraint is recorded
  in spec 4 (a catalog freshness watermark for linearizable isolation),
  v1 is explicitly single-adapter.
- Zero-downtime upgrades v2 (concurrent read-write generations,
  catalog-version gating). We spec against the current halt-and-restart
  flow and keep v2 compatible.
- High availability of controllerd itself (multiple instances per role).
- Changing the cluster-internal (timely mesh) or persist architectures.

## Design overview

Three ideas carry the design.

**The catalog is the command channel for deterministic state.** controllerd
follows the durable catalog and derives everything that deterministically
depends on it: collections, dataflows, cluster and replica lifecycle,
configuration. The adapter's DDL is a catalog commit and nothing else.
Enactment converges asynchronously, which matches today's semantics in
substance: almost no post-commit controller call surfaces errors to users
today. controllerd resolves plans cache-first from the expression cache
(written by the adapter at DDL time and at boot, and by controllerd when it
re-optimizes) and re-optimizes on miss.

**Capability and observation travel on one thin protocol.** The adapter
holds per-collection read capability as leases at controllerd
(grant-at-current-since, standing holds on all collections, auxiliary holds
for historical reads) and observes enactment and frontiers through a
streamed, coalescing mirror that doubles as the "controllerd has applied
the catalog up to here" signal. Ephemeral, query-shaped work (transient
dataflows for slow-path peeks, subscribes, COPY TO, and oneshot ingestions
for COPY FROM) is the only command-shaped traffic. The reverse direction is
strictly: the mirror, responses to adapter requests, and persist
collections controllerd writes anyway. Controller-observed signals the
adapter needs (hydration, replica status, data-plane endpoints) live in
controller-owned persist collections and are consumed from there, so no
event back-channel exists.

**Query data bypasses controllerd.** Cluster processes gain a data-plane
endpoint. The adapter issues fast-path index peeks directly and receives
peek rows, subscribe batches, and COPY TO completions directly. Persist
fast-path peeks move into the adapter as plain persist reads. Controllerd
stays on the control path only (dataflow installation, compaction, frontier
authority). COPY FROM's staged-batch metadata returns through the
controllerd protocol, metadata is not data.

The safety architecture in one line: **validation at the point of execution
is the safety floor, leases make rejections rare, the mirror is progress
only** (spec 2's layering contract). Every reviewer-attacked interleaving
(lease expiry, clock skew, displacement, drop races, generation handoff)
degrades to blocking or retryable errors, never wrong answers, with one
carve-out we closed by rule: as-of recovery refuses a dataflow whose output
hard constraint cannot be met rather than installing it with a gap
(spec 4).

The division of labor in one line each:

- Adapter: SQL, catalog writes, planning and DDL-time plus boot-time
  optimization (notices, expression cache), oracle, timestamp selection,
  write stack (group commit, txn-wal registration, table schema evolution,
  webhooks, statement logging), direct query data plane, persist fast-path
  reads, cluster reconciler (a catalog-level controller), pre-commit
  validation.
- controllerd: catalog follower, plan resolution (cache-first), everything
  cluster-facing (instances, replicas, provisioning, reconciliation),
  critical since handles and read policy enactment, lease and mirror
  service, controller-owned collections, shard finalization, caught-up
  evaluation.
- clusterd: unchanged in role, plus the data-plane endpoint and its peek
  admission state machine.

## Key decisions and rationale

**Catalog-subscribe instead of command-push.** An RPC-push controller would
carry real session state between adapter and controllerd and need a bespoke
reconciliation protocol. With the catalog as the source of truth,
reconciliation nearly disappears (controllerd re-derives everything from
catalog plus persist frontiers, the same way bootstrap works today), and
multiple controllerd instances fall out naturally. The cost is finishing
work that is already in flight: the catalog-implications pipeline and the
elimination of sequencer side-effect closures.

**Leases at controllerd instead of adapter-owned persist handles.** A prior
design rejected centralized RPC read holds. We revisit that: one lease
protocol uniformly covers storage and compute collections (compute holds
have no persist-handle equivalent, so a split model would need two
mechanisms), and per-cluster controllerd gives a query one lease
counterparty. The hot path stays cheap because holds are standing
(installed per collection per adapter incarnation, off the query path) and
timestamps are chosen only above already-granted holds. Historical reads
(`AS OF`) pay one auxiliary-hold round trip. The adapter's short-lived
leased persist handles for persist fast-path reads are the one additional
capability form, bounded by persist's own lease timeout (spec 1).

**As-ofs chosen by controllerd for catalog-derived dataflows.** Uniform
with index and sink behavior today, eliminates a cross-process hold handoff
at CREATE MATERIALIZED VIEW, and keeps the catalog free of timestamps.
Recovery re-derives as-ofs from persist with outputs as hard constraints,
and refuses rather than degrades when a hard constraint cannot be met.
During migration the adapter continues writing the chosen as-of into
`create_sql` as a constraint controllerd honors, dropping that write is a
separate, flag-gated change (see user-visible changes).

**Adapter keeps the write stack, including table persist bring-up.** Group
commit's dependencies are the oracle, txn-wal, and catalog fencing, none of
which involve cluster controllers. Making the adapter own table shard
registration, schema evolution, and webhook bring-up means writes never
wait on controllerd enactment. The only cross-process hazard (finalizing a
shard still registered in txn-wal) reduces to a persist-readable predicate
with a stated observation frontier (spec 4).

**Single catalog writer, back-edge dissolved into persist.** controllerd
never writes the catalog. Flows that look like controller-driven catalog
writes (the record-based ALTER CLUSTER resolution) are a catalog-level
reconciler that stays in the adapter, consuming controller-written persist
collections (hydration, replica status) as inputs. Shard-finalization
completion and data-plane endpoints ride persist the same way.

## User-visible changes

These need explicit scrutiny in review:

1. **Materialized view as-ofs eventually leave `create_sql`.** End state:
   controllerd chooses as-ofs at apply time, the statement stays pure, and
   the output collection's since is the observable truth. Migration
   sequencing: the adapter keeps writing the as-of into `create_sql`
   through the split rollout (controllerd honors it as a constraint), and
   the removal ships later as its own flag-gated change, so any regression
   is attributable.
2. **Optimizer notices are unchanged in practice.** The adapter keeps
   producing them at DDL time and regenerating them at boot (which also
   warms the expression cache). What changes: controllerd may run the
   authoritative plan for an object from a re-optimization the adapter
   never saw (cache miss under configuration drift). Controllerd writes
   every plan it runs back to the cache with writer, build version, and
   optimizer configuration recorded, and EXPLAIN reports that provenance,
   which can differ from the adapter's own optimization in that window.
3. **DDL acknowledgment semantics are formally "committed to catalog".**
   This matches today's behavior in substance, but waits that today are
   short in-process windows become explicit waits on enactment: a SELECT
   immediately after CREATE INDEX waits (bounded by a deadline) for
   enactment and then errors retryably, it never silently falls back to a
   plan without the index. A created materialized view is unqueryable
   until enacted. While controllerd is unavailable, DDL commits and
   enactment converges later, which also means a brownout mode exists
   where DDL acks and nothing materializes until controllerd returns. The
   required observability (enactment lag as a first-class, alertable
   signal) is normative in spec 1.
4. **Controllerd restarts fail in-flight slow-path queries, subscribes,
   and COPY operations** with retryable errors. Today these survive
   controller hiccups because the controllers share the adapter's process,
   and die only with it. SUBSCRIBE in particular becomes less available:
   it now depends on the minimum of two process lifetimes plus the
   data-plane connection. This is a real regression for
   restart-sensitivity of long-running operations, accepted in v1, with
   client-transparent resumption as the identified future mitigation. In
   exchange, fast-path reads, table reads and writes, and DDL survive
   controller restarts, which today they do not.
5. **Persist fast-path peeks succeed without replicas and run on the
   adapter.** Queries served by the persist fast path today execute on a
   replica, post-split they execute in the adapter against persist
   directly, so they succeed even when the cluster has no live replicas,
   and their (plan-shape-bounded) scan cost lands on the adapter process.
6. **EXPLAIN TIMESTAMP reports hold frontiers as sinces.** The adapter no
   longer observes true sinces, only its granted hold frontiers, which are
   a conservative lower bound.

## Availability properties

Explicit goals, to be tested as stated, with honest attribution:

- **New with this design:** fast-path index peeks, persist fast-path
  peeks, table reads and writes, and DDL commits all continue while
  controllerd is down or restarting, bounded by the lease TTL discipline
  (which requires TTL to exceed controllerd's restart-to-lease-serving
  time, a normative boot-ordering requirement in spec 1, and a
  measurement gate below). Scope: this covers a running adapter. An
  adapter that boots while controllerd is down serves DDL, table writes,
  and persist-backed reads only.
- **Inherited, not new:** running dataflows on clusters are undisturbed by
  controller restarts. Replica-side reconciliation already provides this
  across environmentd restarts today, the design preserves it and extends
  it to pending direct peeks (spec 3's reconciliation exemption).
- **New:** an adapter restart releases its state at controllerd
  immediately on re-registration (ordered displacement), not after a TTL
  wait, and sinces never advance past granted holds of live sessions,
  except for dropped collections, whose drops override leases (spec 2).
  Sinces never regress for anyone.

Blast radius by failing component:

| Component down | Keeps working | Degrades or fails |
| --- | --- | --- |
| controllerd | fast-path reads (running adapter), table reads/writes, DDL commits, running dataflows | slow path, SUBSCRIBE, COPY, new-object readability, enactment, since downgrades (frozen) |
| adapter | running dataflows, sources, sinks, controllerd enactment | all client traffic (as today), table upper advancement, since downgrades (frozen once the session expires, until an adapter returns or an operator intervenes) |
| one replica | untargeted peeks (broadcast, first response wins), other replicas | targeted peeks to it, its subscribes (buffer then terminate) |
| oracle | running dataflows, enactment | all timestamp selection, so reads and writes (as today) |
| persist | nothing new | everything (as today) |
| orchestrator | serving, enactment on existing replicas | provisioning, replica lifecycle (as today) |

## Rollout

Phase transitions are gated on measurements, not calendar. The split
topology is selected per environment by deployment configuration (a CRD
field in cloud), and the same build supports both topologies: embedded
(controllerd as a library inside environmentd, speaking the real protocol
in-process) and split. **Topology rollback is a re-rollout at the same
version in embedded mode**, it is never coupled to a version downgrade.
Cross-topology generation handovers (monolith to split and back) are
ordinary generation handovers because v1 keeps today's critical-handle
scheme (spec 4).

**Phase 0, prerequisites** (mergeable independently, valuable on their
own): finish the catalog-implications migration and eliminate sequencer
side-effect closures, subscribe and COPY TO frontier reporting on the
control protocol, factor catalog state resolution and the optimizer out of
the adapter crate, production catalog follower.

**Phase 1, in-process seams** behind a feature flag (off in production, on
in CI per repo convention): the lease, mirror, and ephemeral command
interfaces between the adapter and the embedded controller stack, replacing
direct field access and the ready/process pump. Exit gate: fast-path peek
p50/p99 unchanged versus the direct path under benchmark, seam-level
counters (mirror update rates, hold delta volumes, gating waits) collected
from CI and staging to size phase 2.

**Phase 2, the split**, in sub-milestones, each independently shippable
behind the topology configuration:

- 2a: controllerd binary, protocol over the network, leases and mirror,
  peek data plane, deployment integration (provisioning, network policy,
  readiness aggregation). Exit gates: controllerd kill-and-restart under
  sustained load with fast-path p99 unaffected, cold-boot-to-lease-serving
  measured at representative catalog sizes and TTL set with margin,
  enactment latency (catalog commit to mirror appearance) benchmarked
  alone and under DDL burst, the alert catalog derived from spec 1's
  observability requirements shipping in the same release, and runbooks
  for: controllerd down, lease renewal failure, DDL committed but not
  enacted, promotion stuck per stage, stale capability pinning compaction,
  and split-rollout rollback.
- 2b: subscribe and COPY TO delivery on the data plane (requires the
  phase 0 frontier-reporting work), introspection subscribes move to
  controllerd.
- 2c: persist fast-path in the adapter, MV as-of ownership flip
  (constraint honored, write retained), 0dt flow validation for split
  generations.

**Phase 3, multi-controllerd**: role assignments, per-instance handles with
deterministic identities, event-driven retirement, partition attribution,
per-cluster deployment. Entry gate: the lease-backing cost model of spec 4
(critical handles per instance, compare-and-swap downgrade rate) measured
and accepted, and the multi-instance mirror/lease topology exercised in a
test deployment before any production reassignment.

## Alternatives

**Keep the controllers in-process, continue shrinking the coordinator.**
Delivers loop isolation and stall isolation (largely shipping already) but
not separate restart domains, not serving continuity through controller
faults, and no path to distributed controllers. The in-process seams of
phase 1 are shared work with this alternative, and the measurement gates
mean that if phase 1's numbers show the split buys too little, stopping
there is an explicit, respectable outcome.

**RPC command-push controllerd.** A smaller first step, but the
adapter-controllerd session becomes stateful, reconciliation is a new
bespoke protocol, and multi-controllerd requires redesign. Rejected.

**controllerd as a second catalog writer.** Would let controller-driven
reconciliation (cluster resizing) live controllerd-side. Requires
multi-writer catalog machinery and complicates fencing now. Deferred, the
single-writer constraint is an invariant, and zero-downtime v2 will revisit
multi-writer catalogs on its own schedule.

**Adapter-owned persist since handles instead of leases.** Works for
storage collections, cannot cover compute collections, and leaves two
capability mechanisms on the query path. Rejected for non-uniformity. The
bounded exception is the adapter's short-lived leased persist handles for
persist fast-path reads, whose discipline spec 1 states.

## Open questions

- Persist pubsub topology across three process kinds (who serves, who
  connects to whom). Must be decided at phase 2a entry, since the upgrade
  window depends on it. Default candidate: controllerd serves pubsub for
  its generation.
- Data-plane and protocol authentication mechanics (mTLS versus platform
  tokens). The requirements are normative in specs 1 through 4
  (authenticated identity, token-bearing handshakes honored only from
  authenticated principals), the mechanism is chosen at phase 2a entry.
- Deadline, TTL, and margin values are dyncfg-tunable, their constraints
  (TTL versus boot time, tombstone lifetime versus admission deadline,
  takeover grace versus re-lease time, drain grace versus session
  re-establishment time) are normative in the specs, values come from
  phase 1 and 2a measurements.
