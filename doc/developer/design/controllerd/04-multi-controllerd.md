# Spec 4: Multi-controllerd cooperation

Part of the controllerd design. Depends on specs 1 and 2. This spec defines
the invariants that let multiple controllerd instances share one
environment: role assignment, fencing, durable read capability, shared
collections, and destructive-action permissions. It also defines exactly
which subset ships in v1 (one instance holding all roles) versus phase 3,
because shipping multi-instance machinery that a single instance cannot
exercise is how load-bearing bugs hide until phase 3.

## What ships when

**v1 (single instance, all roles):**

- Token formats everywhere they are durable or semi-durable: the
  critical-handle opaque widened to carry (deploy generation, assignment
  epoch), with assignment epoch fixed at a degenerate value, the control
  and data-plane handshake token fields, orchestrator label fields. The
  existing opaque encoding migrates at the first post-upgrade handle
  takeover, which is the same compare-and-swap the generation handover
  performs anyway.
- Instance identity and incarnations (spec 1) in all protocols.
- Critical handles: **the existing well-known handle per collection**,
  owned by the single instance, fenced by the widened opaque. No handle
  registry, no reaper.
- The destructive-action rule and the shard finalization predicate below,
  both of which are load-bearing on day one.
- Catalog schema reservations for role assignments, consumed by nothing.

**Phase 3 (multiple instances):**

- Role assignments in the catalog, assignment epochs, per-instance handles
  with deterministic identities, event-driven retirement, collection
  partition attribution. The migration from the v1 well-known handle to
  per-instance handles is the successor-takeover procedure defined below,
  built once, used for both the migration and every subsequent
  reassignment.

## Roles and assignment

Controllerd work is partitioned into roles:

- One **cluster role** per cluster: the compute and storage instances for
  that cluster, its replica control connections, its provisioning, its
  per-cluster introspection and statistics, and durable read capability for
  every collection its enacted dataflows and its granted lease holds
  require.
- One **environment role**: environment-wide duties with no cluster
  affinity. Shard finalization, environment-wide controller-owned
  collections, orphaned-resource sweeps (including aged peek-stash
  batches), and read policy enactment for collections not covered by any
  cluster role's capability.

Role assignments are durable catalog state, written only by the adapter.
Deployment machinery instructs the adapter through its administrative
interface, the same channel that drives promotion today, and is responsible
for detecting dead instances and requesting reassignment. An assignment is
(role, instance name, assignment epoch), epochs monotonically increasing
per role. A controllerd instance learns its roles from its catalog
subscription.

**Lease backing across instances.** A query's lease counterparty is the
cluster role of its cluster (spec 2), and a lease session may hold any
collection in the environment (transient dataflows import arbitrary
collections). Therefore a cluster role acquires durable capability
(critical handles) on demand for every collection its lease sessions hold,
not only for its catalog-derived dataflow imports. The cost is up to one
critical handle per (collection, instance) and the corresponding
compare-and-swap downgrade traffic, accepted and to be measured (design
doc, rollout gates). The alternative, cross-instance hold forwarding, is a
protocol change and is rejected. Each instance serves mirror entries for
the collections its sessions can hold, the adapter consumes the mirror of
the granting instance for a given query, storage collection uppers are
consistent across instances because all observe them from persist.

## Fence hierarchy

Ordered from coarse to fine, each layer carries a totally ordered token:

1. **Deploy generation.** Fences whole generations. A catalog fence token
   with a higher generation halts the process, as environmentd behaves
   today.
2. **Assignment epoch, per role.** Fences instances of the same generation
   contending for one role. Carried in the critical-handle opaque (as the
   pair generation and assignment epoch), the control and data-plane
   handshakes, and orchestrator labels.
3. **Incarnations** of lease sessions and connections (specs 1, 2, 3),
   ordered per instance name by the tokens defined in spec 1, including
   the per-boot component. Two processes of the same instance name and
   assignment epoch (a restart overlap) are ordered by the boot component
   at session and connection establishment, their shared durable
   capability is addressed in spec 1's identity model.

Rules:

- Every fencing-relevant durable action (critical handle downgrade,
  orchestrator mutation, control connection establishment, collection
  writes) carries the actor's token. Where an arbiter exists (persist
  compare-and-swap on the opaque, replica handshake checks), it rejects
  lower tokens.
- **Tokens are only honored from authenticated principals.** Replica
  handshake ordering ("accept only tokens at or above the highest seen")
  turns last-writer-wins displacement into ordered displacement, but an
  ordered ratchet driven by a forgeable token is a permanent
  denial-of-service primitive, so authentication of the presenting
  principal is a requirement of the ratchet, not an optional hardening.
- The replica-side high-water mark for handshake tokens lives in process
  memory, so ordering holds per replica-process lifetime. After a replica
  restart a stale owner may win the connection until the current owner
  connects and displaces it, a bounded window that is safe because all
  durable effects carry their own persist-arbitrated fences.
- **Role-holding has a liveness bound.** The orchestrator cannot arbitrate
  epochs, so orchestrator mutations are protected by self-fencing with a
  freshness bound: an instance may act in a role only while it can confirm,
  within a configured recency window, that its applied catalog state
  matches the catalog's current upper (a successful read of the upper
  counts, the catalog need not be advancing). An instance partitioned from
  the catalog loses that confirmation and stops acting (orchestrator
  calls, collection writes, lease granting, mirror serving, handle
  downgrades) when the window expires, by construction, whether or not it
  ever observes its reassignment. An idle catalog does not trip the bound:
  a connected follower confirms freshness regardless of write activity, so
  adapter downtime or DDL quiescence does not paralyze controllerd. This
  same bound ends the window in which a fenced predecessor's
  self-correcting collection writes can fight the current holder's.
- A fenced or freshness-expired role owner stops all activity in the role:
  no handle downgrades, no orchestrator calls, no collection writes, **no
  lease granting, no mirror serving** for that role's scope.
- When the adapter observes (or commits) a reassignment, it tears down
  sessions at the de-assigned instance and re-establishes at the new
  holder. Successor takeover retires predecessor handles only after a
  grace period sized to cover this re-leasing, so reassignment does not
  produce an avoidable error blip on reads near the old holds.

## Durable read capability across instances

Each controllerd instance owns critical since handles for the collections
its roles require. Critical handles are durable and survive the owning
process. Consequences:

- The overall since of a collection is the meet across all live handle
  positions. One instance crashing does not release its capability.
- **Ordering invariant for dependent outputs.** When enacting the creation
  of a dataflow with a durable output, the enacting instance acquires
  critical handles on all inputs at or below the chosen as-of before
  establishing the output collection's initial since. There is no state in
  which an output promises a readable time that its inputs no longer
  cover.
- **Acquire before retire.** A successor retires a predecessor's handles
  only after it has fenced the predecessor's epoch **and** holds its own
  handles at or below the frontiers of the handles being retired. This is
  the takeover-time form of the ordering invariant.
- **As-of recovery.** After a crash, as-ofs for catalog-derived dataflows
  are re-derived by as-of selection with each existing output's since and
  upper as hard constraints, alongside input constraints. For refresh-based
  materialized views the split between the output since (first refresh)
  and an earlier dataflow as-of (for early hydration) is part of that
  re-derivation. **Hard-constraint failure is poisoned, not papered
  over**: if as-of selection cannot satisfy an output's hard constraint
  (inputs compacted past the output's since, which invariant 3 and
  acquire-before-retire exist to prevent), the dataflow is refused and
  surfaced as an error condition, because installing it with a forced
  later as-of would silently serve stale contents in the gap. This
  replaces the current best-effort fallback for this case. The condition
  surfaces through the channels that exist (spec 2 invariant 8): the
  object's error and reason in the controller-owned status collections
  (rendered by introspection relations), and the absence of progress in
  the mirror and frontier-lag metrics, like a wedged source today.
  Already-written history stays readable, the output's since and upper
  are untouched, only progress stops. Resolution is an operator decision.

## Handle identity and retirement (phase 3)

Per-instance handle identities are derived deterministically from
(collection, role, instance name, assignment epoch). Nothing needs to be
enumerated in the catalog per collection or per DDL: the assignment record
alone determines every handle identity an instance may own, so the adapter
commits nothing beyond the assignment, controllerd requests nothing (no
back-channel), and a retiring successor can enumerate a predecessor's
handles from the assignment history plus the catalog's collection set.

Retirement is event-driven from catalog state, there is no periodic
reaper: a successor retires the predecessor's handles on taking over a
role (after the grace period above), and the environment role retires
handles derivable from assignment epochs that no assignment references,
when it observes the assignment change. All retirement follows fence
first, acquire, then retire. Nothing ever retires a handle belonging to
the current epoch of a live role.

The same fence-then-act discipline applies to orchestrator reaping and any
destructive cleanup: an instance acts only for roles it currently holds
(within the staleness bound), only against epochs below its own, and only
on state visible in a catalog snapshot at least as new as the resource's
**creation bound**: the catalog timestamp of the DDL that created the
resource, attached to the resource as an orchestrator label at
provisioning time. A lagging follower therefore cannot reap a resource it
has not yet seen created.

## Shared and controller-owned collections

- **Differential collections** are written with the self-correcting
  pattern and partitioned by responsible role. Partition attribution uses
  the keys the collections already carry (cluster and replica identifiers
  map rows to cluster roles, environment-wide rows belong to the
  environment role), no schema change. The holder of a role asserts the
  desired state of its partition and corrects drift, interleaved writes
  from a fenced predecessor are corrected by the current holder and
  bounded in duration by the staleness bound above.
- **Append-only collections** (status histories, wallclock lag, replica
  metrics) are writer-scoped by the same key-derived partitioning, writers
  append only for roles they hold, and retention or truncation of a
  partition belongs to the role holder (the environment role inherits
  partitions of dropped clusters). Consumers of history collections must
  tolerate duplicated rows from fencing windows. This is a new, explicit
  requirement on those consumers, to be audited in phase 3, today's
  single-writer deployment cannot produce such duplicates.
- The environment role owns collections with no cluster affinity, and the
  replica data-plane endpoint collection (spec 3) belongs to each cluster
  role for its replicas.

## Shard finalization

Finalization is executed by the environment role. A shard may be finalized
only when all of the following hold, evaluated together:

1. It appears in the unfinalized-shard WAL in the catalog (written by the
   adapter's drop transaction).
2. No live collection references it in the catalog at the finalizer's
   applied catalog timestamp.
3. It is not registered in the txn-wal txns shard, **observed at a txns
   shard time at or after the finalizer's applied catalog timestamp** from
   condition 2 (both live in the same timestamp domain today, this
   comparability is an assumption the predicate depends on). A stale read
   of the txns shard does not satisfy this condition: registration is
   time-varying, and only an observation no older than the catalog view
   that showed the drop is evidence that the write stack's forget has run.

The predicate is stable because of a catalog discipline this spec makes
normative: **a shard, once forgotten, is never re-registered**, shard ids
are never reused, and any catalog operation that carries a shard across
item identities (for example table schema evolution, which maps a new
GlobalId to the same data shard) maintains a continuous live catalog
reference so condition 2 blocks finalization throughout.

Finalization is idempotent. Completion is observable directly from persist,
and the adapter clears WAL entries in later catalog transactions by
checking persist. No report channel exists or is needed.

## Zero-downtime deployments

Each deploy generation runs its own controllerd instances against its own
cluster processes, as environmentd generations do today.

- **Read-only phase.** A new-generation controllerd opens the catalog as a
  savepoint snapshot (migrations applied in memory, not committed),
  hydrates its generation's clusters read-only with leased read handles,
  never touches critical handles, and halts to re-hydrate when it observes
  DDL, matching today's preflight behavior. A live cross-version catalog
  follower is explicitly not part of this design. During this phase it
  serves lease sessions to its generation's (read-only) adapter backed by
  leased persist handles rather than critical handles, a stated exception
  to spec 2's grant-backing rule that is safe because leased handles are
  real capability and the generation serves no user traffic before
  cutover.
- **Same-generation epoch fences** (the read-write adapter restarted) are
  handled by the catalog follower transparently: reopen read-only and
  resume, continuing from its frontier when the catalog shard's since
  permits, otherwise by full snapshot plus diff against applied state.
  Neither mode churns enacted controller state.
- **Readiness.** Each controllerd evaluates caught-up state for its
  clusters (spec 1) and exposes readiness for its generation on its status
  endpoint. Deployment machinery aggregates across the adapter and all
  controllerd instances of the generation, drives cutover, and owns any
  operator-forced skip.
- **Promotion.** The generation fence commits through the adapter's
  catalog open. Old-generation instances halt on observing it (or on
  their freshness bound expiring). New-generation instances re-open
  read-write, take over critical handles by compare-and-swapping the
  opaque (fencing the prior generation), and the new generation's adapter
  re-establishes its lease sessions against the read-write instances
  (the read-only-phase sessions and their leased-handle backing do not
  carry over). Ordering: fence commit, then old halts, then new flips
  (halt-and-reopen of the controllerd process is acceptable and must be
  distinguishable from a crash loop by deployment machinery), then
  sessions re-register. The flip window budget must fit inside the lease
  TTL of any session established during the read-only phase, or the
  adapter re-times its reads on re-registration, which is the defined
  behavior rather than a failure.
- **Cross-topology upgrades** (a monolithic old generation, a split new
  generation, or the reverse for rollback) are ordinary generation
  handovers: v1's handle scheme is today's handle scheme, the opaque
  compare-and-swap at takeover works identically whether the previous
  owner was an embedded controller stack or a controllerd process. No
  special migration path exists or is needed.

The catalog-version-gated concurrent read-write upgrade model
(zero-downtime v2) is out of scope. So is multi-adapter operation: for the
record, the forward-looking constraint is that under linearizable
isolation an adapter must have applied the catalog up to a watermark tied
to the client's causality before planning, a rule that becomes normative
only when a second adapter exists.

## Invariants

1. Every role has at most one holder acting within its staleness bound at
   any time, and every destructive or capability-releasing action is
   performed only by the current, unfenced, within-bound holder of the
   relevant role (fence first, then act).
2. Handles are retired only by a successor that has fenced the owning
   epoch and holds its own handles at or below the retired frontiers.
3. Inputs are durably held at or below an output's initial since before
   the output's since is established, and a dataflow whose output hard
   constraint cannot be satisfied is refused, not installed with a later
   as-of.
4. Shard finalization requires the WAL entry, no live catalog reference,
   and absence from the txns shard observed no earlier than the catalog
   view of condition 2, and is idempotent. Forgotten shards are never
   re-registered.
5. Shared collections are partitioned by role using their existing keys,
   and only the role holder asserts or truncates a partition.
6. Role assignment and fences live in the catalog, whose only writer is
   the adapter. Fence tokens are honored only from authenticated
   principals.
