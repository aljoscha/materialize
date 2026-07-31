# Spec 2: The adapter and controllerd protocol

Part of the controllerd design. Depends on the component split and identity
model in spec 1. The cluster data plane is spec 3. This spec defines the
single protocol between an adapter and a controllerd instance: lease
sessions, the frontier mirror, and ephemeral commands. It defines behavior
and invariants, not wire encodings.

The layering contract that everything below serves:

> **Safety comes from validation at the point of execution** (replicas
> reject peeks below the compaction frontier, persist rejects reads below
> the since, controllerd rejects commands that outrun capability).
> **Leases exist to make those rejections rare**, not to make reads safe.
> **The mirror is a progress signal only**, never a safety input.

The catalog is deliberately not part of this protocol. Deterministic state
(what exists, cluster shapes, configuration) travels through the durable
catalog, which controllerd follows on its own. This protocol carries only
what cannot travel through the catalog: read capability leasing, frontier
observation, and ephemeral (query-shaped) work.

## Connection and session model

An adapter maintains one connection per controllerd instance it uses,
multiplexing three exchanges: the lease session, the mirror stream, and
ephemeral commands with their responses. Responses flow only in reply to
adapter requests. The mirror is the only unsolicited stream.

**Establishment.** Session establishment carries the adapter's stable name,
its incarnation token (the catalog fence token per spec 1), its deploy
generation, and its build version. Controllerd rejects establishment when
the build version or the deploy generation differs from its own.
Establishment with an incarnation lower than the highest seen for the name
is rejected. A higher incarnation displaces the prior session immediately:
its holds are released and its transient dataflows are garbage collected
after a short drain grace (see ephemeral commands). A restarting adapter
therefore never waits out its own stale state, and a stale incarnation can
never displace a current one.

Only authenticated adapter principals of the environment may establish
sessions. This protocol carries no authorization semantics: a session is
full-environment read capability (holds on everything, transient dataflow
installation, compaction holdback). Authorization of user queries happens in
the adapter at planning time. Displacement and expiry are liveness
mechanisms, not security revocation.

**Reconnects.** A network reconnect with the same (name, incarnation)
resumes the existing session, it does not displace it. Because downgrade
deltas are not idempotent across a resumed stream, resumption re-synchronizes
with absolute hold frontiers.

**A displaced or generation-fenced incarnation stops serving.** An adapter
that learns of its own displacement (a rejected renewal or request) must
stop serving all client traffic within a bounded deadline, not merely stop
using lease-backed holds. Until it learns, its window of stale service is
bounded by the renewal cadence and TTL below. Data reads in that window
remain linearizable (shared oracle, shared persist, execution-time
validation), catalog staleness in that window matches today's
fenced-process window.

**catalog_ts gating.** Every request that references catalog content
carries `catalog_ts`, the commit timestamp of the catalog state the adapter
observed. Controllerd processes such a request only once its applied
catalog frontier is **beyond** `catalog_ts` (the commit at `catalog_ts` is
applied exactly when the applied frontier is greater than `catalog_ts`),
and answers an explicit, gate-attributed error if it cannot get there
within a deadline. It never answers "unknown object" for objects newer than
its applied view. Usage rule: server-side `catalog_ts` gating is for
commands, adapter-local waiting on the mirror is for local decisions, a
call site uses one, not both.

## Lease sessions

A lease session is the unit of read-capability accounting for one adapter
incarnation at one controllerd instance. It contains a set of
per-collection holds. A hold is a frontier: the promise that the
collection's since will not advance past it while the session is live.
A granting instance must itself hold durable capability (critical handles,
spec 4) at or below every frontier it grants, grants are never backed by
another instance's capability.

- **Install (grant at current since).** The adapter requests holds,
  controllerd installs them at each collection's current since and returns
  the granted frontiers. Install requests are batched: adapter boot
  installs standing holds for all collections in one request. For a
  collection that is live at the request's `catalog_ts` but already dropped
  in controllerd's applied view, install returns a drop-identifying error
  for that collection rather than a grant.
- **Standing holds.** The adapter proactively maintains a hold on every
  collection it can query, installing holds as the mirror reports new
  collections. Hold installation is off the query path: per collection per
  adapter incarnation, not per query.
- **Historical reads (`AS OF`) acquire no extra holds.** A user-pinned
  timestamp below the standing hold is attempted directly and gated by
  execution-time validation alone: persist rejects reads below the since,
  replica admission rejects peeks below the compaction frontier (spec 3),
  and the rejection surfaces as a clean history-compacted error. This is
  the one read path where rejection is an acceptable answer (the user
  named a time that may legitimately be gone), so no lease machinery
  exists to make it rare. For transient dataflows with a pinned `AS OF`,
  admission at controllerd is the validation point (see ephemeral
  commands).
- **Acquire-then-choose.** The adapter never selects a query timestamp
  except at or above the granted frontier of a hold it owns. User-pinned
  `AS OF` timestamps are not selected, they are validated at execution
  (historical reads above). The adapter-internal bookkeeping that ties local query and
  transaction tokens to granted holds must make token registration atomic
  with respect to its downgrade computation, concurrent session threads
  must not observe a hold as downgradable while registering a token under
  it.
- **Downgrades.** The adapter periodically downgrades its holds, driven by
  the oracle read timestamp and bounded below by its live local tokens.
  Downgrades are asynchronous deltas, ordered within the session, encoded
  compactly (one timestamp for the common everything-advances case).
- **Renewal and expiry.** The session has a TTL. The adapter renews by
  heartbeat. Renewal processing at controllerd is constant-work and takes
  strict priority over installs, downgrades, and ephemeral commands, so
  overload cannot starve renewals into a mass-expiry spiral. Both sides
  measure the TTL on monotonic clocks: controllerd expires the session at
  (last renewal received + TTL), the adapter considers its holds unproven
  at (last renewal acknowledged + TTL - skew margin) and must then stop
  serving reads backed by them, re-acquire, and re-time or abort affected
  transactions. The skew margin covers clock rate drift, values are
  deployment configuration with the constraint TTL greater than worst-case
  controllerd restart-to-lease-serving time (spec 1 boot sequence).
  Violations of this discipline degrade to query errors, never wrong
  answers, by the layering contract. In a single-adapter deployment expiry
  of the only session immediately triggers the freeze below, so the TTL
  discipline is availability hygiene there, its safety role begins when
  multiple sessions exist.
- **Zero-session freeze.** While no lease session is registered,
  controllerd does not enact policy-driven since downgrades. Policy targets
  continue to be computed, not enacted. This covers controllerd boot and
  adapter downtime with one rule, there is no separate boot grace. The
  freeze has no automatic expiry by default: resuming downgrades with zero
  registered sessions is an explicit operator action (an escape hatch for a
  permanently-departed adapter), because an automatic timer here would
  convert an adapter partition into a read outage on reconnect. Compaction
  debt during a freeze is bounded by operator response time and is the
  accepted cost.
- **Drops override leases.** When a collection is dropped in the catalog,
  controllerd enacts the drop regardless of outstanding holds. Holds on
  dropped collections resolve to drop-identifying errors on next use. The
  adapter observes the drop through its own catalog and the mirror and
  cancels affected local work (spec 3).

## The frontier mirror

Controllerd streams to each connected adapter:

- Its **applied catalog timestamp**: the frontier of enactment, carrying
  the "controllerd has acted on DDL up to here" signal.
- **Per-collection write frontiers** (uppers) for every collection and
  dataflow it manages and for every collection its sessions can hold
  (storage collection uppers are observed from persist, so any granting
  instance can serve them, spec 4). Transient dataflows are included, and
  sinks carry a write frontier only (a sink has no since and no mirror
  since entry). Appearance of an entry means controllerd has enacted
  creation or installation (controller-side, not replica-confirmed). For
  objects enacted by another instance (multi-instance deployments), the
  authoritative appearance signal and applied catalog timestamp are the
  enacting role holder's mirror. Removal is explicit: the mirror carries
  drop events, the adapter does not infer drops by joining against its
  catalog.

Sinces are deliberately absent from the mirror: capability comes from
granted holds, and EXPLAIN TIMESTAMP reports hold frontiers (design doc,
user-visible changes).

Delivery semantics: the mirror is coalescing. Only the latest frontier per
collection needs delivery, controllerd may skip intermediate values, and
backlog therefore cannot grow beyond one entry per collection. Within one
stream, frontiers are monotone. Across streams (a controllerd restart),
compute collection frontiers may regress to a re-derived earlier value.
Serializable-isolation timestamp freshness can regress with them, which is
legal under serializable and was unobservable today only because sessions
died with the process.

**Retention across restarts.** The adapter retains its mirror copy when the
stream breaks and continues serving from it, holds stay valid per the TTL
discipline. On reconnect it overlays the new stream. Without this rule a
controllerd restart would gate fast-path peeks behind state rebuild, which
would silently void the availability goal.

Consumers on the adapter side:

- **Fast-path gating (advisory).** The adapter prefers to issue a direct
  index peek once the target appears in its mirror, but the normative gate
  is replica-side admission (spec 3): issuing before mirror appearance is
  legal and resolves to wait-or-error at the replica. When the target has
  not appeared, the adapter waits up to a deadline and then returns a
  retryable error. It never silently re-plans without the index: plan
  choice does not degrade based on enactment lag. When applied-catalog-ts
  lag is global (controllerd behind on everything), the adapter prefers
  waiting over issuing transient-dataflow fallbacks, since those land on
  the already-lagging controllerd (a lag-keyed circuit breaker, not a
  per-object timer).
- **DDL waits.** Statement lifecycle logging, ALTER SINK readiness, ALTER
  MATERIALIZED VIEW replacement readiness, and similar frontier waits are
  local waits against the mirror.
- **EXPLAIN TIMESTAMP** and timestamp selection read the mirror for write
  frontiers.

**Channel placement rule.** Ephemeral, latency-sensitive, per-adapter
progress signals ride the mirror. Durable, multi-consumer observations ride
controller-owned persist collections (hydration, replica status, data-plane
endpoints per spec 3). The mirror exists as a channel because timestamp
selection needs millisecond-scale upper observation, which persist-backed
introspection cannot provide (its cadence is the introspection interval,
and the frontiers of the frontier collection are self-referential).

## Ephemeral commands

Ephemeral commands are adapter-issued requests for query-shaped work that
does not derive from the catalog. Each carries the session (name,
incarnation) and a `catalog_ts`. Controllerd validates each against the
session's holds and its applied catalog state and answers accept or reject.
Rejects are explicit, reason-carrying, retryable errors.

- **Create transient dataflow** (slow-path peek dataflows, SUBSCRIBE,
  COPY TO). The request carries the adapter-optimized plan and the
  adapter-chosen as-of (for `AS OF` queries, the user-pinned timestamp).
  Admission requires capability at the as-of: controllerd acquires the
  dataflow's input holds at the as-of, which it manages for the dataflow's
  lifetime anyway. For adapter-chosen as-ofs this is covered by the
  session's holds (acquire-then-choose). For a user-pinned `AS OF` below
  the session's holds, controllerd validates against the actual sinces and
  rejects if history is compacted. Rejected commands surface the error for
  user-pinned `AS OF` and are retried with a fresh timestamp otherwise. The slow-path peek flow
  end to end: the adapter installs the transient dataflow via this command,
  and on acceptance issues a data-plane peek (spec 3) against the transient
  output at the same timestamp, rows return on the data plane. The
  admission validations at controllerd (holds versus as-of) and at the
  replica (compaction versus peek timestamp) are two instances of the
  layering contract, both are required.
- **Drop or cancel transient dataflow.** Explicit teardown. A transient
  dataflow whose output frontier becomes empty is cleaned up automatically,
  which is the normal completion path. The adapter learns of abnormal
  termination (for example replica-side delivery buffer overflow, spec 3)
  through the mirror's drop event for the transient dataflow.
- **COPY FROM (oneshot ingestion).** The request names the cluster and
  source specification. Controllerd runs the oneshot ingestion through its
  storage control connection. Staged batch handles (metadata) return as the
  command response, the append of those batches runs through the adapter's
  write stack. Cancellation is a command. Staged batches whose command
  response was never consumed (displacement, expiry, crash) are garbage
  collected by controllerd, which owns the command.

**Session end and in-flight work.** A session is identified by (name,
incarnation): re-establishment of the same identity within the drain grace
below resumes the same session and its transient dataflows, so an expiry
caused by a renewal blip (process still alive, same incarnation) does not
tear down every subscribe in the environment. On displacement by a higher
incarnation, or when the drain grace after expiry lapses without
re-establishment, transient dataflows of the session are garbage collected
and un-consumed command responses are dropped. Results racing session end
are deliverable to the client only while the adapter's local session token
is live, an adapter whose session lapsed fails the query even if rows
arrived. Teardown of a transient dataflow and release of the holds backing
it are ordered so that in-flight results are delivered or observably
canceled before capability is released.

## Reconciliation

- **Adapter restart.** The new incarnation establishes a session, which
  displaces the old one (ordered, immediate). It installs standing holds
  afresh in one batch and chooses timestamps only from the new grants. It
  does not recover the previous incarnation's transient dataflows.
- **controllerd restart.** Sessions and transient-dataflow bookkeeping
  are ephemeral in controllerd and lost, the dataflows themselves keep
  running on the replicas. Adapters detect the break, keep serving from
  retained mirror state and unexpired holds, and re-establish when
  controllerd serves sessions again (early in its boot, spec 1). Pending
  data-plane peeks are unaffected (spec 3), and the restarted controllerd
  enacts no downgrades until sessions re-register (freeze rule).
- **Transient dataflow adoption.** On re-establishing its session with a
  restarted controllerd, the adapter re-declares the transient dataflows
  it still owns (its running subscribes and COPY TOs, in-flight slow-path
  peeks are simply retried). Controllerd adopts a re-declared dataflow
  that is still running: it enters it into its state and command history,
  so replica reconciliation matches and keeps it, and acquires its input
  holds, which are still available because the freeze rule and the
  durable handle positions kept sinces where the predecessor left them.
  Data-plane delivery of an adopted dataflow is untouched throughout, the
  data-plane connection is independent of the control plane (spec 3). If
  adoption cannot re-acquire capability, or nobody re-declares a dataflow
  within the drain grace, it is torn down with the usual drop event and
  error. Replicas retain unrecognized running transient dataflows across
  reconciliation for the drain grace to give adoption time (spec 3), so
  the drain grace must also cover controllerd
  restart-to-session-re-establishment.
- **Both restart.** Equivalent to controllerd restart followed by adapter
  session establishment. No ordering requirement between the two: a
  controllerd instance acquires its critical handles and serves sessions
  without any adapter present (v1 handle scheme requires no registration
  round trip, spec 4).

## Failure semantics while controllerd is unreachable

- A **running** adapter keeps serving index fast-path peeks (unexpired
  holds, retained mirror, data-plane endpoints from the persisted endpoint
  collection) and persist fast-path reads, until hold self-expiry.
- Table reads and writes (adapter write stack) continue unaffected,
  including writes to just-created tables (persist bring-up is
  adapter-side, spec 1).
- DDL continues: catalog commits do not require controllerd. Enactment
  resumes when controllerd returns. Newly created objects are not readable
  via clusters until enactment (their first holds and dataflows require
  controllerd), except adapter-written collections, which are readable via
  persist fast path immediately.
- A **freshly booted** adapter with controllerd unreachable has no lease
  session, so no cluster-backed reads at all: it serves DDL, table writes,
  and persist-backed reads via its own leased persist handles (whose
  validation is inherent), and defers index-backed reads with retryable
  errors. The design doc states this scope limitation of the availability
  property explicitly.
- Ephemeral commands (slow path, SUBSCRIBE, COPY TO, COPY FROM) fail with
  retryable errors or wait, per the gate-attributed deadlines.

## Invariants

1. Adapter-chosen query timestamps are only chosen at or above granted
   frontiers of holds owned by the choosing adapter (acquire-then-choose).
   User-pinned `AS OF` timestamps are validated at execution instead.
2. A granting instance never advances a collection's since past a hold of
   a live, unexpired session it granted, except when the collection is
   dropped, and it never grants frontiers below its own durable capability.
3. Requests carrying `catalog_ts` are processed only when the applied
   catalog frontier is beyond `catalog_ts`.
4. Arbitrarily delaying, withholding, or coalescing mirror updates never
   produces wrong results, only delays and retryable errors (the testable
   form of "the mirror is not a safety input").
5. An adapter must not serve reads backed by holds whose renewal it cannot
   prove within (TTL - skew margin) on its monotonic clock, and a
   displaced or fenced incarnation stops serving entirely within a bounded
   deadline.
6. Session establishment is ordered by incarnation: lower is rejected,
   higher displaces, reconnects at the same incarnation resume.
7. Transient dataflows are owned by exactly one session and do not survive
   it beyond the drain grace.
8. All controllerd-to-adapter communication is the mirror, responses to
   adapter requests, or persist collections. There is no other
   back-channel.
