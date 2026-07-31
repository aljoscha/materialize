# Review of the controllerd design

Reviewed commit: `0133367d` (`controllerd: initial notes, design doc, and specs`)

## Recommendation

Revise the normative specs before using them as implementation contracts.

The overall direction is strong. In particular, using the catalog as the
command channel, retaining the adapter write stack, separating control and
query data, and layering execution-time validation under leases all fit the
current architecture. The current documents nevertheless leave several
capability and fencing contracts either impossible to implement literally or
weaker than their stated invariants. Most can be fixed without changing the
overall architecture, but they should be resolved in the design rather than
left to individual implementation issues.

This review compares the proposal to the code at the reviewed commit and to
the existing design documents listed under [Review scope](#review-scope). It
distinguishes current behavior from proposed new work. A missing implementation
of an explicitly proposed feature is not itself a finding.

## Mandatory corrections

### 1. The lease-backing rule does not exist for compute collections

Spec 2 requires every granted collection hold to be backed by the granting
controllerd instance's durable capability, identified as a critical handle:

- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:75-81`
- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:300-305`

Spec 4 extends this to a critical handle for every collection a lease session
can hold:

- `doc/developer/design/controllerd/04-multi-controllerd.md:62-74`

The main design simultaneously gives the reason this cannot be the compute
contract: compute holds have no Persist-handle equivalent:

- `doc/developer/design/20260731_controllerd.md:162-172`

That distinction is concrete in the current code. A compute `ReadHold` adds an
entry to an in-memory `MutableAntichain`:

- `src/compute-client/src/controller.rs:1147-1166`
- `src/compute-client/src/controller/instance.rs:2441-2458`

Releasing or downgrading those holds changes dependency holds and emits an
`AllowCompaction` command to the replica:

- `src/compute-client/src/controller/instance.rs:1922-1981`
- `src/compute-client/src/protocol/history.rs:122-150`

The current controller's Persist critical handles are owned by
`StorageCollections` and back storage collection shards. They do not directly
back an index or other compute collection:

- `src/persist-client/src/critical.rs:129-157`
- `src/storage-client/src/storage_collections.rs:653-724`

This also invalidates the literal controllerd boot sequence. Spec 1 says that
controllerd can serve all hold grants before rebuilding controller state
because grants require only critical-handle state:

- `doc/developer/design/controllerd/01-components-and-state.md:136-140`
- `doc/developer/design/controllerd/01-components-and-state.md:246-262`

At that point controllerd has not reconstructed a compute collection's read
frontier or command history and has not reconnected to its replicas. It cannot
perform “grant at current since” for that collection.

The spec needs a separate compute contract, and that contract must prove all of
the following:

1. Storage grants are backed directly by Persist critical handles.
2. A live compute lease is represented in the compute controller's read
   capabilities, holds the dataflow's dependencies, and constrains
   `AllowCompaction` while the lease is live.
3. Restart either defers compute grant and renewal until instance state is
   rebuilt, or recovers previously granted absolute holds early enough to
   constrain rebuild. The latter option also needs a proof that a predecessor's
   in-flight `AllowCompaction` cannot overtake the recovered hold.
4. A new compute grant at “current since” waits until controllerd has
   reconstructed the collection's read frontier.

Without such a distinction, two implementers will build different capability
models, and the stated early-boot availability property has no implementable
compute-side proof.

### 2. A stale same-name controllerd can advance a shared storage handle past a successor's grant

Spec 1 deliberately does not fence overlapping restarts of the same stable
controllerd name at Persist. Both processes share the same critical handle and
opaque:

- `doc/developer/design/controllerd/01-components-and-state.md:40-56`
- `doc/developer/design/controllerd/04-multi-controllerd.md:87-92`

The execution-time validation layer prevents wrong rows, but this is not enough
to satisfy the lease contract. Spec 2 promises that a collection's since does
not pass a live granted hold:

- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:75-81`
- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:300-305`

The following interleaving violates that promise:

1. The old process receives a hold downgrade to 100, but has not yet committed
   its critical-handle downgrade. The durable handle is still at 50.
2. A successor with the same stable name opens the same handle and grants 50 to
   the adapter.
3. The old process commits its already-computed downgrade to 100. Because both
   processes use the same opaque, Persist accepts it.

Persist explicitly allows concurrent processes to use one critical reader ID.
Its protection is equality compare-and-set on the opaque supplied by the
caller:

- `src/persist-client/src/critical.rs:200-221`
- `src/persist-client/src/critical.rs:301-339`

Process supervision and the catalog-freshness deadline only bound this window.
They do not prevent the interleaving. Replica or Persist admission turns the
result into a query error, but the design advertises leases as preventing these
errors during a controllerd restart and states the stronger invariant
explicitly.

Keep the well-known critical reader ID if desired, but fence handle mutation on
every controllerd incarnation. A successor should compare-and-set the handle's
opaque to a token containing its per-boot incarnation before it grants or
renews holds. Any pending predecessor downgrade must then fail. An alternative
is a takeover protocol that proves predecessor quiescence before grants, but a
fresh opaque is both simpler and consistent with the existing critical-reader
API.

### 3. The critical-handle token is underspecified and Persist is assigned a guarantee it does not provide

Spec 4 says that v1 widens the opaque to `(deploy generation, assignment
epoch)`, automatically migrates the existing encoding, and relies on Persist to
reject lower tokens:

- `doc/developer/design/controllerd/04-multi-controllerd.md:13-25`
- `doc/developer/design/controllerd/04-multi-controllerd.md:76-100`

The current opaque has a codec name and exactly eight payload bytes:

- `src/persist-client/src/critical.rs:94-127`

The Persist operation compares opaque values for equality. It does not order
them:

- `src/persist-client/src/critical.rs:200-221`
- `src/persist-client/src/critical.rs:301-339`

Today `StorageCollections` implements ordering in the client. It decodes the
current `PersistEpoch`, compares it with its own epoch, and only then performs
the equality CAS:

- `src/storage-client/src/storage_collections.rs:696-720`

The spec must define:

1. The concrete encoding and overflow policy. Two unrestricted `u64` values do
   not fit into the existing payload.
2. Whether `Opaque` itself is widened, or how the two values are packed into 64
   bits.
3. The client-side ordered takeover loop. Persist arbitrates equality only.
4. Migration from the `PersistEpoch` codec, including how a first new-format
   opener compares a legacy token before changing the codec.
5. How the per-boot fencing required by finding 2 participates in the token or
   otherwise fences handle mutation.

This is durable compatibility state that ships in v1 according to the spec. It
cannot remain an implementation detail.

### 4. Catalog freshness is not an arbiter for orchestrator mutation

Spec 4 permits a role holder to act after confirming that its applied catalog
state matches the current upper within a recency window. It then claims that at
most one role holder acts and that destructive actions are performed only by
the current holder:

- `doc/developer/design/controllerd/04-multi-controllerd.md:112-133`
- `doc/developer/design/controllerd/04-multi-controllerd.md:184-191`
- `doc/developer/design/controllerd/04-multi-controllerd.md:297-302`

The check cannot provide that invariant. A predecessor can confirm freshness,
the adapter can commit reassignment, and the predecessor can then perform an
orchestrator call before its next check. The successor can act during the same
window.

The current orchestrator interface has no conditional or epoch-aware mutation.
`ensure_service` updates the service with a matching ID, and `drop_service`
drops by ID:

- `src/orchestrator/src/lib.rs:53-71`
- `src/orchestrator-kubernetes/src/lib.rs:585-601`
- `src/orchestrator-kubernetes/src/lib.rs:1286-1295`

Current replica service identity contains cluster ID, replica ID, and deploy
generation. It does not contain a role-assignment epoch:

- `src/controller/src/clusters.rs:672-689`
- `src/controller/src/clusters.rs:963-977`
- `src/controller/src/clusters.rs:1002-1017`

Adding an assignment epoch as a mutable label is insufficient. A stale
`drop_service(id)` does not condition deletion on the label still having the
expected value, and a stale `ensure_service(id, ...)` can overwrite the
successor's configuration. The creation-bound check prevents a follower that
has never seen a resource from reaping it. It does not make a later mutation
atomic with role ownership.

Specify one of these mechanisms:

- Epoch-qualified immutable service identities, with successors creating their
  own resources and only reaping strictly older identities.
- Conditional ensure and delete operations whose precondition checks the
  current immutable owner epoch atomically with mutation.
- A handover that fences and acknowledges the predecessor, or waits out the
  full freshness bound, before the successor is allowed to perform conflicting
  or destructive actions.

The design may allow bounded overlap for idempotent, self-correcting work. If
so, invariant 1 must say that, and the document must enumerate which role
actions require exclusive takeover.

### 5. A last-writer-wins expression cache cannot be the enacted-plan provenance store

The documents assign three incompatible roles to the expression cache:

1. It is a non-authoritative, multi-writer, last-writer-wins cache:
   `doc/developer/design/controllerd/01-components-and-state.md:195-199`.
2. It converges to the plan controllerd is running, and EXPLAIN reports that
   plan's provenance:
   `doc/developer/design/controllerd/01-components-and-state.md:127-133` and
   `doc/developer/design/20260731_controllerd.md:209-216`.
3. Sinks are never cached, but controllerd writes every re-optimized plan back:
   `doc/developer/design/controllerd/01-components-and-state.md:76-84` and
   `doc/developer/design/controllerd/01-components-and-state.md:127-130`.

The current cache confirms the first model. Its key is `(build version,
GlobalId, expression type)`. Its value records optimizer features but no writer
or enacted-plan identity:

- `src/catalog/src/expr_cache.rs:55-95`

Concurrent conflicts are retried after syncing, then the caller's value replaces
the current value:

- `src/catalog/src/expr_cache.rs:291-366`
- `src/durable-cache/src/lib.rs:266-329`

Adding provenance fields does not fix the ordering problem. After controllerd
writes the plan it enacted, an adapter boot-time optimization can overwrite the
same key with an advisory plan. The cache then truthfully describes its last
writer but not what is running. This window is unbounded if controllerd has no
reason to resolve that object again.

Choose one contract:

- Keep the expression cache advisory. EXPLAIN may show cached optimization
  provenance, not enacted-plan provenance.
- If exact enacted-plan provenance is required, store it in controller-owned,
  single-writer state keyed by object and role epoch, separate from optimization
  candidates.

In either case, key reusable optimization candidates by every input that makes
plans semantically reusable, and state explicitly that sink plans are neither
read from nor written to the expression cache.

## Strong suggestions

### 6. The freshness rule must define what happens to lease renewal and its backing capability

When a role holder cannot prove catalog freshness, spec 4 stops lease granting
and mirror serving:

- `doc/developer/design/controllerd/04-multi-controllerd.md:112-128`

If “lease granting” includes heartbeat renewal, a catalog-read outage longer
than the freshness window causes the adapter's existing holds to become
unproven and eventually stops all lease-backed reads. This is a new coupling in
v1. Current in-process read holds do not require repeated catalog reads to
remain valid.

- `src/compute-client/src/controller.rs:1147-1166`
- `src/storage-client/src/storage_collections.rs:653-724`

If renewal is allowed to continue, the design needs a different proof. Renewal
does not itself advance a since, but a successor can retire the predecessor's
backing handle after reassignment. The predecessor must not extend a lease
beyond the lifetime of that backing capability. Define whether renewal stops,
how the adapter learns that it stopped, and how takeover grace relates to the
last possible acknowledged expiry. If renewal stops, document the resulting
availability property and size the freshness window for expected catalog read
outages. If it continues, handle retirement needs an arbiter or an acknowledged
handover that preserves every renewed hold.

### 7. Table DDL acknowledgment and adapter boot must preserve the existing write-stack barrier

The design says that tables are writable as soon as the catalog commit lands,
that all DDL is only a catalog commit, and that adapter frontends start before
the controllerd connection:

- `doc/developer/design/controllerd/01-components-and-state.md:72-75`
- `doc/developer/design/controllerd/01-components-and-state.md:85-94`
- `doc/developer/design/controllerd/01-components-and-state.md:224-244`

It is correct that controllerd is not required. It is not correct that the
catalog commit alone makes the table writable. Current post-commit application:

1. Obtains a write timestamp and confirms catalog leadership.
2. Opens the collection and obtains registration metadata.
3. Registers the table through the FIFO group committer.
4. Only then returns the applied timestamp.

See:

- `src/adapter/src/coord/catalog_implications.rs:1097-1142`
- `src/adapter/src/coord/appends.rs:10-31`
- `src/adapter/src/coord/appends.rs:839-871`

Current bootstrap likewise initializes storage collections and tables before
the coordinator begins normal serving:

- `src/adapter/src/coord.rs:2533-2579`
- `src/adapter/src/coord.rs:2877-2891`
- `src/adapter/src/coord.rs:5149-5189`

Narrow the claim to “deterministic controller enactment is catalog-driven.”
CREATE TABLE and webhook acknowledgment must await adapter-local Persist
bring-up and txn-wal registration. Adapter boot must complete the corresponding
registration and system-table snapshot barriers before admitting operations
that depend on them. None of this needs controllerd.

The same section says all user-visible DDL errors occur before commit, while
spec 2 lists post-commit ALTER readiness waits:

- `doc/developer/design/controllerd/01-components-and-state.md:110-117`
- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:194-196`

Define which statements acknowledge at commit and which return a distinct
“committed, but enactment wait failed or timed out” result. A client must be
able to tell whether retrying the statement repeats a durable change.

### 8. The finalization predicate needs the current FIFO lifecycle as an explicit prerequisite

Spec 4 correctly requires a WAL record, no live catalog reference, and txn-wal
absence observed no earlier than the catalog drop:

- `doc/developer/design/controllerd/04-multi-controllerd.md:215-241`

It also makes “once forgotten, never re-registered” normative. The txn-wal API
does not enforce that rule. It explicitly supports registration after forget:

- `src/txn-wal/src/txns.rs:191-215`
- `src/txn-wal/src/txns.rs:1151-1177`
- `src/txn-wal/src/txns.rs:1721-1739`

Today the safety argument is supplied by the single FIFO group committer.
Registration, appends, and forgets are serialized, and drop enqueues forget
after all staged appends:

- `src/adapter/src/coord/appends.rs:10-31`
- `src/adapter/src/coord/appends.rs:273-310`
- `src/adapter/src/coord/ddl.rs:730-752`

Carry that into the normative lifecycle:

1. Catalog existence is desired registration, not instantaneous membership.
2. Create acknowledgment follows successful registration.
3. Drop prevents any new registration request.
4. FIFO orders all earlier registration and appends before forget.
5. Only the resulting forget observation can satisfy finalization.

The predicate is sound under those rules. Without them, the bare txn-wal API
permits the later registration the predicate assumes away.

### 9. Expiry, drain grace, and transient-dataflow holds need separate states

Spec 2 allows the same session incarnation to resume transient dataflows during
the drain grace after lease expiry, and requires capability to remain until
results are delivered or observably canceled:

- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:242-254`

It does not say which holds survive TTL expiry. This matters once more than one
session exists because the zero-session freeze may not apply.

Define two capability classes and their transitions. At TTL expiry, reject new
work and decide explicitly whether ordinary standing holds are retained until
the drain grace or released for reacquisition. Dataflow-scoped input holds for
an existing transient dataflow must survive the expiry if that dataflow can be
resumed. At the end of drain grace, terminate the dataflow observably before
releasing those input holds.

A resumed session either regains its retained standing holds or installs new
ones. The existing transient dataflow continues on its own holds. If the
intended implementation releases all capability at TTL, resumption must be
forbidden and the operation must be recreated.

### 10. Restart overlay semantics for the frontier mirror need an epoch and reset rule

Spec 2 says frontiers are monotone within a stream, compute frontiers may
regress across controllerd restarts, and the adapter retains and “overlays” its
old mirror:

- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:166-179`

“Overlay” does not determine whether a lower new-stream frontier replaces the
retained one, is joined with it, or remains hidden until a snapshot barrier.
The choices have different behavior for local DDL waits and timestamp freshness.
Current controller code ignores regressing frontier reports within one
incarnation:

- `src/compute-client/src/controller/instance.rs:1863-1888`
- `src/compute-client/src/protocol/response.rs:37-50`

Give every mirror stream an epoch and an initial-snapshot completion marker.
Define atomic replacement rules for old-epoch entries, explicit drop handling,
and which consumers may use retained values before the new snapshot completes.
Retained progress must never count as evidence that the new controllerd has
enacted an object.

### 11. Preserve the per-operation oracle observation required by strict serializability

The specs retain oracle ownership in the adapter but do not state the current
per-operation rule. Strict serializability requires the read timestamp to be
chosen using an oracle observation made during the operation's real-time
interval:

- `doc/developer/design/20220516_transactional_consistency.md:38-49`
- `doc/developer/guide-adapter.md`, “Timestamp selection must respect real-time
  bounds”

Current timestamp selection acquires holds first and applies the current
operation's oracle timestamp as a constraint:

- `src/adapter/src/coord/timestamp_selection.rs:270-339`
- `src/adapter/src/coord/timestamp_selection.rs:495-528`

Add a protocol invariant that strict-serializable and bounded-staleness reads
perform the same per-operation shared-oracle observation as today. The lease
grant and frontier mirror constrain readability and readiness. They never
replace linearization at the oracle.

Also clarify the mirror's role in timestamp selection. A mirrored upper may
decide whether to issue or wait. It is not proof that a timestamp is readable,
which is consistent with spec 2's “mirror is progress only” rule.

### 12. The data plane needs implementable terminal, cancellation, and buffering contracts

Three details need tightening before defining a wire protocol.

First, a connection cannot carry the error whose trigger is loss of that same
connection. Spec 3 nevertheless promises an error response on connection loss:

- `doc/developer/design/controllerd/03-cluster-data-plane.md:92-100`
- `doc/developer/design/controllerd/03-cluster-data-plane.md:158-170`

Require an explicit terminal response when the connection remains writable.
Otherwise, transport loss is the terminal signal and the adapter locally fails
every operation owned by that connection exactly once.

Second, untargeted peeks are issued to every replica, but cancellation and
hold-release are described as one acknowledgment:

- `doc/developer/design/controllerd/03-cluster-data-plane.md:63-68`
- `doc/developer/design/controllerd/03-cluster-data-plane.md:101-104`

After choosing a winner, cancel every issued loser. Release the backing hold
only when each copy has completed, acknowledged cancellation, lost transport,
or crossed its deadline.

Third, disconnected output buffering is bounded, but a connected, slow reader
and every asynchronous handoff are unspecified:

- `doc/developer/design/controllerd/03-cluster-data-plane.md:118-143`

Current adapter SUBSCRIBE delivery is explicitly unbounded:

- `src/adapter/src/active_compute_sink.rs:409-415`

Require bounded per-owner queues at replica process merge, replica-to-adapter,
and adapter-to-pgwire handoffs. Overflow should produce one terminal retryable
error and transient-dataflow cleanup. Preserve the current contiguous,
non-overlapping SUBSCRIBE frontier contract and exactly-one COPY TO completion:

- `src/compute-client/src/protocol/response.rs:81-124`

### 13. Phase 3 must retain enough history to retire handles for dropped collections

Spec 4 derives a critical reader identity from `(collection, role, instance,
assignment epoch)` and says a successor can enumerate predecessor handles from
assignment history plus the catalog's collection set:

- `doc/developer/design/controllerd/04-multi-controllerd.md:166-182`

A dropped collection is absent from the live collection set. Critical readers
survive process and catalog-object lifetimes, and losing an ID can pin
compaction permanently:

- `src/persist-client/src/critical.rs:129-147`

The same shard can also move between GlobalIds during table schema evolution,
which the finalization section acknowledges:

- `doc/developer/design/controllerd/04-multi-controllerd.md:232-237`

Base handle identity and enumeration on durable shard identity, not only the
current catalog item identity. Retain assignment and dropped-shard history at
least until every derivable handle is retired and the shard is finalized. The
unfinalized-shard WAL is a natural place to make this discoverable.

### 14. Promotion and readiness need one generation-level state machine

Spec 4 says “new-generation instances re-open read-write” during promotion:

- `doc/developer/design/controllerd/04-multi-controllerd.md:269-282`

Read in isolation, this can mean reopening the catalog writable, which would
violate the adapter-only writer rule. A writable catalog open increments the
same-generation epoch and commits the fence:

- `src/catalog/src/durable/persist.rs:224-273`
- `src/catalog/src/durable/persist.rs:1172-1208`

The likely intended meaning is that controllerd reopens its controller stack in
write-enabled mode while remaining a read-only catalog follower. Say that
explicitly.

Readiness also waits for “all controllerd instances,” without defining the
membership set or replacement of a dead member:

- `doc/developer/design/controllerd/01-components-and-state.md:157-164`
- `doc/developer/design/controllerd/04-multi-controllerd.md:264-268`

Current deployment state distinguishes `Initializing`, `CatchingUp`,
`ReadyToPromote`, `Promoting`, and `IsLeader`:

- `src/environmentd/src/deployment/state.rs:18-48`
- `src/environmentd/src/deployment/state.rs:137-193`

Define a desired, generation-scoped set of stable controllerd readiness IDs,
status expiry and replacement, and the aggregate transition to `IsLeader`.
Promotion should be complete only after the adapter has committed the catalog
fence, every required role owner has acquired its write-enabled capabilities,
and promoted lease sessions are being served. A forced catch-up skip must not
bypass fences or capability acquisition.

Do not make acknowledged old-generation halt a prerequisite. Current fencing is
observed asynchronously. The new generation should proceed after durable fence
and capability takeover, with non-Persist side effects protected by the
corrected role-handover protocol.

### 15. Catalog follower resync needs durable progress and idempotence rules

The production follower is correctly listed as prerequisite work, and the
adapter guide confirms that no code applies another writer's catalog diff
today:

- `doc/developer/design/controllerd/01-components-and-state.md:303-320`
- `doc/developer/guide-adapter.md`, “Catalog changes and their implications”

The proposed “full snapshot plus diff against applied state” must define:

- The durable or reconstructible identity of the last applied catalog
  frontier.
- How a compacted catalog history produces the same desired controller state
  without replaying non-idempotent side effects.
- Idempotence keys for orchestrator and controller-owned collection effects.
- A snapshot-to-desired-state reconciliation that does not drop and recreate
  compatible running resources.

The current implications migration is directionally aligned, but still has
sequencer side-effect closures and state kinds not represented as implications.
Those are correctly called out as phase 0 work. The follower contract should be
completed before that work is declared sufficient.

### 16. The persist fast-path capability statement contradicts degraded boot

Spec 3 says a local Persist fast-path read requires both a controllerd lease
hold and a short-lived leased Persist read handle:

- `doc/developer/design/controllerd/03-cluster-data-plane.md:145-156`

Spec 2 says a freshly booted adapter with no controllerd session can serve the
same read using its leased Persist handle:

- `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:289-294`

The latter is the sound safety boundary. Persist validates the leased read
handle against the shard since. A controllerd lease improves availability by
holding back compaction but is not required for safety when the adapter has no
session. State that distinction explicitly.

## Document consistency and cleanup

These issues are not architectural, but they will mislead implementation issue
authors:

- `CONTROLLERD_DESIGN_NOTES.md:545-547` says the design and specs are complete,
  while `CONTROLLERD_DESIGN_NOTES.md:549-556` says drafting has not started.
- The notes mark COPY FROM as awaiting approval at
  `CONTROLLERD_DESIGN_NOTES.md:551-554`, while spec 2 defines it normatively at
  `doc/developer/design/controllerd/02-adapter-controllerd-protocol.md:234-240`.
- The notes require immediate removal of the MV as-of from `create_sql` at
  `CONTROLLERD_DESIGN_NOTES.md:581-583`, while the final design retains it
  through the split and defers removal at
  `doc/developer/design/20260731_controllerd.md:174-181` and
  `doc/developer/design/20260731_controllerd.md:202-208`.

The notes already call themselves non-normative. Mark the stale handoff and
residual sections as superseded, or remove them before distributing the branch
as the implementation source.

## Decisions that match current behavior and should be preserved

1. **Catalog diffs as the deterministic command channel.** This matches the
   direction of the catalog-implications pipeline and the “no local-only
   assumptions” rule in `doc/developer/guide-adapter.md`.
2. **Adapter ownership of group commit, txn-wal registration, and table
   schema evolution.** Current FIFO ordering is load-bearing and does not
   depend on cluster controllers:
   `src/adapter/src/coord/appends.rs:10-31`.
3. **Acquire holds before choosing a timestamp.** Current timestamp selection
   follows this order:
   `src/adapter/src/coord/timestamp_selection.rs:495-528`.
4. **Execution-time validation as the safety floor.** Replica compaction and
   Persist since checks are the right final protection against lease races.
5. **Control/data separation.** Keeping SUBSCRIBE and COPY TO frontiers on the
   control stream while sending payloads to the issuer preserves controller
   hold management without putting controllerd on the data path.
6. **Controller ownership of introspection subscribes.** This agrees with the
   existing unified compute introspection design. Preserve ordered append and
   deletion processing from
   `doc/developer/design/20240610_unified_compute_introspection.md:82-96`.
7. **Read-only 0dt capability through leased Persist handles.** This matches
   the current milestone-1 design:
   `doc/developer/design/20240531_zero_downtime_upgrades_milestone1.md:143-178`.
8. **Hard output constraints during as-of recovery.** Refusing an inconsistent
   dataflow is preferable to silently choosing a later as-of and serving a gap.
9. **Measurement-gated rollout and same-build embedded rollback.** The rollout
   gives the split an appropriate operational escape hatch.

## Concerns investigated but not reported as blockers

- I did not use a “second live adapter session” interleaving. V1 is explicitly
  single-adapter, and phase 3 deploys multiple controllerd instances rather than
  multiple adapters.
- The current absence of assignment epochs, authenticated data-plane identity,
  controllerd provenance fields, and follower APIs is not itself a finding.
  These are proposed features. Findings above concern the contracts those
  features must implement.
- The txn-wal API's support for re-registration does not by itself invalidate
  finalization. The predicate is sound if the existing FIFO lifecycle and the
  no-new-registration-after-drop rule become normative.
- “Re-open read-write” during promotion most likely refers to the controller
  stack rather than the catalog. The finding asks for unambiguous wording
  because the other interpretation would fence the adapter.
- Compute frontier regression across a controllerd restart is legal under
  serializable isolation. The missing piece is deterministic mirror epoch and
  replacement behavior, not a general prohibition on regression.

## Review scope

The review read all 2,107 lines added by `0133367d` and traced the proposed
contracts through the current catalog, adapter, storage, compute, Persist,
txn-wal, orchestrator, and deployment code. The most relevant existing designs
were:

- `doc/developer/design/20231127_pv2_uci_logical_architecture.md`
- `doc/developer/design/20240117_decoupled_storage_controller.md`
- `doc/developer/design/20240531_zero_downtime_upgrades_milestone1.md`
- `doc/developer/design/20240610_unified_compute_introspection.md`
- `doc/developer/design/20250717_a_small_coordinator_more_scalable_isolated_materialize.md`
- `doc/developer/design/20251219_zero_downtime_upgrades_physical_isolation_high_availability.md`
- `doc/developer/design/20220516_transactional_consistency.md`
- `doc/developer/guide-adapter.md`

No implementation tests were run because the reviewed commit and this report
change documentation only. The report was checked with `git diff --check`.
