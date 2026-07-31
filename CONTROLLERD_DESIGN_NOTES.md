# controllerd design session: working notes

Scratch notes for the in-progress design of splitting the controllers out of
environmentd into a new `controllerd` binary. Not a spec, not committed
content. Records decisions made with the user and open threads, so a later
session can pick up seamlessly.

## Goal (user's framing)

- New binary `controllerd` hosts storage controller, compute controller,
  storage_collections. Independent lifecycle/failure domain from the adapter,
  future independent scaling.
- Design for multiple controllerd instances (e.g. one per cluster)
  cooperating, implement a centralized single controllerd first.
- Controller keeps responsibility for what runs on clusters, absorbing
  cluster "exhaust", ticking things forward, and writing controller-owned
  collections.
- Adapter holds read holds at the controller, periodically downgraded, backing
  its queries.
- Fast-path peeks bypass controllerd: adapter talks directly to clusterd,
  results flow directly back.
- Deliverables: several tight specs (no Rust in them) + a classic design doc.
  Specs only get written once all questions are resolved.

## Decisions so far (round 1, confirmed by user)

1. **Catalog-subscribing controllerd** (pv2 model), not RPC-command-push.
   Adapter commits DDL to the catalog; controllerd subscribes to the durable
   catalog and derives deterministic controller commands itself. Ephemeral
   commands carry a `catalog_ts` for gating. Requires finishing parts of the
   catalog-implications work.
2. **controllerd runs the optimizer** for index/MV dataflows. Catalog stays
   "pure" (SQL only); the expression cache is what makes this feasible.
3. **Unified RPC/lease read holds at controllerd**, revisiting the 2024
   decoupled-storage-controller rejection of RPC holds. Both storage and
   compute holds are leases held at controllerd, downgraded periodically by
   the adapter, expiring if the adapter goes silent. (With per-cluster
   controllerd, a query on cluster C gets all its holds from controllerd(C).)
4. **Streamed frontier mirror**: controllerd streams frontier updates to the
   adapter; timestamp selection runs against the local, slightly stale mirror
   plus held leases. Noted wrinkle: for strict serializable the ts oracle
   already provides the linearization point.
5. **Dedicated clusterd data-plane endpoint**: adapter connects directly to
   clusterd for fast-path peeks AND subscribe batches AND COPY TO completion.
   Transient dataflow creation (slow-path peek, subscribe, copy-to) still goes
   through controllerd; rows/batches flow replica->adapter directly.
   **Persist fast-path peeks move into the adapter** (direct persist read, no
   replica involved) as a simplification.
6. **Design for controllerd-per-cluster, ship centralized first.** Shared
   concerns to design: critical since handles (catalog-tracked handle IDs per
   0dt-v2 doc), environment-wide controller-owned collections (differential:
   self-correcting/partitioned by writer; append-only: writer-ID scoping),
   shard finalization responsibility.
7. **Table writes + adapter-originated introspection appends decouple from
   controllerd**: TableWriter (group commit, txn-wal) moves/stays adapter-side;
   adapter gets its own append machinery for statement logging etc.
   controllerd keeps its own CollectionManager for controller-owned
   collections.
8. **DDL decoupling (open)**: aspiration is that DDL success = catalog commit,
   controllerd converges eventually; awaits on controller work only where
   user-visible behavior demands it. Feasibility exploration in progress
   (agent enumerating today's synchronous DDL<->controller dependencies).
9. **0dt**: spec against current halt-and-restart flow, but make everything
   ready for 0dt v2 (both generations read-write, catalog-version gating,
   catalog-tracked handle IDs).
10. **Spec cut** (agreed): (1) component responsibilities + state ownership
    map, (2) adapter<->controllerd protocol (commands, leases, frontier
    stream, reconciliation, invariants), (3) peek/data-plane bypass
    (clusterd-facing changes), (4) multi-controllerd cooperation invariants,
    plus a classic design doc on top.

## Key code facts (from round-1 research)

- Frontend peek path (`src/adapter/src/frontend_peek.rs`, `peek_client.rs`)
  is an in-process prototype of the bypass: sessions hold `InstanceClient`,
  `Arc<dyn StorageCollections>`, oracle handles, persist client; fast-path
  peek rows flow back on a oneshot without touching the coordinator loop.
- `ReadHold` is an in-process RAII token over an mpsc closure
  (`mz_storage_types::read_holds`); needs a lease-based network replacement.
- Timestamp selection assumes atomic "acquire holds then read frontiers"
  against in-process shared state (`TimestampProvider for Coordinator`).
- DDL apply (`coord/catalog_implications.rs`) treats controller calls as
  infallible-synchronous post-catalog-commit (`.expect(...)`).
- Controllers own no durable state: everything rebuilds from catalog +
  persist. Shard mappings live in the catalog, mutated via `StorageTxn`
  inside catalog transactions committed by the coordinator.
- CTP (`src/service/src/transport.rs`) serves one connection at a time per
  replica; a new controller connection displaces the old. Compute protocol
  has full replica-side reconciliation (`src/compute/src/server.rs::reconcile`);
  storage equivalent in `storage_state.rs::reconcile`.
- Fencing planes: catalog `FenceToken(deploy_generation, epoch)` ->
  `PersistEpoch` opaque on critical since handles -> orchestrator generation
  labels -> CTP displacement. envd_epoch anchors both catalog and persist
  fencing today; controllerd needs its own identity in this hierarchy.
- 0dt already runs a full read-only controller stack: leased read handles
  instead of critical handles, CollectionManager maintains `desired` without
  writing, per-generation clusterd pods, `allow_writes` at promotion.
- Self-correcting write patterns that tolerate concurrent writers:
  `DifferentialWriteTask` (storage-controller/collection_mgmt.rs),
  self-correcting persist sink (compute MV sink), txn-wal multi-committer.
  Append-only collections are NOT multi-writer safe today.
- Subscribe/COPY TO responses currently flow controller -> coordinator loop.
  Peek rows already bypass (oneshot). Watch sets, RTR, oneshot ingestion
  results are in-memory request/response couplings that would become RPCs.
- Group commit's dependencies: oracle, txn-wal (`TableWriteHandle` from
  storage controller today), catalog upper fencing. No cluster controllers.

## Round-2 research findings (all three agents reported)

### Catalog-subscribe readiness
- Durable catalog IS followable by a second process today: `open_read_only` +
  `sync_updates`, proven in tests. `StateUpdateKind` covers items, clusters,
  replicas, system config, shard mappings (`StorageCollectionMetadata`),
  txn-wal shard. Savepoint mode does NOT follow.
- Gap: read-only followers are fenced on every epoch bump (any new writer
  open). Need a production catalog-follower abstraction with reader-friendly
  fencing (reopen-on-epoch-fence or a reader carve-out).
- `apply_catalog_implications` pipeline exists and is diff-driven for
  tables/sources/connections/clusters/replicas/all drops. Sink/index/MV
  Added still ship dataflows from sequencer closures
  (`catalog_transact_with_side_effects`), explicitly marked as pending
  elimination. Closure mechanism (also replayed at DDL-txn COMMIT) is
  incompatible with controllerd; elimination is a prerequisite.
- Expression cache: persist-backed DurableCache, shard id discoverable via
  catalog, keyed (build_version, GlobalId, Local|Global), stores global MIR +
  LIR physical_plan + metainfo, written and awaited BEFORE catalog commit for
  CREATE INDEX/MV precisely so other processes can see plans. But it is a
  cache, not authoritative: controllerd needs the full planner+optimizer
  fallback. Sinks are not cached.
- Biggest engineering item: factoring CatalogState apply/parse
  (mz-adapter/catalog/{state,apply}.rs) + optimize/ module out of mz-adapter
  so controllerd can depend on them (brings mz-sql, mz-transform).
- Shard allocation already happens inside adapter catalog transactions
  (`prepare_state`), mappings flow through the catalog subscription. Wrinkle:
  prepare_state consults live collection state for finalization
  reconciliation; finalization belongs controllerd-side.
- catalog_ts gating: nothing exists today. Primitives: catalog
  `current_upper`/`ensure_not_out_of_sync`, group commit's CatalogUpperHandle.

### DDL sync-dependency census (Q8 outcome)
- Almost no post-commit controller call surfaces errors to users; pattern is
  unwrap_or_terminate (panic + bootstrap fixes). User-visible errors come
  from planning/purification/validation pre-commit. So "DDL success =
  catalog commit" is essentially current semantics.
- Already async awaits (become waits on controllerd frontier feedback):
  ALTER SINK ready (storage watch set), ALTER MV APPLY REPLACEMENT, ALTER
  CLUSTER WAIT UNTIL READY (new record-based path polls a durable
  reconfiguration record resolved by the in-process cluster controller —
  literally the controllerd pattern already implemented; legacy inline path
  scheduled for deletion, don't port).
- MV as-of is chosen pre-commit and written into create_sql: the durable
  handshake controllerd needs already exists for MVs. Index/sink as-ofs are
  chosen at ship time from least_valid_read; controllerd can re-derive.
- Pre-commit validations that are or can be adapter-side (a):
  resource-limit/size checks, connection validation, check_exists,
  check_alter_ingestion_source_desc (pure description compat check),
  storage.config() consumers (derivable from system vars + startup args),
  ComputeInstanceSnapshot (catalog knowledge + adapter transients).
- Hard cases flagged (d): (1) prepare_state on the commit path; (2) read
  holds + frontiers for per-query timestamp selection; (3) txn-wal table
  registration FIFO ordering vs group commit (register after staged appends
  to old collection, forget after staged appends; registration ts becomes
  statement execution ts) — argues for adapter owning txn-wal registration
  end-to-end with TableWriter; (4) webhook monotonic_appender (data-plane
  write channel out of storage controller) — move to adapter append stack;
  (5) instance_client / result channels; (6) RTR probes + INSPECT SHARD.
- Watch sets: consumers are statement-lifecycle logging + ALTER waits; can be
  reimplemented adapter-side against the streamed frontier mirror.

### clusterd data-plane endpoint
- clusterd: two CTP servers (storagectl 2100, computectl 2101), timely mesh
  ports (compute 2102, storage 2103), internal-http 6878. CTP = one client
  at a time; new connection displaces old; new nonce triggers full
  reconciliation. Adapter CANNOT share computectl: needs a sixth
  "computedata" port and a second ingress that bypasses nonce/reconciliation
  and command history.
- Peek commands are broadcast to all workers via the worker-0 command channel
  dataflow; each worker peeks its trace shard and responds; merging in
  PartitionedComputeState (mz_service Partitioned) — reusable by the adapter
  as-is (connect_partitioned). max_result_size is currently learned from
  UpdateConfiguration passing by; adapter must supply out of band.
- Replica keeps NO per-peek hold registry: once a peek is admitted, the
  PendingPeek's cloned trace handle pins the arrangement at the peek ts; the
  controller-held read hold protects only the issue->arrival window. Peek
  before CreateDataflow currently panics (traces.get().unwrap()); peek after
  compaction-past-ts errors. Second-channel design needs wait-or-error
  admission and adapter leases at controllerd to make compaction races
  rare-to-impossible.
- Subscribe/COPY TO responses are ALSO consumed by the controller for
  frontier tracking and read-hold release (instance.rs handle_subscribe/
  copy_to_response). Rerouting them exclusively to the adapter breaks
  controllerd. Fix: report their frontiers via Frontiers responses
  (database-issues#4701) so the data plane carries data only, or tee.
- Peek stash: replica writes batches to a shard derived from peek uuid at
  the peek_stash_persist_location from InstanceConfig; adapter already reads
  + deletes them directly. Survives the split unchanged.
- Persist fast-path peek move to adapter is mostly free: PersistPeek::do_peek
  is a plain persist read (+ txn-wal cache), adapter has PersistClient and
  storage holds already.
- CTP handshake is version+FQDN only; data port wants role auth + the
  retry/backoff treatment.

## Round-3 decisions (all confirmed by user)

1. **Lease/hold model.** No per-query hold RPCs and no dedicated leases for
   txns/DDL either: the adapter runs a local hold-multiplexing layer
   (structurally today's `ReadHold`/`ReadHolds` machinery). Sessions, txns,
   and DDL flows hold local RAII tokens; the aggregate minimum per collection
   is reported to controllerd as ONE lease session per adapter. Granularity
   is **per-collection under one lease session**, not one timeline-wide
   timestamp (else one idle txn pins compaction of the whole timeline).
   Adding a local hold at ts >= the standing hold needs no synchronous round
   trip: the standing hold protects it, so updates flow to controllerd as
   async deltas (today's `ChangeBatch` channel semantics over the network,
   with a session TTL).
2. **Frontier mirror = catalog-enactment signal.** Controllerd streams its
   applied catalog_ts (user framing: "a write frontier that the controllers
   report back") plus per-collection frontiers. Adapter gates direct
   fast-path peeks on the target appearing in the mirror; commands to
   controllerd carry catalog_ts and controllerd gates internally. Watch
   sets, ALTER SINK/MV waits, EXPLAIN TIMESTAMP reimplemented adapter-side
   against the mirror.
3. **Env-wide singleton duties multi-controllerd-aware.** Shard allocation
   stays adapter-side (catalog txn bookkeeping). Finalization + reaping move
   to controllerd and must be made multi-controllerd-safe; user emphasis: be
   very sure reaping is ALLOWED before acting (explicit permission model,
   e.g. catalog-anchored ownership/leases, in the spec). Differential
   env-wide collections: self-correcting / partitioned by responsible
   writer.
4. **RTR probing and INSPECT SHARD become adapter-side library calls**, with
   a very clear interface/boundary for the RTR library (user emphasis).
5. **Webhook appends, statement logging, and txn-wal table registration move
   to the adapter-owned write stack** alongside TableWriter (registration
   FIFO ordering vs group commit is intrinsically write-path).
6. **Controllerd restart safety + availability goal.** Leases are ephemeral
   in controllerd. On controllerd boot: acquire handles, downgrade nothing
   until adapters re-register or a grace period expires. While ZERO adapters
   are registered: freeze lease-backed since downgrades entirely (policy
   machinery keeps computing desired sinces, doesn't enact below the freeze
   floor), with an operational max-freeze-duration escape hatch (dyncfg).
   TTL expiry matters for one stale adapter among several live ones.
   **Explicit design goal: the adapter keeps serving fast-path peeks while
   controllerd restarts** (standing lease TTL outlasts restart, replica
   dataflows keep running, direct data plane).
7. **Provisioning.** Controllerd keeps the orchestrator for clusterd
   provisioning (reacts to cluster/replica DDL from its catalog
   subscription). Provisioning of controllerd itself and cluster->controllerd
   assignment belong to orchestratord/deployment config; our protocol only
   assumes "controllerd is told which clusters it owns".
8. **Q12 resolved: response routing.** Subscribe/COPY TO frontier progress
   goes to controllerd on the ctl connection as `Frontiers` responses (this
   is database-issues#4701; controllerd needs it to downgrade the dataflow's
   input read holds), data batches go ONLY to the adapter on the data
   connection. No data tee, controllerd never on the data path. Hard
   dependency of the data-plane spec.
9. **Q11 resolved: (B), controllerd picks all dataflow as-ofs uniformly at
   apply time.** Invariant: the output collection's initial since and the
   dataflow's as-of are established atomically by the same controllerd, and
   after a crash the as-of is re-derived from the output shard itself (since
   if never written, upper once written, as `as_of_selection` does for warm
   collections). Catalog stays timestamp-free; REFRESH rounding logic moves
   to controllerd; existing `create_sql` as-of honored as an optional
   constraint during migration. **MUST be called out in the design doc as a
   user-visible change needing scrutiny**: `create_sql`/SHOW CREATE no longer
   records the MV as-of, and the MV output since is user-observable.
10. **Q13 resolved: catalog follower fencing semantics.**
    - Same-generation epoch bump (adapter restart): benign, controllerd
      transparently reopens read-only and resumes. Two resume modes:
      continue-from-frontier if the catalog shard since hasn't passed the
      last-applied catalog_ts, else full snapshot + diff against applied
      state. No dataflow churn.
    - Higher-generation token (upgrade promotion): controllerd halts like
      environmentd today, reaped with its generation's clusterds; the new
      generation's controllerd was already running read-only alongside.
      Preserves the current halt-and-restart 0dt flow.
    - Migration-pending open failures only arise across generations,
      subsumed by the above.
    - 0dt v2: case two relaxes to catalog-version gating (the reader
      carve-out), deferred, nothing above blocks it.
    - Standing assumption to write down: controllerd and environmentd of the
      same deploy generation are the same build version (same catalog SQL
      parsing, expression cache keyed on build version).
11. **Q11-adjacent prerequisite**: elimination of the sequencer side-effect
    closures (index/sink/MV creation, DDL-txn COMMIT replay) is declared a
    phase-0/prerequisite workstream in the specs, not re-specified.

## Round-4: adversarial review + resolutions

Two adversarial reviewers (correctness lens, coverage lens) attacked the
round 1-3 state. Core architecture survived: catalog-subscribe model,
optimizer split with expression cache, lease granularity, mirror, response
routing, adapter write stack, persist fast-path move, closure-elimination
prerequisite. The findings below amend or refine earlier decisions. Where a
round-3 decision is superseded, this section wins. Items 1-11 were worked
out with the user (their pushback simplified 4, 9, 11 considerably). Item 12
(COPY FROM) is proposed and flagged for a final nod. Items 13-17 are
review-driven amendments the user did not object to.

1. **Hold acquisition (refines R3.1, supersedes the bare "async delta"
   claim).** The adapter maintains standing holds on ALL collections
   proactively (as the coordinator does today via timeline holds): when the
   mirror reports a new collection, the adapter sends an install request and
   receives the granted since (grant-at-current-since, no reject path, like
   today's acquire_read_holds). Invariant: the adapter never picks a query
   timestamp except >= sinces of holds it already owns (acquire-then-choose,
   never choose-then-acquire). First-touch cost is per collection per
   adapter lifetime, off the query path. Hold/command requests carry
   catalog_ts and controllerd gates them on its own catch-up instead of
   erroring "unknown collection".
2. **Transient dataflow as-ofs (corrects R3.9's overclaim).** Controllerd
   picks as-ofs for CATALOG-DERIVED dataflows only. Transient dataflows
   (slow-path peeks, SUBSCRIBE incl. AS OF, COPY TO) carry adapter-chosen
   as-of + lease session id + catalog_ts; controllerd validates at admission
   that the session holds every import at <= as_of (SinceViolation-class
   rejection on the ctl path, adapter retries with fresh timestamp).
3. **REFRESH MV crash recovery (refines R3.9).** Recovery re-runs as-of
   selection with the output shard's since/upper as a hard constraint, NOT
   "read one shard": the dataflow_as_of vs storage_as_of split must survive
   (dataflow_as_of pulled back toward greatest_available_read for early
   hydration, storage_as_of = first refresh time).
4. **Cross-controllerd dependencies: no peer leases needed.** Critical since
   handles are DURABLE and survive the owning process: controllerd(C) holds
   its own critical handles on every storage collection its dataflows
   import, so inputs stay pinned across its crash. The residual hazard is a
   crash-window ordering invariant: when applying CREATE MV (or any
   dataflow with a durable output), acquire durable input handles at <=
   as-of BEFORE establishing the output collection's since. Plus careful
   handle reaping (item 6).
5. **Data-plane drop/reconciliation races (must be in spec 3).**
   - Replica never panics on unknown-trace data-plane peeks: short-lived
     tombstones distinguish dropped (error now) from not-yet-created (wait,
     with deadline). Today handle_peek does traces.get(id).unwrap() -> panic.
   - Data-plane pending peeks are EXEMPT from ctl-connection reconciliation
     cleanup (today reconcile() drops all pending peeks; that would silently
     kill the availability goal during controllerd restarts).
   - Adapter cancels its own in-flight direct peeks/subscribes on
     mirror-reported drops (the moved equivalent of peeks_to_drop).
   - Drops override leases: holds on dropped collections resolve to errors.
6. **Same-generation controllerd fencing (spec 4 core mechanism).**
   Per-cluster assignment epochs recorded in the catalog by the adapter
   (cluster -> controllerd instance + epoch). Critical-handle opaque becomes
   (deploy_generation, assignment_epoch); CTP handshake and orchestrator
   labels carry the same token; a controllerd observing a higher assignment
   epoch for a cluster halts that ownership. Handle cleanup: critical handle
   IDs registered in the catalog before acquisition (0dt-v2 doc pattern);
   new owner retires predecessor's handles AFTER fencing via assignment
   epoch; periodic reaper retires handles of instances absent from the
   assignment map. Reaping rule: fence first, then retire.
7. **Catalog writing stays adapter-only, and the back-edge dissolves into
   persist.** The cluster controller is a catalog-level reconciler: it reads
   signals and writes durable catalog state; compute/storage controllers
   enact what the catalog says. It STAYS adapter-side. Its inputs (hydration,
   replica status, per-replica frontiers) are already controller-owned
   persist collections written by controllerd; adapter-side consumers
   (cluster reconciler, caught-up evaluation, session notices) read or
   subscribe to those collections. Shards-finalized feedback dissolves too:
   finalization is observable in persist (is_finalized), so the adapter's
   prepare_state clears finalization-WAL entries by checking persist.
   Complete controllerd->adapter flow inventory: (1) the mirror, (2)
   responses to adapter-issued ephemeral commands, (3) persist collections
   controllerd writes anyway. Nothing else, no unsolicited event stream.
8. **Optimizer notices (refines R1.2).** The adapter still optimizes at DDL
   time (it writes the expression cache pre-commit, which is what lets
   controllerd see plans as soon as the item is visible); notices fall out
   of that, as today. Controllerd re-optimizes only on cache miss (version
   bump, feature drift) and DROPS the notices from those runs. Boot-time
   mz_notices rehydration: adapter reads metainfo from the expression cache,
   miss = no notices for that object (accepted degradation). EXPLAIN for
   existing objects: cache read, local advisory re-optimization on miss.
   Nuance to R1.2: both sides have the optimizer, cache-first; the adapter's
   run is authoritative for notices/EXPLAIN, controllerd's for what runs.
9. **Adapter lease self-expiry (simplifies the reviewer's mutual-TTL
   framing).** A restarting adapter simply re-acquires holds like the first
   time (acquire-then-choose). No freeze-floor-conversion rule needed for
   correctness: if sinces advanced during downtime, timestamp selection
   picks max(oracle_ts, since), still linearizable, possibly waits on upper.
   Freeze-at-zero-adapters survives as robustness only (avoids wait storms,
   doubles as controllerd boot grace). The one mandatory rule targets the
   partitioned-but-ALIVE adapter: it must not serve reads backed by holds it
   has not renewed within TTL; expired means stop, re-acquire, re-time or
   abort in-flight txns. Replica peeks and persist reads keep their existing
   compaction validation as defense-in-depth.
10. **Multi-adapter linearizability scoped out.** DML/DQL are linearized
    across adapters by oracle + persist + txn-wal, no issue. The anomaly is
    DDL-visibility only (DROP via adapter A, stale adapter B still resolves
    the name and reads the not-yet-finalized shard). v1 is explicitly
    single-adapter; the recorded forward-compat rule is a pv2-style catalog
    watermark (under linearizable isolation an adapter must have applied
    the catalog up to a watermark tied to client causality before planning).
11. **txn-wal lifecycle (simplifies reviewer's finding).** Membership is
    deterministic from the catalog (table <=> in txn-wal). Register/forget
    timing vs appends is adapter-internal FIFO, unchanged. The only
    cross-process hazard, finalize-vs-forget, reduces to a persist-readable
    predicate: controllerd may finalize a shard only after observing in the
    txns shard that it is not registered. CREATE TABLE + immediate access
    works: table reads are adapter-local persist fast-path, and requests
    carry catalog_ts (see item 1).
12. **COPY FROM (oneshot ingestion).** Adapter->controllerd ephemeral
    command with response: request carries cluster, source spec, catalog_ts;
    controllerd runs it via its storage ctl connection; staged batch handles
    (metadata-sized) return as the command response; cancellation is a
    command; the batch append itself stays in adapter group commit.
13. **Introspection subscribes move wholesale to controllerd** (it owns
    replicas, ctl connections, and the target collections). Amends R3.8's
    absolute phrasing: subscribe DATA routes to the ISSUER of the subscribe
    (adapter for user subscribes, controllerd for its own introspection
    subscribes). The arrangement-sizes history pipeline moves with it.
14. **Transient GlobalId allocation is partitioned per allocating instance**
    (adapter(s), controllerd(s)); transient IDs must not appear in durable
    or user-visible surfaces (today notice IDs leak into
    mz_optimizer_notices rows; resolved by item 8 since notices come from
    the adapter's own optimization runs).
15. **Transient dataflow ownership and GC.** Owned by (adapter instance,
    lease session); GC on session termination (TTL, explicit close) AND
    displacement on re-register (a restarting adapter's new session
    displaces its old one immediately, no TTL wait). Explicit cancel/drop
    ephemeral command for subscribes/COPY TO, plus empty-frontier
    auto-cleanup as the completion contract. Result delivery racing expiry:
    deliverable iff the adapter's local session token is still live (item 9
    rule makes the adapter fail the query otherwise). The #4812
    cancel-before-hold-release ordering generalizes beyond peeks.
16. **0dt phases fully specified (extends Q13).** Read-only phase: the
    new-generation controllerd behaves like today's preflight, savepoint
    snapshot + hydrate + halt-on-observed-DDL (savepoint mode does not
    follow; a live cross-version follower does not exist today and is not
    invented here). Promotion choreography: controllerd derives its role
    from the catalog fence token; ordering: catalog fence commit -> old
    generation halts -> new controllerd flips to read-write (halt-and-reopen
    acceptable) -> adapters re-register leases. Caught-up/readiness:
    controllerd runs per-cluster caught-up + stability evaluation (it owns
    hydration and replica health) and exposes a readiness signal per deploy
    generation; environmentd preflight aggregates it (operational surface,
    e.g. status endpoint, not a protocol back-edge).
17. **Misc homes.** remove_orphaned_replicas gated on catalog_ts (only reap
    replicas absent from a catalog snapshot newer than the replica's
    creation bound). PrivateLink/VPC-endpoint reconciliation + status
    writes move to controllerd (catalog-derived); adapter keeps pre-commit
    connection validation. Spec 1 gets a boot-sequence responsibilities
    table with the rule "adapter boot awaits nothing from controllerd,
    gating is per-object via the mirror". Controllerd gets its own
    dump/internal-HTTP endpoint. Specs note a controllerd "library mode"
    (in-process embedding) for test harnesses.

## Round-5: persona review gauntlet + revision (2026-07-31)

Specs + design doc were written (doc/developer/design/20260731_controllerd.md
+ doc/developer/design/controllerd/01..04) and attacked by nine reviewers
(Linus Torvalds, Kyle Kingsbury, Marc Brooker, tptacek, Dan Luu, and four
contrarians: premise, implementer, YAGNI, SRE). Verdicts: architecture and
consistency story sound (aphyr: "degrades to blocking or retryable errors
rather than wrong answers" survived adversarial interleavings), ~25
spec-text defects found and fixed in a full revision of all five documents.
Highlights of the fixes (all now normative in the specs):

- Identity = (stable name, incarnation), incarnation = catalog fence token
  for the adapter, ordered displacement everywhere, reconnect resumes.
  Build version + generation checked at session establishment.
- AS OF fixed via auxiliary holds (acquire below standing hold, sync RTT on
  historical path only).
- catalog_ts arithmetic pinned (applied frontier beyond commit ts).
- Data-plane state owned by (name, incarnation, connection), NOT lease
  session, survives controllerd restarts, teardown = connection loss or
  target removal, always with error.
- controllerd serves leases+mirror EARLY in boot (before state rebuild),
  adapter retains mirror across restarts, TTL > boot-to-lease-serving is a
  normative constraint. These carry the headline availability property.
- Fast-path gating advisory, replica admission normative, wait-then-error,
  never silent re-plan without index, lag-keyed circuit breaker on
  fallbacks.
- Zero-session freeze unified (no separate boot grace), max-freeze
  resumption is an operator action, not a timer.
- Renewal = constant-work, strict priority; monotonic clock TTL arithmetic
  spelled out; batched hold installs; coalescing mirror.
- Table persist bring-up + schema evolution (alter_table_desc) owned by the
  adapter write stack, so writes never wait on enactment.
- Sinks are never expression-cached (controllerd always optimizes them);
  adapter retains boot-time re-optimization (notices unchanged in practice,
  cache warmed per build).
- v1 keeps the well-known critical handle with widened opaque, registry +
  event-driven retirement (no periodic reaper) deferred to phase 3 with
  deterministic handle-identity derivation (kills the registry back-channel
  circularity, makes cross-topology upgrade/rollback an ordinary generation
  handover).
- Fence hierarchy: authenticated tokens required for the replica ratchet,
  staleness bound on catalog followership as role-holding liveness (covers
  orchestrator-mutation arbiter gap + fenced-writer livelock), fenced-stop
  list includes lease granting + mirror serving, acquire-before-retire.
- Multi-instance lease backing: granting instance acquires own durable
  handles on demand for all leased collections (cost accepted + measured,
  phase 3 entry gate).
- As-of recovery hard-constraint failure poisons the dataflow (was the one
  wrong-answer channel, now closed).
- Finalization predicate got an observation frontier + no-re-registration
  invariant + shard-continuity rule for ALTER TABLE.
- Security: controllerd holds no customer secrets (invariant), port/trust
  matrix in spec 1, session = full-environment read capability stated,
  data-plane identity authenticated, displacement != revocation.
- Observability requirements normative in spec 1 (metrics, trace context,
  correlation ids), alert catalog + runbooks are phase 2a exit gates.
- Rollout: per-environment topology selection, same-build dual topology
  (embedded = rollback path), phase 2 sub-milestones (2a peeks, 2b
  subscribe/COPY TO data plane, 2c persist fast-path + as-of flip),
  measurement gates per phase, honest motivation framing
  (strategy-driven, measurement-gated, no incident-data claim).
- User-visible changes expanded: SUBSCRIBE availability regression,
  persist fast-path without replicas + adapter CPU, EXPLAIN TIMESTAMP hold
  frontiers, MV as-of migration sequencing (keep writing create_sql as-of
  through the split, removal is a later flag-gated change).

Triage rejections (with reasons): YAGNI's cut of the lease TTL (needed for
displaced/partitioned adapter discipline, kept with honesty notes), YAGNI's
deferral of subscribe/COPY TO data plane (user decision, kept as target,
sequenced as milestone 2b), tombstones kept but demoted to optional
accelerator over normative wait-or-error.

## Round-5 verification (post-revision)

Two verification agents ran over the revised documents:

- Findings-closure audit: **pass**. All 11 blockers and every should-fix
  closed with normative text. Four follow-ups it raised were applied:
  controllerd incarnation per-boot component with shared-handle carve-out,
  generation equality (not just ordering) at session establishment, drop
  exception in the availability bullet, freeze in the adapter-down blast
  radius row.
- Fresh-eyes consistency check: strong verdict, two substantive findings
  fixed: (1) the role-liveness bound was re-specified as a FRESHNESS bound
  (confirmation that applied state matches the current catalog upper
  within a recency window) instead of applied-ts-vs-wallclock, so an idle
  catalog or adapter downtime does not paralyze controllerd, (2) the 0dt
  read-only phase now explicitly serves lease sessions backed by leased
  persist handles (stated exception to the grant-backing rule) and the
  promotion text's TTL budget got its antecedent. Also fixed: controllerd
  process identity = (generation, per-boot token) with role epochs as
  separate per-role tokens (multi-role instances have one incarnation),
  session resume-within-drain-grace semantics (session = (name,
  incarnation), re-establishment resumes transient dataflows), mirror
  scope in multi-instance deployments (uppers for all holdable
  collections, enacting role holder's mirror is authoritative for
  appearance), plan provenance mechanism (controllerd write-back is
  mandatory, cache entries record writer/build/config, EXPLAIN reports
  it), readiness HTTP client list, secrets table cells, per-object gating
  wording, finalization time-domain clause, terminology pointers.

Final state: design doc (374 lines) + four specs (1131 lines), style-clean
(no em-dashes, no semicolons, no code), passed adversarial review with all
findings closed or explicitly rejected with reasons.

## Round-6: user review of the change summary (decisions)

- **AS OF goes opportunistic (user directive), auxiliary holds removed.**
  Historical reads below the standing hold acquire no holds: direct
  attempt, execution-time validation (persist since check, replica
  admission), clean history-compacted error. Slow-path pinned AS OF is
  validated at controllerd admission against actual sinces (it manages the
  dataflow's input holds anyway). Specs 2, 3 and the design doc edited,
  invariant 1 reworded (adapter-CHOSEN timestamps acquire-then-choose,
  user-pinned AS OF validated at execution).
- **As-of recovery poison surface named in spec 4**: error + reason in
  controller-owned status collections, no progress in mirror/metrics, like
  a wedged source. History stays readable, resolution is operator-level.
- Clarified for the user (no spec change): zero-session freeze resumes
  automatically when any adapter registers, operator action only for
  permanently-departed adapters. Boot-ordering rationale for early
  lease/mirror serving. Fast-path gating cluster rationale.
- **SUBSCRIBE/COPY FROM bypass declined** (controllerd stays authoritative
  for what runs on clusters, adapter must not grow a mini controller,
  COPY FROM hop is negligible). Instead, user approved **transient
  dataflow adoption** (added to specs 1, 2, 3 and design doc): on session
  re-establishment after a controllerd restart the adapter re-declares
  its running subscribes/COPY TOs, the new controllerd adopts them into
  state and command history (input holds re-acquired under freeze +
  durable handle positions), replicas retain unrecognized transient
  dataflows across reconciliation for the drain grace. Running subscribes
  survive controllerd restarts from 2b on, the SUBSCRIBE regression
  narrows to installation availability plus the 2a delivery window.

## Status / handoff

All open design questions are resolved through four rounds (exploration,
deep-dives, proposals, adversarial review). One item awaits a final nod:
round-4 item 12 (COPY FROM as ephemeral command with staged-batch-handle
response). Next step (NOT yet started, user wants a fresh session/agent for
it): draft the four specs + classic design doc per the agreed cut. Round-4
resolutions supersede earlier text where they conflict.

1. Component responsibilities + state ownership map (durable and ephemeral),
   incl. boot-sequence responsibilities table.
2. Adapter<->controllerd protocol: lease sessions (per-collection standing
   holds, grant-at-current-since installs, downgrade deltas, TTL +
   self-expiry, displacement on re-register), frontier mirror + applied
   catalog_ts stream, ephemeral commands with catalog_ts gating (transient
   dataflows, COPY FROM, cancels), reconciliation on either side's restart,
   invariants (acquire-then-choose, catalog_ts gating).
3. Peek/data-plane bypass: new clusterd data port, second ingress bypassing
   nonce/reconciliation, wait-or-error peek admission, response routing
   (frontiers to ctl, data to adapter, #4701), partitioned client reuse,
   cancellation, peek stash, persist fast-path in adapter.
4. Multi-controllerd cooperation invariants: catalog-tracked critical handle
   ownership, env-wide collections, finalization/reaping permission model,
   fence hierarchy (generation, adapter instance, controllerd instance).

Specs: tight and concise, behavior + interfaces + invariants only, NO Rust
code, not prescriptive about component internals. Then the classic design
doc (doc/developer/design/ style) on top, citing:
20231127_pv2_uci_logical_architecture.md,
20240117_decoupled_storage_controller.md,
20250717_a_small_coordinator_more_scalable_isolated_materialize.md,
20251219_zero_downtime_upgrades_physical_isolation_high_availability.md.
Design doc must include the Q11 user-visible-change callout (MV as-of no
longer in create_sql) and the "adapter serves fast-path peeks across
controllerd restarts" availability goal.

## Residual items for the spec-writing session (not design blockers)

- Fence-token scheme details for (generation, adapter instance, controllerd
  instance) — hierarchy agreed (see rounds 1-3), concrete token layout to be
  written in spec 4.
- Cancellation path details for direct peeks (CancelPeek on the data-plane
  connection; lease release ordering replacing finish_peek's
  cancel-before-hold-drop ordering, cf. database-issues#4812).
- Persist pubsub topology across processes (envd is the pubsub server today;
  with adapter + controllerd + clusterds all writing/reading persist, decide
  who serves pubsub or whether each process pair connects).
- Metrics/introspection ownership per process; replica HTTP proxying
  (`ReplicaHttpLocator` lives in the controller today).
- mz_sessions-style builtin table writes and storage usage stay
  adapter-side (adapter write stack); audit list in 0dt-v2 doc applies.
- max_result_size and similar config for the data-plane connection supplied
  out of band (adapter knows the system vars).
- Data-plane port auth/role handshake on clusterd.
