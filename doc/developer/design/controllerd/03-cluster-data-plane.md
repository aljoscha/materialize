# Spec 3: The cluster data plane

Part of the controllerd design. Depends on specs 1 and 2. This spec defines
the direct connection between adapters and cluster processes for
query-shaped data: fast-path peek issuance, peek results, subscribe batches,
and COPY TO completions. The controller command stream between controllerd
and cluster processes is unchanged except where stated. The admission
machinery defined here is correctness critical: the design's availability
and "errors, not wrong answers" claims depend on it (spec 2's layering
contract), it must not be built as best-effort hardening.

## Motivation constraints

Two properties drive this spec. First, query results must not flow through
controllerd, so that result bandwidth and controllerd availability are
decoupled from query serving. Second, the existing control protocol serves
exactly one client and treats a new connection as a controller replacement,
triggering reconciliation, so adapters cannot share it.

## Endpoint

Each cluster process exposes a data-plane endpoint on its own port, distinct
from the control endpoints and the intra-replica communication ports. The
endpoint accepts multiple concurrent connections without displacement
semantics. Connection lifecycle is independent of the control connection:
data-plane connections and their state survive controller reconnects and
reconciliation.

**Identity and authentication.** Connections carry an authenticated client
identity: role (adapter or controllerd), stable name, and incarnation
(spec 1). Identity is never client-asserted, the replica verifies the
principal (mechanics per deployment platform, decided in phase 2, the
requirement is not deferrable because result delivery is addressed by
identity). Admission is role-gated: only adapters of the environment and
controllerd instances holding the cluster's role connect. The client
likewise authenticates the replica, since a forged replica could return
forged rows. The handshake carries protocol version and the configuration
the data plane needs (result size limits and similar), supplied by the
client from its own system configuration.

Adapters discover data-plane addresses from a controller-owned persist
collection of replica endpoints, written by controllerd as part of replica
lifecycle. Endpoints are durable observations (spec 2's channel placement
rule), so an adapter that boots while controllerd is down can still reach
replicas, and an adapter serving through a controllerd restart does not
lose addressing. Entries carry replica identity and incarnation so stale
endpoints are recognizable.

For multi-process replicas the client connects to every process of the
replica and merges responses with the existing partitioned-client
semantics: a peek is complete when all parts respond, subscribe batches
merge by frontier, COPY TO completions merge by count.

## Peeks on the data plane

A data-plane peek references a target index (or transient peek dataflow
output) by GlobalId, with a peek id, a timestamp, finishing and projection
parameters. Rows return on the same connection. Large results spill to the
peek stash as today: the response carries batch handles, the adapter reads
and deletes the batches from persist directly. Stash batches orphaned by an
adapter crash are swept by age (spec 4, environment role).

**Replica selection.** By default the adapter issues an untargeted peek to
every replica of the cluster and takes the first complete response,
preserving today's masking of slow or rehydrating replicas. Targeted peeks
(replica-pinned sessions) name one replica. Every peek carries a peek-level
deadline at the adapter, independent of admission deadlines, so a stalled
replica or a partial multi-process merge cannot hang a query.

**Capability.** The replica does not track per-peek capability at
admission. Once a peek is admitted, its dataflow state is pinned at the
peek timestamp until the response is sent, as today. The window between
issuance and admission is protected by the adapter's lease-session holds at
controllerd (spec 2). Admission validates that the target's compaction
frontier has not passed the peek timestamp and answers an error otherwise.
This validation is the safety floor for lease expiry and drop races.

**Admission for unknown targets.** Normative behavior: a peek whose target
is unknown is held until the target is installed or an admission deadline
passes, then answered with an error naming the deadline. A peek whose
target was dropped is answered with an error. A peek must never crash the
process (the current panic on unknown targets applies to the control
stream's ordering contract, which does not extend to the data plane).
Replicas may keep short-lived tombstones for dropped collections to
short-circuit the dropped case ahead of the deadline. If implemented, the
tombstone lifetime must be at least the admission deadline plus the
expected mirror staleness bound, so that a delayed peek gets the crisp
"dropped" error rather than a deadline stall. Held peeks are bounded in
count and memory per connection, beyond the bound new peeks are rejected
immediately.

**Reconciliation exemption.** Pending data-plane peeks and delivery state
are owned by (client name, incarnation, data-plane connection). They are
not part of the controller command history and survive control-connection
reconnects and reconciliation. A controllerd restart does not cancel them.
If reconciliation removes a peek's target dataflow, the pending peek
receives an error on its data-plane connection rather than silence.
Teardown of data-plane state happens on data-plane connection loss or
target removal, always with an error response for anything in flight.

**Cancellation.** Peek cancellation is issued on the data-plane connection
by peek id. Completion or cancellation is acknowledged on the data plane,
and the adapter releases the local hold token backing the peek only after
that acknowledgment (cancel-before-release ordering).

**Drops.** When the adapter observes a drop (through its catalog and the
mirror) it cancels its own in-flight peeks and subscribes against the
dropped object. Replica-side admission validation covers the races this
cleanup cannot win.

**Displacement is not revocation.** A displaced or expired adapter
incarnation that still holds open data-plane connections can keep issuing
peeks until admission validation rejects them (compaction catching up) or
the connection is closed. Incident response against a misbehaving adapter
must revoke its network access or credentials, not rely on lease
displacement.

## Subscribes and COPY TO on the data plane

Transient subscribe and COPY TO dataflows are installed through controllerd
(spec 2). Their data is delivered on the data plane to the issuer:

- User subscribes and COPY TO belong to adapter sessions, batches and
  completions flow to the owning adapter (by authenticated identity) on its
  data-plane connections.
- Introspection subscribes are issued by controllerd, which is a data-plane
  client of its own clusters for this purpose, with the same identity,
  ownership, and buffering rules as any client.
- COPY TO writes its output to the target object store from the cluster, as
  today. Only the completion result (row count or error) is delivered on
  the data plane.

Progress reporting is split from data: subscribe and COPY TO frontier
advancement is reported to controllerd on the control stream as ordinary
frontier responses (prerequisite workstream in spec 1), because controllerd
manages the input holds of these dataflows. The control stream carries no
row data, the data plane carries no frontier authority.

If the owning client has no live data-plane connection, cluster processes
buffer bounded amounts of output and then terminate the dataflow, reporting
the termination on the control stream. Controllerd cleans up and the
adapter observes the transient dataflow's drop event in the mirror (spec
2), surfacing a retryable subscribe error to the client.

## Persist fast-path peeks

Peeks against storage collections that today execute on a replica reading
persist move into the adapter: the adapter reads persist directly, applying
the same plan shapes (point lookups and small limited scans, which bound
the work). Cluster processes are not involved, so these reads succeed even
against clusters with no live replicas, a deliberate behavior change
(design doc, user-visible changes). Capability comes from lease-session
holds plus short-lived leased persist read handles per spec 1's persist
handle discipline, validation from persist's own since checks. The scan and
decode cost lands on the adapter process and is bounded by the fast-path
plan shapes.

## Ordering and safety summary

1. Data-plane admission never trusts the client: identity is
   authenticated, timestamps are validated against compaction frontiers,
   unknown targets resolve to wait-or-error, never to undefined behavior.
2. Compaction of a peek target past an in-flight peek's timestamp is
   prevented by lease holds before admission and by dataflow-state pinning
   after admission. The unprotected window is exactly the lease-expiry
   window, and it resolves to errors by rule 1.
3. Data-plane state is owned by (client name, incarnation, connection),
   survives control-plane reconciliation, and is torn down on connection
   loss or target removal, always with an error response rather than
   silence.
4. The control stream carries no query data, the data plane carries no
   frontier authority. Frontier authority for hold management lives with
   controllerd only.
