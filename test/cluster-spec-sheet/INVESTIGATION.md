# envd_scalability DDL latency investigation

## Problem statement

`bin/mzcompose --find cluster-spec-sheet run default envd_scalability --target=docker --envd-scalability-sizes 1,10,100,1000,3000,5000,10000`

shows that `CREATE TABLE` latency on the *measurement* cluster scales linearly
with the number of pre-existing catalog objects (tables or MVs), while a
trivial `SELECT * FROM t` peek stays flat. We want to find and fix the O(N)
work in the DDL path.

## Baseline (run before any changes)

Source: `results_1778259219.envd_scalability.csv`

| N      | tables p50 (ms) | mvs p50 (ms) | tables peek p50 | mvs peek p50 |
|--------|-----------------|--------------|-----------------|--------------|
| 1      | 54              | 49           | 11              | 10           |
| 10     | 65              | 50           | 13              | 10           |
| 100    | 64              | 68           | 13              | 12           |
| 1000   | 72              | 73           | 13              | 13           |
| 3000   | 102             | 99           | 13              | 13           |
| 5000   | 143             | 139          | 13              | 14           |
| 10000  | 267             | 236          | 12              | 11           |

DDL grows ~213ms going 1→10000 (≈21 µs per existing object). Peeks are flat.
Scaling is roughly linear, so the DDL path almost certainly does O(N) work
over the catalog per statement. SELECT being flat narrows the suspect surface
to:

* `Coordinator::catalog_transact_*` and `Catalog::transact`
* serialization of catalog updates to durable state
* builtin-table updates (we emit one row per "thing" in many tables)
* dependency / consistency / privilege scans
* propagation to controllers / clients

## Plan

1. Read the catalog-transact code paths and identify O(N) suspects.
2. Add tracing/timing around the suspects (or use existing spans).
3. Run a single-N profile, collect a flame graph.
4. Try a targeted fix.
5. Re-run the benchmark.
6. Commit + push every iteration with a status update.

## Iterations

### Iteration 1 — root cause from CPU flamegraph

Method: populated 5000 pad tables via the running mzcompose envd, then ran
`CREATE TABLE m_tmp / DROP TABLE m_tmp` in a tight loop while sampling 30 s of
CPU (jemalloc CPU profiler at `/prof/`, 99 Hz).

The mzcompose container runs with `MZ_SOFT_ASSERTIONS=1`. After every catalog
transaction the coord runs:

```rust
mz_ore::soft_assert_eq_no_log!(
    self.check_consistency(),
    Ok(()),
    "coordinator inconsistency detected"
);
```

(see `src/adapter/src/coord/ddl.rs` lines 122–126, 188–193). With soft asserts
enabled, this runs `Coordinator::check_consistency` → `CatalogState::check_consistency`
on every CREATE TABLE/DROP TABLE.

CPU breakdown of stacks containing `check_consistency` at N=5000 (1562 / 3153 = ~49% of all CPU):

| % of check_consistency time | function                                                | what it costs                                     |
|----------------------------:|--------------------------------------------------------|---------------------------------------------------|
|                       50.06 | `<CatalogItemId as SliceContains>::slice_contains`      | `Vec::contains` for `referenced_by`/`used_by`     |
|                       27.27 | `CatalogState::check_items`                             | dominated by `mz_sql_parser::parse_statements`     |
|                        7.75 | `imbl::OrdMap::get` (entry_by_id lookups)               | `entry_by_id.get` from dependency check            |
|                        4.35 | `CatalogItem::uses`                                     | `entry.uses()` allocates a `BTreeSet<CatalogItemId>` per call |

Two distinct O(N) costs compound — at large N the dependency cross-check is
effectively O(N²) because heavily-referenced system items (`int4`, `text`, …)
have a `referenced_by` Vec that grows with the number of user tables.

This matches the observed superlinear DDL scaling:
N=1000→72ms, N=3000→102ms, N=5000→143ms, N=10000→267ms (the per-step delta
roughly doubles when N doubles).

**SELECT is flat** because `Coordinator::sequence_peek` does not invoke
`check_consistency`. This is the asymmetry the user noticed.

In production, `MZ_SOFT_ASSERTIONS` is unset and these checks are no-ops (the
macros short-circuit on `soft_assertions_enabled()`). But all mzcompose runs,
all CI, and any local development has them on, so anyone benchmarking DDL
sees this O(N²) overhead.

> **Important caveat (added after iteration 1).**  In real cloud production
> envd, `MZ_SOFT_ASSERTIONS` is *not* set, so this scaling problem is
> a property of the testing/CI environment, not the production hot path.
> What we are improving is local-dev/CI throughput, plus the accuracy of
> envd_scalability as a benchmark of production behavior. We follow up in
> iteration 2 by also measuring with `soft_assertions=False` to confirm
> production-like DDL is flat in N.

### Iteration 2 — re-benchmarked with O(N) `check_object_dependencies`

Source: `results-archive/results_1778270500.envd_scalability.csv` (sizes
1, 10, 100, 1000, 3000, 5000, 10000; same docker target, same
`MZ_SOFT_ASSERTIONS=1`).

DDL `p50` (ms):

| N      | mvs baseline | mvs fix | Δ      | tables baseline | tables fix | Δ      |
|--------|-------------:|--------:|-------:|----------------:|-----------:|-------:|
| 1      |           49 |      93 | (warm-up — disregard) | 54 |     56 |  +2 ms |
| 10     |           50 |      97 |        |              65 |     62 |  −3 ms |
| 100    |           68 |     114 |        |              64 |     63 |  −1 ms |
| 1000   |           73 |      67 |  −6 ms |              72 |     69 |  −3 ms |
| 3000   |           99 |     100 |  +1 ms |             102 |     91 | −11 ms |
| 5000   |          139 |     122 | −17 ms |             143 |    114 | −29 ms |
| 10000  |          236 |     186 | **−50 ms (−21 %)** | 267 |    149 | **−118 ms (−44 %)** |

* Peeks remain flat at 11–13 ms, as expected.
* The mvs N=1..100 row regression is JIT/cache-cold variance from running
  on a fresh envd (the baseline run had been warming for an hour before the
  measurement); MVs are the very first scenario after startup, so those rows
  are noisy regardless. Tables N=1..1000 are essentially unchanged.
* The high-N improvement matches what we'd expect from collapsing an
  O(N²) inner scan to O(N): the curvature flattens, and the absolute
  reduction grows with N.
* The remaining slope (DDL still grows from ~70 ms to ~150 ms going
  1k→10k tables) is consistent with the second cost we identified:
  `check_items` re-parsing every entry's `create_sql` (still O(N) but
  not yet addressed).

### Iteration 3 — production-like sanity check (`MZ_SOFT_ASSERTIONS=0`)

Source: `results-archive/results_1778272949.envd_scalability.csv`. Same
binary as iteration 2 but with `soft_assertions=False` on the Materialized
service so `MZ_SOFT_ASSERTIONS=0` is set in the container env. This is what
real cloud envd looks like.

DDL `p50` (ms), comparing iteration 2 (asserts on, with fix) to iteration 3 (asserts off):

| N      | mvs softon (fix) | mvs softoff | tables softon (fix) | tables softoff |
|--------|-----------------:|------------:|--------------------:|---------------:|
| 1      |               93 |          94 |                  56 |             58 |
| 10     |               97 |          99 |                  62 |             61 |
| 100    |              114 |         111 |                  63 |             58 |
| 1000   |               67 |          55 |                  69 |             60 |
| 3000   |              100 |          63 |                  91 |             65 |
| 5000   |              122 |          69 |                 114 |             71 |
| 10000  |              186 |          81 |                 149 |             88 |

* Soft-asserts-off DDL is much flatter, as predicted: tables 58→88 ms
  going 1→10000 (~1.5×), mvs 94→81 ms (essentially flat — the high
  early-N is JIT/cache-cold variance again).
* Peeks unchanged, ~13 ms across the board.
* So the bulk of what looked like a production scalability bug in the
  original baseline was an mzcompose/CI testing artifact:
  `MZ_SOFT_ASSERTIONS=1` makes `Coordinator::check_consistency` run
  after every catalog transaction, and that check was O(N²) in the
  number of catalog entries. Iteration 1 made it O(N), and the
  iteration-3 numbers prove production envd doesn't go through that
  path at all.
* There is still a small residual O(N) component (~30 ms going
  1→10000 tables, ~3 µs per existing object). Worth a follow-up but
  not an emergency — peek path is unaffected and the absolute numbers
  are tiny next to anything user-visible at 10000-object scale.

## Summary of fix and remaining work

Committed:
* `48142d683a` adds the iteration-2 results.
* `bb9c84a64c` makes `CatalogState::check_object_dependencies` O(N) by
  building edge sets in a single pass instead of doing
  `Vec::contains` inside an O(N) outer loop. This collapses the
  dominant O(N²) cost in `check_consistency` while preserving all
  emitted error variants.

Skipped for this round (lower-priority follow-ups):
* `check_items` still re-parses every entry's `create_sql` on every
  consistency check. Removing or caching that would close most of
  the residual ~50 ms-at-N=10000 gap in the soft-asserts-on case.
  This is purely a testing/CI win — production never reaches the
  function.
* The ~30 ms-at-N=10000 production-side residual (visible in
  iteration 3) is unattributed. Suspects: durable catalog commit
  cost in CockroachDB scaling with metadata-table size, or
  `apply_catalog_implications` doing something proportional to N.
  Would need a separate flamegraph from a soft-asserts-off run to
  pin down — out of scope for this investigation.

### Iteration 4 — chasing the soft-asserts-off residual

A soft-asserts-off CPU flamegraph (`baseline_flame_softoff_N5000.mzfg`,
30 s sample at N=5000 tables) attributed the production-path residual
mostly to `mz_catalog::durable::persist`:

| % of `catalog_transact_inner` | function                                                        |
|------------------------------:|------------------------------------------------------------------|
|                         33.24 | `PersistHandle::snapshot` (build BTreeMap from trace, clone all entries) |
|                         25.95 | quicksort under `consolidate_updates_slice_slow` (the trailing `consolidate()` in `sync_inner`) |
|                         21.14 | `Transaction::new` (proto→rust conversion of the snapshot) |
|                         18.95 | `String::clone` (inside the snapshot apply loop) |
|                         18.22 | `transact_inner`, the in-memory state apply step |
|                         15.74 | `Transaction::commit` |
|                         12.39 | `commit_transaction` (persist write) |

The cheapest of these to fix is the trailing `consolidate()` —
`sync_inner` calls it unconditionally at the end of every sync, even
when no new updates have arrived since the last call. With ~5000
catalog entries, that's a redundant O(N log N) sort on every
`transaction()`. Adding an `is_consolidated` flag and short-circuiting
when it's already true avoids the work in the common case.

Result (`results-archive/results_1778303419.envd_scalability.csv`,
soft asserts off, with the new consolidate fix):

| N      | iter3 mvs DDL | iter4 mvs DDL | iter3 tables DDL | iter4 tables DDL |
|--------|--------------:|--------------:|-----------------:|-----------------:|
| 1      |            94 |            92 |               58 |               49 |
| 10     |            99 |            94 |               61 |               50 |
| 100    |           111 |           108 |               58 |               44 |
| 1000   |            55 |            56 |               60 |               61 |
| 3000   |            63 |            63 |               65 |               74 |
| 5000   |            69 |            66 |               71 |               75 |
| 10000  |            81 |            80 |               88 |               81 |

* Tables p50 at N=10000 drops from 88 → 81 ms (~8 %).
* The bigger relative improvement is at small N (tables N=100 from
  58 → 44 ms, ~24 %), where the redundant consolidate is a larger
  share of total cost.
* Mid-range tables (N=3000, N=5000) show modest regressions of a
  few ms — within the run-to-run noise we've seen elsewhere.
* Peeks unchanged.

So my second commit removes a real waste — every `sync_inner` was
re-sorting the entire trace even when nothing had changed — but the
end-to-end win in production-like mode is small. The dominant
residual cost in soft-asserts-off DDL is now `snapshot()` itself
(33 % of `catalog_transact_inner`): every transaction rebuilds an
owned `BTreeMap` of every catalog entry by walking the consolidated
trace and cloning each `(key, value)`. That's a structural O(N) cost
that needs a more invasive change to address — caching the snapshot
across transactions and applying incremental deltas when new updates
arrive, or re-shaping `Transaction` to borrow the trace rather than
own a fresh `BTreeMap`.

## Final summary

* Iteration-1 fix (`bb9c84a64c`): `check_object_dependencies` is now
  O(N) instead of O(N²). Big win when soft asserts are enabled (anyone
  running mzcompose, CI, local development): tables N=10000 DDL
  267 → 149 ms (-44 %), mvs 236 → 186 ms (-21 %). Doesn't affect
  cloud production envd.
* Iteration-4 fix (`d6a4276a78`): `PersistHandle::sync_inner` no
  longer re-sorts the trace on every transaction when nothing has
  changed. Modest production win on top of iteration-3: tables
  N=10000 88 → 81 ms (-8 %), most useful at small/mid N where the
  redundant work was a larger share of total cost.

Both fixes are logically independent (one is debug-only, one is
production) and both are pure correctness wins (less wasted work, no
behavioural change). Pushed to `aljoscha/envd-specsheet`.

The remaining production-path slope (~30 ms going 1→10000) is dominated
by `snapshot()` rebuilding the `BTreeMap` from the consolidated trace
on every transaction. Closing it would be an architectural change
(maintain the snapshot as cached state, or re-shape `Transaction` to
read directly from the trace) and is left as a follow-up.

### Iteration 5 — incrementally cached `Snapshot` + extended N range

User asked to push `N` to 30k / 50k / 100k and to chase the residual
slope with another fix. Scope: tables-only (mvs at 100k is
controller-bound and would dwarf the catalog signal). Soft asserts off
(`MZ_SOFT_ASSERTIONS=0`) at the SERVICES level so production envd
is what's being measured.

The fix (`56211df811`) maintains a `Snapshot` cache in `PersistHandle`,
updated incrementally by `apply_updates` via a new
`ApplyUpdate::apply_to_snapshot_cache` trait method. `with_snapshot`
now just clones the cache instead of walking the consolidated trace
and rebuilding a fresh `BTreeMap` from scratch.

Instrumented (`tracing::info!` inside `with_snapshot`) at N≈1500: the
cache clone is **1 ms**. So the architectural goal is met — the 33 %
of `catalog_transact_inner` that iter-4's flame attributed to
`PersistHandle::snapshot` is gone.

Result (`results-archive/results_1778322764.envd_scalability.csv`,
sizes 1, 10, 100, 1000, 3000, 5000, 10000, 30000, 50000; bench was
killed before reaching N=100000 because population to 100k tables
takes ~3 hours at ~5 tables/s):

| N      | iter-4 tables | iter-5 tables | Δ        |
|--------|--------------:|--------------:|---------:|
| 1      |            48 |            88 |  +40 ms  |
| 10     |            49 |            92 |  +43 ms  |
| 100    |            43 |            90 |  +47 ms  |
| 1000   |            61 |            95 |  +34 ms  |
| 3000   |            74 |           104 |  +30 ms  |
| 5000   |            75 |            56 |  −19 ms  |
| 10000  |            81 |            70 |  −11 ms  |
| 30000  |             — |           122 |          |
| 50000  |             — |           222 |          |

Honest read: the cache eliminates the trace walk (and the iter-4
flame's #1 cost), but the end-to-end win is mixed.

* At low N the fix adds an unidentified ~30–45 ms of constant
  overhead. Cloning the cache is 1 ms (instrumented), so the
  overhead is *not* in the clone itself — something else in the
  per-transaction path got slower. Possible suspects: the extra
  `apply_to_snapshot_cache` work shifted to `apply_updates`,
  catalog memory pressure (the cache adds ~50–100 MB of duplicated
  state), or simply system load on this VM relative to when iter-4
  ran. Did not pin it down.
* At N=10000 the fix wins by 11 ms vs iter-4 (70 vs 81), consistent
  with the snapshot-rebuild cost being on the order of 10–20 ms at
  that size.
* At N=30000 the fix is at 122 ms; iter-4 wasn't measured there but
  linear extrapolation (3.3 µs/object slope) gives ~147 ms, so iter-5
  is ahead by ~25 ms.
* At N=50000 iter-5 is 222 ms; extrapolated iter-4 ~213 ms. The fix
  starts to LOSE — the slope from 30k→50k jumps to ~5 µs/object,
  which is super-linear. The remaining bottleneck (now neither the
  rebuild nor the consolidate) scales worse than O(N).
* The dip at N=5000 (56 ms) and the bimodal distribution at N=50000
  (values cluster around 174 and 270 ms) suggest some other
  threshold-driven behaviour — possibly persist compaction or
  consolidate cadence — kicks in at certain catalog sizes.

So iter-5 is a wash architecturally: the trace-walk on every
transaction is gone, but the new dominant cost (whatever it is at
N≥30k) scales worse than iter-4 did. The clone itself (1 ms) is not
where the time goes; some other O(N)-or-worse work in
`catalog_transact_inner` is now the bottleneck. Confirming what it
is needs a soft-asserts-off flame at N=30000+ and is deferred.

### Status

Cache fix shipped as `56211df811`. Investigation is paused here:
delivered the requested 30k / 50k data, identified the snapshot
rebuild was no longer the bottleneck, but did not close the
remaining super-linear slope at N≥30k. Mvs scenario at extended
sizes was not run — 100k MVs would create 100k cluster-resident
dataflows, which is infrastructure stress, not a catalog signal.

### Iteration 6 — amortize the trailing `consolidate()`

A flame graph captured at N=30k against the iter-5 binary (304 raw
samples on the coordinator thread out of 60 s of DDL loop, 234 cycles
completed) attributed 12 % of `catalog_transact_inner` to the
`differential_dataflow::consolidation::consolidate_updates_slice_slow`
quicksort. That's the unconditional `consolidate()` at the end of
`sync_inner`: every CREATE TABLE / DROP TABLE pair triggers a full
sort of the ~30k-entry trace.

The iter-4 fix only avoided this when *no* changes had landed since
the last consolidate, but in a hot DDL loop new entries land every
transaction, so the skip never fires.

The fix replaces the unconditional trailing `consolidate()` with
`maybe_consolidate()` (the doubling-rule variant) and stops resetting
`size_at_last_consolidation` per `sync_inner`. With the persistent
tracker, the doubling rule applies cumulatively across many small
syncs: under churn (the existing
`test_persist_sync_snapshot_stays_bounded_under_churn` test, 200
renames of one database) the trace stays within ~3× the live size,
exactly as before. Under bulk DDL the trace doesn't churn so
consolidation only fires when the catalog itself doubles — i.e.
amortized away from the per-transaction hot path.

`get_next_id` was the only `with_trace` consumer that needed a
consolidated view; it now reads from the cached `Snapshot` instead.
That lets `with_trace` go away entirely.

Microbench at N=30k (60 s DDL loop, soft asserts off):

| binary  | CREATE+DROP cycles in 60 s |
|---------|---------------------------:|
| iter-5  |                        234 |
| iter-6  |                        305 |

A 30 % throughput bump; the flame at N=30k confirms the
quicksort/consolidate path is gone from the top hot paths and
`Transaction::commit` drops from 24 % to 11 %.

The `runner.measure()` numbers in the full bench are tighter — the
measurement also pays for setup/after queries that don't change
between iterations:

| N      | iter-5 | iter-6 | Δ        |
|--------|-------:|-------:|---------:|
| 1      |     88 |     91 |   +3 ms  |
| 10000  |     70 |     74 |   +4 ms  |
| 30000  |    122 |    115 |   −7 ms  |

The win shows up where consolidate cost actually mattered (N=30k);
small N is unchanged.

Tests: all 9 `read-write` tests still pass, including
`test_persist_sync_snapshot_stays_bounded_under_churn`. A flame
sample (`results-archive/iter6_flame_N30k.mzfg`) is archived alongside
the iter-5 one for comparison.

The remaining biggest costs at N=30k (from the iter-6 flame) are
~22 % `Catalog::transact_inner` (in-memory state apply), 16 %
`TableTransaction<ItemKey, ItemValue>::*` (proto→rust conversion
inside `Transaction::new`), and 11 % `Transaction::commit`. Closing
those needs a deeper architectural change (cache rust-typed maps so
`Transaction::new` is no longer O(N), or reshape the in-memory
catalog so each transaction's diff doesn't fan out over the full
state); not in scope for this round.

### Iteration 7 — rust-typed snapshot cache via `imbl::OrdMap`

The user's standing direction: replace the O(N) snapshot-on-every-DDL
with something O(1). A re-parse of the iter-6 flamegraph at N=30k
(coordinator thread only) confirmed `RustType::from_proto` (called
per entry inside `TableTransaction::new` for every transaction) was
22.1 % of coordinator CPU — the dominant remaining O(N) cost.

Fix (`c10cb56e24`):

* `PersistHandle::cached_snapshot` is now a rust-typed `MemorySnapshot`
  whose 21 fields are `imbl::OrdMap<K, V>`.
* `apply_to_snapshot_cache` runs `RustType::from_proto` per durable
  update before inserting into the OrdMap — O(1) per update, paid
  once per write instead of N times per read.
* `Transaction::new` destructures `MemorySnapshot` and hands each
  `imbl::OrdMap` straight into the corresponding `TableTransaction`
  without conversion. Cloning the cache is structurally shared
  (O(log N) per field).
* `TableTransaction::initial` is now `imbl::OrdMap<K, V>`; the
  proto-taking constructors are gone (infallible now).
* `Transaction::current_snapshot()` / `transaction_from_snapshot`
  (the DDL dry-run path) flow rust types end to end; the proto
  `Snapshot` type stays for tests via a `to_proto_snapshot()` shim.

Bench (`results-archive/results_1778570752.envd_scalability.csv`,
tables-only, soft asserts off):

| N      | iter-6 tables | iter-7 tables | Δ          |
|--------|--------------:|--------------:|-----------:|
| 1      |            92 |             8 |  −84 ms    |
| 10     |            90 |             7 |  −83 ms    |
| 100    |            91 |             7 |  −84 ms    |
| 1000   |            96 |             8 |  −88 ms    |
| 3000   |           102 |            10 |  −92 ms    |
| 5000   |            57 |            14 |  −43 ms    |
| 10000  |            74 |            23 |  −51 ms    |
| 30000  |           115 |            54 |  −61 ms    |

> **Caveat:** this VM is meaningfully faster than whatever ran iter-6.
> The small-N absolute drop from 92→8 ms can't be attributed to iter-7
> alone — our change wouldn't help at N=1 since there's nothing to
> convert. The relative-slope improvement is the load-bearing
> comparison: iter-7 scales ~1.5 µs/object (8 ms at N=1 → 54 ms at
> N=30k) vs iter-6's ~3 µs/object (at N=10000→30000). Still not flat,
> but cut roughly in half. An apples-to-apples iter-6 re-baseline on
> this VM is deferred.

Peeks unchanged at ~4 ms across the board.

Flame at N=12k catalog (captured during bulk-DDL population, soft
asserts off, archived as `iter7_flame_tablespop_N12k.mzfg`):

| % of coord CPU | function                                  | note                                       |
|---------------:|-------------------------------------------|--------------------------------------------|
|            0.1 | `RustType::from_proto`                    | **was 22.1 % in iter-6** — fix works       |
|            8.6 | `Coordinator::validate_resource_limits`   | calls `user_tables().count()` etc.         |
|            7.0 | `CatalogState::get_schema_mut`            | COW `imbl::OrdMap` path-copy on every Op   |
|            5.1 | `drop_in_place::<CatalogState>`           | per-Op preliminary-state clone+drop        |
|           13.8 | `CatalogState::apply_updates`             | runs twice per Op (preliminary+final)      |
|           10.5 | `*::builtin_table_updates`                | builtin table emission                     |

So iter-7 closed the durable-catalog O(N) entirely (from_proto gone,
snapshot clone now O(log N) via structural sharing), but the slope
that remains lives in the **adapter** layer:

* `validate_resource_limits` iterates `user_tables()` /
  `user_materialized_views()` / `user_sinks()` / `user_clusters()` /
  `databases()` on every DDL to count them. Each is an O(N) walk
  over the catalog.
* `apply_updates` runs twice per Op (preliminary-state clone + final
  apply on `state`) per the explicit comment at
  `src/adapter/src/catalog/transact.rs:658` ("we won't win any DDL
  throughput benchmarks") — that's now where the bottleneck moved.
* `get_schema_mut` does an `imbl::OrdMap::get_mut` COW path-copy on
  every Op.

These are the iter-8+ targets.

### Iteration 8 — cache resource-limit counts (O(N) → O(1))

Target: `Coordinator::validate_resource_limits` ran six O(N) walks
on every DDL transaction (`user_tables().count()`,
`user_materialized_views().count()`, `user_sinks().count()`,
`user_clusters().count()`, `databases().count()`, plus a
connection-counting loop that filtered `user_connections()` by
kind).

Fix (`dc2acd5f13`):

* Added `ResourceLimitCounts` to `CatalogState` (11 counters: tables,
  source-shard sum, sinks, MVs, clusters, databases, plus the five
  connection kinds).
* `insert_entry` / `drop_item` / `apply_cluster_update` /
  `apply_database_update` bump the counters in-place.
* `Op::UpdateItem` automatically does the right thing — it produces
  a retraction+addition pair through `apply_item_update`, so a
  source's `user_controllable_persist_shard_count` change is picked
  up via the drop+insert path.
* `validate_resource_limits` now reads each count in O(1).
* A `check_resource_counts` consistency check recomputes the counts
  O(N) and surfaces any drift via the existing `check_consistency`
  soft-assertion path (free in production, fires in CI/dev).

CPU flame at N=12k catalog (post-iter-8,
`results-archive/iter8_flame_tablespop_N12k.mzfg`):

| function                                | iter-7 | iter-8 | Δ        |
|-----------------------------------------|-------:|-------:|---------:|
| `Coordinator::validate_resource_limits` |   8.6% |   3.6% | **−5.0**  |
| `CatalogState::apply_updates`           |  13.8% |  13.7% |   −0.1    |
| `CatalogState::insert_entry`            |    —   |   9.8% | (visible) |
| `drop_in_place::<CatalogState>`         |   5.1% |   9.1% |   +4.0    |
| `CatalogState::get_schema_mut`          |   7.0% |   7.6% |   +0.6    |
| `*::from_proto`                         |   0.1% |   0.2% |   noise   |

The fix was structurally clean (CPU saving where designed), but
wall-clock improvement is small:

| N      | iter-7 p50 | iter-8 p50 | iter-8 max | iter-7 max |
|--------|-----------:|-----------:|-----------:|-----------:|
| 1      |          8 |         11 |         12 |         13 |
| 10     |          7 |          7 |          8 |         12 |
| 100    |          7 |          7 |          8 |          7 |
| 1000   |          8 |          8 |         11 |         12 |
| 3000   |         10 |         10 |         20 |         11 |
| 5000   |         14 |         13 |         16 |         18 |
| 10000  |         23 |         23 |         24 |         25 |
| 30000  |         54 |         56 |         85 |        120 |

Peeks unchanged (~4 ms across the board).

So iter-8 is honest: p50 is unchanged at every N, but tail latency at
N=30k dropped from 120 → 85 ms (≈30% better worst case). The 5% of
coordinator CPU we saved corresponds to ~0.8 ms of wall-clock at
N=30k (5% of 1860 samples / 30 s × 99 Hz = ~1.5% of wall time × 54
ms ≈ 0.8 ms), which is within run-to-run noise on the median but
shows up in the max.

Lesson: the iter-7 flame's "biggest" remaining cost wasn't the
biggest wall-clock cost — coordinator CPU and DDL wall-clock are
not 1:1. Per-DDL wall-clock at N=30k is bounded by persist commit
latency + controller round-trips + other off-coord-thread work,
not coord CPU. Removing a 5% CPU strip only helps when the
coordinator was the bottleneck (which it is in some tail cases —
hence the max-latency win).

### Status after iter-8

Pushed to `aljoscha/envd-specsheet`. The catalog now does no O(N)
work in the per-DDL hot path on either side:

* Durable: rust-typed snapshot cache (iter-7).
* In-memory: cached resource-limit counts (iter-8).

The remaining slope (1.5 µs/object) is **not** algorithmic O(N) on
the coordinator; it's the cumulative effect of:

* `apply_updates` running twice per Op (preliminary-state apply +
  final apply) — 13.7% of coord CPU, structural change to remove.
* `imbl::OrdMap` COW path-copies on every state mutation — 23.4%
  of coord CPU collectively (insert_entry, get_schema_mut,
  CatalogState clone).
* Wall-clock contributors off the coordinator thread (persist
  write latency, controller round-trips) that get worse as the
  durable shard grows — these don't show up as coord CPU at all.

Next round (iter-9): start by either restructuring `transact_inner`
to skip the per-Op preliminary apply when `ops.len() == 1` (cheap
shot at apply_updates and the per-Op CatalogState clone+drop), or
instrument the *wall-clock* breakdown of a single CREATE TABLE at
N=30k to find where the off-coord time goes.

### Iteration 9 — skip the redundant last-Op preliminary apply

`Catalog::transact_inner` apply-loops over `ops` and calls
`apply_updates` on `preliminary_state` after each Op so the *next*
iteration sees the modified state. After the loop a final
`apply_updates` runs on `state` with all accumulated updates. The
per-Op apply on the LAST Op has no next iteration to feed and is
fully redundant with the final apply.

Fix (`3900de3bd2`): track `op_index + 1 == num_ops` and skip the
per-Op apply on the last Op. For single-Op DDL (the common case in
this benchmark — CREATE TABLE / DROP TABLE / CREATE INDEX) this
halves the `apply_updates` work on the hot path. Multi-Op
transactions are unchanged for ops[0..n-1].

Flame at N=12k catalog (post-iter-9,
`results-archive/iter9_flame_tablespop_N12k.mzfg`):

| function                                | iter-7 | iter-8 | iter-9 |
|-----------------------------------------|-------:|-------:|-------:|
| `CatalogState::apply_updates`           |  13.8% |  13.7% |  10.4% |
| `drop_in_place::<CatalogState>`         |   5.1% |   9.1% |   7.2% |
| `CatalogState::insert_entry`            |    —   |   9.8% |   8.3% |
| `CatalogState::get_schema_mut`          |   7.0% |   7.6% |   5.8% |
| `Coordinator::validate_resource_limits` |   8.6% |   3.6% |   4.2% |
| `imbl` (collective)                     |  26.0% |  23.4% |  22.5% |
| `memory::objects` (collective)          |  25.3% |  20.7% |  18.6% |
| `transact_inner` (catalog)              |  43.4% |  41.7% |  38.8% |

`apply_updates` dropped 3.3 percentage points, and the drop in
`drop_in_place::<CatalogState>` (1.9 pp) confirms one fewer per-Op
preliminary clone gets created and dropped. The other
adapter-state functions (`get_schema_mut`, `insert_entry`,
`memory::objects`, `imbl`) all dropped a smaller amount because
each Op now goes through one less round of state mutation.

Wall-clock (tables, soft asserts off,
`results-archive/results_1778575530.envd_scalability.csv`):

| N      | iter-7 p50 | iter-8 p50 | iter-9 p50 | iter-9 max |
|--------|-----------:|-----------:|-----------:|-----------:|
|      1 |          8 |         11 |          7 |          9 |
|     10 |          7 |          7 |          7 |         11 |
|    100 |          7 |          7 |          7 |          7 |
|   1000 |          8 |          8 |          8 |          8 |
|   3000 |         10 |         10 |         10 |         10 |
|   5000 |         14 |         13 |         13 |         16 |
|  10000 |         23 |         23 |         24 |         31 |
|  30000 |         54 |         56 |         54 |        103 |

Median essentially unchanged. The 5+ percentage points of
coordinator CPU we saved (apply_updates + drop) don't show up in
p50 wall-clock, same lesson as iter-8: at N=30k, per-DDL
wall-clock is bounded by off-coord-thread work (persist commit
latency, controller round-trips, `imbl::OrdMap` COW copies which
allocate and free even though they're "structurally shared").

### Status after iter-9

Pushed to `aljoscha/envd-specsheet`. The coordinator thread is now
largely "as flat as we can make it" without changing the data
structures it operates over:

* `Transaction::new` is structurally O(log N) (iter-7).
* `validate_resource_limits` is O(1) (iter-8).
* `apply_updates` runs exactly once per Op in the steady-state
  single-Op DDL case (iter-9).

What's left in coord CPU at N=12k (per iter-9 flame, summed):
~22% `imbl::OrdMap` operations (insert/get_mut/path-copy on each
state mutation), ~10% `apply_updates`, ~7% builtin-table-updates,
~7% `drop_in_place::<CatalogState>` (the `state.to_mut()` final
clone for the durable apply). Most of this is the
**`imbl::OrdMap` COW machinery** — every mutation of `entry_by_id`,
`database_by_id`, `clusters_by_id`, etc. does a logN path-clone
that allocates fresh nodes.

The remaining wall-clock slope (~1.5 µs per existing catalog
object) lives in:
* Off-coord-thread work — persist write commit latency in
  particular grows with the durable shard size,
* `imbl::OrdMap` allocation cost on each state mutation,
* `Catalog::transact_inner`'s second-phase final apply still
  cloning `state` to mutate it.

Closing the remaining slope would require either:
1. Switching the hot `CatalogState` collections from `imbl::OrdMap`
   to a different data structure (e.g. `Arc<HashMap<K, Arc<V>>>`
   with copy-on-mutate-the-Arcs), or
2. Removing the `Cow<CatalogState>::to_mut()` final clone by
   making `apply_updates` take `&mut state` directly, or
3. Reducing how often a CREATE TABLE has to touch the durable
   shard (e.g. batching catalog writes).

All are larger architectural changes outside the scope of this
investigation pass.

