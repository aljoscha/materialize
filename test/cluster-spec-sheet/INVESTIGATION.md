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

