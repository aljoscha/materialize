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

