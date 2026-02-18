# DDL Performance Optimization Log

## Current Setup

Problem: individual DDL statements (CREATE TABLE, CREATE VIEW, CREATE INDEX,
DROP, ALTER) become slow when thousands of objects already exist. A statement
that takes milliseconds on a fresh environment can take multiple seconds at
scale. We measure per-statement latency at various object counts.

```bash
# Debug build (faster to compile, makes O(n) scaling more visible)
bin/environmentd --build-only
bin/environmentd --reset -- --system-parameter-default=log_filter=info

# Optimized build (for confirming fixes against realistic absolute numbers)
bin/environmentd --optimized --build-only
bin/environmentd --reset --optimized -- --system-parameter-default=log_filter=info

# Connect as materialize user (external port)
psql -U materialize -h localhost -p 6875 materialize

# Connect as mz_system (internal port — needed for ALTER SYSTEM SET, etc.)
psql -U mz_system -h localhost -p 6877 materialize
```

**Important:** After `--reset`, you must raise object limits before creating
large numbers of objects:
```bash
psql -U mz_system -h localhost -p 6877 materialize -c "
  ALTER SYSTEM SET max_tables = 100000;
  ALTER SYSTEM SET max_materialized_views = 100000;
  ALTER SYSTEM SET max_objects_per_schema = 100000;
"
```

To bulk-create objects, write a SQL file and use `psql -f` (much faster than
one psql invocation per statement):
```bash
for i in $(seq 1 1000); do
  echo "CREATE TABLE t_$i (a int, b text);"
done > /tmp/bulk_create.sql
psql -U materialize -h localhost -p 6875 materialize -f /tmp/bulk_create.sql
```

## Profiling Setup

Use `perf record` with frame pointers:

```bash
# Record (after creating objects — run DDL batch during perf recording)
perf record -F 9000 --call-graph fp -p <environmentd-pid> -- sleep <duration>
# (start DDL batch shortly after perf starts)

# Process
perf script | inferno-collapse-perf | rustfilt > /tmp/collapsed.txt

# View as flamegraph
cat /tmp/collapsed.txt | inferno-flamegraph > /tmp/flame.svg

# Or analyze with grep
grep -E 'some_function' /tmp/collapsed.txt | awk '{sum += $NF} END {print sum}'
```

Key profiling notes:
- Use `--call-graph fp` (frame pointers) — the `--profile optimized` build has frame pointers enabled
- 9000 Hz sampling rate gives good resolution without excessive overhead
- Coordinator is single-threaded — DDL goes through the coordinator, so look for
  per-statement CPU time there
- Look for O(n) or O(n^2) patterns: functions whose per-call time grows with object count

## Optimization Log

### Session 1: Baseline measurements (2026-02-18)

**Goal:** Establish baseline DDL latency at increasing object counts.

**Setup:** Debug build, `bin/environmentd` (no `--optimized`). Created tables
with default indexes using `psql -f` batch files. Each table is
`CREATE TABLE bt_N (a int, b text)` which also creates a default index, so
each table contributes ~2 user objects.

**Measurement method:** 5 individual `psql -c` invocations per DDL type at each
scale, measuring wall-clock time with `date +%s%N`. Each sample is a fresh psql
connection → execute DDL → close.

**Results (median of 5 samples, debug build):**

| User Objects | CREATE TABLE | CREATE VIEW | CREATE INDEX | DROP TABLE |
|-------------|-------------|-------------|-------------|------------|
| 0           | 225 ms      | 182 ms      | 218 ms      | 176 ms     |
| ~116        | 250 ms      | 207 ms      | 229 ms      | 193 ms     |
| ~531        | 337 ms      | 287 ms      | 299 ms      | 281 ms     |
| ~1046       | 426 ms      | 374 ms      | 414 ms      | 362 ms     |
| ~2061       | 700 ms      | 630 ms      | 700 ms      | 610 ms     |
| ~3076       | 1034 ms     | 966 ms      | 1006 ms     | 927 ms     |

**Key observations:**
- All DDL types show roughly linear O(n) scaling: ~250-270ms additional latency
  per 1000 user objects.
- All DDL types scale at approximately the same rate, suggesting a shared
  bottleneck in the DDL path rather than type-specific overhead.
- Even the baseline (~225ms for CREATE TABLE) is substantial for a debug build.
  Some of this is likely fixed overhead (persist, cockroach writes, etc).
- At 3000 tables (~6000 user objects counting indexes), a single CREATE TABLE
  takes ~1 second in a debug build.
- The linear scaling across all DDL types points to something in the common DDL
  path that iterates over all existing objects — likely in catalog operations,
  dependency tracking, or coordinator bookkeeping.

**Raw data:** See /tmp/ddl-bench-results.txt for full 5-sample measurements.

**Next step:** Profile the slow path with `perf record` at ~3000 tables to
identify which functions dominate the per-statement cost and where the O(n)
scaling lives.

### Session 2: Profiling — found check_consistency as dominant bottleneck (2026-02-18)

**Goal:** Profile DDL at ~3000 tables to identify where the O(n) scaling lives.

**Setup:** Same debug build. environmentd running with ~3091 user objects (3000+
tables from Session 1). Ran `perf record -F 99 --call-graph fp` during 20
CREATE TABLE statements, processed with `inferno-collapse-perf`.

**Results — Coordinator time breakdown during CREATE TABLE at ~3000 objects:**

| Component | % of coordinator time | Description |
|-----------|----------------------|-------------|
| `check_consistency` | **52.4%** | O(n) consistency check running on EVERY DDL |
| `catalog_transact_inner` (excl check) | 24.5% | Actual catalog mutation + persist writes |
| persist/durable writes | ~17% | Writing to CockroachDB (within transact_inner) |
| `apply_catalog_implications` | 1.5% | Applying side effects (compute/storage) |
| Other coordinator overhead | ~4% | Message handling, sequencing, etc. |

**Inside `check_consistency` breakdown:**

| Sub-check | % of check_consistency | What it does |
|-----------|----------------------|--------------|
| `check_object_dependencies` | **56.1%** | Iterates ALL objects, checks bidirectional dependency consistency |
| `check_items` | **39.7%** | Iterates ALL objects, **re-parses every object's CREATE SQL** |
| `check_internal_fields` | 1.4% | |
| `check_read_holds` | 1.4% | |
| `check_comments` | 0.5% | |
| `check_roles` | 0.4% | |

**Root cause:** `check_consistency()` is called from `catalog_transact_with_side_effects`
and `catalog_transact_with_context` (the two DDL transaction paths) via
`mz_ore::soft_assert_eq_no_log!`. This macro checks `soft_assertions_enabled()`
which returns **true in debug builds** (`cfg!(debug_assertions)`). So every single
DDL statement triggers a full consistency check that iterates over every object in
the catalog.

The two dominant sub-checks are:
1. **`check_object_dependencies`** (56% of check): Iterates all entries in
   `entry_by_id`, for each checks `references()`, `uses()`, `referenced_by()`,
   `used_by()` — verifying bidirectional consistency. O(n × m) where m is avg
   dependencies per object.
2. **`check_items`** (40% of check): Iterates all database → schema → item
   entries and **re-parses the `create_sql` string** for every single object via
   `mz_sql::parse::parse()`. This is the most wasteful — SQL parsing is expensive
   and done for ALL objects on every DDL statement.

**Code locations:**
- `src/adapter/src/coord/ddl.rs:122-126` — soft_assert calling check_consistency
- `src/adapter/src/coord/ddl.rs:189-193` — same in catalog_transact_with_context
- `src/adapter/src/coord/consistency.rs:46` — Coordinator::check_consistency
- `src/adapter/src/catalog/consistency.rs:63` — CatalogState::check_consistency
- `src/ore/src/assert.rs:62` — soft assertions enabled by default in debug builds

**Implications:**
- In **production (release) builds**, `check_consistency` does NOT run (soft
  assertions are disabled), so this bottleneck doesn't affect production.
- In **debug builds** (used for development, testing), this creates severe O(n)
  DDL degradation. This is the entirety of the O(n) scaling we measured in
  Session 1.
- Once we eliminate check_consistency from the hot path, the remaining
  catalog_transact time (~17% persist writes + ~7% in-memory catalog work) may
  reveal a second layer of scaling, but it should be much smaller.

**Next step:** Disable or optimize the consistency check in the DDL hot path.
Options:
1. Skip `check_consistency` entirely during normal DDL in debug builds (only
   run on explicit API call or periodically).
2. Make `check_consistency` incremental — only check the objects that were
   actually modified by the current DDL operation.
3. Optimize the sub-checks: replace `check_items`'s full SQL re-parse with a
   cheaper validation, and optimize `check_object_dependencies`'s contains()
   lookups.

### Session 3: Make check_consistency incremental (2026-02-18)

**Goal:** Eliminate the O(n) scaling from `check_consistency` by making the two
expensive sub-checks (`check_object_dependencies` and `check_items`) only
examine the items affected by the current DDL operation.

**Approach:** Added targeted consistency check methods that scope the expensive
work to only the changed items:
- `check_object_dependencies_for_ids`: only checks dependency bidirectionality
  for the changed items and their immediate dependency neighbors
- `check_items_for_ids`: still iterates schema/item maps (cheap lookups) but
  only re-parses `create_sql` for the changed items
- `check_consistency_for_ids`: replaces the full check in DDL transaction paths

In `coord/ddl.rs`, the two catalog_transact methods now extract affected item
IDs from the ops list before consuming them, then pass those IDs to the targeted
check instead of calling the full `check_consistency()`.

The full `check_consistency()` is preserved for the explicit API endpoint and
other callers — only the DDL hot path uses the incremental version.

**Code changes:**
- `src/adapter/src/catalog/consistency.rs` — added `check_consistency_for_ids`,
  `check_object_dependencies_for_ids`, `check_items_for_ids`
- `src/adapter/src/coord/consistency.rs` — added `check_consistency_for_ids`,
  `affected_item_ids`
- `src/adapter/src/coord/ddl.rs` — modified `catalog_transact_with_side_effects`
  and `catalog_transact_with_context` to use targeted checks

**Results (median of 5 samples, debug build):**

| User Objects | Before (Session 1) | After (Session 3) | Improvement |
|-------------|--------------------|--------------------|-------------|
| 0           | 225 ms             | 183 ms             | 19%         |
| ~116        | 250 ms             | 200 ms             | 20%         |
| ~531        | 337 ms             | 254 ms             | 25%         |
| ~1046       | 426 ms             | 330 ms             | 23%         |
| ~2061       | 700 ms             | 443 ms             | 37%         |
| ~3076       | 1034 ms            | 600 ms             | 42%         |

**Key observations:**
- The O(n) scaling slope improved from ~270ms/1000 objects to ~140ms/1000
  objects — roughly a **2x improvement in scaling**.
- At 3000 tables, CREATE TABLE went from ~1034ms to ~600ms (42% faster).
- The improvement grows with scale (19% at baseline → 42% at 3000 tables),
  confirming we eliminated a significant O(n) component.
- There is still O(n) scaling at ~140ms/1000 objects — this comes from the
  remaining cheap sub-checks (check_internal_fields, check_roles, check_comments
  etc.) which still iterate all objects, plus some O(n) cost in the catalog
  transaction path itself (e.g. builtin table updates, clones, etc.).

**Next step:** Profile again at ~3000 tables to identify the remaining O(n)
sources. The remaining ~140ms/1000 objects is split between the cheap
consistency sub-checks (which could also be made incremental) and the actual
catalog transaction overhead.

### Session 4: Second profiling — identified multiple O(n) layers (2026-02-18)

**Goal:** Profile DDL again after Session 3's incremental check_consistency fix
to identify the remaining O(n) sources behind ~140ms/1000 objects scaling.

**Setup:** Debug build with Session 3 changes applied. environmentd running with
~3091 user objects (~3031 tables). Ran `perf record -F 99 --call-graph fp`
during 30 CREATE TABLE statements (took 17.7s total, ~590ms/DDL — consistent
with Session 3 measurements).

**Results — Full DDL time breakdown (`sequence_create_table` at ~3000 tables):**

| Component | % of DDL time | O(n)? | Description |
|-----------|--------------|-------|-------------|
| **Cow\<CatalogState\>::to_mut** | **16.1%** | **YES** | Clones ENTIRE CatalogState on every DDL |
| **drop(CatalogState clone)** | **15.1%** | **YES** | Drops the full clone after transaction |
| check_consistency_for_ids | 17.6% | partial | Incremental, but sub-checks still iterate all |
| allocate_user_id | 16.9% | YES | Allocates IDs via persist snapshot (see below) |
| with_snapshot (persist trace replay) | 11.5% | YES | Replays ALL trace entries into BTreeMaps |
| TableTransaction::verify | 11.4% | YES | Iterates all items checking uniqueness |
| TableTransaction::new | 6.7% | YES | Converts ALL snapshot entries proto→Rust |
| Persist actual writes | 7.7% | no | O(1) writes for the DDL operation |
| apply_updates | 5.0% | no | In-memory catalog state update |
| apply_catalog_implications | 3.9% | no | Compute/storage side effects |

**Inside `check_consistency_for_ids` (17.6% of total):**

| Sub-check | % of check_consistency | Still O(n)? |
|-----------|----------------------|-------------|
| check_object_dependencies_for_ids | 59.6% | Small n (only changed items + neighbors) |
| check_internal_fields | 14.2% | YES — iterates entry_by_id, entry_by_global_id |
| check_read_holds | 11.6% | Unknown |
| check_items_for_ids | 5.5% | No — only re-parses changed items |
| check_comments | 5.5% | YES — iterates all comments |
| check_roles | 2.8% | YES — iterates entry_by_id |

**Key finding #1: Cow\<CatalogState\> clone+drop = 31.2% of DDL time**

The `transact_inner` function (`src/adapter/src/catalog/transact.rs:576-579`)
creates two `Cow::Borrowed(state)` references:
- `preliminary_state` — tracks intermediate state between ops in the loop
- `state` — the final result

Each calls `.to_mut()` which clones the **entire CatalogState** including:
- `entry_by_id`: BTreeMap\<CatalogItemId, CatalogEntry\> — **13.8% alone** (cloning ~6000 entries)
- `entry_by_global_id`: BTreeMap\<GlobalId, CatalogItemId\> — 0.5%
- All other maps (databases, schemas, clusters, roles, etc.)

After the transaction, both clones are dropped (15.1% of DDL time). Together,
clone+drop accounts for **31.2%** of DDL time and scales O(n) with catalog size.

**Key finding #2: Durable catalog snapshot is rebuilt from scratch every DDL**

The persist-backed durable catalog uses `with_snapshot()` to replay the entire
consolidated trace into BTreeMaps for every transaction. This happens in two
separate paths per CREATE TABLE:
- `allocate_user_id` — allocates new CatalogItemId + GlobalId (14.1% via snapshot)
- `catalog_transact_inner` — the main transaction (11.5% via snapshot)

The `Snapshot` is then wrapped in `TableTransaction` objects:
- `TableTransaction::new` converts all proto entries to Rust types (6.7%)
- `TableTransaction::verify` checks uniqueness of all items (11.4%)

Together, the durable catalog path (with_snapshot + TableTransaction::new +
verify) across both call sites accounts for ~30% of DDL time.

**Summary of O(n) scaling sources:**

| Source | DDL % | Scaling |
|--------|-------|---------|
| Cow\<CatalogState\> clone | 16.1% | O(n) — clones all catalog entries |
| Cow\<CatalogState\> drop | 15.1% | O(n) — drops the clone |
| with_snapshot (2 calls) | 11.5% | O(n) — replays entire persist trace |
| TableTransaction::verify | 11.4% | O(n) — checks all items for uniqueness |
| check_consistency sub-checks | ~6% | O(n) — check_internal_fields, check_roles, etc. |
| TableTransaction::new | 6.7% | O(n) — converts all entries |
| **Total O(n)** | **~67%** | |

**Possible optimizations (ranked by impact):**

1. **Eliminate Cow\<CatalogState\> deep clone (31.2%)** — The biggest single win.
   Options:
   - Use `Arc` fields inside CatalogState so clone is O(1) (clone-on-write at
     field level rather than full struct clone)
   - Restructure `transact_inner` to mutate in place with rollback capability
   - Use persistent/immutable data structures (e.g., `im` crate) for the large
     BTreeMaps

2. **Cache or incrementally update the durable catalog snapshot (~30%)** — Instead
   of replaying the entire trace on every transaction, maintain a cached Snapshot
   and apply only the deltas from the latest transaction. This would make
   `with_snapshot`, `TableTransaction::new`, and `TableTransaction::verify` all
   O(delta) instead of O(n).

3. **Make remaining check_consistency sub-checks incremental (~6%)** — Same
   approach as Session 3 but for `check_internal_fields`, `check_roles`,
   `check_comments`, and `check_read_holds`.

**Next step:** Tackle optimization #1 (Cow clone elimination) as it's the
single largest O(n) source at 31.2% of DDL time. The most promising approach is
to use `Arc` or persistent data structures for the large maps inside
CatalogState so that `Cow::to_mut` becomes O(1) instead of O(n).
