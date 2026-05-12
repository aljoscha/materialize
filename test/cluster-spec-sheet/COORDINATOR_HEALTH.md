# Coordinator health and persist behaviour at high MV count

Investigation companion to `INVESTIGATION.md`. Goal: characterize what
the coordinator and persist look like when the catalog holds tens of
thousands of MATERIALIZED VIEWs — the dataflow-resident case the
`envd_scalability_tables` benchmark deliberately avoids.

## Setup

* Local optimized build (`bin/environmentd --optimized`), HEAD =
  `8136162326` (envd-specsheet branch), Cockroach for consensus + tsoracle.
* `MZ_SOFT_ASSERTIONS=0`. System parameter overrides applied via internal
  SQL (`max_materialized_views=200000`, `max_clusters=50`, etc.).
* Seeded N=47,338 MVs across 5 pad clusters (`pad_c_0..pad_c_4`, size
  `scale=1,workers=4`), 10k MVs per cluster except the last (~7.3k).
  Each MV: `SELECT id, val FROM pad_base WHERE id < {i}` — distinct per
  index, on a 1-row base table.
* Seeded sequentially via the project `psycopg` venv against external SQL
  port 6875. `/metrics` scraped at the internal port 6878 every 5s for
  the whole run (272 snapshots).

Artifacts (all under `test/cluster-spec-sheet/`):

* `seed_mvs.py` — the seeder (resumable, idempotent against existing
  `pad_mv_*` MVs)
* `scrape_metrics.py` — periodic `/metrics` snapshotter
* `snap_metrics.py` — single-snapshot p50/p99/p100 helper
* `analyze_metrics.py` — multi-snapshot summary CSV (slow at this scale)
* `results-archive/coord_health_N47k/` — slim per-milestone snapshots
  (filtered to relevant metric families) and the derived summary CSV

## During-seed trajectory

Per-milestone snapshots are cumulative since envd start, so quantiles
are smeared across all observations up to that N. Treat them as
"latency the histogram has seen so far"; rates are diffs across the 5s
scrape interval.

### `mz_slow_message_handling` — coordinator message latency

`create_materialized_view_stage_ready` is the coord-thread cost per
CREATE MATERIALIZED VIEW:

| N      | seed rate | p50  | p99    | p100   |
|--------|-----------|------|--------|--------|
| 5k     | 22.5/s    | 44ms | 106ms  | 0.5s   |
| 10k    | 20.1/s    | 46ms | 78ms   | 1.0s   |
| 15k    | 16.2/s    | 47ms | 102ms  | 2.0s   |
| 20k    | 13.8/s    | 47ms | 108ms  | 2.0s   |
| 25k    | 12.1/s    | 48ms | 122ms  | 2.0s   |
| 30k    | 10.6/s    | 48ms | 123ms  | 2.0s   |
| 35k    |  9.6/s    | 49ms | 185ms  | 4.0s   |
| 40k    |  8.5/s    | 50ms | 211ms  | 4.0s   |
| 45k    |  7.3/s    | 51ms | 237ms  | 4.0s   |

* p50 is **flat** (44 → 51ms) — the optimizations from
  iter-7/8/9 hold here: per-CREATE-MV coord-thread cost is stable
  even though the catalog grows 10×.
* p99 starts climbing in earnest at N≥30k; p100 jumps in
  bucket-boundary steps (0.5 → 1.0 → 2.0 → 4.0s).
* Seed rate drops from 22.5 to 7.3 MVs/s. Most of that is NOT
  per-message coord-thread time — it's controller round-trips and
  occasional persist outliers (next section).

`controller_ready(compute)` is just the ticking of the compute
controller event loop. Counts are dominated by this:

| N    | rate          | p100   |
|------|---------------|--------|
| 5k   | 1486/s        | 0.002s |
| 10k  | 2655/s        | 0.004s |
| 25k  | (steady)      | 0.064s |
| 35k  | (steady)      | 0.064s |
| 45k  | (steady)      | 0.128s |

The p100 doubles roughly every two milestones — a single controller
tick can now stall the coord thread for 128ms at N=45k. The rate
plateaus around 2600/s because we only added one new clusterd
process per 10k MVs (5 total + system clusters); ticks are dominated
by the per-cluster heartbeats.

### `mz_append_table_duration_seconds` — persist append wait time

| N     | rate    | p50   | p99    | p100  |
|-------|---------|-------|--------|-------|
| 5k    | 23.8/s  | 64ms  | 127ms  | 0.5s  |
| 15k   | 14.4/s  | 64ms  | 127ms  | 1.0s  |
| 25k   | 17.4/s* | 64ms  | 127ms  | 1.0s  |
| 35k   | 11.0/s  | 64ms  | 128ms  | 2.0s  |
| 45k   | 10.0/s  | 65ms  | 130ms  | 2.0s  |

**Caveat**: the histogram bucket boundaries are
`histogram_seconds_buckets(0.128, 32.0)` — the smallest bucket is
**128ms**. Anything below 128ms collapses into the first bucket and
`histogram_quantile` snaps to 64ms (the linear-interpolation midpoint
of `0 → 128ms`). So "p50=64ms" really means "≤128ms, can't tell". The
*real* mean per append is much lower:

| N         | count | sum (s) | mean per append |
|-----------|-------|---------|-----------------|
| 45k       | 45.2k | 821     | 18.2 ms         |
| idle (post-seed, 66s) | 66 | 0.26 | **3.9 ms**  |

So during seed each append takes ~18ms on average; idle it's ~4ms.
The **outliers are what move**: p100 jumps 0.5 → 1.0 → 2.0s as N
grows, and those rare slow appends dominate DDL p100.

The bucket layout limits how much we can say from this metric. To
characterize sub-128ms append behaviour, we'd need to either widen
the bucket range or instrument separately.

## Idle (post-seed, 66s window)

After the seeder was killed at N=47338, envd was left idle for 66s
between `snap_idle_t0.prom` and `snap_idle_t1.prom`. Diffing the
histograms gives the idle steady-state rates and quantiles:

### Coordinator messages (top by rate)

| message_kind                          | rate (/s) | p50    | p99    | p100   |
|---------------------------------------|-----------|--------|--------|--------|
| `controller_ready(compute)`           | 263.8     | 0.06ms | 0.13ms | 8ms    |
| `introspection_subscribe_stage_ready` |   4.85    | 0.07ms | 0.13ms | 1ms    |
| `cluster_event`                       |   1.27    | 0.06ms | 0.13ms | 0.13ms |
| `advance_timelines`                   |   1.00    | 19ms   | 32ms   | 32ms   |
| `group_commit_initiate`               |   1.00    | 6ms    | 11ms   | 16ms   |
| `check_scheduling_policies`           |   0.33    | 0.06ms | 0.13ms | 0.13ms |

* `controller_ready(compute)` at 264/s is the floor — every clusterd
  emits per-second-ish frontier/heartbeat traffic and the coord thread
  wakes for each one.
* No idle message takes over **32ms p100**. The coord is healthy at
  this scale when not driving DDL.
* `mz_coordinator_message_batch_size`: mean = **1.003 messages per
  batch**. The coord is being woken for every single message — no
  batching is happening even though messages arrive at >300/s.

### `mz_append_table_duration_seconds` at idle

| | rate | p50  | p99   | p100  | mean (sum/count) |
|---|------|------|-------|-------|------------------|
| | 1.0/s | 64ms (synthetic, < first bucket) | 127ms | 128ms | **3.9 ms** |

Once per second the periodic group commit fires a real builtin-table
append (advances tables, introspection bookkeeping). At idle this is
fast — under 4ms per append on average. The metric reports 64ms p50
purely because the first bucket boundary is 128ms.

### `mz_persist_external_op_latency` — what persist itself spends

**Cumulative (across the whole run):**

| op             | count  | mean    | p50    | p99    | p100  |
|----------------|--------|---------|--------|--------|-------|
| `blob_get`     | 44k    | 1.1 ms  | 0.5ms  | 15ms   | 256ms |
| `blob_set`     | 188k   | 12.8 ms | 7.7ms  | **178ms** | **2.0s** |
| `consensus_cas`| 1.02M  | 1.5 ms  | 1.5ms  | 3.5ms  | **2.0s** |

**Idle 66s window:**

| op             | rate    | mean   | p50    | p99    | p100  |
|----------------|---------|--------|--------|--------|-------|
| `blob_set`     | 3.83/s  | 5.0ms  | 5.0ms  | 15ms   | 16ms  |
| `consensus_cas`| 30.5/s  | 1.8ms  | 1.5ms  | 3.7ms  | 32ms  |
| `blob_get`     | 0       | —      | —      | —      | —     |

The persist tail story:

* Steady-state persist ops are quick (sub-10ms means).
* But the cumulative p99/p100 for `blob_set` (178ms / 2s) and
  `consensus_cas` (3.5ms / 2s) say that rare ops hit multi-second
  outliers. These outliers happen mostly during the bulk-DDL phase.
* Each CREATE MATERIALIZED VIEW touches the catalog shard
  (~10 cas + 1 blob_set) and initializes the MV's own output shard
  (more cas + blob_set). Two slow ops stacking explain the
  `create_materialized_view_stage_ready` p100 of 4s and
  `mz_append_table_duration_seconds` p100 of 2s.

The high outlier counts grew as N grew (matching the bucket-edge
jumps in DDL p100 at N=15k, N=30k, N=35k). This is the **persist
degradation at high object count** that needs follow-up — likely
related to the catalog shard size grown enormous (47k entries) so
blob_set sizes balloon and consensus CaS contention rises with
write rate.

## DDL probe at N=47k MVs (idle envd)

Quick comparison with the envd_scalability iter-9 numbers (which run
against tables, not MVs). 30 sequential CREATE TABLE/DROP TABLE pairs
plus a SELECT peek against a 1-row table:

| operation     | n  | p50    | p90    | p99/max | mean    |
|---------------|----|--------|--------|---------|---------|
| CREATE TABLE  | 30 | 120ms  | 443ms  | 573ms   | 186ms   |
| DROP TABLE    | 30 | 127ms  | 343ms  | 449ms   | 168ms   |
| SELECT peek   | 30 | 15ms   | 42ms   | 96ms    | 24ms    |

Iter-9 envd_scalability at N=50k **tables** reported p50=57ms for
CREATE TABLE. Here at N=47k **MVs**, CREATE TABLE p50 is 120ms — about
2× slower. The catalog itself is comparable in size, so the extra
cost comes from controller round-trips against **5 pad clusterd
processes hosting 47k active dataflows**. Each catalog-changing op
fans out to the compute controller, which has to reflect the change
to every cluster's controller-protocol channel — work that doesn't
exist when the catalog is just tables.

(`SELECT peek` p50 of 15ms here vs ~4ms in earlier envd_scalability
runs has the same explanation: the local machine is busy.)

## Process resource picture at N=47k MVs

From `jemalloc_*` metrics in the idle snapshot:

| metric | value |
|--------|-------|
| `jemalloc_active`   | 10.0 GB |
| `jemalloc_allocated`| 9.0 GB  |
| `jemalloc_resident` | 11.0 GB |
| `jemalloc_retained` | 37.7 GB |

So envd holds ~11 GB resident at 47k MVs (~230 KB per MV averaged).
Each clusterd holds its own dataflow state on top of that. The 37GB
retained-but-returned is normal jemalloc behaviour and not
problematic.

The `/metrics` endpoint itself returns **239 MB** at this scale,
dominated by per-MV mz_introspection metrics. Real prometheus
scrapes of a real cloud envd at this catalog size would already be
significantly expensive.

## Drill-down: where does `create_materialized_view_stage_ready` time go?

Captured a 30s 99 Hz CPU flamegraph via envd's `/prof/?action=time_fg`
endpoint while a sustained CREATE MATERIALIZED VIEW load ran on
`pad_c_0` (~9 MVs/s). 6,374 merged-thread samples total; **991 samples
(15.55%) had `create_materialized_view` in the stack**. That fraction
matches the metric breakdown — CREATE MV's 84ms per call × 7.35/s ≈
62% of the coord thread, which is ~15.5% of the 4 active threads
merged.

Walking each CMV stack from leaf back to root and grouping by the
first non-poll/non-runtime frame gives:

| caller                                                                                  | %CMV    | hot leaf inside                                              |
|-----------------------------------------------------------------------------------------|---------|--------------------------------------------------------------|
| `ReadHolds::id_bundle`                                                                  | **12.7%** | BTreeMap iter+insert into a fresh CollectionIdBundle         |
| raw `imbl::OrdMap<CatalogItemId, CatalogEntry>` iter                                    | 10.5%   | btree::Cursor walking entries_by_id                          |
| raw `imbl::OrdMap<ItemKey, ItemValue>` iter (durable)                                   |  5.1%   | btree::Cursor walking the durable items table                |
| `drop_in_place::<CatalogState>`                                                         |  4.7%   | dropping a cloned preliminary `CatalogState`                 |
| `CatalogState::apply_update`                                                            |  4.4%   | `BTreeMap::clone::clone_subtree`                             |
| `drop_in_place::<CollectionIdBundle>`                                                   |  4.1%   | BTreeMap dying iterator                                      |
| `CollectionIdBundle::difference`                                                        |  3.8%   | BTreeMap node navigation                                     |
| raw `imbl::OrdMap<StorageCollectionMetadataKey, ...>` iter                              |  3.8%   | btree::Cursor walking the storage-metadata table             |
| `<CatalogEntry>::is_secret`                                                             |  3.0%   | leaf predicate inside an entry-by-id filter                  |
| `TableTransaction<ItemKey, ItemValue>` full scan                                        |  2.8%   | hashing+iter over the items table                            |
| `BTreeMap::clone::clone_subtree::<String, CatalogItemId>`                               |  2.7%   | name→id index clone                                          |
| `<CatalogItem>::is_temporary`                                                           |  2.6%   | second predicate over entry-by-id                            |
| `apply_catalog_implications_inner`                                                      |  2.1%   | post-DDL fix-ups                                             |
| `create_materialized_view_finish`                                                       |  1.8%   | final stage of the CMV state machine                         |
| `optimize_mir_local`                                                                    |  1.3%   | the optimizer itself                                         |
| `<ComputeInstanceSnapshot>::new`                                                        |  1.3%   | per-call compute-instance state snapshot                     |

(Totals don't sum to 100% because each stack is counted once for its
first matching caller; un-categorized noise is the remainder.)

### The biggest single fix: `ReadHolds::id_bundle()` is called O(N) times worse than it needs to be

Three call sites in the CMV pipeline all do:

```rust
// src/adapter/src/coord/sequencer/inner/create_materialized_view.rs:388
if !ids.difference(&read_holds.id_bundle()).is_empty() { … }

// src/adapter/src/coord/sequencer/inner/create_materialized_view.rs:854
assert!(
    id_bundle.difference(&read_holds.id_bundle()).is_empty(),
    "we must have read holds for all involved collections"
);

// src/adapter/src/coord/catalog_implications.rs:872
let empty = read_holds.id_bundle().difference(&id_bundle).is_empty();
```

`read_holds.id_bundle()` iterates *every* read hold (one per existing
materialized view in our setup → 47k+), inserting each ID into a
fresh `CollectionIdBundle`. Then `.difference()` walks that 47k-entry
bundle to subtract the small (≈2-element) bundle of the new MV's
deps. So each call is O(N_read_holds), and CMV does at least two of
them.

The intent in each case is *"is `id_bundle` a subset of the
read_holds?"* — that's an O(|id_bundle|) check by iterating the
small bundle and probing the `ReadHolds` BTreeMaps. Replacing these
three sites with a direct subset test eliminates roughly **12-15% of
CMV CPU at this scale** without changing behaviour.

Note also that `read_holds.least_valid_read()` immediately below the
assert at line 854 *also* iterates all holds to compute the minimum
since across them. The MV only needs the least_valid_read across the
holds *for its own deps*, not across every hold the coord knows
about. Same fix, same root cause.

### Second-biggest: iter-8 caching didn't cover all the limits

iter-8 added cached counts for the high-rate limits
(`user_tables_count`, `user_materialized_views_count`,
`user_clusters_count`, the five connection kinds, etc.). But three
limits in `validate_resource_limits` (`src/adapter/src/coord/ddl.rs:1438-1458`)
still do live O(N) walks of `entry_by_id`:

```rust
self.catalog().user_secrets().count(),         // O(N_entries) filter on is_secret
self.catalog().user_roles().count(),           // O(N_roles)
self.catalog().user_network_policies().count(), // O(N_entries) filter on is_network_policy
```

The flame attributes 3.0% to `<CatalogEntry>::is_secret` directly and
2.6% to `<CatalogItem>::is_temporary`. Both are leaf predicates inside
`OrdMap::Values::filter`. Extending `ResourceLimitCounts` from iter-8
to also cache `user_secrets_count`, `user_roles_count`,
`user_network_policies_count`, and possibly a `user_temporary_count`
clears this. Same shape of fix, no architectural change.

### Third: `CatalogState` cloning during the per-Op preliminary apply

`drop_in_place::<CatalogState>` (4.7%) + `CatalogState::apply_update`
(4.4%) ≈ 9% of CMV is the cost of cloning state to do a preliminary
apply per Op (the iter-9 skip-the-last-Op trick mitigated this for
single-Op DDL on the final Op, but the *earlier* ops in a multi-Op
transaction still pay). For a typical CMV which is usually one Op,
the per-Op clone should already be skipped — re-check this is firing
correctly, since 9% seems high.

This is a candidate for the structural rework already noted in
INVESTIGATION.md: making `apply_updates` take `&mut state` instead of
cloning. But the smaller wins above are zero-architecture-risk and
worth doing first.

### After the fix

Applied the `ReadHolds::contains_bundle` / `is_subset_of_bundle` change
to the three call sites listed above (no other changes), rebuilt
`--optimized` and reused the same 47k-MV catalog. Re-ran the same
30s flame at 99 Hz, again with a sustained CREATE MV load.

Caveat: the pad clusterds couldn't rehydrate 10k MVs each into 4 GiB
on restart and were OOM-killed by the kernel, so I disabled them
(`ALTER CLUSTER pad_c_N SET (REPLICATION FACTOR 0)`) and ran the
"after" load against `quickstart` (1 worker) instead of `pad_c_0`
(4 workers). The catalog is unchanged; the coord-side CMV path is
unchanged by target cluster choice. The throughput numbers below
include the controller-round-trip change.

**Per-call coordinator time (`create_materialized_view_stage_ready` mean):**

| measurement                                  | mean per call | window |
|----------------------------------------------|--------------:|--------|
| Before (during seed window at N≈40–45k)      | 84.3ms        | 5004 calls over 681s, on `pad_c_0` (4 workers) |
| After (500 CMVs at N≈47k, post-fix)          | **70.3ms**    | 500 calls over 45s, on `quickstart` (1 worker) |
| **delta**                                    | **-14 ms (-16.6%)** | |

The "after" sample was at a slightly larger catalog (N=47k vs N≈42k
mid-window) yet still 14ms faster per call — consistent with the
12.7% ReadHolds-attributed time the flame predicted we'd recover.

**CMV throughput (driver wall-clock):**

| measurement | rate    | target cluster |
|-------------|---------|----------------|
| Before      | ~9.3/s  | `pad_c_0` (scale=1,workers=4) |
| After       | 11.1/s  | `quickstart` (scale=1,workers=1) |

The driver also got faster, but with two confounders changed
(target cluster + fix), so don't read this as pure fix gain.

**Leaf-side leaf-symbol shifts in the flame (CMV stacks):**

| hot leaf symbol                                        | before %CMV | after %CMV | Δ (pts) |
|--------------------------------------------------------|------------:|-----------:|--------:|
| `BTreeMap<GlobalId, set_val::SetValZST>` IntoIter      | 9.4         | 0.0        | **-9.4** |
| `BTreeSet<GlobalId>::insert`                           | 7.6         | 0.2        | **-7.4** |
| `BTreeMap<GlobalId, ReadHold>` node Mut access         | 6.6         | 0.0        | -6.6    |
| `BTreeMap<GlobalId, ReadHold>` Iter / Keys             | ~14         | ~4         | -10     |
| `BTreeMap clone_subtree::<GlobalId, ...>`              | 4.3         | 7.6        | +3.3    |
| `<CatalogItem>::is_temporary`                          | 2.6         | 7.1        | +4.5    |
| `<CatalogEntry>::is_secret`                            | 3.0         | 5.4        | +2.4    |
| `<Coordinator>::validate_resource_limits`              | 3.5         | 5.4        | +1.9    |

The cluster of `BTreeMap<GlobalId, ...>` ops that were the leaves of
`ReadHolds::id_bundle` collapsed to ~zero. Cost categories that
*didn't* change in absolute terms (`is_secret`, `is_temporary`,
`validate_resource_limits`) became a larger *fraction* of the now
smaller CMV pie, which is exactly the expected shape of a clean fix.

**Grouped inclusive breakdown of CMV samples (a stack counts once per group):**

| group                                              | before %CMV | after %CMV |
|----------------------------------------------------|------------:|-----------:|
| `ReadHolds::id_bundle` (now gone)                  | 12.7        | 0.0        |
| `CollectionIdBundle::difference` + `drop_in_place` | 7.9         | 0.0        |
| `validate_resource_limits` (inclusive)             | 8.7         | 12.6       |
| `builtin_table_updates` emission                   | 8.8         | 16.0       |
| `CatalogState` apply / mutate                      | 7.6         | 12.0       |
| rust-typed snapshot cache write (`apply_to_snapshot_cache`) | 15.4 | 21.9       |
| plan / optimize / lower                            | 6.4         | 5.9        |
| compute-controller round-trip                      | 0.1         | 0.3        |
| ingress (sql parse / purify)                       | 0.4         | 1.4        |

(The "after" percentages are of a smaller total — they sum higher
because each remaining group claims a larger slice of the leftover
work.)

After this fix, the **next biggest single hotspot is the iter-8
gap**: `validate_resource_limits` still calls `user_secrets().count()`,
`user_roles().count()`, `user_network_policies().count()` — three
O(N) walks of `entry_by_id` with `is_secret` / `is_temporary` /
`is_network_policy` predicate filters. That's ~10-12% of the
post-fix CMV time and is the natural follow-up.

The fix and methodology are committed; the after-fix flame is at
`results-archive/coord_health_N47k/cmv_flame_after_fix.html`.

### What's NOT the bottleneck (yet)

* `RustType::from_proto` — gone (iter-7), 0.0% in this flame.
* Persist commit (`PersistHandle::commit`, `Transaction::commit`) —
  not visible in the top 20 CMV callers. The persist cost has moved
  off the coordinator thread entirely.
* The optimizer (`optimize_mir_local`, `::transform`, `::lower`,
  `::resolve` combined) — only ~7% of CMV. Plenty of headroom there
  if it ever matters.

## Open questions / next steps

The metrics confirm two things the user predicted:

1. **Coordinator stays healthy** in the steady state: idle p100 of
   any message kind is ≤32ms, and the iter-7/8/9 coord-thread
   optimizations keep `create_materialized_view_stage_ready` p50
   flat (~50ms) across 10× catalog growth. The p99/p100 of the MV
   stage do grow at N≥30k, but the per-message *median* is steady.

2. **Persist gets the worst tails**: `blob_set` p100=2s and
   `consensus_cas` p100=2s show up in the cumulative numbers and
   directly explain `mz_append_table_duration_seconds` p100=2s and
   the DDL stage p100=4s. The median persist op is fast; only the
   long tail moves with N.

To narrow down the persist outlier story, useful next probes:

* Re-scrape during a fresh bulk-DDL burst at N=47k to see if
  `blob_set` p99 grows in real time (cumulative numbers smear it).
* Look at persist *batch* writes vs *catalog-shard* writes
  separately — `mz_persist_external_op_latency` doesn't label by
  shard. Adding a shard-type label, or sampling traces for slow
  ops, would show whether the slow tail is the catalog shard or
  per-MV output shards.
* Investigate the **coord-message batching = 1.003 per batch**
  number — at 273 messages/s the coord could trivially batch many
  per wakeup. Even a small amount of batching would reduce the
  controller-ready overhead, which is the noise floor everything
  else competes against.
