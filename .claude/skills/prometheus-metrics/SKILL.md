---
name: prometheus-metrics
description: >
  This skill should be used when working on performance analysis, benchmarking,
  latency analysis, throughput analysis, or when the user mentions Prometheus
  metrics, histograms, QPS, percentiles, p50, p90, p99, or the /metrics
  endpoint. Trigger when the user asks about metric values, wants to measure
  performance, or needs to compare benchmark runs.
---

# Prometheus Metrics Skill

Materialize exposes Prometheus metrics at `http://localhost:6878/metrics`. Use
this skill to fetch, parse, and reason about these metrics.

## Fetching Metrics

Fetching metrics requires network access, so use `dangerouslyDisableSandbox`:

```bash
# Fetch all metrics
curl -s http://localhost:6878/metrics

# Filter to specific metrics
curl -s http://localhost:6878/metrics | grep "metric_name"
```

Always filter output — the full metrics page is large.

## Metric Types

### Counter (`_total` suffix)

Monotonically increasing. To get a rate, take two snapshots and compute:

```
rate = (count_after - count_before) / elapsed_seconds
```

### Gauge

Instantaneous value. Read directly — no delta needed.

### Histogram (`_bucket`, `_sum`, `_count` suffixes)

Histograms have three components:

- `_bucket{le="X"}` — cumulative count of observations <= X. Each bucket
  includes all observations from lower buckets.
- `_sum` — total sum of all observed values.
- `_count` — total number of observations (same as `_bucket{le="+Inf"}`).

**Average latency** from a histogram:

```
avg = delta(_sum) / delta(_count)
```

**QPS/throughput:**

```
qps = delta(_count) / elapsed_seconds
```

## Computing Percentiles from Histogram Buckets

Histogram buckets are cumulative. To estimate a percentile (e.g., p99):

1. Parse all `_bucket` lines for the metric. Extract `le` (upper bound) and
   count for each bucket.
2. Compute `target = percentile * total_count` (where total_count is the
   `+Inf` bucket).
3. Find the first bucket where `count >= target`. Call this bucket's upper
   bound `upper` and its count `count_upper`. The previous bucket has bound
   `lower` and count `count_lower`.
4. Linear interpolate:

```
estimated_value = lower + (upper - lower) * (target - count_lower) / (count_upper - count_lower)
```

For the lowest bucket (no previous bucket), use `lower = 0`.

### Example

Given these buckets (after computing deltas if doing before/after):

```
le="0.001" count=100
le="0.002" count=450
le="0.004" count=900
le="0.008" count=950
le="+Inf"  count=1000
```

For p50 (target = 500):
- Bucket le=0.002 has count 450 < 500, bucket le=0.004 has count 900 >= 500
- p50 = 0.002 + (0.004 - 0.002) * (500 - 450) / (900 - 450) = 0.00222

For p99 (target = 990):
- Bucket le=0.008 has count 950 < 990, bucket le=+Inf has count 1000 >= 990
- Since +Inf is not a real bound, report "p99 > 0.008s" or use _sum/_count for
  the average in that tail bucket.

## Snapshot-Based Benchmarking Workflow

When measuring performance across a benchmark run:

1. **Before benchmark:** Save a snapshot of the relevant metrics:
   ```bash
   curl -s http://localhost:6878/metrics | grep "metric_name" > /tmp/claude/metrics_before.txt
   ```

2. **Run the benchmark.**

3. **After benchmark:** Save another snapshot:
   ```bash
   curl -s http://localhost:6878/metrics | grep "metric_name" > /tmp/claude/metrics_after.txt
   ```

4. **Compute deltas** for `_count`, `_sum`, and each `_bucket` value between
   the two snapshots.

5. **Report:** QPS, average latency, p50, p90, p99, p99.9.

Always report the same set of metrics across runs for consistent comparison.

## Tips

- The `+Inf` bucket count equals `_count` — use either.
- Histograms with labels (e.g., `method="try_frontend_peek_cached"`) need label
  matching when filtering.
- When grepping for a histogram, use the base name to get all three components:
  `grep "mz_frontend_peek_seconds"` returns buckets, sum, and count.
- For quick sanity checks, `_sum / _count` gives the all-time average. For
  per-run averages, always use deltas.
