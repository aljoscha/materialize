"""Analyze a directory of prometheus-format snapshots from envd /metrics.

Given a directory written by scrape_metrics.py, this script extracts
the two histograms we care about for Coordinator health investigation:

* mz_slow_message_handling_bucket (labeled by message_kind)
* mz_append_table_duration_seconds_bucket (unlabeled)

For each snapshot we compute p50/p90/p99/p100 from the histogram
buckets (linear interpolation across the bucket boundary), and emit
a CSV row per (timestamp, metric, label_combo). The output CSV is
straightforward to plot or pivot afterwards.

Also computes per-interval *rate* of the histogram _count series,
so you can see the raw throughput of coordinator messages and
table appends over time.

Run:
    python3 analyze_metrics.py --dir /tmp/mv_seed_metrics --out summary.csv
"""

import argparse
import csv
import os
import pathlib
import re
import sys
from collections import defaultdict
from typing import Iterable


METRICS_OF_INTEREST = {
    "mz_slow_message_handling": ["message_kind"],
    "mz_append_table_duration_seconds": [],
}


# Regex for parsing histogram bucket lines:
#   mz_slow_message_handling_bucket{message_kind="Command",le="0.000128"} 12
LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[0-9eE+\-.\.naN]+)$"
)


def parse_labels(s: str) -> dict[str, str]:
    """Parse Prometheus label string like `a="x",b="y"` (we lean on the
    fact that values are quoted and don't contain commas in our case)."""
    out = {}
    if not s:
        return out
    parts = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', s)
    for k, v in parts:
        out[k] = v
    return out


def label_key(labels: dict[str, str], keep: list[str]) -> tuple:
    return tuple((k, labels.get(k, "")) for k in keep)


def parse_snapshot(path: pathlib.Path) -> dict[tuple, dict]:
    """Return: { (metric_name, label_key): {"buckets": [(le, cum_count)], "count": N, "sum": S} }"""
    out: dict[tuple, dict] = defaultdict(
        lambda: {"buckets": [], "count": None, "sum": None}
    )
    for raw in path.read_bytes().splitlines():
        line = raw.decode("utf-8", errors="replace").strip()
        if not line or line.startswith("#"):
            continue
        m = LINE_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        labels = parse_labels(m.group("labels") or "")
        try:
            value = float(m.group("value"))
        except ValueError:
            continue

        for metric, label_keys in METRICS_OF_INTEREST.items():
            if name == f"{metric}_bucket":
                key = (metric, label_key(labels, label_keys))
                le_str = labels.get("le", "")
                try:
                    le = float("inf") if le_str == "+Inf" else float(le_str)
                except ValueError:
                    continue
                out[key]["buckets"].append((le, value))
            elif name == f"{metric}_count":
                key = (metric, label_key(labels, label_keys))
                out[key]["count"] = value
            elif name == f"{metric}_sum":
                key = (metric, label_key(labels, label_keys))
                out[key]["sum"] = value

    # Sort buckets ascending by `le` for each entry
    for v in out.values():
        v["buckets"].sort()
    return out


def histogram_quantile(buckets: list[tuple[float, float]], q: float) -> float | None:
    """Linear-interpolation prometheus-style histogram_quantile.

    `buckets` is sorted ascending by `le` with cumulative counts.
    Returns None if no observations yet, else a finite estimate
    (clamping at the highest finite bucket boundary if q == 1.0).
    """
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    target = q * total
    prev_le = 0.0
    prev_count = 0.0
    for le, count in buckets:
        if count >= target:
            if le == float("inf"):
                # All over-the-line; clamp to the previous boundary.
                return prev_le
            # Linear interpolation inside the bucket from prev_le to le
            denom = count - prev_count
            if denom <= 0:
                return le
            frac = (target - prev_count) / denom
            return prev_le + frac * (le - prev_le)
        prev_le = 0.0 if le == float("inf") else le
        prev_count = count
    return prev_le


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", required=True, help="directory of *.prom snapshots")
    ap.add_argument("--out", required=True, help="output CSV path")
    args = ap.parse_args()

    d = pathlib.Path(args.dir)
    snap_paths = sorted(d.glob("metrics_*.prom"))
    if not snap_paths:
        print(f"no snapshots in {d}", file=sys.stderr)
        return 1

    # Parse all snapshots
    rows = []
    prev_per_key: dict[tuple, tuple] = {}  # (count, ts)
    for path in snap_paths:
        m = re.match(r"metrics_(\d+)\.prom$", path.name)
        if not m:
            continue
        ts = int(m.group(1))
        snap = parse_snapshot(path)

        for key, hist in snap.items():
            metric, lk = key
            count = hist["count"]
            total_sum = hist["sum"]
            buckets = hist["buckets"]

            p50 = histogram_quantile(buckets, 0.50)
            p90 = histogram_quantile(buckets, 0.90)
            p99 = histogram_quantile(buckets, 0.99)
            p100 = histogram_quantile(buckets, 1.0)

            rate_per_s = ""
            if key in prev_per_key and count is not None:
                prev_count, prev_ts = prev_per_key[key]
                dt = ts - prev_ts
                if dt > 0 and prev_count is not None:
                    rate_per_s = f"{(count - prev_count) / dt:.3f}"

            rows.append(
                {
                    "ts": ts,
                    "metric": metric,
                    "labels": " ".join(f"{k}={v}" for k, v in lk),
                    "count": "" if count is None else int(count),
                    "sum": "" if total_sum is None else f"{total_sum:.6f}",
                    "rate_per_s": rate_per_s,
                    "p50_s": "" if p50 is None else f"{p50:.6g}",
                    "p90_s": "" if p90 is None else f"{p90:.6g}",
                    "p99_s": "" if p99 is None else f"{p99:.6g}",
                    "p100_s": "" if p100 is None else f"{p100:.6g}",
                }
            )

            if count is not None:
                prev_per_key[key] = (count, ts)

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "ts",
                "metric",
                "labels",
                "count",
                "sum",
                "rate_per_s",
                "p50_s",
                "p90_s",
                "p99_s",
                "p100_s",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"wrote {len(rows)} rows to {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
