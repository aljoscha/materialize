"""Build a milestone-by-milestone summary CSV from slim snapshots."""
import csv
import pathlib
import re
import sys
from collections import defaultdict

LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$"
)


def parse(path):
    data = defaultdict(lambda: {"buckets": {}, "count": 0.0, "sum": 0.0})
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = LINE_RE.match(line)
        if not m:
            continue
        name, lbls = m["name"], m["labels"] or ""
        try:
            v = float(m["value"])
        except ValueError:
            continue
        labels = dict(
            re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', lbls)
        )
        # We classify by metric base name + extra labels we care about
        for base, keep in [
            ("mz_slow_message_handling", ["message_kind"]),
            ("mz_append_table_duration_seconds", []),
            ("mz_coordinator_message_batch_size", []),
            ("mz_persist_external_op_latency", ["op"]),
        ]:
            kl = tuple((k, labels.get(k, "")) for k in keep)
            key = (base, kl)
            if name == f"{base}_bucket":
                le_s = labels.get("le", "0")
                le = float("inf") if le_s == "+Inf" else float(le_s)
                data[key]["buckets"][le] = v
            elif name == f"{base}_count":
                data[key]["count"] = v
            elif name == f"{base}_sum":
                data[key]["sum"] = v
    return data


def hq(buckets, q):
    bs = sorted(buckets.items())
    if not bs:
        return None
    total = bs[-1][1]
    if total <= 0:
        return None
    target = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in bs:
        if c >= target:
            if le == float("inf"):
                return prev_le
            d = c - prev_c
            return prev_le + ((target - prev_c) / d) * (le - prev_le) if d > 0 else le
        prev_le = 0.0 if le == float("inf") else le
        prev_c = c
    return prev_le


MILESTONES = [
    ("5k", 5000),
    ("10k", 10000),
    ("15k", 15000),
    ("20k", 20000),
    ("25k", 25000),
    ("30k", 30000),
    ("35k", 35000),
    ("40k", 40000),
    ("45k", 45000),
    ("post_seed", 47338),
    ("idle_t1", 47338),
]

base_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path("slim")
out = sys.argv[2] if len(sys.argv) > 2 else "milestone_summary.csv"

w = csv.writer(open(out, "w"))
w.writerow(
    [
        "milestone",
        "n_mvs",
        "metric",
        "label",
        "cumulative_count",
        "cumulative_sum_s",
        "mean_ms",
        "p50_ms",
        "p99_ms",
        "p100_ms",
    ]
)

for name, n in MILESTONES:
    path = base_dir / f"snap_{name}.slim.prom"
    if not path.exists():
        continue
    data = parse(path)
    for (metric, lk), v in data.items():
        if v["count"] is None or v["count"] < 10:
            continue
        label = " ".join(f"{k}={vv}" for k, vv in lk)
        p50 = hq(v["buckets"], 0.5)
        p99 = hq(v["buckets"], 0.99)
        p100 = hq(v["buckets"], 1.0)
        mean_ms = (v["sum"] / v["count"]) * 1000 if v["count"] else 0
        w.writerow(
            [
                name,
                n,
                metric,
                label,
                int(v["count"]),
                f"{v['sum']:.3f}",
                f"{mean_ms:.3f}",
                f"{p50*1000:.3f}" if p50 is not None else "",
                f"{p99*1000:.3f}" if p99 is not None else "",
                f"{p100*1000:.3f}" if p100 is not None else "",
            ]
        )

print(f"wrote {out}")
