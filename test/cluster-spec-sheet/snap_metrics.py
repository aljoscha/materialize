"""Fast single-snapshot summary of a /metrics prom file.

Prints p50/p99/p100 (ms) and count for mz_slow_message_handling and
mz_append_table_duration_seconds.

    python3 snap_metrics.py /tmp/mv_seed_metrics/snap_20k.prom
"""

import re
import sys
from collections import defaultdict


LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$"
)

METRICS = [
    ("mz_slow_message_handling", ["message_kind"]),
    ("mz_append_table_duration_seconds", []),
]


def parse(path: str):
    data = defaultdict(lambda: {"buckets": [], "count": None, "sum": None})
    with open(path) as f:
        for line in f:
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
            labels = dict(re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', lbls))
            for metric, keep in METRICS:
                kl = tuple((k, labels.get(k, "")) for k in keep)
                key = (metric, kl)
                if name == f"{metric}_bucket":
                    le_s = labels.get("le", "0")
                    le = float("inf") if le_s == "+Inf" else float(le_s)
                    data[key]["buckets"].append((le, v))
                elif name == f"{metric}_count":
                    data[key]["count"] = v
                elif name == f"{metric}_sum":
                    data[key]["sum"] = v
    return data


def hq(buckets, q):
    if not buckets:
        return None
    bs = sorted(buckets)
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


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    data = parse(sys.argv[1])
    rows = []
    for (m, l), v in data.items():
        if v["count"] is None or v["count"] < 100:
            continue
        kind = dict(l).get("message_kind", "")
        rows.append(
            (
                m,
                kind,
                int(v["count"]),
                hq(v["buckets"], 0.5),
                hq(v["buckets"], 0.99),
                hq(v["buckets"], 1.0),
                v["sum"] or 0.0,
            )
        )
    rows.sort(key=lambda x: (x[0], -x[2]))
    print(f"{'metric':<36}{'kind':<37}{'cnt':>9}{'p50_ms':>10}{'p99_ms':>10}{'p100_ms':>10}{'sum_s':>10}")
    for m, k, c, p50, p99, p100, s in rows:
        if "controller_ready" in k and "compute" not in k:
            continue
        fmt = lambda x: f"{x*1000:.2f}" if x is not None else ""
        print(f"{m[:35]:<36}{k[:36]:<37}{c:>9}{fmt(p50):>10}{fmt(p99):>10}{fmt(p100):>10}{f'{s:.1f}':>10}")


if __name__ == "__main__":
    main()
