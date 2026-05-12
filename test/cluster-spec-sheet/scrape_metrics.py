"""Periodic snapshotter for envd's /metrics endpoint.

Writes one prometheus-formatted snapshot per interval to an output
directory, named `metrics_<unix_ts>.prom`. Designed so you can later
compute rates / quantiles over time by diffing two snapshots.

Run alongside the seeding/load process:

    python3 test/cluster-spec-sheet/scrape_metrics.py \
        --out /tmp/mv_seed_metrics --interval 5
"""

import argparse
import os
import pathlib
import sys
import time
import urllib.request


def fetch(url: str, timeout: float) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://localhost:6878/metrics")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between scrapes")
    ap.add_argument("--once", action="store_true", help="single snapshot then exit")
    args = ap.parse_args()

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    print(f"scraping {args.url} every {args.interval}s → {out}", flush=True)

    while True:
        ts = int(time.time())
        try:
            body = fetch(args.url, timeout=10.0)
            path = out / f"metrics_{ts}.prom"
            path.write_bytes(body)
            size_k = len(body) // 1024
            print(f"  {ts}: wrote {path.name} ({size_k} KiB)", flush=True)
        except Exception as e:
            print(f"  {ts}: scrape failed: {e}", flush=True)
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
