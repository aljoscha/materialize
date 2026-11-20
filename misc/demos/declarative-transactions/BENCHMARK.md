# DDL transaction benchmark

On one r5a.4xlarge, the prototype committed 0.24–0.41 rename-only transactions
per second, versus 30–39 for ordinary transactions. Both committed all 120
rename-only attempts. With one client, adding a single insert cost little on
the declarative path. With four clients, 58 of 96 mixed attempts failed with
SQLSTATE 40001. Each client owned a separate schema and table.

## Setup

Both revisions used Rust 1.98.1 and `bin/environmentd --optimized --build-only`,
with debug info off. Runs were sequential with fresh environments, one replica
worker, ordinary catalog storage for upstream and the shared demo WAL for the
prototype. Each client ran one warmup and 12 measured attempts in each cell.
Renames form a chain on one schema containing one table. Writes are single-row
inserts into that table under its final name. The generator checks the final
table name and row count. Throughput counts only commits, over the full
measurement window including failures. Percentiles cover successful transactions
from BEGIN through COMMIT. The client does not retry a rejected transaction.

## Results

| Revision / mode | Renames | Inserts | Clients | Commits / attempts | Commits/s | p50 ms | p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| 5dd39771b6 / ordinary | 1 | 0 | 1 | 12/12 | 36.434 | 26.4 | 34.5 |
| 5dd39771b6 / ordinary | 1 | 0 | 4 | 48/48 | 39.412 | 100.6 | 116.4 |
| 5dd39771b6 / ordinary | 1 | 1 | 1 | 0/12 | 0.000 | n/a | n/a |
| 5dd39771b6 / ordinary | 1 | 1 | 4 | 0/48 | 0.000 | n/a | n/a |
| 5dd39771b6 / ordinary | 2 | 0 | 1 | 12/12 | 30.017 | 33.1 | 35.0 |
| 5dd39771b6 / ordinary | 2 | 0 | 4 | 48/48 | 29.831 | 132.8 | 153.5 |
| 5dd39771b6 / ordinary | 2 | 1 | 1 | 0/12 | 0.000 | n/a | n/a |
| 5dd39771b6 / ordinary | 2 | 1 | 4 | 0/48 | 0.000 | n/a | n/a |
| dc2e88aee1 / declarative | 1 | 0 | 1 | 12/12 | 0.385 | 2,592.1 | 2,694.3 |
| dc2e88aee1 / declarative | 1 | 0 | 4 | 48/48 | 0.410 | 9,724.3 | 12,191.9 |
| dc2e88aee1 / declarative | 1 | 1 | 1 | 12/12 | 0.390 | 2,558.1 | 2,610.7 |
| dc2e88aee1 / declarative | 1 | 1 | 4 | 20/48 | 0.176 | 4,745.3 | 9,344.8 |
| dc2e88aee1 / declarative | 2 | 0 | 1 | 12/12 | 0.235 | 4,252.1 | 4,331.8 |
| dc2e88aee1 / declarative | 2 | 0 | 4 | 48/48 | 0.251 | 15,892.3 | 19,854.5 |
| dc2e88aee1 / declarative | 2 | 1 | 1 | 12/12 | 0.238 | 4,185.5 | 4,257.2 |
| dc2e88aee1 / declarative | 2 | 1 | 4 | 18/48 | 0.101 | 7,766.4 | 15,475.6 |

Upstream rejected the INSERT in every mixed transaction (25006), so those cells
measure a capability difference, not a latency comparison. On the prototype
the mixed failures reported that a statement would produce a different result
at commit. This is consistent with the catalog-revision guard on pre-bound DML.
These end-to-end timings do not separate planning, compute installation, frontier
progress, retries or WAL latency. With 12 attempts per client, treat the tail
percentiles as descriptive, not a production capacity estimate.

## Reproduce

Point MZ_URL at a disposable service for the chosen build, enable demo WAL
before first catalog open for the prototype, and run from the checkout with
the generator:

```sh
bin/pyactivate misc/demos/declarative-transactions/benchmark.py \
  --mode declarative --renames 1 2 --writes 0 1 --clients 1 4 --warmup 1 --count 12
```

Use `--mode ordinary` against upstream. The [baseline](benchmark-baseline.jsonl)
and [prototype](benchmark-change.jsonl) JSONL files contain elapsed times,
p99, and error counts as well.
