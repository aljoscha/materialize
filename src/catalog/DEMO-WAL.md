# Fresh demo catalog on the table WAL

Set `MZ_DEMO_CATALOG_TXN_WAL=1` **before the first catalog open** and on every restart. Leave it unset for a normal environment. There is no conversion in either direction. Use a new organization ID and isolated persist location for the demo.

First open compares and appends at the initial upper of the normal catalog shard. Ordinary creation appends nothing. Demo creation installs an immutable routing record with a future catalog content version, so an older binary also refuses to open it. Only after verifying that record does the demo open its catalog data shard and deterministic table WAL. The storage WAL ID is included in catalog initialization. Back up the routing shard, data shards and WAL together.

## Commit contract

- Use `DurableCatalogState::shares_table_wal()` for discovery after open. Do not infer ownership from the presence of a table WAL in storage metadata. Normal catalogs have one too.
- Consume catalog operation updates as for `Transaction::commit`. Call `Transaction::commit_with_table_diffs(ts, tables)` on the same live transaction. The lower-level operation is `DurableCatalogState::commit_transaction_with_table_diffs(batch, ts, tables)`.
- `TableDiffs` maps existing storage shard IDs to `Vec<(Row, Diff)>`. Resolve IDs, authorize writes, and validate rows against the current table schema before calling. This API does not create participants, coordinate schema changes, validate a table read set, or perform adapter bookkeeping.
- Reserve one timestamp at or above the transaction upper. Success returns exactly `ts.step_forward()`. Do not send those diffs through the ordinary storage append path or advance the same WAL again at that timestamp. Publish in-memory catalog state, responses, and timestamp completion only after success.
- A competing WAL write invalidates the expected upper, even if the proposed timestamp is far in the future. A mixed commit never silently rebases. `WalTimestampPassed(upper)` reports a pre-commit timestamp conflict. Neither this error nor `NotWritable` publishes a mixed commit. Treat fencing and out-of-sync errors as for ordinary catalog commits. Do not retry a possibly committed operation after process loss without rereading its state.
- Registration of the catalog is permanent. No direct writer, including a repair tool, may write its data shard. Recovery applies WAL decisions before reading physical catalog state. Empty progress is filled only after applying decisions through that timestamp.

Exercise the ownership mismatch, mixed commit, interrupted apply, and stale writer cases with `cargo test -p mz-catalog --lib demo_wal_recovery_and_fencing`.
