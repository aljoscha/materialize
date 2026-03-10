I'm implementing "Table Groups" for PostgreSQL sources in Materialize. The full
design doc is at @doc/developer/design/20260310_table_groups.md — read it to
understand what needs to be implemented and to see what's next.

Summary: Table groups let users match multiple upstream PG tables by schema name
+ regex pattern, merging them into a single Materialize collection with typed
columns and `mz_source_schema`/`mz_source_table` identification columns
prepended. All matched tables must have the same schema. New tables matching the
pattern are auto-discovered via periodic polling (WAL-only, no snapshot).

## Workflow

* Each logical unit of work gets committed as a jj change with a good
  description, then continue to the next step.
* Before committing, verify that what you produced compiles (`cargo check`) and
  is high quality and works.
* Code should be simple and clean, well-commented explaining what/how/why.
* Minimal changes — if we iterate and try multiple things, clean up to the
  minimum required fix at the end.
* Minimal changes — don't refactor surrounding code unnecessarily.
* **Read this file again after each context compaction.**
* Update this file's "Progress log" section after each milestone.
* Consult the design doc (@doc/developer/design/20260310_table_groups.md) to
  determine what to implement next.

## Key Code Paths

| Area | Location |
|------|----------|
| Parser AST | `src/sql-parser/src/ast/defs/statement.rs` |
| Parser | `src/sql-parser/src/parser.rs` |
| Planning | `src/sql/src/plan/statement/ddl.rs` |
| Purification | `src/sql/src/pure/postgres.rs` |
| Storage types | `src/storage-types/src/sources/postgres.rs` |
| Source exports | `src/storage-types/src/sources.rs` |
| PG ingestion | `src/storage/src/source/postgres.rs` |
| Snapshot | `src/storage/src/source/postgres/snapshot.rs` |
| Replication | `src/storage/src/source/postgres/replication.rs` |
| Sequencer | `src/adapter/src/coord/sequencer/inner.rs` |
| Catalog | `src/catalog/` |
| Protobuf | `src/storage-types/src/sources/postgres.proto` |

## Current Status

Core implementation complete. Remaining: known gaps below, then test coverage.

## Known Gaps

1. **Schema change re-addition**: Design doc says "If the table's schema is later
   changed back to be compatible, periodic polling re-adds it." This doesn't work
   because the schema validator removes the table from `table_info` in the
   replication loop, but `spawn_table_group_discovery` maintains a separate
   `known_oids` set that doesn't learn about the removal. The table stays in
   `known_oids`, so it's never re-detected as "new". Fix: the discovery task needs
   a way to learn about schema-validator removals (e.g. a shared set, or a channel
   from the replication loop back to the discovery task).

2. **Incompatible auto-discovered tables**: Design doc says "Incompatible tables
   are skipped with a warning in source status." Currently only trace-logged, no
   `HealthStatusMessage` emitted. Fix: send a `HealthStatusMessage` with a warning
   when an incompatible table is skipped during discovery.

## Progress Log

(Update after each milestone — what was implemented, key decisions made.)

- Design doc created: `doc/developer/design/20260310_table_groups.md`
- Parser AST + Parser: Added `CreateTableGroupStatement`, `TableGroup` ObjectType,
  parsing for `CREATE TABLE GROUP ... FROM SOURCE (SCHEMAS (...), TABLE PATTERN '...')`
  and `DROP TABLE GROUP`. Reuses `TableFromSourceOption` for WITH clause. Stubs added
  in planning layer. Purification registration done.
- Storage types: Added `PostgresTableGroupExportDetails` struct, protobuf message
  `ProtoPostgresTableGroupExportStatementDetails`, and `PostgresTableGroup` variant
  in `SourceExportDetails`/`SourceExportStatementDetails` with proto roundtrip.
- Purification + Planning: Implemented `purify_create_table_group` in `src/sql/src/pure.rs`
  (connects to upstream PG, discovers tables, filters by schema+pattern, validates
  identical schemas, generates columns with mz_source_schema/mz_source_table prepended,
  stores details as hex-encoded protobuf). Implemented `plan_create_table_group` in
  `src/sql/src/plan/statement/ddl.rs` (decodes protobuf, generates column casts,
  builds PostgresTableGroupExportDetails, produces CreateTablePlan). Added
  `PurifiedCreateTableGroup` variant and message_handler dispatch.
- Storage layer: Extended SourceOutputInfo with TableGroupContext, modified render()
  to register multiple OIDs per table group output, added cast_row_for_table_group()
  for prepending identification columns. Updated snapshot and replication decoders
  to handle table group outputs.
- Auto-discovery: Added TableGroupInfo and spawn_table_group_discovery for periodic
  polling of upstream PG tables. Discovery task validates new tables against canonical
  schema, adds/removes from table_info via channel. Replication loop processes
  discovery results on keepalive messages.
