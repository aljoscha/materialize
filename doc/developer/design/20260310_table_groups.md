# Table Groups for PostgreSQL Sources

## The Problem

Materialize's current PostgreSQL source model creates one table (subsource) per upstream PG table, each backed by its own persist shard. This works well for targeted replication but becomes unwieldy for use cases with many structurally identical tables:

- **Time-partitioned tables**: `events_2024_01`, `events_2024_02`, ...
- **Multi-tenant databases**: `tenant_a.orders`, `tenant_b.orders`
- **Sharded tables**: `orders_shard_0`, `orders_shard_1`, ...

In these scenarios, users want to treat many upstream tables as a single logical collection in Materialize. Today this requires creating individual tables for each upstream table and using `UNION ALL` views, which is manual, doesn't handle new tables appearing upstream, and creates many persist shards.

## Success Criteria

- Users can define a pattern (schema names + optional table name regex) that matches multiple upstream PG tables and merges them into a single queryable Materialize relation.
- All matched tables must have the same schema. The system validates this at creation time and at runtime.
- The merged relation includes identification columns (`mz_source_schema`, `mz_source_table`) so users can distinguish which upstream table a row came from.
- New upstream tables matching the pattern are automatically discovered and included without user intervention.
- The merged relation supports indexes, materialized views, and SUBSCRIBE like any other relation.

## Out of Scope

- **JSONB envelope format**: A future extension could store row data as JSONB for tables with heterogeneous schemas. Not in scope for v1.
- **ALTER TABLE GROUP**: Changing the pattern post-creation. Users must DROP and re-CREATE in v1.
- **Snapshotting auto-discovered tables**: New tables found after initial creation only receive WAL data from the point of discovery, not historical data. Full snapshotting of newly discovered tables is a future enhancement.
- **MySQL / SQL Server table groups**: This spec covers PostgreSQL sources only. The design could be extended to other source types later.

## Solution Proposal

### DDL

```sql
CREATE TABLE GROUP <name>
    FROM SOURCE <source_name>
    (SCHEMAS ('<schema1>', '<schema2>', ...),
     [TABLE PATTERN '<regex>'])
    [WITH (TEXT COLUMNS (<col>, ...), EXCLUDE COLUMNS (<col>, ...))];
```

- **SCHEMAS** (required): One or more upstream PostgreSQL schema names to match tables in.
- **TABLE PATTERN** (optional): A regex pattern (Rust `regex` crate syntax) applied to table names within the matched schemas. If omitted, all tables in the specified schemas are matched.
- **TEXT COLUMNS / EXCLUDE COLUMNS**: Apply uniformly to all tables in the group. Column names must exist in the shared schema.

```sql
DROP TABLE GROUP [IF EXISTS] <name> [CASCADE | RESTRICT];
```

Examples:

```sql
-- Merge all events_* tables from public schema
CREATE TABLE GROUP all_events
    FROM SOURCE pg_src
    (SCHEMAS ('public'), TABLE PATTERN '^events_');

-- Multi-tenant: merge orders from multiple schemas
CREATE TABLE GROUP all_orders
    FROM SOURCE pg_src
    (SCHEMAS ('tenant_a', 'tenant_b', 'tenant_c'));

-- Query like any other relation
SELECT * FROM all_events WHERE mz_source_table = 'events_2024';

-- Create indexes, materialized views on top
CREATE INDEX ON all_events (mz_source_table, id);

CREATE MATERIALIZED VIEW event_counts AS
    SELECT mz_source_table, count(*) FROM all_events GROUP BY 1;
```

### Resulting Relation Schema

The table group's schema is the upstream tables' shared schema, prepended with identification columns:

| Column | Type | Description |
|--------|------|-------------|
| `mz_source_schema` | `text NOT NULL` | Upstream PG schema name |
| `mz_source_table` | `text NOT NULL` | Upstream PG table name |
| *(data columns)* | *(typed)* | Columns from the shared table schema |

Primary key: `(mz_source_schema, mz_source_table, <upstream_pk_columns>...)`. The identification columns must be part of the key to prevent rows from different upstream tables with the same PK from incorrectly canceling each other in differential dataflow.

### Schema Inference and Enforcement

The table group's column schema is auto-detected from the upstream tables during purification, following the same pattern as `CREATE TABLE FROM SOURCE` (see `generate_source_export_statement_values` in `src/sql/src/pure/postgres.rs`). Users never specify columns directly.

Purification flow:

1. Connect to upstream PG, discover all tables matching `SCHEMAS` + `TABLE PATTERN`.
2. All matching tables must have the same schema: same column names, same types, same nullability. Column order may differ (matched by name, not position).
3. Pick the first matching table's `PostgresTableDesc` as the "canonical schema".
4. Validate all other matching tables against the canonical schema. If any table differs, creation fails with an error identifying the mismatch.
5. Generate column definitions from the canonical schema, prepended with the `mz_source_schema` and `mz_source_table` identification columns.
6. All matching tables must have the same primary key structure (same PK columns), or all must have no primary key.

At runtime, when a `Relation` message arrives in the WAL indicating a schema change for a table in the group, the new schema is validated against the canonical schema. Incompatible tables are removed from the group and an error is reported in source status. The rest of the group continues operating.

### Auto-Discovery

When new tables appear in the upstream PG database that match the schema/pattern:

1. A periodic polling task (default: every 60s) calls `publication_info` to discover new tables.
2. New tables are validated against the canonical schema.
3. Compatible tables are added to the group and begin receiving WAL data only from the point of discovery (no snapshot of existing data).
4. Incompatible tables are skipped with a warning in source status.

Users who need historical data from a newly discovered table must DROP and re-CREATE the table group.

### Table Removal

When an upstream table in the group is dropped:

1. The replication stream stops sending rows for that table's OID.
2. Periodic polling detects the table is gone and removes it from tracking.
3. Existing data from the dropped table remains in the persist shard. No retractions are emitted—the historical data represents a true record of what was in the table.
4. The group continues operating for remaining tables.

### Schema Changes on Upstream Tables

If an upstream table's schema changes incompatibly:

1. That table is removed from the group's active set.
2. An error is reported in source status identifying the table and mismatch.
3. The group continues for other tables.
4. If the table's schema is later changed back to be compatible, periodic polling re-adds it.

### Catalog Representation

Implement as a variant of the existing `Table` type with `TableDataSource::DataSource` / `IngestionExport`, adding a new `SourceExportDetails` variant:

```rust
// In src/storage-types/src/sources.rs
pub enum SourceExportDetails {
    // ... existing variants ...
    PostgresTableGroup(PostgresTableGroupExportDetails),  // NEW
}
```

```rust
// In src/storage-types/src/sources/postgres.rs
pub struct PostgresTableGroupExportDetails {
    /// The canonical schema all tables must match
    pub canonical_desc: PostgresTableDesc,
    /// The upstream PG schemas to filter on
    pub schema_names: Vec<String>,
    /// Optional regex pattern for table names
    pub table_pattern: Option<String>,
    /// Column casts (shared by all tables since schemas match)
    pub column_casts: Vec<(CastType, MirScalarExpr)>,
    /// Tables matched at creation time
    pub initial_tables: Vec<PostgresTableDesc>,
}
```

This reuses the existing Table + IngestionExport infrastructure, minimizing changes to catalog, coordinator, and sequencer.

### Storage Layer Changes

**Row routing**: Currently `table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>` maps each upstream table OID to its output index. For a table group, multiple OIDs map to the same output index (the single persist shard). The existing partition-by-output-index logic handles this correctly.

**Row construction**: Both snapshot and replication paths must prepend identification columns (`mz_source_schema`, `mz_source_table`) to every row. This must be consistent across both paths—any inconsistency causes differential dataflow correctness bugs.

**SourceOutputInfo extension**:

```rust
struct SourceOutputInfo {
    // ... existing fields ...
    /// Present for table group outputs; identifies the upstream table
    table_group_context: Option<TableGroupContext>,
}

struct TableGroupContext {
    schema_name: String,
    table_name: String,
}
```

**Auto-discovery**: The replication operator spawns a periodic task that calls `publication_info` to discover new matching tables, validates their schema, and adds/removes entries in `table_info`. New tables receive WAL data only from discovery time.

### Compatibility

- **Indexes**: Work normally on the table group relation.
- **Materialized views**: Can SELECT from table groups.
- **SUBSCRIBE**: Returns rows with identification columns.
- **Overlap with individual tables**: A table can appear both in a table group and as a standalone `CREATE TABLE FROM SOURCE`. Both get data independently (separate persist shards).

## Alternatives

### JSONB Envelope

Instead of requiring identical schemas, store row data as JSONB:

```sql
CREATE TABLE GROUP all_tables
    FROM SOURCE pg_src
    (SCHEMAS ('public'))
    WITH (FORMAT = 'jsonb');
-- Schema: mz_source_schema text, mz_source_table text, data jsonb
```

This removes the same-schema requirement but loses type safety and query performance. Could be added as a future option orthogonal to the typed approach.

### Extend CREATE TABLE FROM SOURCE

Instead of a new `CREATE TABLE GROUP` statement, extend the existing `CREATE TABLE FROM SOURCE` with pattern matching:

```sql
CREATE TABLE all_events FROM SOURCE pg_src
    (SCHEMAS ('public'), TABLE PATTERN '^events_');
```

This minimizes DDL surface area but conflates two very different semantics (single-table vs multi-table) in one statement.

## Open Questions

None remaining—all questions resolved during design:

- Auto-discovery: WAL-only, no snapshotting
- Schemas: Multiple schemas supported
- Pattern syntax: Regex (Rust `regex` crate)
- ALTER: Not in v1, DROP + CREATE only
- Schema inference: Auto-detected from upstream, not user-specified
