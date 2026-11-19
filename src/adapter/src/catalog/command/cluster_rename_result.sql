,
allocator AS (
    SELECT data, (data->'value'->>'next_id')::uint8 AS base
    FROM mz_internal.mz_catalog_raw
    WHERE data->>'kind' = 'IdAlloc' AND data->'key'->>'name' = 'auditlog'
),
ordered_moves AS (
    SELECT *, row_number() OVER (ORDER BY step) AS pos
    FROM events{n} WHERE tag = 'audit'
),
failures AS (
    SELECT * FROM events{n} WHERE tag NOT IN ('notice', 'audit')
    UNION ALL
    SELECT step, CASE WHEN base IS NULL THEN 'missing_allocator' ELSE 'id_exhaustion' END,
           id, old_val, new_val, detail, username
    FROM ordered_moves LEFT JOIN allocator ON true
    WHERE base IS NULL OR base::numeric + pos > 18446744073709551615
),
failure AS (
    SELECT * FROM failures ORDER BY step, CASE WHEN tag = 'ambiguous' THEN 0 ELSE 1 END, id LIMIT 1
),
diagnostics AS (
    SELECT * FROM failure UNION ALL
    SELECT * FROM events{n} WHERE tag = 'notice' AND NOT EXISTS (SELECT FROM failure)
),
moves AS (
    SELECT * FROM ordered_moves WHERE NOT EXISTS (SELECT FROM failure)
),
-- Patch only the changed paths. A retraction must match the durable row,
-- including nulls and fields that the rename does not interpret.
edits(old_data, new_data) AS (
    SELECT r.data, r.data || jsonb_build_object('value',
        (r.data->'value') || jsonb_build_object('name', s.name))
    FROM schemas{n} s JOIN raw_schemas r USING (id)
    WHERE r.data->'value'->>'name' <> s.name
    UNION ALL
    SELECT b.data, b.data || jsonb_build_object('value',
        (b.data->'value') || jsonb_build_object('definition',
            (b.data->'value'->'definition') || jsonb_build_object('V1',
                (b.data->'value'->'definition'->'V1') || jsonb_build_object('create_sql', it.def))))
    FROM items{n} it JOIN items0 b USING (id) WHERE it.def <> b.def
    UNION ALL
    SELECT data, data || jsonb_build_object('value', (data->'value') ||
        jsonb_build_object('next_id', base + (SELECT count(*) FROM moves)::uint8))
    FROM allocator WHERE EXISTS (SELECT FROM moves)
),
catalog(sign, payload) AS (
    SELECT -1::int8, old_data FROM edits UNION ALL SELECT 1::int8, new_data FROM edits
    UNION ALL
    SELECT 1::int8, jsonb_build_object('kind', 'AuditLog', 'key', jsonb_build_object(
        'event', jsonb_build_object('V1', jsonb_build_object(
            'id', base + (pos - 1)::uint8, 'event_type', {alter}, 'object_type', {schema},
            'user', jsonb_build_object('inner', username),
            'occurred_at', jsonb_build_object('millis', {audit_time}::uint8),
            'details', jsonb_build_object('RenameSchemaV1', jsonb_build_object(
                'id', id, 'database_name', detail, 'old_name', old_val, 'new_name', new_val))))))
    FROM moves CROSS JOIN allocator
)
SELECT 'catalog'::text AS channel, sign, payload FROM catalog WHERE NOT EXISTS (SELECT FROM failure)
UNION ALL
SELECT CASE WHEN tag = 'notice' THEN 'notice' ELSE 'error' END, 1::int8,
    jsonb_build_object('step', step, 'code', CASE
        WHEN tag IN ('no_create', 'role_dropped', 'not_owner') THEN '42501'
        WHEN tag = 'unsupported_temp' THEN '0A000'
        WHEN tag = 'notice' THEN '42704'
        ELSE 'XX000' END, 'message', CASE tag
        WHEN 'missing_allocator' THEN 'auditlog id allocator missing'
        WHEN 'id_exhaustion' THEN 'id counter overflows i64'
        WHEN 'unknown_database' THEN 'unknown database ''' || detail || ''''
        WHEN 'unknown_schema' THEN 'unknown schema ''' || old_val || ''''
        WHEN 'occupied' THEN 'schema ''' || new_val || ''' already exists'
        WHEN 'reserved' THEN 'unacceptable schema name ''' || new_val || ''''
        WHEN 'system_schema' THEN 'renaming the ' || old_val || ' schema is not supported'
        WHEN 'ambient_temp' THEN 'cannot rename schemas in the ambient database: "' || old_val || '"'
        WHEN 'unsupported_temp' THEN 'qualified temporary schema rename are not supported'
        WHEN 'role_dropped' THEN 'role ' || detail || ' was concurrently dropped'
        WHEN 'not_owner' THEN 'must be owner of SCHEMA ' || detail || '.' || old_val
        WHEN 'no_create' THEN 'permission denied for DATABASE ' || old_val
        WHEN 'ambiguous' THEN 'renaming conflict: in ' || new_val ||
            CASE WHEN new_val = old_val THEN '' ELSE ', which uses ' || old_val END ||
            ', ambiguous reference to schema named ' || detail
        WHEN 'notice' THEN 'SCHEMA ' || detail || ' does not exist, skipping'
        END, 'detail', CASE tag
        WHEN 'reserved' THEN 'The prefixes "mz_" and "pg_" are reserved for system schemas.'
        WHEN 'role_dropped' THEN 'Please disconnect and re-connect with a valid role.'
        WHEN 'no_create' THEN 'The ''' || detail || ''' role needs CREATE privileges on DATABASE ' || old_val
        END, 'hint', NULL::text)
FROM diagnostics
