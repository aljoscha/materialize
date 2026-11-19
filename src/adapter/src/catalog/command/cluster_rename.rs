// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile unresolved schema renames to a private, cluster-evaluated SELECT.
//!
//! Plan with the system catalog and trusted builtin search path, never the
//! client's search path. Read all imports at the same certified timestamp.
//! Before consuming *any* result (including a notice or error), fence the
//! durable catalog upper and the system configuration used to compile it.
//! Matching the returned before-images alone does not protect negative reads:
//! a new dependent, conflicting schema, or revoked grant can invalidate them.

use std::fmt::Write;

use mz_ore::str::StrExt;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal as lit};
use mz_sql::ast::{Statement, UnresolvedObjectName};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::SystemVars;

use super::SchemaRenameProgram;
use crate::AdapterError;

/// Return (channel text, sign int8, payload jsonb). Catalog payloads are
/// complete durable before/after images. Diagnostic payloads include a
/// zero-based step. The first error suppresses all other outputs.
pub(crate) fn candidate_sql(
    program: &SchemaRenameProgram,
    vars: &SystemVars,
    audit_time: u64,
) -> Result<String, AdapterError> {
    let audit_time = vars
        .unsafe_mock_audit_event_timestamp()
        .map(u64::from)
        .unwrap_or(audit_time);
    let mut sql = String::from(
        "WITH
        raw_schemas AS (
            SELECT mz_internal.parse_catalog_id(data->'key'->'id') AS id, data
            FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'Schema'
        ),
        schemas0 AS (SELECT id, database_id, owner_id, name FROM mz_catalog.mz_schemas),
        items0 AS (
            SELECT data, mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
                   mz_internal.parse_catalog_id(data->'value'->'schema_id') AS schema_id,
                   data->'value'->'definition'->'V1'->>'create_sql' AS def,
                   data->'value'->>'name' AS item_name,
                   data->'value'->>'ephemeral_owner_session' AS temp_session
            FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'Item'
        ),
        edges AS (
            SELECT object_id, referenced_object_id FROM mz_internal.mz_object_dependencies
            UNION ALL
            SELECT i.id, e.ref FROM items0 i,
                jsonb_array_elements_text(mz_internal.parse_catalog_item_references(i.def)->'ids') e(ref)
            WHERE temp_session IS NOT NULL
        ),
        events0(step, tag, id, old_val, new_val, detail, username) AS (
            SELECT 0::int8, ''::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text WHERE false
        )",
    );
    for (i, step) in program.steps.iter().enumerate() {
        let Statement::AlterObjectRename(rename) = &*step.statement else {
            return Err(AdapterError::Unsupported("not a schema rename"));
        };
        let UnresolvedObjectName::Schema(name) = &rename.name else {
            return Err(AdapterError::Unsupported("not a schema rename"));
        };
        let norm = mz_sql::normalize::unresolved_schema_name(name.clone())?;
        let explicit = norm.database.is_some();
        let database = norm.database.as_deref().unwrap_or(step.session.database());
        let db = lit(database);
        let quoted_db = database.quoted().to_string();
        let quoted_db = lit(&quoted_db);
        let username = lit(&step.session.user().name);
        let src = lit(&norm.schema);
        let dest = lit(rename.to_item_name.as_str());
        let spelling = name.to_ast_string_simple();
        let quoted = spelling.quoted().to_string();
        let notice = lit(&quoted);
        let missing = rename.if_exists;
        let reserved = crate::catalog::is_reserved_name(rename.to_item_name.as_str());
        let temp = norm.schema == mz_repr::namespaces::MZ_TEMP_SCHEMA;
        let roles = step.session.role_metadata();
        let current = roles.current_role;
        let session = roles.session_role;
        let auth = roles.authenticated_role;
        let bypass = step.session.is_superuser()
            || (!mz_sql::rbac::is_rbac_enabled_for_session(vars, &step.session)
                && !current.is_system()
                && !session.is_system());
        let next = i + 1;
        // Dependencies are keyed by stable IDs, but both the namespace and the
        // text fed into the rewrite belong to this instruction's prefix.
        write!(sql, ",
        memberships{i} AS (
            WITH MUTUALLY RECURSIVE reach(id text) AS (
                SELECT '{current}'::text UNION
                SELECT m.role_id FROM reach JOIN mz_catalog.mz_role_members m ON m.member = reach.id
            )
            SELECT id FROM reach UNION SELECT 'p'
        ),
        role_error{i} AS (
            SELECT CASE
                WHEN NOT EXISTS (SELECT FROM mz_catalog.mz_roles WHERE id = '{current}') THEN '{current}'
                WHEN NOT EXISTS (SELECT FROM mz_catalog.mz_roles WHERE id = '{session}') THEN '{session}'
                WHEN NOT EXISTS (SELECT FROM mz_catalog.mz_roles WHERE id = '{auth}') THEN '{auth}'
            END AS missing_role
        ),
        db{i} AS (SELECT * FROM mz_catalog.mz_databases WHERE name = {db}),
        target{i} AS (
            SELECT s.* FROM schemas{i} s WHERE s.name = {src}
              AND (s.database_id IN (SELECT id FROM db{i})
                OR (s.database_id IS NULL AND (NOT {explicit} OR EXISTS (SELECT FROM db{i}))))
            ORDER BY s.database_id NULLS LAST LIMIT 1
        ),
        decision{i} AS (
            SELECT s.id, r.missing_role, CASE
                WHEN {temp} AND NOT {explicit} THEN 'ambient_temp'
                WHEN s.id IS NULL AND {temp} AND EXISTS (SELECT FROM db{i}) THEN 'unsupported_temp'
                WHEN s.id IS NULL AND NOT {missing} THEN CASE
                    WHEN {explicit} AND NOT EXISTS (SELECT FROM db{i}) THEN 'unknown_database'
                    ELSE 'unknown_schema' END
                WHEN EXISTS (SELECT FROM schemas{i} dst WHERE dst.name = {dest}
                    AND dst.database_id IS NOT DISTINCT FROM s.database_id AND s.id IS NOT NULL)
                    THEN 'occupied'
                WHEN s.id LIKE 's%' OR (s.id IS NOT NULL AND s.database_id IS NULL)
                    THEN 'system_schema'
                WHEN r.missing_role IS NOT NULL THEN 'role_dropped'
                WHEN s.id IS NULL THEN 'notice'
                WHEN NOT {bypass} AND s.owner_id NOT IN (SELECT id FROM memberships{i}) THEN 'not_owner'
                WHEN (NOT {bypass} OR s.database_id LIKE 's%') AND NOT EXISTS (
                    SELECT FROM db{i}, unnest(privileges) AS a(priv)
                    WHERE mz_internal.mz_aclitem_grantee(priv) IN (SELECT id FROM memberships{i})
                      AND mz_internal.mz_acl_item_contains_privilege(priv, 'CREATE')
                ) THEN 'no_create'
                WHEN {reserved} THEN 'reserved'
                ELSE 'audit' END AS tag
            FROM role_error{i} r LEFT JOIN target{i} s ON true
        ),
        members{i} AS (
            SELECT it.id FROM items{i} it JOIN decision{i} d ON it.schema_id = d.id WHERE d.tag = 'audit'
        ),
        affected{i} AS (
            SELECT id FROM members{i} UNION SELECT e.object_id FROM edges e
            JOIN members{i} m ON e.referenced_object_id = m.id
        ),
        rewrite{i} AS (
            SELECT it.id, mz_internal.rewrite_catalog_schema(it.def, {db}, {src}, {dest}) AS result
            FROM items{i} it JOIN affected{i} a ON a.id = it.id
        ),
        items{next} AS (
            SELECT it.id, it.schema_id, coalesce(r.result->>'create_sql', it.def) AS def, it.item_name, it.temp_session
            FROM items{i} it LEFT JOIN rewrite{i} r USING (id)
        ),
        schemas{next} AS (
            SELECT s.id, s.database_id, s.owner_id, CASE WHEN d.tag = 'audit' THEN {dest} ELSE s.name END AS name
            FROM schemas{i} s LEFT JOIN decision{i} d ON s.id = d.id
        ),
        events{next} AS (
            SELECT * FROM events{i} UNION ALL
            SELECT {i}::int8, tag, id, CASE WHEN tag = 'no_create' THEN {quoted_db} ELSE {src} END, {dest}, CASE
                WHEN tag = 'no_create' THEN (SELECT name FROM mz_catalog.mz_roles WHERE id = '{current}')
                WHEN tag = 'notice' THEN {notice} WHEN tag = 'role_dropped' THEN missing_role ELSE {db} END, {username}
            FROM decision{i} UNION ALL
            SELECT {i}::int8, 'ambiguous', r.id, {db} || '.' || {src},
                coalesce(d.name || '.', '') || coalesce(s.name, 'mz_temp') || '.' || it.item_name,
                result->>'ambiguous_schema', {username}
            FROM rewrite{i} r JOIN items{i} it USING (id)
            LEFT JOIN schemas{i} s ON it.schema_id = s.id
            LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
            WHERE result->>'ambiguous_schema' IS NOT NULL
        )").expect("writing to String");
    }
    use mz_catalog::durable::objects::serialization::proto::audit_log_event_v1::{
        EventType, ObjectType,
    };
    let n = program.steps.len();
    write!(
        sql,
        include_str!("cluster_rename_result.sql"),
        n = n,
        audit_time = audit_time,
        alter = EventType::Alter as i32,
        schema = ObjectType::Schema as i32
    )
    .expect("writing to String");
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_sql::plan::{Params, Plan};
    use mz_sql_parser::parser::parse_statements;

    use super::*;
    use crate::catalog::Catalog;
    use crate::session::Session;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn plan_chained_renames_with_quoted_names() {
        Catalog::with_debug(|catalog| async move {
            let mut program = SchemaRenameProgram::default();
            let mut session = Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session
                .start_transaction(mz_ore::now::to_datetime(1000), None, None)
                .unwrap();
            for stmt in parse_statements(
                r#"ALTER SCHEMA public RENAME TO "a'; SELECT 1; --";
                ALTER SCHEMA IF EXISTS "a'; SELECT 1; --" RENAME TO public"#,
            )
            .unwrap()
            {
                program
                    .push(Arc::new(stmt.ast), Params::empty(), session.meta())
                    .unwrap();
            }
            let sql = candidate_sql(&program, catalog.system_config(), 1000).unwrap();
            let stmts = parse_statements(&sql).unwrap();
            let [stmt] = &stmts[..] else {
                panic!("expected one candidate query");
            };
            let sys = catalog.for_system_session();
            let (stmt, ids) = mz_sql::names::resolve(&sys, stmt.ast.clone()).unwrap();
            let (plan, _) = mz_sql::plan::plan(None, &sys, stmt, &Params::empty(), &ids).unwrap();
            assert!(matches!(plan, Plan::Select(_)));
        })
        .await;
    }
}
