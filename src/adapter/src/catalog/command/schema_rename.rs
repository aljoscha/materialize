// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordered keyed joins for user-schema renames. Only names referenced by an
//! instruction can affect the result, so seed the relation by index probes.

use std::collections::BTreeMap;

use mz_sql::ast::UnresolvedObjectName;
use mz_sql::ast::display::AstDisplay;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{
    DatabaseId, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{AlterNoopPlan, AlterSchemaRenamePlan, PlanError};

use super::*;

#[derive(Debug)]
pub(super) struct Instruction {
    database: String,
    source: String,
    destination: String,
    /// SQL spelling for the IF EXISTS notice.
    skip_missing: Option<String>,
}

impl Instruction {
    pub(super) fn lower(stmt: &Statement<Raw>, session: &SessionMeta) -> Option<Self> {
        let Statement::AlterObjectRename(rename) = stmt else {
            return None;
        };
        let UnresolvedObjectName::Schema(name) = &rename.name else {
            return None;
        };
        let source = mz_sql::normalize::unresolved_schema_name(name.clone()).ok()?;
        Some(Self {
            database: source.database.unwrap_or_else(|| session.database().into()),
            source: source.schema,
            destination: rename.to_item_name.as_str().into(),
            skip_missing: rename.if_exists.then(|| name.to_ast_string_simple()),
        })
    }
}

type Namespace = BTreeMap<(DatabaseId, String), SchemaId>;

enum Joined {
    Missing,
    Occupied,
    Move(SchemaId),
}

impl Instruction {
    fn join(&self, rows: &Namespace, db: DatabaseId) -> Joined {
        match (
            rows.get(&(db, self.source.clone())),
            rows.get(&(db, self.destination.clone())),
        ) {
            (None, _) => Joined::Missing,
            (Some(_), Some(_)) => Joined::Occupied,
            (Some(id), None) => Joined::Move(*id),
        }
    }
}

impl SchemaRenameProgram {
    /// None delegates the whole prefix to SQL, including its diagnostic names.
    pub(super) fn compile_user(
        &self,
        base: &CatalogState,
    ) -> Option<(Vec<Op>, Result<Vec<SchemaRenameOutcome>, AdapterError>)> {
        let mut rows = Namespace::new();
        let mut instructions = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            let instr = step.instruction.as_ref()?;
            let db = *base.database_by_name.get(&instr.database)?;
            let initial = &base.database_by_id[&db].schemas_by_name;
            for name in [&instr.source, &instr.destination] {
                // SQL owns ambient lookup, lazy temporary schemas and builtin restrictions.
                if name == mz_repr::namespaces::MZ_TEMP_SCHEMA
                    || base.ambient_schemas_by_name.contains_key(name)
                {
                    return None;
                }
                if let Some(id) = initial.get(name) {
                    if id.is_system() {
                        return None;
                    }
                    rows.insert((db, name.clone()), *id);
                }
            }
            instructions.push((db, instr, &step.session));
        }

        let mut ops = Vec::new();
        let mut outcomes = Vec::new();
        let deps = ResolvedIds::empty(); // Schema names do not reference catalog items.
        for (db, instr, session) in instructions {
            let (plan, notices) = match instr.join(&rows, db) {
                Joined::Missing => {
                    let Some(name) = &instr.skip_missing else {
                        let error =
                            PlanError::Catalog(CatalogError::UnknownSchema(instr.source.clone()));
                        return Some((ops, Err(error.into())));
                    };
                    let object_type = ObjectType::Schema;
                    (
                        Plan::AlterNoop(AlterNoopPlan { object_type }),
                        vec![PlanNotice::ObjectDoesNotExist {
                            name: name.clone(),
                            object_type,
                        }],
                    )
                }
                Joined::Occupied => {
                    let error = PlanError::Catalog(CatalogError::SchemaAlreadyExists(
                        instr.destination.clone(),
                    ));
                    return Some((ops, Err(error.into())));
                }
                Joined::Move(id) => {
                    rows.remove(&(db, instr.source.clone()));
                    rows.insert((db, instr.destination.clone()), id);
                    (
                        Plan::AlterSchemaRename(AlterSchemaRenamePlan {
                            cur_schema_spec: (
                                ResolvedDatabaseSpecifier::Id(db),
                                SchemaSpecifier::Id(id),
                            ),
                            new_schema_name: instr.destination.clone(),
                        }),
                        Vec::new(),
                    )
                }
            };
            let (tx, _rx) = mpsc::unbounded_channel();
            let catalog = base.for_session_metadata(session, tx);
            // Ownership and CREATE privileges are keyed by stable IDs. On denial
            // use the SQL backend so the error names the schema at this prefix.
            mz_sql::rbac::check_plan(&catalog, None, session, &plan, None, &deps, &deps).ok()?;
            let (writes, _) = rename_ops(&plan);
            outcomes.push(SchemaRenameOutcome {
                changed: !writes.is_empty(),
                notices,
            });
            ops.extend(writes);
        }
        Some((ops, Ok(outcomes)))
    }
}

pub(super) fn project(namespace: &mut CatalogState, op: &Op) {
    let Op::RenameSchema {
        database_spec,
        schema_spec,
        new_name,
        ..
    } = op
    else {
        unreachable!("schema rename only");
    };
    if let (ResolvedDatabaseSpecifier::Id(db), SchemaSpecifier::Id(id)) =
        (database_spec, schema_spec)
    {
        let database = namespace
            .database_by_id
            .get_mut(db)
            .expect("resolved database");
        let schema = database.schemas_by_id.get_mut(id).expect("resolved schema");
        let old = std::mem::replace(&mut schema.name.schema, new_name.clone());
        database.schemas_by_name.remove(&old);
        database.schemas_by_name.insert(new_name.clone(), *id);
    }
}
