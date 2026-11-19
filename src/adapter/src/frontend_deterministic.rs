// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordered, one-attempt evaluation. Nothing is written while evaluating.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::consolidation::consolidate;
use mz_catalog::memory::objects::{CatalogItem, Table, TableDataSource};
use mz_expr::{CollectionPlan, Id, MirRelationExpr, MirScalarExpr, UnaryFunc};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_sql::plan::ReadThenWritePlan;
use mz_transform::TransformCtx;
use mz_transform::typecheck::empty_typechecking_context;

use crate::catalog::Catalog;
use crate::error::AdapterError;
use crate::frontend_read_then_write::apply_mutation_to_mir;
use crate::optimize::{OptimizerError, optimize_mir_constant};
use crate::peek_client::StorageCollectionsHandle;

type Rows = Vec<(Row, Diff)>;

#[derive(Debug)]
pub(crate) struct DeterministicMutation {
    id: CatalogItemId,
    generation: GlobalId,
    desc: RelationDesc,
    inputs: BTreeMap<CatalogItemId, GlobalId>,
    mutation: MirRelationExpr,
}

impl DeterministicMutation {
    /// Use the statement's authorized catalog snapshot, including prior staged DDL.
    pub(crate) fn prepare(
        catalog: &Catalog,
        plan: ReadThenWritePlan,
    ) -> Result<Self, AdapterError> {
        if !plan.returning.is_empty() || !plan.finishing.is_trivial(plan.selection.arity()) {
            return Err(AdapterError::Unsupported(
                "RETURNING or nontrivial finishing in deterministic mutations",
            ));
        }
        if plan.selection.contains_unmaterializable() {
            return Err(AdapterError::Unsupported(
                "unstable expressions in deterministic mutations",
            ));
        }
        let table = |id: CatalogItemId| {
            let entry = catalog.try_get_entry(&id).ok_or_else(|| changed(id))?;
            if !id.is_user()
                || !matches!(
                    entry.item(),
                    CatalogItem::Table(Table {
                        data_source: TableDataSource::TableWrites { .. },
                        ..
                    })
                )
            {
                return Err(AdapterError::Unsupported(
                    "deterministic mutations on relations other than writable user tables",
                ));
            }
            Ok(entry)
        };
        let target = table(plan.id)?;
        let mut inputs = BTreeMap::new();
        for dep in plan.selection.depends_on() {
            let id = catalog
                .try_resolve_item_id(&dep)
                .ok_or_else(|| changed(dep))?;
            inputs.insert(id, dep);
            if table(id)?.latest_global_id() != dep {
                return Err(changed(dep));
            }
        }

        let selection = plan.selection.lower(catalog.system_config(), None)?;
        let mutation = apply_mutation_to_mir(selection, &plan.kind, &plan.assignments);
        mutation.try_visit_scalars(&mut validate_scalar)?;
        // Exclude partial-order selections and order-sensitive aggregates. They can
        // choose different tuples from the same multiset.
        let mut unsupported = false;
        mutation.visit_pre(|e| {
            unsupported |= match e {
                MirRelationExpr::TopK { .. } | MirRelationExpr::LetRec { .. } => true,
                MirRelationExpr::Reduce { aggregates, .. } => aggregates.iter().any(|a| {
                    !matches!(
                        a.func,
                        mz_expr::AggregateFunc::Count
                            | mz_expr::AggregateFunc::Any
                            | mz_expr::AggregateFunc::All
                    )
                }),
                MirRelationExpr::FlatMap { func, .. } => {
                    !matches!(func, mz_expr::TableFunc::Wrap { .. })
                }
                _ => false,
            }
        });
        if unsupported {
            return Err(AdapterError::Unsupported(
                "this relational operator in deterministic mutations",
            ));
        }
        Ok(Self {
            id: plan.id,
            generation: target.latest_global_id(),
            desc: target
                .relation_desc_latest()
                .expect("validated table")
                .into_owned(),
            mutation,
            inputs,
        })
    }
}

fn validate_scalar(root: &MirScalarExpr) -> Result<(), AdapterError> {
    let err = || {
        AdapterError::Unsupported("unstable or unbounded expressions in deterministic mutations")
    };
    let mut todo = vec![root];
    while let Some(expr) = todo.pop() {
        match expr {
            MirScalarExpr::CallUnmaterializable(_) => return Err(err()),
            MirScalarExpr::CallUnary { func, .. } => {
                // Cast payloads can carry expressions not returned by scalar.children().
                let mut func = func;
                while let UnaryFunc::TryCast(wrapped) = func {
                    func = wrapped.inner();
                }
                match func {
                    UnaryFunc::Sleep(_) | UnaryFunc::Panic(_) => return Err(err()),
                    f if f.is_unbounded() => return Err(err()),
                    UnaryFunc::CastStringToArray(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastStringToList(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastStringToMap(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastStringToRange(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastArrayToArray(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastList1ToList2(f) => todo.push(&f.cast_expr),
                    UnaryFunc::CastArrayToJsonb(f) => todo.push(&f.cast_element),
                    UnaryFunc::CastListToJsonb(f) => todo.push(&f.cast_element),
                    UnaryFunc::CastRecord1ToRecord2(f) => todo.extend(f.cast_exprs.iter()),
                    _ => (),
                }
            }
            _ => (),
        }
        todo.extend(expr.children());
    }
    Ok(())
}

fn changed(id: impl std::fmt::Display) -> AdapterError {
    AdapterError::ConcurrentDependencyMutation {
        dependency_id: id.to_string(),
    }
}

/// The commit must compare-and-append at exactly `write_ts`, including for empty
/// payloads, and validate the catalog preconditions for planning in the same commit.
#[derive(Debug)]
pub(crate) struct CertifiedWrites {
    pub(crate) write_ts: Timestamp,
    pub(crate) writes: BTreeMap<CatalogItemId, (GlobalId, Rows)>,
}

/// Evaluate at one externally chosen OCC target. On conflict call again with
/// the new target. `created` identifies tables created empty by this transaction.
pub(crate) async fn evaluate_deterministic(
    storage: &StorageCollectionsHandle,
    catalog: &Catalog,
    statements: &[DeterministicMutation],
    created: &BTreeSet<GlobalId>,
    write_ts: Timestamp,
) -> Result<CertifiedWrites, AdapterError> {
    let as_of = write_ts.step_back().ok_or(AdapterError::Unsupported(
        "writing at the minimum timestamp",
    ))?;
    let mut generations = BTreeMap::new();
    let mut inputs = BTreeSet::new();
    for s in statements {
        inputs.extend(s.inputs.values().copied());
        for (id, gid) in s
            .inputs
            .iter()
            .map(|(id, gid)| (*id, *gid))
            .chain([(s.id, s.generation)])
        {
            if generations
                .insert(id, gid)
                .is_some_and(|previous| previous != gid)
            {
                return Err(changed(id));
            }
        }
    }
    let holds = storage
        .acquire_read_holds(inputs.difference(created).copied().collect())
        .map_err(|e| changed(e.0))?;
    let limit = catalog.system_config().max_result_size();
    let mut bytes = 0;
    let mut snapshot = BTreeMap::new();
    for hold in &holds {
        let mut rows = Vec::new();
        // The txn-aware storage cursor certifies all updates through as_of,
        // including when a table has no physical updates at that timestamp.
        let mut cursor = storage.snapshot_cursor(hold.id(), as_of).await?;
        while let Some(batch) = cursor.next().await {
            for (data, _, diff) in batch {
                let row = data
                    .0
                    .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!(e)))?;
                charge(&mut bytes, &row, limit)?;
                rows.push((row, Diff::from(diff)));
            }
        }
        consolidate(&mut rows);
        snapshot.insert(hold.id(), rows);
    }
    drop(holds);
    for id in inputs.intersection(created) {
        snapshot.insert(*id, vec![]);
    }
    evaluate(
        statements,
        snapshot,
        write_ts,
        &OptimizerFeatures::from(catalog.system_config()),
        limit,
    )
}

fn charge(bytes: &mut u64, row: &Row, limit: u64) -> Result<(), AdapterError> {
    *bytes = bytes.saturating_add(u64::try_from(row.byte_len().max(1)).unwrap_or(u64::MAX));
    if *bytes > limit {
        return Err(AdapterError::ResultSize(format!(
            "deterministic transaction exceeds {limit} bytes"
        )));
    }
    Ok(())
}

fn evaluate(
    statements: &[DeterministicMutation],
    mut snapshot: BTreeMap<GlobalId, Rows>,
    write_ts: Timestamp,
    features: &OptimizerFeatures,
    limit: u64,
) -> Result<CertifiedWrites, AdapterError> {
    let mut writes = BTreeMap::new();
    let mut bytes = 0;
    for stmt in statements {
        let mut expr = stmt.mutation.clone();
        let mut missing = None;
        expr.visit_pre_mut(|e| {
            if let MirRelationExpr::Get {
                id: Id::Global(id),
                typ,
                ..
            } = e
            {
                if let Some(rows) = snapshot.get(id) {
                    *e = MirRelationExpr::Constant {
                        rows: Ok(rows.clone()),
                        typ: typ.clone(),
                    };
                } else {
                    missing = Some(*id);
                }
            }
        });
        if let Some(id) = missing {
            return Err(changed(id));
        }

        let typecheck = empty_typechecking_context();
        let mut meta = Default::default();
        let mut ctx = TransformCtx::local(features, &typecheck, &mut meta, None, None);
        let mut result = mz_transform::catch_unwind_optimize(|| -> Result<_, OptimizerError> {
            optimize_mir_constant(expr, &mut ctx, true)
        })?;
        let (rows, _) = result.as_const_mut().ok_or(AdapterError::Unsupported(
            "deterministic mutation too large or complex for constant evaluation",
        ))?;
        let rows = rows.as_mut().map_err(|e| e.clone())?;
        consolidate(rows);
        for (row, diff) in rows.iter() {
            charge(&mut bytes, row, limit)?;
            if row.iter().count() != stmt.desc.arity() {
                return Err(changed(stmt.generation));
            }
            if diff.is_positive() {
                for (idx, (datum, typ)) in row.iter().zip(stmt.desc.iter_types()).enumerate() {
                    stmt.desc.constraints_met(idx, &datum)?;
                    if !datum.is_instance_of_sql(typ) {
                        return Err(changed(stmt.generation));
                    }
                }
            }
        }
        if let Some(visible) = snapshot.get_mut(&stmt.generation) {
            visible.extend(rows.iter().cloned());
            consolidate(visible);
        }
        let (generation, output) = writes
            .entry(stmt.id)
            .or_insert_with(|| (stmt.generation, Rows::new()));
        if *generation != stmt.generation {
            return Err(changed(stmt.id));
        }
        output.append(rows);
        consolidate(output);
    }
    Ok(CertifiedWrites { write_ts, writes })
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprRelationType, SqlScalarType};
    use mz_sql::plan::MutationKind;

    use super::*;

    #[mz_ore::test]
    fn rejects_session_and_logical_time_even_in_unselected_branches() {
        for f in [
            mz_expr::UnmaterializableFunc::CurrentUser,
            mz_expr::UnmaterializableFunc::MzNow,
        ] {
            let expr = MirScalarExpr::If {
                cond: Box::new(MirScalarExpr::literal_true()),
                then: Box::new(MirScalarExpr::column(0)),
                els: Box::new(MirScalarExpr::CallUnmaterializable(f)),
            };
            assert!(validate_scalar(&expr).is_err());
        }
    }

    #[mz_ore::test]
    fn dependent_copy_survives_source_cancellation_and_retries_from_scratch() {
        let a = GlobalId::User(1);
        let b = GlobalId::User(2);
        let desc = RelationDesc::builder()
            .with_column("x", SqlScalarType::Int64.nullable(false))
            .finish();
        let typ = ReprRelationType::from(desc.typ());
        let row = |n| Row::pack_slice(&[Datum::Int64(n)]);
        let source = MirRelationExpr::global_get(a, typ.clone());
        let stmt = |id, generation, mutation| DeterministicMutation {
            id: CatalogItemId::User(id),
            generation,
            mutation,
            desc: desc.clone(),
            inputs: BTreeMap::new(),
        };
        let statements = [
            stmt(
                1,
                a,
                MirRelationExpr::Constant {
                    rows: Ok(vec![(row(7), Diff::ONE)]),
                    typ,
                },
            ),
            stmt(2, b, source.clone()),
            stmt(
                1,
                a,
                apply_mutation_to_mir(source, &MutationKind::Delete, &BTreeMap::new()),
            ),
        ];
        for (target, base) in [(9, vec![]), (10, vec![(row(8), Diff::from(2))])] {
            let output = evaluate(
                &statements,
                BTreeMap::from([(a, base.clone())]),
                Timestamp::new(target),
                &Default::default(),
                u64::MAX,
            )
            .unwrap();
            assert_eq!(output.write_ts, Timestamp::new(target));
            let mut copied = base.clone();
            copied.push((row(7), Diff::ONE));
            consolidate(&mut copied);
            assert_eq!(output.writes[&CatalogItemId::User(2)], (b, copied));
            assert_eq!(
                output.writes[&CatalogItemId::User(1)],
                (a, base.into_iter().map(|(r, d)| (r, -d)).collect())
            );
        }
    }
}
