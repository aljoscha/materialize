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

use mz_catalog::memory::objects::{CatalogItem, Table, TableDataSource};
use mz_expr::visit::Visit;
use mz_expr::{CollectionPlan, Id, LocalId, MirRelationExpr, MirScalarExpr, UnaryFunc};
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, ReprRelationType};
use mz_sql::plan::ReadThenWritePlan;

use crate::catalog::Catalog;
use crate::error::AdapterError;
use crate::frontend_read_then_write::apply_mutation_to_mir;

#[derive(Debug)]
pub(crate) struct DeclarativeMutation {
    id: CatalogItemId,
    generation: GlobalId,
    desc: RelationDesc,
    inputs: BTreeMap<CatalogItemId, GlobalId>,
    mutation: MirRelationExpr,
}

impl DeclarativeMutation {
    /// Use the statement's authorized catalog snapshot, including prior staged DDL.
    pub(crate) fn prepare(
        catalog: &Catalog,
        plan: ReadThenWritePlan,
    ) -> Result<Self, AdapterError> {
        if !plan.returning.is_empty() || !plan.finishing.is_trivial(plan.selection.arity()) {
            return Err(AdapterError::Unsupported(
                "RETURNING or nontrivial finishing in declarative mutations",
            ));
        }
        if plan.selection.contains_unmaterializable() {
            return Err(AdapterError::Unsupported(
                "unstable expressions in declarative mutations",
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
                    "declarative mutations on relations other than writable user tables",
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
                "this relational operator in declarative mutations",
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
    let err =
        || AdapterError::Unsupported("unstable or unbounded expressions in declarative mutations");
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

fn validate_generations(statements: &[DeclarativeMutation]) -> Result<(), AdapterError> {
    let mut generations = BTreeMap::new();
    for s in statements {
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
    Ok(())
}

/// Compile cumulative signed writes at a common input timestamp. These collections
/// can contain retractions and cannot be peeked as ordinary result rows.
pub(crate) fn compile_table_diffs(
    mutations: &[DeclarativeMutation],
) -> Result<BTreeMap<(CatalogItemId, GlobalId), MirRelationExpr>, AdapterError> {
    validate_generations(mutations)?;
    // Each prefix is a Let binding. Inlining it at every read would double the
    // size of the plan at each successive INSERT INTO t SELECT * FROM t.
    let mut fresh = mz_ore::id_gen::IdGen::default();
    let mut bindings = Vec::new();
    let mut latest: BTreeMap<GlobalId, (LocalId, MirRelationExpr)> = BTreeMap::new();
    let mut targets = BTreeSet::new();
    for s in mutations {
        let mut delta = s.mutation.clone();
        mz_transform::normalize_lets::renumber_bindings(&mut delta, &mut fresh)
            .map_err(crate::optimize::OptimizerError::from)?;
        let mut deps = BTreeSet::new();
        // Postorder leaves the original global Get inside the union untouched.
        let inputs: BTreeSet<_> = s.inputs.values().copied().collect();
        delta.try_visit_mut_post(&mut |e: &mut MirRelationExpr| {
            if let MirRelationExpr::Get {
                id: Id::Global(gid),
                ..
            } = e
            {
                if !inputs.contains(gid) {
                    return Err(changed(*gid));
                }
                if let Some((id, prefix)) = latest.get(gid) {
                    deps.insert(*id);
                    *e = e.take_dangerous().union(prefix.clone());
                }
            }
            Ok(())
        })?;
        delta = check_write_columns(s, delta)?;
        if let Some((id, prefix)) = latest.get(&s.generation) {
            deps.insert(*id);
            delta = prefix.clone().union(delta);
        }
        let id = LocalId::new(fresh.allocate_id());
        latest.insert(
            s.generation,
            (id, MirRelationExpr::local_get(id, delta.typ())),
        );
        bindings.push((id, delta, deps));
        targets.insert((s.id, s.generation));
    }

    Ok(targets
        .into_iter()
        .map(|key| {
            let (root, mut body) = latest[&key.1].clone();
            let mut needed = BTreeSet::from([root]);
            for (id, value, deps) in bindings.iter().rev() {
                if needed.remove(id) {
                    needed.extend(deps);
                    body = MirRelationExpr::Let {
                        id: *id,
                        value: Box::new(value.clone()),
                        body: Box::new(body),
                    };
                }
            }
            (key, body)
        })
        .collect())
}

fn check_write_columns(
    stmt: &DeclarativeMutation,
    delta: MirRelationExpr,
) -> Result<MirRelationExpr, AdapterError> {
    use mz_expr::func::variadic::ErrorIfNull;
    use mz_repr::{Datum, NotNullViolation, ReprScalarType};

    let typ = delta.typ();
    let target = ReprRelationType::from(stmt.desc.typ());
    if typ.arity() != target.arity() {
        return Err(changed(stmt.generation));
    }
    let mut outputs: Vec<_> = (0..typ.arity()).collect();
    let mut checks = Vec::new();
    for (i, (src, dst)) in typ
        .column_types
        .iter()
        .zip(&target.column_types)
        .enumerate()
    {
        if src.scalar_type != dst.scalar_type {
            return Err(changed(stmt.generation));
        }
        // Check before accumulating writes: a later DELETE could hide a bad insert.
        // Retractions are old target rows, which already satisfy the constraint.
        if src.nullable && !dst.nullable {
            let msg = NotNullViolation(stmt.desc.get_name(i).clone()).to_string();
            outputs[i] = typ.arity() + checks.len();
            checks.push(MirScalarExpr::call_variadic(
                ErrorIfNull,
                vec![
                    MirScalarExpr::column(i),
                    MirScalarExpr::literal_ok(Datum::String(&msg), ReprScalarType::String),
                ],
            ));
        }
    }
    if checks.is_empty() {
        Ok(delta)
    } else {
        Ok(delta.map(checks).project(outputs))
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprRelationType, SqlScalarType};
    use mz_sql::plan::MutationKind;

    use super::*;
    use crate::optimize::{OptimizerError, optimize_mir_constant};
    use differential_dataflow::consolidation::consolidate;
    use mz_repr::optimize::OptimizerFeatures;
    use mz_repr::{Diff, Row, Timestamp};
    use mz_transform::{TransformCtx, typecheck::empty_typechecking_context};

    type Rows = Vec<(Row, Diff)>;
    #[derive(Debug)]
    struct CertifiedWrites {
        write_ts: Timestamp,
        writes: BTreeMap<CatalogItemId, (GlobalId, Rows)>,
    }
    fn charge(bytes: &mut u64, row: &Row, limit: u64) -> Result<(), AdapterError> {
        *bytes = bytes.saturating_add(u64::try_from(row.byte_len().max(1)).unwrap_or(u64::MAX));
        if *bytes > limit {
            return Err(AdapterError::ResultSize(format!(
                "declarative transaction exceeds {limit} bytes"
            )));
        }
        Ok(())
    }

    fn evaluate(
        statements: &[DeclarativeMutation],
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
            let mut result =
                mz_transform::catch_unwind_optimize(|| -> Result<_, OptimizerError> {
                    optimize_mir_constant(expr, &mut ctx, true)
                })?;
            let (rows, _) = result.as_const_mut().ok_or(AdapterError::Unsupported(
                "declarative mutation too large or complex for constant evaluation",
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
    fn delete_does_not_hide_not_null_violation() {
        let id = CatalogItemId::User(1);
        let generation = GlobalId::User(1);
        let desc = RelationDesc::builder()
            .with_column("x", SqlScalarType::Int64.nullable(false))
            .finish();
        let target = ReprRelationType::from(desc.typ());
        let mut nullable = target.clone();
        nullable.column_types[0].nullable = true;
        let insert = DeclarativeMutation {
            id,
            generation,
            desc: desc.clone(),
            inputs: BTreeMap::new(),
            mutation: MirRelationExpr::Constant {
                rows: Ok(vec![(Row::pack_slice(&[Datum::Null]), Diff::ONE)]),
                typ: nullable,
            },
        };
        let delete = DeclarativeMutation {
            id,
            generation,
            desc: desc.clone(),
            inputs: BTreeMap::from([(id, generation)]),
            mutation: MirRelationExpr::global_get(generation, target).negate(),
        };
        let delta = compile_table_diffs(&[insert, delete])
            .unwrap()
            .remove(&(id, generation))
            .unwrap();
        let result = evaluate(
            &[DeclarativeMutation {
                id,
                generation,
                desc,
                inputs: BTreeMap::new(),
                mutation: delta,
            }],
            BTreeMap::from([(generation, Vec::new())]),
            Timestamp::new(1),
            &Default::default(),
            u64::MAX,
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("violates not-null constraint")
        );
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
        let stmt = |id, generation, mutation: MirRelationExpr| DeclarativeMutation {
            id: CatalogItemId::User(id),
            generation,
            inputs: mutation
                .depends_on()
                .into_iter()
                .map(|gid| (CatalogItemId::User(1), gid))
                .collect(),
            mutation,
            desc: desc.clone(),
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
        let compiled = compile_table_diffs(&statements).unwrap();
        assert_eq!(compiled.len(), 2);
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
            for (&(id, generation), delta) in &compiled {
                assert_eq!(delta.depends_on(), BTreeSet::from([a]));
                let output_stmt = DeclarativeMutation {
                    id,
                    generation,
                    mutation: delta.clone(),
                    desc: desc.clone(),
                    inputs: BTreeMap::new(),
                };
                let compiled_output = evaluate(
                    &[output_stmt],
                    BTreeMap::from([(a, base.clone())]),
                    output.write_ts,
                    &Default::default(),
                    u64::MAX,
                )
                .unwrap();
                assert_eq!(compiled_output.writes[&id], output.writes[&id]);
            }
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
