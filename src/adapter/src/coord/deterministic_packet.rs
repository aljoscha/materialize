// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A single arrangement can carry branches with unrelated SQL row types.

use std::collections::BTreeMap;
use std::ops::Range;

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, ReprRelationType, ReprScalarType, Row};

use crate::AdapterError;

/// The first column identifies the branch. Each branch keeps its own typed
/// slice, so writing a table never round trips values through JSON or text.
#[derive(Debug)]
pub(super) struct CandidateLayout {
    slices: Vec<Range<usize>>,
}

impl CandidateLayout {
    pub(super) fn pack(branches: Vec<MirRelationExpr>) -> (Self, MirRelationExpr) {
        let mut types = vec![ReprScalarType::Int64.nullable(false)];
        let slices: Vec<_> = branches
            .iter()
            .map(|expr| {
                let start = types.len();
                types.extend(
                    expr.typ()
                        .column_types
                        .into_iter()
                        .map(|t| t.scalar_type.nullable(true)),
                );
                start..types.len()
            })
            .collect();
        let inputs = branches
            .into_iter()
            .enumerate()
            .map(|(tag, expr)| {
                let arity = expr.arity();
                let mut scalars = vec![MirScalarExpr::literal_ok(
                    Datum::Int64(i64::try_from(tag).expect("branch fits i64")),
                    ReprScalarType::Int64,
                )];
                let mut outputs = vec![arity];
                for (column, typ) in types.iter().enumerate().skip(1) {
                    if slices[tag].contains(&column) {
                        outputs.push(column - slices[tag].start);
                    } else {
                        outputs.push(arity + scalars.len());
                        scalars.push(MirScalarExpr::literal_null(typ.scalar_type.clone()));
                    }
                }
                expr.map(scalars).project(outputs)
            })
            .collect();
        (
            Self { slices },
            MirRelationExpr::union_many(inputs, ReprRelationType::new(types)),
        )
    }

    pub(super) fn unpack(&self, row: &Row) -> Result<(usize, Row), AdapterError> {
        let invalid =
            || AdapterError::internal("deterministic commit", "invalid compute candidate tag");
        let mut datums = row.iter();
        let Some(Datum::Int64(tag)) = datums.next() else {
            return Err(invalid());
        };
        let tag = usize::try_from(tag).map_err(|_| invalid())?;
        let slice = self.slices.get(tag).ok_or_else(invalid)?;
        Ok((
            tag,
            Row::pack(datums.skip(slice.start - 1).take(slice.len())),
        ))
    }
}

/// Peeks reject negative multiplicities. Export positive and negative parts
/// separately so a delete carries the original row with positive multiplicity.
pub(super) fn table_branches(
    tables: BTreeMap<(CatalogItemId, GlobalId), MirRelationExpr>,
) -> (Vec<(CatalogItemId, GlobalId, Diff)>, Vec<MirRelationExpr>) {
    let mut info = Vec::new();
    let mut exprs = Vec::new();
    for ((id, generation), delta) in tables {
        info.push((id, generation, Diff::ONE));
        exprs.push(delta.clone().threshold());
        info.push((id, generation, -Diff::ONE));
        exprs.push(delta.negate().threshold());
    }
    (info, exprs)
}
