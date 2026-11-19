// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Install once, read at a closed timestamp, and publish only with an exact WAL CAS.

use super::*;

use crate::coord::deterministic_compute::DeterministicCompute;
use crate::coord::deterministic_packet::{CandidateLayout, table_branches};
use crate::coord::peek::PeekResponseUnary;
use crate::session::DeterministicBlock;
use futures::StreamExt;
use mz_ore::collections::CollectionExt;
use mz_repr::{Diff, Row};
use mz_sql::plan::{Params, Plan};

#[derive(Debug)]
struct Run {
    txn: PendingTxn,
    output: DeterministicCompute,
    layout: CandidateLayout,
    tables: Vec<(CatalogItemId, GlobalId, Diff)>,
    renames: catalog::command::SchemaRenameProgram,
    rbac_enabled: bool,
    revision: Option<u64>,
    attempt: usize,
}

#[derive(Debug)]
pub struct DeterministicReady {
    run: Run,
    write_ts: Timestamp,
    rows: Result<Vec<Row>, AdapterError>,
}

impl Coordinator {
    async fn deterministic_write_timestamp(&mut self) -> Timestamp {
        let ts = self
            .get_local_write_ts()
            .await
            .timestamp
            .max(self.catalog().current_upper().await);
        self.catalog()
            .advance_upper(ts)
            .await
            .unwrap_or_terminate("unable to advance catalog upper for deterministic commit");
        ts
    }

    pub(crate) async fn sequence_deterministic_commit(
        &mut self,
        txn: PendingTxn,
        block: DeterministicBlock,
    ) {
        let prepared = async {
            if self.controller.read_only() {
                return Err(AdapterError::ReadOnly);
            }
            if !self.catalog().shares_table_wal().await {
                return Err(AdapterError::Unsupported(
                    "deterministic transactions outside a demo WAL environment",
                ));
            }
            let session = txn.ctx.session();
            let cluster = self.catalog().active_cluster(session)?.id;
            let (tables, mut branches) = table_branches(
                crate::frontend_deterministic::compile_table_diffs(&block.mutations)?,
            );
            let catalog_branch = cluster_mir(&block.renames, self.catalog())?;
            branches.push(catalog_branch);
            // Independently lowered branches each start numbering local Gets
            // at zero. The optimizer requires unique bindings across the union.
            let mut ids = mz_ore::id_gen::IdGen::default();
            for branch in &mut branches {
                mz_transform::normalize_lets::renumber_bindings(branch, &mut ids)
                    .map_err(crate::optimize::OptimizerError::from)?;
            }
            let (layout, expr) = CandidateLayout::pack(branches);
            let ts = self.deterministic_write_timestamp().await;
            let as_of = ts.step_back().ok_or(AdapterError::Unsupported(
                "writing at the minimum timestamp",
            ))?;
            let output = self.install_deterministic_compute(cluster, expr, as_of)?;
            Ok((tables, layout, output, ts))
        }
        .await;
        match prepared {
            Ok((tables, layout, output, ts)) => self.read_deterministic_candidate(
                Run {
                    txn,
                    output,
                    layout,
                    tables,
                    renames: block.renames,
                    rbac_enabled: self.catalog().system_config().enable_rbac_checks(),
                    revision: block.catalog_revision,
                    attempt: 1,
                },
                ts,
            ),
            Err(e) => DdlCommitContext::Transaction(txn).finish(Err(e)),
        }
    }

    fn read_deterministic_candidate(&mut self, run: Run, write_ts: Timestamp) {
        let ts = write_ts.step_back().expect("nonzero write timestamp");
        let result = self.peek_deterministic_compute(
            &run.output,
            ts,
            None,
            Some(run.txn.ctx.session().vars().max_query_result_size()),
        );
        let mut stream = match result {
            Ok(stream) => stream,
            Err(e) => {
                self.finish_deterministic_compute(run.output);
                DdlCommitContext::Transaction(run.txn).finish(Err(e));
                return;
            }
        };
        let tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn(|| "deterministic-candidate-read", async move {
            let rows = async {
                let mut data = Vec::new();
                while let Some(response) = stream.next().await {
                    match response {
                        PeekResponseUnary::Rows(mut rows) => {
                            while let Some(row) = rows.next() {
                                data.push(row.to_owned());
                            }
                        }
                        PeekResponseUnary::Error(e) => return Err(e),
                        PeekResponseUnary::Canceled => return Err(AdapterError::Canceled),
                        PeekResponseUnary::DependencyDropped(d) => {
                            return Err(d.to_concurrent_dependency_drop());
                        }
                    }
                }
                Ok(data)
            }
            .await;
            tracing::info!(arrangement = %run.output.index_id(), attempt = run.attempt, %write_ts, "deterministic candidate read");
            if run.attempt == 1 {
                catalog_preparation_test_pause("deterministic_candidate_before_cas").await;
            }
            let _ = tx.send(Message::DeterministicCandidateReady(Box::new(
                DeterministicReady {
                    run,
                    write_ts,
                    rows,
                },
            )));
        });
    }

    pub(crate) async fn deterministic_candidate_ready(&mut self, ready: DeterministicReady) {
        let DeterministicReady {
            mut run,
            write_ts,
            rows,
        } = ready;
        let result = async {
            let rows = rows?;
            let mut catalog_rows = Vec::new();
            let mut diffs = mz_catalog::durable::TableDiffs::new();
            let storage = self.catalog().state().storage_metadata();
            for row in rows {
                let (branch, payload) = run.layout.unpack(&row)?;
                if branch == run.tables.len() {
                    catalog_rows.push(payload);
                } else {
                    let (id, generation, sign) = run.tables[branch];
                    if self
                        .catalog()
                        .try_get_entry(&id)
                        .map(|e| e.latest_global_id())
                        != Some(generation)
                    {
                        return Err(AdapterError::ConcurrentDependencyMutation {
                            dependency_id: id.to_string(),
                        });
                    }
                    diffs
                        .entry(storage.get_collection_shard(generation)?)
                        .or_default()
                        .push((payload, sign));
                }
            }
            // RBAC mode is a literal in the maintained program. Role and
            // grant rows are live inputs, but changing the mode invalidates it.
            if run.rbac_enabled != self.catalog().system_config().enable_rbac_checks() {
                return Err(AdapterError::DDLTransactionOutcomeChanged);
            }
            let (candidate, outcomes) = match catalog::command::cluster_rename::decode_candidate(
                &run.renames,
                catalog_rows,
            ) {
                Ok(candidate) => candidate,
                Err(_) if self.catalog().current_upper().await > write_ts => {
                    return Err(AdapterError::ReadThenWriteContention);
                }
                Err(e) => return Err(e),
            };
            let conn_id = run.txn.ctx.session().conn_id().clone();
            // A move or privilege change can invalidate the table identities or
            // permissions used at SQL bind time, even if the cluster read succeeds.
            if run
                .revision
                .is_some_and(|r| r != self.catalog().transient_revision())
            {
                if write_ts < self.catalog().current_upper().await {
                    return Err(AdapterError::ReadThenWriteContention);
                }
                return Err(AdapterError::DDLTransactionOutcomeChanged);
            }
            let conn = self
                .active_conns
                .get(&conn_id)
                .ok_or(AdapterError::Canceled)?;
            let guard = self
                .serialized_ddl
                .try_lock_owned()
                .map_err(|_| AdapterError::ReadThenWriteContention)?;
            let result = Arc::make_mut(&mut self.catalog)
                .transact_schema_rename_candidate(write_ts, Some(conn), candidate, diffs)
                .await;
            self.release_ddl_lock(guard);
            let TransactionResult {
                builtin_table_updates,
                catalog_updates,
                audit_events,
            } = result?;
            self.apply_local_write(write_ts).await;
            run.txn.ctx.session_mut().apply_write(write_ts);
            self.track_catalog_audit_events(Some(&conn_id), audit_events);
            let updates = self.builtin_table_update().execute(builtin_table_updates);
            self.finish_catalog_transaction(Some(&mut run.txn.ctx), updates, catalog_updates)
                .await;
            for notice in outcomes.into_iter().flat_map(|outcome| outcome.notices) {
                run.txn.ctx.session().add_notice(notice.into());
            }
            Ok(())
        }
        .await;
        if matches!(result, Err(AdapterError::ReadThenWriteContention)) && run.attempt < 32 {
            run.attempt += 1;
            let ts = self.deterministic_write_timestamp().await;
            self.read_deterministic_candidate(run, ts);
        } else {
            self.finish_deterministic_compute(run.output);
            DdlCommitContext::Transaction(run.txn).finish(result);
        }
    }
}

fn cluster_mir(
    program: &catalog::command::SchemaRenameProgram,
    catalog: &catalog::Catalog,
) -> Result<mz_expr::MirRelationExpr, AdapterError> {
    if program.is_empty() {
        return Ok(mz_expr::MirRelationExpr::constant(
            Vec::new(),
            mz_repr::ReprRelationType::empty(),
        ));
    }
    let sql = catalog::command::cluster_rename::candidate_sql(program, catalog.system_config())?;
    let trusted = catalog.for_system_session();
    let ast = mz_sql::parse::parse(&sql)
        .map_err(AdapterError::from)?
        .into_element()
        .ast;
    let (stmt, ids) = mz_sql::names::resolve(&trusted, ast)?;
    let (Plan::Select(plan), _) = mz_sql::plan::plan(None, &trusted, stmt, &Params::empty(), &ids)?
    else {
        return Err(AdapterError::internal(
            "deterministic commit",
            "expected a SELECT plan",
        ));
    };
    let mut expr = plan.source.lower(catalog.system_config(), None)?;
    if !plan.finishing.order_by.is_empty()
        || plan.finishing.limit.is_some()
        || plan.finishing.offset != 0
    {
        return Err(AdapterError::internal(
            "deterministic commit",
            "unexpected catalog query finishing",
        ));
    }
    expr = expr.project(plan.finishing.project);
    Ok(expr)
}
