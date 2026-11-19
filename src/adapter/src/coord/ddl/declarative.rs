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

use crate::command::Command;
use crate::coord::appends::WriteResult;
use crate::coord::declarative_packet::{CandidateLayout, table_branches};
use crate::frontend_read_then_write::CandidateSubscription;
use crate::session::DeclarativeBlock;
use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, Diff, Row};
use mz_sql::plan::{Params, Plan};
use tokio::sync::oneshot;

type TableWrites = BTreeMap<CatalogItemId, (GlobalId, Vec<(Row, Diff)>)>;

#[derive(Debug)]
enum AttemptInput {
    Certified(Result<Candidate, AdapterError>),
    Aborted(AdapterError),
}

#[derive(Debug)]
struct Candidate {
    writes: TableWrites,
    catalog: Vec<(Row, Diff)>,
    notices: Vec<crate::AdapterNotice>,
}

#[derive(Debug)]
pub struct MultiTableWriteAttempt {
    txn: PendingTxn,
    write_ts: Timestamp,
    sink_id: GlobalId,
    revision: Option<u64>,
    rbac_enabled: bool,
    candidate: AttemptInput,
    retry: Option<oneshot::Sender<(PendingTxn, WriteResult)>>,
}

fn decode_candidate(
    layout: &CandidateLayout,
    tables: &[(CatalogItemId, GlobalId)],
    rows: &[(Row, Diff)],
) -> Result<Candidate, AdapterError> {
    let mut catalog = Vec::new();
    let mut notices = BTreeMap::new();
    let invalid = || AdapterError::internal("declarative commit", "invalid candidate envelope");
    let mut writes: TableWrites = tables
        .iter()
        .map(|(id, gid)| (*id, (*gid, Vec::new())))
        .collect();
    for (row, diff) in rows {
        let (branch, payload) = layout.unpack(row)?;
        if branch == tables.len() {
            let [Datum::String(channel), data] = payload.unpack()[..] else {
                return Err(invalid());
            };
            match channel {
                "catalog" => catalog.push((Row::pack([data]), *diff)),
                "error" | "notice" if *diff == Diff::ONE => {
                    let json = mz_repr::adt::jsonb::JsonbRef::from_datum(data).to_serde_json();
                    let message = json["message"].as_str().ok_or_else(invalid)?.to_owned();
                    let code = json["code"].as_str().ok_or_else(invalid)?;
                    let code = tokio_postgres::error::SqlState::from_code(code);
                    let detail = json["detail"].as_str().map(str::to_owned);
                    let hint = json["hint"].as_str().map(str::to_owned);
                    if channel == "error" {
                        return Err(AdapterError::DataflowDiagnostic {
                            code,
                            message,
                            detail,
                            hint,
                        });
                    }
                    let step = json["step"].as_i64().ok_or_else(invalid)?;
                    if notices
                        .insert(
                            step,
                            crate::AdapterNotice::DataflowDiagnostic {
                                code,
                                message,
                                detail,
                                hint,
                            },
                        )
                        .is_some()
                    {
                        return Err(invalid());
                    }
                }
                _ => return Err(invalid()),
            }
        } else {
            writes
                .get_mut(&tables[branch].0)
                .expect("target branch")
                .1
                .push((payload, *diff));
        }
    }
    Ok(Candidate {
        writes,
        catalog,
        notices: notices.into_values().collect(),
    })
}

impl Coordinator {
    async fn declarative_write_timestamp(&mut self) -> Timestamp {
        let ts = self
            .get_local_write_ts()
            .await
            .timestamp
            .max(self.catalog().current_upper().await);
        self.catalog()
            .advance_upper(ts)
            .await
            .unwrap_or_terminate("unable to advance catalog upper for declarative commit");
        ts
    }

    pub(crate) async fn sequence_declarative_commit(
        &mut self,
        txn: PendingTxn,
        block: DeclarativeBlock,
    ) {
        let prepared = async {
            if self.controller.read_only() {
                return Err(AdapterError::ReadOnly);
            }
            if !self.catalog().shares_table_wal().await {
                return Err(AdapterError::Unsupported(
                    "declarative transactions outside a demo WAL environment",
                ));
            }
            let session = txn.ctx.session();
            let emit_plan = session.vars().emit_plan_insights_notice();
            let cluster = self.catalog().active_cluster(session)?.id;
            let (tables, mut branches) = table_branches(
                crate::frontend_declarative::compile_table_diffs(&block.mutations)?,
            );
            let ts = self.declarative_write_timestamp().await;
            let catalog_branch = cluster_mir(&block.renames, self.catalog(), u64::from(ts))?;
            branches.push(catalog_branch);
            // Independently lowered branches each start numbering local Gets
            // at zero. The optimizer requires unique bindings across the union.
            let mut ids = mz_ore::id_gen::IdGen::default();
            for branch in &mut branches {
                mz_transform::normalize_lets::renumber_bindings(branch, &mut ids)
                    .map_err(crate::optimize::OptimizerError::from)?;
            }
            let (layout, expr) = CandidateLayout::pack(branches);
            let as_of = ts.step_back().ok_or(AdapterError::Unsupported(
                "writing at the minimum timestamp",
            ))?;
            let owner = crate::active_compute_sink::ActiveSubscribeOwner::Session {
                conn_id: txn.ctx.session().conn_id().clone(),
                session_uuid: txn.ctx.session().uuid(),
            };
            let (output, rx, notice) = self
                .install_declarative_compute(cluster, expr, as_of, emit_plan, owner)
                .await?;
            if let Some(notice) = notice {
                txn.ctx
                    .session()
                    .add_notice(crate::AdapterNotice::PlanInsights(notice));
            }
            Ok((tables, layout, output, rx, ts))
        }
        .await;
        let (tables, layout, sink_id, mut rx, mut write_ts) = match prepared {
            Ok(p) => p,
            Err(e) => return DdlCommitContext::Transaction(txn).finish(Err(e)),
        };
        // Only pre-bound DML depends on the preview namespace and catalog
        // authorization snapshot. A catalog-only program resolves its names,
        // checks privileges, and discovers dependencies in the dataflow at T.
        let binding_revision = if block.mutations.is_empty() {
            None
        } else {
            block.catalog_revision
        };
        let max_retries = self.catalog().system_config().max_occ_retries();
        let max_size = self
            .catalog()
            .system_config()
            .max_result_size()
            .min(txn.ctx.session().vars().max_query_result_size());
        let rbac_enabled = self.catalog().system_config().enable_rbac_checks();
        let timeout = *txn.ctx.session().vars().statement_timeout();
        let deadline = (!timeout.is_zero()).then(|| tokio::time::Instant::now() + timeout);
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        self.connection_cancel_watches.insert(
            txn.ctx.session().conn_id().clone(),
            (cancel_tx, cancel_rx.clone()),
        );
        let tx = self.internal_cmd_tx.clone();
        let mut txn = txn;
        mz_ore::task::spawn(|| "declarative-subscribe-occ", async move {
            let mut state = CandidateSubscription::new(write_ts, max_size);
            for attempt in 0..=max_retries {
                let read = async {
                    state.advance_to(write_ts)?;
                    while state.certified_rows().is_none() {
                        state.push(rx.recv().await)?;
                    }
                    let rows = state.certified_rows().expect("certified");
                    tracing::info!(subscribe = %sink_id, attempt = attempt + 1, %write_ts, "declarative candidate read");
                    if attempt == 0 {
                        catalog_preparation_test_pause("declarative_candidate_before_cas").await;
                    }
                    Ok::<_, AdapterError>(decode_candidate(&layout, &tables, rows))
                };
                let timeout = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                let candidate = tokio::select! {
                    biased;
                    _ = cancel_rx.changed() => AttemptInput::Aborted(AdapterError::Canceled),
                    _ = timeout => AttemptInput::Aborted(AdapterError::StatementTimeout),
                    result = read => match result {
                        Ok(rows) => AttemptInput::Certified(rows),
                        Err(error) => AttemptInput::Aborted(error),
                    }
                };
                let (result_tx, result_rx) = oneshot::channel();
                let request = MultiTableWriteAttempt {
                    txn,
                    sink_id,
                    candidate,
                    write_ts,
                    rbac_enabled,
                    revision: binding_revision,
                    retry: (attempt < max_retries).then_some(result_tx),
                };
                if tx
                    .send(Message::Command(
                        OpenTelemetryContext::obtain(),
                        Command::AttemptMultiTableTimestampedWrite(Box::new(request)),
                    ))
                    .is_err()
                {
                    return;
                }
                // Only a definitive refusal returns the context for another try.
                let Ok((
                    context,
                    WriteResult::TimestampPassed {
                        next_eligible_timestamp,
                        ..
                    },
                )) = result_rx.await
                else {
                    return;
                };
                txn = context;
                write_ts = next_eligible_timestamp;
            }
        });
    }

    pub(crate) async fn handle_attempt_multi_table_timestamped_write(
        &mut self,
        request: MultiTableWriteAttempt,
    ) {
        let MultiTableWriteAttempt {
            mut txn,
            write_ts,
            sink_id,
            revision,
            rbac_enabled,
            candidate,
            retry,
        } = request;
        let certified = matches!(candidate, AttemptInput::Certified(_));
        let result = async {
            if self.controller.read_only() {
                return Err(AdapterError::ReadOnly);
            }
            let conn_id = txn.ctx.session().conn_id().clone();
            if !self.active_conns.contains_key(&conn_id) {
                return Err(AdapterError::Canceled);
            }
            if rbac_enabled != self.catalog().system_config().enable_rbac_checks() {
                return Err(AdapterError::DDLTransactionOutcomeChanged);
            }
            if self
                .connection_cancel_watches
                .get(&conn_id)
                .is_none_or(|(_, watch)| *watch.borrow())
            {
                return Err(AdapterError::Canceled);
            }
            let candidate = match candidate {
                AttemptInput::Aborted(error) => return Err(error),
                AttemptInput::Certified(Err(_))
                    if self.catalog().current_upper().await > write_ts =>
                {
                    return Err(AdapterError::ReadThenWriteContention);
                }
                AttemptInput::Certified(rows) => rows?,
            };
            if revision.is_some_and(|r| r != self.catalog().transient_revision()) {
                if self.catalog().current_upper().await > write_ts {
                    return Err(AdapterError::ReadThenWriteContention);
                }
                return Err(AdapterError::DDLTransactionOutcomeChanged);
            }
            let storage = self.catalog().state().storage_metadata();
            let mut diffs = mz_catalog::durable::TableDiffs::new();
            for (id, (gid, rows)) in candidate.writes {
                if self
                    .catalog()
                    .try_get_entry(&id)
                    .map(|e| e.latest_global_id())
                    != Some(gid)
                {
                    return Err(AdapterError::ConcurrentDependencyMutation {
                        dependency_id: id.to_string(),
                    });
                }
                diffs.insert(storage.get_collection_shard(gid)?, rows);
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
                .transact_catalog_diffs_candidate(write_ts, Some(conn), candidate.catalog, diffs)
                .await;
            self.release_ddl_lock(guard);
            let TransactionResult {
                builtin_table_updates,
                catalog_updates,
                audit_events,
            } = result?;
            self.apply_local_write(write_ts).await;
            txn.ctx.session_mut().apply_write(write_ts);
            self.track_catalog_audit_events(Some(&conn_id), audit_events);
            let updates = self.builtin_table_update().execute(builtin_table_updates);
            self.finish_catalog_transaction(Some(&mut txn.ctx), updates, catalog_updates)
                .await;
            for notice in candidate.notices {
                txn.ctx.session().add_notice(notice);
            }
            Ok(())
        }
        .await;
        if certified
            && matches!(result, Err(AdapterError::ReadThenWriteContention))
            && let Some(retry) = retry
        {
            let next_eligible_timestamp = self.catalog().current_upper().await;
            if next_eligible_timestamp > write_ts {
                self.catalog()
                    .advance_upper(next_eligible_timestamp)
                    .await
                    .unwrap_or_terminate("unable to sync catalog for declarative retry");
                match retry.send((
                    txn,
                    WriteResult::TimestampPassed {
                        target_timestamp: write_ts,
                        next_eligible_timestamp,
                    },
                )) {
                    Ok(()) => return,
                    Err((context, _)) => txn = context,
                }
            }
        }
        self.connection_cancel_watches
            .remove(txn.ctx.session().conn_id());
        self.finish_declarative_compute(sink_id).await;
        DdlCommitContext::Transaction(txn).finish(result);
    }
}

fn cluster_mir(
    program: &catalog::command::SchemaRenameProgram,
    catalog: &catalog::Catalog,
    audit_time: u64,
) -> Result<mz_expr::MirRelationExpr, AdapterError> {
    if program.is_empty() {
        return Ok(mz_expr::MirRelationExpr::constant(
            Vec::new(),
            mz_repr::ReprRelationType::empty(),
        ));
    }
    let sql = catalog::command::cluster_rename::candidate_sql(
        program,
        catalog.system_config(),
        audit_time,
    )?;
    let trusted = catalog.for_system_session();
    let ast = mz_sql::parse::parse(&sql)
        .map_err(AdapterError::from)?
        .into_element()
        .ast;
    let (stmt, ids) = mz_sql::names::resolve(&trusted, ast)?;
    let (Plan::Select(plan), _) = mz_sql::plan::plan(None, &trusted, stmt, &Params::empty(), &ids)?
    else {
        return Err(AdapterError::internal(
            "declarative commit",
            "expected a SELECT plan",
        ));
    };
    let mut expr = plan.source.lower(catalog.system_config(), None)?;
    if !plan.finishing.order_by.is_empty()
        || plan.finishing.limit.is_some()
        || plan.finishing.offset != 0
    {
        return Err(AdapterError::internal(
            "declarative commit",
            "unexpected catalog query finishing",
        ));
    }
    expr = expr.project(plan.finishing.project);
    // SQL names the sign as a column. Turn it into differential multiplicity
    // inside the dataflow, before any updates reach the subscription.
    let mut ids = mz_ore::id_gen::IdGen::default();
    mz_transform::normalize_lets::renumber_bindings(&mut expr, &mut ids)
        .map_err(crate::optimize::OptimizerError::from)?;
    expr.let_in(&mut ids, |_, get| {
        use mz_expr::MirScalarExpr;
        let branch = |sign| {
            get.clone()
                .filter([MirScalarExpr::column(1).call_binary(
                    MirScalarExpr::literal_ok(Datum::Int64(sign), mz_repr::ReprScalarType::Int64),
                    mz_expr::func::Eq,
                )])
                .project(vec![0, 2])
        };
        Ok(branch(1).union(branch(-1).negate()))
    })
}
