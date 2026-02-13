// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use itertools::{Either, Itertools};
use mz_adapter_types::dyncfgs::CONSTRAINT_BASED_TIMESTAMP_SELECTION;
use mz_adapter_types::timestamp_selection::ConstraintBasedTimestampSelection;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_ore::{soft_assert_eq_or_log, soft_assert_or_log, soft_panic_or_log};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, GlobalId, IntoRowIterator, RelationDesc, Timestamp};
use mz_sql::ast::Raw;
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::Params;
use mz_sql::plan::{self, Explainee, ExplaineeStatement, Plan, QueryWhen};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::{CopyDirection, CopyRelation, ExplainStage, ShowStatement, Statement};
use mz_storage_types::sources::Timeline;
use mz_transform::EmptyStatisticsOracle;
use mz_transform::dataflow::DataflowMetainfo;
use opentelemetry::trace::TraceContextExt;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{Span, debug};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use mz_catalog::memory::objects::CatalogItem;

use crate::catalog::{Catalog, CatalogState};
use crate::command::Command;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::coord::sequencer::{eval_copy_to_uri, statistics_oracle};
use crate::coord::timeline::timedomain_for;
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{
    Coordinator, CopyToContext, ExecuteContextGuard, ExplainContext, ExplainPlanContext,
    TargetCluster,
};
use crate::explain::insights::PlanInsightsContext;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::{Optimize, OptimizerError};
use crate::peek_client::PlanCacheEntry;
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::statement_logging::WatchSetCreation;
use crate::statement_logging::{StatementEndedExecutionReason, StatementLifecycleEvent};
use crate::{
    AdapterError, AdapterNotice, CollectionIdBundle, ExecuteResponse, PeekClient, ReadHolds,
    TimelineContext, TimestampContext, TimestampProvider, optimize,
};
use crate::{coord, metrics};

/// Extract the target ReadHold from a ReadHolds struct for a fast-path peek plan.
/// This removes the appropriate hold from the ReadHolds (compute hold for PeekExisting,
/// storage hold for PeekPersist) and returns it. For Constant plans, returns None.
fn extract_target_read_hold(
    peek_plan: &PeekPlan,
    compute_instance: ComputeInstanceId,
    read_holds: &mut ReadHolds<Timestamp>,
) -> Option<mz_storage_types::read_holds::ReadHold<Timestamp>> {
    match peek_plan {
        PeekPlan::FastPath(FastPathPlan::PeekExisting(_coll_id, idx_id, _, _)) => Some(
            read_holds
                .compute_holds
                .remove(&(compute_instance, *idx_id))
                .expect("missing compute read hold for PeekExisting"),
        ),
        PeekPlan::FastPath(FastPathPlan::PeekPersist(coll_id, _, _)) => Some(
            read_holds
                .storage_holds
                .remove(coll_id)
                .expect("missing storage read hold for PeekPersist"),
        ),
        _ => None,
    }
}

impl PeekClient {
    /// Fast path that bypasses declare/portal for plan-cached queries.
    ///
    /// Called directly from the pgwire simple query path when the plan cache
    /// has this SQL. Handles statement logging, query metrics, and delegates
    /// to `try_frontend_peek_cached`. Returns `Ok(Some(response))` if handled.
    pub(crate) async fn try_cached_peek_direct(
        &mut self,
        session: &mut Session,
        catalog: Arc<Catalog>,
        cached: Arc<PlanCacheEntry>,
        params: &Params,
        logging: &Arc<qcell::QCell<crate::statement_logging::PreparedStatementLoggingInfo>>,
        lifecycle_timestamps: crate::session::LifecycleTimestamps,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        // Increment query_total metric using cached counter handle.
        let session_type = metrics::session_type_label_value(session.user());
        let statement_type = metrics::statement_type_label_value(&cached.parsed_stmt);
        self.query_total_counter(session_type, statement_type).inc();

        // Set up statement logging.
        let statement_logging_id = {
            let result = self.statement_logging_frontend.begin_statement_execution(
                session,
                params,
                logging,
                catalog.system_config(),
                Some(lifecycle_timestamps),
            );
            if let Some((logging_id, began_execution, mseh_update, prepared_statement)) = result {
                self.log_began_execution(began_execution, mseh_update, prepared_statement);
                Some(logging_id)
            } else {
                None
            }
        };

        let peek_start = Instant::now();

        let result = self
            .try_frontend_peek_cached(session, catalog, cached, statement_logging_id)
            .await;

        // Log end of execution (mirrors try_frontend_peek).
        if let Some(logging_id) = statement_logging_id {
            let reason = match &result {
                Ok(Some(ExecuteResponse::SendingRowsStreaming { .. })) => {
                    self.peek_outer_histogram()
                        .observe(peek_start.elapsed().as_secs_f64());
                    return result;
                }
                Ok(Some(resp @ ExecuteResponse::CopyTo { resp: inner, .. })) => {
                    match inner.as_ref() {
                        ExecuteResponse::SendingRowsStreaming { .. } => {
                            self.peek_outer_histogram()
                                .observe(peek_start.elapsed().as_secs_f64());
                            return result;
                        }
                        _ => resp.into(),
                    }
                }
                Ok(None) => {
                    soft_panic_or_log!(
                        "Bailed out from try_frontend_peek_cached after logging began"
                    );
                    self.log_ended_execution(
                        logging_id,
                        StatementEndedExecutionReason::Errored {
                            error: "Internal error: bailed out from try_frontend_peek_cached"
                                .to_string(),
                        },
                    );
                    self.peek_outer_histogram()
                        .observe(peek_start.elapsed().as_secs_f64());
                    return result;
                }
                Ok(Some(resp)) => resp.into(),
                Err(e) => StatementEndedExecutionReason::Errored {
                    error: e.to_string(),
                },
            };
            self.log_ended_execution(logging_id, reason);
        }

        self.peek_outer_histogram()
            .observe(peek_start.elapsed().as_secs_f64());
        result
    }

    /// Attempt to sequence a peek from the session task.
    ///
    /// Returns `Ok(Some(response))` if we handled the peek, or `Ok(None)` to fall back to the
    /// Coordinator's sequencing. If it returns an error, it should be returned to the user.
    ///
    /// `outer_ctx_extra` is Some when we are executing as part of an outer statement, e.g., a FETCH
    /// triggering the execution of the underlying query.
    pub(crate) async fn try_frontend_peek(
        &mut self,
        portal_name: &str,
        session: &mut Session,
        outer_ctx_extra: &mut Option<ExecuteContextGuard>,
        declare_catalog: Option<Arc<Catalog>>,
        declare_resolved_ids: Option<mz_sql::names::ResolvedIds>,
        declare_plan: Option<mz_sql::plan::Plan>,
        declare_sql: Option<Arc<str>>,
        declare_desc: Option<mz_sql::plan::StatementDesc>,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        // # From handle_execute

        if session.vars().emit_trace_id_notice() {
            let span_context = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .clone();
            if span_context.is_valid() {
                session.add_notice(AdapterNotice::QueryTrace {
                    trace_id: span_context.trace_id(),
                });
            }
        }

        // Reuse the catalog snapshot from declare() if available, avoiding a
        // redundant round-trip through the adapter command channel.
        let catalog = match declare_catalog {
            Some(catalog) => catalog,
            None => self.catalog_snapshot("try_frontend_peek").await,
        };

        // Extract things from the portal.
        let (stmt, params, logging, lifecycle_timestamps) = {
            if let Err(err) = Coordinator::verify_portal(&*catalog, session, portal_name) {
                outer_ctx_extra
                    .take()
                    .and_then(|guard| guard.defuse().retire());
                return Err(err);
            }
            let portal = session
                .get_portal_unverified(portal_name)
                // The portal is a session-level thing, so it couldn't have concurrently disappeared
                // since the above verification.
                .expect("called verify_portal above");
            let params = portal.parameters.clone();
            let stmt = portal.stmt.clone();
            let logging = Arc::clone(&portal.logging);
            let lifecycle_timestamps = portal.lifecycle_timestamps.clone();
            (stmt, params, logging, lifecycle_timestamps)
        };

        // Before planning, check if this is a statement type we can handle.
        // This must happen BEFORE statement logging setup to avoid orphaned execution records.
        if let Some(ref stmt) = stmt {
            match &**stmt {
                Statement::Select(_)
                | Statement::ExplainAnalyzeObject(_)
                | Statement::ExplainAnalyzeCluster(_)
                | Statement::Show(ShowStatement::ShowObjects(_))
                | Statement::Show(ShowStatement::ShowColumns(_)) => {
                    // These are always fine, just continue.
                    // Note: EXPLAIN ANALYZE will `plan` to `Plan::Select`.
                    // Note: ShowObjects plans to `Plan::Select`, ShowColumns plans to `Plan::ShowColumns`.
                    // We handle `Plan::ShowColumns` specially in `try_frontend_peek_inner`.
                }
                Statement::ExplainPlan(explain_stmt) => {
                    // Only handle ExplainPlan for SELECT statements.
                    // We don't want to handle e.g. EXPLAIN CREATE MATERIALIZED VIEW here, because that
                    // requires purification before planning, which the frontend peek sequencing doesn't
                    // do.
                    match &explain_stmt.explainee {
                        mz_sql_parser::ast::Explainee::Select(..) => {
                            // This is a SELECT, continue
                        }
                        _ => {
                            debug!(
                                "Bailing out from try_frontend_peek, because EXPLAIN is not for a SELECT query"
                            );
                            return Ok(None);
                        }
                    }
                }
                Statement::ExplainPushdown(explain_stmt) => {
                    // Only handle EXPLAIN FILTER PUSHDOWN for non-BROKEN SELECT statements
                    match &explain_stmt.explainee {
                        mz_sql_parser::ast::Explainee::Select(_, false) => {}
                        _ => {
                            debug!(
                                "Bailing out from try_frontend_peek, because EXPLAIN FILTER PUSHDOWN is not for a SELECT query or is for EXPLAIN BROKEN"
                            );
                            return Ok(None);
                        }
                    }
                }
                Statement::Copy(copy_stmt) => {
                    match &copy_stmt.direction {
                        CopyDirection::To => {
                            // Check for SUBSCRIBE inside COPY TO - we don't handle Plan::Subscribe
                            if matches!(&copy_stmt.relation, CopyRelation::Subscribe(_)) {
                                debug!(
                                    "Bailing out from try_frontend_peek, because COPY (SUBSCRIBE ...) TO is not supported"
                                );
                                return Ok(None);
                            }
                            // This is COPY TO (SELECT), continue
                        }
                        CopyDirection::From => {
                            debug!(
                                "Bailing out from try_frontend_peek, because COPY FROM is not supported"
                            );
                            return Ok(None);
                        }
                    }
                }
                _ => {
                    debug!(
                        "Bailing out from try_frontend_peek, because statement type is not supported"
                    );
                    return Ok(None);
                }
            }
        }

        // Set up statement logging, and log the beginning of execution.
        // (But only if we're not executing in the context of another statement.)
        let statement_logging_id = if outer_ctx_extra.is_none() {
            // This is a new statement, so begin statement logging
            let result = self.statement_logging_frontend.begin_statement_execution(
                session,
                &params,
                &logging,
                catalog.system_config(),
                lifecycle_timestamps,
            );

            if let Some((logging_id, began_execution, mseh_update, prepared_statement)) = result {
                self.log_began_execution(began_execution, mseh_update, prepared_statement);
                Some(logging_id)
            } else {
                None
            }
        } else {
            // We're executing in the context of another statement (e.g., FETCH),
            // so extract the statement logging ID from the outer context if present.
            // We take ownership and retire the outer context here. The end of execution will be
            // logged in one of the following ways:
            // - At the end of this function, if the execution is finished by then.
            // - Later by the Coordinator, either due to RegisterFrontendPeek or ExecuteSlowPathPeek.
            outer_ctx_extra
                .take()
                .and_then(|guard| guard.defuse().retire())
        };

        let peek_start = Instant::now();

        let result = self
            .try_frontend_peek_inner(
                session,
                catalog,
                stmt,
                params,
                statement_logging_id,
                declare_resolved_ids,
                declare_plan,
                declare_sql,
                declare_desc,
            )
            .await;

        // Log the end of execution if we are logging this statement and execution has already
        // ended.
        if let Some(logging_id) = statement_logging_id {
            let reason = match &result {
                // Streaming results are handled asynchronously by the coordinator
                Ok(Some(ExecuteResponse::SendingRowsStreaming { .. })) => {
                    // Don't log here - the peek is still executing.
                    // It will be logged when handle_peek_notification is called.
                    self.metrics()
                        .frontend_peek_seconds
                        .with_label_values(&["try_frontend_peek"])
                        .observe(peek_start.elapsed().as_secs_f64());
                    return result;
                }
                // COPY TO needs to check its inner response
                Ok(Some(resp @ ExecuteResponse::CopyTo { resp: inner, .. })) => {
                    match inner.as_ref() {
                        ExecuteResponse::SendingRowsStreaming { .. } => {
                            // Don't log here - the peek is still executing.
                            // It will be logged when handle_peek_notification is called.
                            self.metrics()
                                .frontend_peek_seconds
                                .with_label_values(&["try_frontend_peek"])
                                .observe(peek_start.elapsed().as_secs_f64());
                            return result;
                        }
                        // For non-streaming COPY TO responses, use the outer CopyTo for conversion
                        _ => resp.into(),
                    }
                }
                // Bailout case, which should not happen
                Ok(None) => {
                    soft_panic_or_log!(
                        "Bailed out from `try_frontend_peek_inner` after we already logged the beginning of statement execution."
                    );
                    // This statement will be handled by the old peek sequencing, which will do its
                    // own statement logging from the beginning. So, let's close out this one.
                    self.log_ended_execution(
                        logging_id,
                        StatementEndedExecutionReason::Errored {
                            error: "Internal error: bailed out from `try_frontend_peek_inner`"
                                .to_string(),
                        },
                    );
                    self.metrics()
                        .frontend_peek_seconds
                        .with_label_values(&["try_frontend_peek"])
                        .observe(peek_start.elapsed().as_secs_f64());
                    return result;
                }
                // All other success responses - use the From implementation
                // TODO(peek-seq): After we delete the old peek sequencing, we'll be able to adjust
                // the From implementation to do exactly what we need in the frontend peek
                // sequencing, so that the above special cases won't be needed.
                Ok(Some(resp)) => resp.into(),
                Err(e) => StatementEndedExecutionReason::Errored {
                    error: e.to_string(),
                },
            };

            self.log_ended_execution(logging_id, reason);
        }

        self.metrics()
            .frontend_peek_seconds
            .with_label_values(&["try_frontend_peek"])
            .observe(peek_start.elapsed().as_secs_f64());
        result
    }

    /// This is encapsulated in an inner function so that the outer function can still do statement
    /// logging after the `?` returns of the inner function.
    #[mz_ore::instrument(level = "debug")]
    async fn try_frontend_peek_inner(
        &mut self,
        session: &mut Session,
        catalog: Arc<Catalog>,
        stmt: Option<Arc<Statement<Raw>>>,
        params: Params,
        statement_logging_id: Option<crate::statement_logging::StatementLoggingId>,
        declare_resolved_ids: Option<mz_sql::names::ResolvedIds>,
        declare_plan: Option<mz_sql::plan::Plan>,
        declare_sql: Option<Arc<str>>,
        declare_desc: Option<mz_sql::plan::StatementDesc>,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        let inner_start = Instant::now();

        let stmt = match stmt {
            Some(stmt) => stmt,
            None => {
                debug!("try_frontend_peek_inner succeeded on an empty query");
                return Ok(Some(ExecuteResponse::EmptyQuery));
            }
        };

        session
            .metrics()
            .query_total(&[
                metrics::session_type_label_value(session.user()),
                metrics::statement_type_label_value(&stmt),
            ])
            .inc();

        // Plan cache fast path: if we have a cached optimization result for this SQL,
        // skip planning and optimization entirely and jump to timestamp determination
        // and peek execution.
        if let Some(sql_key) = &declare_sql {
            if let Some(cached) = self.plan_cache.get(sql_key).map(Arc::clone) {
                return self
                    .try_frontend_peek_cached(session, catalog, cached, statement_logging_id)
                    .await;
            }
        }

        // # From handle_execute_inner

        // Reuse the plan and resolved_ids from declare() if available, avoiding
        // redundant name resolution and planning (which each call plan_root_query).
        let (plan, resolved_ids, conn_catalog) = match (declare_plan, declare_resolved_ids) {
            (Some(plan), Some(resolved_ids)) => {
                // We have both the plan and resolved_ids from declare().
                // We still need a conn_catalog for RBAC and downstream processing.
                let conn_catalog = catalog.for_session(session);
                (plan, resolved_ids, conn_catalog)
            }
            _ => {
                // Fall back to the full resolve + plan path.
                let conn_catalog = catalog.for_session(session);
                let (resolved_stmt, resolved_ids) =
                    mz_sql::names::resolve(&conn_catalog, (*stmt).clone())?;
                let pcx = session.pcx();
                let plan = mz_sql::plan::plan(
                    Some(pcx),
                    &conn_catalog,
                    resolved_stmt,
                    &params,
                    &resolved_ids,
                )?;
                (plan, resolved_ids, conn_catalog)
            }
        };

        let (select_plan, explain_ctx, copy_to_ctx) = match &plan {
            Plan::Select(select_plan) => {
                let explain_ctx = if session.vars().emit_plan_insights_notice() {
                    let optimizer_trace = OptimizerTrace::new(ExplainStage::PlanInsights.paths());
                    ExplainContext::PlanInsightsNotice(optimizer_trace)
                } else {
                    ExplainContext::None
                };
                (select_plan, explain_ctx, None)
            }
            Plan::ShowColumns(show_columns_plan) => {
                // ShowColumns wraps a SelectPlan, extract it and proceed as normal.
                (&show_columns_plan.select_plan, ExplainContext::None, None)
            }
            Plan::ExplainPlan(plan::ExplainPlanPlan {
                stage,
                format,
                config,
                explainee: Explainee::Statement(ExplaineeStatement::Select { broken, plan, desc }),
            }) => {
                // Create OptimizerTrace to collect optimizer plans
                let optimizer_trace = OptimizerTrace::new(stage.paths());
                let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
                    broken: *broken,
                    config: config.clone(),
                    format: *format,
                    stage: *stage,
                    replan: None,
                    desc: Some(desc.clone()),
                    optimizer_trace,
                });
                (plan, explain_ctx, None)
            }
            // COPY TO S3
            Plan::CopyTo(plan::CopyToPlan {
                select_plan,
                desc,
                to,
                connection,
                connection_id,
                format,
                max_file_size,
            }) => {
                let uri = eval_copy_to_uri(to.clone(), session, catalog.state())?;

                // (output_batch_count will be set later)
                let copy_to_ctx = CopyToContext {
                    desc: desc.clone(),
                    uri,
                    connection: connection.clone(),
                    connection_id: *connection_id,
                    format: format.clone(),
                    max_file_size: *max_file_size,
                    output_batch_count: None,
                };

                (select_plan, ExplainContext::None, Some(copy_to_ctx))
            }
            Plan::ExplainPushdown(plan::ExplainPushdownPlan { explainee }) => {
                // Only handle EXPLAIN FILTER PUSHDOWN for SELECT statements
                match explainee {
                    plan::Explainee::Statement(plan::ExplaineeStatement::Select {
                        broken: false,
                        plan,
                        desc: _,
                    }) => {
                        let explain_ctx = ExplainContext::Pushdown;
                        (plan, explain_ctx, None)
                    }
                    _ => {
                        // This shouldn't happen because we already checked for this at the AST
                        // level before calling `try_frontend_peek_inner`.
                        soft_panic_or_log!(
                            "unexpected EXPLAIN FILTER PUSHDOWN plan kind in frontend peek sequencing: {:?}",
                            explainee
                        );
                        debug!(
                            "Bailing out from try_frontend_peek_inner, because EXPLAIN FILTER PUSHDOWN is not for a SELECT query or is EXPLAIN BROKEN"
                        );
                        return Ok(None);
                    }
                }
            }
            Plan::SideEffectingFunc(sef_plan) => {
                // Side-effecting functions need Coordinator state (e.g., active_conns),
                // so delegate to the Coordinator via a Command.
                // The RBAC check is performed in the Coordinator where active_conns is available.
                let response = self
                    .call_coordinator(|tx| Command::ExecuteSideEffectingFunc {
                        plan: sef_plan.clone(),
                        conn_id: session.conn_id().clone(),
                        current_role: session.role_metadata().current_role,
                        tx,
                    })
                    .await?;
                return Ok(Some(response));
            }
            _ => {
                // This shouldn't happen because we already checked for this at the AST
                // level before calling `try_frontend_peek_inner`.
                soft_panic_or_log!(
                    "Unexpected plan kind in frontend peek sequencing: {:?}",
                    plan
                );
                debug!(
                    "Bailing out from try_frontend_peek_inner, because the Plan is not a SELECT, side-effecting SELECT, EXPLAIN SELECT, EXPLAIN FILTER PUSHDOWN, or COPY TO S3"
                );
                return Ok(None);
            }
        };

        // # From sequence_plan

        // We have checked the plan kind above.
        assert!(plan.allowed_in_read_only());

        // Compute source_ids early so we can share them across multiple consumers
        // (auto_run_on_catalog_server, validate_timeline_context, check_log_reads, etc.)
        // instead of calling depends_on() independently in each.
        let source_ids = select_plan.source.depends_on();
        let could_run_expensive_function = select_plan.source.could_run_expensive_function();

        let target_cluster = match session.transaction().cluster() {
            // Use the current transaction's cluster.
            Some(cluster_id) => TargetCluster::Transaction(cluster_id),
            // If there isn't a current cluster set for a transaction, then try to auto route.
            None => coord::catalog_serving::auto_run_on_catalog_server_with_source_ids(
                &conn_catalog,
                session,
                &source_ids,
                could_run_expensive_function,
            ),
        };
        let (cluster, target_cluster_id, target_cluster_name) = {
            let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
            (cluster, cluster.id, &cluster.name)
        };

        // Log cluster selection
        if let Some(logging_id) = &statement_logging_id {
            self.log_set_cluster(*logging_id, target_cluster_id);
        }

        coord::catalog_serving::check_cluster_restrictions(
            target_cluster_name.as_str(),
            &conn_catalog,
            &plan,
        )?;

        rbac::check_plan(
            &conn_catalog,
            // We can't look at `active_conns` here, but that's ok, because this case was handled
            // above already inside `Command::ExecuteSideEffectingFunc`.
            None::<fn(u32) -> Option<RoleId>>,
            session,
            &plan,
            Some(target_cluster_id),
            &resolved_ids,
        )?;

        if let Some(wait_future) = coord::appends::waiting_on_startup_appends_with_source_ids(
            &*catalog,
            session,
            &source_ids,
        ) {
            wait_future.await;
        }

        let max_query_result_size = Some(session.vars().max_query_result_size());

        // # From sequence_peek

        // # From peek_validate

        let compute_instance_snapshot =
            ComputeInstanceSnapshot::new_without_collections(cluster.id());

        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster.id()).config.features())
            .override_from(&explain_ctx);

        if cluster.replicas().next().is_none() && explain_ctx.needs_cluster() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let (_, view_id) = self.transient_id_gen.allocate_id();
        let (_, index_id) = self.transient_id_gen.allocate_id();

        let mut optimizer = if let Some(mut copy_to_ctx) = copy_to_ctx {
            // COPY TO path: calculate output_batch_count and create copy_to optimizer
            let worker_counts = cluster.replicas().map(|r| {
                let loc = &r.config.location;
                loc.workers().unwrap_or_else(|| loc.num_processes())
            });
            let max_worker_count = match worker_counts.max() {
                Some(count) => u64::cast_from(count),
                None => {
                    return Err(AdapterError::NoClusterReplicasAvailable {
                        name: cluster.name.clone(),
                        is_managed: cluster.is_managed(),
                    });
                }
            };
            copy_to_ctx.output_batch_count = Some(max_worker_count);

            Either::Right(optimize::copy_to::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance_snapshot.clone(),
                view_id,
                copy_to_ctx,
                optimizer_config,
                self.optimizer_metrics.clone(),
            ))
        } else {
            // SELECT/EXPLAIN path: create peek optimizer
            Either::Left(optimize::peek::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance_snapshot.clone(),
                select_plan.finishing.clone(),
                view_id,
                index_id,
                optimizer_config,
                self.optimizer_metrics.clone(),
            ))
        };

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        // Fast path for validate_timeline_context: when the query depends on a single
        // Table or Source, we can return the timeline directly without creating BTreeSets,
        // Vecs, or traversing transitive dependencies.
        // TODO(peek-seq): validate_timeline_context can be expensive in real scenarios (not in
        // simple benchmarks), because it traverses transitive dependencies even of indexed views and
        // materialized views (also traversing their MIR plans).
        let mut timeline_context = if source_ids.len() == 1 {
            let gid = *source_ids.iter().next().unwrap();
            if let Some(item_id) = catalog.try_resolve_item_id(&gid) {
                let entry = catalog.state().get_entry(&item_id);
                match entry.item() {
                    CatalogItem::Table(table) => {
                        TimelineContext::TimelineDependent(table.timeline())
                    }
                    CatalogItem::Source(source) => {
                        TimelineContext::TimelineDependent(source.timeline.clone())
                    }
                    _ => catalog.validate_timeline_context(source_ids.iter().copied())?,
                }
            } else {
                catalog.validate_timeline_context(source_ids.iter().copied())?
            }
        } else {
            catalog.validate_timeline_context(source_ids.iter().copied())?
        };
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && select_plan.source.contains_temporal()?
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let notices = coord::sequencer::check_log_reads(
            &catalog,
            cluster,
            &source_ids,
            &mut target_replica,
            session.vars(),
        )?;
        session.add_notices(notices);

        // # From peek_linearize_timestamp

        let isolation_level = session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(&timeline_context);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, &select_plan.when);

        let oracle_read_ts = match timeline {
            Some(timeline) if needs_linearized_read_ts => {
                let oracle = self.ensure_oracle(timeline).await?;
                let read_ts_start = Instant::now();
                let ts = oracle.read_ts().await;
                self.metrics()
                    .frontend_peek_read_ts_seconds
                    .observe(read_ts_start.elapsed().as_secs_f64());
                Some(ts)
            }
            Some(_) | None => None,
        };

        // # From peek_real_time_recency

        let vars = session.vars();
        let real_time_recency_ts: Option<Timestamp> = if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            // Only call the coordinator when we actually need real-time recency
            self.call_coordinator(|tx| Command::DetermineRealTimeRecentTimestamp {
                source_ids: source_ids.clone(),
                real_time_recency_timeout: *vars.real_time_recency_timeout(),
                tx,
            })
            .await?
        } else {
            None
        };

        // # From peek_timestamp_read_hold

        let dataflow_builder = DataflowBuilder::new(catalog.state(), compute_instance_snapshot);
        let input_id_bundle = dataflow_builder.sufficient_collections(source_ids.clone());

        // ## From sequence_peek_timestamp

        // Warning: This will be false for AS OF queries, even if we are otherwise inside a
        // multi-statement transaction. (It's also false for FreshestTableWrite, which is currently
        // only read-then-write queries, which can't be part of multi-statement transactions, so
        // FreshestTableWrite doesn't matter.)
        //
        // TODO(peek-seq): It's not totally clear to me what the intended semantics are for AS OF
        // queries inside a transaction: We clearly can't use the transaction timestamp, but the old
        // peek sequencing still does a timedomain validation. The new peek sequencing does not do
        // timedomain validation for AS OF queries, which seems more natural. But I'm thinking that
        // it would be the cleanest to just simply disallow AS OF queries inside transactions.
        let in_immediate_multi_stmt_txn = session
            .transaction()
            .in_immediate_multi_stmt_txn(&select_plan.when);

        // Fetch or generate a timestamp for this query and fetch or acquire read holds.
        let (determination, mut read_holds) = match session
            .get_transaction_timestamp_determination()
        {
            // Use the transaction's timestamp if it exists and this isn't an AS OF query.
            // (`in_immediate_multi_stmt_txn` is false for AS OF queries.)
            Some(
                determination @ TimestampDetermination {
                    timestamp_context: TimestampContext::TimelineTimestamp { .. },
                    ..
                },
            ) if in_immediate_multi_stmt_txn => {
                // This is a subsequent (non-AS OF, non-constant) query in a multi-statement
                // transaction. We now:
                // - Validate that the query only accesses collections within the transaction's
                //   timedomain (which we know from the stored read holds).
                // - Use the transaction's stored timestamp determination.
                // - Use the (relevant subset of the) transaction's read holds.

                let txn_read_holds_opt = self
                    .call_coordinator(|tx| Command::GetTransactionReadHoldsBundle {
                        conn_id: session.conn_id().clone(),
                        tx,
                    })
                    .await;

                if let Some(txn_read_holds) = txn_read_holds_opt {
                    let allowed_id_bundle = txn_read_holds.id_bundle();
                    let outside = input_id_bundle.difference(&allowed_id_bundle);

                    // Queries without a timestamp and timeline can belong to any existing timedomain.
                    if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                        let valid_names =
                            allowed_id_bundle.resolve_names(&*catalog, session.conn_id());
                        let invalid_names = outside.resolve_names(&*catalog, session.conn_id());
                        return Err(AdapterError::RelationOutsideTimeDomain {
                            relations: invalid_names,
                            names: valid_names,
                        });
                    }

                    // Extract the subset of read holds for the collections this query accesses.
                    let read_holds = txn_read_holds.subset(&input_id_bundle);

                    (determination, read_holds)
                } else {
                    // This should never happen: we're in a subsequent query of a multi-statement
                    // transaction (we have a transaction timestamp), but the coordinator has no
                    // transaction read holds stored. This indicates a bug in the transaction
                    // handling.
                    return Err(AdapterError::Internal(
                        "Missing transaction read holds for multi-statement transaction"
                            .to_string(),
                    ));
                }
            }
            _ => {
                // There is no timestamp determination yet for this transaction. Either:
                // - We are not in a multi-statement transaction.
                // - This is the first (non-AS OF) query in a multi-statement transaction.
                // - This is an AS OF query.
                // - This is a constant query (`TimestampContext::NoTimestamp`).

                let timedomain_bundle;
                let determine_bundle = if in_immediate_multi_stmt_txn {
                    // This is the first (non-AS OF) query in a multi-statement transaction.
                    // Determine a timestamp that will be valid for anything in any schema
                    // referenced by the first query.
                    timedomain_bundle = timedomain_for(
                        &*catalog,
                        &dataflow_builder,
                        &source_ids,
                        &timeline_context,
                        session.conn_id(),
                        target_cluster_id,
                    )?;
                    &timedomain_bundle
                } else {
                    // Simply use the inputs of the current query.
                    &input_id_bundle
                };
                let (determination, read_holds) = self
                    .frontend_determine_timestamp(
                        catalog.state(),
                        session,
                        determine_bundle,
                        &select_plan.when,
                        target_cluster_id,
                        &timeline_context,
                        oracle_read_ts,
                        real_time_recency_ts,
                    )
                    .await?;

                // If this is the first (non-AS OF) query in a multi-statement transaction, store
                // the read holds in the coordinator, so subsequent queries can validate against
                // them.
                if in_immediate_multi_stmt_txn {
                    self.call_coordinator(|tx| Command::StoreTransactionReadHolds {
                        conn_id: session.conn_id().clone(),
                        read_holds: read_holds.clone(),
                        tx,
                    })
                    .await;
                }

                (determination, read_holds)
            }
        };

        {
            // Assert that we have a read hold for all the collections in our `input_id_bundle`.
            for id in input_id_bundle.iter() {
                let s = read_holds.storage_holds.contains_key(&id);
                let c = read_holds
                    .compute_ids()
                    .map(|(_instance, coll)| coll)
                    .contains(&id);
                soft_assert_or_log!(
                    s || c,
                    "missing read hold for collection {} in `input_id_bundle",
                    id
                );
            }

            // Assert that each part of the `input_id_bundle` corresponds to the right part of
            // `read_holds`.
            for id in input_id_bundle.storage_ids.iter() {
                soft_assert_or_log!(
                    read_holds.storage_holds.contains_key(id),
                    "missing storage read hold for collection {} in `input_id_bundle",
                    id
                );
            }
            for id in input_id_bundle
                .compute_ids
                .iter()
                .flat_map(|(_instance, colls)| colls)
            {
                soft_assert_or_log!(
                    read_holds
                        .compute_ids()
                        .map(|(_instance, coll)| coll)
                        .contains(id),
                    "missing compute read hold for collection {} in `input_id_bundle",
                    id,
                );
            }
        }

        // (TODO(peek-seq): The below TODO is copied from the old peek sequencing. We should resolve
        // this when we decide what to with `AS OF` in transactions.)
        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let requires_linearization = (&explain_ctx).into();
        // For implicit single-statement transactions (the common case for simple
        // SELECT queries via the simple query protocol), skip recording transaction
        // ops. The ops are only needed for multi-statement transactions to ensure
        // consistent timestamps across statements. For implicit single-statement
        // transactions, the transaction is immediately committed locally via
        // end_transaction_local() which just drops the ops without reading them.
        // This avoids cloning the entire TimestampDetermination (including
        // Antichain Vecs) on every query.
        if !session.transaction().is_implicit()
            || session.transaction().is_in_multi_statement_transaction()
        {
            let mut transaction_determination = determination.clone();
            if select_plan.when.is_transactional() {
                session.add_transaction_ops(TransactionOps::Peeks {
                    determination: transaction_determination,
                    cluster_id: target_cluster_id,
                    requires_linearization,
                })?;
            } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
                // If the query uses AS OF, then ignore the timestamp.
                transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
                session.add_transaction_ops(TransactionOps::Peeks {
                    determination: transaction_determination,
                    cluster_id: target_cluster_id,
                    requires_linearization,
                })?;
            }
        }

        // # From peek_optimize

        let stats = statistics_oracle(
            session,
            &source_ids,
            &determination.timestamp_context.antichain(),
            true,
            catalog.system_config(),
            &*self.storage_collections,
        )
        .await
        .unwrap_or_else(|_| Box::new(EmptyStatisticsOracle));

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let timestamp_context = determination.timestamp_context.clone();
        let session_meta = session.meta();
        let now = catalog.config().now.clone();
        let select_plan = select_plan.clone();
        let target_cluster_name = target_cluster_name.clone();
        let needs_plan_insights = explain_ctx.needs_plan_insights();
        let determination_for_pushdown = if matches!(explain_ctx, ExplainContext::Pushdown) {
            // This is a hairy data structure, so avoid this clone if we are not in
            // EXPLAIN FILTER PUSHDOWN.
            Some(determination.clone())
        } else {
            None
        };

        let span = Span::current();

        // Prepare data for plan insights if needed
        let catalog_for_insights = if needs_plan_insights {
            Some(Arc::clone(&catalog))
        } else {
            None
        };
        let mut compute_instances = BTreeMap::new();
        if needs_plan_insights {
            for user_cluster in catalog.user_clusters() {
                let snapshot = ComputeInstanceSnapshot::new_without_collections(user_cluster.id);
                compute_instances.insert(user_cluster.name.clone(), snapshot);
            }
        }

        // Enum for branching among various execution steps after optimization
        enum Execution {
            Peek {
                global_lir_plan: optimize::peek::GlobalLirPlan,
                optimization_finished_at: EpochMillis,
                plan_insights_optimizer_trace: Option<OptimizerTrace>,
                insights_ctx: Option<Box<PlanInsightsContext>>,
            },
            CopyToS3 {
                global_lir_plan: optimize::copy_to::GlobalLirPlan,
                source_ids: BTreeSet<GlobalId>,
            },
            ExplainPlan {
                df_meta: DataflowMetainfo,
                explain_ctx: ExplainPlanContext,
                optimizer: optimize::peek::Optimizer,
                insights_ctx: Option<Box<PlanInsightsContext>>,
            },
            ExplainPushdown {
                imports: BTreeMap<GlobalId, mz_expr::MapFilterProject>,
                determination: TimestampDetermination<Timestamp>,
            },
        }

        let source_ids_for_closure = source_ids.clone();
        let optimization_result = mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                span.in_scope(|| {
                    let _dispatch_guard = explain_ctx.dispatch_guard();

                    let raw_expr = select_plan.source.clone();

                    // The purpose of wrapping the following in a closure is to control where the
                    // `?`s return from, so that even when a `catch_unwind_optimize` call fails,
                    // we can still handle `EXPLAIN BROKEN`.
                    let pipeline = || -> Result<
                        Either<
                            optimize::peek::GlobalLirPlan,
                            optimize::copy_to::GlobalLirPlan,
                        >,
                        OptimizerError,
                    > {
                        match optimizer.as_mut() {
                            Either::Left(optimizer) => {
                                // SELECT/EXPLAIN path
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan =
                                    optimizer.catch_unwind_optimize(raw_expr.clone())?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan = local_mir_plan.resolve(
                                    timestamp_context.clone(),
                                    &session_meta,
                                    stats,
                                );
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan =
                                    optimizer.catch_unwind_optimize(local_mir_plan)?;
                                Ok(Either::Left(global_lir_plan))
                            }
                            Either::Right(optimizer) => {
                                // COPY TO path
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan =
                                    optimizer.catch_unwind_optimize(raw_expr.clone())?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan = local_mir_plan.resolve(
                                    timestamp_context.clone(),
                                    &session_meta,
                                    stats,
                                );
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan =
                                    optimizer.catch_unwind_optimize(local_mir_plan)?;
                                Ok(Either::Right(global_lir_plan))
                            }
                        }
                    };

                    let global_lir_plan_result = pipeline();
                    let optimization_finished_at = now();

                    let create_insights_ctx =
                        |optimizer: &optimize::peek::Optimizer,
                         is_notice: bool|
                         -> Option<Box<PlanInsightsContext>> {
                            if !needs_plan_insights {
                                return None;
                            }

                            let catalog = catalog_for_insights.as_ref()?;

                            let enable_re_optimize = if needs_plan_insights {
                                // Disable any plan insights that use the optimizer if we only want the
                                // notice and plan optimization took longer than the threshold. This is
                                // to prevent a situation where optimizing takes a while and there are
                                // lots of clusters, which would delay peek execution by the product of
                                // those.
                                //
                                // (This heuristic doesn't work well, see #9492.)
                                let dyncfgs = catalog.system_config().dyncfgs();
                                let opt_limit = mz_adapter_types::dyncfgs
                                ::PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION
                                .get(dyncfgs);
                                !(is_notice && optimizer.duration() > opt_limit)
                            } else {
                                false
                            };

                            Some(Box::new(PlanInsightsContext {
                                stmt: select_plan
                                    .select
                                    .as_deref()
                                    .map(Clone::clone)
                                    .map(Statement::Select),
                                raw_expr: raw_expr.clone(),
                                catalog: Arc::clone(catalog),
                                compute_instances,
                                target_instance: target_cluster_name,
                                metrics: optimizer.metrics().clone(),
                                finishing: optimizer.finishing().clone(),
                                optimizer_config: optimizer.config().clone(),
                                session: session_meta,
                                timestamp_context,
                                view_id: optimizer.select_id(),
                                index_id: optimizer.index_id(),
                                enable_re_optimize,
                            }))
                        };

                    match global_lir_plan_result {
                        Ok(Either::Left(global_lir_plan)) => {
                            // SELECT/EXPLAIN path
                            let optimizer = optimizer.unwrap_left();
                            match explain_ctx {
                                ExplainContext::Plan(explain_ctx) => {
                                    let (_, df_meta, _) = global_lir_plan.unapply();
                                    let insights_ctx = create_insights_ctx(&optimizer, false);
                                    Ok(Execution::ExplainPlan {
                                        df_meta,
                                        explain_ctx,
                                        optimizer,
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::None => Ok(Execution::Peek {
                                    global_lir_plan,
                                    optimization_finished_at,
                                    plan_insights_optimizer_trace: None,
                                    insights_ctx: None,
                                }),
                                ExplainContext::PlanInsightsNotice(optimizer_trace) => {
                                    let insights_ctx = create_insights_ctx(&optimizer, true);
                                    Ok(Execution::Peek {
                                        global_lir_plan,
                                        optimization_finished_at,
                                        plan_insights_optimizer_trace: Some(optimizer_trace),
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::Pushdown => {
                                    let (plan, _, _) = global_lir_plan.unapply();
                                    let imports = match plan {
                                        PeekPlan::SlowPath(plan) => plan
                                            .desc
                                            .source_imports
                                            .into_iter()
                                            .filter_map(|(id, source_import)| {
                                                source_import
                                                    .desc
                                                    .arguments
                                                    .operators
                                                    .map(|mfp| (id, mfp))
                                            })
                                            .collect(),
                                        PeekPlan::FastPath(_) => {
                                            std::collections::BTreeMap::default()
                                        }
                                    };
                                    Ok(Execution::ExplainPushdown {
                                        imports,
                                        determination: determination_for_pushdown
                                            .expect("it's present for the ExplainPushdown case"),
                                    })
                                }
                            }
                        }
                        Ok(Either::Right(global_lir_plan)) => {
                            // COPY TO S3 path
                            Ok(Execution::CopyToS3 {
                                global_lir_plan,
                                source_ids: source_ids_for_closure,
                            })
                        }
                        Err(err) => {
                            if optimizer.is_right() {
                                // COPY TO has no EXPLAIN BROKEN support
                                return Err(err);
                            }
                            // SELECT/EXPLAIN error handling
                            let optimizer = optimizer.expect_left("checked above");
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                if explain_ctx.broken {
                                    // EXPLAIN BROKEN: log error and continue with defaults
                                    tracing::error!(
                                        "error while handling EXPLAIN statement: {}",
                                        err
                                    );
                                    Ok(Execution::ExplainPlan {
                                        df_meta: Default::default(),
                                        explain_ctx,
                                        optimizer,
                                        insights_ctx: None,
                                    })
                                } else {
                                    Err(err)
                                }
                            } else {
                                Err(err)
                            }
                        }
                    }
                })
            },
        )
        .await
        .map_err(|optimizer_error| {
            AdapterError::Internal(format!("internal error in optimizer: {}", optimizer_error))
        })?;

        // Log optimization finished
        if let Some(logging_id) = &statement_logging_id {
            self.log_lifecycle_event(*logging_id, StatementLifecycleEvent::OptimizationFinished);
        }

        // Handle the optimization result: either generate EXPLAIN output or continue with execution
        let result = match optimization_result {
            Execution::ExplainPlan {
                df_meta,
                explain_ctx,
                optimizer,
                insights_ctx,
            } => {
                let rows = coord::sequencer::explain_plan_inner(
                    session,
                    &catalog,
                    df_meta,
                    explain_ctx,
                    optimizer,
                    insights_ctx,
                )
                .await?;

                Ok(Some(ExecuteResponse::SendingRowsImmediate {
                    rows: Box::new(rows.into_row_iter()),
                }))
            }
            Execution::ExplainPushdown {
                imports,
                determination,
            } => {
                // # From peek_explain_pushdown

                let as_of = determination.timestamp_context.antichain();
                let mz_now = determination
                    .timestamp_context
                    .timestamp()
                    .map(|t| ResultSpec::value(Datum::MzTimestamp(*t)))
                    .unwrap_or_else(ResultSpec::value_all);

                Ok(Some(
                    coord::sequencer::explain_pushdown_future_inner(
                        session,
                        &*catalog,
                        &self.storage_collections,
                        as_of,
                        mz_now,
                        imports,
                    )
                    .await
                    .await?,
                ))
            }
            Execution::Peek {
                global_lir_plan,
                optimization_finished_at: _optimization_finished_at,
                plan_insights_optimizer_trace,
                insights_ctx,
            } => {
                // Continue with normal execution
                // # From peek_finish

                let (peek_plan, df_meta, typ) = global_lir_plan.unapply();

                // Populate the plan cache for future queries with the same SQL text.
                if let (Some(sql_key), Some(desc)) = (&declare_sql, &declare_desc) {
                    // Pre-build the RelationDesc for the peek command at cache population time.
                    // This avoids per-query format!("peek_{i}") and RelationDesc::new() overhead.
                    let cols = (0..typ.arity()).map(|i| format!("peek_{i}"));
                    let result_desc = Arc::new(RelationDesc::new(typ.clone(), cols));
                    let single_compute_collection_ids = if input_id_bundle.compute_ids.len() == 1 {
                        let (_, ids) = input_id_bundle.compute_ids.first_key_value().unwrap();
                        Some(ids.iter().copied().collect::<Vec<_>>())
                    } else {
                        None
                    };
                    let relation_desc_for_response =
                        desc.relation_desc.as_ref().map(|rd| Arc::new(rd.clone()));
                    self.plan_cache.insert(
                        Arc::clone(sql_key),
                        Arc::new(PlanCacheEntry {
                            desc: desc.clone(),
                            peek_plan: peek_plan.clone(),
                            df_meta: df_meta.clone(),
                            typ: typ.clone(),
                            source_ids: source_ids.clone(),
                            finishing: select_plan.finishing.clone(),
                            when: select_plan.when.clone(),
                            input_id_bundle: input_id_bundle.clone(),
                            timeline_context: timeline_context.clone(),
                            target_cluster_id,
                            parsed_stmt: Arc::clone(&stmt),
                            result_desc,
                            single_compute_collection_ids,
                            relation_desc_for_response,
                        }),
                    );
                }

                coord::sequencer::emit_optimizer_notices(
                    &*catalog,
                    session,
                    &df_meta.optimizer_notices,
                );

                // Generate plan insights notice if needed
                if let Some(trace) = plan_insights_optimizer_trace {
                    let target_cluster = catalog.get_cluster(target_cluster_id);
                    let features = OptimizerFeatures::from(catalog.system_config())
                        .override_from(&target_cluster.config.features());
                    let insights = trace
                        .into_plan_insights(
                            &features,
                            &catalog.for_session(session),
                            Some(select_plan.finishing.clone()),
                            Some(target_cluster),
                            df_meta.clone(),
                            insights_ctx,
                        )
                        .await?;
                    session.add_notice(AdapterNotice::PlanInsights(insights));
                }

                // # Now back to peek_finish

                let watch_set = statement_logging_id.map(|logging_id| {
                    WatchSetCreation::new(
                        logging_id,
                        catalog.state(),
                        &input_id_bundle,
                        determination.timestamp_context.timestamp_or_default(),
                    )
                });

                let max_result_size = catalog.system_config().max_result_size();

                // Clone determination if we need it for emit_timestamp_notice, since it may be
                // moved into Command::ExecuteSlowPathPeek.
                let determination_for_notice = if session.vars().emit_timestamp_notice() {
                    Some(determination.clone())
                } else {
                    None
                };

                let response = match peek_plan {
                    PeekPlan::FastPath(fast_path_plan) => {
                        if let Some(logging_id) = &statement_logging_id {
                            // TODO(peek-seq): Actually, we should log it also for
                            // FastPathPlan::Constant. The only reason we are not doing so at the
                            // moment is to match the old peek sequencing, so that statement logging
                            // tests pass with the frontend peek sequencing turned both on and off.
                            //
                            // When the old sequencing is removed, we should make a couple of
                            // changes in how we log timestamps:
                            // - Move this up to just after timestamp determination, so that it
                            //   appears in the log as soon as possible.
                            // - Do it also for Constant peeks.
                            // - Currently, slow-path peeks' timestamp logging is done by
                            //   `implement_peek_plan`. We could remove it from there, and just do
                            //   it here.
                            if !matches!(fast_path_plan, FastPathPlan::Constant(..)) {
                                self.log_set_timestamp(
                                    *logging_id,
                                    determination.timestamp_context.timestamp_or_default(),
                                );
                            }
                        }

                        let row_set_finishing_seconds =
                            session.metrics().row_set_finishing_seconds().clone();

                        let peek_stash_read_batch_size_bytes =
                            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                                .get(catalog.system_config().dyncfgs());
                        let peek_stash_read_memory_budget_bytes =
                            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                                .get(catalog.system_config().dyncfgs());

                        // Build RelationDesc for the peek command (not cached on first execution).
                        let cols = (0..typ.arity()).map(|i| format!("peek_{i}"));
                        let result_desc = Arc::new(RelationDesc::new(typ, cols));
                        // Extract the target read hold from the ReadHolds struct
                        // to avoid passing the entire struct (with its BTreeMaps).
                        let target_hold = match &fast_path_plan {
                            FastPathPlan::PeekExisting(_coll_id, idx_id, _, _) => Some(
                                read_holds
                                    .compute_holds
                                    .remove(&(target_cluster_id, *idx_id))
                                    .expect("missing compute read hold for PeekExisting"),
                            ),
                            FastPathPlan::PeekPersist(coll_id, _, _) => Some(
                                read_holds
                                    .storage_holds
                                    .remove(coll_id)
                                    .expect("missing storage read hold for PeekPersist"),
                            ),
                            FastPathPlan::Constant(..) => None,
                        };
                        self.implement_fast_path_peek_plan(
                            fast_path_plan,
                            determination.timestamp_context.timestamp_or_default(),
                            select_plan.finishing,
                            target_cluster_id,
                            target_replica,
                            result_desc,
                            max_result_size,
                            max_query_result_size,
                            row_set_finishing_seconds,
                            target_hold,
                            peek_stash_read_batch_size_bytes,
                            peek_stash_read_memory_budget_bytes,
                            session.conn_id().clone(),
                            source_ids,
                            watch_set,
                        )
                        .await?
                    }
                    PeekPlan::SlowPath(dataflow_plan) => {
                        {
                            // Assert that we have some read holds for all the imports of the dataflow.
                            for id in dataflow_plan.desc.source_imports.keys() {
                                soft_assert_or_log!(
                                    read_holds.storage_holds.contains_key(id),
                                    "missing read hold for the source import {}",
                                    id
                                );
                            }
                            for id in dataflow_plan.desc.index_imports.keys() {
                                soft_assert_or_log!(
                                    read_holds
                                        .compute_ids()
                                        .map(|(_instance, coll)| coll)
                                        .contains(id),
                                    "missing read hold for the index import {}",
                                    id,
                                );
                            }

                            // Also check the holds against the as_of.
                            for (id, h) in read_holds.storage_holds.iter() {
                                let as_of = dataflow_plan
                                    .desc
                                    .as_of
                                    .clone()
                                    .expect("dataflow has an as_of")
                                    .into_element();
                                soft_assert_or_log!(
                                    h.since().less_equal(&as_of),
                                    "storage read hold at {:?} for collection {} is not enough for as_of {:?}",
                                    h.since(),
                                    id,
                                    as_of
                                );
                            }
                            for ((instance, id), h) in read_holds.compute_holds.iter() {
                                soft_assert_eq_or_log!(
                                    *instance,
                                    target_cluster_id,
                                    "the read hold on {} is on the wrong cluster",
                                    id
                                );
                                let as_of = dataflow_plan
                                    .desc
                                    .as_of
                                    .clone()
                                    .expect("dataflow has an as_of")
                                    .into_element();
                                soft_assert_or_log!(
                                    h.since().less_equal(&as_of),
                                    "compute read hold at {:?} for collection {} is not enough for as_of {:?}",
                                    h.since(),
                                    id,
                                    as_of
                                );
                            }
                        }

                        if let Some(logging_id) = &statement_logging_id {
                            self.log_set_transient_index_id(*logging_id, dataflow_plan.id);
                        }

                        self.call_coordinator(|tx| Command::ExecuteSlowPathPeek {
                            dataflow_plan: Box::new(dataflow_plan),
                            determination,
                            finishing: select_plan.finishing,
                            compute_instance: target_cluster_id,
                            target_replica,
                            intermediate_result_type: typ,
                            source_ids,
                            conn_id: session.conn_id().clone(),
                            max_result_size,
                            max_query_result_size,
                            watch_set,
                            tx,
                        })
                        .await?
                    }
                };

                // Add timestamp notice if emit_timestamp_notice is enabled
                if let Some(determination) = determination_for_notice {
                    let explanation = self
                        .call_coordinator(|tx| Command::ExplainTimestamp {
                            conn_id: session.conn_id().clone(),
                            session_wall_time: session.pcx().wall_time,
                            cluster_id: target_cluster_id,
                            id_bundle: input_id_bundle.clone(),
                            determination,
                            tx,
                        })
                        .await;
                    session.add_notice(AdapterNotice::QueryTimestamp { explanation });
                }

                Ok(Some(match select_plan.copy_to {
                    None => response,
                    // COPY TO STDOUT
                    Some(format) => ExecuteResponse::CopyTo {
                        format,
                        resp: Box::new(response),
                    },
                }))
            }
            Execution::CopyToS3 {
                global_lir_plan,
                source_ids,
            } => {
                let (df_desc, df_meta) = global_lir_plan.unapply();

                coord::sequencer::emit_optimizer_notices(
                    &*catalog,
                    session,
                    &df_meta.optimizer_notices,
                );

                // Extract S3 sink connection info for preflight check
                let sink_id = df_desc.sink_id();
                let sinks = &df_desc.sink_exports;
                if sinks.len() != 1 {
                    return Err(AdapterError::Internal(
                        "expected exactly one copy to s3 sink".into(),
                    ));
                }
                let (_, sink_desc) = sinks
                    .first_key_value()
                    .expect("known to be exactly one copy to s3 sink");
                let s3_sink_connection = match &sink_desc.connection {
                    mz_compute_types::sinks::ComputeSinkConnection::CopyToS3Oneshot(conn) => {
                        conn.clone()
                    }
                    _ => {
                        return Err(AdapterError::Internal(
                            "expected copy to s3 oneshot sink".into(),
                        ));
                    }
                };

                // Perform S3 preflight check in background task (via coordinator).
                // This runs slow S3 operations without blocking the coordinator's main task.
                self.call_coordinator(|tx| Command::CopyToPreflight {
                    s3_sink_connection,
                    sink_id,
                    tx,
                })
                .await?;

                // Preflight succeeded, now execute the actual COPY TO dataflow
                let watch_set = statement_logging_id.map(|logging_id| {
                    WatchSetCreation::new(
                        logging_id,
                        catalog.state(),
                        &input_id_bundle,
                        determination.timestamp_context.timestamp_or_default(),
                    )
                });

                let response = self
                    .call_coordinator(|tx| Command::ExecuteCopyTo {
                        df_desc: Box::new(df_desc),
                        compute_instance: target_cluster_id,
                        target_replica,
                        source_ids,
                        conn_id: session.conn_id().clone(),
                        watch_set,
                        tx,
                    })
                    .await?;

                Ok(Some(response))
            }
        };
        self.metrics()
            .frontend_peek_seconds
            .with_label_values(&["try_frontend_peek_inner"])
            .observe(inner_start.elapsed().as_secs_f64());
        result
    }

    /// (Similar to Coordinator::determine_timestamp)
    /// Determines the timestamp for a query, acquires read holds that ensure the
    /// query remains executable at that time, and returns those.
    /// The caller is responsible for eventually dropping those read holds.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn frontend_determine_timestamp(
        &mut self,
        catalog_state: &CatalogState,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {
        // this is copy-pasted from Coordinator

        // Ensure the instance ID string is cached (avoids per-query to_string() formatting).
        self.instance_id_str(compute_instance);

        let constraint_based = ConstraintBasedTimestampSelection::from_str(
            &CONSTRAINT_BASED_TIMESTAMP_SELECTION.get(catalog_state.system_config().dyncfgs()),
        );

        let isolation_level = session.vars().transaction_isolation();

        let (read_holds, upper) = self
            .acquire_read_holds_and_least_valid_write(id_bundle)
            .await
            .map_err(|err| {
                AdapterError::concurrent_dependency_drop_from_collection_lookup_error(
                    err,
                    compute_instance,
                )
            })?;
        // Only clone `upper` when we need it for the StrictSerializable comparison
        // below. For the common Serializable case, pass `upper` by move to avoid
        // an unnecessary Antichain (Vec) clone on every query.
        let needs_strict_serializable_comparison = isolation_level
            == &IsolationLevel::StrictSerializable
            && real_time_recency_ts.is_none();
        let upper_for_strict = if needs_strict_serializable_comparison {
            Some(upper.clone())
        } else {
            None
        };
        let (det, read_holds) = <Coordinator as TimestampProvider>::determine_timestamp_for_inner(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            &constraint_based,
            read_holds,
            upper,
        )?;

        session
            .metrics()
            .determine_timestamp(&[
                match det.respond_immediately() {
                    true => "true",
                    false => "false",
                },
                isolation_level.as_str(),
                &self.cached_instance_id_strs[&compute_instance],
                constraint_based.as_str(),
            ])
            .inc();
        if !det.respond_immediately() && needs_strict_serializable_comparison {
            // Note down the difference between StrictSerializable and Serializable into a metric.
            if let Some(strict) = det.timestamp_context.timestamp() {
                let (serializable_det, _tmp_read_holds) =
                    <Coordinator as TimestampProvider>::determine_timestamp_for_inner(
                        session,
                        id_bundle,
                        when,
                        compute_instance,
                        timeline_context,
                        oracle_read_ts,
                        real_time_recency_ts,
                        isolation_level,
                        &constraint_based,
                        read_holds.clone(),
                        upper_for_strict.expect("cloned when needs_strict_serializable_comparison"),
                    )?;
                if let Some(serializable) = serializable_det.timestamp_context.timestamp() {
                    session
                        .metrics()
                        .timestamp_difference_for_strict_serializable_ms(&[
                            &self.cached_instance_id_strs[&compute_instance],
                            constraint_based.as_str(),
                        ])
                        .observe(f64::cast_lossy(u64::from(
                            strict.saturating_sub(*serializable),
                        )));
                }
            }
        }

        Ok((det, read_holds))
    }

    /// Fast path for queries with a cached plan. Skips parsing, name resolution,
    /// planning, optimization, RBAC, and cluster resolution. Only performs the
    /// per-query work that must happen on every execution: timestamp determination,
    /// read holds, and peek execution.
    #[mz_ore::instrument(level = "debug")]
    async fn try_frontend_peek_cached(
        &mut self,
        session: &mut Session,
        catalog: Arc<Catalog>,
        cached: Arc<PlanCacheEntry>,
        statement_logging_id: Option<crate::statement_logging::StatementLoggingId>,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        let cached_start = Instant::now();

        // Borrow fields from the Arc'd cache entry instead of cloning the entire struct.
        // Only clone the fields that are consumed (moved) downstream.
        let target_cluster_id = cached.target_cluster_id;
        let peek_plan = cached.peek_plan.clone();
        let result_desc = cached.result_desc.clone();
        let finishing = cached.finishing.clone();
        // source_ids is only used as `depends_on` in RegisterFrontendPeek/PendingPeek,
        // where it is stored but never read. Pass an empty set to avoid cloning.
        let source_ids = std::collections::BTreeSet::new();

        // Log cluster selection
        if let Some(logging_id) = &statement_logging_id {
            self.log_set_cluster(*logging_id, target_cluster_id);
        }

        // Log optimization finished (trivially, since we skipped it)
        if let Some(logging_id) = &statement_logging_id {
            self.log_lifecycle_event(*logging_id, StatementLifecycleEvent::OptimizationFinished);
        }

        let max_query_result_size = Some(session.vars().max_query_result_size());

        // # Timestamp determination
        //
        // For the common case of a single-collection fast-path peek with
        // Serializable isolation and QueryWhen::Immediately (no AS OF, not in
        // a multi-statement transaction), we inline the timestamp determination
        // to avoid the overhead of the general framework:
        // - No Antichain allocations for least_valid_read() / upper join
        // - No BTreeMap iteration in determine_timestamp_for_inner
        // - No DataflowBuilder / ComputeInstanceSnapshot creation
        // - No constraint_based config fetch or metrics observation

        let in_immediate_multi_stmt_txn = session
            .transaction()
            .in_immediate_multi_stmt_txn(&cached.when);

        // Fast path conditions: single compute collection, no storage,
        // Serializable or StrictSerializable, QueryWhen::Immediately,
        // not in a multi-statement transaction, timeline-dependent context.
        let isolation_level = session.vars().transaction_isolation();
        let is_serializable = isolation_level == &IsolationLevel::Serializable;
        let is_strict_serializable = isolation_level == &IsolationLevel::StrictSerializable;
        let can_use_inline_timestamp = !in_immediate_multi_stmt_txn
            && cached.when == QueryWhen::Immediately
            && (is_serializable || is_strict_serializable)
            && cached.input_id_bundle.storage_ids.is_empty()
            && cached.input_id_bundle.compute_ids.len() == 1
            && matches!(
                &cached.timeline_context,
                TimelineContext::TimelineDependent(_) | TimelineContext::TimestampDependent
            );

        let (determination, target_read_hold) = if can_use_inline_timestamp
            && session.get_transaction_timestamp_determination().is_none()
        {
            // Inline fast path: directly acquire read hold and compute timestamp
            // without going through frontend_determine_timestamp.

            // For StrictSerializable, we need the oracle timestamp.
            // For Serializable, we skip the oracle and use the write frontier.
            let oracle_read_ts = if is_strict_serializable {
                let timeline = Coordinator::get_timeline(&cached.timeline_context);
                match timeline {
                    Some(timeline) => {
                        let oracle = self.ensure_oracle(timeline).await?;
                        // Fast path: read the shared atomic directly (no channel
                        // round-trip). Falls back to the slow batched path if the
                        // oracle hasn't been initialized yet.
                        let ts = match oracle.peek_read_ts_fast() {
                            Some(ts) => ts,
                            None => {
                                let read_ts_start = Instant::now();
                                let ts = oracle.read_ts().await;
                                self.metrics()
                                    .frontend_peek_read_ts_seconds
                                    .observe(read_ts_start.elapsed().as_secs_f64());
                                ts
                            }
                        };
                        Some(ts)
                    }
                    None => None,
                }
            } else {
                None
            };

            let instance_id = target_cluster_id;
            let collection_ids = cached
                .single_compute_collection_ids
                .as_ref()
                .expect("single_compute_collection_ids must be set when compute_ids.len() == 1");

            // Ensure compute client is available and get shared state for each collection.
            self.ensure_compute_instance_client(instance_id)
                .await
                .map_err(|err| {
                    AdapterError::concurrent_dependency_drop_from_collection_lookup_error(
                        err.into(),
                        instance_id,
                    )
                })?;

            // For the single-collection fast path, acquire read holds using
            // a persistent cached hold to avoid mutex contention.
            //
            // `acquire_read_hold_direct` locks a `Mutex<MutableAntichain>` and
            // triggers `rebuild()` on every call. At 64+ concurrent connections,
            // this mutex becomes a serialization bottleneck (~13% of function time).
            //
            // Instead, we keep a persistent ReadHold per (instance, collection)
            // on the PeekClient. For each query, we clone the cached hold:
            // `ReadHold::clone()` sends +1 through an async mpsc channel (no mutex),
            // and the corresponding drop sends -1 (also no mutex). The cached hold
            // prevents compaction at the collection's since, so the clone's since
            // is always valid.
            let mut chosen_ts = Timestamp::minimum();
            let mut target_read_hold = None;

            for &id in collection_ids {
                let key = (instance_id, id);
                if !self.shared_collection_states.contains_key(&key) {
                    let shared = self.compute_instances[&instance_id]
                        .collection_shared_state(id)
                        .await
                        .map_err(|err| {
                            AdapterError::concurrent_dependency_drop_from_collection_lookup_error(
                                err,
                                instance_id,
                            )
                        })?;
                    self.shared_collection_states.insert(key, shared);
                }

                // Ensure we have a cached persistent read hold for this collection.
                // The persistent hold is acquired once (via the mutex) and kept alive
                // across queries. Per-query holds are then cloned from it (no mutex).
                if !self.cached_read_holds.contains_key(&key) {
                    let shared = self
                        .shared_collection_states
                        .get(&key)
                        .expect("just inserted");
                    let hold =
                        self.compute_instances[&instance_id].acquire_read_hold_direct(id, shared);
                    self.cached_read_holds.insert(key, hold);
                }

                // Clone the cached hold — sends +1 via async channel, no mutex.
                let read_hold = self.cached_read_holds[&key].clone();

                // Use the cached hold's since as a lower bound for chosen_ts.
                if let Some(since_ts) = read_hold.since().as_option() {
                    chosen_ts = std::cmp::max(chosen_ts, *since_ts);
                }

                // For Serializable: also advance to upper - 1.
                // For StrictSerializable: skip this (use oracle timestamp instead).
                if is_serializable {
                    let shared = self
                        .shared_collection_states
                        .get(&key)
                        .expect("just inserted");
                    let write_upper: Option<Timestamp> = shared
                        .lock_write_frontier(|f: &mut Antichain<Timestamp>| f.as_option().copied());
                    if let Some(upper_ts) = write_upper {
                        let stepped_back = upper_ts.saturating_sub(1);
                        chosen_ts = std::cmp::max(chosen_ts, stepped_back);
                    } else {
                        // Empty upper means the trace is complete.
                        chosen_ts = Timestamp::MAX;
                    }
                }

                target_read_hold = Some(read_hold);
            }

            // For StrictSerializable, advance to the oracle timestamp.
            if let Some(oracle_ts) = &oracle_read_ts {
                chosen_ts = std::cmp::max(chosen_ts, *oracle_ts);
            }

            // Build the TimestampContext directly.
            let timestamp_context = match &cached.timeline_context {
                TimelineContext::TimelineDependent(timeline) => {
                    TimestampContext::TimelineTimestamp {
                        timeline: timeline.clone(),
                        chosen_ts,
                        oracle_ts: oracle_read_ts,
                    }
                }
                TimelineContext::TimestampDependent => TimestampContext::TimelineTimestamp {
                    timeline: Timeline::EpochMilliseconds,
                    chosen_ts,
                    oracle_ts: oracle_read_ts,
                },
                TimelineContext::TimestampIndependent => TimestampContext::NoTimestamp,
            };

            let determination = TimestampDetermination {
                timestamp_context,
                since: Antichain::from_elem(chosen_ts),
                upper: Antichain::from_elem(chosen_ts.step_forward()),
                largest_not_in_advance_of_upper: chosen_ts,
                oracle_read_ts,
                session_oracle_read_ts: None,
                real_time_recency_ts: None,
                constraints: None,
            };

            (determination, target_read_hold)
        } else {
            // General path: handles multi-statement transactions, non-Serializable
            // isolation levels, oracle timestamps, and other edge cases.
            let isolation_level = session.vars().transaction_isolation().clone();
            let timeline = Coordinator::get_timeline(&cached.timeline_context);
            let needs_linearized_read_ts =
                Coordinator::needs_linearized_read_ts(&isolation_level, &cached.when);

            let oracle_read_ts = match timeline {
                Some(timeline) if needs_linearized_read_ts => {
                    let oracle = self.ensure_oracle(timeline).await?;
                    let read_ts_start = Instant::now();
                    let ts = oracle.read_ts().await;
                    self.metrics()
                        .frontend_peek_read_ts_seconds
                        .observe(read_ts_start.elapsed().as_secs_f64());
                    Some(ts)
                }
                Some(_) | None => None,
            };
            let real_time_recency_ts: Option<Timestamp> = None;

            match session.get_transaction_timestamp_determination() {
                Some(
                    determination @ TimestampDetermination {
                        timestamp_context: TimestampContext::TimelineTimestamp { .. },
                        ..
                    },
                ) if in_immediate_multi_stmt_txn => {
                    let txn_read_holds_opt = self
                        .call_coordinator(|tx| Command::GetTransactionReadHoldsBundle {
                            conn_id: session.conn_id().clone(),
                            tx,
                        })
                        .await;

                    if let Some(txn_read_holds) = txn_read_holds_opt {
                        let mut read_holds = txn_read_holds.subset(&cached.input_id_bundle);
                        // Extract the target read hold from the ReadHolds struct.
                        let hold = extract_target_read_hold(
                            &cached.peek_plan,
                            target_cluster_id,
                            &mut read_holds,
                        );
                        (determination, hold)
                    } else {
                        return Err(AdapterError::Internal(
                            "Missing transaction read holds for multi-statement transaction"
                                .to_string(),
                        ));
                    }
                }
                _ => {
                    let timedomain_bundle;
                    let determine_bundle = if in_immediate_multi_stmt_txn {
                        let compute_instance_snapshot =
                            ComputeInstanceSnapshot::new_without_collections(target_cluster_id);
                        let dataflow_builder =
                            DataflowBuilder::new(catalog.state(), compute_instance_snapshot);
                        timedomain_bundle = timedomain_for(
                            &*catalog,
                            &dataflow_builder,
                            &source_ids,
                            &cached.timeline_context,
                            session.conn_id(),
                            target_cluster_id,
                        )?;
                        &timedomain_bundle
                    } else {
                        &cached.input_id_bundle
                    };
                    let (determination, mut read_holds) = self
                        .frontend_determine_timestamp(
                            catalog.state(),
                            session,
                            determine_bundle,
                            &cached.when,
                            target_cluster_id,
                            &cached.timeline_context,
                            oracle_read_ts,
                            real_time_recency_ts,
                        )
                        .await?;
                    let hold = extract_target_read_hold(
                        &cached.peek_plan,
                        target_cluster_id,
                        &mut read_holds,
                    );
                    (determination, hold)
                }
            }
        };

        // For implicit single-statement transactions (the common case for simple
        // SELECT queries via the simple query protocol), skip recording transaction
        // ops entirely. The ops are only needed for multi-statement transactions to
        // ensure consistent timestamps across statements. For implicit single-statement
        // transactions, the transaction is immediately committed locally via
        // end_transaction_local() which just drops the ops without reading them.
        // This avoids cloning the entire TimestampDetermination (including
        // Antichain Vecs) on every query.
        if !session.transaction().is_implicit()
            || session.transaction().is_in_multi_statement_transaction()
        {
            let requires_linearization = crate::session::RequireLinearization::NotRequired;
            let mut transaction_determination = determination.clone();
            if cached.when.is_transactional() {
                session.add_transaction_ops(TransactionOps::Peeks {
                    determination: transaction_determination,
                    cluster_id: target_cluster_id,
                    requires_linearization,
                })?;
            } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
                transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
                session.add_transaction_ops(TransactionOps::Peeks {
                    determination: transaction_determination,
                    cluster_id: target_cluster_id,
                    requires_linearization,
                })?;
            }
        }

        // Execute the peek plan
        let target_replica = None;
        let max_result_size = catalog.system_config().max_result_size();

        let watch_set = statement_logging_id.map(|logging_id| {
            WatchSetCreation::new(
                logging_id,
                catalog.state(),
                &cached.input_id_bundle,
                determination.timestamp_context.timestamp_or_default(),
            )
        });

        // Drop the Arc reference before consuming fields, since we've already
        // cloned the consumed fields and switched to borrowing for the rest.
        drop(cached);

        let response = match peek_plan {
            PeekPlan::FastPath(fast_path_plan) => {
                if let Some(logging_id) = &statement_logging_id {
                    if !matches!(fast_path_plan, FastPathPlan::Constant(..)) {
                        self.log_set_timestamp(
                            *logging_id,
                            determination.timestamp_context.timestamp_or_default(),
                        );
                    }
                }

                let row_set_finishing_seconds =
                    session.metrics().row_set_finishing_seconds().clone();

                let (peek_stash_read_batch_size_bytes, peek_stash_read_memory_budget_bytes) =
                    self.peek_stash_handles(catalog.system_config().dyncfgs());

                self.implement_fast_path_peek_plan(
                    fast_path_plan,
                    determination.timestamp_context.timestamp_or_default(),
                    finishing,
                    target_cluster_id,
                    target_replica,
                    result_desc,
                    max_result_size,
                    max_query_result_size,
                    row_set_finishing_seconds,
                    target_read_hold,
                    peek_stash_read_batch_size_bytes,
                    peek_stash_read_memory_budget_bytes,
                    session.conn_id().clone(),
                    source_ids,
                    watch_set,
                )
                .await?
            }
            PeekPlan::SlowPath(dataflow_plan) => {
                self.call_coordinator(|tx| Command::ExecuteSlowPathPeek {
                    dataflow_plan: Box::new(dataflow_plan),
                    determination,
                    finishing,
                    compute_instance: target_cluster_id,
                    target_replica,
                    intermediate_result_type: result_desc.typ().clone(),
                    source_ids,
                    conn_id: session.conn_id().clone(),
                    max_result_size,
                    max_query_result_size,
                    watch_set,
                    tx,
                })
                .await?
            }
        };

        self.peek_cached_histogram()
            .observe(cached_start.elapsed().as_secs_f64());
        Ok(Some(response))
    }
}
