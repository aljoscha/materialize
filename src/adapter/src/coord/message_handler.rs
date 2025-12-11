// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for processing [`Coordinator`] messages. The [`Coordinator`] receives
//! messages from various sources (ex: controller, clients, background tasks, etc).

use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::time::{Duration, Instant};

use futures::FutureExt;
use maplit::btreemap;
use mz_catalog::memory::objects::ClusterReplicaProcessStatus;
use mz_controller::ControllerResponse;
use mz_controller::clusters::{ClusterEvent, ClusterStatus};
use mz_ore::instrument;
use mz_ore::now::EpochMillis;
use mz_ore::option::OptionExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{soft_assert_or_log, task};
use mz_persist_client::usage::ShardsUsageReferenced;
use mz_repr::{Datum, Diff, Row};
use mz_sql::ast::Statement;
use mz_sql::pure::PurifiedStatement;
use mz_storage_client::controller::IntrospectionType;
use opentelemetry::trace::TraceContextExt;
use rand::{Rng, SeedableRng, rngs};
use serde_json::json;
use tracing::{Instrument, Level, event, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::{BuiltinTableUpdate, Op};
use crate::command::Command;
use crate::coord::{
    AlterConnectionValidationReady, ClusterReplicaStatuses, Coordinator,
    CreateConnectionValidationReady, Message, PurifiedStatementReady, WatchSetResponse,
};
use crate::telemetry::{EventDetails, SegmentClientExt};
use crate::{AdapterNotice, TimestampContext};

impl Coordinator {
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 74KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move Futures of inner function calls onto the heap
    /// (i.e. Box it).
    #[instrument]
    pub(crate) async fn handle_message(&mut self, msg: Message) -> () {
        match msg {
            Message::Command(otel_ctx, cmd) => {
                // TODO: We need a Span that is not none for the otel_ctx to attach the parent
                // relationship to. If we swap the otel_ctx in `Command::Message` for a Span, we
                // can downgrade this to a debug_span.
                let span = tracing::info_span!("message_command").or_current();
                span.in_scope(|| otel_ctx.attach_as_parent());
                self.message_command(cmd).instrument(span).await
            }
            Message::ControllerReady { controller: _ } => {
                let Coordinator {
                    controller,
                    catalog,
                    ..
                } = self;
                let storage_metadata = catalog.state().storage_metadata();
                if let Some(m) = controller
                    .process(storage_metadata)
                    .expect("`process` never returns an error")
                {
                    self.message_controller(m).boxed_local().await
                }
            }
            Message::PurifiedStatementReady(ready) => {
                self.message_purified_statement_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::CreateConnectionValidationReady(ready) => {
                self.message_create_connection_validation_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::AlterConnectionValidationReady(ready) => {
                self.message_alter_connection_validation_ready(ready)
                    .boxed_local()
                    .await
            }
            Message::TryDeferred {
                conn_id,
                acquired_lock,
            } => self.try_deferred(conn_id, acquired_lock).await,
            Message::GroupCommitInitiate(span, permit) => {
                // Add an OpenTelemetry link to our current span.
                tracing::Span::current().add_link(span.context().span().span_context().clone());
                self.try_group_commit(permit)
                    .instrument(span)
                    .boxed_local()
                    .await
            }
            Message::AdvanceTimelines => {
                self.advance_timelines().boxed_local().await;
            }
            Message::ClusterEvent(event) => self.message_cluster_event(event).boxed_local().await,
            Message::CancelPendingPeeks { conn_id } => {
                self.cancel_pending_peeks(&conn_id);
            }
            Message::LinearizeReads => {
                self.message_linearize_reads().boxed_local().await;
            }
            Message::StagedBatches {
                conn_id,
                table_id,
                batches,
            } => {
                self.commit_staged_batches(conn_id, table_id, batches);
            }
            Message::StorageUsageSchedule => {
                self.schedule_storage_usage_collection().boxed_local().await;
            }
            Message::StorageUsageFetch => {
                self.storage_usage_fetch().boxed_local().await;
            }
            Message::StorageUsageUpdate(sizes) => {
                self.storage_usage_update(sizes).boxed_local().await;
            }
            Message::StorageUsagePrune(expired) => {
                self.storage_usage_prune(expired).boxed_local().await;
            }
            Message::RetireExecute {
                otel_ctx,
                data,
                reason,
            } => {
                otel_ctx.attach_as_parent();
                self.retire_execution(reason, data);
            }
            Message::ExecuteSingleStatementTransaction {
                ctx,
                otel_ctx,
                stmt,
                params,
            } => {
                otel_ctx.attach_as_parent();
                self.sequence_execute_single_statement_transaction(ctx, stmt, params)
                    .boxed_local()
                    .await;
            }
            Message::PeekStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateIndexStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateViewStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::CreateMaterializedViewStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::SubscribeStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::IntrospectionSubscribeStageReady { span, stage } => {
                self.sequence_staged((), span, stage).boxed_local().await;
            }
            Message::ExplainTimestampStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::SecretStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::ClusterStageReady { ctx, span, stage } => {
                self.sequence_staged(ctx, span, stage).boxed_local().await;
            }
            Message::DrainStatementLog => {
                self.drain_statement_log();
            }
            Message::PrivateLinkVpcEndpointEvents(events) => {
                if !self.controller.read_only() {
                    self.controller
                            .storage
                            .append_introspection_updates(
                                mz_storage_client::controller::IntrospectionType::PrivatelinkConnectionStatusHistory,
                                events
                                    .into_iter()
                                    .map(|e| (mz_repr::Row::from(e), Diff::ONE))
                                    .collect(),
                            );
                }
            }
            Message::CheckSchedulingPolicies => {
                self.check_scheduling_policies().boxed_local().await;
            }
            Message::SchedulingDecisions(decisions) => {
                self.handle_scheduling_decisions(decisions)
                    .boxed_local()
                    .await;
            }
            Message::DeferredStatementReady => {
                self.handle_deferred_statement().boxed_local().await;
            }
            Message::Widget(widget_msg) => {
                self.handle_widget_message(widget_msg).boxed_local().await;
            }
            Message::WidgetDataRequest(request) => {
                self.handle_widget_data_request(request).await;
            }
        }
    }

    /// Handle a message from a widget.
    async fn handle_widget_message(&mut self, msg: crate::widget::WidgetMessage) {
        use crate::widget::WidgetMessage;
        match msg {
            WidgetMessage::Actions {
                widget_name,
                actions,
            } => {
                tracing::info!(
                    widget = widget_name,
                    num_actions = actions.len(),
                    "coordinator received widget actions"
                );
                for (i, action) in actions.into_iter().enumerate() {
                    self.execute_widget_action(widget_name, i, action).await;
                }
            }
            WidgetMessage::Error { widget_name, error } => {
                tracing::warn!(widget = widget_name, error = %error, "widget reported error");
            }
        }
    }

    /// Execute a single widget action.
    async fn execute_widget_action(
        &mut self,
        widget_name: &'static str,
        action_index: usize,
        action: crate::widget::WidgetAction,
    ) {
        use crate::widget::WidgetAction;
        match action {
            WidgetAction::ExecuteDdl {
                cluster_id,
                strategy_id,
                action_type,
                sql,
                reason,
            } => {
                tracing::info!(
                    widget = widget_name,
                    action_index = action_index,
                    reason = %reason,
                    sql = %sql,
                    "executing widget DDL action"
                );

                let action_id = Self::next_scaling_action_id((self.catalog().config().now)());
                let cluster_id_str = cluster_id.to_string();
                let strategy_id = strategy_id.unwrap_or_else(|| "<unknown>".to_string());
                let action_type = action_type.unwrap_or_else(|| "unknown".to_string());

                let insert_sql = format!(
                    "INSERT INTO mz_internal.mz_scaling_actions \
                     (action_id, strategy_id, cluster_id, action_type, action_sql, reason, executed, error_message, created_at) \
                     VALUES ({}, '{}', '{}', '{}', '{}', '{}', false, NULL, now())",
                    action_id,
                    Self::escape_single_quotes(&strategy_id),
                    Self::escape_single_quotes(&cluster_id_str),
                    Self::escape_single_quotes(&action_type),
                    Self::escape_single_quotes(&sql),
                    Self::escape_single_quotes(&reason),
                );

                if let Err(e) = self
                    .execute_widget_dml(&insert_sql, "log scaling action")
                    .await
                {
                    tracing::warn!(
                        widget = widget_name,
                        action_id,
                        error = %e,
                        "failed to insert mz_scaling_actions row"
                    );
                }

                // Global kill switch: still log actions, but don't execute them.
                if !self.catalog().system_config().auto_scaling_enabled() {
                    tracing::info!(
                        widget = widget_name,
                        action_id,
                        sql = %sql,
                        "auto_scaling_enabled is off; skipping widget DDL execution"
                    );
                    return;
                }

                // Execute the DDL using the internal SQL execution path.
                let ddl_result = self.execute_widget_ddl(&sql, &reason).await;

                let update_sql = match &ddl_result {
                    Ok(()) => format!(
                        "UPDATE mz_internal.mz_scaling_actions \
                         SET executed = true, error_message = NULL \
                         WHERE action_id = {}",
                        action_id
                    ),
                    Err(e) => format!(
                        "UPDATE mz_internal.mz_scaling_actions \
                         SET executed = false, error_message = '{}' \
                         WHERE action_id = {}",
                        Self::escape_single_quotes(&e.to_string()),
                        action_id
                    ),
                };

                if let Err(e) = self
                    .execute_widget_dml(&update_sql, "update scaling action result")
                    .await
                {
                    tracing::warn!(
                        widget = widget_name,
                        action_id,
                        error = %e,
                        "failed to update mz_scaling_actions row"
                    );
                }

                match ddl_result {
                    Ok(()) => tracing::info!(
                        widget = widget_name,
                        action_id,
                        sql = %sql,
                        "widget DDL action executed successfully"
                    ),
                    Err(e) => tracing::warn!(
                        widget = widget_name,
                        action_id,
                        sql = %sql,
                        error = %e,
                        "widget DDL action failed"
                    ),
                }
            }
            WidgetAction::ExecuteDml { sql, reason } => {
                tracing::info!(
                    widget = widget_name,
                    action_index = action_index,
                    reason = %reason,
                    sql = %sql,
                    "executing widget DML action"
                );

                // Execute the DML using the internal SQL execution path
                match self.execute_widget_dml(&sql, &reason).await {
                    Ok(()) => {
                        tracing::info!(
                            widget = widget_name,
                            sql = %sql,
                            "widget DML action executed successfully"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            widget = widget_name,
                            sql = %sql,
                            error = %e,
                            "widget DML action failed"
                        );
                    }
                }
            }
        }
    }
    fn escape_single_quotes(s: &str) -> String {
        s.replace('\'', "''")
    }

    fn next_scaling_action_id(now_millis: u64) -> i64 {
        use std::sync::atomic::{AtomicI64, Ordering};

        static LAST: AtomicI64 = AtomicI64::new(0);
        let now = i64::try_from(now_millis).unwrap_or(i64::MAX);

        let mut last = LAST.load(Ordering::Relaxed);
        loop {
            let next = std::cmp::max(last.saturating_add(1), now);
            match LAST.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return next,
                Err(observed) => last = observed,
            }
        }
    }

    /// Execute a DDL action on behalf of a widget.
    ///
    /// This parses and executes DDL statements (CREATE/DROP CLUSTER REPLICA)
    /// using the same code paths as user DDL, but with system privileges.
    async fn execute_widget_ddl(
        &mut self,
        sql: &str,
        reason: &str,
    ) -> Result<(), crate::AdapterError> {
        use crate::catalog::ReplicaCreateDropReason;
        use mz_ore::collections::CollectionExt;
        use mz_sql::names::ObjectId;
        use mz_sql::plan::{Params, Plan};

        // Step 1: Parse the SQL
        let stmts = mz_sql_parser::parser::parse_statements(sql)
            .map_err(crate::AdapterError::ParseError)?;

        if stmts.len() != 1 {
            return Err(crate::AdapterError::Internal(format!(
                "widget DDL must be exactly one statement, got {}",
                stmts.len()
            )));
        }

        let stmt_raw = stmts.into_element().ast;

        // Step 2: Resolve names using the system session catalog
        let catalog = self.catalog().for_system_session();
        let (stmt, resolved_ids) =
            mz_sql::names::resolve(&catalog, stmt_raw).map_err(crate::AdapterError::PlanError)?;

        // Step 3: Plan the statement
        let params = Params::empty();
        let plan = mz_sql::plan::plan(None, &catalog, stmt, &params, &resolved_ids)
            .map_err(crate::AdapterError::PlanError)?;

        // Step 4: Execute based on plan type, using the inner methods that accept a reason
        let auto_scaling_reason = ReplicaCreateDropReason::AutoScaling(reason.to_string());

        match plan {
            Plan::CreateClusterReplica(plan) => {
                self.sequence_create_cluster_replica_inner(None, plan, auto_scaling_reason)
                    .await?;
                Ok(())
            }
            Plan::DropObjects(plan) => {
                // Extract cluster replica drops from the plan
                let replicas: Vec<_> = plan
                    .drop_ids
                    .into_iter()
                    .filter_map(|id| {
                        if let ObjectId::ClusterReplica((cluster_id, replica_id)) = id {
                            Some((cluster_id, replica_id))
                        } else {
                            tracing::warn!(
                                ?id,
                                "widget attempted to drop non-replica object, ignoring"
                            );
                            None
                        }
                    })
                    .collect();

                if replicas.is_empty() {
                    return Ok(());
                }

                self.sequence_drop_cluster_replicas(replicas, auto_scaling_reason)
                    .await
            }
            _ => Err(crate::AdapterError::Internal(
                "widget DDL only supports CREATE/DROP CLUSTER REPLICA, got unsupported plan type"
                    .to_string(),
            )),
        }
    }

    /// Execute a DML action on behalf of a widget.
    ///
    /// This parses and executes DML statements (INSERT/UPDATE/DELETE) on writable
    /// builtin tables using the same code paths as user DML, ensuring proper
    /// transactional consistency.
    ///
    /// IMPORTANT: Widgets should use this method for all table modifications rather
    /// than directly manipulating `BuiltinTableUpdate`, as direct manipulation can
    /// lead to inconsistent table state.
    async fn execute_widget_dml(
        &mut self,
        sql: &str,
        reason: &str,
    ) -> Result<(), crate::AdapterError> {
        use std::sync::Arc;

        use mz_adapter_types::connection::ConnectionId;
        use mz_ore::collections::CollectionExt;
        use mz_sql::plan::Params;
        use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
        use mz_sql_parser::ast::Raw;
        use tokio::sync::oneshot;
        use uuid::Uuid;

        use crate::coord::ExecuteContextExtra;
        use crate::session::{Session, SessionConfig};
        use crate::util::ClientTransmitter;

        // Step 1: Parse the SQL into exactly one statement.
        let stmts = mz_sql_parser::parser::parse_statements(sql)
            .map_err(crate::AdapterError::ParseError)?;
        if stmts.len() != 1 {
            return Err(crate::AdapterError::Internal(format!(
                "widget DML must be exactly one statement, got {}",
                stmts.len()
            )));
        }
        let stmt: Arc<mz_sql::ast::Statement<Raw>> = Arc::new(stmts.into_element().ast);

        // Step 2: Execute the statement through the normal single-statement
        // execution path (ReadThenWrite) using a system session.
        //
        // This ensures proper transactional semantics for writable builtin tables.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());

        // Pick a stable-ish static connection id for internal widget execution.
        // Keep it below 2^31 so it remains i32-compatible for any pgwire logging.
        const WIDGET_INTERNAL_CONN_ID: ConnectionId = ConnectionId::Static(2_000_000_000);

        let mut session = Session::new(
            self.catalog().config().build_info,
            SessionConfig {
                conn_id: WIDGET_INTERNAL_CONN_ID,
                uuid: Uuid::new_v4(),
                user: mz_sql::session::user::SYSTEM_USER.name.clone(),
                client_ip: None,
                external_metadata_rx: None,
                internal_user_metadata: None,
                helm_chart_version: self.catalog().config().helm_chart_version.clone(),
            },
            self.metrics.session_metrics(),
        );
        session.initialize_role_metadata(MZ_SYSTEM_ROLE_ID);

        tracing::debug!(reason = reason, %sql, "widget executing DML");
        let ctx = crate::coord::ExecuteContext::from_parts(
            sub_ct,
            self.internal_cmd_tx.clone(),
            session,
            ExecuteContextExtra::default(),
        );

        self.sequence_execute_single_statement_transaction(ctx, stmt, Params::empty())
            .await;

        let resp = sub_rx.await.map_err(|_| {
            crate::AdapterError::Internal(
                "widget DML execution response channel dropped".to_string(),
            )
        })?;

        match resp.result {
            Ok(_ok) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Handle a data request from a widget.
    ///
    /// This reads data from builtin tables and returns it to the widget.
    /// The widget specifies which table to read by its CatalogItemId.
    async fn handle_widget_data_request(&mut self, request: crate::widget::WidgetDataRequest) {
        use crate::widget::WidgetDataRequest;
        match request {
            WidgetDataRequest::ReadBuiltinTable {
                table_id,
                response_tx,
            } => {
                tracing::debug!(?table_id, "handling widget builtin table read request");

                let result = self.read_builtin_table_for_widget(table_id).await;

                if response_tx.send(result).is_err() {
                    tracing::debug!("widget dropped before receiving table read response");
                }
            }
            WidgetDataRequest::ExecuteSql { sql, response_tx } => {
                tracing::debug!(%sql, "handling widget SQL execution request");

                let result = self.execute_sql_for_widget(&sql).await;

                if response_tx.send(result).is_err() {
                    tracing::debug!("widget dropped before receiving SQL execution response");
                }
            }
        }
    }

    /// Execute a SQL query for a widget.
    ///
    /// This executes read-only SQL queries (SELECT statements) against system tables.
    /// Currently, this only supports simple single-table reads. For complex queries
    /// with joins/aggregations, use `read_builtin_table` to read raw data and
    /// process it in Rust.
    ///
    /// The query is executed with system privileges.
    async fn execute_sql_for_widget(&mut self, sql: &str) -> Result<Vec<Row>, String> {
        use mz_adapter_types::connection::ConnectionId;
        use mz_ore::collections::CollectionExt;
        use mz_sql::plan::Params;
        use mz_sql_parser::ast::Raw;
        use std::sync::Arc;
        use tokio::sync::oneshot;
        use uuid::Uuid;

        // Step 1: Parse the SQL
        let stmts = mz_sql_parser::parser::parse_statements(sql)
            .map_err(|e| format!("SQL parse error: {}", e))?;

        if stmts.len() != 1 {
            return Err(format!(
                "widget SQL must be exactly one statement, got {}",
                stmts.len()
            ));
        }

        let stmt: Arc<mz_sql::ast::Statement<Raw>> = Arc::new(stmts.into_element().ast);

        // Execute through the normal single-statement execution path using a
        // system session, then collect the produced rows.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = crate::util::ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());

        // Allocate a unique-ish connection id for widget SQL execution.
        //
        // Keep it below 2^31 so it remains i32-compatible for any pgwire logging.
        static NEXT_WIDGET_CONN_ID: std::sync::atomic::AtomicU32 =
            std::sync::atomic::AtomicU32::new(2_000_000_000);
        let conn_id = ConnectionId::Static(
            NEXT_WIDGET_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );

        let mut session = crate::session::Session::new(
            self.catalog().config().build_info,
            crate::session::SessionConfig {
                conn_id: conn_id.clone(),
                uuid: Uuid::new_v4(),
                user: mz_sql::session::user::SYSTEM_USER.name.clone(),
                client_ip: None,
                external_metadata_rx: None,
                internal_user_metadata: None,
                helm_chart_version: self.catalog().config().helm_chart_version.clone(),
            },
            self.metrics.session_metrics(),
        );
        session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
        session.start_transaction_single_stmt(self.now_datetime());

        let ctx = crate::coord::ExecuteContext::from_parts(
            sub_ct,
            self.internal_cmd_tx.clone(),
            session,
            crate::coord::ExecuteContextExtra::default(),
        );

        self.handle_execute_inner(stmt, Params::empty(), ctx).await;

        let resp = sub_rx
            .await
            .map_err(|_| "widget SQL execution response channel dropped".to_string())?;

        let crate::command::Response {
            result,
            session,
            otel_ctx: _,
        } = resp;

        let execute_response = result.map_err(|e| e.to_string())?;

        let mut rows_out: Vec<Row> = Vec::new();
        let mut response = execute_response;
        loop {
            match response {
                crate::ExecuteResponse::CopyTo { resp, .. } => {
                    response = *resp;
                }
                crate::ExecuteResponse::SendingRowsImmediate { mut rows } => {
                    while let Some(row_ref) = rows.next() {
                        rows_out.push(row_ref.to_owned());
                    }
                    break;
                }
                crate::ExecuteResponse::SendingRowsStreaming { mut rows, .. } => {
                    use crate::coord::peek::PeekResponseUnary;
                    use futures::StreamExt;

                    while let Some(unary) = rows.next().await {
                        match unary {
                            PeekResponseUnary::Rows(mut iter) => {
                                while let Some(row_ref) = iter.next() {
                                    rows_out.push(row_ref.to_owned());
                                }
                            }
                            PeekResponseUnary::Error(e) => return Err(e),
                            PeekResponseUnary::Canceled => {
                                return Err("widget SQL execution canceled".to_string());
                            }
                        }
                    }
                    break;
                }
                other => {
                    return Err(format!(
                        "widget SQL must be a single SELECT-like statement that returns rows, got {}",
                        other.tag().unwrap_or_else(|| format!("{:?}", other))
                    ));
                }
            }
        }

        // Always clear per-connection coordinator state for this ephemeral session.
        self.clear_connection(&conn_id).await;
        // Clear session-side transaction state.
        let mut session = session;
        let _ = session.clear_transaction();

        Ok(rows_out)
    }

    /// Read all rows from a builtin table for a widget.
    ///
    /// This looks up the table by CatalogItemId, resolves it to a GlobalId,
    /// and reads the latest snapshot from storage.
    async fn read_builtin_table_for_widget(
        &self,
        table_id: mz_repr::CatalogItemId,
    ) -> Result<Vec<Row>, String> {
        // Get the catalog entry for this item
        let entry = self.catalog().try_get_entry(&table_id);
        let entry = match entry {
            Some(entry) => entry,
            None => {
                return Err(format!(
                    "builtin table not found in catalog: {:?}",
                    table_id
                ));
            }
        };

        // Ensure it's a table
        let table = match entry.table() {
            Some(table) => table,
            None => {
                return Err(format!("catalog item {:?} is not a table", table_id));
            }
        };

        // Get the GlobalId for reading (the latest version)
        let global_id = table.global_id_writes();

        tracing::debug!(
            ?table_id,
            ?global_id,
            table_name = %entry.name().item,
            "reading builtin table for widget"
        );

        // Read the latest snapshot from storage
        match self
            .controller
            .storage_collections
            .snapshot_latest(global_id)
            .await
        {
            Ok(rows) => {
                tracing::debug!(
                    ?table_id,
                    num_rows = rows.len(),
                    "read builtin table for widget"
                );
                Ok(rows)
            }
            Err(e) => Err(format!(
                "failed to read builtin table {:?}: {}",
                table_id, e
            )),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub async fn storage_usage_fetch(&mut self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let client = self.storage_usage_client.clone();

        // Record the currently live shards.
        let live_shards: BTreeSet<_> = self
            .controller
            .storage
            .active_collection_metadatas()
            .into_iter()
            .map(|(_id, m)| m.data_shard)
            .collect();

        let collection_metric = self.metrics.storage_usage_collection_time_seconds.clone();

        // Spawn an asynchronous task to compute the storage usage, which
        // requires a slow scan of the underlying storage engine.
        task::spawn(|| "storage_usage_fetch", async move {
            let collection_metric_timer = collection_metric.start_timer();
            let shard_sizes = client.shards_usage_referenced(live_shards).await;
            collection_metric_timer.observe_duration();

            // It is not an error for shard sizes to become ready after
            // `internal_cmd_rx` is dropped.
            if let Err(e) = internal_cmd_tx.send(Message::StorageUsageUpdate(shard_sizes)) {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        });
    }

    #[mz_ore::instrument(level = "debug")]
    async fn storage_usage_update(&mut self, shards_usage: ShardsUsageReferenced) {
        // Similar to audit events, use the oracle ts so this is guaranteed to
        // increase. This is intentionally the timestamp of when collection
        // finished, not when it started, so that we don't write data with a
        // timestamp in the past.
        let collection_timestamp = if self.controller.read_only() {
            self.peek_local_write_ts().await.into()
        } else {
            // Getting a write timestamp bumps the write timestamp in the
            // oracle, which we're not allowed in read-only mode.
            self.get_local_write_ts().await.timestamp.into()
        };

        let ops = shards_usage
            .by_shard
            .into_iter()
            .map(|(shard_id, shard_usage)| Op::WeirdStorageUsageUpdates {
                object_id: Some(shard_id.to_string()),
                size_bytes: shard_usage.size_bytes(),
                collection_timestamp,
            })
            .collect();

        match self.catalog_transact_inner(None, ops).await {
            Ok((table_updates, catalog_updates)) => {
                assert!(
                    catalog_updates.is_empty(),
                    "applying builtin table updates does not produce catalog implications"
                );

                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let task_span =
                    info_span!(parent: None, "coord::storage_usage_update::table_updates");
                OpenTelemetryContext::obtain().attach_as_parent_to(&task_span);
                task::spawn(|| "storage_usage_update_table_updates", async move {
                    table_updates.instrument(task_span).await;
                    // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                    if let Err(e) = internal_cmd_tx.send(Message::StorageUsageSchedule) {
                        warn!("internal_cmd_rx dropped before we could send: {e:?}");
                    }
                });
            }
            Err(err) => tracing::warn!("Failed to update storage metrics: {:?}", err),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn storage_usage_prune(&mut self, expired: Vec<BuiltinTableUpdate>) {
        let (fut, _) = self.builtin_table_update().execute(expired).await;
        task::spawn(|| "storage_usage_pruning_apply", async move {
            fut.await;
        });
    }

    pub async fn schedule_storage_usage_collection(&self) {
        // Instead of using an `tokio::timer::Interval`, we calculate the time until the next
        // usage collection and wait for that amount of time. This is so we can keep the intervals
        // consistent even across restarts. If collection takes too long, it is possible that
        // we miss an interval.

        // 1) Deterministically pick some offset within the collection interval to prevent
        // thundering herds across environments.
        const SEED_LEN: usize = 32;
        let mut seed = [0; SEED_LEN];
        for (i, byte) in self
            .catalog()
            .state()
            .config()
            .environment_id
            .organization_id()
            .as_bytes()
            .into_iter()
            .take(SEED_LEN)
            .enumerate()
        {
            seed[i] = *byte;
        }
        let storage_usage_collection_interval_ms: EpochMillis =
            EpochMillis::try_from(self.storage_usage_collection_interval.as_millis())
                .expect("storage usage collection interval must fit into u64");
        let offset =
            rngs::SmallRng::from_seed(seed).random_range(0..storage_usage_collection_interval_ms);
        let now_ts: EpochMillis = self.peek_local_write_ts().await.into();

        // 2) Determine the amount of ms between now and the next collection time.
        let previous_collection_ts =
            (now_ts - (now_ts % storage_usage_collection_interval_ms)) + offset;
        let next_collection_ts = if previous_collection_ts > now_ts {
            previous_collection_ts
        } else {
            previous_collection_ts + storage_usage_collection_interval_ms
        };
        let next_collection_interval = Duration::from_millis(next_collection_ts - now_ts);

        // 3) Sleep for that amount of time, then initiate another storage usage collection.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "storage_usage_collection", async move {
            tokio::time::sleep(next_collection_interval).await;
            if internal_cmd_tx.send(Message::StorageUsageFetch).is_err() {
                // If sending fails, the main thread has shutdown.
            }
        });
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_command(&mut self, cmd: Command) {
        self.handle_command(cmd).await;
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_controller(&mut self, message: ControllerResponse) {
        event!(Level::TRACE, message = format!("{:?}", message));
        match message {
            ControllerResponse::PeekNotification(uuid, response, otel_ctx) => {
                self.handle_peek_notification(uuid, response, otel_ctx);
            }
            ControllerResponse::SubscribeResponse(sink_id, response) => {
                if let Some(ActiveComputeSink::Subscribe(active_subscribe)) =
                    self.active_compute_sinks.get_mut(&sink_id)
                {
                    let finished = active_subscribe.process_response(response);
                    if finished {
                        self.retire_compute_sinks(btreemap! {
                            sink_id => ActiveComputeSinkRetireReason::Finished,
                        })
                        .await;
                    }

                    soft_assert_or_log!(
                        !self.introspection_subscribes.contains_key(&sink_id),
                        "`sink_id` {sink_id} unexpectedly found in both `active_subscribes` \
                         and `introspection_subscribes`",
                    );
                } else if self.introspection_subscribes.contains_key(&sink_id) {
                    self.handle_introspection_subscribe_batch(sink_id, response)
                        .await;
                } else {
                    // Cancellation may cause us to receive responses for subscribes no longer
                    // tracked, so we quietly ignore them.
                }
            }
            ControllerResponse::CopyToResponse(sink_id, response) => {
                match self.drop_compute_sink(sink_id).await {
                    Some(ActiveComputeSink::CopyTo(active_copy_to)) => {
                        active_copy_to.retire_with_response(response);
                    }
                    _ => {
                        // Cancellation may cause us to receive responses for subscribes no longer
                        // tracked, so we quietly ignore them.
                    }
                }
            }
            ControllerResponse::WatchSetFinished(ws_ids) => {
                let now = self.now();
                for ws_id in ws_ids {
                    let Some((conn_id, rsp)) = self.installed_watch_sets.remove(&ws_id) else {
                        continue;
                    };
                    self.connection_watch_sets
                        .get_mut(&conn_id)
                        .expect("corrupted coordinator state: unknown connection id")
                        .remove(&ws_id);
                    if self.connection_watch_sets[&conn_id].is_empty() {
                        self.connection_watch_sets.remove(&conn_id);
                    }

                    match rsp {
                        WatchSetResponse::StatementDependenciesReady(id, ev) => {
                            self.record_statement_lifecycle_event(&id, &ev, now);
                        }
                        WatchSetResponse::AlterSinkReady(ctx) => {
                            self.sequence_alter_sink_finish(ctx).await;
                        }
                    }
                }
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_purified_statement_ready(
        &mut self,
        PurifiedStatementReady {
            ctx,
            result,
            params,
            mut plan_validity,
            original_stmt,
            otel_ctx,
        }: PurifiedStatementReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after purification, as a
        // `DROP CONNECTION` or other `DROP` may have sneaked in. If any have gone missing, we
        // repurify the original statement. This will either produce a nice
        // "unknown connector" error, or pick up a new connector that has
        // replaced the dropped connector.
        //
        // n.b. an `ALTER CONNECTION` occurring during purification is OK
        // because we always look up/populate a connection's state after
        // committing to the catalog, so are guaranteed to see the connection's
        // most recent version.
        if plan_validity.check(self.catalog()).is_err() {
            self.handle_execute_inner(original_stmt, params, ctx).await;
            return;
        }

        let purified_statement = match result {
            Ok(ok) => ok,
            Err(e) => return ctx.retire(Err(e)),
        };

        let plan = match purified_statement {
            PurifiedStatement::PurifiedCreateSource {
                create_progress_subsource_stmt,
                create_source_stmt,
                subsources,
                available_source_references,
            } => {
                self.plan_purified_create_source(
                    &ctx,
                    params,
                    create_progress_subsource_stmt,
                    create_source_stmt,
                    subsources,
                    available_source_references,
                )
                .await
            }
            PurifiedStatement::PurifiedAlterSourceAddSubsources {
                source_name,
                options,
                subsources,
            } => {
                self.plan_purified_alter_source_add_subsource(
                    ctx.session(),
                    params,
                    source_name,
                    options,
                    subsources,
                )
                .await
            }
            PurifiedStatement::PurifiedAlterSourceRefreshReferences {
                source_name,
                available_source_references,
            } => self.plan_purified_alter_source_refresh_references(
                ctx.session(),
                params,
                source_name,
                available_source_references,
            ),
            o @ (PurifiedStatement::PurifiedAlterSource { .. }
            | PurifiedStatement::PurifiedCreateSink(..)
            | PurifiedStatement::PurifiedCreateTableFromSource { .. }) => {
                // Unify these into a `Statement`.
                let stmt = match o {
                    PurifiedStatement::PurifiedAlterSource { alter_source_stmt } => {
                        Statement::AlterSource(alter_source_stmt)
                    }
                    PurifiedStatement::PurifiedCreateTableFromSource { stmt } => {
                        Statement::CreateTableFromSource(stmt)
                    }
                    PurifiedStatement::PurifiedCreateSink(stmt) => Statement::CreateSink(stmt),
                    PurifiedStatement::PurifiedCreateSource { .. }
                    | PurifiedStatement::PurifiedAlterSourceAddSubsources { .. }
                    | PurifiedStatement::PurifiedAlterSourceRefreshReferences { .. } => {
                        unreachable!("not part of exterior match stmt")
                    }
                };

                // Determine all dependencies, not just those in the statement
                // itself.
                let catalog = self.catalog().for_session(ctx.session());
                let resolved_ids = mz_sql::names::visit_dependencies(&catalog, &stmt);
                self.plan_statement(ctx.session(), stmt, &params, &resolved_ids)
                    .map(|plan| (plan, resolved_ids))
            }
        };

        match plan {
            Ok((plan, resolved_ids)) => self.sequence_plan(ctx, plan, resolved_ids).await,
            Err(e) => ctx.retire(Err(e)),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_create_connection_validation_ready(
        &mut self,
        CreateConnectionValidationReady {
            mut ctx,
            result,
            connection_id,
            connection_gid,
            mut plan_validity,
            otel_ctx,
            resolved_ids,
        }: CreateConnectionValidationReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after validation, as a
        // `DROP SECRET` may have sneaked in.
        //
        // WARNING: If we support `ALTER SECRET`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if let Err(e) = plan_validity.check(self.catalog()) {
            let _ = self.secrets_controller.delete(connection_id).await;
            return ctx.retire(Err(e));
        }

        let plan = match result {
            Ok(ok) => ok,
            Err(e) => {
                let _ = self.secrets_controller.delete(connection_id).await;
                return ctx.retire(Err(e));
            }
        };

        let result = self
            .sequence_create_connection_stage_finish(
                &mut ctx,
                connection_id,
                connection_gid,
                plan,
                resolved_ids,
            )
            .await;
        ctx.retire(result);
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_alter_connection_validation_ready(
        &mut self,
        AlterConnectionValidationReady {
            mut ctx,
            result,
            connection_id,
            connection_gid: _,
            mut plan_validity,
            otel_ctx,
            resolved_ids: _,
        }: AlterConnectionValidationReady,
    ) {
        otel_ctx.attach_as_parent();

        // Ensure that all dependencies still exist after validation, as a
        // `DROP SECRET` may have sneaked in.
        //
        // WARNING: If we support `ALTER SECRET`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if let Err(e) = plan_validity.check(self.catalog()) {
            return ctx.retire(Err(e));
        }

        let conn = match result {
            Ok(ok) => ok,
            Err(e) => {
                return ctx.retire(Err(e));
            }
        };

        let result = self
            .sequence_alter_connection_stage_finish(ctx.session_mut(), connection_id, conn)
            .await;
        ctx.retire(result);
    }

    #[mz_ore::instrument(level = "debug")]
    async fn message_cluster_event(&mut self, event: ClusterEvent) {
        event!(Level::TRACE, event = format!("{:?}", event));

        if let Some(segment_client) = &self.segment_client {
            let env_id = &self.catalog().config().environment_id;
            let mut properties = json!({
                "cluster_id": event.cluster_id.to_string(),
                "replica_id": event.replica_id.to_string(),
                "process_id": event.process_id,
                "status": event.status.as_kebab_case_str(),
            });
            match event.status {
                ClusterStatus::Online => (),
                ClusterStatus::Offline(reason) => {
                    let properties = match &mut properties {
                        serde_json::Value::Object(map) => map,
                        _ => unreachable!(),
                    };
                    properties.insert(
                        "reason".into(),
                        json!(reason.display_or("unknown").to_string()),
                    );
                }
            };
            segment_client.environment_track(
                env_id,
                "Cluster Changed Status",
                properties,
                EventDetails {
                    timestamp: Some(event.time),
                    ..Default::default()
                },
            );
        }

        // It is possible that we receive a status update for a replica that has
        // already been dropped from the catalog. Just ignore these events.
        let Some(replica_statues) = self
            .cluster_replica_statuses
            .try_get_cluster_replica_statuses(event.cluster_id, event.replica_id)
        else {
            return;
        };

        if event.status != replica_statues[&event.process_id].status {
            if !self.controller.read_only() {
                let offline_reason = match event.status {
                    ClusterStatus::Online => None,
                    ClusterStatus::Offline(None) => None,
                    ClusterStatus::Offline(Some(reason)) => Some(reason.to_string()),
                };
                let row = Row::pack_slice(&[
                    Datum::String(&event.replica_id.to_string()),
                    Datum::UInt64(event.process_id),
                    Datum::String(event.status.as_kebab_case_str()),
                    Datum::from(offline_reason.as_deref()),
                    Datum::TimestampTz(event.time.try_into().expect("must fit")),
                ]);
                self.controller.storage.append_introspection_updates(
                    IntrospectionType::ReplicaStatusHistory,
                    vec![(row, Diff::ONE)],
                );
            }

            let old_replica_status =
                ClusterReplicaStatuses::cluster_replica_status(replica_statues);

            let new_process_status = ClusterReplicaProcessStatus {
                status: event.status,
                time: event.time,
            };
            self.cluster_replica_statuses.ensure_cluster_status(
                event.cluster_id,
                event.replica_id,
                event.process_id,
                new_process_status,
            );

            let cluster = self.catalog().get_cluster(event.cluster_id);
            let replica = cluster.replica(event.replica_id).expect("Replica exists");
            let new_replica_status = self
                .cluster_replica_statuses
                .get_cluster_replica_status(event.cluster_id, event.replica_id);

            if old_replica_status != new_replica_status {
                let notifier = self.broadcast_notice_tx();
                let notice = AdapterNotice::ClusterReplicaStatusChanged {
                    cluster: cluster.name.clone(),
                    replica: replica.name.clone(),
                    status: new_replica_status,
                    time: event.time,
                };
                notifier(notice);
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    /// Linearizes sending the results of a read transaction by,
    ///   1. Holding back any results that were executed at some point in the future, until the
    ///   containing timeline has advanced to that point in the future.
    ///   2. Confirming that we are still the current leader before sending results to the client.
    async fn message_linearize_reads(&mut self) {
        let mut shortest_wait = Duration::MAX;
        let mut ready_txns = Vec::new();

        // Cache for `TimestampOracle::read_ts` calls. These are somewhat
        // expensive so we cache the value. This is correct since all we're
        // risking is being too conservative. We will not accidentally "release"
        // a result too early.
        let mut cached_oracle_ts = BTreeMap::new();

        for (conn_id, mut read_txn) in std::mem::take(&mut self.pending_linearize_read_txns) {
            if let TimestampContext::TimelineTimestamp {
                timeline,
                chosen_ts,
                oracle_ts,
            } = read_txn.timestamp_context()
            {
                let oracle_ts = match oracle_ts {
                    Some(oracle_ts) => oracle_ts,
                    None => {
                        // There was no oracle timestamp, so no need to delay.
                        ready_txns.push(read_txn);
                        continue;
                    }
                };

                if chosen_ts <= oracle_ts {
                    // Chosen ts was already <= the oracle ts, so we're good
                    // to go!
                    ready_txns.push(read_txn);
                    continue;
                }

                // See what the oracle timestamp is now and delay when needed.
                let current_oracle_ts = cached_oracle_ts.entry(timeline.clone());
                let current_oracle_ts = match current_oracle_ts {
                    btree_map::Entry::Vacant(entry) => {
                        let timestamp_oracle = self.get_timestamp_oracle(timeline);
                        let read_ts = timestamp_oracle.read_ts().await;
                        entry.insert(read_ts.clone());
                        read_ts
                    }
                    btree_map::Entry::Occupied(entry) => entry.get().clone(),
                };

                if *chosen_ts <= current_oracle_ts {
                    ready_txns.push(read_txn);
                } else {
                    let wait =
                        Duration::from_millis(chosen_ts.saturating_sub(current_oracle_ts).into());
                    if wait < shortest_wait {
                        shortest_wait = wait;
                    }
                    read_txn.num_requeues += 1;
                    self.pending_linearize_read_txns.insert(conn_id, read_txn);
                }
            } else {
                ready_txns.push(read_txn);
            }
        }

        if !ready_txns.is_empty() {
            // Sniff out one ctx, this is where tracing breaks down because we
            // process all outstanding txns as a batch here.
            let otel_ctx = ready_txns.first().expect("known to exist").otel_ctx.clone();
            let span = tracing::debug_span!("message_linearize_reads");
            otel_ctx.attach_as_parent_to(&span);

            let now = Instant::now();
            for ready_txn in ready_txns {
                let span = tracing::debug_span!("retire_read_results");
                ready_txn.otel_ctx.attach_as_parent_to(&span);
                let _entered = span.enter();
                self.metrics
                    .linearize_message_seconds
                    .with_label_values(&[
                        ready_txn.txn.label(),
                        if ready_txn.num_requeues == 0 {
                            "true"
                        } else {
                            "false"
                        },
                    ])
                    .observe((now - ready_txn.created).as_secs_f64());
                if let Some((ctx, result)) = ready_txn.txn.finish() {
                    ctx.retire(result);
                }
            }
        }

        if !self.pending_linearize_read_txns.is_empty() {
            // Cap wait time to 1s.
            let remaining_ms = std::cmp::min(shortest_wait, Duration::from_millis(1_000));
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "deferred_read_txns", async move {
                tokio::time::sleep(remaining_ms).await;
                // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                let result = internal_cmd_tx.send(Message::LinearizeReads);
                if let Err(e) = result {
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            });
        }
    }
}
