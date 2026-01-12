// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Subscribe-based read-then-write implementation using optimistic concurrency control.
//!
//! This module implements DELETE and UPDATE operations using a subscribe with optimistic
//! concurrency control (OCC). Instead of acquiring pessimistic write locks and doing a
//! peek-then-write, we:
//!
//! 1. Set up a subscribe on the rows to be modified (negated for DELETE)
//! 2. The subscribe outputs rows that will be used as diffs
//! 3. On each frontier advance, we try to write the accumulated diffs
//! 4. If the timestamp has passed, we retry with the next frontier
//!
//! This approach removes the need for write locks and allows for better concurrency.

use std::collections::BTreeMap;
use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::Arc;

use differential_dataflow::consolidation;
use itertools::Itertools;
use mz_expr::{CollectionPlan, Id, LocalId, MirRelationExpr, MirScalarExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::task;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, Diff, GlobalId, IntoRowIterator, RelationDesc, Row, RowArena, Timestamp};
use mz_sql::plan::{self, MutationKind, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use timely::progress::Antichain;
use tokio::sync::{Semaphore, mpsc};
use tracing::Span;

use crate::active_compute_sink::{ActiveComputeSink, ActiveSubscribe};
use crate::command::ExecuteResponse;
use crate::coord::appends::TimestampedWriteResult;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{Coordinator, Message, PlanValidity, StageResult, Staged, TargetCluster};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::session::TransactionOps;
use crate::{ExecuteContext, PeekResponseUnary, TimelineContext, optimize};

/// Transform a MIR expression to produce the appropriate diffs for a mutation.
///
/// - DELETE: Negates the expression to produce `(row, -1)` diffs
/// - UPDATE: Unions negated old rows with mapped new rows to produce both
///   `(old_row, -1)` and `(new_row, +1)` diffs
fn apply_mutation_to_mir(
    expr: MirRelationExpr,
    kind: &MutationKind,
    assignments: &BTreeMap<usize, MirScalarExpr>,
) -> MirRelationExpr {
    match kind {
        MutationKind::Delete => {
            // Negate to produce (row, -1) diffs
            MirRelationExpr::Negate {
                input: Box::new(expr),
            }
        }
        MutationKind::Update => {
            // For UPDATE, emit both (old_row, -1) and (new_row, +1).
            // We union:
            // - new_rows: Map with assignments applied, then Project to table arity
            // - old_rows: Negate (produces -1 diffs)
            //
            // We use a Let binding to avoid duplicating the expression, which would
            // cause "Shadowing of let binding" errors if expr contains Let bindings.

            let arity = expr.arity();

            // Find a fresh LocalId that won't conflict with any in the expression
            let mut max_id = 0_u64;
            expr.visit_pre(|e| {
                if let MirRelationExpr::Let { id, .. } = e {
                    max_id = std::cmp::max(max_id, id.into());
                }
            });
            let binding_id = LocalId::new(max_id + 1);

            // Reference to the bound expression
            let get_binding = MirRelationExpr::Get {
                id: Id::Local(binding_id),
                typ: expr.typ(),
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            };

            // Build map expressions: for each column, use assignment or identity
            let map_scalars: Vec<MirScalarExpr> = (0..arity)
                .map(|i| {
                    assignments
                        .get(&i)
                        .cloned()
                        .unwrap_or_else(|| MirScalarExpr::column(i))
                })
                .collect();

            // new_rows: apply assignments via Map, then Project to keep only new columns
            // Map appends columns, so we project columns arity..2*arity
            let new_rows = get_binding
                .clone()
                .map(map_scalars)
                .project((arity..2 * arity).collect());

            // old_rows: negate the original selection
            let old_rows = MirRelationExpr::Negate {
                input: Box::new(get_binding),
            };

            // Union produces both old (-1) and new (+1) diffs
            let body = new_rows.union(old_rows);

            // Wrap in Let binding
            MirRelationExpr::Let {
                id: binding_id,
                value: Box::new(expr),
                body: Box::new(body),
            }
        }
        MutationKind::Insert => {
            // Should not reach here (filtered in sequence_read_then_write_subscribe)
            unreachable!("INSERT filtered in sequence_read_then_write_subscribe");
        }
    }
}

/// State for optimizing the MIR plan for a read-then-write subscribe.
#[derive(Debug)]
pub struct ReadThenWriteSubscribeOptimizeMir {
    pub validity: PlanValidity,
    pub plan: plan::ReadThenWritePlan,
    pub timeline: TimelineContext,
    pub cluster_id: mz_compute_types::ComputeInstanceId,
    pub replica_id: Option<mz_cluster_client::ReplicaId>,
}

/// State for timestamp selection and LIR optimization.
#[derive(Debug)]
pub struct ReadThenWriteSubscribeTimestampOptimizeLir {
    pub validity: PlanValidity,
    pub plan: plan::ReadThenWritePlan,
    pub timeline: TimelineContext,
    pub optimizer: optimize::subscribe::Optimizer,
    pub global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
    pub replica_id: Option<mz_cluster_client::ReplicaId>,
}

/// State for finishing the subscribe setup and starting the OCC loop.
#[derive(Debug)]
pub struct ReadThenWriteSubscribeFinish {
    pub validity: PlanValidity,
    pub cluster_id: mz_compute_types::ComputeInstanceId,
    pub plan: plan::ReadThenWritePlan,
    pub global_lir_plan: optimize::subscribe::GlobalLirPlan,
    pub replica_id: Option<mz_cluster_client::ReplicaId>,
    pub read_holds: crate::ReadHolds<Timestamp>,
}

/// Stages for the read-then-write subscribe flow.
#[derive(Debug)]
pub enum ReadThenWriteSubscribeStage {
    OptimizeMir(ReadThenWriteSubscribeOptimizeMir),
    TimestampOptimizeLir(ReadThenWriteSubscribeTimestampOptimizeLir),
    Finish(ReadThenWriteSubscribeFinish),
}

impl Staged for ReadThenWriteSubscribeStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            ReadThenWriteSubscribeStage::OptimizeMir(stage) => &mut stage.validity,
            ReadThenWriteSubscribeStage::TimestampOptimizeLir(stage) => &mut stage.validity,
            ReadThenWriteSubscribeStage::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            ReadThenWriteSubscribeStage::OptimizeMir(stage) => {
                coord.read_then_write_subscribe_optimize_mir(stage)
            }
            ReadThenWriteSubscribeStage::TimestampOptimizeLir(stage) => {
                coord
                    .read_then_write_subscribe_timestamp_optimize_lir(ctx, stage)
                    .await
            }
            ReadThenWriteSubscribeStage::Finish(stage) => {
                coord.read_then_write_subscribe_finish(ctx, stage).await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::ReadThenWriteSubscribeStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    /// Sequence a read-then-write operation using subscribe with OCC.
    ///
    /// This is an alternative to `sequence_read_then_write` that uses a subscribe
    /// instead of a peek, with optimistic concurrency control instead of pessimistic locks.
    pub(crate) async fn sequence_read_then_write_subscribe(
        &mut self,
        mut ctx: ExecuteContext,
        plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
    ) {
        // DELETE and UPDATE are supported; INSERT uses a different path
        if matches!(plan.kind, MutationKind::Insert) {
            ctx.retire(Err(AdapterError::Unsupported(
                "INSERT is not supported with read-then-write subscribe",
            )));
            return;
        }

        let stage = return_if_err!(
            self.read_then_write_subscribe_validate(ctx.session_mut(), plan, target_cluster),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    fn read_then_write_subscribe_validate(
        &mut self,
        session: &mut crate::session::Session,
        plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
    ) -> Result<ReadThenWriteSubscribeStage, AdapterError> {
        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let replica_id = session
            .vars()
            .cluster_replica()
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        // Determine timeline
        let depends_on = plan.selection.depends_on();
        let timeline = self
            .catalog()
            .validate_timeline_context(depends_on.iter().copied())?;

        let dependencies: std::collections::BTreeSet<CatalogItemId> = depends_on
            .iter()
            .map(|id| self.catalog().resolve_item_id(id))
            .collect();
        let validity = PlanValidity::new(
            self.catalog().transient_revision(),
            dependencies,
            Some(cluster_id),
            replica_id,
            session.role_metadata().clone(),
        );

        // This is an internal operation, we add it to the transaction
        session.add_transaction_ops(TransactionOps::Writes(vec![]))?;

        Ok(ReadThenWriteSubscribeStage::OptimizeMir(
            ReadThenWriteSubscribeOptimizeMir {
                validity,
                plan,
                timeline,
                cluster_id,
                replica_id,
            },
        ))
    }

    fn read_then_write_subscribe_optimize_mir(
        &self,
        ReadThenWriteSubscribeOptimizeMir {
            mut validity,
            plan,
            timeline,
            cluster_id,
            replica_id,
        }: ReadThenWriteSubscribeOptimizeMir,
    ) -> Result<StageResult<Box<ReadThenWriteSubscribeStage>>, AdapterError> {
        let plan::ReadThenWritePlan {
            id: _,
            selection,
            finishing: _,
            assignments,
            kind,
            returning: _,
        } = &plan;

        // Lower HIR to MIR
        let expr = selection
            .clone()
            .lower(self.catalog().system_config(), None)?;

        // Transform for subscribe-based diffs
        let expr = apply_mutation_to_mir(expr, kind, assignments);

        // Collect optimizer parameters
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance does not exist");
        let (_, view_id) = self.allocate_transient_id();
        let (_, sink_id) = self.allocate_transient_id();
        let debug_name = format!("read-then-write-subscribe-{}", sink_id);
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features());

        // Build an optimizer for the subscribe.
        // Note: We use with_snapshot=true to get the current state,
        // and up_to=None to keep running until we succeed.
        let mut optimizer = optimize::subscribe::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            view_id,
            sink_id,
            true, // with_snapshot
            None, // up_to - keep running
            debug_name,
            optimizer_config,
            self.optimizer_metrics(),
        );
        let catalog = self.owned_catalog();

        // Get the relation desc for the expression
        let expr_typ = expr.typ();
        // Create anonymous column names for the relation desc
        let column_names: Vec<String> = (0..expr_typ.column_types.len())
            .map(|i| format!("column{}", i))
            .collect();
        let relation_desc = RelationDesc::new(expr_typ, column_names.iter().map(|s| s.as_str()));

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize read-then-write subscribe (mir)",
            move || {
                span.in_scope(|| {
                    // Create the subscribe from the query (with negation for DELETE)
                    let from = mz_sql::plan::SubscribeFrom::Query {
                        expr,
                        desc: relation_desc,
                    };

                    // MIR => MIR optimization (global)
                    let global_mir_plan = optimizer.catch_unwind_optimize(from)?;

                    // Add introduced indexes as validity dependencies
                    validity.extend_dependencies(
                        global_mir_plan
                            .id_bundle(optimizer.cluster_id())
                            .iter()
                            .map(|id| catalog.resolve_item_id(&id)),
                    );

                    let stage = ReadThenWriteSubscribeStage::TimestampOptimizeLir(
                        ReadThenWriteSubscribeTimestampOptimizeLir {
                            validity,
                            plan,
                            timeline,
                            optimizer,
                            global_mir_plan,
                            replica_id,
                        },
                    );
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    async fn read_then_write_subscribe_timestamp_optimize_lir(
        &mut self,
        ctx: &ExecuteContext,
        ReadThenWriteSubscribeTimestampOptimizeLir {
            validity,
            plan,
            timeline,
            mut optimizer,
            global_mir_plan,
            replica_id,
        }: ReadThenWriteSubscribeTimestampOptimizeLir,
    ) -> Result<StageResult<Box<ReadThenWriteSubscribeStage>>, AdapterError> {
        // Timestamp selection - use FreshestTableWrite to see all previous writes
        let when = QueryWhen::FreshestTableWrite;
        let oracle_read_ts = self.oracle_read_ts(ctx.session(), &timeline, &when).await;
        let bundle = &global_mir_plan.id_bundle(optimizer.cluster_id());
        let (determination, read_holds) = self.determine_timestamp(
            ctx.session(),
            bundle,
            &when,
            optimizer.cluster_id(),
            &timeline,
            oracle_read_ts,
            None,
        )?;

        let as_of = determination.timestamp_context.timestamp_or_default();

        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }

        // Don't store read_holds in the connection-keyed txn_read_holds map because
        // multiple operations on the same connection could overwrite each other's holds.
        // Instead, pass them through the stages directly to avoid SinceViolation errors.

        let global_mir_plan = global_mir_plan.resolve(Antichain::from_elem(as_of));

        // Optimize LIR
        let span = Span::current();
        let cluster_id = optimizer.cluster_id();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize read-then-write subscribe (lir)",
            move || {
                span.in_scope(|| {
                    // MIR => LIR lowering and LIR => LIR optimization (global)
                    let global_lir_plan =
                        optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    let stage = ReadThenWriteSubscribeStage::Finish(ReadThenWriteSubscribeFinish {
                        validity,
                        cluster_id,
                        plan,
                        global_lir_plan,
                        replica_id,
                        read_holds,
                    });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    async fn read_then_write_subscribe_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        ReadThenWriteSubscribeFinish {
            validity: _,
            cluster_id,
            plan,
            global_lir_plan,
            replica_id,
            read_holds,
        }: ReadThenWriteSubscribeFinish,
    ) -> Result<StageResult<Box<ReadThenWriteSubscribeStage>>, AdapterError> {
        let sink_id = global_lir_plan.sink_id();
        let target_id = plan.id;
        let kind = plan.kind;
        let returning = plan.returning;

        let (df_desc, _df_meta) = global_lir_plan.unapply();

        let as_of = df_desc
            .as_of
            .clone()
            .map(|a| a.into_element())
            .expect("set to Some in an earlier stage");

        let arity = df_desc
            .sink_exports
            .values()
            .next()
            .expect("has sink")
            .from_desc
            .arity();

        let depends_on = plan.selection.depends_on();

        // Extract data needed to create the subscribe later (after semaphore acquired)
        let conn_id = ctx.session().conn_id().clone();
        let session_uuid = ctx.session().uuid();
        let start_time = self.now();

        // Spawn the OCC loop task that will:
        // 1. Acquire the semaphore
        // 2. Send a message to the coordinator to create the subscribe
        // 3. Run the OCC loop
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let occ_write_semaphore = Arc::clone(&self.occ_write_semaphore);

        Ok(StageResult::HandleRetire(task::spawn(
            || format!("read_then_write_subscribe_occ:{target_id}"),
            Self::run_read_then_write_occ_loop(
                internal_cmd_tx,
                occ_write_semaphore,
                // Data for creating subscribe
                conn_id,
                session_uuid,
                df_desc,
                cluster_id,
                replica_id,
                depends_on,
                as_of,
                arity,
                sink_id,
                start_time,
                // Data for the OCC loop
                target_id,
                kind,
                returning,
                // Read holds passed directly to avoid connection-keyed storage issues
                read_holds,
            ),
        )))
    }

    /// Run the OCC loop that receives subscribe rows and tries to write them.
    ///
    /// This receives rows from the subscribe (which include the negated diffs for DELETE),
    /// accumulates them, and on each frontier advance attempts to write to the target table.
    /// If the timestamp has passed, it retries on the next frontier advance.
    ///
    /// The semaphore permit is acquired at the start of this loop (not before spawning) to avoid
    /// blocking the coordinator. This prevents deadlock where all permits are held by
    /// OCC loops that need to send messages to the coordinator to complete.
    ///
    /// After acquiring the semaphore, we send a message to the coordinator to create the
    /// subscribe and return the channel. This ensures subscribes only start when permits
    /// are available, avoiding wasted compute resources.
    #[allow(clippy::too_many_arguments)]
    async fn run_read_then_write_occ_loop(
        internal_cmd_tx: tokio::sync::mpsc::UnboundedSender<Message>,
        occ_write_semaphore: Arc<Semaphore>,
        // Data for creating subscribe
        conn_id: mz_adapter_types::connection::ConnectionId,
        session_uuid: uuid::Uuid,
        df_desc: crate::optimize::LirDataflowDescription,
        cluster_id: mz_compute_types::ComputeInstanceId,
        replica_id: Option<mz_cluster_client::ReplicaId>,
        depends_on: std::collections::BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        start_time: mz_ore::now::EpochMillis,
        // Data for the OCC loop
        target_id: CatalogItemId,
        kind: MutationKind,
        returning: Vec<MirScalarExpr>,
        // Read holds passed directly to avoid connection-keyed storage issues
        read_holds: crate::ReadHolds<Timestamp>,
    ) -> Result<ExecuteResponse, AdapterError> {
        /// State for the OCC loop
        struct OccState {
            /// Accumulated rows with their diffs
            all_diffs: Vec<(Row, Timestamp, Diff)>,
            /// Current frontier (upper) timestamp
            current_upper: Option<Timestamp>,
            /// Whether we've seen the first progress message (snapshot complete)
            initial_progress_seen: bool,
        }

        impl OccState {
            fn new() -> Self {
                Self {
                    all_diffs: Vec::new(),
                    current_upper: None,
                    initial_progress_seen: false,
                }
            }
        }

        // Acquire semaphore permit to limit concurrent OCC operations.
        // This is done inside the spawned task to avoid blocking the coordinator,
        // which would cause deadlock when all permits are held.
        let _permit = occ_write_semaphore
            .acquire_owned()
            .await
            .expect("semaphore closed");

        // Now that we have the permit, request the coordinator to create the subscribe.
        // This ensures subscribes only start when we have capacity, avoiding wasted compute.
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let _ = internal_cmd_tx.send(Message::StartReadThenWriteSubscribe {
            conn_id,
            session_uuid,
            df_desc,
            cluster_id,
            replica_id,
            depends_on,
            as_of,
            arity,
            sink_id,
            start_time,
            response_tx,
            read_holds,
        });

        // Wait for the coordinator to create the subscribe and return the channel
        let mut rx: mpsc::UnboundedReceiver<PeekResponseUnary> = match response_rx.await {
            Ok(rx) => rx,
            Err(_) => {
                return Err(AdapterError::Internal(
                    "failed to receive subscribe channel from coordinator".into(),
                ));
            }
        };

        /// Result of processing a subscribe message
        enum ProcessResult {
            /// Continue processing, optionally ready to try a write
            Continue { ready_to_write: bool },
            /// No rows matched, return early with zero count
            NoRowsMatched,
            /// Error occurred
            Error(AdapterError),
            /// Channel closed
            ChannelClosed,
        }

        /// Process a single subscribe message, updating state
        fn process_message(
            response: PeekResponseUnary,
            state: &mut OccState,
        ) -> ProcessResult {
            match response {
                PeekResponseUnary::Rows(mut rows) => {
                    let mut saw_progress = false;

                    while let Some(row) = rows.next() {
                        let mut datums = row.iter();

                        // Extract mz_timestamp
                        let ts_datum = datums.next().expect("missing mz_timestamp");
                        let ts = match ts_datum {
                            mz_repr::Datum::Numeric(n) => {
                                let ts_u64: u64 = n.0.try_into().unwrap_or(0);
                                Timestamp::new(ts_u64)
                            }
                            _ => continue,
                        };

                        // Extract mz_progressed
                        let progressed_datum = datums.next().expect("missing mz_progressed");
                        let is_progress = matches!(progressed_datum, mz_repr::Datum::True);

                        if is_progress {
                            state.current_upper = Some(ts);
                            saw_progress = true;

                            if !state.initial_progress_seen {
                                state.initial_progress_seen = true;
                            } else if state.all_diffs.is_empty() {
                                // No rows matched the selection after initial snapshot
                                return ProcessResult::NoRowsMatched;
                            }
                        } else {
                            // Data row - extract mz_diff
                            let diff_datum = datums.next().expect("missing mz_diff");
                            let diff = match diff_datum {
                                mz_repr::Datum::Int64(d) => Diff::from(d),
                                _ => continue,
                            };

                            let data_row = Row::pack(datums);
                            state.all_diffs.push((data_row, ts, diff));
                        }
                    }

                    // Ready to write if we saw progress and have diffs
                    let ready_to_write =
                        saw_progress && state.initial_progress_seen && !state.all_diffs.is_empty();
                    ProcessResult::Continue { ready_to_write }
                }
                PeekResponseUnary::Error(e) => ProcessResult::Error(AdapterError::Internal(e)),
                PeekResponseUnary::Canceled => ProcessResult::Error(AdapterError::Canceled),
            }
        }

        let mut state = OccState::new();

        loop {
            // Wait for the next message
            let msg = match rx.recv().await {
                Some(msg) => msg,
                None => {
                    let _ = internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });
                    return Err(AdapterError::Internal("subscribe channel closed".into()));
                }
            };

            // Process the message
            match process_message(msg, &mut state) {
                ProcessResult::Continue { ready_to_write } => {
                    if !ready_to_write {
                        continue;
                    }

                    // Sync to the latest state by draining any pending messages before
                    // attempting a write. This ensures we use the most recent frontier.
                    loop {
                        match rx.try_recv() {
                            Ok(msg) => {
                                match process_message(msg, &mut state) {
                                    ProcessResult::Continue { .. } => {
                                        // Keep draining
                                    }
                                    ProcessResult::NoRowsMatched => {
                                        let _ = internal_cmd_tx
                                            .send(Message::DropReadThenWriteSubscribe { sink_id });

                                        if !returning.is_empty() {
                                            // Empty result set for RETURNING
                                            let rows: Vec<Row> = vec![];
                                            return Ok(ExecuteResponse::SendingRowsImmediate {
                                                rows: Box::new(rows.into_row_iter()),
                                            });
                                        }

                                        return Ok(match kind {
                                            MutationKind::Delete => ExecuteResponse::Deleted(0),
                                            MutationKind::Update => ExecuteResponse::Updated(0),
                                            MutationKind::Insert => unreachable!(),
                                        });
                                    }
                                    ProcessResult::Error(e) => {
                                        let _ = internal_cmd_tx
                                            .send(Message::DropReadThenWriteSubscribe { sink_id });
                                        return Err(e);
                                    }
                                    ProcessResult::ChannelClosed => {
                                        let _ = internal_cmd_tx
                                            .send(Message::DropReadThenWriteSubscribe { sink_id });
                                        return Err(AdapterError::Internal(
                                            "subscribe channel closed".into(),
                                        ));
                                    }
                                }
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                // No more pending messages, we're synced
                                break;
                            }
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                let _ = internal_cmd_tx
                                    .send(Message::DropReadThenWriteSubscribe { sink_id });
                                return Err(AdapterError::Internal(
                                    "subscribe channel closed".into(),
                                ));
                            }
                        }
                    }

                    // Use the latest frontier we've seen
                    let write_ts = state
                        .current_upper
                        .expect("must have seen progress to be ready to write");

                    // Prepare the diffs for writing
                    for (_update, ts, _diff) in state.all_diffs.iter_mut() {
                        *ts = Timestamp::MIN;
                    }
                    consolidation::consolidate_updates(&mut state.all_diffs);

                    // Submit the write
                    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                    let _ = internal_cmd_tx.send(Message::ReadThenWriteSubscribeAttemptWrite {
                        target_id,
                        diffs: state
                            .all_diffs
                            .iter()
                            .map(|(row, _ts, diff)| (row.clone(), diff.clone()))
                            .collect_vec(),
                        write_ts,
                        sink_id,
                        result_tx,
                    });

                    match result_rx.await {
                        Ok(TimestampedWriteResult::Success { .. }) => {
                            let _ =
                                internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });

                            if returning.is_empty() {
                                // No RETURNING clause: return row count
                                let row_count = state
                                    .all_diffs
                                    .iter()
                                    .map(|(_, _, diff)| diff.into_inner().unsigned_abs())
                                    .sum::<u64>();

                                let response = match kind {
                                    MutationKind::Delete => {
                                        ExecuteResponse::Deleted(row_count as usize)
                                    }
                                    MutationKind::Update => {
                                        ExecuteResponse::Updated((row_count / 2) as usize)
                                    }
                                    MutationKind::Insert => {
                                        ExecuteResponse::Inserted(row_count as usize)
                                    }
                                };
                                return Ok(response);
                            }

                            // RETURNING: evaluate expressions against rows
                            let mut returning_rows = Vec::new();
                            let arena = RowArena::new();

                            for (row, _ts, diff) in &state.all_diffs {
                                // For DELETE: evaluate against rows with negative diff
                                // For UPDATE: evaluate against rows with positive diff (new rows)
                                let include = match kind {
                                    MutationKind::Delete => diff.is_negative(),
                                    MutationKind::Update => diff.is_positive(),
                                    MutationKind::Insert => unreachable!(),
                                };

                                if !include {
                                    continue;
                                }

                                let mut returning_row = Row::with_capacity(returning.len());
                                let mut packer = returning_row.packer();
                                let datums: Vec<_> = row.iter().collect();

                                for expr in &returning {
                                    match expr.eval(&datums, &arena) {
                                        Ok(datum) => packer.push(datum),
                                        Err(err) => return Err(err.into()),
                                    }
                                }

                                let multiplicity = NonZeroUsize::try_from(
                                    NonZeroI64::try_from(diff.into_inner().abs())
                                        .expect("diff is non-zero"),
                                )
                                .map_err(AdapterError::from)?;

                                returning_rows.push((returning_row, multiplicity));
                            }

                            // Return rows immediately
                            // Expand multiplicity and collect into Vec<Row>
                            let rows: Vec<Row> = returning_rows
                                .into_iter()
                                .flat_map(|(row, count)| std::iter::repeat(row).take(count.get()))
                                .collect();
                            return Ok(ExecuteResponse::SendingRowsImmediate {
                                rows: Box::new(rows.into_row_iter()),
                            });
                        }
                        Ok(TimestampedWriteResult::TimestampPassed { .. }) => {
                            // Timestamp passed, need to wait for next frontier advance.
                            // The permit is released here (dropped), allowing other loops to try.
                            continue;
                        }
                        Ok(TimestampedWriteResult::Cancelled) => {
                            let _ =
                                internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });
                            return Err(AdapterError::Canceled);
                        }
                        Err(_) => {
                            let _ =
                                internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });
                            return Err(AdapterError::Internal(
                                "write result channel closed".into(),
                            ));
                        }
                    }
                }
                ProcessResult::NoRowsMatched => {
                    let _ = internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });

                    if !returning.is_empty() {
                        // Empty result set for RETURNING
                        let rows: Vec<Row> = vec![];
                        return Ok(ExecuteResponse::SendingRowsImmediate {
                            rows: Box::new(rows.into_row_iter()),
                        });
                    }

                    return Ok(match kind {
                        MutationKind::Delete => ExecuteResponse::Deleted(0),
                        MutationKind::Update => ExecuteResponse::Updated(0),
                        MutationKind::Insert => unreachable!(),
                    });
                }
                ProcessResult::Error(e) => {
                    let _ = internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });
                    return Err(e);
                }
                ProcessResult::ChannelClosed => {
                    let _ = internal_cmd_tx.send(Message::DropReadThenWriteSubscribe { sink_id });
                    return Err(AdapterError::Internal("subscribe channel closed".into()));
                }
            }
        }
    }

    /// Handle the write attempt from the OCC loop.
    pub(crate) async fn handle_read_then_write_subscribe_attempt_write(
        &mut self,
        target_id: CatalogItemId,
        diffs: Vec<(Row, Diff)>,
        write_ts: Timestamp,
        result_tx: tokio::sync::oneshot::Sender<TimestampedWriteResult>,
    ) {
        use crate::coord::appends::PendingWriteTxn;
        use mz_storage_client::client::TableData;
        use smallvec::smallvec;
        use std::collections::BTreeMap;
        use tracing::Span;

        // Early check if timestamp already passed
        let current_write_ts = self.peek_local_write_ts().await;
        if write_ts < current_write_ts {
            let _ = result_tx.send(TimestampedWriteResult::TimestampPassed {
                target_timestamp: write_ts,
                current_write_ts,
            });
            return;
        }

        // Create TableData from accumulated diffs
        let table_data = TableData::Rows(diffs);
        let writes = BTreeMap::from([(target_id, smallvec![table_data])]);

        tracing::trace!(?writes, ?write_ts, "about to attempt read-then-write");

        // Push internal timestamped write directly to pending_writes
        self.pending_writes
            .push(PendingWriteTxn::InternalTimestamped {
                span: Span::current(),
                writes,
                target_timestamp: write_ts,
                result_tx,
            });
        self.trigger_group_commit();
    }

    /// Handle a request to start a read-then-write subscribe.
    ///
    /// This is called from the spawned OCC task after it has acquired the semaphore permit.
    /// We create the subscribe here (on the coordinator) and return the channel to the task.
    ///
    /// The `read_holds` parameter contains the read holds for this specific operation.
    /// They are passed directly through the stages (not via the connection-keyed txn_read_holds map)
    /// to avoid issues where multiple operations on the same connection could interfere with
    /// each other's holds.
    pub(crate) async fn handle_start_read_then_write_subscribe(
        &mut self,
        conn_id: mz_adapter_types::connection::ConnectionId,
        session_uuid: uuid::Uuid,
        df_desc: crate::optimize::LirDataflowDescription,
        cluster_id: mz_compute_types::ComputeInstanceId,
        replica_id: Option<mz_cluster_client::ReplicaId>,
        depends_on: std::collections::BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        start_time: mz_ore::now::EpochMillis,
        response_tx: tokio::sync::oneshot::Sender<mpsc::UnboundedReceiver<PeekResponseUnary>>,
        read_holds: crate::ReadHolds<Timestamp>,
    ) {
        use mz_sql::plan::SubscribeOutput;

        // Check if connection still exists. If not, the client disconnected
        // while waiting for the semaphore - the operation is effectively cancelled.
        if !self.active_conns.contains_key(&conn_id) {
            // Drop response_tx without sending - this signals cancellation to the OCC loop
            // which will return an error when the oneshot receiver fails.
            // The read_holds will be dropped automatically, releasing the holds.
            return;
        }

        // Create the channel for subscribe responses
        let (tx, rx) = mpsc::unbounded_channel();

        let active_subscribe = ActiveSubscribe {
            conn_id: conn_id.clone(),
            session_uuid,
            channel: tx,
            emit_progress: true, // We need progress updates for OCC
            as_of,
            arity,
            cluster_id,
            depends_on,
            start_time,
            output: SubscribeOutput::Diffs, // Output format for diffs
            internal: true,                 // Internal subscribe - skip builtin table updates
        };
        active_subscribe.initialize();

        // Add metadata for the subscribe
        let write_notify_fut = self
            .add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe))
            .await;

        // Ship dataflow
        let ship_dataflow_fut = self.ship_dataflow(df_desc, cluster_id, replica_id);

        let ((), ()) = futures::future::join(write_notify_fut, ship_dataflow_fut).await;

        // Send the receiver back to the OCC loop
        let _ = response_tx.send(rx);

        // The read_holds are dropped here at the end of the function, after ship_dataflow
        // has completed. This ensures the since doesn't advance past our as_of timestamp
        // until the dataflow is running.
        drop(read_holds);
    }

    /// Drop a read-then-write subscribe.
    pub(crate) async fn drop_read_then_write_subscribe(&mut self, sink_id: GlobalId) {
        // Use drop_compute_sink instead of remove_active_compute_sink to also
        // cancel the dataflow on the compute side, not just remove bookkeeping.
        let _ = self.drop_compute_sink(sink_id).await;
    }
}
