// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A transaction-scoped compute arrangement for repeated snapshot reads.

use std::sync::Arc;

use futures::stream::BoxStream;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::peek_lifecycle::PeekLifecycle;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{IndexDesc, MirDataflowDescription};
use mz_compute_types::plan::LirRelationExpr;
use mz_expr::{CollectionPlan, MirRelationExpr, MirScalarExpr, RowSetFinishing, SafeMfpPlan};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{RelationDesc, ReprRelationType, Timestamp};
use mz_storage_types::read_holds::ReadHold;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::empty_typechecking_context;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::AdapterError;
use crate::coord::Coordinator;
use crate::coord::peek::{PeekDataflowPlan, PeekResponseUnary};
use crate::optimize::dataflows::{DataflowBuilder, ExprPrep, ExprPrepMaintained};
use crate::optimize::{OptimizerConfig, OptimizerError};

/// Pins the output since at the installation timestamp until explicitly finished.
#[derive(Debug)]
#[must_use = "finish_deterministic_compute must release the controller collection"]
pub(super) struct DeterministicCompute {
    cluster: ComputeInstanceId,
    hold: ReadHold,
    mfp: SafeMfpPlan,
    desc: RelationDesc,
}

impl DeterministicCompute {
    pub(super) fn index_id(&self) -> mz_repr::GlobalId {
        self.hold.id()
    }
}

impl Coordinator {
    /// Installs an authorized relation-valued MIR output on the transaction's resolved cluster.
    /// The inputs must still be readable at `as_of`. Finish the handle on every exit path.
    pub(super) fn install_deterministic_compute(
        &mut self,
        cluster_id: ComputeInstanceId,
        output: MirRelationExpr,
        as_of: Timestamp,
        emit_plan: bool,
    ) -> Result<(DeterministicCompute, Option<String>), AdapterError> {
        let missing_cluster = || AdapterError::ConcurrentDependencyDrop {
            dependency_kind: "cluster",
            dependency_id: cluster_id.to_string(),
        };
        let cluster = self
            .catalog()
            .try_get_cluster(cluster_id)
            .ok_or_else(missing_cluster)?;
        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }
        for id in output.depends_on() {
            if self.catalog().try_get_entry_by_global_id(&id).is_none() {
                return Err(AdapterError::ConcurrentDependencyDrop {
                    dependency_kind: "relation",
                    dependency_id: id.to_string(),
                });
            }
        }
        let compute = self
            .instance_snapshot(cluster_id)
            .map_err(|_| missing_cluster())?;
        let config = OptimizerConfig::from(self.catalog().system_config())
            .override_from(&cluster.config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(cluster_id));
        let (_, view) = self.transient_id_gen.allocate_id();
        let (_, index) = self.transient_id_gen.allocate_id();
        let mut builder = DataflowBuilder::new(self.catalog().state(), compute);
        let typecheck = empty_typechecking_context();
        let mut meta = DataflowMetainfo::default();
        let mut local =
            TransformCtx::local(&config.features, &typecheck, &mut meta, None, Some(view));
        #[allow(deprecated)]
        let output = mz_transform::Optimizer::logical_optimizer(&mut local)
            .optimize(output, &mut local)
            .map_err(OptimizerError::from)?;
        let typ = output.as_inner().sql_typ();
        let arity = typ.arity();
        let mut df = MirDataflowDescription::new(format!("deterministic-{index}"));
        builder.import_view_into_dataflow(&view, &output, &mut df, &config.features)?;
        builder.maybe_reoptimize_imported_views(&mut df, &config)?;
        df.export_index(
            index,
            IndexDesc {
                on_id: view,
                key: typ
                    .default_key()
                    .into_iter()
                    .map(MirScalarExpr::column)
                    .collect(),
            },
            ReprRelationType::from(&typ),
        );
        df.visit_children(
            |r| ExprPrepMaintained.prep_relation_expr(r),
            |s| ExprPrepMaintained.prep_scalar_expr(s),
        )?;
        df.set_as_of(Antichain::from_elem(as_of));
        let mut global = TransformCtx::global(
            &builder,
            &mz_transform::EmptyStatisticsOracle,
            &config.features,
            &typecheck,
            &mut meta,
            None,
        );
        mz_transform::optimize_dataflow(&mut df, &mut global, false)
            .map_err(OptimizerError::from)?;
        for build in &mut df.objects_to_build {
            normalize_lets(&mut build.plan.0, &config.features).map_err(OptimizerError::from)?;
        }
        let df = LirRelationExpr::finalize_dataflow(df, &config.features, None)
            .map_err(OptimizerError::from)?;
        let notice = if emit_plan {
            let text = crate::explain::explain_plan(
                df.clone(),
                mz_repr::explain::ExplainFormat::Text,
                &mz_repr::explain::ExplainConfig {
                    redacted: true,
                    ..Default::default()
                },
                &config.features,
                &self.catalog().for_system_session(),
                Default::default(),
                Some(&cluster.name),
            )?;
            Some(format!(
                "Deterministic transaction physical plan (arrangement {index}):\n{text}"
            ))
        } else {
            None
        };
        let plan = PeekDataflowPlan::new(df, index, &typ);
        let desc = RelationDesc::new(typ, (0..arity).map(|i| format!("peek_{i}")));
        let (_, hold, mfp) = self.create_transient_peek_target(plan, arity, cluster_id)?;
        Ok((
            DeterministicCompute {
                cluster: cluster_id,
                hold,
                mfp,
                desc,
            },
            notice,
        ))
    }

    /// Queues a full snapshot read. Consume the stream to EOF off the coordinator loop.
    pub(super) fn peek_deterministic_compute(
        &self,
        output: &DeterministicCompute,
        timestamp: Timestamp,
        target_replica: Option<ReplicaId>,
        max_query_result_size: Option<u64>,
    ) -> Result<BoxStream<'static, PeekResponseUnary>, AdapterError> {
        let hold = output
            .hold
            .try_clone()
            .map_err(|_| AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "cluster",
                dependency_id: output.cluster.to_string(),
            })?;
        let finishing = RowSetFinishing::trivial(output.desc.arity());
        let (tx, rx) = oneshot::channel();
        self.controller
            .compute
            .peek(
                output.cluster,
                PeekTarget::Index { id: hold.id() },
                None,
                Uuid::new_v4(),
                timestamp,
                output.desc.clone(),
                finishing.clone(),
                output.mfp.clone(),
                hold,
                target_replica,
                tx,
                // The enclosing commit retires the statement after publication.
                // Consume this internal read's controller notification here.
                Some(Arc::new(PeekLifecycle::new(|_| {}))),
            )
            .map_err(AdapterError::concurrent_dependency_drop_from_peek_error)?;
        let vars = self.catalog().system_config();
        Ok(Box::pin(crate::peek_client::create_peek_response_stream(
            rx,
            finishing,
            vars.max_result_size(),
            max_query_result_size,
            self.metrics.row_set_finishing_seconds(),
            self.persist_client.clone(),
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                .get(vars.dyncfgs()),
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                .get(vars.dyncfgs()),
        )))
    }

    /// Drops the arrangement. Already queued reads retain their own read holds.
    pub(super) fn finish_deterministic_compute(&mut self, output: DeterministicCompute) {
        self.drop_compute_collections(vec![(output.cluster, output.hold.id())]);
    }
}
