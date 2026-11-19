// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A transaction-scoped subscribe over the packed candidate relation.

use mz_compute_types::ComputeInstanceId;
use mz_expr::{CollectionPlan, MirRelationExpr};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};

use crate::AdapterError;
use crate::active_compute_sink::ActiveSubscribeOwner;
use crate::coord::Coordinator;
use crate::coord::peek::PeekResponseUnary;
use crate::optimize::{Optimize, OptimizerConfig, subscribe};

impl Coordinator {
    /// Subscribes to an authorized MIR output, including a snapshot at `as_of`.
    /// Drain the receiver off the coordinator loop. After success, call
    /// `finish_declarative_compute` on every exit, even if the receiver closed.
    pub(super) async fn install_declarative_compute(
        &mut self,
        cluster_id: ComputeInstanceId,
        output: MirRelationExpr,
        as_of: Timestamp,
        emit_plan: bool,
        owner: ActiveSubscribeOwner,
    ) -> Result<
        (
            GlobalId,
            mpsc::UnboundedReceiver<PeekResponseUnary>,
            Option<String>,
        ),
        AdapterError,
    > {
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
        let depends_on = output.depends_on();
        for id in &depends_on {
            if self.catalog().try_get_entry_by_global_id(id).is_none() {
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
        let (_, view) = self.allocate_transient_id();
        let (_, sink_id) = self.allocate_transient_id();
        let mut optimizer = subscribe::Optimizer::new(
            self.owned_catalog(),
            compute,
            view,
            sink_id,
            true,
            None,
            format!("declarative-{sink_id}"),
            config,
            self.optimizer_metrics(),
        );
        let typ = output.sql_typ();
        let arity = typ.arity();
        let desc = RelationDesc::new(typ, (0..arity).map(|i| format!("column{i}")));
        let mir =
            mz_transform::catch_unwind_optimize(|| optimizer.optimize_query(output, desc, vec![]))?;
        let read_holds = self.acquire_read_holds(&mir.id_bundle(cluster_id));
        let since = read_holds.least_valid_read();
        if !since.less_equal(&as_of) {
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "declarative transaction inputs are not readable at {as_of} (since: {since:?})"
            )));
        }
        let lir = optimizer.catch_unwind_optimize(mir.resolve(Antichain::from_elem(as_of)))?;
        let (df, _) = lir.unapply();
        let notice = if emit_plan {
            let text = crate::explain::explain_plan(
                df.clone(),
                mz_repr::explain::ExplainFormat::Text,
                &mz_repr::explain::ExplainConfig {
                    redacted: false,
                    ..Default::default()
                },
                &optimizer.config().features,
                &self.catalog().for_system_session(),
                Default::default(),
                Some(&cluster.name),
            )?;
            Some(format!(
                "Declarative transaction physical plan (subscribe {sink_id}):\n{text}"
            ))
        } else {
            None
        };
        // Invoke the command handler directly: queueing a coordinator command
        // and awaiting its reply on the coordinator loop would deadlock.
        let (tx, rx) = oneshot::channel();
        self.handle_create_internal_subscribe(
            df,
            cluster_id,
            None,
            depends_on,
            as_of,
            arity,
            sink_id,
            owner,
            self.now(),
            read_holds,
            tx,
        )
        .await;
        let rows = rx
            .await
            .expect("internal subscribe handler sent a response")?;
        Ok((sink_id, rows, notice))
    }

    pub(super) async fn finish_declarative_compute(&mut self, sink_id: GlobalId) {
        self.drop_internal_subscribe(sink_id).await;
    }
}
