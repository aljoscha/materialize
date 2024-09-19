// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module encapsulates all of the [`Coordinator`]'s logic for creating, dropping,
//! and altering objects.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::Future;
use maplit::{btreemap, btreeset};
use mz_adapter_types::compaction::SINCE_GRANULARITY;
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::VersionedEvent;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Sink};
use mz_catalog::SYSTEM_CONN_ID;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::instrument;
use mz_ore::now::to_datetime;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogSchema;
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{
    self, SystemVars, Var, MAX_AWS_PRIVATELINK_CONNECTIONS, MAX_CLUSTERS,
    MAX_CREDIT_CONSUMPTION_RATE, MAX_DATABASES, MAX_KAFKA_CONNECTIONS, MAX_MATERIALIZED_VIEWS,
    MAX_MYSQL_CONNECTIONS, MAX_OBJECTS_PER_SCHEMA, MAX_POSTGRES_CONNECTIONS,
    MAX_REPLICAS_PER_CLUSTER, MAX_ROLES, MAX_SCHEMAS_PER_DATABASE, MAX_SECRETS, MAX_SINKS,
    MAX_SOURCES, MAX_TABLES,
};
use mz_storage_client::controller::ExportDescription;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::read_policy::ReadPolicy;
use serde_json::json;
use tracing::{event, info_span, Instrument, Level};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::side_effects::CatalogSideEffect;
use crate::catalog::{DropObjectInfo, Op, ReplicaCreateDropReason, TransactionResult};
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::{Coordinator, ReplicaMetadata};
use crate::session::{Session, Transaction, TransactionOps};
use crate::telemetry::{EventDetails, SegmentClientExt};
use crate::util::ResultExt;
use crate::{catalog, flags, AdapterError, ExecuteContext, TimestampProvider};

impl Coordinator {
    /// Same as [`Self::catalog_transact_conn`] but takes a [`Session`].
    ///
    /// This asserts that there are no side effects being generated from
    /// applying the given `ops`.
    #[instrument(name = "coord::catalog_transact")]
    pub(crate) async fn catalog_transact(
        &mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        self.catalog_transact_conn(session.map(|session| session.conn_id()), ops)
            .await
    }

    /// Same as [`Self::catalog_transact_conn`] but takes a [`Session`] and runs
    /// builtin table updates concurrently with any side effects (e.g. creating
    /// collections).
    ///
    /// This asserts that there are no side effects being generated from
    /// applying the given `ops`.
    // TODO(aljoscha): Remove this method once all call-sites have been migrated
    // to the newer catalog_transact_and_apply_side_effects. The latter is what
    // allows us to apply side effects to the controller either when initially
    // applying the ops to the catalog _or_ when following catalog changes from
    // another process.
    #[instrument(name = "coord::catalog_transact_with_side_effects")]
    pub(crate) async fn catalog_transact_with_side_effects<'c, F, Fut>(
        &'c mut self,
        session: Option<&Session>,
        ops: Vec<catalog::Op>,
        side_effect: F,
    ) -> Result<(), AdapterError>
    where
        F: FnOnce(&'c mut Coordinator) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (table_updates, derived_side_effects) = self
            .catalog_transact_inner(session.map(|session| session.conn_id()), ops)
            .await?;

        assert!(derived_side_effects.is_empty(), "cannot apply ops that produce side effects when using catalog_transact_with_side_effects, but got: {:?}", derived_side_effects);

        let side_effects_fut = side_effect(self);

        // Run our side effects concurrently with the table updates.
        let ((), ()) = futures::future::join(
            side_effects_fut.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::side_effects_fut"
            )),
            table_updates.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::table_updates"
            )),
        )
        .await;

        Ok(())
    }

    /// Same as [`Self::catalog_transact_conn`] but takes a [`Session`] and runs
    /// builtin table updates concurrently with any side effects that are
    /// generated as part of applying the given `ops` (e.g. creating
    /// collections).
    #[instrument(name = "coord::catalog_transact_and_apply_side_effects")]
    pub(crate) async fn catalog_transact_and_apply_side_effects(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let conn_id = ctx.as_ref().map(|ctx| ctx.session().conn_id());
        let (table_updates, derived_side_effects) =
            self.catalog_transact_inner(conn_id, ops).await?;

        let side_effects_fut = self.controller_apply_side_effects(ctx, derived_side_effects);

        // Run our side effects concurrently with the table updates.
        let (side_effects_res, ()) = futures::future::join(
            side_effects_fut.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::side_effects_fut"
            )),
            table_updates.instrument(info_span!(
                "coord::catalog_transact_with_side_effects::table_updates"
            )),
        )
        .await;

        // We would get into an inconsistent state if we updated the catalog but
        // then failed to apply the side effects to the controller. Easiest
        // thing to do is panic and let restart/bootstrap handle it.
        side_effects_res.expect("cannot fail to apply side effects");

        // WIP: Do we want to do this check here? We can't do it inside
        // catalog_transact_inner anymore because we only apply the side effects
        // out here.

        // Note: It's important that we keep the function call inside macro, this way we only run
        // the consistency checks if sort assertions are enabled.
        mz_ore::soft_assert_eq_no_log!(
            self.check_consistency(),
            Ok(()),
            "coordinator inconsistency detected"
        );

        Ok(())
    }

    /// Same as [`Self::catalog_transact_inner`] but awaits the table updates.
    ///
    /// This asserts that there are no side effects being generated from
    /// applying the given `ops`.
    #[instrument(name = "coord::catalog_transact_conn")]
    pub(crate) async fn catalog_transact_conn(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let (table_updates, derived_side_effects) =
            self.catalog_transact_inner(conn_id, ops).await?;

        assert!(derived_side_effects.is_empty(), "cannot apply ops that produce side effects when using catalog_transact_with_side_effects, but got: {:?}", derived_side_effects);

        table_updates
            .instrument(info_span!("coord::catalog_transact_conn::table_updates"))
            .await;

        Ok(())
    }

    /// Executes a Catalog transaction with handling if the provided [`Session`]
    /// is in a SQL transaction that is executing DDL.
    #[instrument(name = "coord::catalog_transact_with_ddl_transaction")]
    pub(crate) async fn catalog_transact_with_ddl_transaction(
        &mut self,
        session: &mut Session,
        ops: Vec<catalog::Op>,
    ) -> Result<(), AdapterError> {
        let Some(Transaction {
            ops:
                TransactionOps::DDL {
                    ops: txn_ops,
                    revision: txn_revision,
                    state: _,
                },
            ..
        }) = session.transaction().inner()
        else {
            return self.catalog_transact(Some(session), ops).await;
        };

        // Make sure our Catalog hasn't changed since openning the transaction.
        if self.catalog().transient_revision() != *txn_revision {
            return Err(AdapterError::DDLTransactionRace);
        }

        // Combine the existing ops with the new ops so we can replay them.
        let mut all_ops = Vec::with_capacity(ops.len() + txn_ops.len() + 1);
        all_ops.extend(txn_ops.iter().cloned());
        all_ops.extend(ops.clone());
        all_ops.push(Op::TransactionDryRun);

        // Run our Catalog transaction, but abort before committing.
        let result = self.catalog_transact(Some(session), all_ops).await;

        match result {
            // We purposefully fail with this error to prevent committing the transaction.
            Err(AdapterError::TransactionDryRun { new_ops, new_state }) => {
                // Sets these ops to our transaction, bailing if the Catalog has changed since we
                // ran the transaction.
                session.transaction_mut().add_ops(TransactionOps::DDL {
                    ops: new_ops,
                    state: new_state,
                    revision: self.catalog().transient_revision(),
                })?;
                Ok(())
            }
            Ok(_) => unreachable!("unexpected success!"),
            Err(e) => Err(e),
        }
    }

    /// Perform a catalog transaction. [`Coordinator::ship_dataflow`] must be
    /// called after this function successfully returns on any built
    /// [`DataflowDesc`](mz_compute_types::dataflows::DataflowDesc).
    #[instrument(name = "coord::catalog_transact_inner")]
    pub(crate) async fn catalog_transact_inner<'a>(
        &mut self,
        conn_id: Option<&ConnectionId>,
        ops: Vec<catalog::Op>,
    ) -> Result<(BuiltinTableAppendNotify, Vec<CatalogSideEffect>), AdapterError> {
        if self.controller.read_only() {
            return Err(AdapterError::ReadOnly);
        }

        event!(Level::TRACE, ops = format!("{:?}", ops));

        let mut webhook_sources_to_restart = BTreeSet::new();
        let mut clusters_to_drop = vec![];
        let mut cluster_replicas_to_drop = vec![];
        let mut clusters_to_create = vec![];
        let mut cluster_replicas_to_create = vec![];
        let mut update_tracing_config = false;
        let mut update_compute_config = false;
        let mut update_storage_config = false;
        let mut update_pg_timestamp_oracle_config = false;
        let mut update_metrics_retention = false;
        let mut update_secrets_caching_config = false;
        let mut update_cluster_scheduling_config = false;
        let mut update_arrangement_exert_proportionality = false;
        let mut update_http_config = false;

        for op in &ops {
            match op {
                catalog::Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match &drop_object_info {
                            catalog::DropObjectInfo::Item(_) => {
                                // Nothing to do, these will be handled by
                                // applying the side effects that we return.
                            }
                            catalog::DropObjectInfo::Cluster(id) => {
                                clusters_to_drop.push(*id);
                            }
                            catalog::DropObjectInfo::ClusterReplica((
                                cluster_id,
                                replica_id,
                                _reason,
                            )) => {
                                // Drop the cluster replica itself.
                                cluster_replicas_to_drop.push((*cluster_id, *replica_id));
                            }
                            _ => (),
                        }
                    }
                }
                catalog::Op::ResetSystemConfiguration { name }
                | catalog::Op::UpdateSystemConfiguration { name, .. } => {
                    update_tracing_config |= vars::is_tracing_var(name);
                    update_compute_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_compute_config_var(name);
                    update_storage_config |= self
                        .catalog
                        .state()
                        .system_config()
                        .is_storage_config_var(name);
                    update_pg_timestamp_oracle_config |=
                        vars::is_pg_timestamp_oracle_config_var(name);
                    update_metrics_retention |= name == vars::METRICS_RETENTION.name();
                    update_secrets_caching_config |= vars::is_secrets_caching_var(name);
                    update_cluster_scheduling_config |= vars::is_cluster_scheduling_var(name);
                    update_arrangement_exert_proportionality |=
                        name == vars::ARRANGEMENT_EXERT_PROPORTIONALITY.name();
                    update_http_config |= vars::is_http_config_var(name);
                }
                catalog::Op::ResetAllSystemConfiguration => {
                    // Assume they all need to be updated.
                    // We could see if the config's have actually changed, but
                    // this is simpler.
                    update_tracing_config = true;
                    update_compute_config = true;
                    update_storage_config = true;
                    update_pg_timestamp_oracle_config = true;
                    update_metrics_retention = true;
                    update_secrets_caching_config = true;
                    update_cluster_scheduling_config = true;
                    update_arrangement_exert_proportionality = true;
                    update_http_config = true;
                }
                catalog::Op::RenameItem { id, .. } => {
                    let item = self.catalog().get_entry(id);
                    let is_webhook_source = item
                        .source()
                        .map(|s| matches!(s.data_source, DataSourceDesc::Webhook { .. }))
                        .unwrap_or(false);
                    if is_webhook_source {
                        webhook_sources_to_restart.insert(*id);
                    }
                }
                catalog::Op::RenameSchema {
                    database_spec,
                    schema_spec,
                    ..
                } => {
                    let schema = self.catalog().get_schema(
                        database_spec,
                        schema_spec,
                        conn_id.unwrap_or(&SYSTEM_CONN_ID),
                    );
                    let webhook_sources = schema.item_ids().filter(|id| {
                        let item = self.catalog().get_entry(id);
                        item.source()
                            .map(|s| matches!(s.data_source, DataSourceDesc::Webhook { .. }))
                            .unwrap_or(false)
                    });
                    webhook_sources_to_restart.extend(webhook_sources);
                }
                catalog::Op::CreateCluster { id, .. } => {
                    clusters_to_create.push(*id);
                }
                catalog::Op::CreateClusterReplica {
                    cluster_id,
                    id,
                    config,
                    ..
                } => {
                    cluster_replicas_to_create.push((
                        *cluster_id,
                        *id,
                        config.location.num_processes(),
                    ));
                }
                _ => (),
            }
        }

        self.validate_resource_limits(&ops, conn_id.unwrap_or(&SYSTEM_CONN_ID))?;

        // This will produce timestamps that are guaranteed to increase on each
        // call, and also never be behind the system clock. If the system clock
        // hasn't advanced (or has gone backward), it will increment by 1. For
        // the audit log, we need to balance "close (within 10s or so) to the
        // system clock" and "always goes up". We've chosen here to prioritize
        // always going up, and believe we will always be close to the system
        // clock because it is well configured (chrony) and so may only rarely
        // regress or pause for 10s.
        let oracle_write_ts = self.get_local_write_ts().await.timestamp;

        let Coordinator {
            catalog,
            active_conns,
            controller,
            cluster_replica_statuses,
            ..
        } = self;
        let catalog = Arc::make_mut(catalog);
        let conn = conn_id.map(|id| active_conns.get(id).expect("connection must exist"));

        let TransactionResult {
            mut builtin_table_updates,
            side_effects,
            audit_events,
        } = catalog
            .transact(Some(&mut *controller.storage), oracle_write_ts, conn, ops)
            .await?;

        // Update in-memory cluster replica statuses.
        // TODO(jkosh44) All these builtin table updates should be handled as a builtin source
        // updates elsewhere.
        for (cluster_id, replica_id) in &cluster_replicas_to_drop {
            let replica_statuses =
                cluster_replica_statuses.remove_cluster_replica_statuses(cluster_id, replica_id);
            for (process_id, status) in replica_statuses {
                let builtin_table_update = catalog.state().pack_cluster_replica_status_update(
                    *replica_id,
                    process_id,
                    &status,
                    -1,
                );
                let builtin_table_update = catalog
                    .state()
                    .resolve_builtin_table_update(builtin_table_update);
                builtin_table_updates.push(builtin_table_update);
            }
        }
        for cluster_id in &clusters_to_drop {
            let cluster_statuses = cluster_replica_statuses.remove_cluster_statuses(cluster_id);
            for (replica_id, replica_statuses) in cluster_statuses {
                for (process_id, status) in replica_statuses {
                    let builtin_table_update = catalog
                        .state()
                        .pack_cluster_replica_status_update(replica_id, process_id, &status, -1);
                    let builtin_table_update = catalog
                        .state()
                        .resolve_builtin_table_update(builtin_table_update);
                    builtin_table_updates.push(builtin_table_update);
                }
            }
        }
        for cluster_id in clusters_to_create {
            cluster_replica_statuses.initialize_cluster_statuses(cluster_id);
            for (replica_id, replica_statuses) in
                cluster_replica_statuses.get_cluster_statuses(cluster_id)
            {
                for (process_id, status) in replica_statuses {
                    let builtin_table_update = catalog.state().pack_cluster_replica_status_update(
                        *replica_id,
                        *process_id,
                        status,
                        1,
                    );
                    let builtin_table_update = catalog
                        .state()
                        .resolve_builtin_table_update(builtin_table_update);
                    builtin_table_updates.push(builtin_table_update);
                }
            }
        }
        let now = to_datetime((catalog.config().now)());
        for (cluster_id, replica_id, num_processes) in cluster_replicas_to_create {
            cluster_replica_statuses.initialize_cluster_replica_statuses(
                cluster_id,
                replica_id,
                num_processes,
                now,
            );
            for (process_id, status) in
                cluster_replica_statuses.get_cluster_replica_statuses(cluster_id, replica_id)
            {
                let builtin_table_update = catalog.state().pack_cluster_replica_status_update(
                    replica_id,
                    *process_id,
                    status,
                    1,
                );
                let builtin_table_update = catalog
                    .state()
                    .resolve_builtin_table_update(builtin_table_update);
                builtin_table_updates.push(builtin_table_update);
            }
        }

        // Append our builtin table updates, then return the notify so we can run other tasks in
        // parallel.
        let builtin_update_notify = self
            .builtin_table_update()
            .execute(builtin_table_updates)
            .await;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            if !webhook_sources_to_restart.is_empty() {
                self.restart_webhook_sources(webhook_sources_to_restart);
            }

            if !cluster_replicas_to_drop.is_empty() {
                fail::fail_point!("after_catalog_drop_replica");
                for (cluster_id, replica_id) in cluster_replicas_to_drop {
                    self.drop_replica_pack_builtin_updates(cluster_id, replica_id);
                }
            }

            if update_compute_config {
                self.update_compute_config();
            }
            if update_storage_config {
                self.update_storage_config();
            }
            if update_pg_timestamp_oracle_config {
                self.update_pg_timestamp_oracle_config();
            }
            if update_metrics_retention {
                self.update_metrics_retention();
            }
            if update_tracing_config {
                self.update_tracing_config();
            }
            if update_secrets_caching_config {
                self.update_secrets_caching_config();
            }
            if update_cluster_scheduling_config {
                self.update_cluster_scheduling_config();
            }
            if update_arrangement_exert_proportionality {
                self.update_arrangement_exert_proportionality();
            }
            if update_http_config {
                self.update_http_config();
            }
        }
        .instrument(info_span!("coord::catalog_transact_with::finalize"))
        .await;

        let conn = conn_id.and_then(|id| self.active_conns.get(id));
        if let Some(segment_client) = &self.segment_client {
            for VersionedEvent::V1(event) in audit_events {
                let event_type = format!(
                    "{} {}",
                    event.object_type.as_title_case(),
                    event.event_type.as_title_case()
                );
                segment_client.environment_track(
                    &self.catalog().config().environment_id,
                    event_type,
                    json!({ "details": event.details.as_json() }),
                    EventDetails {
                        user_id: conn
                            .and_then(|c| c.user().external_metadata.as_ref())
                            .map(|m| m.user_id),
                        application_name: conn.map(|c| c.application_name()),
                        ..Default::default()
                    },
                );
            }
        }

        Ok((builtin_update_notify, side_effects))
    }

    fn drop_replica_pack_builtin_updates(&mut self, _cluster_id: ClusterId, replica_id: ReplicaId) {
        if let Some(Some(ReplicaMetadata { metrics })) =
            self.transient_replica_metadata.insert(replica_id, None)
        {
            let mut updates = vec![];
            if let Some(metrics) = metrics {
                let retractions = self
                    .catalog()
                    .state()
                    .pack_replica_metric_updates(replica_id, &metrics, -1);
                let retractions = self
                    .catalog()
                    .state()
                    .resolve_builtin_table_updates(retractions);
                updates.extend(retractions);
            }
            self.builtin_table_update().background(updates);
        }
    }

    fn restart_webhook_sources(&mut self, sources: impl IntoIterator<Item = GlobalId>) {
        for id in sources {
            self.active_webhooks.remove(&id);
        }
    }

    /// Like `drop_compute_sinks`, but for a single compute sink.
    ///
    /// Returns the controller's state for the compute sink if the identified
    /// sink was known to the controller. It is the caller's responsibility to
    /// retire the returned sink. Consider using `retire_compute_sinks` instead.
    #[must_use]
    pub async fn drop_compute_sink(&mut self, sink_id: GlobalId) -> Option<ActiveComputeSink> {
        self.drop_compute_sinks([sink_id]).await.remove(&sink_id)
    }

    /// Drops a batch of compute sinks.
    ///
    /// For each sink that exists, the coordinator and controller's state
    /// associated with the sink is removed.
    ///
    /// Returns a map containing the controller's state for each sink that was
    /// removed. It is the caller's responsibility to retire the returned sinks.
    /// Consider using `retire_compute_sinks` instead.
    #[must_use]
    pub async fn drop_compute_sinks(
        &mut self,
        sink_ids: impl IntoIterator<Item = GlobalId>,
    ) -> BTreeMap<GlobalId, ActiveComputeSink> {
        let mut by_id = BTreeMap::new();
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for sink_id in sink_ids {
            let sink = match self.remove_active_compute_sink(sink_id).await {
                None => {
                    tracing::error!(%sink_id, "drop_compute_sinks called on nonexistent sink");
                    continue;
                }
                Some(sink) => sink,
            };

            by_cluster
                .entry(sink.cluster_id())
                .or_default()
                .push(sink_id);
            by_id.insert(sink_id, sink);
        }
        for (cluster_id, ids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }
        by_id
    }

    /// Retires a batch of sinks with disparate reasons for retirement.
    ///
    /// Each sink identified in `reasons` is dropped (see `drop_compute_sinks`),
    /// then retired with its corresponding reason.
    pub async fn retire_compute_sinks(
        &mut self,
        mut reasons: BTreeMap<GlobalId, ActiveComputeSinkRetireReason>,
    ) {
        let sink_ids = reasons.keys().cloned();
        for (id, sink) in self.drop_compute_sinks(sink_ids).await {
            let reason = reasons
                .remove(&id)
                .expect("all returned IDs are in `reasons`");
            sink.retire(reason);
        }
    }

    /// Drops all pending replicas for a set of clusters
    /// that are undergoing reconfiguration.
    pub async fn drop_reconfiguration_replicas(
        &mut self,
        cluster_ids: BTreeSet<ClusterId>,
    ) -> Result<(), AdapterError> {
        let pending_cluster_ops: Vec<Op> = cluster_ids
            .iter()
            .map(|c| {
                self.catalog()
                    .get_cluster(c.clone())
                    .replicas()
                    .filter_map(|r| match r.config.location {
                        ReplicaLocation::Managed(ref l) if l.pending => {
                            Some(DropObjectInfo::ClusterReplica((
                                c.clone(),
                                r.replica_id,
                                ReplicaCreateDropReason::Manual,
                            )))
                        }
                        _ => None,
                    })
                    .collect::<Vec<DropObjectInfo>>()
            })
            .filter_map(|pending_replica_drop_ops_by_cluster| {
                match pending_replica_drop_ops_by_cluster.len() {
                    0 => None,
                    _ => Some(Op::DropObjects(pending_replica_drop_ops_by_cluster)),
                }
            })
            .collect();
        if !pending_cluster_ops.is_empty() {
            self.catalog_transact(None, pending_cluster_ops).await?;
        }
        Ok(())
    }

    /// Cancels all active compute sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_compute_sinks_for_conn(&mut self, conn_id: &ConnectionId) {
        self.retire_compute_sinks_for_conn(conn_id, ActiveComputeSinkRetireReason::Canceled)
            .await
    }

    /// Cancels all active cluster reconfigurations sinks for the identified connection.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn cancel_cluster_reconfigurations_for_conn(
        &mut self,
        conn_id: &ConnectionId,
    ) {
        self.retire_cluster_reconfigurations_for_conn(conn_id).await
    }

    /// Retires all active compute sinks for the identified connection with the
    /// specified reason.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn retire_compute_sinks_for_conn(
        &mut self,
        conn_id: &ConnectionId,
        reason: ActiveComputeSinkRetireReason,
    ) {
        let drop_sinks = self
            .active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .drop_sinks
            .iter()
            .map(|sink_id| (*sink_id, reason.clone()))
            .collect();
        self.retire_compute_sinks(drop_sinks).await;
    }

    /// Cleans pending cluster reconfiguraiotns for the identified connection
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn retire_cluster_reconfigurations_for_conn(
        &mut self,
        conn_id: &ConnectionId,
    ) {
        let reconfiguring_clusters = self
            .active_conns
            .get(conn_id)
            .expect("must exist for active session")
            .pending_cluster_alters
            .clone();
        // try to drop reconfig replicas
        self.drop_reconfiguration_replicas(reconfiguring_clusters)
            .await
            .unwrap_or_terminate("cannot fail to drop reconfiguration replicas");

        self.active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .pending_cluster_alters
            .clear();
    }

    pub(crate) fn drop_indexes(&mut self, indexes: Vec<(ClusterId, GlobalId)>) {
        let mut by_cluster: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (cluster_id, id) in indexes {
            by_cluster.entry(cluster_id).or_default().push(id);
        }
        for (cluster_id, ids) in by_cluster {
            let compute = &mut self.controller.compute;
            // A cluster could have been dropped, so verify it exists.
            if compute.instance_exists(cluster_id) {
                compute
                    .drop_collections(cluster_id, ids)
                    .unwrap_or_terminate("cannot fail to drop collections");
            }
        }
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    pub(crate) async fn drop_temp_items(&mut self, conn_id: &ConnectionId) {
        let temp_items = self.catalog().state().get_temp_items(conn_id).collect();
        let all_items = self.catalog().object_dependents(&temp_items, conn_id);

        if all_items.is_empty() {
            return;
        }
        let op = Op::DropObjects(
            all_items
                .into_iter()
                .map(DropObjectInfo::manual_drop_from_object_id)
                .collect(),
        );

        self.catalog_transact_conn(Some(conn_id), vec![op])
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    fn update_cluster_scheduling_config(&mut self) {
        let config = flags::orchestrator_scheduling_config(self.catalog.system_config());
        self.controller
            .update_orchestrator_scheduling_config(config);
    }

    fn update_secrets_caching_config(&mut self) {
        let config = flags::caching_config(self.catalog.system_config());
        self.caching_secrets_reader.set_policy(config);
    }

    fn update_tracing_config(&mut self) {
        let tracing = flags::tracing_config(self.catalog().system_config());
        tracing.apply(&self.tracing_handle);
    }

    fn update_compute_config(&mut self) {
        let config_params = flags::compute_config(self.catalog().system_config());
        self.controller.compute.update_configuration(config_params);
    }

    fn update_storage_config(&mut self) {
        let config_params = flags::storage_config(self.catalog().system_config());
        self.controller.storage.update_parameters(config_params);
    }

    fn update_pg_timestamp_oracle_config(&mut self) {
        let config_params = flags::pg_timstamp_oracle_config(self.catalog().system_config());
        if let Some(config) = self.pg_timestamp_oracle_config.as_ref() {
            config_params.apply(config)
        }
    }

    fn update_metrics_retention(&mut self) {
        let duration = self.catalog().system_config().metrics_retention();
        let policy = ReadPolicy::lag_writes_by(
            Timestamp::new(u64::try_from(duration.as_millis()).unwrap_or_else(|_e| {
                tracing::error!("Absurd metrics retention duration: {duration:?}.");
                u64::MAX
            })),
            SINCE_GRANULARITY,
        );
        let storage_policies = self
            .catalog()
            .entries()
            .filter(|entry| {
                entry.item().is_retained_metrics_object()
                    && entry.item().is_compute_object_on_cluster().is_none()
            })
            .map(|entry| (entry.id(), policy.clone()))
            .collect::<Vec<_>>();
        let compute_policies = self
            .catalog()
            .entries()
            .filter_map(|entry| {
                if let (true, Some(cluster_id)) = (
                    entry.item().is_retained_metrics_object(),
                    entry.item().is_compute_object_on_cluster(),
                ) {
                    Some((cluster_id, entry.id(), policy.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        self.update_storage_read_policies(storage_policies);
        self.update_compute_read_policies(compute_policies);
    }

    fn update_arrangement_exert_proportionality(&mut self) {
        let prop = self
            .catalog()
            .system_config()
            .arrangement_exert_proportionality();
        self.controller
            .compute
            .set_arrangement_exert_proportionality(prop);
    }

    fn update_http_config(&mut self) {
        let webhook_request_limit = self
            .catalog()
            .system_config()
            .webhook_concurrent_request_limit();
        self.webhook_concurrency_limit
            .set_limit(webhook_request_limit);
    }

    pub(crate) async fn create_storage_export(
        &mut self,
        id: GlobalId,
        sink: &Sink,
    ) -> Result<(), AdapterError> {
        // Validate `sink.from` is in fact a storage collection
        self.controller.storage.check_exists(sink.from)?;

        let status_id = Some(
            self.catalog()
                .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_SINK_STATUS_HISTORY),
        );

        // The AsOf is used to determine at what time to snapshot reading from
        // the persist collection.  This is primarily relevant when we do _not_
        // want to include the snapshot in the sink.
        //
        // We choose the smallest as_of that is legal, according to the sinked
        // collection's since.
        let id_bundle = crate::CollectionIdBundle {
            storage_ids: btreeset! {sink.from},
            compute_ids: btreemap! {},
        };

        // We're putting in place read holds, such that create_exports, below,
        // which calls update_read_capabilities, can successfully do so.
        // Otherwise, the since of dependencies might move along concurrently,
        // pulling the rug from under us!
        //
        // TODO: Maybe in the future, pass those holds on to storage, to hold on
        // to them and downgrade when possible?
        let read_holds = self.acquire_read_holds(&id_bundle);
        let as_of = self.least_valid_read(&read_holds);

        let storage_sink_from_entry = self.catalog().get_entry(&sink.from);
        let storage_sink_desc = mz_storage_types::sinks::StorageSinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog().resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .expect("indexes can only be built on items with descs")
                .into_owned(),
            connection: sink
                .connection
                .clone()
                .into_inline_connection(self.catalog().state()),
            partition_strategy: sink.partition_strategy.clone(),
            envelope: sink.envelope,
            as_of,
            with_snapshot: sink.with_snapshot,
            version: sink.version,
            status_id,
            from_storage_metadata: (),
        };

        let res = self
            .controller
            .storage
            .create_exports(vec![(
                id,
                ExportDescription {
                    sink: storage_sink_desc,
                    instance_id: sink.cluster_id,
                },
            )])
            .await;

        // Drop read holds after the export has been created, at which point
        // storage will have put in its own read holds.
        drop(read_holds);

        Ok(res?)
    }

    /// Validate all resource limits in a catalog transaction and return an error if that limit is
    /// exceeded.
    fn validate_resource_limits(
        &self,
        ops: &Vec<catalog::Op>,
        conn_id: &ConnectionId,
    ) -> Result<(), AdapterError> {
        let mut new_kafka_connections = 0;
        let mut new_postgres_connections = 0;
        let mut new_mysql_connections = 0;
        let mut new_aws_privatelink_connections = 0;
        let mut new_tables = 0;
        let mut new_sources = 0;
        let mut new_sinks = 0;
        let mut new_materialized_views = 0;
        let mut new_clusters = 0;
        let mut new_replicas_per_cluster = BTreeMap::new();
        let mut new_credit_consumption_rate = Numeric::zero();
        let mut new_databases = 0;
        let mut new_schemas_per_database = BTreeMap::new();
        let mut new_objects_per_schema = BTreeMap::new();
        let mut new_secrets = 0;
        let mut new_roles = 0;
        for op in ops {
            match op {
                Op::CreateDatabase { .. } => {
                    new_databases += 1;
                }
                Op::CreateSchema { database_id, .. } => {
                    if let ResolvedDatabaseSpecifier::Id(database_id) = database_id {
                        *new_schemas_per_database.entry(database_id).or_insert(0) += 1;
                    }
                }
                Op::CreateRole { .. } => {
                    new_roles += 1;
                }
                Op::CreateCluster { .. } => {
                    // TODO(benesch): having deprecated linked clusters, remove
                    // the `max_sources` and `max_sinks` limit, and set a higher
                    // max cluster limit?
                    new_clusters += 1;
                }
                Op::CreateClusterReplica {
                    cluster_id, config, ..
                } => {
                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) += 1;
                    if let ReplicaLocation::Managed(location) = &config.location {
                        let replica_allocation = self
                            .catalog()
                            .cluster_replica_sizes()
                            .0
                            .get(location.size_for_billing())
                            .expect("location size is validated against the cluster replica sizes");
                        new_credit_consumption_rate += replica_allocation.credits_per_hour
                    }
                }
                Op::CreateItem { name, item, .. } => {
                    *new_objects_per_schema
                        .entry((
                            name.qualifiers.database_spec.clone(),
                            name.qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) += 1;
                    match item {
                        CatalogItem::Connection(connection) => {
                            use mz_storage_types::connections::Connection;
                            match connection.connection {
                                Connection::Kafka(_) => new_kafka_connections += 1,
                                Connection::Postgres(_) => new_postgres_connections += 1,
                                Connection::MySql(_) => new_mysql_connections += 1,
                                Connection::AwsPrivatelink(_) => {
                                    new_aws_privatelink_connections += 1
                                }
                                Connection::Csr(_) | Connection::Ssh(_) | Connection::Aws(_) => {}
                            }
                        }
                        CatalogItem::Table(_) => {
                            new_tables += 1;
                        }
                        CatalogItem::Source(source) => {
                            new_sources += source.user_controllable_persist_shard_count()
                        }
                        CatalogItem::Sink(_) => new_sinks += 1,
                        CatalogItem::MaterializedView(_) => {
                            new_materialized_views += 1;
                        }
                        CatalogItem::Secret(_) => {
                            new_secrets += 1;
                        }
                        CatalogItem::Log(_)
                        | CatalogItem::View(_)
                        | CatalogItem::Index(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_) => {}
                    }
                }
                Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match drop_object_info {
                            DropObjectInfo::Cluster(_) => {
                                new_clusters -= 1;
                            }
                            DropObjectInfo::ClusterReplica((cluster_id, replica_id, _reason)) => {
                                *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) -= 1;
                                let cluster =
                                    self.catalog().get_cluster_replica(*cluster_id, *replica_id);
                                if let ReplicaLocation::Managed(location) = &cluster.config.location
                                {
                                    let replica_allocation = self
                                .catalog()
                                .cluster_replica_sizes()
                                .0
                                .get(location.size_for_billing())
                                .expect(
                                    "location size is validated against the cluster replica sizes",
                                );
                                    new_credit_consumption_rate -=
                                        replica_allocation.credits_per_hour
                                }
                            }
                            DropObjectInfo::Database(_) => {
                                new_databases -= 1;
                            }
                            DropObjectInfo::Schema((database_spec, _)) => {
                                if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                                    *new_schemas_per_database.entry(database_id).or_insert(0) -= 1;
                                }
                            }
                            DropObjectInfo::Role(_) => {
                                new_roles -= 1;
                            }
                            DropObjectInfo::Item(id) => {
                                let entry = self.catalog().get_entry(id);
                                *new_objects_per_schema
                                    .entry((
                                        entry.name().qualifiers.database_spec.clone(),
                                        entry.name().qualifiers.schema_spec.clone(),
                                    ))
                                    .or_insert(0) -= 1;
                                match entry.item() {
                            CatalogItem::Connection(connection) => match connection.connection {
                                mz_storage_types::connections::Connection::AwsPrivatelink(_) => {
                                    new_aws_privatelink_connections -= 1;
                                }
                                _ => (),
                            },
                            CatalogItem::Table(_) => {
                                new_tables -= 1;
                            }
                            CatalogItem::Source(source) => {
                                new_sources -= source.user_controllable_persist_shard_count()
                            }
                            CatalogItem::Sink(_) => new_sinks -= 1,
                            CatalogItem::MaterializedView(_) => {
                                new_materialized_views -= 1;
                            }
                            CatalogItem::Secret(_) => {
                                new_secrets -= 1;
                            }
                            CatalogItem::Log(_)
                            | CatalogItem::View(_)
                            | CatalogItem::Index(_)
                            | CatalogItem::Type(_)
                            | CatalogItem::Func(_) => {}
                        }
                            }
                        }
                    }
                }
                Op::UpdateItem {
                    name: _,
                    id,
                    to_item,
                } => match to_item {
                    CatalogItem::Source(source) => {
                        let current_source = self
                            .catalog()
                            .get_entry(id)
                            .source()
                            .expect("source update is for source item");

                        new_sources += source.user_controllable_persist_shard_count()
                            - current_source.user_controllable_persist_shard_count();
                    }
                    CatalogItem::Connection(_)
                    | CatalogItem::Table(_)
                    | CatalogItem::Sink(_)
                    | CatalogItem::MaterializedView(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::Index(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_) => {}
                },
                Op::AlterRole { .. }
                | Op::AlterRetainHistory { .. }
                | Op::UpdatePrivilege { .. }
                | Op::UpdateDefaultPrivilege { .. }
                | Op::GrantRole { .. }
                | Op::RenameCluster { .. }
                | Op::RenameClusterReplica { .. }
                | Op::RenameItem { .. }
                | Op::RenameSchema { .. }
                | Op::UpdateOwner { .. }
                | Op::RevokeRole { .. }
                | Op::UpdateClusterConfig { .. }
                | Op::UpdateClusterReplicaConfig { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::Comment { .. }
                | Op::WeirdBuiltinTableUpdates { .. }
                | Op::TransactionDryRun => {}
            }
        }

        let mut current_aws_privatelink_connections = 0;
        let mut current_postgres_connections = 0;
        let mut current_mysql_connections = 0;
        let mut current_kafka_connections = 0;
        for c in self.catalog().user_connections() {
            let connection = c
                .connection()
                .expect("`user_connections()` only returns connection objects");

            use mz_storage_types::connections::Connection;
            match connection.connection {
                Connection::AwsPrivatelink(_) => current_aws_privatelink_connections += 1,
                Connection::Postgres(_) => current_postgres_connections += 1,
                Connection::MySql(_) => current_mysql_connections += 1,
                Connection::Kafka(_) => current_kafka_connections += 1,
                Connection::Csr(_) | Connection::Ssh(_) | Connection::Aws(_) => {}
            }
        }
        self.validate_resource_limit(
            current_kafka_connections,
            new_kafka_connections,
            SystemVars::max_kafka_connections,
            "Kafka Connection",
            MAX_KAFKA_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_postgres_connections,
            new_postgres_connections,
            SystemVars::max_postgres_connections,
            "PostgreSQL Connection",
            MAX_POSTGRES_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_mysql_connections,
            new_mysql_connections,
            SystemVars::max_mysql_connections,
            "MySQL Connection",
            MAX_MYSQL_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_aws_privatelink_connections,
            new_aws_privatelink_connections,
            SystemVars::max_aws_privatelink_connections,
            "AWS PrivateLink Connection",
            MAX_AWS_PRIVATELINK_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_tables().count(),
            new_tables,
            SystemVars::max_tables,
            "table",
            MAX_TABLES.name(),
        )?;

        let current_sources: usize = self
            .catalog()
            .user_sources()
            .filter_map(|source| source.source())
            .map(|source| source.user_controllable_persist_shard_count())
            .sum::<i64>()
            .try_into()
            .expect("non-negative sum of sources");

        self.validate_resource_limit(
            current_sources,
            new_sources,
            SystemVars::max_sources,
            "source",
            MAX_SOURCES.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_sinks().count(),
            new_sinks,
            SystemVars::max_sinks,
            "sink",
            MAX_SINKS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_materialized_views().count(),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "materialized view",
            MAX_MATERIALIZED_VIEWS.name(),
        )?;
        self.validate_resource_limit(
            // Linked compute clusters don't count against the limit, since
            // we have a separate sources and sinks limit.
            //
            // TODO(benesch): remove the `max_sources` and `max_sinks` limit,
            // and set a higher max cluster limit?
            self.catalog().user_clusters().count(),
            new_clusters,
            SystemVars::max_clusters,
            "cluster",
            MAX_CLUSTERS.name(),
        )?;
        for (cluster_id, new_replicas) in new_replicas_per_cluster {
            // It's possible that the cluster hasn't been created yet.
            let current_amount = self
                .catalog()
                .try_get_cluster(cluster_id)
                .map(|instance| instance.replicas().count())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }
        let current_credit_consumption_rate = self
            .catalog()
            .user_cluster_replicas()
            .filter_map(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => Some(location.size_for_billing()),
                ReplicaLocation::Unmanaged(_) => None,
            })
            .map(|size| {
                self.catalog()
                    .cluster_replica_sizes()
                    .0
                    .get(size)
                    .expect("location size is validated against the cluster replica sizes")
                    .credits_per_hour
            })
            .sum();
        self.validate_resource_limit_numeric(
            current_credit_consumption_rate,
            new_credit_consumption_rate,
            SystemVars::max_credit_consumption_rate,
            "cluster replica",
            MAX_CREDIT_CONSUMPTION_RATE.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().databases().count(),
            new_databases,
            SystemVars::max_databases,
            "database",
            MAX_DATABASES.name(),
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.catalog().get_database(database_id).schemas_by_id.len(),
                new_schemas,
                SystemVars::max_schemas_per_database,
                "schema",
                MAX_SCHEMAS_PER_DATABASE.name(),
            )?;
        }
        for ((database_spec, schema_spec), new_objects) in new_objects_per_schema {
            self.validate_resource_limit(
                self.catalog()
                    .get_schema(&database_spec, &schema_spec, conn_id)
                    .items
                    .len(),
                new_objects,
                SystemVars::max_objects_per_schema,
                "object",
                MAX_OBJECTS_PER_SCHEMA.name(),
            )?;
        }
        self.validate_resource_limit(
            self.catalog().user_secrets().count(),
            new_secrets,
            SystemVars::max_secrets,
            "secret",
            MAX_SECRETS.name(),
        )?;
        self.validate_resource_limit(
            self.catalog().user_roles().count(),
            new_roles,
            SystemVars::max_roles,
            "role",
            MAX_ROLES.name(),
        )?;
        Ok(())
    }

    /// Validate a specific type of resource limit and return an error if that limit is exceeded.
    pub(crate) fn validate_resource_limit<F>(
        &self,
        current_amount: usize,
        new_instances: i64,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> u32,
    {
        if new_instances <= 0 {
            return Ok(());
        }

        let limit: i64 = resource_limit(self.catalog().system_config()).into();
        let current_amount: Option<i64> = current_amount.try_into().ok();
        let desired =
            current_amount.and_then(|current_amount| current_amount.checked_add(new_instances));

        let exceeds_limit = if let Some(desired) = desired {
            desired > limit
        } else {
            true
        };

        let desired = desired
            .map(|desired| desired.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        let current = current_amount
            .map(|current| current.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        if exceeds_limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired,
                limit: limit.to_string(),
                current,
            })
        } else {
            Ok(())
        }
    }

    /// Validate a specific type of float resource limit and return an error if that limit is exceeded.
    ///
    /// This is very similar to [`Self::validate_resource_limit`] but for numerics.
    fn validate_resource_limit_numeric<F>(
        &self,
        current_amount: Numeric,
        new_amount: Numeric,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> Numeric,
    {
        if new_amount <= Numeric::zero() {
            return Ok(());
        }

        let limit = resource_limit(self.catalog().system_config());
        // Floats will overflow to infinity instead of panicking, which has the correct comparison
        // semantics.
        // NaN should be impossible here since both values are positive.
        let desired = current_amount + new_amount;
        if desired > limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired: desired.to_string(),
                limit: limit.to_string(),
                current: current_amount.to_string(),
            })
        } else {
            Ok(())
        }
    }
}
