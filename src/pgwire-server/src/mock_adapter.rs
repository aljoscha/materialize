// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mock adapter service that creates a real mz_adapter::Client but only
//! supports the bare minimum of commands needed.

use std::collections::{BTreeMap, BTreeSet};
use std::future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_adapter::catalog::Catalog;
use mz_adapter::statement_logging::{StatementLoggingFrontend, ThrottlingState};
use mz_adapter::{
    CatalogSnapshot, Client, Command, ExecuteResponse, Response, StartupResponse,
};
use mz_build_info::{BuildInfo, build_info};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::{PersistClient, ShardId};
use mz_persist_types::Codec64;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{Datum, GlobalId, IntoRowIterator, RelationDesc, RelationVersion, Row};
use mz_sql::catalog::EnvironmentId;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::session::vars::SystemVars;
use mz_storage_client::client::TimestamplessUpdateBuilder;
use mz_storage_client::controller::{CollectionDescription, StorageMetadata, StorageTxn};
use mz_storage_client::storage_collections::{
    CollectionFrontiers, SnapshotCursor, StorageCollections,
};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use rand::SeedableRng;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use tracing::debug;

const BUILD_INFO: BuildInfo = build_info!();

/// A stub implementation of [`StorageCollections`] that panics on all methods.
///
/// This is only used by the mock adapter for pgwire testing, where no actual
/// storage operations are performed.
#[derive(Debug)]
struct StubStorageCollections;

#[async_trait]
impl StorageCollections for StubStorageCollections {
    type Timestamp = mz_repr::Timestamp;

    async fn initialize_state(
        &self,
        _txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        _init_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn update_parameters(&self, _config_params: StorageParameters) {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn collection_metadata(&self, _id: GlobalId) -> Result<CollectionMetadata, CollectionMissing> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn collections_frontiers(
        &self,
        _ids: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, CollectionMissing> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn check_exists(&self, _id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn snapshot_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn snapshot_parts_stats(
        &self,
        _id: GlobalId,
        _as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn snapshot(
        &self,
        _id: GlobalId,
        _as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError<Self::Timestamp>>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn snapshot_latest(
        &self,
        _id: GlobalId,
    ) -> Result<Vec<Row>, StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn snapshot_cursor(
        &self,
        _id: GlobalId,
        _as_of: Self::Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>>
    where
        Self::Timestamp: Codec64,
    {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn snapshot_and_stream(
        &self,
        _id: GlobalId,
        _as_of: Self::Timestamp,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<'static, (SourceData, Self::Timestamp, StorageDiff)>,
            StorageError<Self::Timestamp>,
        >,
    > {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn create_update_builder(
        &self,
        _id: GlobalId,
    ) -> BoxFuture<
        'static,
        Result<
            TimestamplessUpdateBuilder<SourceData, (), Self::Timestamp, StorageDiff>,
            StorageError<Self::Timestamp>,
        >,
    >
    where
        Self::Timestamp: Codec64,
    {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn prepare_state(
        &self,
        _txn: &mut (dyn StorageTxn<Self::Timestamp> + Send),
        _ids_to_add: BTreeSet<GlobalId>,
        _ids_to_drop: BTreeSet<GlobalId>,
        _ids_to_register: BTreeMap<GlobalId, ShardId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn create_collections_for_bootstrap(
        &self,
        _storage_metadata: &StorageMetadata,
        _register_ts: Option<Self::Timestamp>,
        _collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
        _migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    async fn alter_table_desc(
        &self,
        _existing_collection: GlobalId,
        _new_collection: GlobalId,
        _new_desc: RelationDesc,
        _expected_version: RelationVersion,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn drop_collections_unvalidated(
        &self,
        _storage_metadata: &StorageMetadata,
        _identifiers: Vec<GlobalId>,
    ) {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn set_read_policies(&self, _policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn acquire_read_holds(
        &self,
        _desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, CollectionMissing> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn determine_time_dependence(
        &self,
        _id: GlobalId,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        unimplemented!("mock adapter does not support storage operations")
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        unimplemented!("mock adapter does not support storage operations")
    }
}

pub struct MockAdapterService {
    client: Client,
    _handle: mz_ore::task::JoinHandle<()>,
}

impl MockAdapterService {
    pub async fn new(metrics_registry: &MetricsRegistry) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let metrics = mz_adapter::metrics::Metrics::register_into(metrics_registry);

        let environment_id = EnvironmentId::for_tests();

        // Get ourselves a debug catalog, which uses env vars to read the actual
        // catalog in mzdata.
        let mut catalog_extract = None;
        Catalog::with_debug(|catalog| async {
            catalog_extract.replace(catalog);
        })
        .await;
        let catalog = Arc::new(catalog_extract.expect("missing catalog"));
        let (catalog_watch_tx, catalog_watch_rx) = tokio::sync::watch::channel(Arc::clone(&catalog));
        let _ = catalog_watch_tx; // keep alive
        let client = Client::new(
            &BUILD_INFO,
            cmd_tx,
            metrics,
            SYSTEM_TIME.clone(),
            environment_id,
            None, // No segment client
            catalog_watch_rx,
        );

        let storage_collections: Arc<
            dyn StorageCollections<Timestamp = mz_repr::Timestamp> + Send + Sync,
        > = Arc::new(StubStorageCollections);
        let transient_id_gen = Arc::new(TransientIdGen::new());
        let optimizer_metrics =
            OptimizerMetrics::register_into(metrics_registry, Duration::from_secs(60));
        let persist_client = PersistClient::new_for_tests().await;
        let statement_logging_frontend = StatementLoggingFrontend {
            throttling_state: Arc::new(ThrottlingState::new(&SYSTEM_TIME)),
            reproducible_rng: Arc::new(Mutex::new(rand_chacha::ChaCha8Rng::seed_from_u64(42))),
            build_info_human_version: BUILD_INFO.human_version(None),
            now: SYSTEM_TIME.clone(),
        };

        let handle = task::spawn(|| "mock_adapter_service", async move {
            while let Some((_ctx, cmd)) = cmd_rx.recv().await {
                match cmd {
                    Command::CatalogSnapshot { tx } => {
                        let catalog_snapshot = CatalogSnapshot {
                            catalog: Arc::clone(&catalog),
                        };

                        let _ = tx.send(catalog_snapshot);
                    }
                    Command::GetSystemVars { tx } => {
                        let _ = tx.send(SystemVars::default());
                        ()
                    }
                    Command::Startup { tx, .. } => {
                        let role_id = mz_repr::role_id::RoleId::Public;
                        let res = Ok(StartupResponse {
                            role_id,
                            write_notify: Box::pin(future::ready(())),
                            session_defaults: BTreeMap::default(),
                            catalog: Arc::clone(&catalog),
                            storage_collections: Arc::clone(&storage_collections),
                            transient_id_gen: Arc::clone(&transient_id_gen),
                            optimizer_metrics: optimizer_metrics.clone(),
                            persist_client: persist_client.clone(),
                            statement_logging_frontend: statement_logging_frontend.clone(),
                            mock_peek_data: None,
                        });
                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Execute { tx, session, .. } => {
                        let mut rows: Vec<Row> = Vec::new();
                        rows.push(Row::pack_slice(&[Datum::Int32(1)]));

                        let row_iter = Box::new(rows.into_row_iter());

                        let execute_response =
                            ExecuteResponse::SendingRowsImmediate { rows: row_iter };

                        let res = Response {
                            result: Ok(execute_response),
                            session,
                            otel_ctx: OpenTelemetryContext::obtain(),
                        };

                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Commit {
                        tx,
                        session,
                        action: _,
                        ..
                    } => {
                        let execute_response = ExecuteResponse::TransactionCommitted {
                            params: BTreeMap::new(),
                        };

                        let res = Response {
                            result: Ok(execute_response),
                            session,
                            otel_ctx: OpenTelemetryContext::obtain(),
                        };

                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Terminate { tx, .. } => {
                        if let Some(tx) = tx {
                            if let Err(err) = tx.send(Ok(())) {
                                tracing::error!("error terminating session: {:?}", err);
                            }
                        }
                    }
                    _command => {
                        // info!(command = command.to_string(), "command not implemented");
                    }
                }
            }
            debug!("Mock adapter service command handler shutting down");
        });

        Self {
            client,
            _handle: handle,
        }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
