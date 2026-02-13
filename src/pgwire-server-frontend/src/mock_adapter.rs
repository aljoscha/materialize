// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mock adapter service for testing frontend peek sequencing.
//!
//! Unlike the base pgwire-server's mock adapter, this one:
//! - Enables `enable_frontend_peek_sequencing` on the catalog
//! - Does NOT set `execute_override` in StartupResponse
//! - Pre-creates a table `t(id text, value int4)` with a default index
//! - Uses a real PostgresTimestampOracle backed by CockroachDB
//!
//! This forces queries to go through the real frontend peek path:
//! catalog snapshot → SQL planning → optimization → index peek (mocked at compute layer).

use std::collections::{BTreeMap, BTreeSet};
use std::future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use mz_adapter::catalog::{Catalog, Op};
use mz_adapter::index_sql;
use mz_adapter::statement_logging::{StatementLoggingFrontend, ThrottlingState};
use mz_adapter::{
    CatalogSnapshot, Client, Command, ExecuteResponse, Response, StartupResponse,
    SuperuserAttribute,
};
use mz_build_info::{BuildInfo, build_info};
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{CatalogItem, Index, Table, TableDataSource};
use mz_controller_types::ClusterId;
use mz_expr::MirScalarExpr;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME};
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::{PersistClient, ShardId};
use mz_persist_types::Codec64;
use mz_repr::SqlScalarType;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{
    Datum, GlobalId, IntoRowIterator, RelationDesc, RelationVersion, Row, Timestamp,
    VersionedRelationDesc,
};
use mz_sql::DEFAULT_SCHEMA;
use mz_sql::catalog::EnvironmentId;
use mz_sql::names::{
    FullItemName, ItemQualifiers, QualifiedItemName, RawDatabaseSpecifier,
    ResolvedDatabaseSpecifier, ResolvedIds,
};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{SystemVars, VarInput};
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
use mz_timestamp_oracle::TimestampOracle;
use mz_timestamp_oracle::batching_oracle::BatchingTimestampOracle;
use mz_timestamp_oracle::postgres_oracle::{
    PostgresTimestampOracle, PostgresTimestampOracleConfig,
};
use rand::SeedableRng;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::mpsc;
use tracing::{debug, info};

const BUILD_INFO: BuildInfo = build_info!();

/// A simple in-memory timestamp oracle for use when no CockroachDB is available.
/// Uses atomic u64 for lock-free read_ts (the hot path).
#[derive(Debug)]
struct InMemoryTimestampOracle {
    ts: std::sync::atomic::AtomicU64,
}

impl InMemoryTimestampOracle {
    fn new() -> Self {
        Self {
            ts: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl mz_timestamp_oracle::TimestampOracle<Timestamp> for InMemoryTimestampOracle {
    async fn write_ts(&self) -> mz_timestamp_oracle::WriteTimestamp<Timestamp> {
        let ts = self.ts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        mz_timestamp_oracle::WriteTimestamp {
            timestamp: Timestamp::new(ts),
            advance_to: Timestamp::new(ts + 1),
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        Timestamp::new(self.ts.load(std::sync::atomic::Ordering::SeqCst))
    }

    async fn read_ts(&self) -> Timestamp {
        Timestamp::new(
            self.ts
                .load(std::sync::atomic::Ordering::SeqCst)
                .saturating_sub(1),
        )
    }

    async fn apply_write(&self, lower_bound: Timestamp) {
        let val: u64 = lower_bound.into();
        self.ts.fetch_max(val, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Default database name used by Materialize.
const DEFAULT_DATABASE_NAME: &str = "materialize";

/// A stub implementation of [`StorageCollections`] that returns no-op results
/// for methods that may be called during indexed table queries (acquire_read_holds,
/// collections_frontiers), and panics on all other methods.
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
        ids: Vec<GlobalId>,
    ) -> Result<Vec<CollectionFrontiers<Self::Timestamp>>, CollectionMissing> {
        // Return minimal frontiers for each ID — these shouldn't be called for
        // indexed table queries, but safer than panicking.
        Ok(ids
            .into_iter()
            .map(|id| CollectionFrontiers {
                id,
                write_frontier: Antichain::from_elem(1.into()),
                implied_capability: Antichain::from_elem(TimelyTimestamp::minimum()),
                read_capabilities: Antichain::from_elem(TimelyTimestamp::minimum()),
            })
            .collect())
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
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, CollectionMissing> {
        // Return no-op read holds — these shouldn't be called for indexed table
        // queries but safer than panicking.
        let no_op_tx: mz_storage_types::read_holds::ChangeTx<mz_repr::Timestamp> =
            Arc::new(|_id, _changes| Ok(()));
        Ok(desired_holds
            .into_iter()
            .map(|id| {
                ReadHold::new(
                    id,
                    Antichain::from_elem(TimelyTimestamp::minimum()),
                    no_op_tx.clone(),
                )
            })
            .collect())
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
    pub async fn new(metrics_registry: &MetricsRegistry, metadata_url: Option<String>) -> Self {
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
        let mut catalog = catalog_extract.expect("missing catalog");

        // Enable frontend peek sequencing on the catalog.
        catalog
            .system_config_mut()
            .set("enable_frontend_peek_sequencing", VarInput::Flat("on"))
            .expect("failed to enable frontend peek sequencing");
        info!("Enabled enable_frontend_peek_sequencing on catalog");

        // --- Create table t(id text, value int4) and a default index ---
        let mock_peek_data = Self::create_table_and_index(&mut catalog).await;

        let catalog = Arc::new(catalog);
        let client = Client::new(
            &BUILD_INFO,
            cmd_tx,
            metrics,
            SYSTEM_TIME.clone(),
            environment_id,
            None, // No segment client
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

        // Set up the real PostgresTimestampOracle if a CockroachDB URL is provided.
        let pg_oracle_config = metadata_url.map(|url| {
            let sensitive_url: SensitiveUrl = url.parse().expect("invalid METADATA_BACKEND_URL");
            PostgresTimestampOracleConfig::new(&sensitive_url, metrics_registry)
        });

        let handle = task::spawn(|| "mock_adapter_service", async move {
            // Lazily-created oracle, shared across requests.
            let mut oracle: Option<Arc<dyn TimestampOracle<Timestamp> + Send + Sync>> = None;

            // Process commands without yielding back to the tokio scheduler
            // between commands. This avoids the cooperative scheduling budget
            // forcing unnecessary task wake-ups when the channel has many
            // pending commands from concurrent connections.
            while let Some((_ctx, cmd)) = cmd_rx.recv().await {
                Self::handle_command(
                    cmd,
                    &catalog,
                    &storage_collections,
                    &transient_id_gen,
                    &optimizer_metrics,
                    &persist_client,
                    &statement_logging_frontend,
                    &mock_peek_data,
                    &pg_oracle_config,
                    &mut oracle,
                )
                .await;
                // Drain all pending commands without yielding.
                while let Ok((_ctx, cmd)) = cmd_rx.try_recv() {
                    Self::handle_command(
                        cmd,
                        &catalog,
                        &storage_collections,
                        &transient_id_gen,
                        &optimizer_metrics,
                        &persist_client,
                        &statement_logging_frontend,
                        &mock_peek_data,
                        &pg_oracle_config,
                        &mut oracle,
                    )
                    .await;
                }
            }
            debug!("Mock adapter service command handler shutting down");
        });

        Self {
            client,
            _handle: handle,
        }
    }

    /// Create table `t(id text, value int4)` and a default index on all columns.
    /// Returns the mock_peek_data map: index_global_id -> empty rows.
    async fn create_table_and_index(catalog: &mut Catalog) -> Option<BTreeMap<GlobalId, Vec<Row>>> {
        // Allocate IDs for table and index.
        let (table_item_id, table_global_id) = catalog
            .allocate_user_id_for_test()
            .await
            .expect("failed to allocate table ID");
        let (index_item_id, index_global_id) = catalog
            .allocate_user_id_for_test()
            .await
            .expect("failed to allocate index ID");

        // Resolve database and schema.
        let database = catalog
            .resolve_database(DEFAULT_DATABASE_NAME)
            .expect("failed to resolve database");
        let database_name = database.name.clone();
        let database_id = database.id;
        let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
        let schema = catalog
            .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
            .expect("failed to resolve schema");
        let schema_name = schema.name.schema.clone();
        let schema_spec = schema.id.clone();

        // Build table relation description: (id text, value int4)
        let table_desc = RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(true))
            .with_column("value", SqlScalarType::Int32.nullable(true))
            .finish();

        let create_table_sql = format!(
            "CREATE TABLE {}.{}.t (id text, value int4)",
            database_name, schema_name,
        );

        // Create the table.
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateItem {
                    id: table_item_id,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: database_spec.clone(),
                            schema_spec: schema_spec.clone(),
                        },
                        item: "t".to_string(),
                    },
                    item: CatalogItem::Table(Table {
                        create_sql: Some(create_table_sql),
                        desc: VersionedRelationDesc::new(table_desc.clone()),
                        collections: [(RelationVersion::root(), table_global_id)]
                            .into_iter()
                            .collect(),
                        conn_id: None,
                        resolved_ids: ResolvedIds::empty(),
                        custom_logical_compaction_window: None,
                        is_retained_metrics_object: false,
                        data_source: TableDataSource::TableWrites { defaults: vec![] },
                    }),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to create table t");

        info!(
            "Created table t (item_id={}, global_id={})",
            table_item_id, table_global_id
        );

        // Create the default index on all columns.
        let cluster_id = ClusterId::User(1);
        let keys: Vec<usize> = (0..table_desc.arity()).collect();

        let index_name = "t_primary_idx".to_string();
        let full_table_name = FullItemName {
            database: RawDatabaseSpecifier::Name(database_name.clone()),
            schema: schema_name.clone(),
            item: "t".to_string(),
        };
        let create_index_sql = index_sql(
            index_name.clone(),
            cluster_id,
            full_table_name,
            &table_desc,
            &keys,
        );

        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateItem {
                    id: index_item_id,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec,
                            schema_spec,
                        },
                        item: index_name,
                    },
                    item: CatalogItem::Index(Index {
                        global_id: index_global_id,
                        on: table_global_id,
                        keys: keys.iter().map(|&i| MirScalarExpr::column(i)).collect(),
                        create_sql: create_index_sql,
                        conn_id: None,
                        resolved_ids: [(table_item_id, table_global_id)].into_iter().collect(),
                        cluster_id,
                        custom_logical_compaction_window: None,
                        is_retained_metrics_object: false,
                    }),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to create index on table t");

        info!(
            "Created index t_primary_idx (item_id={}, global_id={}) on cluster {:?}",
            index_item_id, index_global_id, cluster_id
        );

        // Return mock peek data: the index returns empty rows.
        let mut mock_data = BTreeMap::new();
        mock_data.insert(index_global_id, Vec::new());
        Some(mock_data)
    }

    async fn handle_command(
        cmd: Command,
        catalog: &Arc<Catalog>,
        storage_collections: &Arc<
            dyn StorageCollections<Timestamp = mz_repr::Timestamp> + Send + Sync,
        >,
        transient_id_gen: &Arc<TransientIdGen>,
        optimizer_metrics: &OptimizerMetrics,
        persist_client: &PersistClient,
        statement_logging_frontend: &StatementLoggingFrontend,
        _mock_peek_data: &Option<BTreeMap<GlobalId, Vec<Row>>>,
        pg_oracle_config: &Option<PostgresTimestampOracleConfig>,
        oracle: &mut Option<Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    ) {
        match cmd {
            Command::CatalogSnapshot { tx } => {
                let _ = tx.send(CatalogSnapshot {
                    catalog: Arc::clone(catalog),
                });
            }
            Command::GetSystemVars { tx } => {
                let _ = tx.send(SystemVars::default());
            }
            Command::GetOracle { timeline, tx } => {
                let result = match oracle {
                    Some(o) => Ok(Arc::clone(o)),
                    None => {
                        if let Some(config) = pg_oracle_config {
                            let now_fn: NowFn = NowFn::from(|| 0u64);
                            let initial_ts = Timestamp::new(0);
                            let pg_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                                Arc::new(
                                    PostgresTimestampOracle::open(
                                        config.clone(),
                                        timeline.to_string(),
                                        initial_ts,
                                        now_fn,
                                        false,
                                    )
                                    .await,
                                );
                            let batching_metrics = Arc::clone(config.metrics());
                            let batching =
                                BatchingTimestampOracle::new(batching_metrics, pg_oracle);
                            let arc: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                                Arc::new(batching);
                            *oracle = Some(Arc::clone(&arc));
                            Ok(arc)
                        } else {
                            // Use in-memory oracle when no CockroachDB URL is provided.
                            let arc: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                                Arc::new(InMemoryTimestampOracle::new());
                            *oracle = Some(Arc::clone(&arc));
                            Ok(arc)
                        }
                    }
                };
                let _ = tx.send(result);
            }
            Command::RegisterFrontendPeek { .. } => {}
            Command::UnregisterFrontendPeek { .. } => {}
            Command::FrontendStatementLogging(..) => {
                // No-op: we don't log statements in the mock adapter.
            }
            Command::Startup { tx, .. } => {
                // No execute_override: queries go through the real frontend peek path.
                let res = Ok(StartupResponse {
                    role_id: MZ_SYSTEM_ROLE_ID,
                    superuser_attribute: SuperuserAttribute(None),
                    write_notify: Box::pin(future::ready(())),
                    session_defaults: BTreeMap::default(),
                    catalog: Arc::clone(catalog),
                    storage_collections: Arc::clone(storage_collections),
                    transient_id_gen: Arc::clone(transient_id_gen),
                    optimizer_metrics: optimizer_metrics.clone(),
                    persist_client: persist_client.clone(),
                    statement_logging_frontend: statement_logging_frontend.clone(),
                });
                let _ = tx.send(res);
            }
            Command::Execute { tx, session, .. } => {
                // Fallback for queries that don't go through frontend peek.
                let mut rows: Vec<Row> = Vec::new();
                rows.push(Row::pack_slice(&[Datum::Int32(1)]));

                let row_iter = Box::new(rows.into_row_iter());

                let _ = tx.send(Response {
                    result: Ok(ExecuteResponse::SendingRowsImmediate { rows: row_iter }),
                    session,
                    otel_ctx: OpenTelemetryContext::obtain(),
                });
            }
            Command::Commit {
                tx,
                session,
                action: _,
                ..
            } => {
                let _ = tx.send(Response {
                    result: Ok(ExecuteResponse::TransactionCommitted {
                        params: BTreeMap::new(),
                    }),
                    session,
                    otel_ctx: OpenTelemetryContext::obtain(),
                });
            }
            Command::Terminate { tx, .. } => {
                if let Some(tx) = tx {
                    let _ = tx.send(Ok(()));
                }
            }
            _command => {}
        }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
