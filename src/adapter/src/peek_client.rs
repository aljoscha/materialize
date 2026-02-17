// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate;
use mz_compute_client::controller::InstanceClient;
use mz_compute_client::controller::error::{CollectionLookupError, InstanceMissing};
use mz_compute_client::controller::instance_client::SharedCollectionState;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_types::ComputeInstanceId;
use mz_expr::RowSetFinishing;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_persist_client::PersistClient;
use mz_repr::SqlRelationType;
use mz_repr::Timestamp;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_sql::ast::{Raw, Statement};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{QueryWhen, StatementDesc};
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use mz_transform::dataflow::DataflowMetainfo;
use prometheus::Histogram;
use prometheus::core::{AtomicU64, GenericCounter};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::catalog::Catalog;

/// Generate a UUID using a thread-local fast (non-cryptographic) RNG.
///
/// `Uuid::new_v4()` reads from `/dev/urandom` on every call, which is a syscall.
/// For peek UUIDs we only need uniqueness (not cryptographic randomness), so we
/// seed a fast PRNG once from the OS and reuse it for all subsequent UUIDs on
/// this thread.
fn fast_new_uuid_v4() -> Uuid {
    use rand::RngCore;
    thread_local! {
        static RNG: std::cell::RefCell<rand::rngs::SmallRng> = std::cell::RefCell::new(
            <rand::rngs::SmallRng as rand::SeedableRng>::from_os_rng()
        );
    }
    RNG.with(|rng| {
        let mut bytes = [0u8; 16];
        rng.borrow_mut().fill_bytes(&mut bytes);
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    })
}

use crate::command::{CatalogSnapshot, Command};
use crate::coord::Coordinator;
use crate::coord::peek::{FastPathPlan, PeekPlan, PeekResponseUnary};
use crate::statement_logging::WatchSetCreation;
use crate::statement_logging::{
    FrontendStatementLoggingEvent, PreparedStatementEvent, StatementLoggingFrontend,
    StatementLoggingId,
};
use crate::{
    AdapterError, Client, CollectionIdBundle, ReadHolds, TimelineContext, statement_logging,
};

/// Storage collections trait alias we need to consult for since/frontiers.
pub type StorageCollectionsHandle = Arc<
    dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = Timestamp>
        + Send
        + Sync,
>;

/// Cached result of planning and optimization for a query, keyed by SQL text.
/// This allows us to skip parsing, name resolution, planning, and optimization
/// for repeated queries.
#[derive(Clone, Debug)]
pub struct PlanCacheEntry {
    /// The statement description (for portal setup in declare).
    pub desc: StatementDesc,
    /// The optimized peek plan (FastPath or SlowPath).
    pub peek_plan: PeekPlan,
    /// Dataflow metadata from optimization.
    pub df_meta: DataflowMetainfo,
    /// The result type from optimization.
    pub typ: SqlRelationType,
    /// Pre-computed source IDs (from depends_on).
    pub source_ids: BTreeSet<GlobalId>,
    /// The finishing instructions (ORDER BY, LIMIT, OFFSET, PROJECT).
    pub finishing: RowSetFinishing,
    /// When to execute the query.
    pub when: QueryWhen,
    /// Pre-computed collection ID bundle (for timestamp determination).
    pub input_id_bundle: CollectionIdBundle,
    /// Pre-computed timeline context.
    pub timeline_context: TimelineContext,
    /// The target cluster ID for execution.
    pub target_cluster_id: ComputeInstanceId,
    /// The parsed SQL statement, cached to skip SQL parsing on cache hit.
    /// Stored as Arc to avoid deep-cloning the AST on every cache lookup.
    pub parsed_stmt: Arc<Statement<Raw>>,
    /// Pre-built result description for the peek command. Cached to avoid
    /// per-query `format!("peek_{i}")` column name construction and
    /// `RelationDesc::new()` BTreeMap building. Wrapped in `Arc` to avoid
    /// per-query deep clones of the `BTreeMap<ColumnIndex, ColumnMetadata>`
    /// (which scales ~5.6x worse at 64 connections due to allocator contention).
    pub result_desc: Arc<RelationDesc>,
    /// Pre-extracted compute collection IDs for the single-compute-instance
    /// fast path. When `input_id_bundle.compute_ids` has exactly one entry,
    /// this stores the collection IDs directly, avoiding a per-query
    /// `BTreeMap::first_key_value()` traversal that scales poorly under
    /// concurrency (8x overhead at 64 connections due to CPU cache pressure).
    pub single_compute_collection_ids: Option<Vec<GlobalId>>,
    /// Pre-wrapped `RelationDesc` from `desc.relation_desc` for the pgwire
    /// response encoding. Stored as `Arc` to avoid deep-cloning the
    /// `BTreeMap<ColumnIndex, ColumnMetadata>` on every cached peek.
    /// This is the table's RelationDesc (with actual column names), used for
    /// RowDescription encoding, distinct from `result_desc` (which has
    /// auto-generated "peek_0", "peek_1" names for the compute command).
    pub relation_desc_for_response: Option<Arc<RelationDesc>>,
}

/// Clients needed for peek sequencing in the Adapter Frontend.
#[derive(Debug)]
pub struct PeekClient {
    coordinator_client: Client,
    /// Channels to talk to each compute Instance task directly. Lazily populated.
    /// Note that these are never cleaned up. In theory, this could lead to a very slow memory leak
    /// if a long-running user session keeps peeking on clusters that are being created and dropped
    /// in a hot loop. Hopefully this won't occur any time soon.
    pub(crate) compute_instances: BTreeMap<ComputeInstanceId, InstanceClient<Timestamp>>,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
    /// A generator for transient `GlobalId`s, shared with Coordinator.
    pub transient_id_gen: Arc<TransientIdGen>,
    pub optimizer_metrics: OptimizerMetrics,
    /// Per-timeline oracles from the coordinator. Lazily populated.
    oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    persist_client: PersistClient,
    /// Statement logging state for frontend peek sequencing.
    pub statement_logging_frontend: StatementLoggingFrontend,
    /// Optional mock data for peek testing. When set, PeekExisting queries
    /// on these GlobalIds return mock rows, bypassing the compute layer.
    mock_peek_data: Option<BTreeMap<GlobalId, Vec<Row>>>,
    /// Plan cache: maps SQL text to cached planning + optimization results.
    /// Allows skipping parse/resolve/plan/optimize for repeated queries.
    /// Wrapped in `Arc` so cache lookups only do an atomic refcount increment
    /// instead of deep-cloning the entire entry.
    pub plan_cache: HashMap<Arc<str>, Arc<PlanCacheEntry>>,
    /// Cached shared collection state for compute collections, allowing direct
    /// mutex-based access to read capabilities and write frontiers without going
    /// through the compute instance task via `call_sync`.
    pub(crate) shared_collection_states:
        BTreeMap<(ComputeInstanceId, GlobalId), SharedCollectionState<Timestamp>>,
    /// Cached string representation of compute instance IDs, to avoid per-query
    /// `to_string()` String allocations in metric label construction.
    pub(crate) cached_instance_id_strs: BTreeMap<ComputeInstanceId, String>,
    /// Cached dyncfg handles for peek stash configuration. Avoids per-query
    /// BTreeMap<String> lookups in `Config::get()`. Lazily initialized.
    cached_peek_stash_handles: Option<(
        mz_dyncfg::ConfigValHandle<usize>,
        mz_dyncfg::ConfigValHandle<usize>,
    )>,
    /// Cached Histogram handle for `frontend_peek_seconds` with the
    /// "try_frontend_peek_cached" label. Avoids per-query `with_label_values`
    /// HashMap lookup.
    cached_peek_cached_histogram: Option<Histogram>,
    /// Cached Histogram handle for `frontend_peek_seconds` with the
    /// "try_frontend_peek" label. Used in `try_cached_peek_direct`.
    cached_peek_outer_histogram: Option<Histogram>,
    /// Cached `query_total` counter keyed by `(session_type, statement_type)`.
    /// Avoids per-query `with_label_values` HashMap lookup.
    cached_query_total: Option<(&'static str, &'static str, GenericCounter<AtomicU64>)>,
    /// Persistent read holds for compute collections, keyed by (instance, id).
    /// These prevent compaction at the collection's since frontier and allow
    /// per-query read holds to be acquired via `ReadHold::clone()` (which sends
    /// +1 through an async channel) instead of `acquire_read_hold_direct()`
    /// (which locks a contended mutex and triggers `MutableAntichain::rebuild()`).
    /// At 64+ concurrent connections, this eliminates the mutex serialization
    /// bottleneck in the read hold acquisition path.
    pub(crate) cached_read_holds: BTreeMap<(ComputeInstanceId, GlobalId), ReadHold<Timestamp>>,
}

impl PeekClient {
    /// Creates a PeekClient.
    pub fn new(
        coordinator_client: Client,
        storage_collections: StorageCollectionsHandle,
        transient_id_gen: Arc<TransientIdGen>,
        optimizer_metrics: OptimizerMetrics,
        persist_client: PersistClient,
        statement_logging_frontend: StatementLoggingFrontend,
        mock_peek_data: Option<BTreeMap<GlobalId, Vec<Row>>>,
    ) -> Self {
        Self {
            coordinator_client,
            compute_instances: Default::default(), // lazily populated
            storage_collections,
            transient_id_gen,
            optimizer_metrics,
            statement_logging_frontend,
            oracles: Default::default(), // lazily populated
            persist_client,
            mock_peek_data,
            plan_cache: Default::default(),
            shared_collection_states: Default::default(),
            cached_instance_id_strs: Default::default(),
            cached_peek_stash_handles: None,
            cached_peek_cached_histogram: None,
            cached_peek_outer_histogram: None,
            cached_query_total: None,
            cached_read_holds: Default::default(),
        }
    }

    pub(crate) fn metrics(&self) -> &crate::metrics::Metrics {
        self.coordinator_client.metrics()
    }

    /// Get the cached string representation of a compute instance ID.
    /// Avoids per-query `to_string()` String allocations for metric labels.
    pub fn instance_id_str(&mut self, instance_id: ComputeInstanceId) -> &str {
        self.cached_instance_id_strs
            .entry(instance_id)
            .or_insert_with(|| instance_id.to_string())
    }

    /// Get cached dyncfg handles for peek stash configuration.
    /// Amortizes the BTreeMap<String> name lookup to a one-time cost;
    /// subsequent calls just do an atomic load.
    pub fn peek_stash_handles(&mut self, dyncfgs: &mz_dyncfg::ConfigSet) -> (usize, usize) {
        let handles = self.cached_peek_stash_handles.get_or_insert_with(|| {
            (
                mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                    .handle(dyncfgs),
                mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                    .handle(dyncfgs),
            )
        });
        (handles.0.get(), handles.1.get())
    }

    /// Get the cached Histogram handle for `frontend_peek_seconds` with the
    /// "try_frontend_peek_cached" label.
    pub fn peek_cached_histogram(&mut self) -> &Histogram {
        self.cached_peek_cached_histogram.get_or_insert_with(|| {
            self.coordinator_client
                .metrics()
                .frontend_peek_seconds
                .with_label_values(&["try_frontend_peek_cached"])
        })
    }

    /// Get the cached Histogram handle for `frontend_peek_seconds` with the
    /// "try_frontend_peek" label. Used in `try_cached_peek_direct`.
    pub fn peek_outer_histogram(&mut self) -> &Histogram {
        self.cached_peek_outer_histogram.get_or_insert_with(|| {
            self.coordinator_client
                .metrics()
                .frontend_peek_seconds
                .with_label_values(&["try_frontend_peek"])
        })
    }

    /// Get the cached `query_total` counter for the given label pair.
    /// If the labels changed (different user type or statement type),
    /// the cache is invalidated and a new counter is resolved.
    pub fn query_total_counter(
        &mut self,
        session_type: &'static str,
        statement_type: &'static str,
    ) -> &GenericCounter<AtomicU64> {
        if let Some((st, stmt, _)) = &self.cached_query_total {
            if *st == session_type && *stmt == statement_type {
                return &self.cached_query_total.as_ref().unwrap().2;
            }
        }
        let counter = self
            .coordinator_client
            .metrics()
            .query_total
            .with_label_values(&[session_type, statement_type]);
        self.cached_query_total = Some((session_type, statement_type, counter));
        &self.cached_query_total.as_ref().unwrap().2
    }

    /// Ensures a compute instance client is cached. After calling this, access
    /// the client via `self.compute_instances[&compute_instance]`.
    pub async fn ensure_compute_instance_client(
        &mut self,
        compute_instance: ComputeInstanceId,
    ) -> Result<(), InstanceMissing> {
        if !self.compute_instances.contains_key(&compute_instance) {
            let client = self
                .call_coordinator(|tx| Command::GetComputeInstanceClient {
                    instance_id: compute_instance,
                    tx,
                })
                .await?;
            self.compute_instances.insert(compute_instance, client);
        }
        Ok(())
    }

    pub async fn ensure_oracle(
        &mut self,
        timeline: Timeline,
    ) -> Result<&mut Arc<dyn TimestampOracle<Timestamp> + Send + Sync>, AdapterError> {
        if !self.oracles.contains_key(&timeline) {
            let oracle = self
                .call_coordinator(|tx| Command::GetOracle {
                    timeline: timeline.clone(),
                    tx,
                })
                .await?;
            self.oracles.insert(timeline.clone(), oracle);
        }
        Ok(self.oracles.get_mut(&timeline).expect("ensured above"))
    }

    /// Fetch a snapshot of the catalog for use in frontend peek sequencing.
    /// Records the time taken in the adapter metrics, labeled by `context`.
    pub async fn catalog_snapshot(&self, context: &str) -> Arc<Catalog> {
        let start = std::time::Instant::now();
        let CatalogSnapshot { catalog } = self
            .call_coordinator(|tx| Command::CatalogSnapshot { tx })
            .await;
        self.coordinator_client
            .metrics()
            .catalog_snapshot_seconds
            .with_label_values(&[context])
            .observe(start.elapsed().as_secs_f64());
        catalog
    }

    pub(crate) async fn call_coordinator<T, F>(&self, f: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.coordinator_client.send(f(tx));
        rx.await
            .expect("if the coordinator is still alive, it shouldn't have dropped our call")
    }

    /// Acquire read holds on the given compute/storage collections, and
    /// determine the smallest common valid write frontier among the specified collections.
    ///
    /// Similar to `Coordinator::acquire_read_holds` and `TimestampProvider::least_valid_write`
    /// combined.
    ///
    /// Note: Unlike the Coordinator/StorageController's `least_valid_write` that treats sinks
    /// specially when fetching storage frontiers (see `mz_storage_controller::collections_frontiers`),
    /// we intentionally do not special‑case sinks here because peeks never read from sinks.
    /// Therefore, using `StorageCollections::collections_frontiers` is sufficient.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    pub async fn acquire_read_holds_and_least_valid_write(
        &mut self,
        id_bundle: &CollectionIdBundle,
    ) -> Result<(ReadHolds<Timestamp>, Antichain<Timestamp>), CollectionLookupError> {
        let mut read_holds = ReadHolds::new();
        let mut upper = Antichain::new();

        if !id_bundle.storage_ids.is_empty() {
            let desired_storage: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            let storage_read_holds = self
                .storage_collections
                .acquire_read_holds(desired_storage)?;
            read_holds.storage_holds = storage_read_holds
                .into_iter()
                .map(|hold| (hold.id(), hold))
                .collect();

            let storage_ids: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            for f in self
                .storage_collections
                .collections_frontiers(storage_ids)?
            {
                upper.extend(f.write_frontier);
            }
        }

        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            // Check if ALL collection_ids for this instance are mocked.
            let all_mocked = self.mock_peek_data.as_ref().is_some_and(|mock_data| {
                collection_ids.iter().all(|id| mock_data.contains_key(id))
            });

            if all_mocked {
                // Create no-op read holds for mocked collections.
                let no_op_tx: mz_storage_types::read_holds::ChangeTx<Timestamp> =
                    Arc::new(|_id, _changes| Ok(()));
                for &id in collection_ids {
                    let hold = ReadHold::new(
                        id,
                        Antichain::from_elem(TimelyTimestamp::minimum()),
                        no_op_tx.clone(),
                    );
                    let prev = read_holds.compute_holds.insert((instance_id, id), hold);
                    assert!(
                        prev.is_none(),
                        "duplicate compute ID in id_bundle {id_bundle:?}"
                    );
                }
                upper.extend(Antichain::from_elem(1.into()));
            } else {
                self.ensure_compute_instance_client(instance_id).await?;

                for &id in collection_ids {
                    let key = (instance_id, id);
                    // Lazily fetch and cache the SharedCollectionState on first access.
                    if !self.shared_collection_states.contains_key(&key) {
                        let shared = self.compute_instances[&instance_id]
                            .collection_shared_state(id)
                            .await?;
                        self.shared_collection_states.insert(key, shared);
                    }
                    let shared = self
                        .shared_collection_states
                        .get(&key)
                        .expect("just inserted");

                    let read_hold =
                        self.compute_instances[&instance_id].acquire_read_hold_direct(id, shared);
                    let write_frontier =
                        shared.lock_write_frontier(|f: &mut Antichain<Timestamp>| f.clone());

                    let prev = read_holds
                        .compute_holds
                        .insert((instance_id, id), read_hold);
                    assert!(
                        prev.is_none(),
                        "duplicate compute ID in id_bundle {id_bundle:?}"
                    );

                    upper.extend(write_frontier);
                }
            }
        }

        Ok((read_holds, upper))
    }

    /// Implement a fast-path peek plan.
    /// This is similar to `Coordinator::implement_peek_plan`, but only for fast path peeks.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    ///
    /// Note: `input_read_holds` has holds for all inputs. For fast-path peeks, this includes the
    /// peek target. For slow-path peeks (to be implemented later), we'll need to additionally call
    /// into the Controller to acquire a hold on the peek target after we create the dataflow.
    pub async fn implement_fast_path_peek_plan(
        &mut self,
        fast_path: FastPathPlan,
        timestamp: Timestamp,
        finishing: mz_expr::RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<mz_cluster_client::ReplicaId>,
        result_desc: Arc<RelationDesc>,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
        row_set_finishing_seconds: Option<Histogram>,
        target_read_hold: Option<ReadHold<Timestamp>>,
        peek_stash_read_batch_size_bytes: usize,
        peek_stash_read_memory_budget_bytes: usize,
        conn_id: mz_adapter_types::connection::ConnectionId,
        depends_on: std::collections::BTreeSet<mz_repr::GlobalId>,
        watch_set: Option<WatchSetCreation>,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let FastPathPlan::Constant(rows_res, _) = fast_path {
            // For constant queries with statement logging, immediately log that
            // dependencies are "ready" (trivially, because there are none).
            if let Some(ref ws) = watch_set {
                self.log_lifecycle_event(
                    ws.logging_id,
                    statement_logging::StatementLifecycleEvent::StorageDependenciesFinished,
                );
                self.log_lifecycle_event(
                    ws.logging_id,
                    statement_logging::StatementLifecycleEvent::ComputeDependenciesFinished,
                );
            }

            let mut rows = match rows_res {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            consolidate(&mut rows);

            let mut results = Vec::new();
            for (row, count) in rows {
                let count = match u64::try_from(count.into_inner()) {
                    Ok(u) => usize::cast_from(u),
                    Err(_) => {
                        return Err(AdapterError::Unstructured(anyhow::anyhow!(
                            "Negative multiplicity in constant result: {}",
                            count
                        )));
                    }
                };
                match std::num::NonZeroUsize::new(count) {
                    Some(nzu) => {
                        results.push((row, nzu));
                    }
                    None => {
                        // No need to retain 0 diffs.
                    }
                };
            }
            let row_collection = RowCollection::new(results, &finishing.order_by);
            return match finishing.finish(
                row_collection,
                max_result_size,
                max_returned_query_size,
                row_set_finishing_seconds.as_ref(),
            ) {
                Ok((rows, _bytes)) => Ok(Coordinator::send_immediate_rows(rows)),
                // TODO(peek-seq): make this a structured error. (also in the old sequencing)
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }

        // If this is a PeekExisting on a mocked index, return mock rows via the
        // streaming path so that metrics (e.g. mz_time_to_first_row_seconds) are
        // recorded with the correct instance_id and strategy labels.
        if let FastPathPlan::PeekExisting(_coll_id, idx_id, _, _) = &fast_path {
            if let Some(mock_rows) = self.mock_peek_data.as_ref().and_then(|m| m.get(idx_id)) {
                let results: Vec<_> = mock_rows
                    .iter()
                    .map(|row| (row.clone(), std::num::NonZeroUsize::new(1).unwrap()))
                    .collect();
                let row_collection = RowCollection::new(results, &finishing.order_by);
                return match finishing.finish(
                    row_collection,
                    max_result_size,
                    max_returned_query_size,
                    row_set_finishing_seconds.as_ref(),
                ) {
                    Ok((rows, _bytes)) => {
                        let row_iter = Box::new(mz_repr::IntoRowIterator::into_row_iter(rows));
                        let stream = futures::stream::once(futures::future::ready(
                            PeekResponseUnary::Rows(row_iter),
                        ));
                        Ok(crate::ExecuteResponse::SendingRowsStreaming {
                            rows: Box::pin(stream),
                            instance_id: compute_instance,
                            strategy: statement_logging::StatementExecutionStrategy::FastPath,
                        })
                    }
                    Err(e) => Err(AdapterError::ResultSize(e)),
                };
            }
        }

        let (peek_target, target_read_hold, literal_constraints, mfp, strategy) = match fast_path {
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, mfp) => {
                let peek_target = PeekTarget::Index { id: idx_id };
                let target_read_hold =
                    target_read_hold.expect("missing read hold for PeekExisting peek target");
                let strategy = statement_logging::StatementExecutionStrategy::FastPath;
                (
                    peek_target,
                    target_read_hold,
                    literal_constraints,
                    mfp,
                    strategy,
                )
            }
            FastPathPlan::PeekPersist(coll_id, literal_constraint, mfp) => {
                let literal_constraints = literal_constraint.map(|r| vec![r]);
                let metadata = self
                    .storage_collections
                    .collection_metadata(coll_id)
                    .map_err(AdapterError::concurrent_dependency_drop_from_collection_missing)?
                    .clone();
                let peek_target = PeekTarget::Persist {
                    id: coll_id,
                    metadata,
                };
                let target_read_hold =
                    target_read_hold.expect("missing read hold for PeekPersist peek target");
                let strategy = statement_logging::StatementExecutionStrategy::PersistFastPath;
                (
                    peek_target,
                    target_read_hold,
                    literal_constraints,
                    mfp,
                    strategy,
                )
            }
            _ => {
                // FastPathPlan::Constant handled above.
                unreachable!()
            }
        };

        let (rows_tx, rows_rx) = oneshot::channel();
        let uuid = fast_new_uuid_v4();

        // result_desc is now passed in pre-built (cached in PlanCacheEntry or built by caller).

        self.ensure_compute_instance_client(compute_instance)
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_instance_missing)?;

        // Register coordinator tracking of this peek (fire-and-forget).
        // The coordinator will process this asynchronously. There is a benign race where compute
        // might respond before the coordinator stores the registration, in which case the
        // peek notification handler will silently ignore the response (it already handles
        // missing peeks for cancellation).
        //
        // Warning: If we fail to actually issue the peek after this point, then we need to
        // unregister it to avoid an orphaned registration.
        self.coordinator_client.send(Command::RegisterFrontendPeek {
            uuid,
            conn_id: conn_id.clone(),
            cluster_id: compute_instance,
            depends_on,
            is_fast_path: true,
            watch_set,
        });

        let finishing_for_instance = finishing.clone();
        // Unwrap Arc<RelationDesc> to owned. For the PeekExisting fast path
        // (cached queries), this clone is unavoidable because the Peek command
        // is serialized and sent to the compute worker. However, for Constant
        // fast-path peeks (handled above), the Arc avoids the clone entirely.
        let result_desc_owned = Arc::unwrap_or_clone(result_desc);
        let peek_result = self.compute_instances[&compute_instance].peek(
            peek_target,
            literal_constraints,
            uuid,
            timestamp,
            result_desc_owned,
            finishing_for_instance,
            mfp,
            target_read_hold,
            target_replica,
            rows_tx,
        );

        if let Err(err) = peek_result {
            // Clean up the registered peek since the peek failed to issue (fire-and-forget).
            // The frontend will handle statement logging for the error.
            self.coordinator_client
                .send(Command::UnregisterFrontendPeek { uuid });
            return Err(AdapterError::internal("frontend peek error", err));
        }

        let peek_response_stream = Coordinator::create_peek_response_stream(
            rows_rx,
            finishing,
            max_result_size,
            max_returned_query_size,
            row_set_finishing_seconds,
            self.persist_client.clone(),
            peek_stash_read_batch_size_bytes,
            peek_stash_read_memory_budget_bytes,
        );

        Ok(crate::ExecuteResponse::SendingRowsStreaming {
            rows: Box::pin(peek_response_stream),
            instance_id: compute_instance,
            strategy,
        })
    }

    // Statement logging helper methods

    /// Log the beginning of statement execution.
    pub(crate) fn log_began_execution(
        &self,
        record: statement_logging::StatementBeganExecutionRecord,
        mseh_update: Row,
        prepared_statement: Option<PreparedStatementEvent>,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::BeganExecution {
                    record,
                    mseh_update,
                    prepared_statement,
                },
            ));
    }

    /// Log cluster selection for a statement.
    pub(crate) fn log_set_cluster(
        &self,
        id: StatementLoggingId,
        cluster_id: mz_controller_types::ClusterId,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetCluster { id, cluster_id },
            ));
    }

    /// Log timestamp determination for a statement.
    pub(crate) fn log_set_timestamp(&self, id: StatementLoggingId, timestamp: mz_repr::Timestamp) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetTimestamp { id, timestamp },
            ));
    }

    /// Log transient index ID for a statement.
    pub(crate) fn log_set_transient_index_id(
        &self,
        id: StatementLoggingId,
        transient_index_id: mz_repr::GlobalId,
    ) {
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::SetTransientIndex {
                    id,
                    transient_index_id,
                },
            ));
    }

    /// Log a statement lifecycle event.
    pub(crate) fn log_lifecycle_event(
        &self,
        id: StatementLoggingId,
        event: statement_logging::StatementLifecycleEvent,
    ) {
        let when = (self.statement_logging_frontend.now)();
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::Lifecycle { id, event, when },
            ));
    }

    /// Log the end of statement execution.
    pub(crate) fn log_ended_execution(
        &self,
        id: StatementLoggingId,
        reason: statement_logging::StatementEndedExecutionReason,
    ) {
        let ended_at = (self.statement_logging_frontend.now)();
        let record = statement_logging::StatementEndedExecutionRecord {
            id: id.0,
            reason,
            ended_at,
        };
        self.coordinator_client
            .send(Command::FrontendStatementLogging(
                FrontendStatementLoggingEvent::EndedExecution(record),
            ));
    }
}
