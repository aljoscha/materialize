// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstraction for maintaining a _durable_ log of compute commands.

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;
use tracing::info;

use mz_expr::{CollectionPlan, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::Listen;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistClient;
use mz_persist_client::ShardId;
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec;
use mz_repr::{GlobalId, RelationDesc, RelationType};
use mz_storage_client::controller::CollectionMetadata;

use crate::controller::ComputeInstanceId;
use crate::protocol::command::{ComputeCommand, ComputeParameters, Peek};
use crate::types::dataflows::BuildDesc;
use crate::types::dataflows::IndexDesc;
use crate::types::sinks::ComputeSinkConnection;
use crate::types::sources::SourceInstanceDesc;

#[derive(Debug)]
pub struct DurableProtocol<T: Timestamp = mz_repr::Timestamp> {
    instance_id: ComputeInstanceId,
    trace: Vec<(DurableComputeCommand<T>, u64, i64)>,

    tx: UnboundedSender<(tracing::Span, ComputeCommand<T>)>,

    upper: u64,
    listen: Listen<DurableComputeCommand<T>, (), u64, i64>,
    write_handle: WriteHandle<DurableComputeCommand<T>, (), u64, i64>,
}

/// Compute protocol commands, sent by the compute controller to replicas.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableComputeCommand<T: Ord + PartialOrd = mz_repr::Timestamp> {
    /// `UpdateConfiguration` instructs the replica to update its configuration, according to the
    /// given [`ComputeParameters`].
    ///
    /// Parameter updates transmitted through this command must be applied by the replica as soon
    /// as it receives the command, and they must be apply globally to all replica state, even
    /// dataflows and pending peeks that were created before the parameter update. This property
    /// allows the replica to hoist `UpdateConfiguration` commands during reconciliation.
    ///
    /// Configuration parameters that should not be applied globally, but only to specific
    /// dataflows or peeks, should be added to the [`DataflowDescription`] or [`Peek`] types,
    /// rather than as [`ComputeParameters`].
    UpdateConfiguration(ComputeParameters),

    /// `CreateDataflows` instructs the replica to create and start maintaining dataflows according
    /// to the given [`DataflowDescription`]s.
    ///
    /// If a `CreateDataflows` command defines multiple dataflows, the list of
    /// [`DataflowDescription`]s must be topologically ordered according to the dependency
    /// relation.
    ///
    /// Each [`DataflowDescription`] must have the following properties:
    ///
    ///   * Dataflow imports are valid:
    ///     * Imported storage collections specified in [`source_imports`] exist and are readable by
    ///       the compute replica.
    ///     * Imported indexes specified in [`index_imports`] have been created on the replica
    ///       previously, either by previous `CreateDataflows` commands, or by the same
    ///       `CreateDataflows` command.
    ///   * Dataflow imports are readable at the specified [`as_of`]. In other words: The `since`s of
    ///     imported collections are not beyond the dataflow [`as_of`].
    ///   * Dataflow exports have unique IDs, i.e., the IDs of exports from dataflows a replica is
    ///     instructed to create do not repeat (within a single protocol iteration).
    ///   * The dataflow objects defined in [`objects_to_build`] are topologically ordered according
    ///     to the dependency relation.
    ///
    /// A dataflow description that violates any of the above properties can cause the replica to
    /// exhibit undefined behavior, such as panicking or production of incorrect results. A replica
    /// should prefer panicking over producing incorrect results.
    ///
    /// After receiving a `CreateDataflows` command, for created dataflows that export indexes or
    /// storage sinks, the replica must produce [`FrontierUppers`] responses that report the
    /// advancement of the `upper` frontiers of these compute collections.
    ///
    /// After receiving a `CreateDataflows` command, for created dataflows that export subscribes,
    /// the replica must produce [`SubscribeResponse`]s that report the progress and results of the
    /// subscribes.
    ///
    /// [`objects_to_build`]: DataflowDescription::objects_to_build
    /// [`source_imports`]: DataflowDescription::source_imports
    /// [`index_imports`]: DataflowDescription::index_imports
    /// [`as_of`]: DataflowDescription::as_of
    /// [`FrontierUppers`]: super::response::ComputeResponse::FrontierUppers
    /// [`SubscribeResponse`]: super::response::ComputeResponse::SubscribeResponse
    /// [Initialization Stage]: super#initialization-stage
    CreateDataflows(Vec<DurableDataflowDescription<crate::plan::Plan<T>, CollectionMetadata>>),

    /// Optional as of for the identified object.
    AsOf(AntichainWrapper<T>),

    /// `Peek` instructs the replica to perform a peek at an index.
    ///
    /// The [`Peek`] description must have the following properties:
    ///
    ///   * The target index has previously been created by a corresponding `CreateDataflows`
    ///     command.
    ///   * The [`Peek::uuid`] is unique, i.e., the UUIDs of peeks a replica gets instructed to
    ///     perform do not repeat (within a single protocol iteration).
    ///
    /// A [`Peek`] description that violates any of the above properties can cause the replica to
    /// exhibit undefined behavior.
    ///
    /// Specifying a [`Peek::timestamp`] that is less than the target index’s `since` frontier does
    /// not provoke undefined behavior. Instead, the replica must produce a [`PeekResponse::Error`]
    /// in response.
    ///
    /// After receiving a `Peek` command, the replica must eventually produce a single
    /// [`PeekResponse`]:
    ///
    ///    * For peeks that were not cancelled: either [`Rows`] or [`Error`].
    ///    * For peeks that were cancelled: either [`Rows`], or [`Error`], or [`Canceled`].
    ///
    /// [`PeekResponse`]: super::response::PeekResponse
    /// [`PeekResponse::Error`]: super::response::PeekResponse::Error
    /// [`Rows`]: super::response::PeekResponse::Rows
    /// [`Error`]: super::response::PeekResponse::Error
    /// [`Canceled`]: super::response::PeekResponse::Canceled
    Peek(Peek<T>),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AntichainWrapper<T = mz_repr::Timestamp>(Antichain<T>);

impl<T: Ord + PartialOrd> PartialOrd for AntichainWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0 == other.0 {
            return Some(std::cmp::Ordering::Equal);
        }

        // YOLO partial order, because we only need it for consolidation.
        let res = self
            .0
            .first()
            .and_then(|a| other.0.first().and_then(|b| a.partial_cmp(b)));

        res
    }
}

impl<T: Ord + PartialOrd> Ord for AntichainWrapper<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.0 == other.0 {
            return std::cmp::Ordering::Equal;
        }

        // YOLO order, because we only need it for consolidation.
        let res = self
            .0
            .first()
            .and_then(|a| other.0.first().and_then(|b| a.partial_cmp(b)));

        res.unwrap_or(std::cmp::Ordering::Less)
    }
}

/// Durable version of [`DataflowDescription`] that doesn't have an `as_of` field.
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct DurableDataflowDescription<P, S: 'static = (), T: PartialOrd + Ord = mz_repr::Timestamp>
{
    /// Sources instantiations made available to the dataflow pair with monotonicity information.
    pub source_imports: BTreeMap<GlobalId, (SourceInstanceDesc<S>, bool)>,
    /// Indexes made available to the dataflow.
    /// (id of new index, description of index, relationtype of base source/view, monotonic)
    pub index_imports: BTreeMap<GlobalId, (IndexDesc, RelationType, bool)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<P>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
    pub index_exports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: BTreeMap<GlobalId, DurableComputeSinkDesc<S>>,
    /// Human readable name
    pub debug_name: String,
    /// Frontier beyond which the dataflow should not execute.
    /// Specifically, updates at times greater or equal to this frontier are suppressed.
    /// This is often set to `as_of + 1` to enable "batch" computations.
    pub until: AntichainWrapper<T>,
}

/// Durable version of [`ComputeSinkDesc`] that doesn't have an `as_of` field.
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct DurableComputeSinkDesc<S: 'static = ()> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: ComputeSinkConnection<S>,
    pub with_snapshot: bool,
}

impl<T: Timestamp> DurableProtocol<T> {
    pub async fn new(
        instance_id: ComputeInstanceId,
        shard_id: ShardId,
        persist_client: PersistClient,
    ) -> DurableProtocol<T> {
        let purpose = "DurableCoordState".to_string();
        let (mut write_handle, mut read_handle) = persist_client
            .open(
                shard_id,
                &purpose,
                Arc::new(TodoSchema::default()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid usage");

        let since = read_handle.since().clone();
        let upper: u64 = write_handle
            .upper()
            .as_option()
            .cloned()
            .expect("we use a totally ordered time and never finalize the shard");

        let upper = if upper == 0 {
            // Get around the fact that we can't get a snapshot and listen when
            // the upper is 0.
            //
            // And yes, we should probably not do this in a production impl.
            // const EMPTY_UPDATES: &[((StateUpdate<T>, ()), u64, i64)] = &[];
            let batch_builder = write_handle.builder(Antichain::from_elem(0));
            let mut batch = batch_builder
                .finish(Antichain::from_elem(1))
                .await
                .expect("invalid usage");

            let _res = write_handle
                .compare_and_append_batch(
                    &mut [&mut batch],
                    Antichain::from_elem(0),
                    Antichain::from_elem(1),
                )
                .await
                .expect("unvalid usage");
            1
        } else {
            upper
        };

        let mut restart_as_of = upper.saturating_sub(1);
        restart_as_of.advance_by(since.borrow());

        // Always true, currently!
        let initial_snapshot = if restart_as_of > 0 {
            read_handle
                .snapshot_and_fetch(Antichain::from_elem(restart_as_of))
                .await
                .expect("we have advanced the restart_as_of by the since")
        } else {
            Vec::new()
        };

        let initial_snapshot = initial_snapshot
            .into_iter()
            .map(|((key, _unit), ts, diff)| (key.expect("key decoding error"), ts, diff))
            .collect_vec();

        debug!("initial snapshot: {:?}", initial_snapshot);

        debug!("creating listen at {:?}", restart_as_of);
        let listen = read_handle
            .listen(Antichain::from_elem(restart_as_of))
            .await
            .expect("invalid usage");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tracing::Span, _)>();

        mz_ore::task::spawn(|| "DurableProtocolWorker", async move {
            while let Some(cmd) = rx.recv().await {
                println!("got cmd: {:?}", cmd);
            }
            tracing::trace!("shutting down durable protocol worker task");
        });

        let mut this = Self {
            instance_id,
            trace: Vec::new(),
            tx,
            upper: restart_as_of,
            listen,
            write_handle,
        };

        this
    }

    pub fn send(&self, cmd: ComputeCommand<T>) {
        self.tx
            .send((tracing::Span::current(), cmd))
            .expect("worker exited while its handle was alive")
    }
}

impl<T: Timestamp> Codec for DurableComputeCommand<T> {
    type Schema = TodoSchema<DurableComputeCommand<T>>;

    fn codec_name() -> String {
        "DurableComputeCommandSerde".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes = serde_json::to_vec(&self).expect("failed to encode DurableComputeCommand");
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        serde_json::from_slice(buf).map_err(|err| err.to_string())
    }
}
