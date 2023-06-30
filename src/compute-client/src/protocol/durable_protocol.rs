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
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, info};

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
use crate::types::dataflows::IndexDesc;
use crate::types::dataflows::{BuildDesc, DataflowDescription};
use crate::types::sinks::{ComputeSinkConnection, ComputeSinkDesc};
use crate::types::sources::SourceInstanceDesc;

#[derive(Debug)]
pub struct DurableProtocol<T: Timestamp = mz_repr::Timestamp> {
    tx: UnboundedSender<(tracing::Span, ComputeCommand<T>)>,
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

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(tracing::Span, _)>();

        let mut worker = DurableProtocolWorker {
            instance_id,
            trace: Vec::new(),
            current_configuration: None,
            current_dataflows: BTreeMap::new(),
            current_as_ofs: BTreeMap::new(),
            current_peeks: BTreeMap::new(),
            rx,
            upper: restart_as_of,
            listen,
            write_handle,
        };

        worker.apply_updates(initial_snapshot);

        mz_ore::task::spawn(
            || "DurableProtocolWorker",
            async move { worker.run().await },
        );

        let this = Self { tx };

        this
    }

    pub fn send(&self, cmd: ComputeCommand<T>) {
        self.tx
            .send((tracing::Span::current(), cmd))
            .expect("worker exited while its handle was alive")
    }
}

/// State of the protocol worker, which does the work of translating incoming
/// [`ComputeCommands`](ComputeCommand) to their differentialized version:
/// [`DurableComputeCommand`].
///
/// We keep a materialized view of the current state of things, so that we can differentialize
/// commands when they come in. The `current_*` fields contain this current state.
#[derive(Debug)]
pub struct DurableProtocolWorker<T: Timestamp = mz_repr::Timestamp> {
    instance_id: ComputeInstanceId,

    // Current state corresponding to what's in the trace/shard.
    current_configuration: Option<ComputeParameters>,
    current_dataflows: BTreeMap<
        DurableDataflowDescription<crate::plan::Plan<T>, CollectionMetadata, T>,
        Vec<GlobalId>,
    >,
    current_as_ofs: BTreeMap<GlobalId, AntichainWrapper<T>>,
    current_peeks: BTreeMap<uuid::Uuid, Peek<T>>,

    /// Commands as they exist in the shard, for checking which incoming commands we still have
    /// to write.
    trace: Vec<(TargetedDurableComputeCommand<T>, u64, i64)>,

    rx: UnboundedReceiver<(tracing::Span, ComputeCommand<T>)>,

    upper: u64,
    listen: Listen<TargetedDurableComputeCommand<T>, (), u64, i64>,
    write_handle: WriteHandle<TargetedDurableComputeCommand<T>, (), u64, i64>,
}

impl<T: Timestamp> DurableProtocolWorker<T> {
    async fn run(mut self) {
        while let Some((_span, cmd)) = self.rx.recv().await {
            loop {
                let durable_cmds = self.absorb(&cmd);

                let mut cmds = durable_cmds
                    .into_iter()
                    .map(|(cmd, diff)| {
                        (
                            TargetedDurableComputeCommand(self.instance_id.clone(), cmd),
                            diff,
                        )
                    })
                    .collect_vec();

                // See if we have any dataflows where none of the exports are needed anymore. We can
                // drop those.
                //
                // Should probably not do this check every time, though!
                for (dataflow, exports) in self.current_dataflows.iter() {
                    if exports.is_empty() {
                        cmds.push((
                            TargetedDurableComputeCommand(
                                self.instance_id.clone(),
                                DurableComputeCommand::CreateDataflows(dataflow.clone()),
                            ),
                            -1,
                        ));

                        // This is brittle right now, so we need to rework, but because the exports
                        // are empty we know that all he AsOfs for exported objects are at the empty
                        // Antichain. We should emit retractions for those.
                        for export_id in dataflow.export_ids() {
                            cmds.push((
                                TargetedDurableComputeCommand(
                                    self.instance_id.clone(),
                                    DurableComputeCommand::AsOf(
                                        export_id,
                                        AntichainWrapper(Antichain::new()),
                                    ),
                                ),
                                -1,
                            ));
                        }
                    }
                }

                if cmds.is_empty() {
                    break;
                }

                let res = self.commit_updates(cmds).await;

                match res {
                    Ok(_) => {
                        // All's well!
                        break;
                    }
                    Err(upper_mismatch) => {
                        debug!(
                            "error while trying to append durable commands: {:?}",
                            upper_mismatch
                        );
                        self.sync_to_recent_upper().await;
                    }
                }
            }
        }

        tracing::trace!("shutting down durable protocol worker task");
    }

    /// Absorb new compute commands and synthesize durable compute commands when necessary, based on
    /// our current view of the contents of the protocol log.
    ///
    /// It's important that this does not get a mutable reference to current state: state should
    /// only be modified when hearing back about updates from the log, even our own updates.
    fn absorb(&self, cmd: &ComputeCommand<T>) -> Vec<(DurableComputeCommand<T>, i64)> {
        let mut cmds = Vec::new();

        match cmd {
            ComputeCommand::CreateTimely { .. } => {
                // unreachable!("should not be in the durable protocol")
            }
            ComputeCommand::CreateInstance(_) => {
                // unreachable!("should not be in the durable protocol")
            }
            ComputeCommand::InitializationComplete => {
                // unreachable!("should not be in the durable protocol")
            }
            ComputeCommand::UpdateConfiguration(config) => {
                match &self.current_configuration {
                    Some(current_configuration) if current_configuration == config => {
                        // Nothing to do!
                    }
                    Some(current_configuration) => {
                        // Need to retract the previous value and create an update for the new
                        // config.
                        cmds.push((
                            DurableComputeCommand::UpdateConfiguration(
                                current_configuration.clone(),
                            ),
                            -1,
                        ));
                        cmds.push((
                            DurableComputeCommand::UpdateConfiguration(config.clone()),
                            1,
                        ));
                    }
                    None => {
                        cmds.push((
                            DurableComputeCommand::UpdateConfiguration(config.clone()),
                            1,
                        ));
                    }
                }
            }
            ComputeCommand::CreateDataflows(dataflows) => {
                let mut durable_dataflows = Vec::new();
                let mut as_ofs = Vec::new();

                for dataflow in dataflows {
                    let (durable_dataflow, mut new_as_ofs) =
                        DurableDataflowDescription::from(dataflow.clone());

                    if self.current_dataflows.contains_key(&durable_dataflow) {
                        // Nothing to do!
                        continue;
                    }

                    // Need to persist an update.
                    durable_dataflows.push(durable_dataflow);
                    as_ofs.append(&mut new_as_ofs);
                }

                for durable_dataflow in durable_dataflows {
                    cmds.push((DurableComputeCommand::CreateDataflows(durable_dataflow), 1));
                }

                for (id, as_of) in as_ofs {
                    match self.current_as_ofs.get(&id) {
                        Some(current_as_of) if current_as_of == &as_of => {
                            // Nothing to do!
                        }
                        Some(current_as_of)
                            if PartialOrder::less_than(&current_as_of.0, &as_of.0) =>
                        {
                            // Need to retract the previous value and create an update for the new
                            // as_of.
                            cmds.push((
                                DurableComputeCommand::AsOf(id.clone(), current_as_of.clone()),
                                -1,
                            ));
                            cmds.push((DurableComputeCommand::AsOf(id.clone(), as_of), 1));
                        }
                        Some(current_as_of) => {
                            panic!(
                                "cannot regress as of for {id} from {:?} to {:?}",
                                current_as_of.0, as_of.0
                            );
                        }
                        None => {
                            cmds.push((DurableComputeCommand::AsOf(id.clone(), as_of), 1));
                        }
                    }
                }
            }
            ComputeCommand::AllowCompaction(frontiers) => {
                for (id, as_of) in frontiers {
                    match self.current_as_ofs.get(&id) {
                        Some(current_as_of) if &current_as_of.0 == as_of => {
                            // Nothing to do!
                        }
                        Some(current_as_of) if PartialOrder::less_than(&current_as_of.0, as_of) => {
                            // Need to retract the previous value and create an update for the new
                            // as_of.
                            cmds.push((
                                DurableComputeCommand::AsOf(id.clone(), current_as_of.clone()),
                                -1,
                            ));
                            cmds.push((
                                DurableComputeCommand::AsOf(
                                    id.clone(),
                                    AntichainWrapper(as_of.clone()),
                                ),
                                1,
                            ));
                        }
                        Some(current_as_of) => {
                            panic!(
                                "cannot regress as of for {id} from {:?} to {:?}",
                                current_as_of.0, as_of
                            );
                        }
                        None => {
                            cmds.push((
                                DurableComputeCommand::AsOf(
                                    id.clone(),
                                    AntichainWrapper(as_of.clone()),
                                ),
                                1,
                            ));
                        }
                    }
                }
            }
            ComputeCommand::Peek(peek) => {
                if !self.current_peeks.contains_key(&peek.uuid) {
                    cmds.push((DurableComputeCommand::Peek(peek.clone()), 1));
                }
            }
            ComputeCommand::CancelPeeks { uuids } => {
                for uuid in uuids {
                    let peek = self.current_peeks.get(uuid);
                    match peek {
                        Some(peek) => {
                            cmds.push((DurableComputeCommand::Peek(peek.clone()), -1));
                        }
                        None => {
                            panic!("no peek for uuid {:?}", uuid);
                        }
                    }
                }
            }
        }

        cmds
    }

    async fn commit_updates(
        &mut self,
        updates: Vec<(TargetedDurableComputeCommand<T>, i64)>,
    ) -> Result<(), UpperMismatch<u64>> {
        debug!("commit_updates {:?}", updates);

        let current_upper = self.upper.clone();
        // Yeah yeah, need to make this better...
        let next_upper = current_upper + 1;

        let mut batch_builder = self
            .write_handle
            .builder(Antichain::from_elem(current_upper));

        for (state_update, diff) in updates.into_iter() {
            batch_builder
                .add(&state_update, &(), &current_upper, &diff)
                .await
                .expect("invalid usage");
        }

        let mut batch = batch_builder
            .finish(Antichain::from_elem(next_upper))
            .await
            .expect("invalid usage");

        let res = self
            .write_handle
            .compare_and_append_batch(
                &mut [&mut batch],
                Antichain::from_elem(current_upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage");

        match res {
            Ok(()) => {
                // All's well! We can sync back our updates to update our
                // in-memory maps/traces.
                self.sync(next_upper).await;
            }
            Err(upper_mismatch) => return Err(upper_mismatch),
        }

        Ok(())
    }

    pub async fn sync_to_recent_upper(&mut self) {
        let upper = self.write_handle.fetch_recent_upper().await;
        let upper = upper.as_option().cloned().expect("closed out shard");
        self.sync(upper).await
    }

    async fn sync(&mut self, target_upper: u64) {
        let mut updates: Vec<(TargetedDurableComputeCommand<T>, u64, i64)> = Vec::new();

        // Tail the log until we reach the target upper. Note that, in the
        // common case, we are also the writer, so we are waiting to read back
        // what we wrote.
        while self.upper < target_upper {
            let listen_events = self.listen.fetch_next().await;

            for listen_event in listen_events.into_iter() {
                match listen_event {
                    ListenEvent::Progress(upper) => {
                        debug!("sync; progress: {:?}", upper);
                        self.upper = upper
                            .as_option()
                            .cloned()
                            .expect("we use a totally ordered time and never finalize the shard");
                    }
                    ListenEvent::Updates(batch_updates) => {
                        debug!("sync; updates: {:?}", batch_updates);
                        let mut batch_updates = batch_updates
                            .into_iter()
                            .map(|((key, _unit), ts, diff)| {
                                (key.expect("key decoding error"), ts, diff)
                            })
                            .collect_vec();

                        updates.append(&mut batch_updates);
                    }
                }
            }
        }

        self.apply_updates(updates);
    }

    // TODO(aljoscha): We should keep the collections as a _trace_, with the
    // full diffs. And only collapse them down to a map when needed. And also
    // ensure that there is only one entry for a given `GlobalId` then.
    fn apply_updates(&mut self, mut updates: Vec<(TargetedDurableComputeCommand<T>, u64, i64)>) {
        let interesting_cmds = updates
            .iter()
            .cloned()
            .filter(|(cmd, _ts, _diff)| cmd.0 == self.instance_id)
            .filter(|(cmd, _ts, _diff)| {
                !matches!(cmd.1, DurableComputeCommand::AsOf(GlobalId::System(_), _))
            })
            .map(|(cmd, _ts, diff)| (cmd.1, diff))
            .collect_vec();
        if !interesting_cmds.is_empty() {
            info!("durable protocol updates: {:?}", interesting_cmds);
        }

        self.trace.append(&mut updates);

        let upper_frontier = Antichain::from_elem(self.upper);
        for (_update, ts, _diff) in self.trace.iter_mut() {
            ts.advance_by(upper_frontier.borrow());
        }

        // TODO(aljoscha): Make sure we reduce the capacity of `self.trace` if
        // it grows too much.
        consolidation::consolidate_updates(&mut self.trace);

        // TODO(aljoscha): This is very inefficient, we're rebuilding the
        // in-memory cash of every type of collection every time we're applying
        // updates. It's easy and correct, though.
        self.current_dataflows = BTreeMap::new();
        self.current_as_ofs = BTreeMap::new();
        self.current_peeks = BTreeMap::new();

        for update in self.trace.iter() {
            let state_update = match update {
                (state_update, _ts, 1) if state_update.0 == self.instance_id => {
                    state_update.1.clone()
                }
                (_state_update, _ts, 1) => {
                    continue;
                }
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            };
            match state_update {
                DurableComputeCommand::UpdateConfiguration(config) => {
                    self.current_configuration.replace(config);
                }
                DurableComputeCommand::CreateDataflows(dataflow) => {
                    let exports = dataflow.export_ids().collect_vec();
                    let prev = self.current_dataflows.insert(dataflow, exports);
                    assert!(prev.is_none());
                }
                DurableComputeCommand::AsOf(_id, _as_of) => {
                    // Do nothing, we handle these after all the others so that they apply on "good"
                    // state. This is a _stupid_ way of doing things, and we should stop rebuilding
                    // the maps from scratch every time.
                }
                DurableComputeCommand::Peek(peek) => {
                    let prev = self.current_peeks.insert(peek.uuid.clone(), peek);
                    assert!(prev.is_none());
                }
            }
        }
        for update in self.trace.iter() {
            let state_update = match update {
                (state_update, _ts, 1) if state_update.0 == self.instance_id => {
                    state_update.1.clone()
                }
                (_state_update, _ts, 1) => {
                    continue;
                }
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            };
            match state_update {
                DurableComputeCommand::AsOf(id, as_of) => {
                    if as_of.0.is_empty() {
                        // We might have the change to remove referenced exports from a dataflow
                        // description.
                        //
                        // Again, this is a hare-brained way of doing things, but it should be
                        // correct. See above about making this whole operation incremental instead
                        // of rebuilding from scratch every time.
                        for exports in self.current_dataflows.values_mut() {
                            exports.retain(|export_id| &id != export_id);
                        }
                    }

                    let prev = self.current_as_ofs.insert(id, as_of);
                    assert!(prev.is_none());
                }
                _ => (), // Ignore.
            }
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetedDurableComputeCommand<T: Ord + PartialOrd = mz_repr::Timestamp>(
    ComputeInstanceId,
    DurableComputeCommand<T>,
);

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
    CreateDataflows(DurableDataflowDescription<crate::plan::Plan<T>, CollectionMetadata, T>),

    /// Optional as of for the identified object.
    AsOf(GlobalId, AntichainWrapper<T>),

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

impl<T: Timestamp> Codec for TargetedDurableComputeCommand<T> {
    type Schema = TodoSchema<TargetedDurableComputeCommand<T>>;

    fn codec_name() -> String {
        "DurableComputeCommandSerde".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes: Vec<u8> = bincode::serialize(&self).unwrap();
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        bincode::deserialize(&buf[..]).map_err(|err| err.to_string())
    }
}

/// Durable version of [`DataflowDescription`] that doesn't have an `as_of` field. The `as_of` is
/// the part that can change as we allow compaction, which makes it hard to compare descriptions
/// that are otherwise the same, plus it makes it more costly to repeatedly send retractions/updates
/// on the protocol if we have to do that every time the `as_of` changes.
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
    pub sink_exports: BTreeMap<GlobalId, DurableComputeSinkDesc<S, T>>,
    /// Human readable name
    pub debug_name: String,
    /// Frontier beyond which the dataflow should not execute.
    /// Specifically, updates at times greater or equal to this frontier are suppressed.
    /// This is often set to `as_of + 1` to enable "batch" computations.
    pub until: AntichainWrapper<T>,
}

impl<P, S, T: Timestamp> DurableDataflowDescription<P, S, T> {
    fn from(
        desc: DataflowDescription<P, S, T>,
    ) -> (
        DurableDataflowDescription<P, S, T>,
        Vec<(GlobalId, AntichainWrapper<T>)>,
    ) {
        let DataflowDescription {
            source_imports,
            index_imports,
            objects_to_build,
            index_exports,
            sink_exports,
            as_of,
            until,
            debug_name,
        } = desc;

        // If there is an `as_of`, we need to "explode" it to all the exports to make it
        // explicit: the as_of _does_ only apply to exports, and AllowCompaction commands will
        // target individual exports, not a DataflowDescription.
        let as_ofs = if let Some(as_of) = as_of {
            index_exports
                .keys()
                .map(|export_id| (export_id.clone(), AntichainWrapper(as_of.clone())))
                .collect()
        } else {
            Vec::new()
        };

        let durable_sink_exports = sink_exports
            .into_iter()
            .map(|(id, desc)| {
                let ComputeSinkDesc {
                    from,
                    from_desc,
                    connection,
                    with_snapshot,
                    up_to,
                } = desc;
                let durable_desc = DurableComputeSinkDesc {
                    from,
                    from_desc,
                    connection,
                    with_snapshot,
                    up_to: AntichainWrapper(up_to),
                };

                (id, durable_desc)
            })
            .collect();

        let durable_dataflow_desc = DurableDataflowDescription {
            source_imports,
            index_imports,
            objects_to_build,
            index_exports,
            sink_exports: durable_sink_exports,
            until: AntichainWrapper(until),
            debug_name,
        };

        (durable_dataflow_desc, as_ofs)
    }

    /// Identifiers of exported objects (indexes and sinks).
    pub fn export_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.index_exports
            .keys()
            .chain(self.sink_exports.keys())
            .cloned()
    }
}

/// Durable version of [`ComputeSinkDesc`] that doesn't have an `as_of` field.
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct DurableComputeSinkDesc<S: 'static = (), T: PartialOrd + Ord = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: ComputeSinkConnection<S>,
    pub with_snapshot: bool,
    pub up_to: AntichainWrapper<T>,
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
