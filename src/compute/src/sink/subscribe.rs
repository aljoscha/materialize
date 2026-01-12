// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use futures::StreamExt;
use differential_dataflow::{AsCollection, VecCollection};
use mz_compute_client::protocol::response::{
    StashedSubscribeBatch, SubscribeBatch, SubscribeResponse,
};
use mz_compute_types::sinks::{
    ComputeSinkDesc, SubscribeOutputFormat, SubscribePersistBatchesConfig, SubscribeSinkConnection,
};
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::ShardId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::StorageDiff;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::probe::{Handle, ProbeNotify};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::Antichain;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;

use crate::render::StartSignal;
use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for SubscribeSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: VecCollection<G, Row, Diff>,
        err_collection: VecCollection<G, DataflowError, Diff>,
        _ct_times: Option<VecCollection<G, (), Diff>>,
        output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        match &self.output_format {
            SubscribeOutputFormat::Rows => render_rows_subscribe(
                compute_state,
                sink,
                sink_id,
                as_of,
                sinked_collection,
                err_collection,
                output_probe,
            ),
            SubscribeOutputFormat::PersistBatches(config) => render_batch_subscribe(
                compute_state,
                sink,
                sink_id,
                as_of,
                sinked_collection,
                err_collection,
                output_probe,
                config,
            ),
        }
    }
}

/// Render the inline rows subscribe sink (original behavior).
fn render_rows_subscribe<G>(
    compute_state: &mut crate::compute_state::ComputeState,
    sink: &ComputeSinkDesc<CollectionMetadata>,
    sink_id: GlobalId,
    as_of: Antichain<Timestamp>,
    sinked_collection: VecCollection<G, Row, Diff>,
    err_collection: VecCollection<G, DataflowError, Diff>,
    output_probe: &Handle<Timestamp>,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // An encapsulation of the Subscribe response protocol.
    // Used to send rows and progress messages,
    // and alert if the dataflow was dropped before completing.
    let subscribe_protocol_handle = Rc::new(RefCell::new(Some(SubscribeProtocol {
        sink_id,
        sink_as_of: as_of.clone(),
        subscribe_response_buffer: Some(Rc::clone(&compute_state.subscribe_response_buffer)),
        prev_upper: Antichain::from_elem(Timestamp::minimum()),
        poison: None,
    })));
    let subscribe_protocol_weak = Rc::downgrade(&subscribe_protocol_handle);
    let sinked_collection = sinked_collection
        .inner
        .probe_notify_with(vec![output_probe.clone()])
        .as_collection();
    subscribe(
        sinked_collection,
        err_collection,
        sink_id,
        sink.with_snapshot,
        as_of,
        sink.up_to.clone(),
        subscribe_protocol_handle,
    );

    // Inform the coordinator that we have been dropped,
    // and destroy the subscribe protocol so the sink operator
    // can't send spurious messages while shutting down.
    Some(Rc::new(scopeguard::guard((), move |_| {
        if let Some(subscribe_protocol_handle) = subscribe_protocol_weak.upgrade() {
            std::mem::drop(subscribe_protocol_handle.borrow_mut().take())
        }
    })))
}

fn subscribe<G>(
    sinked_collection: VecCollection<G, Row, Diff>,
    err_collection: VecCollection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    with_snapshot: bool,
    as_of: Antichain<G::Timestamp>,
    up_to: Antichain<G::Timestamp>,
    subscribe_protocol_handle: Rc<RefCell<Option<SubscribeProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!("subscribe-{}", sink_id);
    let mut op = OperatorBuilder::new(name, sinked_collection.scope());
    let mut ok_input = op.new_input(&sinked_collection.inner, Pipeline);
    let mut err_input = op.new_input(&err_collection.inner, Pipeline);

    op.build(|_cap| {
        let mut rows_to_emit = Vec::new();
        let mut errors_to_emit = Vec::new();
        let mut finished = false;

        move |frontiers| {
            if finished {
                // Drain the inputs, to avoid the operator being constantly rescheduled
                ok_input.for_each(|_, _| {});
                err_input.for_each(|_, _| {});
                return;
            }

            let mut frontier = Antichain::new();
            for input_frontier in frontiers {
                frontier.extend(input_frontier.frontier().iter().copied());
            }

            let should_emit = |time: &Timestamp| {
                let beyond_as_of = if with_snapshot {
                    as_of.less_equal(time)
                } else {
                    as_of.less_than(time)
                };
                let before_up_to = !up_to.less_equal(time);
                beyond_as_of && before_up_to
            };

            ok_input.for_each(|_, data| {
                for (row, time, diff) in data.drain(..) {
                    if should_emit(&time) {
                        rows_to_emit.push((time, row, diff));
                    }
                }
            });
            err_input.for_each(|_, data| {
                for (error, time, diff) in data.drain(..) {
                    if should_emit(&time) {
                        errors_to_emit.push((time, error, diff));
                    }
                }
            });

            if let Some(subscribe_protocol) = subscribe_protocol_handle.borrow_mut().deref_mut() {
                subscribe_protocol.send_batch(
                    frontier.clone(),
                    &mut rows_to_emit,
                    &mut errors_to_emit,
                );
            }

            if PartialOrder::less_equal(&up_to, &frontier) {
                finished = true;
                // We are done; indicate this by sending a batch at the
                // empty frontier.
                if let Some(subscribe_protocol) = subscribe_protocol_handle.borrow_mut().deref_mut()
                {
                    subscribe_protocol.send_batch(
                        Antichain::default(),
                        &mut Vec::new(),
                        &mut Vec::new(),
                    );
                }
            }
        }
    });
}

/// A type that guides the transmission of rows back to the coordinator.
///
/// A protocol instance may `send` rows indefinitely in response to `send_batch` calls.
/// A `send_batch` call advancing the upper to the empty frontier is used to indicate the end of
/// a stream. If the stream is not advanced to the empty frontier, the `Drop` implementation sends
/// an indication that the protocol has finished without completion.
struct SubscribeProtocol {
    pub sink_id: GlobalId,
    pub sink_as_of: Antichain<Timestamp>,
    pub subscribe_response_buffer: Option<Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>>,
    pub prev_upper: Antichain<Timestamp>,
    /// The error poisoning this subscribe, if any.
    ///
    /// As soon as a subscribe has encountered an error, it is poisoned: It will only return the
    /// same error in subsequent batches, until it is dropped. The subscribe protocol currently
    /// does not support retracting errors (database-issues#5182).
    pub poison: Option<String>,
}

impl SubscribeProtocol {
    /// Attempt to send a batch of rows with the given `upper`.
    ///
    /// This method filters the updates to send based on the provided `upper`. Updates are only
    /// sent when their times are before `upper`. If `upper` has not advanced far enough, no batch
    /// will be sent. `rows` and `errors` that have been sent are drained from their respective
    /// vectors, only entries that have not been sent remain after the call returns. The caller is
    /// expected to re-submit these entries, potentially along with new ones, at a later `upper`.
    ///
    /// The subscribe protocol only supports reporting a single error. Because of this, it will
    /// only actually send the first error received in a `SubscribeResponse`. Subsequent errors are
    /// dropped. To simplify life for the caller, this method still maintains the illusion that
    /// `errors` are handled the same way as `rows`.
    fn send_batch(
        &mut self,
        upper: Antichain<Timestamp>,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
    ) {
        // Only send a batch if both conditions hold:
        //  a) `upper` has reached or passed the sink's `as_of` frontier.
        //  b) `upper` is different from when we last sent a batch.
        if !PartialOrder::less_equal(&self.sink_as_of, &upper) || upper == self.prev_upper {
            return;
        }

        // The compute protocol requires us to only send out consolidated batches.
        consolidate_updates(rows);
        consolidate_updates(errors);

        let (keep_rows, ship_rows) = rows.drain(..).partition(|u| upper.less_equal(&u.0));
        let (keep_errors, ship_errors) = errors.drain(..).partition(|u| upper.less_equal(&u.0));
        *rows = keep_rows;
        *errors = keep_errors;

        let updates = match (&self.poison, ship_errors.first()) {
            (Some(error), _) => {
                // The subscribe is poisoned; keep sending the same error.
                Err(error.clone())
            }
            (None, Some((_, error, _))) => {
                // The subscribe encountered its first error; poison it.
                let error = error.to_string();
                self.poison = Some(error.clone());
                Err(error)
            }
            (None, None) => {
                // No error encountered so for; ship the rows we have!
                Ok(ship_rows)
            }
        };

        let buffer = self
            .subscribe_response_buffer
            .as_mut()
            .expect("The subscribe response buffer is only cleared on drop.");

        buffer.borrow_mut().push((
            self.sink_id,
            SubscribeResponse::Batch(SubscribeBatch {
                lower: self.prev_upper.clone(),
                upper: upper.clone(),
                updates,
            }),
        ));

        let input_exhausted = upper.is_empty();
        self.prev_upper = upper;
        if input_exhausted {
            // The dataflow's input has been exhausted; clear the channel,
            // to avoid sending `SubscribeResponse::DroppedAt`.
            self.subscribe_response_buffer = None;
        }
    }
}

impl Drop for SubscribeProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.subscribe_response_buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }
    }
}

/// Render the persist batch subscribe sink.
///
/// This sink writes subscribe results to persist batches instead of sending
/// inline rows, using an async operator pattern similar to the materialized
/// view sink.
fn render_batch_subscribe<G>(
    compute_state: &mut crate::compute_state::ComputeState,
    sink: &ComputeSinkDesc<CollectionMetadata>,
    sink_id: GlobalId,
    as_of: Antichain<Timestamp>,
    sinked_collection: VecCollection<G, Row, Diff>,
    err_collection: VecCollection<G, DataflowError, Diff>,
    output_probe: &Handle<Timestamp>,
    config: &SubscribePersistBatchesConfig,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = sinked_collection.scope();
    let persist_clients = Arc::clone(&compute_state.persist_clients);
    let subscribe_response_buffer = Rc::clone(&compute_state.subscribe_response_buffer);
    let shard_id = config.shard_id;
    let persist_location = config.persist_location.clone();
    let relation_desc = config.relation_desc.clone();
    let with_snapshot = sink.with_snapshot;
    let up_to = sink.up_to.clone();

    let sinked_collection = sinked_collection
        .inner
        .probe_notify_with(vec![output_probe.clone()])
        .as_collection();

    let name = format!("subscribe-batch-{}", sink_id);
    let mut op = AsyncOperatorBuilder::new(name, scope);

    let mut ok_input = op.new_disconnected_input(&sinked_collection.inner, Pipeline);
    let mut err_input = op.new_disconnected_input(&err_collection.inner, Pipeline);

    let button = op.build(move |_capabilities| async move {
        // Create protocol for batch output
        let mut protocol = BatchSubscribeProtocol::new(
            sink_id,
            as_of.clone(),
            subscribe_response_buffer,
            shard_id,
            persist_location,
            relation_desc,
            persist_clients,
        )
        .await;

        let mut rows_to_emit: Vec<(Timestamp, Row, Diff)> = Vec::new();
        let mut errors_to_emit: Vec<(Timestamp, DataflowError, Diff)> = Vec::new();
        let mut finished = false;

        let should_emit = |time: &Timestamp| {
            let beyond_as_of = if with_snapshot {
                as_of.less_equal(time)
            } else {
                as_of.less_than(time)
            };
            let before_up_to = !up_to.less_equal(time);
            beyond_as_of && before_up_to
        };

        loop {
            let maybe_batch = tokio::select! {
                Some(event) = ok_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            for (row, time, diff) in data {
                                if should_emit(&time) {
                                    rows_to_emit.push((time, row, diff));
                                }
                            }
                            None
                        }
                        Event::Progress(frontier) => {
                            protocol.maybe_send_batch(
                                frontier,
                                &mut rows_to_emit,
                                &mut errors_to_emit,
                                &up_to,
                            ).await
                        }
                    }
                }
                Some(event) = err_input.next() => {
                    match event {
                        Event::Data(_cap, data) => {
                            for (error, time, diff) in data {
                                if should_emit(&time) {
                                    errors_to_emit.push((time, error, diff));
                                }
                            }
                            None
                        }
                        Event::Progress(frontier) => {
                            protocol.maybe_send_batch(
                                frontier,
                                &mut rows_to_emit,
                                &mut errors_to_emit,
                                &up_to,
                            ).await
                        }
                    }
                }
                // All inputs are exhausted, so we can shut down.
                else => {
                    finished = true;
                    None
                }
            };

            if let Some(()) = maybe_batch {
                // Batch was sent
            }

            if finished {
                // Send final batch at empty frontier if we haven't already
                protocol
                    .send_final_batch(&mut rows_to_emit, &mut errors_to_emit)
                    .await;
                break;
            }
        }
    });

    Some(Rc::new(button.press_on_drop()))
}

/// Protocol for sending subscribe responses as persist batches.
struct BatchSubscribeProtocol {
    sink_id: GlobalId,
    sink_as_of: Antichain<Timestamp>,
    subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    prev_upper: Antichain<Timestamp>,
    poison: Option<String>,
    shard_id: ShardId,
    persist_writer: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
}

impl BatchSubscribeProtocol {
    async fn new(
        sink_id: GlobalId,
        sink_as_of: Antichain<Timestamp>,
        subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
        shard_id: ShardId,
        persist_location: mz_persist_types::PersistLocation,
        relation_desc: mz_repr::RelationDesc,
        persist_clients: Arc<PersistClientCache>,
    ) -> Self {
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("error opening persist client");

        let persist_writer = persist_client
            .open_writer(
                shard_id,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: sink_id.to_string(),
                    handle_purpose: format!("subscribe sink {sink_id}"),
                },
            )
            .await
            .expect("error opening persist writer");

        Self {
            sink_id,
            sink_as_of,
            subscribe_response_buffer,
            prev_upper: Antichain::from_elem(Timestamp::minimum()),
            poison: None,
            shard_id,
            persist_writer,
        }
    }

    /// Attempt to send a batch if the frontier has advanced enough.
    async fn maybe_send_batch(
        &mut self,
        frontier: Antichain<Timestamp>,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
        up_to: &Antichain<Timestamp>,
    ) -> Option<()> {
        // Compute the overall frontier from both inputs
        let upper = frontier;

        // Only send a batch if both conditions hold:
        //  a) `upper` has reached or passed the sink's `as_of` frontier.
        //  b) `upper` is different from when we last sent a batch.
        if !PartialOrder::less_equal(&self.sink_as_of, &upper) || upper == self.prev_upper {
            return None;
        }

        self.send_batch_impl(upper.clone(), rows, errors).await;

        // Check if we've finished (up_to reached)
        if PartialOrder::less_equal(up_to, &upper) {
            // Send final empty batch at empty frontier
            self.send_batch_impl(Antichain::default(), &mut Vec::new(), &mut Vec::new())
                .await;
        }

        Some(())
    }

    /// Send the final batch when the operator is shutting down.
    async fn send_final_batch(
        &mut self,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
    ) {
        // Only send if we haven't already sent an empty-frontier batch
        if !self.prev_upper.is_empty() {
            self.send_batch_impl(Antichain::default(), rows, errors)
                .await;
        }
    }

    async fn send_batch_impl(
        &mut self,
        upper: Antichain<Timestamp>,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
    ) {
        // Consolidate updates
        consolidate_updates(rows);
        consolidate_updates(errors);

        // Partition into keep vs ship based on upper
        let (keep_rows, ship_rows): (Vec<_>, Vec<_>) =
            rows.drain(..).partition(|u| upper.less_equal(&u.0));
        let (keep_errors, ship_errors): (Vec<_>, Vec<_>) =
            errors.drain(..).partition(|u| upper.less_equal(&u.0));
        *rows = keep_rows;
        *errors = keep_errors;

        // Handle errors/poison
        let batches_result = match (&self.poison, ship_errors.first()) {
            (Some(error), _) => {
                // The subscribe is poisoned; keep sending the same error.
                Err(error.clone())
            }
            (None, Some((_, error, _))) => {
                // The subscribe encountered its first error; poison it.
                let error = error.to_string();
                self.poison = Some(error.clone());
                Err(error)
            }
            (None, None) => {
                // No error; write batch to persist
                match self.write_batch_to_persist(&ship_rows).await {
                    Ok(batches) => Ok(batches),
                    Err(e) => {
                        let error = e.to_string();
                        self.poison = Some(error.clone());
                        Err(error)
                    }
                }
            }
        };

        self.subscribe_response_buffer.borrow_mut().push((
            self.sink_id,
            SubscribeResponse::StashedBatch(StashedSubscribeBatch {
                lower: self.prev_upper.clone(),
                upper: upper.clone(),
                shard_id: self.shard_id,
                batches: batches_result,
            }),
        ));

        self.prev_upper = upper;
    }

    /// Write rows to persist and return the batch references.
    async fn write_batch_to_persist(
        &mut self,
        rows: &[(Timestamp, Row, Diff)],
    ) -> Result<Vec<ProtoBatch>, String> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Convert rows to the format expected by persist
        let updates = rows
            .iter()
            .map(|(time, row, diff)| ((SourceData(Ok(row.clone())), ()), *time, diff.into_inner()));

        let lower = self.prev_upper.clone();
        let upper = rows
            .iter()
            .map(|(t, _, _)| *t)
            .max()
            .map(|t| Antichain::from_elem(t.step_forward()))
            .unwrap_or_else(|| lower.clone());

        let batch = self
            .persist_writer
            .batch(updates, lower, upper)
            .await
            .map_err(|e| e.to_string())?;

        Ok(vec![batch.into_transmittable_batch()])
    }
}

impl Drop for BatchSubscribeProtocol {
    fn drop(&mut self) {
        // Send DroppedAt if we haven't finished cleanly
        if !self.prev_upper.is_empty() {
            self.subscribe_response_buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }
    }
}
