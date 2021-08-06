// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};

use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{Collection, Hashable};

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, ChangeBatch};
use timely::scheduling::Activator;
use timely::PartialOrder;

use dataflow_types::*;
use expr::GlobalId;
use interchange::envelopes::{combine_at_timestamp, dbz_format, upsert_format};
use repr::{Datum, Diff, RelationDesc, Row, Timestamp};

use crate::render::context::ArrangementFlavor;
use crate::render::context::Context;
use crate::render::{RelevantTokens, RenderState};
use crate::sink::cdcv2::kafka::KafkaCdcV2SinkWriter;
use crate::sink::cdcv2::CdcV2SinkWrite;
use crate::sink::SinkBaseMetrics;

impl<G> Context<G, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        render_state: &mut RenderState,
        worker_id: usize,
        tokens: &mut RelevantTokens,
        import_ids: HashSet<GlobalId>,
        sink_id: GlobalId,
        sink: &SinkDesc,
        metrics: &SinkBaseMetrics,
    ) {
        // put together tokens that belong to the export
        let mut needed_source_tokens = Vec::new();
        let mut needed_additional_tokens = Vec::new();
        let mut needed_sink_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(addls) = tokens.additional_tokens.get(&import_id) {
                needed_additional_tokens.extend_from_slice(addls);
            }
            if let Some(source_token) = tokens.source_tokens.get(&import_id) {
                needed_source_tokens.push(source_token.clone());
            }
        }

        let sink_token = match sink.envelope {
            Some(SinkEnvelope::CdcV2) => match sink.connector {
                SinkConnector::Kafka(ref k) => {
                    let sink_name = format!("kafka-cdcv2-{}", sink_id);

                    let kafka_sink = KafkaCdcV2SinkWriter::new(
                        k.clone(),
                        sink_name,
                        &sink_id,
                        worker_id.to_string(),
                        &metrics.kafka,
                    );

                    self.render_cdcv2_sink(render_state, sink_id, sink, metrics, kafka_sink)
                }
                _ => unimplemented!(
                    "CDCv2 envelope not implemented for {}",
                    sink.connector.name()
                ),
            },

            _ => self.render_classic_sink(render_state, sink_id, sink, metrics),
        };

        if let Some(sink_token) = sink_token {
            needed_sink_tokens.push(sink_token);
        }

        let tokens = Rc::new((
            needed_sink_tokens,
            needed_source_tokens,
            needed_additional_tokens,
        ));
        render_state
            .dataflow_tokens
            .insert(sink_id, Box::new(tokens));
    }

    fn render_classic_sink(
        &mut self,
        render_state: &mut RenderState,
        sink_id: GlobalId,
        sink: &SinkDesc,
        metrics: &SinkBaseMetrics,
    ) -> Option<Box<dyn Any>> {
        let sink_render = get_sink_render_for(&sink.connector);

        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.
        let (collection, _err_collection) = self
            .lookup_id(expr::Id::Global(sink.from))
            .expect("Sink source collection not loaded")
            .as_collection();

        let collection = apply_sink_envelope(sink, &sink_render, collection);

        let sink_token =
            sink_render.render_continuous_sink(render_state, sink, sink_id, collection, metrics);

        sink_token
    }

    fn render_cdcv2_sink<Sw>(
        &mut self,
        _render_state: &mut RenderState,
        sink_id: GlobalId,
        sink_desc: &SinkDesc,
        _metrics: &SinkBaseMetrics,
        sink: Sw,
    ) -> Option<Box<dyn Any>>
    where
        Sw: CdcV2SinkWrite + 'static,
    {
        let input = self
            .lookup_id(expr::Id::Global(sink_desc.from))
            .expect("Sink import not loaded");

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let sink_rc = Rc::new(RefCell::new(sink));

        // Set a number of tuples after which the operator should yield.
        // This allows us to remain responsive even when enumerating a substantial
        // arrangement, as well as provides time to accumulate our produced output.
        let refuel = 1_000_000;

        let updates_sink_rc = Rc::downgrade(&sink_rc);
        let updates_sink_shutdown_flag = Arc::clone(&shutdown_flag);

        // TODO(aljoscha): we need to report errors back to the coordinator,
        // to mark the sink as failed.
        let (timestamp_updates, _errs) = if let Some(flavor) = input.arranged.values().next() {
            match flavor {
                ArrangementFlavor::Local(oks, errs) => {
                    let timestamp_updates = self.render_cdcv2_updates_operator(
                        oks,
                        sink_id,
                        updates_sink_rc,
                        updates_sink_shutdown_flag,
                        refuel,
                    );
                    let errs = errs.as_collection(|k, _v| k.clone());
                    (timestamp_updates, errs)
                }
                ArrangementFlavor::Trace(_, oks, errs) => {
                    let timestamp_updates = self.render_cdcv2_updates_operator(
                        oks,
                        sink_id,
                        updates_sink_rc,
                        updates_sink_shutdown_flag,
                        refuel,
                    );
                    let errs = errs.as_collection(|k, _v| k.clone());
                    (timestamp_updates, errs)
                }
            }
        } else {
            // need to create our own arrangement first
            let arrange_name = format!("CdcV2SinkArrange(sink = {})", sink_id);

            let (oks, errs) = input.as_collection();
            use crate::arrangement::manager::RowSpine as DefaultValTrace;
            // TODO(aljoscha): cloning the row here doesn't seem super smart.😅
            let oks = oks
                .map(|row| (row.clone(), row))
                .arrange_named::<DefaultValTrace<Row, Row, _, _>>(&arrange_name);

            let timestamp_updates = self.render_cdcv2_updates_operator(
                &oks,
                sink_id,
                updates_sink_rc,
                updates_sink_shutdown_flag,
                refuel,
            );
            (timestamp_updates, errs)
        };

        let progress_sink_rc = Rc::downgrade(&sink_rc);
        self.render_cdcv2_progress_operator(sink_id, progress_sink_rc, timestamp_updates);

        Some(Box::new(SinkToken {
            shutdown_flag,
            _sink_rc: sink_rc,
        }))
    }

    fn render_cdcv2_updates_operator<'a, Tr, Sw>(
        &mut self,
        trace: &Arranged<G, Tr>,
        sink_id: GlobalId,
        sink_rc: Weak<RefCell<Sw>>,
        shutdown_flag: Arc<AtomicBool>,
        refuel: usize,
    ) -> Stream<G, (Timestamp, i64)>
    where
        Tr: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = repr::Diff>
            + Clone
            + 'static,
        Tr::Batch: BatchReader<Row, Tr::Val, G::Timestamp, repr::Diff> + 'static,
        Tr::Cursor: Cursor<Row, Tr::Val, G::Timestamp, repr::Diff> + 'static,
        Sw: CdcV2SinkWrite + 'static,
    {
        let name = format!("CdcV2UpdatesOperator(sink = {})", sink_id);

        trace.stream.unary(Pipeline, &name, move |_, info| {
            // Acquire an activator to reschedule the operator when it has unfinished work.
            let activations = trace.stream.scope().activations();
            let activator = Activator::new(&info.address[..], activations);
            // Maintain a list of work to do, cursor to navigate and process.
            let mut todo = std::collections::VecDeque::new();
            move |input, output| {
                if shutdown_flag.load(Ordering::SeqCst) {
                    info!("should be shutting down cdcv2 sink: {}", sink_id);
                    return;
                }

                // First, dequeue all batches.
                input.for_each(|time, data| {
                    let capability = time.retain();
                    for batch in data.iter() {
                        // enqueue a capability, cursor, and batch.
                        todo.push_back(PendingSinkWork::new(
                            capability.clone(),
                            batch.cursor(),
                            batch.clone(),
                        ));
                    }
                });

                // Second, make progress on `todo`, if we still have a sink.
                if let Some(sink) = sink_rc.upgrade() {
                    let mut sink = sink.borrow_mut();

                    let mut timestamps = ChangeBatch::new();

                    let mut fuel = refuel;
                    while !todo.is_empty() && fuel > 0 {
                        match todo.front_mut().unwrap().do_work(
                            &mut *sink,
                            &mut timestamps,
                            &mut fuel,
                        ) {
                            Ok(Some(backoff)) => {
                                debug!("Sink {} wants us to back off, obliging.", sink_id);
                                activator.activate_after(backoff);
                                return;
                            }
                            Err(e) => panic!("Error writing to sink: {}", e),
                            _ => (),
                        }
                        if fuel > 0 {
                            let batch = todo.pop_front().expect("known to exist");
                            let mut session = output.session(&batch.capability);
                            session.give_iterator(timestamps.drain())
                        }
                    }
                } else {
                    // We have been terminated, but may still receive indefinite data.
                    // Just ignore
                    info!("should be shutting down cdcv2 sink: {}", sink_id);
                }

                // If we have not finished all work, re-activate the operator.
                if !todo.is_empty() {
                    activator.activate();
                }
            }
        })
    }

    fn render_cdcv2_progress_operator<S, Sw>(
        &mut self,
        sink_id: GlobalId,
        sink_rc: Weak<RefCell<Sw>>,
        timestamp_updates: Stream<S, (Timestamp, i64)>,
    ) where
        Sw: CdcV2SinkWrite + 'static,
        S: Scope<Timestamp = Timestamp>,
    {
        let name = format!("CdcV2ProgressOperator(sink = {})", sink_id);

        // We use a lower-level builder here to get access to the operator address, for rescheduling.
        let mut builder = OperatorBuilder::new(name, timestamp_updates.scope());
        let reactivator = timestamp_updates
            .scope()
            .activator_for(&builder.operator_info().address);
        let sink_hash = sink_id.hashed();
        let mut input = builder.new_input(&timestamp_updates, Exchange::new(move |_| sink_hash));
        let should_write = timestamp_updates.scope().index()
            == (sink_hash as usize) % timestamp_updates.scope().peers();

        // We now record the numbers of updates at each timestamp between lower and upper bounds.
        // Track the advancing frontier, to know when to produce utterances.
        let mut frontier =
            Antichain::from_elem(<Timestamp as timely::progress::Timestamp>::minimum());
        // Track accumulated counts for timestamps.
        let mut timestamps = ChangeBatch::new();
        // Stash for serialized data yet to send.
        let mut send_queue = std::collections::VecDeque::new();
        let mut retain = Vec::new();

        builder.build_reschedule(|_capabilities| {
            move |frontiers| {
                let mut input = FrontieredInputHandle::new(&mut input, &frontiers[0]);

                // We want to drain inputs no matter what.
                // We could do this after the next step, as we are certain these timestamps will
                // not be part of a closed frontier (as they have not yet been read). This has the
                // potential to make things speedier as we scan less and keep a smaller footprint.
                input.for_each(|_capability, counts| {
                    timestamps.extend(counts.iter().cloned());
                });

                if should_write {
                    if let Some(sink) = sink_rc.upgrade() {
                        let mut sink = sink.borrow_mut();

                        // If our frontier advances strictly, we have the opportunity to issue a progress statement.
                        if <_ as PartialOrder>::less_than(
                            &frontier.borrow(),
                            &input.frontier.frontier(),
                        ) {
                            let new_frontier = input.frontier.frontier();

                            // Extract the timestamp counts to announce.
                            let mut announce = Vec::new();
                            for (time, count) in timestamps.drain() {
                                if !new_frontier.less_equal(&time) {
                                    announce.push((time, count));
                                } else {
                                    retain.push((time, count));
                                }
                            }
                            timestamps.extend(retain.drain(..));

                            // Announce the lower bound, upper bound, and timestamp counts.
                            let lower = frontier.elements().to_vec();
                            let upper = new_frontier.to_vec();
                            let counts = announce;
                            send_queue.push_back((lower, upper, counts));

                            // Advance our frontier to track our progress utterance.
                            frontier = input.frontier.frontier().to_owned();

                            while let Some(message) = send_queue.front() {
                                match sink.write_progress(&message.0, &message.1, &message.2) {
                                    Err(e) => panic!("Sink {} errored: {:?}", sink_id, e),
                                    Ok(result) => {
                                        if let Some(duration) = result {
                                            // Reschedule after `duration` and then bail.
                                            reactivator.activate_after(duration);
                                            // Signal that work remains to be done.
                                            return true;
                                        } else {
                                            send_queue.pop_front();
                                        }
                                    }
                                }
                            }
                        }

                        match sink.errors() {
                            Some(errors) => panic!("Sink {} errored: {:?}", sink_id, errors),
                            _ => (),
                        }

                        // Signal incompleteness if messages remain to be sent.
                        !sink.done() || !send_queue.is_empty()
                    } else {
                        timestamps.clear();
                        send_queue.clear();
                        // Signal that there are no outstanding writes.
                        false
                    }
                } else {
                    false
                }
            }
        });
    }
}

pub struct PendingSinkWork<K, C: Cursor<K, Row, Timestamp, Diff>> {
    capability: Capability<Timestamp>,
    cursor: C,
    batch: C::Storage,
    current_time: Option<Timestamp>,
}

impl<K: PartialEq, C: Cursor<K, Row, Timestamp, Diff>> PendingSinkWork<K, C> {
    /// Create a new bundle of pending work, from the capability, cursor, and backing storage.
    fn new(capability: Capability<Timestamp>, cursor: C, batch: C::Storage) -> Self {
        Self {
            capability,
            cursor,
            batch,
            current_time: None,
        }
    }

    /// Perform roughly `fuel` work through the cursor, sending updates to the given `sink`.
    ///
    /// If the sink wants us to back off or returns an `Err`, we exit early.
    fn do_work<Sw>(
        &mut self,
        sink: &mut Sw,
        timestamps: &mut ChangeBatch<u64>,
        fuel: &mut usize,
    ) -> Result<Option<Duration>, anyhow::Error>
    where
        Sw: CdcV2SinkWrite + 'static,
    {
        // Attempt to make progress on this batch.
        let mut work: usize = 0;
        while let Some(_key) = self.cursor.get_key(&self.batch) {
            while let Some(val) = self.cursor.get_val(&self.batch) {
                let mut result = None;

                // TODO(aljoscha): This current_time business is working around
                // the fact that we cannot step the cursor by time. Maybe
                // we should change that. We could a) just accept this, b)
                // send out whole batches per value, 3) change Cursor to allow
                // stepping by time.
                let mut current_time = self.current_time.take();
                self.cursor.map_times(&self.batch, |time, diff| {
                    if let Some(current_time) = current_time.as_ref() {
                        if time < current_time {
                            return;
                        }
                    }
                    match sink.write_updates(&[(val, time, diff)]) {
                        Ok(Some(duration)) => {
                            current_time.insert(time.clone());
                            result.insert(Ok(Some(duration)));
                        }
                        Err(e) => {
                            current_time.insert(time.clone());
                            result.insert(Err(e));
                        }
                        Ok(_) => (),
                    }
                    timestamps.update(time.clone(), 1);
                    work += 1;
                });
                if let Some(result) = result {
                    return result;
                }

                self.cursor.step_val(&self.batch);
                if work >= *fuel {
                    *fuel = 0;
                    return Ok(None);
                }
            }
            self.cursor.step_key(&self.batch);
        }
        *fuel -= work;

        Ok(None)
    }
}

struct SinkToken<S> {
    shutdown_flag: Arc<AtomicBool>,
    // this is only here to be dropped when the SinkToken is dropped
    _sink_rc: Rc<RefCell<S>>,
}

impl<S> Drop for SinkToken<S> {
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
    }
}

fn apply_sink_envelope<G>(
    sink: &SinkDesc,
    sink_render: &Box<dyn SinkRender<G>>,
    collection: Collection<G, Row, Diff>,
) -> Collection<G, (Option<Row>, Option<Row>), Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    // Some connectors support keys - extract them.
    let keyed = if sink_render.uses_keys() {
        let user_key_indices = sink_render
            .get_key_indices()
            .map(|key_indices| key_indices.to_vec());

        let relation_key_indices = sink_render
            .get_relation_key_indices()
            .map(|key_indices| key_indices.to_vec());

        // We have three cases here, in descending priority:
        //
        // 1. if there is a user-specified key, use that to consolidate and
        //  distribute work
        // 2. if the sinked relation has a known primary key, use that to
        //  consolidate and distribute work but don't write to the sink
        // 3. if none of the above, use the whole row as key to
        //  consolidate and distribute work but don't write to the sink

        let keyed = if user_key_indices.is_some() {
            let key_indices = user_key_indices.expect("known to exist");
            collection.map(move |row| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                // Does it matter?
                let datums = row.unpack();
                let key = Row::pack(key_indices.iter().map(|&idx| datums[idx].clone()));
                (Some(key), row)
            })
        } else if relation_key_indices.is_some() {
            let relation_key_indices = relation_key_indices.expect("known to exist");
            collection.map(move |row| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                // Does it matter?
                let datums = row.unpack();
                let key = Row::pack(relation_key_indices.iter().map(|&idx| datums[idx].clone()));
                (Some(key), row)
            })
        } else {
            collection.map(|row| {
                (
                    Some(Row::pack(Some(Datum::Int64(row.hashed() as i64)))),
                    row,
                )
            })
        };
        keyed
    } else {
        collection.map(|row| (None, row))
    };

    // Apply the envelope.
    // * "Debezium" consolidates the stream, sorts it by time, and produces DiffPairs from it.
    //   It then renders those as Avro.
    // * Upsert" does the same, except at the last step, it renders the diff pair in upsert format.
    //   (As part of doing so, it asserts that there are not multiple conflicting values at the same timestamp)
    // * "Tail" writes some metadata.
    let collection = match sink.envelope {
        Some(SinkEnvelope::Debezium) => {
            let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

            // if there is no user-specified key, remove the synthetic
            // distribution key again
            let user_key_indices = sink_render.get_key_indices();
            let combined = if user_key_indices.is_some() {
                combined
            } else {
                combined.map(|(_key, value)| (None, value))
            };

            // This has to be an `Rc<RefCell<...>>` because the inner closure (passed to `Iterator::map`) references it, and it might outlive the outer closure.
            let rp = Rc::new(RefCell::new(Row::default()));
            let collection = combined.flat_map(move |(mut k, v)| {
                let max_idx = v.len() - 1;
                let rp = rp.clone();
                v.into_iter().enumerate().map(move |(idx, dp)| {
                    let k = if idx == max_idx { k.take() } else { k.clone() };
                    (k, Some(dbz_format(&mut *rp.borrow_mut(), dp)))
                })
            });
            collection
        }
        Some(SinkEnvelope::Upsert) => {
            let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

            let collection = combined.map(|(k, v)| {
                let v = upsert_format(v);
                (k, v)
            });
            collection
        }
        Some(SinkEnvelope::CdcV2) => unreachable!("CDCv2 sinks are not rendered as classic sinks"),
        // No envelope, this can only happen for TAIL sinks, which work
        // on vanilla rows.
        None => keyed.map(|(key, value)| (key, Some(value))),
    };

    collection
}

pub trait SinkRender<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn uses_keys(&self) -> bool;

    fn get_key_desc(&self) -> Option<&RelationDesc>;

    fn get_key_indices(&self) -> Option<&[usize]>;

    fn get_relation_key_indices(&self) -> Option<&[usize]>;

    fn get_value_desc(&self) -> &RelationDesc;

    fn render_continuous_sink(
        &self,
        render_state: &mut RenderState,
        sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        metrics: &SinkBaseMetrics,
    ) -> Option<Box<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>;
}

fn get_sink_render_for<G>(connector: &SinkConnector) -> Box<dyn SinkRender<G>>
where
    G: Scope<Timestamp = Timestamp>,
{
    match connector {
        SinkConnector::Kafka(connector) => Box::new(connector.clone()),
        SinkConnector::AvroOcf(connector) => Box::new(connector.clone()),
        SinkConnector::Tail(connector) => Box::new(connector.clone()),
    }
}
