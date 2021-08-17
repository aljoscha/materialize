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
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Operator;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;

use dataflow_types::*;
use expr::GlobalId;
use interchange::envelopes::{combine_at_timestamp, dbz_format, upsert_format};
use repr::{Datum, Diff, RelationDesc, Row, Timestamp};

use crate::logging::errors::{ErrorEvent, ErrorLogger};
use crate::render::context::Context;
use crate::render::{RelevantTokens, RenderState};
use crate::sink::SinkBaseMetrics;

impl<'g, G> Context<Child<'g, G, G::Timestamp>, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        render_state: &mut RenderState,
        mut error_logging: Option<ErrorLogger>,
        tokens: &mut RelevantTokens,
        import_ids: HashSet<GlobalId>,
        sink_id: GlobalId,
        sink: &SinkDesc,
        metrics: &SinkBaseMetrics,
    ) {
        let sink_render = get_sink_render_for(&sink.connector);

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

        let (collection, err_collection) = self
            .lookup_id(expr::Id::Global(sink.from))
            .expect("Sink source collection not loaded")
            .as_collection();

        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.
        // TODO(aljoscha): What to do with the sink? Should we mark it as failed? Send an error out
        // through the sink? Should we mark the sink as "un-failed" if/when errors retract?
        let error_sink_id = sink_id.clone();
        let error_op_name = format!("ForwardSinkErrors(sink_id={}", error_sink_id);

        // TODO(aljoscha): factor this out into a common operator that is used both by the
        // index rendering and sink rendering code
        err_collection
            .inner
            .unary_frontier(Pipeline, &error_op_name, |_default_cap, _info| {
                let mut vector = Vec::new();
                let mut reported_errors: HashMap<DataflowError, Diff> = HashMap::new();
                move |input, _output: &mut OutputHandle<_, (), _>| {
                    // if we're being shut down, retract any outstanding
                    // errors to clear the error log from errors of this
                    // index
                    if input.frontier().frontier().is_empty() {
                        if let Some(error_logging) = error_logging.as_mut() {
                            for (error, diff) in reported_errors.drain() {
                                error_logging.log(ErrorEvent::SinkError(
                                    error_sink_id,
                                    error.clone(),
                                    -diff,
                                ));
                            }
                        }
                    }

                    while let Some((_time, data)) = input.next() {
                        data.swap(&mut vector);

                        for (error, _ts, diff) in vector.drain(..) {
                            if let Some(error_logging) = error_logging.as_mut() {
                                reported_errors
                                    .entry(error.clone())
                                    .and_modify(|existing_diff| *existing_diff += diff)
                                    .or_insert(1);
                                reported_errors.retain(|_error, diff| *diff > 0);
                                error_logging.log(ErrorEvent::SinkError(
                                    error_sink_id,
                                    error.clone(),
                                    diff,
                                ));
                            }
                        }
                    }
                }
            });

        let collection = apply_sink_envelope(sink, &sink_render, collection);

        let sink_token =
            sink_render.render_continuous_sink(render_state, sink, sink_id, collection, metrics);

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
}

fn apply_sink_envelope<'a, G>(
    sink: &SinkDesc,
    sink_render: &Box<dyn SinkRender<G>>,
    collection: Collection<Child<'a, G, G::Timestamp>, Row, Diff>,
) -> Collection<Child<'a, G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>
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
        sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
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
