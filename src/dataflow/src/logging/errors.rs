// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for errors that surface in dataflows

use std::time::Duration;

use dataflow_types::logging::ErrorLog;
use dataflow_types::DataflowError;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::logging::WorkerIdentifier;

use super::LogVariant;
use crate::arrangement::KeysValsHandle;
use expr::GlobalId;
use repr::Diff;
use repr::{Datum, Row, Timestamp};

/// Type alias for logging of materialized events.
pub type ErrorLogger = timely::logging_core::Logger<ErrorEvent, WorkerIdentifier>;

/// A logged dataflow error.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum ErrorEvent {
    /// Errors that happen in index dataflows
    IndexError(GlobalId, DataflowError, Diff),
    /// Errors that happen in sink dataflows
    SinkError(GlobalId, DataflowError, Diff),
}

/// Constructs the logging dataflows and returns a logger and trace handles.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &dataflow_types::logging::LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, ErrorEvent)>>,
) -> std::collections::HashMap<LogVariant, (Vec<usize>, KeysValsHandle)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: dataflow error logging", move |scope| {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux =
            OperatorBuilder::new("Materialize Error Logging Demux".to_string(), scope.clone());
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut index_errors_out, index_errors) = demux.new_output();
        let (mut sink_errors_out, sink_errors) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            move |_frontiers| {
                let mut index_errors = index_errors_out.activate();
                let mut sink_errors = sink_errors_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut index_errors_session = index_errors.session(&time);
                    let mut sink_errors_session = sink_errors.session(&time);

                    for (time, _worker, error_event) in demux_buffer.drain(..) {
                        let time_ns = time.as_nanos() as Timestamp;
                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                        let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                        let time_ms = time_ms as Timestamp;

                        match error_event {
                            ErrorEvent::IndexError(id, error, diff) => {
                                index_errors_session.give((id, error, time_ms, diff));
                            }
                            ErrorEvent::SinkError(id, error, diff) => {
                                sink_errors_session.give((id, error, time_ms, diff));
                            }
                        }
                    }
                });
            }
        });

        let index_errors_current = index_errors
            .map(move |(index_id, error, time_ms, diff)| {
                ((index_id, error.to_string()), time_ms, diff)
            })
            .as_collection()
            .map({
                move |(index_id, error)| {
                    Row::pack_slice(&[Datum::String(&index_id.to_string()), Datum::String(&error)])
                }
            });

        let sink_errors_current = sink_errors
            .map(move |(sink_id, error, time_ms, diff)| {
                ((sink_id, error.to_string()), time_ms, diff)
            })
            .as_collection()
            .map({
                move |(sink_id, error)| {
                    Row::pack_slice(&[Datum::String(&sink_id.to_string()), Datum::String(&error)])
                }
            });

        let logs = vec![
            (LogVariant::Errors(ErrorLog::Index), index_errors_current),
            (LogVariant::Errors(ErrorLog::Sink), sink_errors_current),
        ];

        use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let key_clone = key.clone();
                let trace = collection
                    .map({
                        let mut row_packer = Row::default();
                        move |row| {
                            let datums = row.unpack();
                            row_packer.extend(key.iter().map(|k| datums[*k]));
                            (row_packer.finish_and_reuse(), row)
                        }
                    })
                    .arrange_by_key()
                    .trace;
                result.insert(variant, (key_clone, trace));
            }
        }
        result
    });

    traces
}
