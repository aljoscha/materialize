// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an ingest service via gRPC.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::{CapabilitySet, Concat, Map, OkErr};
use timely::dataflow::{Scope, Stream};

use futures_util::Stream as FuturesStream;

use crate::render::RenderState;
use crate::source::PartitionMetrics;
use crate::source::SourceToken;
use dataflow_types::{
    DataflowError, DecodeError, GrpcIngestSourceConnector, SourceError, SourceErrorDetails,
};
use ingest_model::materialize_ingest::ingest_data_client::IngestDataClient;
use ingest_model::materialize_ingest::read_source_response::Update;
use ingest_model::materialize_ingest::SourceSealed;
use ingest_model::materialize_ingest::{ReadSourceRequest, SourceRecords};
use repr::{Diff, Row, Timestamp};

/// Creates a new source that reads from an ingest service via gRPC.
pub fn grpc_ingest_source<G>(
    _render_state: &mut RenderState,
    scope: &G,
    source_name: String,
    connector: GrpcIngestSourceConnector,
    metrics: PartitionMetrics,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let async_stream = async_stream::try_stream! {
        // We just loop forever. Eventually we might want to think about bubbling
        // up non-recoverable errors.
        'outer: loop {
            let mut client = match IngestDataClient::connect(connector.grpc_address.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    log::trace!("Creating client: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let read_request = ReadSourceRequest {
                stream_name: connector.collection_name.clone(),
            };

            let mut stream = match client.read_source(read_request).await {
                Ok(stream) => stream.into_inner(),
                Err(e) => {
                    log::trace!("Reading from source: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            loop {
                let message = match stream.message().await {
                    Ok(message) => message,
                    Err(e) => {
                        log::trace!("Reading from source stream: {}", e);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue 'outer;
                    }
                };
                match message {
                    Some(message) => yield message,
                    None => break 'outer,
                }
            }
        }
    };

    let mut pinned_stream = Box::pin(async_stream);

    // TODO: We have to use the MZ source() helper to create a source that can be dropped by
    // dropping the returned token.
    let timely_stream = source(scope, "AsyncGrpcSource", move |capability, info| {
        let activator = Arc::new(scope.sync_activator_for(&info.address[..]));

        let mut cap_set = CapabilitySet::from_elem(capability);
        let mut current_seal_ts = 0;

        move |output| {
            let waker = futures_util::task::waker_ref(&activator);
            let mut context = Context::from_waker(&waker);

            while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                match item {
                    Some(Ok(response)) => match response.update {
                        Some(Update::Records(SourceRecords {
                            bincode_encoded_rows,
                        })) => {
                            let cap = cap_set.delayed(&current_seal_ts);
                            let mut session = output.session(&cap);
                            for encoded_row in bincode_encoded_rows {
                                let ((key, value), ts, diff): (
                                    (Result<Row, DecodeError>, Result<Row, DecodeError>),
                                    Timestamp,
                                    Diff,
                                ) = bincode::deserialize(&encoded_row).unwrap();
                                if ts >= current_seal_ts {
                                    session.give(Ok(((key, value), ts, diff)));
                                }
                            }
                        }
                        Some(Update::Sealed(SourceSealed { timestamp })) => {
                            current_seal_ts = timestamp;
                            cap_set.downgrade(vec![current_seal_ts]);
                        }
                        _ => unreachable!(),
                    },
                    Some(Err(e)) => {
                        let cap = cap_set.delayed(&current_seal_ts);
                        let mut session = output.session(&cap);
                        session.give(Err((e, current_seal_ts, 1)));
                    }
                    None => {
                        cap_set.downgrade(&[]);
                        break;
                    }
                }
            }
        }
    });

    let (ok_stream, persist_err_stream) = timely_stream.ok_err(|x| x);
    let persist_err_stream = persist_err_stream.map(move |(err, ts, diff)| {
        (
            DataflowError::from(SourceError::new(
                source_name.clone(),
                SourceErrorDetails::Persistence(err),
            )),
            ts,
            diff,
        )
    });

    let mut row_packer = Row::default();
    let (ok_stream, err_stream) = ok_stream.ok_err(move |((key, value), ts, diff)| {
        let key = match key {
            Ok(key) => key,
            Err(e) => return Err((DataflowError::from(e), ts, diff)),
        };
        let value = match value {
            Ok(value) => value,
            Err(e) => return Err((DataflowError::from(e), ts, diff)),
        };
        let unpacked_key = key.unpack();
        let unpacked_value = value.unpack();
        row_packer.extend(unpacked_key);
        row_packer.extend(unpacked_value);
        metrics.messages_ingested.inc();
        Ok((row_packer.finish_and_reuse(), ts, diff))
    });

    let err_stream = err_stream.concat(&persist_err_stream);

    (ok_stream, err_stream, None)
}
