// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC server that serves information from the catalog to distributed components of Materialize.

use std::time::Duration;

use dataflow_types::DecodeError;
use persist::indexed::{ListenEvent, Snapshot};
use repr::Row;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use ingest_model::materialize_ingest::ingest_data_server::IngestData;
use ingest_model::materialize_ingest::read_source_response::Update;
use ingest_model::materialize_ingest::{
    ReadSourceRequest, ReadSourceResponse, SourceRecords, SourceSealed,
};
use persist::indexed::runtime::RuntimeClient;
use persist_types::Codec;

#[derive(Debug, Clone)]
pub struct Config {
    pub persister: RuntimeClient,
}

/// A [`IngestData`] backed by a persistence runtime.
#[derive(Debug)]
pub struct PersistenceIngestData {
    persister: RuntimeClient,
}

impl PersistenceIngestData {
    pub fn new(config: Config) -> PersistenceIngestData {
        PersistenceIngestData {
            persister: config.persister,
        }
    }
}

#[tonic::async_trait]
impl IngestData for PersistenceIngestData {
    type ReadSourceStream = ReceiverStream<Result<ReadSourceResponse, Status>>;

    async fn read_source(
        &self,
        request: Request<ReadSourceRequest>,
    ) -> Result<Response<Self::ReadSourceStream>, Status> {
        let persister = self.persister.clone();
        let (tx, rx) = mpsc::channel(4);

        let stream_name = request.get_ref().stream_name.clone();

        let (_write, read) = persister
            .create_or_load::<Result<Row, DecodeError>, Result<Row, DecodeError>>(&stream_name);

        let (listen_tx, listen_rx) = crossbeam_channel::unbounded();

        // TODO: There is no "un-listen".
        let snapshot = read
            .listen(listen_tx)
            .map_err(|e| Status::internal(format!("Listening on stream: {}", e)))?;

        tokio::spawn(async move {
            for record in snapshot.into_iter() {
                match record {
                    Ok(record) => {
                        let encoded_row = bincode::serialize(&record).unwrap();
                        let source_records = SourceRecords {
                            bincode_encoded_rows: vec![encoded_row],
                        };
                        let response = ReadSourceResponse {
                            update: Some(Update::Records(source_records)),
                        };
                        if let Err(e) = tx.send(Ok(response)).await {
                            log::info!("Client disconnected: {}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        if let Err(e) = tx
                            .send(Err(Status::internal(format!(
                                "Error reading from snapshot: {}",
                                e
                            ))))
                            .await
                        {
                            log::info!("Client disconnected: {}", e);
                            return;
                        }
                    }
                }
            }

            loop {
                let listen_result = listen_rx.try_recv();
                match listen_result {
                    Ok(e) => match e {
                        ListenEvent::Records(records) => {
                            let bincode_encoded_rows = records
                                .iter()
                                .map(|((key, value), ts, diff)| {
                                    let decoded_key =
                                        Result::<Row, DecodeError>::decode(&key).unwrap();
                                    let decoded_value =
                                        Result::<Row, DecodeError>::decode(&value).unwrap();
                                    ((decoded_key, decoded_value), ts, diff)
                                })
                                .map(|record| bincode::serialize(&record))
                                .collect::<Result<Vec<_>, _>>()
                                .unwrap();
                            let source_records = SourceRecords {
                                bincode_encoded_rows,
                            };
                            let response = ReadSourceResponse {
                                update: Some(Update::Records(source_records)),
                            };
                            if let Err(e) = tx.send(Ok(response)).await {
                                log::info!("Client disconnected: {}", e);
                                return;
                            }
                        }
                        ListenEvent::Sealed(ts) => {
                            let response = ReadSourceResponse {
                                update: Some(Update::Sealed(SourceSealed { timestamp: ts })),
                            };
                            if let Err(e) = tx.send(Ok(response)).await {
                                log::info!("Client disconnected: {}", e);
                                return;
                            }
                        }
                    },
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        // TODO: Maybe we can be smarter than just waiting. Even though it is async
                        // waiting...
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
