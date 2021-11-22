// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use coord::coord::Message as CoordMessage;
use coord::timestamp::TimestampMessage;
use dataflow_types::{DataflowDesc, SourceDesc};
use expr::GlobalId;
use ingest_model::materialize_ingest::ingest_control_client::IngestControlClient;
use ingest_model::materialize_ingest::ListRequest;

/// Serves the ingester based on the provided configuration.
pub async fn serve<DC>(
    config: Config,
    mut dataflow_client: DC,
    mut ts_command_rx: tokio::sync::mpsc::UnboundedReceiver<coord::coord::Message>,
) -> Result<Handle, anyhow::Error>
where
    DC: dataflow_types::client::Client + 'static,
{
    let mut ingester = Ingester::new(config);

    let mut update_interval = tokio::time::interval(ingester.config.update_interval);

    let (drain_trigger, mut drain_tripwire) = oneshot::channel();
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                // Messages from the timestamper thread
                Some(coord_msg) = ts_command_rx.recv() => {
                    match coord_msg {
                        CoordMessage::AdvanceSourceTimestamp(update) => {
                            let source_id = update.id;
                            let ts_update = update.update;
                            dataflow_client
                                .send(dataflow_types::client::Command::AdvanceSourceTimestamp {
                                    id: source_id,
                                    update: ts_update,
                                })
                                .await;
                        }
                        msg => { panic!("Unexpected coordinator message: {:?}", msg); }
                    }
                },
                // Ingester updates
                _ = update_interval.tick() => {
                    let result = ingester.tick(&mut dataflow_client).await;
                    if let Err(e) = result {
                        log::warn!("{}", e);
                    }
                },
                // Messages from the dataflow layer
                Some(response) = dataflow_client.recv() => {
                    ingester.handle_dataflow_message(response);
                },
                _ = &mut drain_tripwire => {
                    println!("Ingester got shutdown signal...");
                    break;
                }
            }
        }
    });

    let start_instant = Instant::now();
    let handle = Handle {
        start_instant,
        // _thread: thread.join_on_drop(),
        _drain_trigger: drain_trigger,
    };

    Ok(handle)
}

/// Configures a ingester.
#[derive(Debug, Clone)]
pub struct Config {
    /// The coordinator gRPC endpoint.
    pub coord_grpc_addr: SocketAddr,
    /// The frequency at which we query new sources from the control plane.
    pub update_interval: Duration,
    /// Channel to communicate source status updates to the timestamper thread.
    pub ts_tx: std::sync::mpsc::Sender<coord::timestamp::TimestampMessage>,
}

/// Ingester service.
pub struct Ingester {
    config: Config,
    active_sources: HashMap<GlobalId, SourceDesc>,
}

impl Ingester {
    fn new(config: Config) -> Self {
        Ingester {
            config,
            active_sources: HashMap::new(),
        }
    }

    async fn tick<DC>(&mut self, dataflow_client: &mut DC) -> Result<(), String>
    where
        DC: dataflow_types::client::Client,
    {
        log::trace!("Updating list of sources from ingest control...");
        let mut client =
            IngestControlClient::connect(format!("http://{}", self.config.coord_grpc_addr))
                .await
                .map_err(|e| format!("Connecting to ingest control plane: {}", e))?;

        let request = tonic::Request::new(ListRequest {
            shard_id: "".into(),
        });

        let response = client
            .list_sources(request)
            .await
            .map_err(|e| format!("Requesting list of sources to ingest: {}", e))?;

        let encoded_sources = &response.get_ref().sources;

        for encoded_source in encoded_sources.iter() {
            let (id, source_desc): (GlobalId, SourceDesc) =
                serde_json::from_slice(&encoded_source.json_encoded_source)
                    .map_err(|e| format!("Decoding source: {}", e))?;

            let current_value = self.active_sources.insert(id, source_desc.clone());

            if current_value.is_some() {
                continue;
            }

            log::info!("Rendering new source: {:#?}", source_desc);
            self.render_source(id, source_desc, dataflow_client).await?;
        }

        Ok(())
    }

    async fn render_source<DC>(
        &mut self,
        id: GlobalId,
        source_desc: SourceDesc,
        dataflow_client: &mut DC,
    ) -> Result<(), String>
    where
        DC: dataflow_types::client::Client,
    {
        self.config
            .ts_tx
            .send(TimestampMessage::Add(id, source_desc.connector.clone()))
            .map_err(|e| format!("Adding timestamping for source: {}", e))?;

        dataflow_client
            .send(dataflow_types::client::Command::AddSourceTimestamping {
                id,
                connector: source_desc.connector.clone(),
                bindings: Vec::new(), // we never "store" bindings on the "coordinator"
            })
            .await;

        let mut dataflow = DataflowDesc::new(format!("source-{}", id));
        dataflow.import_source(id, source_desc, id);

        transform::optimize_dataflow(&mut dataflow, &HashMap::new())
            .map_err(|e| format!("Error optimizing rendered source dataflow: {}", e))?;
        let finalized_dataflow = dataflow_types::Plan::finalize_dataflow(dataflow)
            .map_err(|_e| format!("Error finalizing rendered source dataflow"))?;

        dataflow_client
            .send(dataflow_types::client::Command::CreateDataflows(vec![
                finalized_dataflow,
            ]))
            .await;

        Ok(())
    }

    fn handle_dataflow_message(&self, dataflow_response: dataflow_types::client::Response) {
        match dataflow_response {
            dataflow_types::client::Response::FrontierUppers(_uppers) => {
                // not interested for now, or probably ever
            }
            msg => {
                log::info!("Got message from dataflow: {:?}", msg);
            }
        }
    }
}

/// A handle to a running ingester.
///
/// The ingester runs on its own thread. Dropping the handle will wait for
/// the ingester's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the ingester have dropped.
pub struct Handle {
    pub(crate) start_instant: Instant,
    pub(crate) _drain_trigger: oneshot::Sender<()>,
}

impl Handle {
    /// Returns the instant at which the ingester booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}
