// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::warn;
use tracing::{info, trace};

use mz_dataflow_types::sources::persistence::SourcePersistDesc;
use mz_dataflow_types::sources::SourceDesc;
use mz_expr::GlobalId;
use mz_ingest_model::materialize_ingest::ingest_control_client::IngestControlClient;
use mz_ingest_model::materialize_ingest::ListRequest;

/// Serves the ingester based on the provided configuration.
pub async fn serve(config: Config) -> Result<Handle, anyhow::Error> {
    let mut ingester = Ingester::new(config);

    let mut update_interval = tokio::time::interval(ingester.config.update_interval);

    let (drain_trigger, mut drain_tripwire) = oneshot::channel();
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = update_interval.tick() => {
                    let result = ingester.tick().await;
                    if let Err(e) = result {
                        warn!("{}", e);
                    }
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
    pub coord_grpc_addr: String,
    /// The frequency at which we query new sources from the control plane.
    pub update_interval: Duration,
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
    async fn tick(&mut self) -> Result<(), String> {
        trace!("Updating list of sources from ingest control...");
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
            let (id, source_desc, persist_desc): (GlobalId, SourceDesc, SourcePersistDesc) =
                serde_json::from_slice(&encoded_source.json_encoded_source)
                    .map_err(|e| format!("Decoding source: {}", e))?;

            let current_value = self.active_sources.insert(id, source_desc.clone());

            if current_value.is_none() {
                info!("New source: {:?}, persist: {:?}", source_desc, persist_desc);
            }
        }

        Ok(())
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
