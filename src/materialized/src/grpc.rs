// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC server that serves information from the catalog to distributed components of Materialize.

use tonic::{Request, Response, Status};
use tracing::trace;

use mz_coord::session::Session;
use mz_ingest_model::materialize_ingest::ingest_control_server::IngestControl;
use mz_ingest_model::materialize_ingest::{
    ListRequest, ListResponse, SourceDescription as ProtoSourceDescription,
};

const SYSTEM_USER: &str = "mz_system";

#[derive(Debug, Clone)]
pub struct Config {
    pub coord_client: mz_coord::Client,
}

#[derive(Debug)]
pub struct CoordIngestControl {
    coord_client: mz_coord::Client,
}

impl CoordIngestControl {
    pub fn new(config: Config) -> CoordIngestControl {
        CoordIngestControl {
            coord_client: config.coord_client,
        }
    }
}

#[tonic::async_trait]
impl IngestControl for CoordIngestControl {
    async fn list_sources(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        trace!("Request for persistent sources: {:?}", request);

        let coord_client = self
            .coord_client
            .new_conn()
            .map_err(|e| Status::internal(e.to_string()))?;

        let session = Session::new(coord_client.conn_id(), SYSTEM_USER.to_string());
        let (mut coord_client, _) = coord_client
            .startup(session, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let persistent_sources = coord_client
            .list_persistent_sources()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let encoded_sources: Vec<_> = persistent_sources
            .into_iter()
            .map(|source| {
                let encoded_source = serde_json::to_vec(&source).unwrap();

                ProtoSourceDescription {
                    json_encoded_source: encoded_source,
                }
            })
            .collect();

        let reply = ListResponse {
            sources: encoded_sources,
        };

        coord_client.terminate().await;
        Ok(Response::new(reply))
    }
}
