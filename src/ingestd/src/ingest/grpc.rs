// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC server that serves information from the catalog to distributed components of Materialize.

// use std::pin::Pin;
// use std::sync::Arc;

// use futures::Stream;
// use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use ingest_model::materialize_ingest::ingest_data_server::IngestData;
use ingest_model::materialize_ingest::{ReadSourceRequest, ReadSourceResponse};

#[derive(Debug, Clone)]
pub struct Config {}

/// A [`IngestData`] backed by a persistence runtime.
#[derive(Debug)]
pub struct PersistenceIngestData {}

impl PersistenceIngestData {
    pub fn new(_config: Config) -> PersistenceIngestData {
        PersistenceIngestData {}
    }
}

#[tonic::async_trait]
impl IngestData for PersistenceIngestData {
    type ReadSourceStream = ReceiverStream<Result<ReadSourceResponse, Status>>;

    async fn read_source(
        &self,
        _request: Request<ReadSourceRequest>,
    ) -> Result<Response<Self::ReadSourceStream>, Status> {
        unimplemented!();
    }
}
