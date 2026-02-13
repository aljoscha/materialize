// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A minimal pgwire server using the sunng87/pgwire crate, for benchmarking
//! against our own pgwire implementation. Hardcoded to respond to every query
//! as if it were `SELECT 1`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, stream};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Handler that responds to every query with a single row containing the
/// integer `1`, without parsing the query at all.
pub struct SelectOneHandler;

#[async_trait]
impl NoopStartupHandler for SelectOneHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for SelectOneHandler {
    async fn do_query<C>(&self, _client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = Arc::new(vec![FieldInfo::new(
            "?column?".into(),
            None,
            None,
            Type::INT4,
            FieldFormat::Text,
        )]);

        let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
        encoder.encode_field(&1i32)?;
        let row = encoder.take_row();

        let data_stream = stream::iter(vec![Ok(row)]);
        let response = Response::Query(QueryResponse::new(fields, data_stream));

        Ok(vec![response])
    }
}

/// Factory that hands out [`SelectOneHandler`] instances.
pub struct SelectOneHandlerFactory {
    handler: Arc<SelectOneHandler>,
}

impl SelectOneHandlerFactory {
    pub fn new() -> Self {
        Self {
            handler: Arc::new(SelectOneHandler),
        }
    }
}

impl PgWireServerHandlers for SelectOneHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.handler.clone()
    }
}
