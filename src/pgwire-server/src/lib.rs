// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A minimal pgwire/SQL server that uses the real mz_pgwire::Server from environmentd.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use mz_ore::task;
use mz_pgwire_common::ConnectionCounter;
use mz_server_core::{ConnectionStream, ListenerHandle, ReloadingTlsConfig, ServeConfig};

pub mod mock_adapter;

/// Configuration for a pgwire server listener.
#[derive(Debug, Clone)]
pub struct SqlListenerConfig {
    /// The IP address and port to listen for connections on.
    pub addr: SocketAddr,
    /// Whether to enable TLS.
    pub enable_tls: bool,
}

/// A listener for SQL connections.
pub struct SqlListener {
    pub handle: ListenerHandle,
    connection_stream: Pin<Box<dyn ConnectionStream>>,
    #[allow(dead_code)]
    config: SqlListenerConfig,
}

impl SqlListener {
    /// Binds a SQL listener to the specified address.
    pub async fn bind(config: SqlListenerConfig) -> Result<Self, io::Error> {
        let (handle, connection_stream) = mz_server_core::listen(&config.addr).await?;
        Ok(Self {
            handle,
            connection_stream,
            config,
        })
    }

    pub async fn serve_sql(
        self,
        name: String,
        tls_reloading_context: Option<ReloadingTlsConfig>,
        active_connection_counter: ConnectionCounter,
    ) -> ListenerHandle {
        let label: &'static str = Box::leak(name.into_boxed_str());
        let tls = tls_reloading_context;
        let active_connection_counter = active_connection_counter;

        task::spawn(|| format!("{}_sql_server", label), {
            let connection_stream = self.connection_stream;
            use crate::mock_adapter::MockAdapterService;
            use mz_ore::metrics::MetricsRegistry;

            // Create a metrics registry
            let metrics_registry = MetricsRegistry::new();

            // Create the mock adapter service with a REAL adapter client!
            let adapter_service = MockAdapterService::new(&metrics_registry).await;
            let adapter_client = adapter_service.client();

            let pgwire_metrics = mz_pgwire::MetricsConfig::register_into(&metrics_registry);
            let sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
                label,
                tls,
                adapter_client,
                authenticator: mz_authenticator::Authenticator::None,
                metrics: pgwire_metrics,
                active_connection_counter,
                helm_chart_version: None,
                allowed_roles: mz_server_core::listeners::AllowedRoles::NormalAndInternal,
            });

            tracing::info!("running pgwire server");
            mz_server_core::serve(ServeConfig {
                conns: connection_stream,
                server: sql_server,
                dyncfg: None,
            })
        });

        self.handle
    }
}
