// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mock adapter service that creates a real mz_adapter::Client but only
//! supports the bare minimum of commands needed.

use std::collections::BTreeMap;
use std::future;
use std::sync::Arc;

use mz_adapter::catalog::Catalog;
use mz_adapter::command::CatalogSnapshot;
use mz_adapter::{Client, Command, ExecuteResponse, Response, StartupResponse};
use mz_build_info::{BuildInfo, build_info};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{Datum, IntoRowIterator, Row};
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::SystemVars;
use tokio::sync::mpsc;
use tracing::{debug, info};

const BUILD_INFO: BuildInfo = build_info!();

pub struct MockAdapterService {
    client: Client,
    _handle: mz_ore::task::JoinHandle<()>,
}

impl MockAdapterService {
    pub async fn new(metrics_registry: &MetricsRegistry) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let metrics = mz_adapter::metrics::Metrics::register_into(metrics_registry);

        let environment_id = EnvironmentId::for_tests();

        let client = Client::new(
            &BUILD_INFO,
            cmd_tx,
            metrics,
            SYSTEM_TIME.clone(),
            environment_id,
            None, // No segment client
        );

        // Get ourselves a debug catalog, which uses env vars to read the actual
        // catalog in mzdata.
        let mut catalog_extract = None;
        Catalog::with_debug(|catalog| async {
            catalog_extract.replace(catalog);
        })
        .await;
        let catalog = Arc::new(catalog_extract.expect("missing catalog"));

        let handle = task::spawn(|| "mock_adapter_service", async move {
            while let Some((_ctx, cmd)) = cmd_rx.recv().await {
                match cmd {
                    Command::CatalogSnapshot { tx } => {
                        let catalog_snapshot = CatalogSnapshot {
                            catalog: Arc::clone(&catalog),
                        };

                        let _ = tx.send(catalog_snapshot);
                    }
                    Command::GetSystemVars { tx } => {
                        let _ = tx.send(SystemVars::default());
                        ()
                    }
                    Command::Startup { tx, .. } => {
                        let role_id = mz_repr::role_id::RoleId::Public;
                        let res = Ok(StartupResponse {
                            role_id,
                            write_notify: Box::pin(future::ready(())),
                            session_defaults: BTreeMap::default(),
                            catalog: Arc::clone(&catalog),
                        });
                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Execute { tx, session, .. } => {
                        let mut rows: Vec<Row> = Vec::new();
                        // rows.push(Row::pack_slice(&[Datum::Int64(1)]));

                        let row_iter = Box::new(rows.into_row_iter());

                        let execute_response =
                            ExecuteResponse::SendingRowsImmediate { rows: row_iter };

                        let res = Response {
                            result: Ok(execute_response),
                            session,
                            otel_ctx: OpenTelemetryContext::obtain(),
                        };

                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Commit {
                        tx,
                        session,
                        action: _,
                        ..
                    } => {
                        let execute_response = ExecuteResponse::TransactionCommitted {
                            params: BTreeMap::new(),
                        };

                        let res = Response {
                            result: Ok(execute_response),
                            session,
                            otel_ctx: OpenTelemetryContext::obtain(),
                        };

                        if let Err(err) = tx.send(res) {
                            tracing::error!("error starting session: {:?}", err);
                        }
                    }
                    Command::Terminate { tx, .. } => {
                        if let Some(tx) = tx {
                            if let Err(err) = tx.send(Ok(())) {
                                tracing::error!("error terminating session: {:?}", err);
                            }
                        }
                    }
                    command => {
                        info!(?command, "command not implemented");
                    }
                }
            }
            debug!("Mock adapter service command handler shutting down");
        });

        Self {
            client,
            _handle: handle,
        }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
