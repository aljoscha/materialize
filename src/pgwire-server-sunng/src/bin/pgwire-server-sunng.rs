// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A simple standalone pgwire server (sunng87/pgwire) for testing raw
//! throughput of a minimal SELECT 1 workload.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use mz_pgwire_server_sunng::SelectOneHandlerFactory;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

fn main() {
    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(std::thread::available_parallelism().map_or(8, |n| n.get()))
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    runtime.block_on(async {
        let addr: SocketAddr = env::args()
            .nth(1)
            .or_else(|| env::var("PGWIRE_LISTEN_ADDR").ok())
            .unwrap_or_else(|| "127.0.0.1:6875".to_string())
            .parse()
            .expect("Invalid listen address");

        eprintln!("Starting pgwire-server-sunng on {}", addr);
        eprintln!("Press Ctrl+C to stop the server");

        let factory = Arc::new(SelectOneHandlerFactory::new());

        let listener = TcpListener::bind(addr).await.expect("Failed to bind");
        eprintln!("Successfully bound to {}", addr);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, _addr)) => {
                        let factory_ref = factory.clone();
                        tokio::spawn(async move {
                            if let Err(e) = process_socket(socket, None, factory_ref).await {
                                tracing::warn!("connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("accept error: {}", e);
                    }
                }
            }
        });

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        eprintln!("Shutting down pgwire-server-sunng");
    });
}
