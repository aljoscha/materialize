// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A simple standalone pgwire server for testing the raw performance of our
//! pgwire implementation with a SELECT 1 workload.

use std::env;
use std::net::SocketAddr;

use mz_pgwire_common::ConnectionCounter;
use mz_pgwire_server::SqlListener;
use mz_pgwire_server::SqlListenerConfig;

fn main() {
    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(32)
        // The adapter layer requires a larger stack than the default 2 MiB.
        // environmentd uses 3 MiB in release; use mz_ore's STACK_SIZE which
        // is 16 MiB in debug builds to match test infrastructure.
        .thread_stack_size(mz_ore::stack::STACK_SIZE)
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

        eprintln!("Starting pgwire-server on {}", addr);
        eprintln!("Press Ctrl+C to stop the server");

        let config = SqlListenerConfig {
            addr,
            enable_tls: false,
        };

        let connection_counter = ConnectionCounter::default();
        connection_counter.update_limit(20_000);

        let listener = match SqlListener::bind(config).await {
            Ok(listener) => {
                eprintln!("Successfully bound to {}", addr);
                listener
            }
            Err(e) => {
                eprintln!("Failed to bind listener: {}", e);
                std::process::exit(1);
            }
        };

        let _handle = listener
            .serve_sql(
                "pgwire".to_string(),
                None, // No TLS for now
                connection_counter,
            )
            .await;

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        eprintln!("Shutting down pgwire server");
    });
}
