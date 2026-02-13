// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A simple standalone pgwire server for testing the performance of frontend
//! peek sequencing with a SELECT 1 workload.
//!
//! Unlike the base pgwire-server testbed, this one enables
//! `enable_frontend_peek_sequencing` and does NOT set an `execute_override`,
//! so queries go through the real frontend peek path: SQL planning,
//! optimization, and constant fast-path evaluation.

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::net::SocketAddr;

use mz_pgwire_common::ConnectionCounter;
use mz_pgwire_server_frontend::SqlListener;
use mz_pgwire_server_frontend::SqlListenerConfig;

fn main() {
    tracing_subscriber::fmt::init();

    let worker_threads = env::var("WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
    eprintln!("Using {} worker threads", worker_threads);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
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

        eprintln!("Starting pgwire-server-frontend on {}", addr);
        eprintln!("Frontend peek sequencing: ENABLED");
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
                "pgwire-frontend".to_string(),
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
