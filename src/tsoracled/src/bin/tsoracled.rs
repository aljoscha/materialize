// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timestamp Oracle gRPC service binary.
//!
//! This service eliminates most CRDB round-trips by serving timestamps from
//! memory, only persisting to CRDB when the pre-allocation window is
//! exhausted.

use std::net::SocketAddr;
use std::sync::LazyLock;

use clap::Parser;
use mz_build_info::{BuildInfo, build_info};
use mz_ore::cli::{self, CliConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_timestamp_oracle::postgres_oracle::PostgresTimestampOracleConfig;
use mz_tsoracled::{ProtoTsOracleServer, TsoracledServer};
use tracing::info;

const BUILD_INFO: BuildInfo = build_info!();

static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Timestamp Oracle gRPC service for Materialize.
#[derive(Parser, Debug)]
#[clap(
    name = "tsoracled",
    next_line_help = true,
    version = VERSION.as_str(),
)]
struct Args {
    /// Address to listen on for gRPC connections.
    #[clap(long, default_value = "127.0.0.1:6880")]
    listen_addr: SocketAddr,

    /// Postgres/CRDB URL for durable timestamp storage.
    #[clap(long, env = "TIMESTAMP_ORACLE_URL")]
    timestamp_oracle_url: SensitiveUrl,

    /// Pre-allocation window size in timestamp units (ms for EpochMilliseconds).
    #[clap(long, default_value = "1000")]
    window_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_"),
        enable_version_flag: true,
    });

    // Set up basic tracing.
    tracing_subscriber::fmt::init();

    info!(
        "tsoracled {} starting",
        BUILD_INFO.human_version(None::<String>)
    );

    let metrics_registry = MetricsRegistry::new();
    let pg_config = PostgresTimestampOracleConfig::new(&args.timestamp_oracle_url, &metrics_registry);
    let now_fn = SYSTEM_TIME.clone();

    let server = TsoracledServer::new(pg_config, now_fn, args.window_size).await?;

    info!("listening on {} for gRPC connections", args.listen_addr);

    tonic::transport::Server::builder()
        .add_service(ProtoTsOracleServer::new(server))
        .serve(args.listen_addr)
        .await?;

    Ok(())
}
