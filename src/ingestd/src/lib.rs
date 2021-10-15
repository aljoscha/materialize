// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materializ ingest service.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use compile_time_run::run_command_str;
use tokio::net::TcpListener;

use mz_build_info::BuildInfo;
use mz_coord::LoggingConfig;
use mz_coord::PersistConfig;
use mz_ore::metrics::MetricsRegistry;
use mz_pid_file::PidFile;

use crate::server_metrics::Metrics;

mod ingest;
mod server_metrics;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
#[cfg(not(target_os = "macos"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: env!("CARGO_PKG_VERSION"),
    sha: run_command_str!(
        "sh",
        "-c",
        r#"if [ -n "$MZ_DEV_BUILD_SHA" ]; then
            echo "$MZ_DEV_BUILD_SHA"
        else
            # Unfortunately we need to suppress error messages from `git`, as
            # run_command_str will display no error message at all if we print
            # more than one line of output to stderr.
            git rev-parse --verify HEAD 2>/dev/null || {
                printf "error: unable to determine Git SHA; " >&2
                printf "either build from working Git clone " >&2
                printf "(see https://materialize.com/docs/install/#build-from-source), " >&2
                printf "or specify SHA manually by setting MZ_DEV_BUILD_SHA environment variable" >&2
                exit 1
            }
        fi"#
    ),
    time: run_command_str!("date", "-u", "+%Y-%m-%dT%H:%M:%SZ"),
    target_triple: env!("TARGET_TRIPLE"),
};

/// Configuration for a `ingestd` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Timely and Differential worker options. ===
    /// The number of Timely worker threads that this process should host.
    pub workers: usize,
    /// The Timely worker configuration.
    pub timely_worker: timely::WorkerConfig,

    // === Performance tuning options. ===
    pub logging: Option<LoggingConfig>,
    /// The frequency at which to update introspection.
    pub introspection_frequency: Duration,
    /// The historical window in which distinctions are maintained for
    /// arrangements.
    ///
    /// As arrangements accept new timestamps they may optionally collapse prior
    /// timestamps to the same value, retaining their effect but removing their
    /// distinction. A large value or `None` results in a large amount of
    /// historical detail for arrangements; this increases the logical times at
    /// which they can be accurately queried, but consumes more memory. A low
    /// value reduces the amount of memory required but also risks not being
    /// able to use the arrangement in a query that has other constraints on the
    /// timestamps used (e.g. when joined with other arrangements).
    pub logical_compaction_window: Option<Duration>,
    /// The interval at which sources should be timestamped.
    pub timestamp_frequency: Duration,

    // === Connection options. ===
    /// The coordinator gRPC endpoint.
    pub coord_grpc_addr: String,
    /// The IP address and port to listen on.
    pub listen_addr: SocketAddr,
    /// The frequency at which we query new sources from the control plane.
    pub update_interval: Duration,

    // === Storage options. ===
    /// The directory in which `ingestd` should store its own metadata.
    pub data_directory: PathBuf,

    // === Mode switches. ===
    /// Whether to permit usage of experimental features.
    pub experimental_mode: bool,
    /// Whether to run in safe mode.
    pub safe_mode: bool,
    /// The place where the server's metrics will be reported from.
    pub metrics_registry: MetricsRegistry,
    /// Configuration of the persistence runtime and features.
    pub persist: PersistConfig,
}

/// Start a `ingestd` server.
pub async fn serve(config: Config) -> Result<Server, anyhow::Error> {
    let workers = config.workers;

    // Attempt to acquire PID file lock.
    let pid_file = PidFile::open(config.data_directory.join("ingestd.pid"))?;

    // Initialize network listener.
    let listener = TcpListener::bind(&config.listen_addr).await?;
    let local_addr = listener.local_addr()?;

    let ingest_config = ingest::Config {
        coord_grpc_addr: config.coord_grpc_addr,
        update_interval: config.update_interval,
    };
    let ingest_handle = ingest::serve(ingest_config).await?;

    // Register metrics.
    let mut metrics_registry = config.metrics_registry;
    let _metrics = Metrics::register_with(
        &mut metrics_registry,
        workers,
        ingest_handle.start_instant(),
    );

    Ok(Server {
        local_addr,
        _pid_file: pid_file,
        _ingest_handle: ingest_handle,
    })
}

/// A running `ingestd` server.
pub struct Server {
    local_addr: SocketAddr,
    _pid_file: PidFile,
    _ingest_handle: ingest::Handle,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
