// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};
use std::time::Duration;
use std::{cmp, fs, process};

use anyhow::{bail, Context};
use chrono::Utc;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::runtime::Runtime as TokioRuntime;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;
use tracing_subscriber::EnvFilter;

use materialized::http;
use materialized::mux::Mux;
use materialized::server_metrics::Metrics;
use mz_coord::{LoggingConfig, PersistConfig, PersistFileStorage, PersistStorage};
use mz_dataflow_types::client::{Client, InstanceConfig};
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflowd::{RemoteClient, SplitClient};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task;

/// Independent coordinator server for Materialize.
#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for SQL connections.
    #[clap(
        long,
        env = "COORDD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6875"
    )]
    listen_addr: String,
    /// The address of the dataflowd servers to connect to.
    #[clap()]
    dataflowd_addr: Vec<String>,
    /// The address of the storage dataflowd servers to connect to.
    #[clap(long)]
    storaged_addr: Vec<String>,
    /// Where to store data.
    #[clap(
        short = 'D',
        long,
        env = "COORDD_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mzdata"
    )]
    data_directory: PathBuf,

    /// An external ID to use for all AWS AssumeRole operations.
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,

    /// [DANGEROUS] Enable experimental features.
    #[clap(long, env = "MZ_EXPERIMENTAL")]
    experimental: bool,

    /// Enable persistent user tables. Has to be used with --experimental.
    #[clap(long, hide = true)]
    persistent_user_tables: bool,

    /// Disable persistence of all system tables.
    ///
    /// This is a test of the upcoming persistence system. The data is stored on
    /// the filesystem in a sub-directory of the Materialize data_directory.
    /// This test is enabled by default to allow us to collect data from a
    /// variety of deployments, but setting this flag to true to opt out of the
    /// test is always safe.
    #[clap(long)]
    disable_persistent_system_tables_test: bool,

    /// An S3 location used to persist data, specified as s3://<bucket>/<path>.
    ///
    /// The `<path>` is a prefix prepended to all S3 object keys used for
    /// persistence and allowed to be empty.
    ///
    /// Additional configuration can be specified by appending url-like query
    /// parameters: `?<key1>=<val1>&<key2>=<val2>...`
    ///
    /// Supported additional configurations are:
    ///
    /// - `aws_role_arn=arn:aws:...`
    ///
    /// Ignored if persistence is disabled. Ignored if --persist_storage_enabled
    /// is false.
    ///
    /// If unset, files stored under `--data-directory/-D` are used instead. If
    /// set, S3 credentials and region must be available in the process or
    /// environment: for details see
    /// https://github.com/rusoto/rusoto/blob/rusoto-v0.47.0/AWS-CREDENTIALS.md.
    #[clap(long, hide = true, default_value_t)]
    persist_storage: String,

    /// Enable the --persist_storage flag. Has to be used with --experimental.
    #[structopt(long, hide = true)]
    persist_storage_enabled: bool,

    /// Enable persistent Kafka source. Has to be used with --experimental.
    #[structopt(long, hide = true)]
    persistent_kafka_sources: bool,

    /// Maximum allowed size of the in-memory persist storage cache, in bytes. Has
    /// to be used with --experimental.
    #[structopt(long, hide = true)]
    persist_cache_size_limit: Option<usize>,

    /// Default frequency with which to advance timestamps, this is also used to drive the persist
    /// step interval.
    #[clap(long, env = "MZ_TIMESTAMP_FREQUENCY", hide = true, parse(try_from_str = mz_repr::util::parse_duration), value_name = "DURATION", default_value = "1s")]
    timestamp_frequency: Duration,
}

fn main() {
    // Start Tokio runtime.
    let ncpus_useful = usize::max(1, cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ncpus_useful)
            // The default thread name exceeds the Linux limit on thread name
            // length, so pick something shorter.
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, atomic::Ordering::SeqCst);
                format!("tokio:work-{}", id)
            })
            .enable_all()
            .build()
            .unwrap(),
    );

    let shared_runtime = Arc::clone(&runtime);

    runtime.block_on(async move {
        if let Err(err) = run(mz_ore::cli::parse_args(), shared_runtime).await {
            eprintln!("coordd: {:#}", err);
            process::exit(1);
        }
    })
}

async fn run(args: Args, async_runtime: Arc<TokioRuntime>) -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("COORDD_LOG_FILTER").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let (dataflow_client, dataflow_instance): (Box<dyn Client + Send + 'static>, _) =
        if args.storaged_addr.is_empty() {
            info!(
                "connecting to dataflowd server at {:?}...",
                args.dataflowd_addr
            );
            let client = RemoteClient::connect(&args.dataflowd_addr).await?;
            (Box::new(client), InstanceConfig::Virtual)
        } else {
            info!(
                "connecting to dataflowd server at {:?} and storaged server at {:?}...",
                args.dataflowd_addr, args.storaged_addr
            );
            let storage_client = RemoteClient::connect(&args.storaged_addr).await?;
            let client = SplitClient::new(storage_client);
            let config = InstanceConfig::Remote(args.dataflowd_addr);
            (Box::new(client), config)
        };

    let experimental_mode = args.experimental;
    let mut metrics_registry = MetricsRegistry::new();
    let data_directory = args.data_directory;
    fs::create_dir_all(&data_directory)
        .with_context(|| format!("creating data directory: {}", data_directory.display()))?;
    let coord_storage = mz_coord::catalog::storage::Connection::open(
        &data_directory.join("catalog"),
        Some(experimental_mode),
    )?;

    // Configure persistence core.
    let persist_config = {
        let user_table_enabled = if args.experimental && args.persistent_user_tables {
            true
        } else if args.persistent_user_tables {
            bail!("cannot specify --persistent-user-tables without --experimental");
        } else {
            false
        };
        let system_table_enabled = false;

        let storage = if args.persist_storage_enabled {
            if args.persist_storage.is_empty() {
                bail!("--persist-storage must be specified with --persist-storage-enabled");
            } else if !args.experimental {
                bail!("cannot specify --persist-storage-enabled without --experimental");
            } else {
                PersistStorage::try_from(args.persist_storage)?
            }
        } else {
            PersistStorage::File(PersistFileStorage {
                blob_path: data_directory.join("persist").join("blob"),
            })
        };

        let persistent_kafka_sources_enabled = if args.experimental && args.persistent_kafka_sources
        {
            true
        } else if args.persistent_kafka_sources {
            bail!("cannot specify --persistent-kafka-sources without --experimental");
        } else {
            false
        };

        let cache_size_limit = {
            if args.persist_cache_size_limit.is_some() && !args.experimental {
                bail!("cannot specify --persist-cache-size-limit without --experimental");
            }

            args.persist_cache_size_limit
        };

        let lock_info = format!(
            "coordd {mz_version}\nos: {os}\nstart time: {start_time}\n",
            mz_version = materialized::BUILD_INFO.human_version(),
            os = os_info::get(),
            start_time = Utc::now(),
        );

        // The min_step_interval knob allows tuning a tradeoff between latency and storage usage.
        // As persist gets more sophisticated over time, we'll no longer need this knob,
        // but in the meantime we need it to make tests reasonably performant.
        // The --timestamp-frequency flag similarly gives testing a control over
        // latency vs resource usage, so for simplicity we reuse it here."
        let min_step_interval = args.timestamp_frequency;

        PersistConfig {
            async_runtime: Some(Arc::clone(&async_runtime)),
            storage,
            user_table_enabled,
            system_table_enabled,
            kafka_sources_enabled: persistent_kafka_sources_enabled,
            lock_info,
            min_step_interval,
            cache_size_limit,
        }
    };

    let persister = persist_config
        .init(
            coord_storage.cluster_id(),
            materialized::BUILD_INFO,
            &metrics_registry,
        )
        .await?;

    // TODO: expose as a parameter
    let granularity = Duration::from_secs(1);
    let (coord_handle, coord_client) = mz_coord::serve(mz_coord::Config {
        dataflow_client,
        dataflow_instance,
        logging: Some(LoggingConfig {
            log_logging: false,
            granularity,
            retain_readings_for: granularity,
            metrics_scraping_interval: Some(granularity),
        }),
        storage: coord_storage,
        timestamp_frequency: Duration::from_secs(1),
        logical_compaction_window: Some(Duration::from_millis(1)),
        experimental_mode,
        disable_user_indexes: false,
        safe_mode: false,
        build_info: &materialized::BUILD_INFO,
        aws_external_id: args
            .aws_external_id
            .map(AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable)
            .unwrap_or(AwsExternalId::NotProvided),
        metrics_registry: metrics_registry.clone(),
        persister,
        now: SYSTEM_TIME.clone(),
    })
    .await?;

    let metrics = Metrics::register_with(
        &mut metrics_registry,
        usize::MAX,
        coord_handle.start_instant(),
    );

    let listener = TcpListener::bind(&args.listen_addr).await?;

    task::spawn(|| "pgwire_server", {
        let pgwire_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: None,
            coord_client: coord_client.clone(),
            metrics_registry: &metrics_registry,
            frontegg: None,
        });
        let http_server = http::Server::new(http::Config {
            tls: None,
            frontegg: None,
            coord_client,
            metrics_registry,
            global_metrics: metrics,
            pgwire_metrics: pgwire_server.metrics(),
        });
        let mut mux = Mux::new();
        mux.add_handler(pgwire_server);
        mux.add_handler(http_server);

        info!(
            "listening for pgwire connections on {}...",
            listener.local_addr()?
        );

        async move {
            let mut incoming = TcpListenerStream::new(listener);
            mux.serve(incoming.by_ref()).await;
        }
    });
    Ok(())
}
