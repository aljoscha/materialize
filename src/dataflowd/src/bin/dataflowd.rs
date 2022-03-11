// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{cmp, fs, process};

use anyhow;
use anyhow::{bail, Context};
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use tokio::net::TcpListener;
use tokio::runtime::Runtime as TokioRuntime;
use tokio::select;
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use mz_dataflow::DummyBoundary;
use mz_dataflow_types::client::Client;
use mz_dataflow_types::sources::AwsExternalId;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_util::persistcfg::{PersistConfig, PersistFileStorage, PersistStorage};

#[derive(clap::ArgEnum, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum RuntimeType {
    /// Host only the compute portion of the dataflow.
    Compute,
    /// Host host storage and compute portions of the dataflow.
    LegacyMultiplexed,
    /// Host only the storage portion of the dataflow.
    Storage,
}

/// Independent dataflow server for Materialize.
#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for a connection from the coordinator.
    #[clap(
        long,
        env = "DATAFLOWD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6876"
    )]
    listen_addr: String,
    /// Number of dataflow worker threads.
    #[clap(
        short,
        long,
        env = "DATAFLOWD_WORKERS",
        value_name = "W",
        default_value = "1"
    )]
    workers: usize,
    /// Number of this dataflowd process.
    #[clap(
        short = 'p',
        long,
        env = "DATAFLOWD_PROCESS",
        value_name = "P",
        default_value = "0"
    )]
    process: usize,
    /// Total number of dataflowd processes.
    #[clap(
        short = 'n',
        long,
        env = "DATAFLOWD_PROCESSES",
        value_name = "N",
        default_value = "1"
    )]
    processes: usize,
    /// The hostnames of all dataflowd processes in the cluster.
    #[clap()]
    hosts: Vec<String>,

    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,
    /// The type of runtime hosted by this dataflowd
    #[clap(arg_enum, long, default_value = "legacy-multiplexed")]
    runtime: RuntimeType,
    /// The address of the storage server to bind or connect to.
    #[clap(
        long,
        env = "DATAFLOWD_STORAGE_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6877"
    )]
    storage_addr: String,
    #[clap(long, default_value = "0")]
    storage_workers: usize,

    // === Storage options. ===
    /// Where to store data.
    #[clap(
        short = 'D',
        long,
        env = "MZ_DATAFLOWD_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mz_dataflowd_data"
    )]
    data_directory: PathBuf,

    /// [DANGEROUS] Enable experimental features.
    #[clap(long, env = "MZ_EXPERIMENTAL")]
    experimental: bool,

    /// Unique ID to use for the persistence lock. MUST be 16 bytes because we create a Uuid from
    /// this. This will go away once we don't require persist locks anymore.
    #[structopt(long, default_value = "aaaaaaaaaaaaaaaa")]
    persist_reentrance_id: String,

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
            eprintln!("dataflowd: {:#}", err);
            process::exit(1);
        }
    })
}

fn create_communication_config(args: &Args) -> Result<timely::CommunicationConfig, anyhow::Error> {
    let threads = args.workers;
    let process = args.process;
    let processes = args.processes;
    let report = true;

    if processes > 1 {
        let mut addresses = Vec::new();
        if args.hosts.is_empty() {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        } else {
            if let Ok(file) = ::std::fs::File::open(args.hosts[0].clone()) {
                let reader = ::std::io::BufReader::new(file);
                use ::std::io::BufRead;
                for line in reader.lines().take(processes) {
                    addresses.push(line?);
                }
            } else {
                addresses.extend(args.hosts.iter().cloned());
            }
            if addresses.len() < processes {
                bail!(
                    "could only read {} addresses from {:?}, but -n: {}",
                    addresses.len(),
                    args.hosts,
                    processes
                );
            }
        }

        assert!(
            matches!(
                args.runtime,
                RuntimeType::LegacyMultiplexed | RuntimeType::Compute
            ),
            "Storage runtime with TCP boundary doesn't yet horizontally scaled Timely"
        );
        assert_eq!(processes, addresses.len());
        Ok(timely::CommunicationConfig::Cluster {
            threads,
            process,
            addresses,
            report,
            log_fn: Box::new(|_| None),
        })
    } else if threads > 1 {
        Ok(timely::CommunicationConfig::Process(threads))
    } else {
        Ok(timely::CommunicationConfig::Thread)
    }
}

fn create_timely_config(args: &Args) -> Result<timely::Config, anyhow::Error> {
    Ok(timely::Config {
        worker: timely::WorkerConfig::default(),
        communication: create_communication_config(args)?,
    })
}

async fn run(args: Args, async_runtime: Arc<TokioRuntime>) -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("DATAFLOWD_LOG_FILTER")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if args.workers == 0 {
        bail!("--workers must be greater than 0");
    }
    let timely_config = create_timely_config(&args)?;
    let metrics_registry = MetricsRegistry::new();

    // Configure storage.
    let data_directory = args.data_directory;
    fs::create_dir_all(&data_directory)
        .with_context(|| format!("creating data directory: {}", data_directory.display()))?;

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
            "dataflowd \nnum workers: {num_workers}\n",
            num_workers = args.workers,
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

    let reentrance_id = Uuid::from_slice(&args.persist_reentrance_id.as_bytes());
    let reentrance_id = match reentrance_id {
        Ok(id) => id,
        Err(e) => panic!("Invalid reentrance ID: {}", e),
    };
    let persister = persist_config.init(reentrance_id, mz_dataflowd::BUILD_INFO, &metrics_registry).await?;

    info!("about to bind to {:?}", args.listen_addr);
    let listener = TcpListener::bind(args.listen_addr).await?;

    info!(
        "listening for coordinator connection on {}...",
        listener.local_addr()?
    );

    let config = mz_dataflow::Config {
        workers: args.workers,
        timely_config,
        experimental_mode: false,
        metrics_registry: MetricsRegistry::new(),
        now: SYSTEM_TIME.clone(),
        persister: persister.runtime,
        aws_external_id: args
            .aws_external_id
            .map(AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable)
            .unwrap_or(AwsExternalId::NotProvided),
    };

    let (_server, mut client) = match args.runtime {
        RuntimeType::LegacyMultiplexed => mz_dataflow::serve(config),
        RuntimeType::Compute => {
            assert!(args.storage_workers > 0, "Storage workers needs to be > 0");
            let (storage_client, _thread) = mz_dataflow::tcp_boundary::client::connect(
                args.storage_addr,
                config.workers,
                args.storage_workers,
            )
            .await?;
            let boundary = (0..config.workers)
                .into_iter()
                .map(|_| Some((DummyBoundary, storage_client.clone())))
                .collect::<Vec<_>>();
            let boundary = Arc::new(Mutex::new(boundary));
            let workers = config.workers;
            mz_dataflow::serve_boundary(config, move |index| {
                boundary.lock().unwrap()[index % workers].take().unwrap()
            })
        }
        RuntimeType::Storage => {
            let (storage_server, _thread) =
                mz_dataflow::tcp_boundary::server::serve(args.storage_addr).await?;
            let boundary = (0..config.workers)
                .into_iter()
                .map(|_| Some((storage_server.clone(), DummyBoundary)))
                .collect::<Vec<_>>();
            let boundary = Arc::new(Mutex::new(boundary));
            let workers = config.workers;
            mz_dataflow::serve_boundary(config, move |index| {
                boundary.lock().unwrap()[index % workers].take().unwrap()
            })
        }
    }?;

    let (conn, _addr) = listener.accept().await?;
    info!("coordinator connection accepted");

    let mut conn = mz_dataflowd::tcp::framed_server(conn);
    loop {
        select! {
            cmd = conn.try_next() => match cmd? {
                None => break,
                Some(cmd) => client.send(cmd).await.unwrap(),
            },
            Some(response) = client.recv() => conn.send(response).await?,
        }
    }

    info!("coordinator connection gone; terminating");
    Ok(())
}
