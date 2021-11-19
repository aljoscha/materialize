// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize ingest server.

use std::cmp;
use std::convert::TryFrom;
use std::env;
use std::ffi::CStr;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::panic;
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ::tracing::error;
use ::tracing::info;
use anyhow::{bail, Context};
use backtrace::Backtrace;
use chrono::Utc;
use clap::{AppSettings, Parser};
use itertools::Itertools;
use lazy_static::lazy_static;
use sysinfo::{ProcessorExt, SystemExt};

use mz_coord::{PersistConfig, PersistFileStorage, PersistStorage};
use mz_dataflow_types::sources::AwsExternalId;
use mz_ore::cgroup::{detect_memory_limit, MemoryLimit};
use mz_ore::metric;
use mz_ore::metrics::raw::IntCounterVec;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::metrics::ThirdPartyMetric;

use self::tracing::MetricsRecorderLayer;

mod sys;
mod tracing;

type OptionalDuration = Option<Duration>;

fn parse_optional_duration(s: &str) -> Result<OptionalDuration, anyhow::Error> {
    match s {
        "off" => Ok(None),
        _ => Ok(Some(mz_repr::util::parse_duration(s)?)),
    }
}

/// The streaming SQL materialized view engine (ingest service).
#[derive(Parser)]
#[clap(next_line_help = true, args_override_self = true, global_setting = AppSettings::NoAutoVersion)]
struct Args {
    // === Special modes. ===
    /// Print version information and exit.
    ///
    /// Specify twice to additionally print version information for selected
    /// dependencies.
    #[clap(short, long, parse(from_occurrences))]
    version: usize,
    /// Allow running this dev (unoptimized) build.
    #[cfg(debug_assertions)]
    #[clap(long)]
    dev: bool,
    // TODO(benesch): add an environment variable once we upgrade to clap v3.
    // Doesn't presently work in clap v2. See: clap-rs/clap#1476.
    /// [DANGEROUS] Enable experimental features.
    #[clap(long)]
    experimental: bool,
    /// Whether to run in safe mode.
    ///
    /// In safe mode, features that provide access to the underlying machine,
    /// like file sources and sinks, are disabled.
    ///
    /// This option is intended for use by the cloud product
    /// (cloud.materialize.com), but may be useful in other contexts as well.
    #[clap(long, hide = true)]
    safe: bool,

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

    /// Maximum allowed size of the in-memory persist storage cache, in bytes. Has
    /// to be used with --experimental.
    #[structopt(long, hide = true)]
    persist_cache_size_limit: Option<usize>,

    // === Timely worker configuration. ===
    /// Number of dataflow worker threads.
    #[clap(short, long, env = "MZ_WORKERS", value_name = "N", default_value_t)]
    workers: WorkerCount,
    /// Log Timely logging itself.
    #[clap(long, hide = true)]
    debug_introspection: bool,
    /// Retain prometheus metrics for this amount of time.
    #[clap(short, long, hide = true, parse(try_from_str = mz_repr::util::parse_duration), default_value = "5min")]
    retain_prometheus_metrics: Duration,

    // === Performance tuning parameters. ===
    /// The frequency at which to update introspection sources.
    ///
    /// The introspection sources are the built-in sources in the mz_catalog
    /// schema, like mz_scheduling_elapsed, that reflect the internal state of
    /// Materialize's dataflow engine.
    ///
    /// Set to "off" to disable introspection.
    #[clap(long, env = "MZ_INTROSPECTION_FREQUENCY", parse(try_from_str = parse_optional_duration), value_name = "FREQUENCY", default_value = "1s")]
    introspection_frequency: OptionalDuration,
    /// How much historical detail to maintain in arrangements.
    ///
    /// Set to "off" to disable logical compaction.
    #[clap(long, env = "MZ_LOGICAL_COMPACTION_WINDOW", parse(try_from_str = parse_optional_duration), value_name = "DURATION", default_value = "1ms")]
    logical_compaction_window: OptionalDuration,
    /// Default frequency with which to advance timestamps
    #[clap(long, env = "MZ_TIMESTAMP_FREQUENCY", hide = true, parse(try_from_str =mz_repr::util::parse_duration), value_name = "DURATION", default_value = "1s")]
    timestamp_frequency: Duration,
    /// Default frequency with which to scrape prometheus metrics
    #[clap(long, env = "MZ_METRICS_SCRAPING_INTERVAL", hide = true, parse(try_from_str = parse_optional_duration), value_name = "DURATION", default_value = "30s")]
    metrics_scraping_interval: OptionalDuration,

    /// [ADVANCED] Timely progress tracking mode.
    #[clap(long, env = "MZ_TIMELY_PROGRESS_MODE", value_name = "MODE", possible_values = &["eager", "demand"], default_value = "demand")]
    timely_progress_mode: timely::worker::ProgressMode,
    /// [ADVANCED] Amount of compaction to perform when idle.
    #[clap(long, env = "MZ_DIFFERENTIAL_IDLE_MERGE_EFFORT", value_name = "N")]
    differential_idle_merge_effort: Option<isize>,

    // === Logging options. ===
    /// Where to emit log messages.
    ///
    /// The special value "stderr" will emit messages to the standard error
    /// stream. All other values are taken as file paths.
    #[clap(long, env = "MZ_LOG_FILE", value_name = "PATH")]
    log_file: Option<String>,
    /// Which log messages to emit.
    ///
    /// This value is a comma-separated list of filter directives. Each filter
    /// directive has the following format:
    ///
    ///     [module::path=]level
    ///
    /// A directive indicates that log messages from the specified module that
    /// are at least as severe as the specified level should be emitted. If a
    /// directive omits the module, then it implicitly applies to all modules.
    /// When directives conflict, the last directive wins. If a log message does
    /// not match any directive, it is not emitted.
    ///
    /// The module path of a log message reflects its location in Materialize's
    /// source code. Choosing module paths for filter directives requires
    /// familiarity with Materialize's codebase and is intended for advanced
    /// users. Note that module paths change frequency from release to release.
    ///
    /// The valid levels for a log message are, in increasing order of severity:
    /// trace, debug, info, warn, and error. The special level "off" may be used
    /// in a directive to suppress all log messages, even errors.
    //
    /// The default value for this option is "info".
    #[clap(
        long,
        env = "MZ_LOG_FILTER",
        value_name = "FILTER",
        default_value = "info"
    )]
    log_filter: String,

    /// Prevent dumping of backtraces on SIGSEGV/SIGBUS
    ///
    /// In the case of OOMs and memory corruptions, it may be advantageous to NOT dump backtraces,
    /// as the attempt to dump the backtraces will segfault on its own, corrupting the core file
    /// further and obfuscating the original bug.
    #[clap(long, hide = true, env = "MZ_NO_SIGBUS_SIGSEGV_BACKTRACES")]
    no_sigbus_sigsegv_backtraces: bool,

    // == Connection options.
    /// The address on which to listen for connections.
    #[clap(
        long,
        env = "MZ_INGEST_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6675"
    )]
    listen_addr: SocketAddr,
    /// The coordinator gRPC endpoint
    #[clap(
        long,
        env = "MZ_COORD_GRPC_ADDR",
        value_name = "HOST:PORT",
        default_value = "[::1]:50051"
    )]
    coord_grpc_addr: String,
    /// How often to request new sources from the ingest control endpoint.
    #[structopt(short, long, parse(try_from_str = mz_repr::util::parse_duration), default_value = "500ms")]
    update_interval: Duration,

    // === Storage options. ===
    /// Where to store data.
    #[clap(
        short = 'D',
        long,
        env = "MZ_INGEST_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mz_ingest_data"
    )]
    data_directory: PathBuf,

    // === AWS options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,
}

/// This type is a hack to allow a dynamic default for the `--workers` argument, which depends on
/// the number of available CPUs. Ideally clap would expose a `default_fn` rather than accepting
/// only string literals.
struct WorkerCount(usize);

impl Default for WorkerCount {
    fn default() -> Self {
        WorkerCount(cmp::max(
            1,
            // When inside a cgroup with a cpu limit,
            // the logical cpus can be lower than the physical cpus.
            cmp::min(num_cpus::get(), num_cpus::get_physical()) / 2,
        ))
    }
}

impl FromStr for WorkerCount {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<WorkerCount, anyhow::Error> {
        let n = s.parse()?;
        if n == 0 {
            bail!("must be greater than zero");
        }
        Ok(WorkerCount(n))
    }
}

impl fmt::Display for WorkerCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn main() {
    if let Err(err) = run(Args::parse()) {
        eprintln!("ingestd: {:#}", err);
        process::exit(1);
    }
}

fn run(args: Args) -> Result<(), anyhow::Error> {
    panic::set_hook(Box::new(handle_panic));

    if !args.no_sigbus_sigsegv_backtraces {
        sys::enable_sigbus_sigsegv_backtraces()?;
    }

    sys::enable_sigusr2_coverage_dump()?;
    sys::enable_termination_signal_cleanup()?;

    if args.version > 0 {
        println!("ingestd {}", ingestd::BUILD_INFO.human_version());
        if args.version > 1 {
            for bi in build_info() {
                println!("{}", bi);
            }
        }
        return Ok(());
    }

    // Prevent accidental usage of development builds.
    //
    // TODO(benesch): offload environment variable check to clap once we upgrade
    // to clap v3. Doesn't presently work in clap v2. See: clap-rs/clap#1476.
    #[cfg(debug_assertions)]
    if !args.dev && !mz_ore::env::is_var_truthy("MZ_DEV") {
        bail!(
            "refusing to run dev (unoptimized) binary without explicit opt-in\n\
             hint: Pass the '--dev' option or set MZ_DEV=1 in your environment to opt in.\n\
             hint: Or perhaps you meant to use a release binary?"
        );
    }

    // Configure Timely and Differential workers.
    let log_logging = args.debug_introspection;
    let retain_readings_for = args.retain_prometheus_metrics;
    let metrics_scraping_interval = args.metrics_scraping_interval;
    let logging = args
        .introspection_frequency
        .map(|granularity| mz_coord::LoggingConfig {
            granularity,
            log_logging,
            retain_readings_for,
            metrics_scraping_interval,
        });
    if log_logging && logging.is_none() {
        bail!(
            "cannot specify --debug-introspection and --introspection-frequency=off simultaneously"
        );
    }

    // Configure storage.
    let data_directory = args.data_directory;
    fs::create_dir_all(&data_directory)
        .with_context(|| format!("creating data directory: {}", data_directory.display()))?;

    let metrics_registry = MetricsRegistry::new();
    // Configure tracing.
    {
        use tracing_subscriber::filter::{EnvFilter, LevelFilter};
        use tracing_subscriber::fmt;
        use tracing_subscriber::layer::{Layer, SubscriberExt};
        use tracing_subscriber::util::SubscriberInitExt;

        let env_filter = EnvFilter::try_new(args.log_filter)
            .context("parsing --log-filter option")?
            // Ensure panics are logged, even if the user has specified
            // otherwise.
            .add_directive("panic=error".parse().unwrap());

        let log_message_counter: ThirdPartyMetric<IntCounterVec> = metrics_registry
            .register_third_party_visible(metric!(
                name: "mz_log_message_total",
                help: "The number of log messages produced by this materialized instance",
                var_labels: ["severity"],
            ));

        match args.log_file.as_deref() {
            Some("stderr") => {
                // The user explicitly directed logs to stderr. Log only to stderr
                // with the user-specified `env_filter`.
                tracing_subscriber::registry()
                    .with(MetricsRecorderLayer::new(log_message_counter))
                    .with(env_filter)
                    .with(
                        fmt::layer()
                            .with_writer(io::stderr)
                            .with_ansi(atty::is(atty::Stream::Stderr)),
                    )
                    .init()
            }
            log_file => {
                // Logging to a file. If the user did not explicitly specify
                // a file, bubble up warnings and errors to stderr.
                let stderr_level = match log_file {
                    Some(_) => LevelFilter::OFF,
                    None => LevelFilter::WARN,
                };
                tracing_subscriber::registry()
                    .with(MetricsRecorderLayer::new(log_message_counter))
                    .with(env_filter)
                    .with({
                        let path = match log_file {
                            Some(log_file) => PathBuf::from(log_file),
                            None => data_directory.join("materialized.log"),
                        };
                        if let Some(parent) = path.parent() {
                            fs::create_dir_all(parent).with_context(|| {
                                format!("creating log file directory: {}", parent.display())
                            })?;
                        }
                        let file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&path)
                            .with_context(|| format!("creating log file: {}", path.display()))?;
                        fmt::layer().with_ansi(false).with_writer(move || {
                            file.try_clone().expect("failed to clone log file")
                        })
                    })
                    .with(
                        fmt::layer()
                            .with_writer(io::stderr)
                            .with_ansi(atty::is(atty::Stream::Stderr))
                            .with_filter(stderr_level),
                    )
                    .init()
            }
        }
    }

    // Configure prometheus process metrics.
    mz_process_collector::register_default_process_collector(&metrics_registry);

    // When inside a cgroup with a cpu limit,
    // the logical cpus can be lower than the physical cpus.
    let ncpus_useful = usize::max(1, cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let memory_limit = detect_memory_limit().unwrap_or_else(|| MemoryLimit {
        max: None,
        swap_max: None,
    });
    let memory_max_str = match memory_limit.max {
        Some(max) => format!(", {}KiB limit", max / 1024),
        None => "".to_owned(),
    };
    let swap_max_str = match memory_limit.swap_max {
        Some(max) => format!(", {}KiB limit", max / 1024),
        None => "".to_owned(),
    };

    // Print system information as the very first thing in the logs. The goal is
    // to increase the probability that we can reproduce a reported bug if all
    // we get is the log file.
    let mut system = sysinfo::System::new();
    system.refresh_system();
    info!(
        "booting server
ingestd {mz_version}
{dep_versions}
invoked as: {invocation}
os: {os}
cpus: {ncpus_logical} logical, {ncpus_physical} physical, {ncpus_useful} useful
cpu0: {cpu0}
memory: {memory_total}KB total, {memory_used}KB used{memory_limit}
swap: {swap_total}KB total, {swap_used}KB used{swap_limit}",
        mz_version = ingestd::BUILD_INFO.human_version(),
        dep_versions = build_info().join("\n"),
        invocation = {
            use shell_words::quote as escape;
            env::vars_os()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
                .filter(|(name, _value)| name.starts_with("MZ_"))
                .map(|(name, value)| format!("{}={}", escape(&name), escape(&value)))
                .chain(env::args().into_iter().map(|arg| escape(&arg).into_owned()))
                .join(" ")
        },
        os = os_info::get(),
        ncpus_logical = num_cpus::get(),
        ncpus_physical = num_cpus::get_physical(),
        ncpus_useful = ncpus_useful,
        cpu0 = {
            match &system.processors().get(0) {
                None => "<unknown>".to_string(),
                Some(cpu0) => format!("{} {}MHz", cpu0.brand(), cpu0.frequency()),
            }
        },
        memory_total = system.total_memory(),
        memory_used = system.used_memory(),
        memory_limit = memory_max_str,
        swap_total = system.total_swap(),
        swap_used = system.used_swap(),
        swap_limit = swap_max_str,
    );

    sys::adjust_rlimits();

    // Build Timely worker configuration.
    let mut timely_worker =
        timely::WorkerConfig::default().progress_mode(args.timely_progress_mode);
    differential_dataflow::configure(
        &mut timely_worker,
        &differential_dataflow::Config {
            idle_merge_effort: args.differential_idle_merge_effort,
        },
    );

    // Start Tokio runtime.
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ncpus_useful)
            // The default thread name exceeds the Linux limit on thread name
            // length, so pick something shorter.
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("tokio:work-{}", id)
            })
            .enable_all()
            .build()?,
    );

    // Configure persistence core.
    let persist_config = {
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
        let cache_size_limit = {
            if args.persist_cache_size_limit.is_some() && !args.experimental {
                bail!("cannot specify --persist-cache-size-limit without --experimental");
            }

            args.persist_cache_size_limit
        };

        let lock_info = format!(
            "ingestd {mz_version}\nos: {os}\nstart time: {start_time}\nnum workers: {num_workers}\n",
            mz_version = ingestd::BUILD_INFO.human_version(),
            os = os_info::get(),
            start_time = Utc::now(),
            num_workers = args.workers.0,
        );

        // The min_step_interval knob allows tuning a tradeoff between latency and storage usage.
        // As persist gets more sophisticated over time, we'll no longer need this knob,
        // but in the meantime we need it to make tests reasonably performant.
        // The --timestamp-frequency flag similarly gives testing a control over
        // latency vs resource usage, so for simplicity we reuse it here."
        let min_step_interval = args.timestamp_frequency;

        PersistConfig {
            async_runtime: Some(Arc::clone(&runtime)),
            storage,
            user_table_enabled: false,
            system_table_enabled: false,
            kafka_sources_enabled: false,
            lock_info,
            min_step_interval,
            cache_size_limit,
        }
    };

    let server = runtime.block_on(ingestd::serve(ingestd::Config {
        workers: args.workers.0,
        timely_worker,
        logging,
        logical_compaction_window: args.logical_compaction_window,
        timestamp_frequency: args.timestamp_frequency,
        listen_addr: args.listen_addr,
        coord_grpc_addr: args.coord_grpc_addr,
        update_interval: args.update_interval,
        data_directory,
        aws_external_id: args
            .aws_external_id
            .map(AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable)
            .unwrap_or(AwsExternalId::NotProvided),
        experimental_mode: args.experimental,
        safe_mode: args.safe,
        introspection_frequency: args
            .introspection_frequency
            .unwrap_or_else(|| Duration::from_secs(1)),
        metrics_registry,
        persist: persist_config,
    }))?;

    eprintln!(
        "=======================================================================
Thank you for trying Materialize!

We are interested in any and all feedback you have, which may be able
to improve both our software and your queries! Please reach out at:

    Web: https://materialize.com
    GitHub issues: https://github.com/MaterializeInc/materialize/issues
    Email: support@materialize.com
    Twitter: @MaterializeInc
=======================================================================
"
    );

    if args.experimental {
        eprintln!(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                WARNING!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Starting Materialize in experimental mode means:

- This node's catalog of views and sources are unstable.

If you use any version of Materialize besides this one, you might
not be able to start the Materialize node. To fix this, you'll have
to remove all of Materialize's data (e.g. rm -rf mzdata) and start
the node anew.

- You must always start this node in experimental mode; it can no
longer be started in non-experimental/regular mode.

For more details, see https://materialize.com/docs/cli#experimental-mode
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"
        );
    }

    println!(
        "ingestd {} listening on {}...",
        ingestd::BUILD_INFO.human_version(),
        server.local_addr(),
    );

    // Block forever.
    loop {
        thread::park();
    }
}

lazy_static! {
    static ref PANIC_MUTEX: Mutex<()> = Mutex::new(());
}

fn handle_panic(panic_info: &PanicInfo) {
    let _guard = PANIC_MUTEX.lock();

    let thr = thread::current();
    let thr_name = thr.name().unwrap_or("<unnamed>");

    let msg = match panic_info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match panic_info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };

    let location = if let Some(loc) = panic_info.location() {
        loc.to_string()
    } else {
        "<unknown>".to_string()
    };

    error!(
        target: "panic",
        "{msg}
thread: {thr_name}
location: {location}
version: {version} ({sha})
backtrace:
{backtrace:?}",
        msg = msg,
        thr_name = thr_name,
        location = location,
        version = ingestd::BUILD_INFO.version,
        sha = ingestd::BUILD_INFO.sha,
        backtrace = Backtrace::new(),
    );
    eprintln!(
        r#"ingestd encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the above details and file a report at:

    https://materialize.com/s/bug
"#,
    );
    process::exit(1);
}

fn build_info() -> Vec<String> {
    let openssl_version =
        unsafe { CStr::from_ptr(openssl_sys::OpenSSL_version(openssl_sys::OPENSSL_VERSION)) };
    let rdkafka_version = unsafe { CStr::from_ptr(rdkafka_sys::bindings::rd_kafka_version_str()) };
    vec![
        openssl_version.to_string_lossy().into_owned(),
        format!("librdkafka v{}", rdkafka_version.to_string_lossy()),
    ]
}
