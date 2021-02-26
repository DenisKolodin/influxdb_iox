//! Entrypoint of InfluxDB IOx binary
#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use clap::{crate_authors, crate_version, App, Arg, ArgMatches, FromArgMatches, IntoApp};
use dotenv::dotenv;
use ingest::parquet::writer::CompressionLevel;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

mod commands {
    pub mod config;
    pub mod convert;
    pub mod file_meta;
    mod input;
    pub mod logging;
    pub mod stats;
}
pub mod influxdb_ioxd;

use commands::{config::Config, logging::LoggingLevel};

enum ReturnCode {
    ConversionFailed = 1,
    MetadataDumpFailed = 2,
    StatsFailed = 3,
    ServerExitedAbnormally = 4,
}

fn main() -> Result<(), std::io::Error> {
    let about = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server:
    influxdb_iox

    # Display all server settings
    influxdb_iox server --help

    # Run the InfluxDB IOx server with extra verbose logging
    influxdb_iox -v

    # Run InfluxDB IOx with full debug logging specified with RUST_LOG
    RUST_LOG=debug influxdb_iox

    # converts line protocol formatted data in temperature.lp to out.parquet
    influxdb_iox convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    influxdb_iox meta 000000000013.tsm

    # Dumps storage statistics about out.parquet to stdout
    influxdb_iox stats out.parquet
"#;
    // load all environment variables from .env before doing anything
    load_dotenv();

    let matches = App::new(about)
        .version(crate_version!())
        .author(crate_authors!())
        .about("InfluxDB IOx server and command line tools")
        .subcommand(
            App::new("convert")
                .about("Convert one storage format to another")
                .arg(
                    Arg::new("INPUT")
                        .about("The input files to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::new("OUTPUT")
                        .takes_value(true)
                        .about("The filename or directory to write the output.")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::new("compression_level")
                        .short('c')
                        .long("compression-level")
                        .about("How much to compress the output data. 'max' compresses the most; 'compatibility' compresses in a manner more likely to be readable by other tools.")
                        .takes_value(true)
                        .possible_values(&["max", "compatibility"])
                        .default_value("compatibility"),
                ),
        )
        .subcommand(
            App::new("meta")
                .about("Print out metadata information about a storage file")
                .arg(
                    Arg::new("INPUT")
                        .about("The input filename to read from")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            App::new("stats")
                .about("Print out storage statistics information to stdout. \
                        If a directory is specified, checks all files recursively")
                .arg(
                    Arg::new("INPUT")
                        .about("The input filename or directory to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::new("per-column")
                        .long("per-column")
                        .about("Include detailed information per column")
                )
                .arg(
                    Arg::new("per-file")
                        .long("per-file")
                        .about("Include detailed information per file")
                ),
        )
         .subcommand(
            commands::config::Config::into_app(),
        )
        .arg(Arg::new("verbose").short('v').long("verbose").multiple(true).about(
            "Enables verbose logging (use 'vv' for even more verbosity). You can also set log level via \
                       the environment variable RUST_LOG=<value>",
        ))
        .arg(Arg::new("num-threads").long("num-threads").takes_value(true).about(
            "Set the maximum number of threads to use. Defaults to the number of cores on the system",
        ))
        .get_matches();

    let tokio_runtime = get_runtime(matches.value_of("num-threads"))?;
    tokio_runtime.block_on(dispatch_args(matches));

    info!("InfluxDB IOx server shutting down");
    Ok(())
}

async fn dispatch_args(matches: ArgMatches) {
    // Logging level is determined via:
    // 1. If RUST_LOG environment variable is set, use that value
    // 2. if `-vv` (multiple instances of verbose), use DEFAULT_DEBUG_LOG_LEVEL
    // 2. if `-v` (single instances of verbose), use DEFAULT_VERBOSE_LOG_LEVEL
    // 3. Otherwise use DEFAULT_LOG_LEVEL
    let logging_level = LoggingLevel::new(matches.occurrences_of("verbose"));

    match matches.subcommand() {
        Some(("convert", sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_path = sub_matches.value_of("INPUT").unwrap();
            let output_path = sub_matches.value_of("OUTPUT").unwrap();
            let compression_level = sub_matches
                .value_of_t::<CompressionLevel>("compression_level")
                .unwrap();
            match commands::convert::convert(&input_path, &output_path, compression_level) {
                Ok(()) => debug!("Conversion completed successfully"),
                Err(e) => {
                    eprintln!("Conversion failed: {}", e);
                    std::process::exit(ReturnCode::ConversionFailed as _)
                }
            }
        }
        Some(("meta", sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_filename = sub_matches.value_of("INPUT").unwrap();
            match commands::file_meta::dump_meta(&input_filename) {
                Ok(()) => debug!("Metadata dump completed successfully"),
                Err(e) => {
                    eprintln!("Metadata dump failed: {}", e);
                    std::process::exit(ReturnCode::MetadataDumpFailed as _)
                }
            }
        }
        Some(("stats", sub_matches)) => {
            logging_level.setup_basic_logging();
            let config = commands::stats::StatsConfig {
                input_path: sub_matches.value_of("INPUT").unwrap().into(),
                per_file: sub_matches.is_present("per-file"),
                per_column: sub_matches.is_present("per-column"),
            };

            match commands::stats::stats(&config).await {
                Ok(()) => debug!("Storage statistics dump completed successfully"),
                Err(e) => {
                    eprintln!("Stats dump failed: {}", e);
                    std::process::exit(ReturnCode::StatsFailed as _)
                }
            }
        }
        // Handle the case where the user explicitly specified the server command
        Some(("server", sub_matches)) => {
            // Note don't set up basic logging here, different logging rules appy in server
            // mode
            let res =
                influxdb_ioxd::main(logging_level, Some(Config::from_arg_matches(sub_matches)))
                    .await;

            if let Err(e) = res {
                error!("Server shutdown with error: {}", e);
                std::process::exit(ReturnCode::ServerExitedAbnormally as _);
            }
        }
        // handle the case where the user didn't specify a command
        _ => {
            // Note don't set up basic logging here, different logging rules appy in server
            // mode
            let res = influxdb_ioxd::main(logging_level, None).await;
            if let Err(e) = res {
                error!("Server shutdown with error: {}", e);
                std::process::exit(ReturnCode::ServerExitedAbnormally as _);
            }
        }
    }
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<&str>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );
            let n = num_threads.parse::<usize>().map_err(|e| {
                let msg = format!(
                    "Invalid num-threads: can not parse '{}' as an integer: {}",
                    num_threads, e
                );
                std::io::Error::new(kind, msg)
            })?;

            match n {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(n)
                    .build(),
            }
        }
    }
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}
