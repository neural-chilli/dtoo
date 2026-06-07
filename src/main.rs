mod cli;
mod config;
#[allow(dead_code)]
mod convert_command;
#[allow(dead_code)]
mod crypto;
mod error;
#[allow(dead_code)]
mod file_resolution;
#[allow(dead_code)]
mod fingerprint;
#[allow(dead_code)]
mod inspect;
#[allow(dead_code)]
mod lineage;
#[allow(dead_code)]
mod manifest;
#[allow(dead_code)]
mod masking;
#[allow(dead_code)]
mod on_error;
#[allow(dead_code)]
mod output_writer;
#[allow(dead_code)]
mod path_utils;
mod polars_engine;
#[allow(dead_code)]
mod profile_command;
#[allow(dead_code)]
mod profiler;
#[allow(dead_code)]
mod query_pipeline;
#[allow(dead_code)]
mod reference_tables;
#[allow(dead_code)]
mod schema;
#[allow(dead_code)]
mod types;

use std::process::ExitCode;

use cli::{Cli, Commands, QueryArgs};
use convert_command::run as run_convert;
use error::DtooError;
use fingerprint::{display_name as fingerprint_display_name, fingerprint_file};
use inspect::run as run_inspect;
use profile_command::run as run_profile;
use query_pipeline::QueryPipeline;

fn main() -> ExitCode {
    run()
}

fn run() -> ExitCode {
    let cli = match Cli::parse_and_validate() {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            return ExitCode::from(error::EXIT_GENERAL_ERROR);
        }
    };

    if let Commands::Query(query) = &cli.command {
        maybe_warn_parquet_stdout(query);
    }

    match dispatch(cli) {
        Ok(()) => ExitCode::from(error::EXIT_SUCCESS),
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::from(err.exit_code())
        }
    }
}

fn dispatch(cli: Cli) -> Result<(), DtooError> {
    match cli.command {
        Commands::Query(args) => QueryPipeline::run(&args),
        Commands::Profile(args) => run_profile(&args),
        Commands::Inspect(args) => run_inspect(&args),
        Commands::Fingerprint(args) => {
            let hash = fingerprint_file(&args.path)?;
            println!("{hash}  {}", fingerprint_display_name(&args.path));
            Ok(())
        }
        Commands::Convert(args) => run_convert(&args),
    }
}

fn maybe_warn_parquet_stdout(query: &QueryArgs) {
    if query.output.is_none() && query.output_format == cli::OutputFormat::Parquet {
        eprintln!("Warning: --output-format parquet with stdout writes binary data to terminal");
    }
}
