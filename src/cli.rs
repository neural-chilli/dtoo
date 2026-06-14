use std::collections::HashSet;
use std::path::PathBuf;

use clap::{
    ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, error::ErrorKind,
    parser::ValueSource,
};

use crate::config::Config;

/// Top-level command-line interface for `dtoo`.
#[derive(Debug, Parser)]
#[command(name = "dtoo", version, about = "Data tooling for engineers")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// Supported `dtoo` subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    Query(Box<QueryArgs>),
    Convert(ConvertArgs),
    Profile(ProfileArgs),
    Inspect(InspectArgs),
    Fingerprint(FingerprintArgs),
    Synth(SynthArgs),
}

/// Arguments for `dtoo query`.
#[derive(Debug, Parser)]
pub struct QueryArgs {
    #[arg(value_name = "PATH")]
    pub paths: Vec<String>,

    #[arg(long)]
    pub glob: Option<String>,

    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    #[arg(long)]
    pub pipe: Option<PipeMode>,

    #[arg(long = "stdin-format")]
    pub stdin_format: Option<StdinFormat>,

    #[arg(long)]
    pub sheet: Option<String>,

    #[arg(long = "where")]
    pub where_clause: Option<String>,

    #[arg(long = "filter-sql")]
    pub filter_sql: Option<String>,

    #[arg(long = "post-sql")]
    pub post_sql: Option<String>,

    #[arg(long = "ref")]
    pub refs: Vec<String>,

    #[arg(long)]
    pub schema: Option<PathBuf>,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long = "output-format", default_value = "csv")]
    pub output_format: OutputFormat,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long)]
    pub lineage: Option<String>,

    #[arg(long)]
    pub mask: Option<String>,

    #[arg(long = "mask-salt", default_value = "")]
    pub mask_salt: String,

    #[arg(long)]
    pub profile: Option<PathBuf>,

    #[arg(long = "profile-format", default_value = "json")]
    pub profile_format: ProfileFormat,

    #[arg(long = "profile-sample", default_value_t = 100)]
    pub profile_sample: u8,

    #[arg(long = "profile-detail", default_value = "standard")]
    pub profile_detail: ProfileDetail,

    #[arg(long = "top-k", default_value_t = 1000)]
    pub top_k: usize,

    #[arg(long)]
    pub limit: Option<usize>,

    #[arg(long = "no-header")]
    pub no_header: bool,

    #[arg(long)]
    pub compress: Option<CompressMethod>,

    #[arg(long)]
    pub count: bool,

    #[arg(long = "expect-at-least")]
    pub expect_at_least: Option<usize>,

    #[arg(long)]
    pub fingerprint: bool,

    #[arg(long = "dry-run")]
    pub dry_run: bool,

    #[arg(long)]
    pub verbose: bool,

    #[arg(long = "on-error", default_value = "fail")]
    pub on_error: OnErrorMode,

    #[arg(long)]
    pub manifest: Option<PathBuf>,

    #[arg(long = "s3-region")]
    pub s3_region: Option<String>,

    #[arg(long = "s3-profile")]
    pub s3_profile: Option<String>,

    #[arg(long = "gcs-project")]
    pub gcs_project: Option<String>,

    #[arg(long = "azure-account")]
    pub azure_account: Option<String>,

    #[arg(long)]
    pub config: Option<PathBuf>,

    #[arg(long = "crypto-profile")]
    pub crypto_profile: Option<String>,

    #[arg(long = "crypto-profiles-file")]
    pub crypto_profiles_file: Option<PathBuf>,

    #[arg(long = "allow-plaintext-pii")]
    pub allow_plaintext_pii: bool,

    #[arg(long = "encrypt-output-profile")]
    pub encrypt_output_profile: Option<String>,

    #[arg(long = "encrypt-columns")]
    pub encrypt_columns: Option<String>,
}

/// Arguments for `dtoo convert`.
#[derive(Debug, Parser)]
pub struct ConvertArgs {
    pub input: String,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long = "output-format", default_value = "csv")]
    pub output_format: OutputFormat,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long)]
    pub sheet: Option<String>,

    #[arg(long = "crypto-profile")]
    pub crypto_profile: Option<String>,

    #[arg(long = "crypto-profiles-file")]
    pub crypto_profiles_file: Option<PathBuf>,

    #[arg(long = "allow-plaintext-pii")]
    pub allow_plaintext_pii: bool,

    #[arg(long = "encrypt-profile")]
    pub encrypt_profile: Option<String>,

    #[arg(long = "encrypt-columns")]
    pub encrypt_columns: Option<String>,

    #[arg(long = "s3-region")]
    pub s3_region: Option<String>,

    #[arg(long = "s3-profile")]
    pub s3_profile: Option<String>,

    #[arg(long = "gcs-project")]
    pub gcs_project: Option<String>,

    #[arg(long = "azure-account")]
    pub azure_account: Option<String>,
}

/// Arguments for `dtoo profile`.
#[derive(Debug, Parser)]
pub struct ProfileArgs {
    pub path: PathBuf,

    #[arg(long, default_value = "json")]
    pub format: ProfileFormat,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long, default_value_t = 100)]
    pub sample: u8,

    #[arg(long, default_value = "standard")]
    pub detail: ProfileDetail,

    #[arg(long = "top-k", default_value_t = 1000)]
    pub top_k: usize,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long = "s3-region")]
    pub s3_region: Option<String>,

    #[arg(long = "s3-profile")]
    pub s3_profile: Option<String>,

    #[arg(long = "gcs-project")]
    pub gcs_project: Option<String>,

    #[arg(long = "azure-account")]
    pub azure_account: Option<String>,

    #[arg(long = "crypto-profile")]
    pub crypto_profile: Option<String>,

    #[arg(long = "crypto-profiles-file")]
    pub crypto_profiles_file: Option<PathBuf>,
}

/// Arguments for `dtoo inspect`.
#[derive(Debug, Parser)]
pub struct InspectArgs {
    pub path: PathBuf,

    #[arg(long, default_value_t = 10)]
    pub rows: usize,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long = "s3-region")]
    pub s3_region: Option<String>,

    #[arg(long = "s3-profile")]
    pub s3_profile: Option<String>,

    #[arg(long = "gcs-project")]
    pub gcs_project: Option<String>,

    #[arg(long = "azure-account")]
    pub azure_account: Option<String>,

    #[arg(long = "crypto-discover")]
    pub crypto_discover: bool,

    #[arg(long = "crypto-profile")]
    pub crypto_profile: Option<String>,

    #[arg(long = "crypto-profiles-file")]
    pub crypto_profiles_file: Option<PathBuf>,
}

/// Arguments for `dtoo fingerprint`.
#[derive(Debug, Parser)]
pub struct FingerprintArgs {
    pub path: PathBuf,
}

/// Arguments for `dtoo synth`.
#[derive(Debug, Parser)]
#[command(about = "Generate synthetic data from one or more profiles")]
pub struct SynthArgs {
    #[arg(long)]
    pub spec: Option<PathBuf>,

    #[arg(long)]
    pub profile: Option<PathBuf>,

    #[arg(long)]
    pub rows: Option<usize>,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long = "output-format", default_value = "csv")]
    pub output_format: OutputFormat,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long)]
    pub compress: Option<CompressMethod>,

    #[arg(long = "no-header")]
    pub no_header: bool,

    #[arg(long)]
    pub seed: Option<u64>,

    #[arg(long = "dry-run")]
    pub dry_run: bool,

    #[arg(long)]
    pub verbose: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum PipeMode {
    File,
    Data,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum StdinFormat {
    Csv,
    Parquet,
    Ndjson,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Csv,
    Parquet,
    Ndjson,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProfileFormat {
    Json,
    Csv,
    Html,
}

/// Controls how much detail is included in a profile report.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ProfileDetail {
    /// Standard output: per-column statistics only (default).
    #[default]
    Standard,
    /// Synth output: adds histograms, top-K values, unique ratios, and correlation matrix.
    Synth,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CompressMethod {
    Gzip,
    Zstd,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum OnErrorMode {
    Skip,
    Fail,
}

impl Cli {
    /// Parse CLI arguments from the process environment.
    pub fn parse_and_validate() -> Result<Self, clap::Error> {
        Self::parse_and_validate_from(std::env::args_os())
    }

    /// Parse CLI arguments from a custom iterator and run validations.
    pub fn parse_and_validate_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args = args
            .into_iter()
            .map(Into::into)
            .collect::<Vec<std::ffi::OsString>>();
        let matches = Self::command().try_get_matches_from(args)?;
        let mut cli = Self::from_arg_matches(&matches)?;

        if let (Commands::Query(query), Some(("query", query_matches))) =
            (&mut cli.command, matches.subcommand())
        {
            query
                .apply_config(query_matches)
                .map_err(|message| Self::command().error(ErrorKind::ValueValidation, message))?;
            query
                .validate()
                .map_err(|message| Self::command().error(ErrorKind::ValueValidation, message))?;
        }

        if let Commands::Synth(synth) = &cli.command {
            synth
                .validate()
                .map_err(|message| Self::command().error(ErrorKind::ValueValidation, message))?;
        }

        Ok(cli)
    }
}

impl SynthArgs {
    fn validate(&self) -> Result<(), String> {
        match (&self.spec, &self.profile) {
            (Some(_), Some(_)) => Err("--spec and --profile are mutually exclusive".to_string()),
            (None, None) => Err("synth requires --spec or --profile".to_string()),
            (Some(_), None) => {
                if self.rows.is_some() || self.output.is_some() {
                    Err("--rows and --output are only valid with --profile (spec mode reads them from the spec)".to_string())
                } else {
                    Ok(())
                }
            }
            (None, Some(_)) => {
                if self.rows.is_none() {
                    Err("--profile mode requires --rows".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl QueryArgs {
    fn validate(&self) -> Result<(), String> {
        self.validate_input_source()?;
        self.validate_pipe_data_dependencies()?;
        self.validate_output_flags()?;
        self.validate_refs()?;
        self.validate_lineage()?;
        self.validate_mask()?;
        Ok(())
    }

    fn validate_input_source(&self) -> Result<(), String> {
        let sources = [
            self.glob.is_some(),
            self.pipe.is_some(),
            !self.paths.is_empty(),
        ];
        let selected = sources.iter().filter(|selected| **selected).count();
        if selected > 1 {
            return Err(
                "--glob, --pipe, and positional paths are mutually exclusive input sources"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn validate_pipe_data_dependencies(&self) -> Result<(), String> {
        if self.pipe == Some(PipeMode::Data) && self.stdin_format.is_none() {
            return Err("--pipe data requires --stdin-format".to_string());
        }
        Ok(())
    }

    fn validate_output_flags(&self) -> Result<(), String> {
        if let Some(limit) = self.limit
            && limit == 0
        {
            return Err("--limit must be a positive integer".to_string());
        }
        if self.encrypt_columns.is_some() && self.encrypt_output_profile.is_none() {
            return Err("--encrypt-columns requires --encrypt-output-profile".to_string());
        }
        Ok(())
    }

    fn validate_refs(&self) -> Result<(), String> {
        let mut seen = HashSet::new();
        for reference in &self.refs {
            let mut parts = reference.splitn(2, '=');
            let Some(name) = parts.next() else {
                return Err("--ref entries must be in NAME=PATH format".to_string());
            };
            let Some(path) = parts.next() else {
                return Err("--ref entries must be in NAME=PATH format".to_string());
            };
            if name.trim().is_empty() || path.trim().is_empty() {
                return Err("--ref entries must be in NAME=PATH format".to_string());
            }

            let name = name.trim();
            if name == "_" || name == "temp_results" {
                return Err(format!(
                    "--ref table name `{name}` is reserved; use a different identifier"
                ));
            }
            if !is_valid_identifier(name) {
                return Err(format!(
                    "--ref table name `{name}` must be a valid identifier (letters, numbers, underscore; cannot start with a number)"
                ));
            }
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(format!("duplicate --ref table name `{name}`"));
            }
        }
        Ok(())
    }

    fn validate_lineage(&self) -> Result<(), String> {
        let Some(lineage) = &self.lineage else {
            return Ok(());
        };

        if lineage == "all" {
            return Ok(());
        }

        let valid_columns = [
            "batch_id",
            "record_id",
            "batch_timestamp",
            "batch_hash",
            "origin_file",
        ];

        for column in lineage.split(',') {
            let trimmed = column.trim();
            if trimmed.is_empty() || !valid_columns.contains(&trimmed) {
                return Err("--lineage must be `all` or a comma-separated subset of: batch_id, record_id, batch_timestamp, batch_hash, origin_file".to_string());
            }
        }

        Ok(())
    }

    fn apply_config(&mut self, query_matches: &ArgMatches) -> Result<(), String> {
        let Some(path) = &self.config else {
            return Ok(());
        };
        let config = crate::config::load_query_config(path).map_err(|err| err.to_string())?;
        self.merge_config(config, query_matches)
    }

    fn merge_config(&mut self, config: Config, query_matches: &ArgMatches) -> Result<(), String> {
        if !has_cli_value(query_matches, "glob")
            && !has_cli_value(query_matches, "paths")
            && let Some(glob) = config.glob
        {
            self.glob = Some(glob);
        }

        if !has_cli_value(query_matches, "exclude")
            && let Some(exclude) = config.exclude
        {
            self.exclude = exclude;
        }

        if !has_cli_value(query_matches, "sheet")
            && let Some(sheet) = config.sheet
        {
            self.sheet = Some(sheet);
        }

        if !has_cli_value(query_matches, "where_clause")
            && let Some(where_clause) = config.where_clause
        {
            self.where_clause = Some(where_clause);
        }

        if !has_cli_value(query_matches, "filter_sql")
            && let Some(filter_sql) = config.filter_sql
        {
            self.filter_sql = Some(filter_sql);
        }

        if !has_cli_value(query_matches, "post_sql")
            && let Some(post_sql) = config.post_sql
        {
            self.post_sql = Some(post_sql);
        }

        if !has_cli_value(query_matches, "refs")
            && let Some(ref_tables) = config.ref_tables
        {
            self.refs = ref_tables
                .into_iter()
                .map(|(name, path)| format!("{name}={path}"))
                .collect();
        }

        if !has_cli_value(query_matches, "schema")
            && let Some(schema) = config.schema
        {
            self.schema = Some(PathBuf::from(schema));
        }

        if !has_cli_value(query_matches, "output")
            && let Some(output) = config.output
        {
            self.output = Some(PathBuf::from(output));
        }

        if !has_cli_value(query_matches, "output_format")
            && let Some(output_format) = config.output_format
        {
            self.output_format = OutputFormat::from_str(&output_format, true).map_err(|_| {
                "--config output_format must be one of: csv, parquet, ndjson".to_string()
            })?;
        }

        if !has_cli_value(query_matches, "delimiter")
            && let Some(delimiter) = config.delimiter
        {
            self.delimiter = parse_delimiter(&delimiter)?;
        }

        if !has_cli_value(query_matches, "compress")
            && let Some(compress) = config.compress
        {
            self.compress = Some(
                CompressMethod::from_str(&compress, true)
                    .map_err(|_| "--config compress must be one of: gzip, zstd".to_string())?,
            );
        }

        if !has_cli_value(query_matches, "limit")
            && let Some(limit) = config.limit
        {
            self.limit = Some(limit);
        }

        if !has_cli_value(query_matches, "no_header")
            && let Some(no_header) = config.no_header
        {
            self.no_header = no_header;
        }

        if !has_cli_value(query_matches, "lineage")
            && let Some(lineage) = config.lineage
        {
            self.lineage = Some(lineage);
        }

        if !has_cli_value(query_matches, "mask")
            && let Some(mask) = config.mask
        {
            self.mask = Some(mask.columns.join(","));
            self.mask_salt = mask.salt.unwrap_or_default();
        }

        if !has_cli_value(query_matches, "profile")
            && let Some(profile) = config.profile
        {
            self.profile = Some(PathBuf::from(profile.path));
            if !has_cli_value(query_matches, "profile_format")
                && let Some(format) = profile.format
            {
                self.profile_format = ProfileFormat::from_str(&format, true).map_err(|_| {
                    "--config profile.format must be one of: json, csv, html".to_string()
                })?;
            }
            if !has_cli_value(query_matches, "profile_sample")
                && let Some(sample) = profile.sample
            {
                self.profile_sample = sample;
            }
        }

        if !has_cli_value(query_matches, "fingerprint")
            && let Some(fingerprint) = config.fingerprint
        {
            self.fingerprint = fingerprint;
        }

        if !has_cli_value(query_matches, "manifest")
            && let Some(manifest) = config.manifest
        {
            self.manifest = Some(PathBuf::from(manifest));
        }

        if !has_cli_value(query_matches, "expect_at_least")
            && let Some(expect_at_least) = config.expect_at_least
        {
            self.expect_at_least = Some(expect_at_least);
        }

        if !has_cli_value(query_matches, "count")
            && let Some(count) = config.count
        {
            self.count = count;
        }

        if !has_cli_value(query_matches, "on_error")
            && let Some(on_error) = config.on_error
        {
            self.on_error = OnErrorMode::from_str(&on_error, true)
                .map_err(|_| "--config on_error must be one of: skip, fail".to_string())?;
        }

        if !has_cli_value(query_matches, "verbose")
            && let Some(verbose) = config.verbose
        {
            self.verbose = verbose;
        }

        if !has_cli_value(query_matches, "dry_run")
            && let Some(dry_run) = config.dry_run
        {
            self.dry_run = dry_run;
        }

        if let Some(cloud) = config.cloud {
            if !has_cli_value(query_matches, "s3_region")
                && let Some(s3_region) = cloud.s3_region
            {
                self.s3_region = Some(s3_region);
            }
            if !has_cli_value(query_matches, "s3_profile")
                && let Some(s3_profile) = cloud.s3_profile
            {
                self.s3_profile = Some(s3_profile);
            }
            if !has_cli_value(query_matches, "gcs_project")
                && let Some(gcs_project) = cloud.gcs_project
            {
                self.gcs_project = Some(gcs_project);
            }
            if !has_cli_value(query_matches, "azure_account")
                && let Some(azure_account) = cloud.azure_account
            {
                self.azure_account = Some(azure_account);
            }
        }

        Ok(())
    }

    fn validate_mask(&self) -> Result<(), String> {
        let Some(mask) = &self.mask else {
            return Ok(());
        };

        if mask.split(',').map(str::trim).all(|token| token.is_empty()) {
            return Err("--mask requires at least one column name".to_string());
        }

        Ok(())
    }
}

fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn has_cli_value(matches: &ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

fn parse_delimiter(value: &str) -> Result<char, String> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err("--config delimiter must be a single character".to_string());
    };
    if chars.next().is_some() {
        return Err("--config delimiter must be a single character".to_string());
    }
    Ok(ch)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn parses_query_defaults() {
        let cli = parse(["dtoo", "query", "input.csv"]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.paths, vec!["input.csv"]);
                assert_eq!(args.output_format, OutputFormat::Csv);
                assert_eq!(args.profile_format, ProfileFormat::Json);
                assert_eq!(args.profile_sample, 100);
                assert_eq!(args.on_error, OnErrorMode::Fail);
                assert!(!args.no_header);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn parses_profile_defaults() {
        let cli = parse(["dtoo", "profile", "input.csv"]);

        match cli.command {
            Commands::Profile(args) => {
                assert_eq!(args.path, PathBuf::from("input.csv"));
                assert_eq!(args.format, ProfileFormat::Json);
                assert_eq!(args.sample, 100);
                assert_eq!(args.delimiter, ',');
                assert_eq!(args.s3_region, None);
                assert_eq!(args.s3_profile, None);
                assert_eq!(args.gcs_project, None);
                assert_eq!(args.azure_account, None);
            }
            _ => panic!("expected profile command"),
        }
    }

    #[test]
    fn parses_profile_cloud_options() {
        let cli = parse([
            "dtoo",
            "profile",
            "s3://bucket/data.parquet",
            "--s3-region",
            "eu-west-1",
            "--s3-profile",
            "analytics",
            "--gcs-project",
            "proj-1",
            "--azure-account",
            "acct1",
        ]);

        match cli.command {
            Commands::Profile(args) => {
                assert_eq!(args.s3_region.as_deref(), Some("eu-west-1"));
                assert_eq!(args.s3_profile.as_deref(), Some("analytics"));
                assert_eq!(args.gcs_project.as_deref(), Some("proj-1"));
                assert_eq!(args.azure_account.as_deref(), Some("acct1"));
            }
            _ => panic!("expected profile command"),
        }
    }

    #[test]
    fn parses_inspect_defaults() {
        let cli = parse(["dtoo", "inspect", "input.csv"]);

        match cli.command {
            Commands::Inspect(args) => {
                assert_eq!(args.path, PathBuf::from("input.csv"));
                assert_eq!(args.rows, 10);
                assert_eq!(args.delimiter, ',');
                assert_eq!(args.s3_region, None);
                assert_eq!(args.s3_profile, None);
                assert_eq!(args.gcs_project, None);
                assert_eq!(args.azure_account, None);
            }
            _ => panic!("expected inspect command"),
        }
    }

    #[test]
    fn parses_fingerprint() {
        let cli = parse(["dtoo", "fingerprint", "input.csv"]);

        match cli.command {
            Commands::Fingerprint(args) => {
                assert_eq!(args.path, PathBuf::from("input.csv"));
            }
            _ => panic!("expected fingerprint command"),
        }
    }

    #[test]
    fn rejects_multiple_input_sources() {
        let err = parse_err(["dtoo", "query", "input.csv", "--glob", "*.csv"]);
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn rejects_pipe_data_without_stdin_format() {
        let err = parse_err(["dtoo", "query", "--pipe", "data"]);
        assert!(err.to_string().contains("--stdin-format"));
    }

    #[test]
    fn allows_no_header_when_not_csv() {
        let cli = parse([
            "dtoo",
            "query",
            "input.csv",
            "--output-format",
            "parquet",
            "--no-header",
        ]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.output_format, OutputFormat::Parquet);
                assert!(args.no_header);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn rejects_limit_zero() {
        let err = parse_err(["dtoo", "query", "input.csv", "--limit", "0"]);
        assert!(err.to_string().contains("positive integer"));
    }

    #[test]
    fn rejects_encrypt_columns_without_encrypt_output_profile() {
        let err = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--encrypt-columns",
            "email,phone",
        ]);
        assert!(
            err.to_string()
                .contains("--encrypt-columns requires --encrypt-output-profile")
        );
    }

    #[test]
    fn rejects_empty_mask_value() {
        let err = parse_err(["dtoo", "query", "input.csv", "--mask", ""]);
        assert!(err.to_string().contains("--mask"));
    }

    #[test]
    fn rejects_invalid_ref_entry() {
        let err = parse_err(["dtoo", "query", "input.csv", "--ref", "badref"]);
        assert!(err.to_string().contains("NAME=PATH"));
    }

    #[test]
    fn rejects_reserved_ref_table_names() {
        let err_magic = parse_err(["dtoo", "query", "input.csv", "--ref", "_=regions.csv"]);
        assert!(err_magic.to_string().contains("reserved"));

        let err_internal = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--ref",
            "temp_results=regions.csv",
        ]);
        assert!(err_internal.to_string().contains("reserved"));
    }

    #[test]
    fn rejects_invalid_ref_table_identifier() {
        let err = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--ref",
            "bad-name=regions.csv",
        ]);
        assert!(err.to_string().contains("identifier"));
    }

    #[test]
    fn rejects_duplicate_ref_table_names() {
        let err = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--ref",
            "regions=a.csv",
            "--ref",
            "regions=b.csv",
        ]);
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn rejects_invalid_lineage_value() {
        let err = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--lineage",
            "record_id,unknown",
        ]);
        assert!(err.to_string().contains("--lineage"));
    }

    #[test]
    fn rejects_missing_config_file() {
        let err = parse_err(["dtoo", "query", "input.csv", "--config", "missing.yaml"]);
        assert!(err.to_string().contains("--config"));
    }

    #[test]
    fn rejects_invalid_yaml_config_file() {
        let path = temp_path("invalid-config", "yaml");
        fs::write(&path, "not: [valid").expect("write invalid config");

        let err = parse_err([
            "dtoo",
            "query",
            "input.csv",
            "--config",
            path.to_string_lossy().as_ref(),
        ]);

        assert!(err.to_string().contains("valid YAML"));

        fs::remove_file(path).ok();
    }

    #[test]
    fn applies_config_defaults_when_cli_values_absent() {
        let path = temp_path("query-config", "yaml");
        fs::write(
            &path,
            r#"
glob: "data/**/*.csv"
exclude:
  - "**/tmp_*"
where: "amount > 200"
ref:
  regions: ref/regions.csv
output: "results/out.parquet"
output_format: parquet
delimiter: "|"
fingerprint: true
verbose: true
count: true
"#,
        )
        .expect("write config");

        let cli = parse(["dtoo", "query", "--config", path.to_string_lossy().as_ref()]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.glob.as_deref(), Some("data/**/*.csv"));
                assert_eq!(args.exclude, vec!["**/tmp_*"]);
                assert_eq!(args.where_clause.as_deref(), Some("amount > 200"));
                assert_eq!(args.refs, vec!["regions=ref/regions.csv"]);
                assert_eq!(
                    args.output
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string()),
                    Some("results/out.parquet".to_string())
                );
                assert_eq!(args.output_format, OutputFormat::Parquet);
                assert_eq!(args.delimiter, '|');
                assert!(args.fingerprint);
                assert!(args.verbose);
                assert!(args.count);
            }
            _ => panic!("expected query command"),
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn cli_values_override_config_values() {
        let path = temp_path("query-config-override", "yaml");
        fs::write(
            &path,
            r#"
where: "amount > 200"
exclude:
  - "**/tmp_*"
output_format: parquet
count: false
verbose: false
"#,
        )
        .expect("write config");

        let cli = parse([
            "dtoo",
            "query",
            "--config",
            path.to_string_lossy().as_ref(),
            "--where",
            "amount > 500",
            "--exclude",
            "**/manual_*",
            "--output-format",
            "csv",
            "--count",
            "--verbose",
        ]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.where_clause.as_deref(), Some("amount > 500"));
                assert_eq!(args.exclude, vec!["**/manual_*"]);
                assert_eq!(args.output_format, OutputFormat::Csv);
                assert!(args.count);
                assert!(args.verbose);
            }
            _ => panic!("expected query command"),
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn config_type_mismatch_reports_field_name() {
        let path = temp_path("query-config-type", "yaml");
        fs::write(&path, "limit: nope\n").expect("write config");

        let err = parse_err(["dtoo", "query", "--config", path.to_string_lossy().as_ref()]);
        assert!(err.to_string().contains("limit"));

        fs::remove_file(path).ok();
    }

    #[test]
    fn positional_paths_override_config_glob() {
        let path = temp_path("query-config-paths-override", "yaml");
        fs::write(&path, "glob: \"data/**/*.csv\"\n").expect("write config");

        let cli = parse([
            "dtoo",
            "query",
            "input.csv",
            "--config",
            path.to_string_lossy().as_ref(),
        ]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.paths, vec!["input.csv"]);
                assert_eq!(args.glob, None);
            }
            _ => panic!("expected query command"),
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn parses_profile_detail_and_top_k() {
        let cli = parse([
            "dtoo",
            "profile",
            "input.csv",
            "--detail",
            "synth",
            "--top-k",
            "500",
        ]);
        match cli.command {
            Commands::Profile(args) => {
                assert_eq!(args.detail, ProfileDetail::Synth);
                assert_eq!(args.top_k, 500);
            }
            _ => panic!("expected profile command"),
        }
    }

    #[test]
    fn parses_query_profile_detail() {
        let cli = parse([
            "dtoo",
            "query",
            "input.csv",
            "--profile",
            "p.json",
            "--profile-detail",
            "synth",
        ]);
        match cli.command {
            Commands::Query(args) => {
                assert_eq!(args.profile_detail, ProfileDetail::Synth);
                assert_eq!(args.top_k, 1000);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn accepts_ref_and_lineage_values() {
        let cli = parse([
            "dtoo",
            "query",
            "input.csv",
            "--ref",
            "regions=ref/regions.csv",
            "--lineage",
            "batch_id,origin_file",
        ]);

        match cli.command {
            Commands::Query(args) => {
                let args = *args;
                assert_eq!(args.refs, vec!["regions=ref/regions.csv"]);
                assert_eq!(args.lineage.as_deref(), Some("batch_id,origin_file"));
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn synth_requires_spec_or_profile() {
        let err = parse_err(["dtoo", "synth"]);
        assert!(err.to_string().contains("--spec or --profile"));
    }

    #[test]
    fn synth_rejects_spec_and_profile_together() {
        let err = parse_err(["dtoo", "synth", "--spec", "s.yaml", "--profile", "p.json"]);
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn synth_profile_mode_requires_rows() {
        let err = parse_err(["dtoo", "synth", "--profile", "p.json"]);
        assert!(err.to_string().contains("--rows"));
    }

    #[test]
    fn synth_spec_mode_rejects_rows_and_output() {
        let err = parse_err(["dtoo", "synth", "--spec", "s.yaml", "--rows", "10"]);
        assert!(err.to_string().contains("--profile"));
    }

    #[test]
    fn synth_parses_valid_invocations() {
        let cli = parse([
            "dtoo",
            "synth",
            "--profile",
            "p.json",
            "--rows",
            "100",
            "--seed",
            "9",
        ]);
        match cli.command {
            Commands::Synth(args) => {
                assert_eq!(args.rows, Some(100));
                assert_eq!(args.seed, Some(9));
            }
            _ => panic!("expected synth command"),
        }
        parse([
            "dtoo",
            "synth",
            "--spec",
            "s.yaml",
            "--dry-run",
            "--verbose",
        ]);
    }

    fn parse<const N: usize>(args: [&str; N]) -> Cli {
        Cli::parse_and_validate_from(args).expect("expected parse success")
    }

    fn parse_err<const N: usize>(args: [&str; N]) -> clap::Error {
        Cli::parse_and_validate_from(args).expect_err("expected parse error")
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}.{ext}"))
    }
}
