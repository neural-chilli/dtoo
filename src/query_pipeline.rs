use chrono::Utc;
use polars::prelude::*;
use std::time::Instant;
use uuid::Uuid;

use crate::{
    cli::{CompressMethod, OnErrorMode, OutputFormat, PipeMode, QueryArgs, StdinFormat},
    crypto,
    error::DtooError,
    file_resolution::{FileFormat, FileResolver, FileResolverConfig, ResolutionReport},
    fingerprint::fingerprint_file,
    lineage::{LineageContext, LineageManager},
    manifest::{
        Manifest, ManifestFileDetail, ManifestFiles, ManifestOutput, ManifestTiming,
        ManifestWriter, build_command, duration_seconds,
    },
    masking::mask_dataframe,
    output_writer::{OutputWriter, OutputWriterConfig},
    polars_engine::PolarsEngine,
    profiler::{ProfileOptions, Profiler},
    reference_tables::{load_reference_lazyframes, parse_reference_tables},
    schema::{SchemaManager, SchemaMode, coerce_to_schema},
    types::{CompressionCodec, ExportFormat, InputFormat},
};

/// Coordinates execution of the `dtoo query` pipeline.
pub struct QueryPipeline;

struct VerboseLogger {
    enabled: bool,
    start: Instant,
}

impl VerboseLogger {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            start: Instant::now(),
        }
    }

    fn log(&self, message: impl AsRef<str>) {
        if !self.enabled {
            return;
        }
        eprintln!("{} {}", self.timestamp(), message.as_ref());
    }

    fn timestamp(&self) -> String {
        let elapsed = self.start.elapsed();
        let mins = elapsed.as_secs() / 60;
        let secs = elapsed.as_secs() % 60;
        let tenths = elapsed.subsec_millis() / 100;
        format!("[{mins:02}:{secs:02}.{tenths}]")
    }

    fn elapsed_seconds_1dp(&self) -> String {
        let elapsed = self.start.elapsed();
        let secs = elapsed.as_secs();
        let tenths = elapsed.subsec_millis() / 100;
        format!("{secs}.{tenths}s")
    }
}

struct TempInputCleanup {
    paths: Vec<String>,
}

impl TempInputCleanup {
    fn from_files(files: &[crate::file_resolution::ResolvedFile]) -> Self {
        Self {
            paths: files
                .iter()
                .filter(|f| f.is_temp)
                .map(|f| f.path.clone())
                .collect(),
        }
    }
}

impl Drop for TempInputCleanup {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Per-file processing status for manifest and summary reporting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileResult {
    pub path: String,
    pub rows: usize,
    pub skipped: bool,
    pub message: Option<String>,
}

/// Basic timing metadata for pipeline execution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Timing {
    pub started_at_unix_ms: u128,
    pub finished_at_unix_ms: u128,
}

/// Summary output of one query pipeline execution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PipelineResult {
    pub files_total: usize,
    pub files_processed: usize,
    pub files_skipped: usize,
    pub file_details: Vec<FileResult>,
    pub rows_output: usize,
    pub fingerprint: Option<String>,
    pub batch_id: Option<String>,
    pub batch_hash: Option<String>,
    pub batch_timestamp: Option<String>,
    pub output_path: Option<String>,
    pub timing: Timing,
}

impl QueryPipeline {
    /// Run the query pipeline from parsed CLI arguments.
    pub fn run(args: &QueryArgs) -> Result<(), DtooError> {
        let mut runner = QueryPipelineRunner::new(args);
        let result = runner.execute();
        write_manifest_if_requested(
            args,
            &runner.summary,
            runner.started_at,
            result.as_ref().err(),
        );
        result
    }
}

struct QueryPipelineRunner<'a> {
    args: &'a QueryArgs,
    logger: VerboseLogger,
    started_at: chrono::DateTime<Utc>,
    summary: PipelineResult,
    decrypted_columns: Vec<String>,
    profile_allows_plaintext: bool,
}

impl<'a> QueryPipelineRunner<'a> {
    fn new(args: &'a QueryArgs) -> Self {
        let started_at = Utc::now();
        Self {
            args,
            logger: VerboseLogger::new(args.verbose),
            started_at,
            summary: PipelineResult {
                batch_id: Some(Uuid::new_v4().to_string()),
                batch_hash: Some(Uuid::new_v4().to_string()),
                batch_timestamp: Some(started_at.to_rfc3339()),
                ..PipelineResult::default()
            },
            decrypted_columns: Vec::new(),
            profile_allows_plaintext: false,
        }
    }

    fn execute(&mut self) -> Result<(), DtooError> {
        self.logger.log("Starting dtoo query");

        let resolution = self.resolve_inputs()?;
        self.record_resolution_meta(resolution.files.len(), resolution.excluded_count);
        let files = resolution.files;

        let _temp_cleanup = TempInputCleanup::from_files(&files);
        if self.args.dry_run {
            self.emit_dry_run(&files);
            return Ok(());
        }

        self.ensure_files_present(&files)?;
        let engine = PolarsEngine::new();
        self.log_schema_mode();
        let refs = self.load_reference_tables(&engine)?;

        let schema_manager = SchemaManager::from_schema_path(self.args.schema.as_deref())?;
        let mut lineage_manager = self.build_lineage_manager(&files)?;

        let frames = self.process_files(&engine, &mut lineage_manager, &refs, &files)?;

        // Union-by-name accumulate, then optional explicit schema coercion.
        let mut acc = engine.concat_by_name(frames)?;
        if let SchemaMode::Explicit(schema) = schema_manager.mode() {
            acc = coerce_to_schema(acc, &schema.columns)?;
        }
        let mut df = engine.collect(acc)?;

        // Crypto decrypt BEFORE post-sql (matches the original ordering).
        df = self.apply_crypto_stage(df)?;
        df = self.apply_post_sql(&engine, &refs, df)?;
        df = self.apply_masking(df)?;
        if let Some(lineage) = &self.args.lineage {
            self.logger
                .log(format!("Adding lineage columns: [{lineage}]"));
        }
        df = lineage_manager.apply_to_dataframe(df)?;
        if let Some(limit) = self.args.limit {
            self.logger
                .log(format!("Applying limit: {}", format_count(limit)));
            df = df.head(Some(limit));
        }

        let row_count = self.compute_row_count_and_validate(df.height())?;
        self.handle_count_output(row_count);
        df = self.apply_output_crypto_if_needed(df)?;
        let written_output_path = self.write_output_if_needed(&engine, &df)?;
        self.write_profile_if_requested(&df)?;
        self.fingerprint_if_requested(written_output_path)?;

        self.raise_partial_failure_if_needed(files.len())?;

        self.logger.log(format!(
            "Done. {} rows written in {}",
            format_count(row_count),
            self.logger.elapsed_seconds_1dp()
        ));
        Ok(())
    }

    fn resolve_inputs(&self) -> Result<ResolutionReport, DtooError> {
        if self.args.dry_run {
            resolve_files_for_dry_run(self.args)
        } else {
            resolve_files(self.args)
        }
    }

    fn record_resolution_meta(&mut self, file_count: usize, excluded_count: usize) {
        self.summary.files_total = file_count;
        self.logger.log(format!(
            "Resolved {} files from {}",
            file_count,
            describe_input_source(self.args)
        ));
        if excluded_count > 0 {
            self.logger
                .log(format!("Excluded {} files", format_count(excluded_count)));
        }
    }

    fn emit_dry_run(&self, files: &[crate::file_resolution::ResolvedFile]) {
        eprintln!("{}", build_dry_run_plan(self.args, files));
    }

    fn ensure_files_present(
        &self,
        files: &[crate::file_resolution::ResolvedFile],
    ) -> Result<(), DtooError> {
        if files.is_empty() {
            return Err(DtooError::Config {
                message: "no input files resolved".to_string(),
            });
        }
        Ok(())
    }

    fn log_schema_mode(&self) {
        if let Some(schema_path) = &self.args.schema {
            self.logger
                .log(format!("Using explicit schema: {}", schema_path.display()));
        } else {
            self.logger.log("Auto-detecting schema");
        }
    }

    fn load_reference_tables(
        &self,
        engine: &PolarsEngine,
    ) -> Result<Vec<(String, LazyFrame)>, DtooError> {
        let parsed = parse_reference_tables(
            &self.args.refs,
            self.args.delimiter,
            self.args.sheet.as_deref(),
        )?;
        let loaded = load_reference_lazyframes(engine, &parsed)?;
        for (name, _) in &loaded {
            self.logger.log(format!("Loading ref table: {name}"));
        }
        Ok(loaded)
    }

    fn build_lineage_manager(
        &mut self,
        files: &[crate::file_resolution::ResolvedFile],
    ) -> Result<LineageManager, DtooError> {
        let lineage_manager = LineageManager::new(
            self.args.lineage.as_deref(),
            LineageContext {
                where_clause: self.args.where_clause.clone(),
                filter_sql: self.args.filter_sql.clone(),
                post_sql: self.args.post_sql.clone(),
                files: files.iter().map(|f| f.path.clone()).collect(),
                schema_path: self
                    .args
                    .schema
                    .as_ref()
                    .map(|path| path.to_string_lossy().to_string()),
            },
        )?;
        self.summary.batch_id = Some(lineage_manager.batch_id().to_string());
        self.summary.batch_hash = Some(lineage_manager.batch_hash().to_string());
        self.summary.batch_timestamp = Some(lineage_manager.batch_timestamp().to_rfc3339());
        Ok(lineage_manager)
    }

    fn process_files(
        &mut self,
        engine: &PolarsEngine,
        lineage_manager: &mut LineageManager,
        refs: &[(String, LazyFrame)],
        files: &[crate::file_resolution::ResolvedFile],
    ) -> Result<Vec<LazyFrame>, DtooError> {
        let mut processed = 0usize;
        let mut skipped = 0usize;
        let track_origin = lineage_manager.requires_origin_tracking();
        let mut frames: Vec<LazyFrame> = Vec::with_capacity(files.len());

        for (idx, file) in files.iter().enumerate() {
            let input_format = to_input_format(file.format.clone(), file.sheet.clone());
            let result = (|| -> Result<(LazyFrame, usize), DtooError> {
                let lf = engine.scan(&file.path, &input_format)?;
                let mut lf = per_file_filter(
                    engine,
                    lf,
                    refs,
                    self.args.where_clause.as_deref(),
                    self.args.filter_sql.as_deref(),
                )?;
                if track_origin {
                    lf = lf.with_column(lit(file.path.as_str()).alias("_origin_file"));
                }
                let rows = engine.row_count(lf.clone())?;
                Ok((lf, rows))
            })();

            match result {
                Ok((lf, rows_matched)) => {
                    frames.push(lf);
                    processed += 1;
                    self.summary.file_details.push(FileResult {
                        path: file.path.clone(),
                        rows: rows_matched,
                        skipped: false,
                        message: None,
                    });
                    self.logger.log(format!(
                        "[{}/{}] {} — {} rows matched",
                        idx + 1,
                        files.len(),
                        file.path,
                        format_count(rows_matched)
                    ));
                }
                Err(err) => {
                    if self.args.on_error == OnErrorMode::Fail {
                        return Err(err);
                    }
                    skipped += 1;
                    self.summary.file_details.push(FileResult {
                        path: file.path.clone(),
                        rows: 0,
                        skipped: true,
                        message: Some(err.to_string()),
                    });
                    self.logger.log(format!(
                        "[{}/{}] {} — SKIPPED ({})",
                        idx + 1,
                        files.len(),
                        file.path,
                        err
                    ));
                    eprintln!("Warning: Skipping {}: {err}", file.path);
                }
            }
        }

        self.summary.files_processed = processed;
        self.summary.files_skipped = skipped;
        if processed == 0 {
            return Err(DtooError::PartialFailure {
                processed,
                total: files.len(),
                skipped,
            });
        }
        Ok(frames)
    }

    fn apply_post_sql(
        &self,
        engine: &PolarsEngine,
        refs: &[(String, LazyFrame)],
        df: DataFrame,
    ) -> Result<DataFrame, DtooError> {
        let Some(post_sql) = &self.args.post_sql else {
            return Ok(df);
        };
        self.logger.log("Applying post-sql...");
        let out = engine.collect(engine.run_sql(df.lazy(), refs, post_sql)?)?;
        self.logger.log(format!(
            "Post-sql complete: {} rows",
            format_count(out.height())
        ));
        Ok(out)
    }

    fn apply_crypto_stage(&mut self, df: DataFrame) -> Result<DataFrame, DtooError> {
        let Some(profile_name) = self.args.crypto_profile.as_deref() else {
            return Ok(df);
        };

        let profile = crypto::resolve_profile(
            profile_name,
            self.args.crypto_profiles_file.as_deref(),
            self.args.config.as_deref(),
        )?;
        let (df, result) = crypto::decrypt_dataframe(df, &profile)?;
        self.decrypted_columns = result.decrypted_columns;
        self.profile_allows_plaintext = profile.output.allow_plaintext;
        if !self.decrypted_columns.is_empty() {
            self.logger.log(format!(
                "Crypto decrypt applied to columns: {}",
                self.decrypted_columns.join(", ")
            ));
        }
        Ok(df)
    }

    fn apply_output_crypto_if_needed(&mut self, df: DataFrame) -> Result<DataFrame, DtooError> {
        let has_output_encrypt = self.args.encrypt_output_profile.is_some();
        crypto::enforce_output_safety(
            &self.decrypted_columns,
            self.args.allow_plaintext_pii || self.profile_allows_plaintext,
            has_output_encrypt,
        )?;

        let Some(profile_name) = self.args.encrypt_output_profile.as_deref() else {
            return Ok(df);
        };

        let profile = crypto::resolve_profile(
            profile_name,
            self.args.crypto_profiles_file.as_deref(),
            self.args.config.as_deref(),
        )?;

        let columns = if let Some(raw) = &self.args.encrypt_columns {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        } else {
            self.decrypted_columns.clone()
        };

        let df = crypto::encrypt_dataframe(df, &profile, &columns)?;
        if !columns.is_empty() {
            self.logger.log(format!(
                "Crypto encrypt applied to columns: {}",
                columns.join(", ")
            ));
        }

        Ok(df)
    }

    fn apply_masking(&self, df: DataFrame) -> Result<DataFrame, DtooError> {
        if let Some(mask) = &self.args.mask {
            self.logger.log(format!("Applying masking: [{mask}]"));
        }
        let columns = parse_mask_columns(self.args.mask.as_deref());
        mask_dataframe(df, &columns, &self.args.mask_salt)
    }

    fn compute_row_count_and_validate(&mut self, row_count: usize) -> Result<usize, DtooError> {
        self.summary.rows_output = row_count;
        self.logger.log(format!(
            "Accumulation complete: {} rows",
            format_count(row_count)
        ));
        if let Some(expected_min) = self.args.expect_at_least
            && row_count < expected_min
        {
            return Err(DtooError::ExpectAtLeast {
                expected: expected_min,
                actual: row_count,
            });
        }
        if let Some(expected_min) = self.args.expect_at_least {
            self.logger.log(format!(
                "Validation: {} rows (expected at least {})",
                format_count(row_count),
                format_count(expected_min)
            ));
        }
        Ok(row_count)
    }

    fn handle_count_output(&self, row_count: usize) {
        if self.args.count {
            self.logger
                .log(format!("Row count: {}", format_count(row_count)));
            println!("{row_count}");
        }
    }

    fn write_output_if_needed(
        &mut self,
        engine: &PolarsEngine,
        df: &DataFrame,
    ) -> Result<Option<std::path::PathBuf>, DtooError> {
        let should_write_output = self.args.output.is_some() || !self.args.count;
        if !should_write_output {
            return Ok(None);
        }

        self.logger
            .log(format!("Writing output: {}", summarize_output(self.args)));
        let writer = OutputWriter::new(OutputWriterConfig {
            output: self.args.output.clone(),
            format: to_export_format(self.args.output_format),
            header: !self.args.no_header,
            delimiter: self.args.delimiter,
            compression: self.args.compress.map(to_compression_codec),
        });
        let written_output_path = writer.write_and_get_destination(engine, df.clone())?;
        self.summary.output_path = written_output_path
            .as_ref()
            .map(|path| path.display().to_string());
        Ok(written_output_path)
    }

    fn write_profile_if_requested(&self, df: &DataFrame) -> Result<(), DtooError> {
        let Some(profile_path) = &self.args.profile else {
            return Ok(());
        };

        if self.args.count && self.args.output.is_none() {
            eprintln!("Warning: --profile is skipped when using --count without --output");
            return Ok(());
        }

        self.logger.log(format!(
            "Writing profile: {} ({})",
            profile_path.display(),
            profile_format_to_str(self.args.profile_format)
        ));
        Profiler::generate(
            df,
            &ProfileOptions {
                path: profile_path.clone(),
                format: self.args.profile_format,
                sample_percentage: self.args.profile_sample,
                detail: crate::cli::ProfileDetail::Standard,
                top_k: 1000,
            },
        )
    }

    fn fingerprint_if_requested(
        &mut self,
        written_output_path: Option<std::path::PathBuf>,
    ) -> Result<(), DtooError> {
        if !self.args.fingerprint {
            return Ok(());
        }

        if let Some(path) = written_output_path {
            let hash = fingerprint_file(&path)?;
            self.summary.fingerprint = Some(format!("sha256:{hash}"));
            self.logger.log(format!("Fingerprint: sha256:{hash}"));
            eprintln!("{hash}  {}", path.display());
            return Ok(());
        }

        eprintln!("Warning: --fingerprint requires --output (cannot fingerprint stdout stream)");
        Ok(())
    }

    fn raise_partial_failure_if_needed(&self, total_files: usize) -> Result<(), DtooError> {
        if self.summary.files_skipped > 0 {
            return Err(DtooError::PartialFailure {
                processed: self.summary.files_processed,
                total: total_files,
                skipped: self.summary.files_skipped,
            });
        }
        Ok(())
    }
}

fn resolve_files(args: &QueryArgs) -> Result<ResolutionReport, DtooError> {
    let resolver = FileResolver::new(FileResolverConfig {
        glob: args.glob.clone(),
        paths: args.paths.clone(),
        pipe_mode: args.pipe,
        stdin_format: args.stdin_format,
        sheet: args.sheet.clone(),
        exclude: args.exclude.clone(),
        delimiter: args.delimiter,
        on_error: args.on_error,
        s3_region: args.s3_region.clone(),
        s3_profile: args.s3_profile.clone(),
        gcs_project: args.gcs_project.clone(),
        azure_account: args.azure_account.clone(),
    });
    resolver.resolve_report()
}

fn resolve_files_for_dry_run(args: &QueryArgs) -> Result<ResolutionReport, DtooError> {
    if args.pipe == Some(PipeMode::Data) {
        return Ok(ResolutionReport {
            files: vec![crate::file_resolution::ResolvedFile {
                path: "(stdin data)".to_string(),
                format: stdin_format_to_file_format(args.stdin_format, args.delimiter),
                sheet: None,
                is_temp: false,
            }],
            excluded_count: 0,
        });
    }

    match resolve_files(args) {
        Ok(resolution) => Ok(resolution),
        Err(DtooError::NoFilesMatched { .. }) => Ok(ResolutionReport {
            files: Vec::new(),
            excluded_count: 0,
        }),
        Err(DtooError::Config { message })
            if args.pipe == Some(PipeMode::File)
                && message.contains("--pipe file received no file paths from stdin") =>
        {
            Ok(ResolutionReport {
                files: Vec::new(),
                excluded_count: 0,
            })
        }
        Err(err) => Err(err),
    }
}

fn build_dry_run_plan(args: &QueryArgs, files: &[crate::file_resolution::ResolvedFile]) -> String {
    let mut lines = vec!["Dry Run".to_string(), "=======".to_string()];
    lines.push(format!("Files matched: {}", files.len()));

    if args.pipe == Some(PipeMode::Data) {
        lines.push("  (stdin data)".to_string());
    } else {
        for file in files.iter().take(20) {
            lines.push(format!("  {}", file.path));
        }
        if files.len() > 20 {
            lines.push(format!("  (+{} more)", files.len() - 20));
        }
    }

    lines.push(format!("Format: {}", summarize_formats(files)));
    lines.push(format!(
        "Schema: {}",
        args.schema.as_ref().map_or_else(
            || "auto-detect (union-by-name)".to_string(),
            |path| path.display().to_string()
        )
    ));
    lines.push(format!(
        "Where: {}",
        args.where_clause.as_deref().unwrap_or("(none)")
    ));
    lines.push(format!(
        "Filter SQL: {}",
        args.filter_sql.as_deref().unwrap_or("(none)")
    ));
    lines.push(format!(
        "Post SQL: {}",
        args.post_sql.as_deref().unwrap_or("(none)")
    ));
    if args.refs.is_empty() {
        lines.push("Reference tables: (none)".to_string());
    } else {
        lines.push("Reference tables:".to_string());
        for reference in &args.refs {
            if let Some((name, path)) = reference.split_once('=') {
                lines.push(format!("  {} = {}", name.trim(), path.trim()));
            } else {
                lines.push(format!("  {reference}"));
            }
        }
    }
    lines.push(format!("Output: {}", summarize_output(args)));
    lines.push(format!(
        "Compress: {}",
        args.compress.map(compress_to_str).unwrap_or("(none)")
    ));
    lines.push(format!(
        "Lineage: {}",
        args.lineage.as_deref().unwrap_or("(none)")
    ));
    lines.push(format!(
        "Masking: {}",
        args.mask.as_deref().unwrap_or("(none)")
    ));
    lines.push(format!("Profile: {}", summarize_profile(args)));
    lines.push(format!(
        "Fingerprint: {}",
        if args.fingerprint { "yes" } else { "no" }
    ));
    lines.push(format!(
        "Manifest: {}",
        args.manifest
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "(none)".to_string())
    ));
    lines.push(format!(
        "Expect at least: {}",
        args.expect_at_least
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(none)".to_string())
    ));
    lines.push(format!("On error: {}", on_error_to_str(args.on_error)));
    lines.join("\n")
}

fn summarize_formats(files: &[crate::file_resolution::ResolvedFile]) -> String {
    if files.is_empty() {
        return "(none)".to_string();
    }

    let mut labels = Vec::new();
    for file in files {
        let label = format_family_label(&file.format);
        if !labels.iter().any(|known| known == label) {
            labels.push(label.to_string());
        }
    }

    if labels.len() > 1 {
        return format!("Mixed ({})", labels.join(", "));
    }

    describe_format(&files[0].format)
}

fn format_family_label(format: &FileFormat) -> &'static str {
    match format {
        FileFormat::Csv { .. } => "CSV",
        FileFormat::Parquet => "Parquet",
        FileFormat::Ndjson => "NDJSON",
        FileFormat::Excel { .. } => "Excel",
    }
}

fn describe_format(format: &FileFormat) -> String {
    match format {
        FileFormat::Csv { delimiter } => format!("CSV (delimiter: {delimiter})"),
        FileFormat::Parquet => "Parquet".to_string(),
        FileFormat::Ndjson => "NDJSON".to_string(),
        FileFormat::Excel { sheet } => sheet.as_ref().map_or_else(
            || "Excel".to_string(),
            |sheet| format!("Excel (sheet: {sheet})"),
        ),
    }
}

fn stdin_format_to_file_format(format: Option<StdinFormat>, delimiter: char) -> FileFormat {
    match format {
        Some(StdinFormat::Csv) => FileFormat::Csv { delimiter },
        Some(StdinFormat::Parquet) => FileFormat::Parquet,
        Some(StdinFormat::Ndjson) | None => FileFormat::Ndjson,
    }
}

fn summarize_output(args: &QueryArgs) -> String {
    if let Some(path) = &args.output {
        return path.display().to_string();
    }
    format!("stdout ({})", output_format_to_str(args.output_format))
}

fn summarize_profile(args: &QueryArgs) -> String {
    args.profile.as_ref().map_or_else(
        || "(none)".to_string(),
        |path| {
            format!(
                "{} ({})",
                path.display(),
                profile_format_to_str(args.profile_format)
            )
        },
    )
}

fn output_format_to_str(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::Csv => "csv",
        OutputFormat::Parquet => "parquet",
        OutputFormat::Ndjson => "ndjson",
    }
}

fn profile_format_to_str(format: crate::cli::ProfileFormat) -> &'static str {
    match format {
        crate::cli::ProfileFormat::Json => "json",
        crate::cli::ProfileFormat::Csv => "csv",
        crate::cli::ProfileFormat::Html => "html",
    }
}

fn compress_to_str(compress: CompressMethod) -> &'static str {
    match compress {
        CompressMethod::Gzip => "gzip",
        CompressMethod::Zstd => "zstd",
    }
}

fn on_error_to_str(on_error: OnErrorMode) -> &'static str {
    match on_error {
        OnErrorMode::Skip => "skip",
        OnErrorMode::Fail => "fail",
    }
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (digits.len() - idx).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn describe_input_source(args: &QueryArgs) -> String {
    if let Some(glob) = &args.glob {
        return format!("glob \"{glob}\"");
    }
    if let Some(mode) = args.pipe {
        return match mode {
            PipeMode::File => "--pipe file".to_string(),
            PipeMode::Data => "--pipe data".to_string(),
        };
    }
    if args.paths.is_empty() {
        return "explicit paths".to_string();
    }
    "explicit paths".to_string()
}

fn write_manifest_if_requested(
    args: &QueryArgs,
    summary: &PipelineResult,
    started_at: chrono::DateTime<Utc>,
    error: Option<&DtooError>,
) {
    let Some(path) = &args.manifest else {
        return;
    };

    let finished_at = Utc::now();
    let manifest = Manifest {
        batch_id: summary.batch_id.clone().unwrap_or_default(),
        batch_hash: summary.batch_hash.clone().unwrap_or_default(),
        batch_timestamp: summary
            .batch_timestamp
            .clone()
            .unwrap_or_else(|| started_at.to_rfc3339()),
        dtoo_version: env!("CARGO_PKG_VERSION").to_string(),
        command: build_command(args),
        files: ManifestFiles {
            total: summary.files_total,
            processed: summary.files_processed,
            skipped: summary.files_skipped,
            details: summary
                .file_details
                .iter()
                .map(|detail| ManifestFileDetail {
                    path: detail.path.clone(),
                    rows_matched: detail.rows,
                    status: if detail.skipped {
                        "skipped".to_string()
                    } else {
                        "ok".to_string()
                    },
                    error: detail.message.clone(),
                })
                .collect(),
        },
        output: ManifestOutput {
            path: summary
                .output_path
                .clone()
                .or_else(|| args.output.as_ref().map(|p| p.display().to_string())),
            format: output_format_to_str(args.output_format).to_string(),
            rows: summary.rows_output,
            fingerprint: summary.fingerprint.clone(),
            compressed: args.compress.is_some(),
        },
        timing: ManifestTiming {
            started: started_at.to_rfc3339(),
            finished: finished_at.to_rfc3339(),
            duration_seconds: duration_seconds(started_at, finished_at),
        },
        error: error.map(ToString::to_string),
    };

    if let Err(err) = ManifestWriter::write(path, &manifest) {
        eprintln!(
            "Warning: failed to write manifest {}: {err}",
            path.display()
        );
    }
}

/// Apply the optional `--where` and `--filter-sql` stages to a single file's
/// LazyFrame, preserving the original where-before-filter ordering.
///
/// `--where` runs first (as `SELECT * FROM _ WHERE …` with no refs available),
/// then `--filter-sql` runs against the result with reference tables registered.
fn per_file_filter(
    engine: &PolarsEngine,
    lf: LazyFrame,
    refs: &[(String, LazyFrame)],
    where_clause: Option<&str>,
    filter_sql: Option<&str>,
) -> Result<LazyFrame, DtooError> {
    match (where_clause, filter_sql) {
        (None, None) => Ok(lf),
        (Some(where_sql), None) => {
            engine.run_sql(lf, &[], &format!("SELECT * FROM _ WHERE {where_sql}"))
        }
        (None, Some(filter)) => engine.run_sql(lf, refs, filter),
        (Some(where_sql), Some(filter)) => {
            let pre = engine.run_sql(lf, &[], &format!("SELECT * FROM _ WHERE {where_sql}"))?;
            engine.run_sql(pre, refs, filter)
        }
    }
}

/// Parse the `--mask` comma-separated column list, mirroring the masking engine's
/// case-insensitive de-duplication so identical behavior is preserved.
fn parse_mask_columns(mask: Option<&str>) -> Vec<String> {
    let Some(mask) = mask else {
        return Vec::new();
    };
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::new();
    for column in mask.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        if seen.insert(column.to_ascii_lowercase()) {
            columns.push(column.to_string());
        }
    }
    columns
}

fn to_input_format(format: FileFormat, sheet: Option<String>) -> InputFormat {
    match format {
        FileFormat::Parquet => InputFormat::Parquet,
        FileFormat::Csv { delimiter } => InputFormat::Csv { delimiter },
        FileFormat::Ndjson => InputFormat::Ndjson,
        FileFormat::Excel { .. } => InputFormat::Excel { sheet },
    }
}

fn to_export_format(format: OutputFormat) -> ExportFormat {
    match format {
        OutputFormat::Csv => ExportFormat::Csv,
        OutputFormat::Parquet => ExportFormat::Parquet,
        OutputFormat::Ndjson => ExportFormat::Ndjson,
    }
}

fn to_compression_codec(codec: CompressMethod) -> CompressionCodec {
    match codec {
        CompressMethod::Gzip => CompressionCodec::Gzip,
        CompressMethod::Zstd => CompressionCodec::Zstd,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::cli::ProfileFormat;

    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn pipeline_runs_with_reference_join_and_writes_output() {
        let input_csv = temp_path("pipeline-input", "csv");
        fs::write(&input_csv, "id,region_id,value\n1,10,100\n").expect("write input");
        let ref_csv = temp_path("pipeline-ref", "csv");
        fs::write(&ref_csv, "id,region_name\n10,EMEA\n").expect("write refs");
        let output_csv = temp_path("pipeline-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: Some(
                "SELECT _.*, r.region_name FROM _ JOIN regions r ON _.region_id = r.id".to_string(),
            ),
            refs: vec![format!("regions={}", ref_csv.to_string_lossy())],
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");

        let contents = fs::read_to_string(&output_csv).expect("read output");
        assert!(contents.contains("region_name"));
        assert!(contents.contains("EMEA"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(ref_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_applies_limit_before_export() {
        let input_csv = temp_path("pipeline-limit-input", "csv");
        fs::write(&input_csv, "id,value\n1,100\n2,200\n").expect("write input");
        let output_csv = temp_path("pipeline-limit-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: Some(1),
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let contents = fs::read_to_string(&output_csv).expect("read output");
        let data_rows = contents.lines().skip(1).count();
        assert_eq!(data_rows, 1);

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_applies_where_before_filter_sql() {
        let input_csv = temp_path("pipeline-where-filter-input", "csv");
        fs::write(
            &input_csv,
            "id,value,note\n1,10,keep\n1,20,drop\n2,10,drop\n",
        )
        .expect("write input");
        let output_csv = temp_path("pipeline-where-filter-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: Some("id = 1".to_string()),
            filter_sql: Some("SELECT * FROM _ WHERE value = 10".to_string()),
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let contents = fs::read_to_string(&output_csv).expect("read output");
        assert!(contents.contains("1,10,keep"));
        assert!(!contents.contains("1,20,drop"));
        assert!(!contents.contains("2,10,drop"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_adds_requested_lineage_columns() {
        let input_csv = temp_path("pipeline-lineage-input", "csv");
        fs::write(&input_csv, "id,value\n1,100\n").expect("write input");
        let output_csv = temp_path("pipeline-lineage-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: Some("batch_id,origin_file".to_string()),
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let contents = fs::read_to_string(&output_csv).expect("read output");
        let header = contents.lines().next().expect("csv header");
        assert!(header.contains("batch_id"));
        assert!(header.contains("origin_file"));
        assert!(contents.contains(input_csv.to_string_lossy().as_ref()));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_lineage_all_includes_record_id() {
        let input_csv = temp_path("pipeline-lineage-all-input", "csv");
        fs::write(&input_csv, "id,value\n1,100\n").expect("write input");
        let output_csv = temp_path("pipeline-lineage-all-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: Some("all".to_string()),
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let contents = fs::read_to_string(&output_csv).expect("read output");
        let header = contents.lines().next().expect("csv header");
        assert!(header.contains("record_id"));
        assert!(header.contains("batch_timestamp"));
        assert!(header.contains("batch_hash"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_applies_masking_before_output() {
        let input_csv = temp_path("pipeline-masking-input", "csv");
        fs::write(&input_csv, "id,email\n1,a@example.com\n2,a@example.com\n").expect("write input");
        let output_csv = temp_path("pipeline-masking-output", "csv");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: Some("email".to_string()),
            mask_salt: "salt-1".to_string(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let contents = fs::read_to_string(&output_csv).expect("read output");
        assert!(!contents.contains("a@example.com"));
        let mut lines = contents.lines().skip(1);
        let first = lines.next().expect("first row");
        let second = lines.next().expect("second row");
        let first_hash = first.split(',').nth(1).expect("hash 1");
        let second_hash = second.split(',').nth(1).expect("hash 2");
        assert_eq!(first_hash, second_hash);

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
    }

    #[test]
    fn pipeline_writes_profile_report() {
        let input_csv = temp_path("pipeline-profile-input", "csv");
        fs::write(&input_csv, "id,email\n1,a@example.com\n2,b@example.com\n").expect("write input");
        let output_csv = temp_path("pipeline-profile-output", "csv");
        let profile_json = temp_path("pipeline-profile-report", "json");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: Some(profile_json.clone()),
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let profile = fs::read_to_string(&profile_json).expect("read profile");
        assert!(profile.contains("\"row_count\": 2"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
        let _ = fs::remove_file(profile_json);
    }

    #[test]
    fn pipeline_fingerprint_requires_output() {
        let input_csv = temp_path("pipeline-fp-input", "csv");
        fs::write(&input_csv, "id\n1\n").expect("write input");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: None,
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: true,
            expect_at_least: None,
            fingerprint: true,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let _ = fs::remove_file(input_csv);
    }

    #[test]
    fn pipeline_writes_manifest_on_success() {
        let input_csv = temp_path("pipeline-manifest-input", "csv");
        fs::write(&input_csv, "id,value\n1,100\n").expect("write input");
        let output_csv = temp_path("pipeline-manifest-output", "csv");
        let manifest_yaml = temp_path("pipeline-manifest", "yaml");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: Some(manifest_yaml.clone()),
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        QueryPipeline::run(&args).expect("pipeline should run");
        let manifest = fs::read_to_string(&manifest_yaml).expect("read manifest");
        assert!(manifest.contains("batch_id:"));
        assert!(manifest.contains("dtoo_version:"));
        assert!(manifest.contains("files:"));
        assert!(manifest.contains("output:"));
        assert!(manifest.contains("rows: 1"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
        let _ = fs::remove_file(manifest_yaml);
    }

    #[test]
    fn pipeline_writes_manifest_on_expect_at_least_failure() {
        let input_csv = temp_path("pipeline-manifest-fail-input", "csv");
        fs::write(&input_csv, "id,value\n1,100\n").expect("write input");
        let output_csv = temp_path("pipeline-manifest-fail-output", "csv");
        let manifest_yaml = temp_path("pipeline-manifest-fail", "yaml");

        let args = QueryArgs {
            paths: vec![input_csv.to_string_lossy().into_owned()],
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: Some(output_csv.clone()),
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: Some(10),
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: Some(manifest_yaml.clone()),
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        };

        let err = QueryPipeline::run(&args).expect_err("expect-at-least should fail");
        assert!(matches!(err, DtooError::ExpectAtLeast { .. }));

        let manifest = fs::read_to_string(&manifest_yaml).expect("read manifest");
        assert!(manifest.contains("error:"));
        assert!(manifest.contains("Expected at least"));
        assert!(manifest.contains("rows: 1"));

        let _ = fs::remove_file(input_csv);
        let _ = fs::remove_file(output_csv);
        let _ = fs::remove_file(manifest_yaml);
    }

    #[test]
    fn dry_run_plan_truncates_file_list_and_shows_summary_fields() {
        let args = base_query_args();
        let files = (0..22)
            .map(|idx| crate::file_resolution::ResolvedFile {
                path: format!("/tmp/input-{idx}.csv"),
                format: FileFormat::Csv { delimiter: ',' },
                sheet: None,
                is_temp: false,
            })
            .collect::<Vec<_>>();

        let plan = build_dry_run_plan(&args, &files);
        assert!(plan.contains("Dry Run"));
        assert!(plan.contains("Files matched: 22"));
        assert!(plan.contains("/tmp/input-0.csv"));
        assert!(plan.contains("(+2 more)"));
        assert!(plan.contains("Format: CSV (delimiter: ,)"));
        assert!(plan.contains("Output: stdout (csv)"));
        assert!(plan.contains("On error: fail"));
    }

    #[test]
    fn dry_run_allows_zero_files_for_missing_glob() {
        let mut args = base_query_args();
        args.glob = Some("/tmp/dtoo-never-matches/**/*.csv".to_string());
        args.dry_run = true;
        args.on_error = OnErrorMode::Fail;

        let result = QueryPipeline::run(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn dry_run_pipe_data_uses_stdin_placeholder_without_reading_input() {
        let mut args = base_query_args();
        args.pipe = Some(PipeMode::Data);
        args.stdin_format = Some(StdinFormat::Csv);
        args.dry_run = true;
        args.delimiter = '|';

        let resolution = resolve_files_for_dry_run(&args).expect("dry-run files should resolve");
        assert_eq!(resolution.files.len(), 1);
        assert_eq!(resolution.files[0].path, "(stdin data)");
        assert_eq!(
            resolution.files[0].format,
            FileFormat::Csv { delimiter: '|' }
        );
    }

    #[test]
    fn format_count_inserts_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(12), "12");
        assert_eq!(format_count(1_204), "1,204");
        assert_eq!(format_count(48_721), "48,721");
    }

    fn base_query_args() -> QueryArgs {
        QueryArgs {
            paths: Vec::new(),
            glob: None,
            exclude: Vec::new(),
            pipe: None,
            stdin_format: None,
            sheet: None,
            where_clause: None,
            filter_sql: None,
            post_sql: None,
            refs: Vec::new(),
            schema: None,
            output: None,
            output_format: OutputFormat::Csv,
            delimiter: ',',
            lineage: None,
            mask: None,
            mask_salt: String::new(),
            profile: None,
            profile_format: ProfileFormat::Json,
            profile_sample: 100,
            limit: None,
            no_header: false,
            compress: None,
            count: false,
            expect_at_least: None,
            fingerprint: false,
            dry_run: false,
            verbose: false,
            on_error: OnErrorMode::Fail,
            manifest: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            config: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_output_profile: None,
            encrypt_columns: None,
        }
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dtoo-{prefix}-{nanos}-{counter}.{ext}"))
    }
}
