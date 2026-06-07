use std::{collections::BTreeMap, path::Path};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{cli::QueryArgs, error::DtooError};

#[derive(Clone, Debug, Serialize)]
pub struct Manifest {
    pub batch_id: String,
    pub batch_hash: String,
    pub batch_timestamp: String,
    pub dtoo_version: String,
    pub command: ManifestCommand,
    pub files: ManifestFiles,
    pub output: ManifestOutput,
    pub timing: ManifestTiming,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManifestCommand {
    pub glob: Option<String>,
    pub exclude: Vec<String>,
    #[serde(rename = "where")]
    pub where_clause: Option<String>,
    pub filter_sql: Option<String>,
    pub post_sql: Option<String>,
    #[serde(rename = "ref")]
    pub ref_tables: BTreeMap<String, String>,
    pub schema: Option<String>,
    pub output_format: String,
    pub lineage: Option<String>,
    pub mask: Vec<String>,
    pub on_error: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManifestFiles {
    pub total: usize,
    pub processed: usize,
    pub skipped: usize,
    pub details: Vec<ManifestFileDetail>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManifestFileDetail {
    pub path: String,
    pub rows_matched: usize,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManifestOutput {
    pub path: Option<String>,
    pub format: String,
    pub rows: usize,
    pub fingerprint: Option<String>,
    pub compressed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManifestTiming {
    pub started: String,
    pub finished: String,
    pub duration_seconds: f64,
}

pub struct ManifestWriter;

impl ManifestWriter {
    pub fn write(path: &Path, manifest: &Manifest) -> Result<(), DtooError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| DtooError::Output {
                message: format!(
                    "failed to create manifest directory {}: {source}",
                    parent.display()
                ),
            })?;
        }

        let contents = serde_yaml::to_string(manifest).map_err(|source| DtooError::Output {
            message: format!("failed to serialize manifest YAML: {source}"),
        })?;

        std::fs::write(path, contents).map_err(|source| DtooError::Output {
            message: format!("failed to write manifest {}: {source}", path.display()),
        })
    }
}

pub fn build_command(args: &QueryArgs) -> ManifestCommand {
    let mut ref_tables = BTreeMap::new();
    for reference in &args.refs {
        if let Some((name, path)) = reference.split_once('=') {
            ref_tables.insert(name.trim().to_string(), path.trim().to_string());
        }
    }

    ManifestCommand {
        glob: args.glob.clone(),
        exclude: args.exclude.clone(),
        where_clause: args.where_clause.clone(),
        filter_sql: args.filter_sql.clone(),
        post_sql: args.post_sql.clone(),
        ref_tables,
        schema: args.schema.as_ref().map(|path| path.display().to_string()),
        output_format: output_format_label(args),
        lineage: args.lineage.clone(),
        mask: mask_columns(args.mask.as_deref()),
        on_error: on_error_label(args),
    }
}

pub fn duration_seconds(started: DateTime<Utc>, finished: DateTime<Utc>) -> f64 {
    let millis = (finished - started).num_milliseconds();
    (millis as f64) / 1000.0
}

fn output_format_label(args: &QueryArgs) -> String {
    match args.output_format {
        crate::cli::OutputFormat::Csv => "csv".to_string(),
        crate::cli::OutputFormat::Parquet => "parquet".to_string(),
        crate::cli::OutputFormat::Ndjson => "ndjson".to_string(),
    }
}

fn on_error_label(args: &QueryArgs) -> String {
    match args.on_error {
        crate::cli::OnErrorMode::Skip => "skip".to_string(),
        crate::cli::OnErrorMode::Fail => "fail".to_string(),
    }
}

fn mask_columns(mask: Option<&str>) -> Vec<String> {
    mask.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|column| !column.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    })
    .unwrap_or_default()
}
