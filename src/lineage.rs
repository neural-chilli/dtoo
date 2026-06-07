use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::DtooError;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum LineageColumn {
    BatchId,
    RecordId,
    BatchTimestamp,
    BatchHash,
    OriginFile,
}

impl LineageColumn {
    fn parse(input: &str) -> Option<Self> {
        match input {
            "batch_id" => Some(Self::BatchId),
            "record_id" => Some(Self::RecordId),
            "batch_timestamp" => Some(Self::BatchTimestamp),
            "batch_hash" => Some(Self::BatchHash),
            "origin_file" => Some(Self::OriginFile),
            _ => None,
        }
    }
}

/// Context used to compute deterministic batch lineage metadata.
#[derive(Clone, Debug, Default)]
pub struct LineageContext {
    pub where_clause: Option<String>,
    pub filter_sql: Option<String>,
    pub post_sql: Option<String>,
    pub files: Vec<String>,
    pub schema_path: Option<String>,
}

/// Handles lineage computation and column application.
#[derive(Clone, Debug)]
pub struct LineageManager {
    requested: HashSet<LineageColumn>,
    batch_id: String,
    batch_timestamp: DateTime<Utc>,
    batch_hash: String,
}

impl LineageManager {
    /// Construct lineage manager from CLI selection and run context.
    pub fn new(lineage: Option<&str>, context: LineageContext) -> Result<Self, DtooError> {
        let requested = parse_requested_columns(lineage)?;
        let batch_id = Uuid::new_v4().to_string();
        let batch_timestamp = Utc::now();
        let batch_hash = compute_batch_hash(&context);

        Ok(Self {
            requested,
            batch_id,
            batch_timestamp,
            batch_hash,
        })
    }

    /// Returns true when `origin_file` lineage requires per-file tracking.
    pub fn requires_origin_tracking(&self) -> bool {
        self.requested.contains(&LineageColumn::OriginFile)
    }

    /// Returns generated batch identifier for this run.
    pub fn batch_id(&self) -> &str {
        &self.batch_id
    }

    /// Returns generated batch hash for this run.
    pub fn batch_hash(&self) -> &str {
        &self.batch_hash
    }

    /// Returns generated batch timestamp for this run.
    pub fn batch_timestamp(&self) -> DateTime<Utc> {
        self.batch_timestamp
    }

    /// Apply requested lineage columns to a DataFrame (native Polars).
    pub fn apply_to_dataframe(
        &self,
        mut df: polars::prelude::DataFrame,
    ) -> Result<polars::prelude::DataFrame, DtooError> {
        use polars::prelude::*;

        if self.requested.is_empty() {
            return Ok(df);
        }

        let n = df.height();
        let schema_err = |e: PolarsError| DtooError::Schema {
            message: e.to_string(),
        };

        if self.requested.contains(&LineageColumn::BatchId) {
            df.with_column(Series::new("batch_id".into(), vec![self.batch_id.clone(); n]).into())
                .map_err(schema_err)?;
        }

        if self.requested.contains(&LineageColumn::RecordId) {
            let ids: Vec<String> = (0..n).map(|_| Uuid::new_v4().to_string()).collect();
            df.with_column(Series::new("record_id".into(), ids).into())
                .map_err(schema_err)?;
        }

        if self.requested.contains(&LineageColumn::BatchTimestamp) {
            df.with_column(
                Series::new(
                    "batch_timestamp".into(),
                    vec![self.batch_timestamp.to_rfc3339(); n],
                )
                .into(),
            )
            .map_err(schema_err)?;
        }

        if self.requested.contains(&LineageColumn::BatchHash) {
            df.with_column(
                Series::new("batch_hash".into(), vec![self.batch_hash.clone(); n]).into(),
            )
            .map_err(schema_err)?;
        }

        let has_origin = df
            .get_column_names()
            .iter()
            .any(|c| c.as_str() == "_origin_file");

        if self.requested.contains(&LineageColumn::OriginFile) {
            if !has_origin {
                return Err(DtooError::Schema {
                    message:
                        "origin_file lineage requested but internal _origin_file column is missing"
                            .to_string(),
                });
            }
            df.rename("_origin_file", "origin_file".into())
                .map_err(schema_err)?;
        } else if has_origin {
            df = df.drop("_origin_file").map_err(schema_err)?;
        }

        Ok(df)
    }
}

fn parse_requested_columns(lineage: Option<&str>) -> Result<HashSet<LineageColumn>, DtooError> {
    let mut requested = HashSet::new();
    let Some(lineage) = lineage else {
        return Ok(requested);
    };

    if lineage == "all" {
        requested.insert(LineageColumn::BatchId);
        requested.insert(LineageColumn::RecordId);
        requested.insert(LineageColumn::BatchTimestamp);
        requested.insert(LineageColumn::BatchHash);
        requested.insert(LineageColumn::OriginFile);
        return Ok(requested);
    }

    for token in lineage.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let Some(column) = LineageColumn::parse(token) else {
            return Err(DtooError::Config {
                message: format!(
                    "invalid --lineage column `{token}`; valid options: batch_id, record_id, batch_timestamp, batch_hash, origin_file"
                ),
            });
        };
        requested.insert(column);
    }
    Ok(requested)
}

fn compute_batch_hash(context: &LineageContext) -> String {
    let mut hasher = Sha256::new();
    hasher.update(context.filter_sql.as_deref().unwrap_or(""));
    hasher.update(context.post_sql.as_deref().unwrap_or(""));
    hasher.update(context.where_clause.as_deref().unwrap_or(""));

    let mut files = context.files.clone();
    files.sort();
    for file in files {
        hasher.update(file);
    }

    if let Some(schema) = &context.schema_path {
        hasher.update(schema);
    }

    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_lineage_adds_requested_columns_and_renames_origin() {
        use polars::prelude::*;
        let df = df!["id" => [1i64], "_origin_file" => ["/tmp/a.csv"]].unwrap();
        let mgr = LineageManager::new(
            Some("batch_id,record_id,origin_file"),
            LineageContext {
                files: vec!["/tmp/a.csv".to_string()],
                ..LineageContext::default()
            },
        )
        .unwrap();
        let out = mgr.apply_to_dataframe(df).unwrap();
        let names = out.get_column_names();
        assert!(names.iter().any(|n| n.as_str() == "batch_id"));
        assert!(names.iter().any(|n| n.as_str() == "record_id"));
        assert!(names.iter().any(|n| n.as_str() == "origin_file"));
        assert!(!names.iter().any(|n| n.as_str() == "_origin_file"));
        assert_eq!(
            out.column("origin_file").unwrap().str().unwrap().get(0),
            Some("/tmp/a.csv")
        );
    }

    #[test]
    fn apply_lineage_origin_requested_but_missing_errors() {
        use polars::prelude::*;
        let df = df!["id" => [1i64]].unwrap();
        let mgr = LineageManager::new(Some("origin_file"), LineageContext::default()).unwrap();
        assert!(matches!(
            mgr.apply_to_dataframe(df),
            Err(DtooError::Schema { .. })
        ));
    }

    #[test]
    fn batch_hash_is_deterministic_for_same_context() {
        let context = LineageContext {
            where_clause: Some("status = 'active'".to_string()),
            filter_sql: Some("SELECT * FROM _".to_string()),
            post_sql: Some("SELECT * FROM _".to_string()),
            files: vec!["b.csv".to_string(), "a.csv".to_string()],
            schema_path: Some("schema.yaml".to_string()),
        };
        let first = compute_batch_hash(&context);
        let second = compute_batch_hash(&context);
        assert_eq!(first, second);
    }
}
