use std::error::Error;

use thiserror::Error;

/// Exit code used for successful command completion.
pub const EXIT_SUCCESS: u8 = 0;
/// Exit code used for general runtime errors.
pub const EXIT_GENERAL_ERROR: u8 = 1;
/// Exit code used when `--expect-at-least` validation fails.
pub const EXIT_VALIDATION_FAILURE: u8 = 2;
/// Exit code used when `--on-error skip` encounters skipped files.
pub const EXIT_PARTIAL_FAILURE: u8 = 3;
/// Exit code used when assertion rules fail (Pro feature).
pub const EXIT_ASSERTION_FAILURE: u8 = 4;
/// Exit code used when schema drift is detected in fail mode (Pro feature).
pub const EXIT_SCHEMA_DRIFT: u8 = 5;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// The canonical error type for all `dtoo` operations.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum DtooError {
    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("Failed to read {path}: {source}")]
    FileRead { path: String, source: BoxError },

    #[error("Permission denied: {path}")]
    PermissionDenied { path: String },

    #[error("Failed to process {path}: {message}")]
    FileProcess { path: String, message: String },

    #[error("SQL error in {context}:\n  {sql}\n  {source}")]
    Sql {
        context: String,
        sql: String,
        source: BoxError,
    },

    #[error("Invalid configuration: {message}")]
    Config { message: String },

    #[error("Schema error: {message}")]
    Schema { message: String },

    #[error("Output error: {message}")]
    Output { message: String },

    #[error("Expected at least {expected} records, got {actual}")]
    ExpectAtLeast { expected: usize, actual: usize },

    #[error("No files matched glob pattern: {pattern}")]
    NoFilesMatched { pattern: String },

    #[error("Extension load failed: {extension}: {source}")]
    ExtensionLoad { extension: String, source: BoxError },

    #[error("Processed {processed}/{total} files ({skipped} skipped)")]
    PartialFailure {
        processed: usize,
        total: usize,
        skipped: usize,
    },

    #[error("Assertion failure: {message}")]
    AssertionFailure { message: String },

    #[error("Schema drift detected: {message}")]
    SchemaDrift { message: String },
}

impl DtooError {
    /// Returns the process exit code associated with this error.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::ExpectAtLeast { .. } => EXIT_VALIDATION_FAILURE,
            Self::PartialFailure { .. } => EXIT_PARTIAL_FAILURE,
            Self::AssertionFailure { .. } => EXIT_ASSERTION_FAILURE,
            Self::SchemaDrift { .. } => EXIT_SCHEMA_DRIFT,
            _ => EXIT_GENERAL_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_expect_at_least_to_validation_exit_code() {
        let err = DtooError::ExpectAtLeast {
            expected: 100,
            actual: 42,
        };
        assert_eq!(err.exit_code(), EXIT_VALIDATION_FAILURE);
    }

    #[test]
    fn maps_partial_failure_to_partial_exit_code() {
        let err = DtooError::PartialFailure {
            processed: 95,
            total: 100,
            skipped: 5,
        };
        assert_eq!(err.exit_code(), EXIT_PARTIAL_FAILURE);
    }

    #[test]
    fn formats_sql_errors_with_context_and_sql() {
        let err = DtooError::Sql {
            context: "--filter-sql".to_string(),
            sql: "SELECT id, namee FROM _".to_string(),
            source: boxed_err("Column \"namee\" not found"),
        };

        let rendered = err.to_string();
        assert!(rendered.contains("SQL error in --filter-sql:"));
        assert!(rendered.contains("SELECT id, namee FROM _"));
        assert!(rendered.contains("Column \"namee\" not found"));
    }

    #[test]
    fn formats_file_process_errors_with_path() {
        let err = DtooError::FileProcess {
            path: "data/2024-01/sales.csv".to_string(),
            message: "Invalid CSV: expected 5 columns but found 6 at row 15".to_string(),
        };

        assert_eq!(
            err.to_string(),
            "Failed to process data/2024-01/sales.csv: Invalid CSV: expected 5 columns but found 6 at row 15"
        );
    }

    fn boxed_err(message: &'static str) -> BoxError {
        Box::new(std::io::Error::other(message))
    }
}
