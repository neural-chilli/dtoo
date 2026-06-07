use crate::{
    cli::OnErrorMode,
    error::{DtooError, EXIT_GENERAL_ERROR, EXIT_PARTIAL_FAILURE, EXIT_SUCCESS},
};

/// Running counts captured during file processing.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FileProcessingStats {
    pub total: usize,
    pub processed: usize,
    pub skipped: usize,
}

/// Final status for the file-processing loop under `--on-error` behavior.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OnErrorCompletion {
    Success,
    PartialFailure { summary: String },
    CompleteFailure { summary: String },
}

impl OnErrorCompletion {
    /// Returns the exit code associated with the completion state.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Success => EXIT_SUCCESS,
            Self::PartialFailure { .. } => EXIT_PARTIAL_FAILURE,
            Self::CompleteFailure { .. } => EXIT_GENERAL_ERROR,
        }
    }
}

/// Tracks per-file outcomes and applies `--on-error fail|skip` semantics.
#[derive(Debug, Clone)]
pub struct OnErrorTracker {
    mode: OnErrorMode,
    stats: FileProcessingStats,
}

impl OnErrorTracker {
    /// Create a tracker for a file set of known total size.
    pub fn new(mode: OnErrorMode, total_files: usize) -> Self {
        Self {
            mode,
            stats: FileProcessingStats {
                total: total_files,
                processed: 0,
                skipped: 0,
            },
        }
    }

    /// Record one successfully-processed file.
    pub fn record_success(&mut self) {
        self.stats.processed += 1;
    }

    /// Record a file error according to the selected `--on-error` mode.
    pub fn record_failure(
        &mut self,
        path: &str,
        message: &str,
    ) -> Result<Option<String>, DtooError> {
        match self.mode {
            OnErrorMode::Fail => Err(DtooError::FileProcess {
                path: path.to_string(),
                message: message.to_string(),
            }),
            OnErrorMode::Skip => {
                self.stats.skipped += 1;
                Ok(Some(format!("Warning: Skipping {path}: {message}")))
            }
        }
    }

    /// Snapshot current processing counters.
    pub fn stats(&self) -> FileProcessingStats {
        self.stats
    }

    /// Finalise processing and produce summary + status.
    pub fn finish(self) -> OnErrorCompletion {
        if self.stats.skipped == 0 {
            return OnErrorCompletion::Success;
        }

        let summary = format!(
            "Processed {}/{} files ({} skipped)",
            self.stats.processed, self.stats.total, self.stats.skipped
        );

        if self.stats.processed == 0 {
            OnErrorCompletion::CompleteFailure { summary }
        } else {
            OnErrorCompletion::PartialFailure { summary }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_mode_aborts_immediately() {
        let mut tracker = OnErrorTracker::new(OnErrorMode::Fail, 2);
        let err = tracker
            .record_failure("data/corrupt.csv", "Invalid CSV at row 15")
            .expect_err("fail mode should abort");

        assert_eq!(
            err.to_string(),
            "Failed to process data/corrupt.csv: Invalid CSV at row 15"
        );
    }

    #[test]
    fn skip_mode_returns_warning_and_continues() {
        let mut tracker = OnErrorTracker::new(OnErrorMode::Skip, 2);
        let warning = tracker
            .record_failure("data/corrupt.csv", "Invalid CSV at row 15")
            .expect("skip mode should continue")
            .expect("warning should be emitted");

        assert_eq!(
            warning,
            "Warning: Skipping data/corrupt.csv: Invalid CSV at row 15"
        );
        assert_eq!(
            tracker.stats(),
            FileProcessingStats {
                total: 2,
                processed: 0,
                skipped: 1,
            }
        );
    }

    #[test]
    fn skip_mode_partial_failure_uses_exit_code_three() {
        let mut tracker = OnErrorTracker::new(OnErrorMode::Skip, 3);
        tracker.record_success();
        let _ = tracker.record_failure("data/corrupt.csv", "bad row");

        let completion = tracker.finish();
        assert_eq!(
            completion,
            OnErrorCompletion::PartialFailure {
                summary: "Processed 1/3 files (1 skipped)".to_string(),
            }
        );
        assert_eq!(completion.exit_code(), EXIT_PARTIAL_FAILURE);
    }

    #[test]
    fn skip_mode_all_failed_uses_general_error() {
        let mut tracker = OnErrorTracker::new(OnErrorMode::Skip, 2);
        let _ = tracker.record_failure("data/a.csv", "bad");
        let _ = tracker.record_failure("data/b.csv", "bad");

        let completion = tracker.finish();
        assert_eq!(
            completion,
            OnErrorCompletion::CompleteFailure {
                summary: "Processed 0/2 files (2 skipped)".to_string(),
            }
        );
        assert_eq!(completion.exit_code(), EXIT_GENERAL_ERROR);
    }
}
