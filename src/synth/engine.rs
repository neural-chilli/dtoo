//! Synth orchestration: spec execution and single-table generation.

use crate::{cli::SynthArgs, error::DtooError};

/// Entry point for `dtoo synth` (filled in by the orchestration task).
pub fn run(_args: &SynthArgs) -> Result<(), DtooError> {
    Err(DtooError::Config {
        message: "synth is not implemented yet".to_string(),
    })
}
